use super::connection::{ConnectionRejector, EngineConnection, HandoffConfig, SnapshotTask};
use super::direct::{DirectConnection, DirectServer, ShardArcConnection};
#[cfg(feature = "embedded")]
use super::transactions::TransactionCoordinator;
#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
use super::transport::{MonoioMultiDirectWorker, MonoioWorkerConfig};
#[cfg(feature = "embedded")]
use super::transport::{
    MultiDirectAddress, MultiDirectConnection, MultiDirectWorker, MultiDirectWorkerMessage,
    TokioHybridWorkerConfig, TokioWorkerConfig,
};
use super::*;

impl ShardCacheServer {
    pub fn new(config: ShardCacheConfig, engine: EngineHandle) -> Self {
        Self {
            config,
            engine: Some(engine),
            mode: ServerMode::Auto,
            unix_socket_path: None,
            embedded_store: None,
            shard_arc_store: None,
            thread_local_embedded_store: false,
        }
    }

    pub fn with_mode(config: ShardCacheConfig, engine: EngineHandle, mode: ServerMode) -> Self {
        Self {
            config,
            engine: Some(engine),
            mode,
            unix_socket_path: None,
            embedded_store: None,
            shard_arc_store: None,
            thread_local_embedded_store: false,
        }
    }

    pub fn direct(config: ShardCacheConfig) -> Self {
        Self {
            config,
            engine: None,
            mode: ServerMode::Direct,
            unix_socket_path: None,
            embedded_store: None,
            shard_arc_store: None,
            thread_local_embedded_store: false,
        }
    }

    /// Exposes an existing embedded store over the shardcache RESP/SCNP server.
    ///
    /// The supplied store remains owned by the embedding process. Local
    /// in-process reads and writes and remote protocol requests all observe the
    /// same data. Server configuration controls the listener and connection
    /// behavior; configure memory limits directly on the store before serving
    /// it if the embedding process owns that policy.
    pub fn from_embedded_store(config: ShardCacheConfig, store: Arc<EmbeddedStore>) -> Self {
        Self {
            config,
            engine: None,
            mode: ServerMode::Direct,
            unix_socket_path: None,
            embedded_store: Some(store),
            shard_arc_store: None,
            thread_local_embedded_store: false,
        }
    }

    /// Exposes a shard-shared embedded store over the benchmark GET/SET server.
    ///
    /// This mode is intentionally narrower than `from_embedded_store`: it is a
    /// measurement surface for architectures where the sharing boundary is an
    /// `Arc` per storage shard rather than one `Arc` around the full store.
    #[doc(hidden)]
    pub fn from_benchmark_shard_arc_embedded_store(
        config: ShardCacheConfig,
        store: Arc<ShardArcEmbeddedStore>,
    ) -> Self {
        Self {
            config,
            engine: None,
            mode: ServerMode::Direct,
            unix_socket_path: None,
            embedded_store: None,
            shard_arc_store: Some(store),
            thread_local_embedded_store: false,
        }
    }

    #[doc(hidden)]
    #[deprecated(
        note = "benchmark topology probe; use from_embedded_store for public fanout or server_endpoint_mode=direct_shard for direct shard ports"
    )]
    pub fn from_shard_arc_embedded_store(
        config: ShardCacheConfig,
        store: Arc<ShardArcEmbeddedStore>,
    ) -> Self {
        Self::from_benchmark_shard_arc_embedded_store(config, store)
    }

    /// Serves the [`LocalEmbeddedStore`]
    /// already installed on this thread.
    ///
    /// This mode is for owner-local embedded deployments where the embedding
    /// process and the TCP server must use the exact same shard-owned memory
    /// without falling back to shared `EmbeddedStore` locks. Install a local
    /// store with `LocalEmbeddedStore::install_local()` before calling `run` or
    /// `run_with_shutdown`; the server leaves that store installed when it
    /// stops.
    pub fn from_thread_local_embedded_store(config: ShardCacheConfig) -> Self {
        Self {
            config,
            engine: None,
            mode: ServerMode::Direct,
            unix_socket_path: None,
            embedded_store: None,
            shard_arc_store: None,
            thread_local_embedded_store: true,
        }
    }

    pub fn with_unix_socket(mut self, path: PathBuf) -> Self {
        self.unix_socket_path = Some(path);
        self
    }

    pub async fn run(self) -> Result<()> {
        self.reject_embedded_kv_overflow_config()?;
        if self.shard_arc_store.is_some() {
            return self
                .run_shard_arc_with_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await;
        }
        if self.should_run_multi_direct() {
            return self
                .run_multi_direct_with_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await;
        }
        if self.should_run_direct() {
            return self
                .run_direct_with_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await;
        }

        self.run_engine_with_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }

    pub async fn run_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        self.reject_embedded_kv_overflow_config()?;
        if self.shard_arc_store.is_some() {
            return self.run_shard_arc_with_shutdown(shutdown).await;
        }
        if self.should_run_multi_direct() {
            return self.run_multi_direct_with_shutdown(shutdown).await;
        }
        if self.should_run_direct() {
            return Err(crate::ShardCacheError::Config(
                "thread-local embedded servers must use run_thread_local_with_shutdown because the owner-local runtime is !Send"
                    .into(),
            ));
        }

        self.run_engine_with_shutdown(shutdown).await
    }

    pub async fn run_thread_local_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        self.reject_embedded_kv_overflow_config()?;
        if !self.should_run_direct() {
            return Err(crate::ShardCacheError::Config(
                "run_thread_local_with_shutdown requires from_thread_local_embedded_store".into(),
            ));
        }
        self.run_direct_with_shutdown(shutdown).await
    }

    fn reject_embedded_kv_overflow_config(&self) -> Result<()> {
        if self.config.kv_overflow.enabled {
            return Err(crate::ShardCacheError::Config(
                "kv_overflow is an embedded KvOverflowStore feature; standalone server mode can be used as an overflow node but cannot act as the wrapped primary"
                    .into(),
            ));
        }
        Ok(())
    }

    async fn run_shard_arc_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        if self.unix_socket_path.is_some() {
            return Err(crate::ShardCacheError::Config(
                "shard-arc embedded server mode does not support unix sockets".into(),
            ));
        }
        let store = self
            .shard_arc_store
            .as_ref()
            .expect("shard-arc server requires store")
            .clone();
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        tracing::info!(
            "shardcache listening on {} (shard-arc embedded GET/SET mode, {} shards)",
            self.config.bind_addr,
            store.shard_count()
        );

        let limiter = Arc::new(Semaphore::new(self.config.max_connections));
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("shutdown requested");
                    break;
                }
                accept_result = listener.accept() => {
                    let (stream, peer_addr) = accept_result?;
                    stream.set_nodelay(true)?;
                    tracing::debug!("accepted connection from {peer_addr}");
                    let permit = match limiter.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            ConnectionRejector::reject(stream).await?;
                            continue;
                        }
                    };
                    let store = store.clone();
                    tokio::spawn(async move {
                        if let Err(error) = ShardArcConnection::handle(stream, store, permit).await {
                            tracing::warn!("shard-arc connection closed with error: {error}");
                        }
                    });
                }
            }
        }

        Ok(())
    }

    async fn run_engine_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        if let Some(path) = self.unix_socket_path.clone() {
            UnixSocketPath::prepare(&path)?;
            let listener = UnixListener::bind(&path)?;
            tracing::info!("shardcache listening on unix://{}", path.display());

            let limiter = Arc::new(Semaphore::new(self.config.max_connections));
            let snapshot_task = SnapshotTask::spawn(self.engine().clone());
            tokio::pin!(shutdown);

            loop {
                tokio::select! {
                    _ = &mut shutdown => {
                        tracing::info!("shutdown requested");
                        break;
                    }
                    accept_result = listener.accept() => {
                        let (stream, _addr) = accept_result?;
                        let permit = match limiter.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                ConnectionRejector::reject(stream).await?;
                                continue;
                            }
                        };
                        let engine = self.engine().clone();
                        let (read_half, write_half) = stream.into_split();
                        let write_handoff = WriteHandoff::spawn(write_half, HandoffConfig::write());
                        tokio::spawn(async move {
                            if let Err(error) =
                                EngineConnection::handle(read_half, write_handoff, engine, permit).await
                            {
                                tracing::warn!("connection closed with error: {error}");
                            }
                        });
                    }
                }
            }

            snapshot_task.abort();
            let _ = snapshot_task.await;
            UnixSocketPath::cleanup(&path);
            return self.engine().shutdown().await;
        }

        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        tracing::info!("shardcache listening on {}", self.config.bind_addr);

        let limiter = Arc::new(Semaphore::new(self.config.max_connections));
        let snapshot_task = SnapshotTask::spawn(self.engine().clone());
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("shutdown requested");
                    break;
                }
                accept_result = listener.accept() => {
                    let (stream, peer_addr) = accept_result?;
                    stream.set_nodelay(true)?;
                    tracing::debug!("accepted connection from {peer_addr}");
                    let permit = match limiter.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            ConnectionRejector::reject(stream).await?;
                            continue;
                        }
                    };
                    let engine = self.engine().clone();
                    let (read_half, write_half) = stream.into_split();
                    let write_handoff = WriteHandoff::spawn(write_half, HandoffConfig::write());
                    tokio::spawn(async move {
                        if let Err(error) =
                            EngineConnection::handle(read_half, write_handoff, engine, permit).await
                        {
                            tracing::warn!("connection closed with error: {error}");
                        }
                    });
                }
            }
        }

        snapshot_task.abort();
        let _ = snapshot_task.await;
        self.engine().shutdown().await
    }
}

trait ServerModeRouting {
    fn should_run_direct(&self) -> bool;
    fn should_run_multi_direct(&self) -> bool;
    fn engine(&self) -> &EngineHandle;
}

impl ServerModeRouting for ShardCacheServer {
    fn should_run_direct(&self) -> bool {
        self.thread_local_embedded_store
    }

    fn should_run_multi_direct(&self) -> bool {
        // Direct mode now always uses multi-direct (with at least 1 worker).
        !self.thread_local_embedded_store
            && (matches!(self.mode, ServerMode::Direct)
                || (matches!(self.mode, ServerMode::Auto)
                    && !self.config.persistence.enabled
                    && self.config.shard_count >= 1))
    }

    fn engine(&self) -> &EngineHandle {
        self.engine
            .as_ref()
            .expect("engine-backed server requires an engine handle")
    }
}

impl ShardCacheServer {
    async fn run_direct_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        if self.thread_local_embedded_store {
            DirectServer::initialize_thread_local(&self.config)?;
        } else {
            DirectServer::initialize(&self.config)?;
        }

        let result = if let Some(path) = self.unix_socket_path.clone() {
            UnixSocketPath::prepare(&path)?;
            let listener = UnixListener::bind(&path)?;
            tracing::info!(
                "shardcache listening on unix://{} (direct local mode)",
                path.display()
            );

            let limiter = Arc::new(Semaphore::new(self.config.max_connections));
            let local = LocalSet::new();
            let config = self.config.clone();
            let result = local.run_until(async move {
                let mut maintenance = interval(config.ttl_sweep_interval());
                maintenance.set_missed_tick_behavior(MissedTickBehavior::Delay);
                tokio::pin!(shutdown);

                loop {
                    tokio::select! {
                        _ = &mut shutdown => {
                            tracing::info!("shutdown requested");
                            break;
                        }
                        _ = maintenance.tick() => {
                            DirectServer::process_maintenance();
                        }
                        accept_result = listener.accept() => {
                            let (stream, _addr) = accept_result?;
                            let permit = match limiter.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    ConnectionRejector::reject(stream).await?;
                                    continue;
                                }
                            };
                            spawn_local(async move {
                                if let Err(error) = DirectConnection::handle(stream, permit).await {
                                    tracing::warn!("connection closed with error: {error}");
                                }
                            });
                        }
                    }
                }

                Ok(())
            })
            .await;
            UnixSocketPath::cleanup(&path);
            result
        } else {
            let listener = TcpListener::bind(&self.config.bind_addr).await?;
            tracing::info!(
                "shardcache listening on {} (direct local mode)",
                self.config.bind_addr
            );

            let limiter = Arc::new(Semaphore::new(self.config.max_connections));
            let local = LocalSet::new();
            let config = self.config.clone();
            local.run_until(async move {
                let mut maintenance = interval(config.ttl_sweep_interval());
                maintenance.set_missed_tick_behavior(MissedTickBehavior::Delay);
                tokio::pin!(shutdown);

                loop {
                    tokio::select! {
                        _ = &mut shutdown => {
                            tracing::info!("shutdown requested");
                            break;
                        }
                        _ = maintenance.tick() => {
                            DirectServer::process_maintenance();
                        }
                        accept_result = listener.accept() => {
                            let (stream, peer_addr) = accept_result?;
                            stream.set_nodelay(true)?;
                            tracing::debug!("accepted connection from {peer_addr}");
                            let permit = match limiter.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    ConnectionRejector::reject(stream).await?;
                                    continue;
                                }
                            };
                            spawn_local(async move {
                                if let Err(error) = DirectConnection::handle(stream, permit).await {
                                    tracing::warn!("connection closed with error: {error}");
                                }
                            });
                        }
                    }
                }

                Ok(())
            })
            .await
        };

        DirectServer::clear();
        result
    }

    async fn run_multi_direct_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        if self.unix_socket_path.is_some() {
            return Err(crate::ShardCacheError::Config(
                "multi-direct mode does not support unix sockets yet; use --shard-count 1".into(),
            ));
        }

        let bind_addr: SocketAddr = self.config.bind_addr.parse().map_err(|error| {
            crate::ShardCacheError::Config(format!(
                "invalid bind addr {}: {error}",
                self.config.bind_addr
            ))
        })?;

        let max_connections = self.config.max_connections;
        let caller_owned_embedded_store = self.embedded_store.is_some();
        let shared_store = self.multi_direct_store()?;
        let shard_count = shared_store.shard_count();
        let fanout_routes_to_owner = caller_owned_embedded_store;
        let limiter = Arc::new(Semaphore::new(max_connections));
        let transaction_coordinator =
            TransactionCoordinator::new(shard_count, self.config.transaction_mode).map(Arc::new);

        #[cfg(all(target_os = "linux", feature = "monoio"))]
        let use_monoio = std::env::var("SHARDCACHE_USE_MONOIO").is_ok_and(|v| v != "0");
        #[cfg(any(not(target_os = "linux"), not(feature = "monoio")))]
        let use_monoio = false;
        if use_monoio && shared_store.overflow_replica_auth().is_some() {
            return Err(crate::ShardCacheError::Config(
                "authenticated kv overflow replicas do not support SHARDCACHE_USE_MONOIO=1 yet"
                    .into(),
            ));
        }
        #[cfg(feature = "scnp-tls")]
        if use_monoio && shared_store.overflow_replica_tls().is_some() {
            return Err(crate::ShardCacheError::Config(
                "TLS kv overflow replicas do not support SHARDCACHE_USE_MONOIO=1 yet".into(),
            ));
        }
        let direct_shard_ports = matches!(
            self.config.server_endpoint_mode,
            ServerEndpointMode::DirectShard
        ) || std::env::var("SHARDCACHE_DIRECT_SHARD_PORTS")
            .is_ok_and(|v| v != "0");
        if use_monoio && fanout_routes_to_owner {
            return Err(crate::ShardCacheError::Config(
                "owner-routed fanout is not supported with SHARDCACHE_USE_MONOIO=1 yet".into(),
            ));
        }
        let available_workers = std::thread::available_parallelism()
            .map(|available| available.get())
            .unwrap_or(shard_count)
            .max(1);
        let worker_count = if direct_shard_ports {
            shard_count
        } else {
            std::env::var("SHARDCACHE_WORKER_COUNT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(available_workers)
        };
        let direct_base_port = direct_shard_ports
            .then(|| MultiDirectAddress::direct_base_port(bind_addr, shard_count))
            .transpose()?;
        if let Some(direct_base_port) = direct_base_port {
            tracing::info!(
                "multi-direct: exposing shard-owned native ports {}-{}",
                direct_base_port,
                direct_base_port.saturating_add(shard_count.saturating_sub(1) as u16)
            );
        }

        if fanout_routes_to_owner {
            tracing::info!(
                "multi-direct: public fanout routes each request to the owning shard worker"
            );
        } else {
            tracing::info!(
                "multi-direct: public fanout assigns whole connections to worker threads"
            );
        }

        let mut worker_txs: Vec<flume::Sender<MultiDirectWorkerMessage>> =
            Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);

        // Resolve available CPU cores for multi-worker pinning. A single worker
        // inherits the process affinity so taskset/cpuset one-vCPU launches stay
        // strict.
        let core_ids: Vec<core_affinity::CoreId> = if worker_count == 1 {
            Vec::new()
        } else {
            core_affinity::get_core_ids().unwrap_or_default()
        };
        match (worker_count, core_ids.is_empty()) {
            (1, _) => tracing::info!("multi-direct: leaving single worker affinity unchanged"),
            (_, true) => {
                tracing::warn!("multi-direct: no core ids available, workers will not be pinned");
            }
            (_, false) => {
                tracing::info!(
                    "multi-direct: pinning {} workers across {} available cores",
                    worker_count,
                    core_ids.len()
                );
            }
        }

        let single_threaded = worker_count == 1 && cfg!(feature = "unsafe");
        let started_at = Instant::now();
        for worker_id in 0..worker_count {
            // Public fanout uses this channel either for legacy whole-socket
            // handoff or for strict per-request shard routing. Shard-port
            // workers bind and accept inside their own pinned worker thread.
            let (tx, rx) = flume::bounded::<MultiDirectWorkerMessage>(256);
            worker_txs.push(tx);
            let store = shared_store.clone();
            let limiter = limiter.clone();
            let transaction_coordinator = transaction_coordinator.clone();
            let core_id = if core_ids.is_empty() {
                None
            } else {
                Some(core_ids[worker_id % core_ids.len()])
            };
            let direct_bind_addr = match direct_base_port {
                Some(port) => Some(MultiDirectAddress::direct_worker_bind_addr(
                    bind_addr, port, worker_id,
                )?),
                None => None,
            };
            let handle = std::thread::Builder::new()
                .name(format!("fc-multi-direct-{worker_id}"))
                .spawn(move || {
                    #[cfg(all(target_os = "linux", feature = "monoio"))]
                    if use_monoio {
                        drop(rx);
                        MonoioMultiDirectWorker::run(
                            MonoioWorkerConfig {
                                worker_id,
                                worker_count,
                                fanout_bind_addr: bind_addr,
                                direct_bind_addr,
                                core_id,
                                single_threaded,
                                started_at,
                                transaction_coordinator,
                            },
                            store,
                            limiter,
                        );
                        return;
                    }
                    if direct_shard_ports {
                        match direct_bind_addr {
                            Some(direct_bind_addr) => MultiDirectWorker::run_hybrid(
                                TokioHybridWorkerConfig {
                                    worker_id,
                                    direct_bind_addr,
                                    core_id,
                                    single_threaded,
                                    owned_shard_id: worker_id,
                                    started_at,
                                    transaction_coordinator,
                                },
                                store,
                                limiter,
                                rx,
                            ),
                            None => tracing::error!(
                                "worker {worker_id} missing direct bind addr despite direct shard ports"
                            ),
                        }
                        return;
                    }
                    MultiDirectWorker::run(
                        TokioWorkerConfig {
                            worker_id,
                            core_id,
                            single_threaded,
                            started_at,
                            transaction_coordinator,
                        },
                        store,
                        limiter,
                        rx,
                    )
                })
                .map_err(|error| {
                    crate::ShardCacheError::Config(format!(
                        "failed to spawn worker thread {worker_id}: {error}"
                    ))
                })?;
            handles.push(handle);
        }

        // Monoio workers accept directly on a shared SO_REUSEPORT listener and,
        // when enabled, shard-owned direct ports. In that case the main runtime
        // only keeps the process alive until ctrl-c. Tokio direct-shard workers
        // still receive fanout connections from the main accept loop below.
        if use_monoio {
            tracing::info!(
                "shardcache main: workers handle accept directly on {}{} ({} workers)",
                bind_addr,
                if direct_shard_ports {
                    " and shard-owned direct ports"
                } else {
                    ""
                },
                worker_count
            );
            shutdown.await;
            tracing::info!("shutdown requested");
            drop(worker_txs);
            for handle in handles {
                let _ = handle.join();
            }
            return Ok(());
        }

        let listener = TcpListener::bind(&bind_addr).await?;
        tracing::info!(
            "shardcache listening on {} (multi-direct, {} workers)",
            bind_addr,
            worker_count
        );

        tokio::pin!(shutdown);

        let worker_txs = Arc::new(worker_txs);
        let mut public_tasks = tokio::task::JoinSet::new();
        let mut next_worker = 0usize;
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("shutdown requested");
                    break;
                }
                finished = public_tasks.join_next(), if !public_tasks.is_empty() => {
                    if let Some(Err(error)) = finished {
                        tracing::warn!("public routed connection task failed: {error}");
                    }
                }
                accept = listener.accept() => {
                    let (stream, _addr) = accept?;
                    let _ = stream.set_nodelay(true);
                    if fanout_routes_to_owner {
                        let permit = match limiter.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                let _ = ConnectionRejector::reject(stream).await;
                                continue;
                            }
                        };
                        let store = shared_store.clone();
                        let worker_txs = Arc::clone(&worker_txs);
                        let transaction_coordinator = transaction_coordinator.clone();
                        public_tasks.spawn(async move {
                            if let Err(error) = MultiDirectConnection::handle_public_routed(
                                stream,
                                store,
                                permit,
                                worker_txs,
                                transaction_coordinator,
                            )
                            .await
                            {
                                tracing::warn!("public routed connection closed with error: {error}");
                            }
                        });
                        continue;
                    }
                    let std_stream = match stream.into_std() {
                        Ok(s) => s,
                        Err(error) => {
                            tracing::warn!("into_std failed: {error}");
                            continue;
                        }
                    };
                    let target = next_worker % worker_txs.len();
                    next_worker = next_worker.wrapping_add(1);
                    if worker_txs[target]
                        .send_async(MultiDirectWorkerMessage::Stream(std_stream))
                        .await
                        .is_err()
                    {
                        tracing::warn!("worker {target} channel closed");
                        break;
                    }
                }
            }
        }

        public_tasks.abort_all();
        while public_tasks.join_next().await.is_some() {}

        // Drop senders to signal workers to exit, then join.
        drop(worker_txs);
        for handle in handles {
            let _ = handle.join();
        }
        Ok(())
    }

    fn multi_direct_store(&self) -> Result<Arc<EmbeddedStore>> {
        if let Some(store) = &self.embedded_store {
            if self.config.kv_overflow_replica.enabled
                && store.route_mode() != EmbeddedRouteMode::OverflowSlot
            {
                return Err(crate::ShardCacheError::Config(
                    "kv overflow replica store must use EmbeddedRouteMode::OverflowSlot".into(),
                ));
            }
            store.configure_overflow_replica(self.overflow_replica_topology(store.shard_count())?);
            store.configure_overflow_replica_auth_runtime(self.overflow_replica_auth_runtime()?);
            #[cfg(feature = "scnp-tls")]
            store.configure_overflow_replica_tls(self.overflow_replica_tls_config()?);
            tracing::info!(
                "multi-direct: serving caller-owned embedded store with {} shards ({:?} routing)",
                store.shard_count(),
                store.route_mode()
            );
            store.configure_object_overflow(ObjectOverflowRuntime::from_config(
                &self.config.object_overflow,
            )?);
            return Ok(Arc::clone(store));
        }

        let route_mode = if self.config.kv_overflow_replica.enabled {
            EmbeddedRouteMode::OverflowSlot
        } else {
            MultiDirectRouteMode::configured()?
        };
        let store = EmbeddedStore::with_route_mode(self.config.shard_count, route_mode);
        store.configure_overflow_replica(self.overflow_replica_topology(store.shard_count())?);
        store.configure_overflow_replica_auth_runtime(self.overflow_replica_auth_runtime()?);
        #[cfg(feature = "scnp-tls")]
        store.configure_overflow_replica_tls(self.overflow_replica_tls_config()?);
        store.configure_memory_policy(
            self.config.per_shard_memory_limit_bytes(),
            self.config.eviction_policy,
        );
        store.configure_object_overflow(ObjectOverflowRuntime::from_config(
            &self.config.object_overflow,
        )?);
        #[cfg(feature = "redis")]
        store.configure_vector_memory_policy(
            self.config.total_memory_limit_bytes(),
            self.config.eviction_policy,
        );
        Ok(Arc::new(store))
    }

    fn overflow_replica_topology(&self, shard_count: usize) -> Result<Option<(String, u16)>> {
        if !self.config.kv_overflow_replica.enabled {
            return Ok(None);
        }
        let bind_addr: SocketAddr = self.config.bind_addr.parse().map_err(|error| {
            crate::ShardCacheError::Config(format!(
                "invalid bind addr {}: {error}",
                self.config.bind_addr
            ))
        })?;
        Ok(Some((
            self.config.kv_overflow_replica.node_id.clone(),
            MultiDirectAddress::direct_base_port(bind_addr, shard_count)?,
        )))
    }

    fn overflow_replica_auth_runtime(
        &self,
    ) -> Result<Option<Arc<crate::storage::OverflowReplicaAuthRuntime>>> {
        let config = &self.config.kv_overflow_replica;
        if let Some(path) = config.auth_token_path.as_ref() {
            let token = read_overflow_replica_auth_file(path)?;
            let reload_path = path.clone();
            let reload = Arc::new(move || read_overflow_replica_auth_file(&reload_path));
            let reload_ms = match config.auth_token_reload_interval_ms {
                0 => 30_000,
                configured => configured,
            };
            let interval = std::time::Duration::from_millis(reload_ms);
            return crate::storage::OverflowReplicaAuthRuntime::new_reloadable(
                token, interval, reload,
            )
            .map(Arc::new)
            .map(Some);
        }
        let Some(name) = config.auth_token_env.as_deref() else {
            return Ok(None);
        };
        let value = std::env::var(name).map_err(|error| {
            crate::ShardCacheError::Config(format!(
                "failed to read kv overflow replica auth token env {name:?}: {error}"
            ))
        })?;
        let token = validate_overflow_replica_auth_token(value.into_bytes(), name)?;
        Ok(Some(Arc::new(
            crate::storage::OverflowReplicaAuthRuntime::new_static(token),
        )))
    }

    #[cfg(feature = "scnp-tls")]
    fn overflow_replica_tls_config(
        &self,
    ) -> Result<Option<Arc<crate::storage::OverflowReplicaTlsRuntime>>> {
        let tls = &self.config.kv_overflow_replica.tls;
        if !tls.enabled {
            return Ok(None);
        }
        let current = build_overflow_replica_tls_server_config(tls)?;
        let fingerprints = parse_client_certificate_fingerprints(&tls.client_cert_sha256)?;
        let reload_tls = tls.clone();
        let reload = Arc::new(move || build_overflow_replica_tls_server_config(&reload_tls));
        Ok(Some(Arc::new(
            crate::storage::OverflowReplicaTlsRuntime::new(
                current,
                reload,
                tls.reload_interval_ms,
                std::time::Duration::from_millis(tls.handshake_timeout_ms),
                fingerprints,
                tls.max_concurrent_handshakes,
            ),
        )))
    }
}

fn read_overflow_replica_auth_file(path: &Path) -> Result<Arc<[u8]>> {
    let mut value = std::fs::read(path)?;
    while value
        .last()
        .is_some_and(|byte| matches!(byte, b'\n' | b'\r'))
    {
        value.pop();
    }
    validate_overflow_replica_auth_token(value, &path.display().to_string())
}

fn validate_overflow_replica_auth_token(value: Vec<u8>, source: &str) -> Result<Arc<[u8]>> {
    if value.is_empty() || value.len() > u16::MAX as usize {
        return Err(crate::ShardCacheError::Config(format!(
            "kv overflow replica auth token {source:?} must contain 1..=65535 bytes"
        )));
    }
    Ok(Arc::from(value))
}

#[cfg(feature = "scnp-tls")]
fn build_overflow_replica_tls_server_config(
    tls: &crate::config::ScnpTlsServerConfig,
) -> Result<Arc<rustls::ServerConfig>> {
    use std::fs::File;
    use std::io::BufReader;

    use rustls::RootCertStore;
    use rustls::server::WebPkiClientVerifier;

    let mut cert_reader = BufReader::new(File::open(&tls.cert_path)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<std::io::Result<Vec<_>>>()?;
    if certs.is_empty() {
        return Err(crate::ShardCacheError::Config(
            "SCNP TLS server certificate file contains no certificates".into(),
        ));
    }
    let mut key_reader = BufReader::new(File::open(&tls.key_path)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
        crate::ShardCacheError::Config("SCNP TLS server key file contains no private key".into())
    })?;
    let builder = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|error| {
        crate::ShardCacheError::Config(format!("invalid SCNP TLS protocol config: {error}"))
    })?;
    let mut config = if let Some(client_ca_path) = &tls.client_ca_path {
        let mut roots = RootCertStore::empty();
        let mut ca_reader = BufReader::new(File::open(client_ca_path)?);
        let certificates =
            rustls_pemfile::certs(&mut ca_reader).collect::<std::io::Result<Vec<_>>>()?;
        if certificates.is_empty() {
            return Err(crate::ShardCacheError::Config(
                "SCNP TLS client CA file contains no certificates".into(),
            ));
        }
        for certificate in certificates {
            roots.add(certificate).map_err(|error| {
                crate::ShardCacheError::Config(format!(
                    "invalid SCNP TLS client CA certificate: {error}"
                ))
            })?;
        }
        let verifier = WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .map_err(|error| {
            crate::ShardCacheError::Config(format!("invalid SCNP TLS client verifier: {error}"))
        })?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
    } else {
        builder.with_no_client_auth().with_single_cert(certs, key)
    }
    .map_err(|error| {
        crate::ShardCacheError::Config(format!("invalid SCNP TLS server identity: {error}"))
    })?;
    config.alpn_protocols = vec![b"scnp/2".to_vec()];
    Ok(Arc::new(config))
}

#[cfg(feature = "scnp-tls")]
fn parse_client_certificate_fingerprints(values: &[String]) -> Result<Arc<[[u8; 32]]>> {
    let mut fingerprints = Vec::with_capacity(values.len());
    for value in values {
        let compact = value.replace(':', "");
        if compact.len() != 64 || !compact.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(crate::ShardCacheError::Config(
                "SCNP client certificate SHA-256 fingerprints must contain 64 hexadecimal digits"
                    .into(),
            ));
        }
        let mut fingerprint = [0u8; 32];
        for (index, byte) in fingerprint.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&compact[index * 2..index * 2 + 2], 16).map_err(|_| {
                crate::ShardCacheError::Config(
                    "invalid SCNP client certificate SHA-256 fingerprint".into(),
                )
            })?;
        }
        fingerprints.push(fingerprint);
    }
    Ok(fingerprints.into())
}

struct MultiDirectRouteMode;

impl MultiDirectRouteMode {
    fn configured() -> Result<EmbeddedRouteMode> {
        match std::env::var("SHARDCACHE_ROUTE_MODE") {
            Ok(value)
                if value.eq_ignore_ascii_case("session_prefix")
                    || value.eq_ignore_ascii_case("session-prefix")
                    || value.eq_ignore_ascii_case("session") =>
            {
                Ok(EmbeddedRouteMode::SessionPrefix)
            }
            Ok(value)
                if value.eq_ignore_ascii_case("full_key")
                    || value.eq_ignore_ascii_case("full-key")
                    || value.eq_ignore_ascii_case("point") =>
            {
                Ok(EmbeddedRouteMode::FullKey)
            }
            Ok(value)
                if value.eq_ignore_ascii_case("overflow_slot")
                    || value.eq_ignore_ascii_case("overflow-slot")
                    || value.eq_ignore_ascii_case("overflow") =>
            {
                Ok(EmbeddedRouteMode::OverflowSlot)
            }
            Ok(value) => Err(crate::ShardCacheError::Config(format!(
                "unknown SHARDCACHE_ROUTE_MODE={value}; use full_key, session_prefix, or overflow_slot"
            ))),
            Err(_) => Ok(EmbeddedRouteMode::FullKey),
        }
    }
}

struct UnixSocketPath;

impl UnixSocketPath {
    fn prepare(path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
    }
}

/// Process-level helpers for launching the server runtime.
pub struct ServerRuntime;

impl ServerRuntime {
    pub async fn launch(config: ShardCacheConfig) -> Result<()> {
        let engine = EngineHandle::open(config.clone())?;
        ShardCacheServer::new(config, engine).run().await
    }

    pub fn initialize_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .try_init();
    }
}
