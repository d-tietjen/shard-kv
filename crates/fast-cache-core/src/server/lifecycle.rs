use super::connection::{ConnectionRejector, EngineConnection, HandoffConfig, SnapshotTask};
use super::direct::{DirectConnection, DirectServer};
#[cfg(feature = "embedded")]
use super::transactions::TransactionCoordinator;
#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
use super::transport::{MonoioMultiDirectWorker, MonoioWorkerConfig};
#[cfg(feature = "embedded")]
use super::transport::{
    MultiDirectAddress, MultiDirectWorker, TokioHybridWorkerConfig, TokioWorkerConfig,
};
use super::*;

impl FastCacheServer {
    pub fn new(config: FastCacheConfig, engine: EngineHandle) -> Self {
        Self {
            config,
            engine: Some(engine),
            mode: ServerMode::Auto,
            unix_socket_path: None,
        }
    }

    pub fn with_mode(config: FastCacheConfig, engine: EngineHandle, mode: ServerMode) -> Self {
        Self {
            config,
            engine: Some(engine),
            mode,
            unix_socket_path: None,
        }
    }

    pub fn direct(config: FastCacheConfig) -> Self {
        Self {
            config,
            engine: None,
            mode: ServerMode::Direct,
            unix_socket_path: None,
        }
    }

    pub fn with_unix_socket(mut self, path: PathBuf) -> Self {
        self.unix_socket_path = Some(path);
        self
    }

    pub async fn run(self) -> Result<()> {
        if self.should_run_multi_direct() {
            return self.run_multi_direct().await;
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
        if self.should_run_direct() {
            return Err(crate::FastCacheError::Config(
                "run_with_shutdown is only available for engine-backed mode; use run() for direct mode"
                    .into(),
            ));
        }

        self.run_engine_with_shutdown(shutdown).await
    }

    async fn run_engine_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        if let Some(path) = self.unix_socket_path.clone() {
            UnixSocketPath::prepare(&path)?;
            let listener = UnixListener::bind(&path)?;
            tracing::info!("fast-cache listening on unix://{}", path.display());

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
        tracing::info!("fast-cache listening on {}", self.config.bind_addr);

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

impl ServerModeRouting for FastCacheServer {
    fn should_run_direct(&self) -> bool {
        // The legacy single-thread DIRECT_STATE path is now superseded by
        // multi-direct with worker_count=1 — same hot-path optimizations
        // (RwLock reads, fused encode, mpsc write decoupling). Keep this
        // returning false so all Direct-mode requests go through multi-direct.
        false
    }

    fn should_run_multi_direct(&self) -> bool {
        // Direct mode now always uses multi-direct (with at least 1 worker).
        matches!(self.mode, ServerMode::Direct)
            || (matches!(self.mode, ServerMode::Auto)
                && !self.config.persistence.enabled
                && self.config.shard_count >= 1)
    }

    fn engine(&self) -> &EngineHandle {
        self.engine
            .as_ref()
            .expect("engine-backed server requires an engine handle")
    }
}

impl FastCacheServer {
    async fn run_direct_with_shutdown<F>(self, shutdown: F) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        DirectServer::initialize(&self.config);

        let result = if let Some(path) = self.unix_socket_path.clone() {
            UnixSocketPath::prepare(&path)?;
            let listener = UnixListener::bind(&path)?;
            tracing::info!(
                "fast-cache listening on unix://{} (direct local mode)",
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
                "fast-cache listening on {} (direct local mode)",
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

    async fn run_multi_direct(self) -> Result<()> {
        if self.unix_socket_path.is_some() {
            return Err(crate::FastCacheError::Config(
                "multi-direct mode does not support unix sockets yet; use --shard-count 1".into(),
            ));
        }

        let bind_addr: SocketAddr = self.config.bind_addr.parse().map_err(|error| {
            crate::FastCacheError::Config(format!(
                "invalid bind addr {}: {error}",
                self.config.bind_addr
            ))
        })?;

        let shard_count = self.config.shard_count;
        let max_connections = self.config.max_connections;

        let route_mode = MultiDirectRouteMode::configured()?;
        let store = EmbeddedStore::with_route_mode(shard_count, route_mode);
        store.configure_memory_policy(
            self.config.per_shard_memory_limit_bytes(),
            self.config.eviction_policy,
        );
        let shared_store = Arc::new(store);
        let limiter = Arc::new(Semaphore::new(max_connections));
        let transaction_coordinator =
            TransactionCoordinator::new(shard_count, self.config.transaction_mode).map(Arc::new);

        #[cfg(all(target_os = "linux", feature = "monoio"))]
        let use_monoio = std::env::var("FAST_CACHE_USE_MONOIO").is_ok_and(|v| v != "0");
        #[cfg(any(not(target_os = "linux"), not(feature = "monoio")))]
        let use_monoio = false;
        let requested_direct_shard_ports =
            std::env::var("FAST_CACHE_DIRECT_SHARD_PORTS").is_ok_and(|v| v != "0");
        let direct_shard_ports = requested_direct_shard_ports;
        if use_monoio {
            tracing::info!("multi-direct: using monoio workers");
        }
        let available_workers = std::thread::available_parallelism()
            .map(|available| available.get())
            .unwrap_or(shard_count)
            .clamp(1, shard_count);
        let worker_count = if direct_shard_ports {
            shard_count
        } else {
            std::env::var("FAST_CACHE_WORKER_COUNT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
                .map(|value| value.min(shard_count))
                .unwrap_or(available_workers)
        };
        let direct_base_port = direct_shard_ports
            .then(|| MultiDirectAddress::direct_base_port(bind_addr, shard_count))
            .transpose()?;
        if direct_shard_ports {
            let direct_base_port =
                direct_base_port.expect("direct shard base port exists when enabled");
            tracing::info!(
                "multi-direct: exposing shard-owned native ports {}-{}",
                direct_base_port,
                direct_base_port.saturating_add(shard_count.saturating_sub(1) as u16)
            );
        }

        let mut worker_txs: Vec<flume::Sender<std::net::TcpStream>> =
            Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);

        // Resolve available CPU cores so each worker can be pinned to one.
        // If we have fewer cores than workers, fall back to round-robin.
        let core_ids: Vec<core_affinity::CoreId> =
            core_affinity::get_core_ids().unwrap_or_default();
        if core_ids.is_empty() {
            tracing::warn!("multi-direct: no core ids available, workers will not be pinned");
        } else {
            tracing::info!(
                "multi-direct: pinning {} workers across {} available cores",
                worker_count,
                core_ids.len()
            );
        }

        let single_threaded = worker_count == 1 && cfg!(feature = "unsafe");
        let started_at = Instant::now();
        for worker_id in 0..worker_count {
            // The channel is only used by the legacy fanout acceptor. Shard-port
            // workers bind and accept inside their own pinned worker thread.
            let (tx, rx) = flume::bounded::<std::net::TcpStream>(256);
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
                    crate::FastCacheError::Config(format!(
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
                "fast-cache main: workers handle accept directly on {}{} ({} workers)",
                bind_addr,
                if direct_shard_ports {
                    " and shard-owned direct ports"
                } else {
                    ""
                },
                worker_count
            );
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown requested");
            drop(worker_txs);
            for handle in handles {
                let _ = handle.join();
            }
            return Ok(());
        }

        let listener = TcpListener::bind(&bind_addr).await?;
        tracing::info!(
            "fast-cache listening on {} (multi-direct, {} workers)",
            bind_addr,
            worker_count
        );

        let shutdown = async {
            let _ = tokio::signal::ctrl_c().await;
        };
        tokio::pin!(shutdown);

        let mut next_worker = 0usize;
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("shutdown requested");
                    break;
                }
                accept = listener.accept() => {
                    let (stream, _addr) = accept?;
                    let _ = stream.set_nodelay(true);
                    // Hand off to a worker as a std::net::TcpStream so it can
                    // be re-registered on the worker's reactor.
                    let std_stream = match stream.into_std() {
                        Ok(s) => s,
                        Err(error) => {
                            tracing::warn!("into_std failed: {error}");
                            continue;
                        }
                    };
                    let target = next_worker % worker_txs.len();
                    next_worker = next_worker.wrapping_add(1);
                    if worker_txs[target].send_async(std_stream).await.is_err() {
                        tracing::warn!("worker {target} channel closed");
                        break;
                    }
                }
            }
        }

        // Drop senders to signal workers to exit, then join.
        drop(worker_txs);
        for handle in handles {
            let _ = handle.join();
        }
        Ok(())
    }
}

struct MultiDirectRouteMode;

impl MultiDirectRouteMode {
    fn configured() -> Result<EmbeddedRouteMode> {
        match std::env::var("FAST_CACHE_ROUTE_MODE") {
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
            Ok(value) => Err(crate::FastCacheError::Config(format!(
                "unknown FAST_CACHE_ROUTE_MODE={value}; use full_key or session_prefix"
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
    pub async fn launch(config: FastCacheConfig) -> Result<()> {
        let engine = EngineHandle::open(config.clone())?;
        FastCacheServer::new(config, engine).run().await
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
