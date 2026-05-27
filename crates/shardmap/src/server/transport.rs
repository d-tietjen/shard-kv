use super::connection::{ConnectionRejector, HandoffConfig};
#[cfg(feature = "embedded")]
use super::direct_protocol::{DirectProtocol, SharedRequestBufferContext};
#[cfg(feature = "embedded")]
use super::fast_write::FastWriteQueue;
#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
use super::fast_write::{FastWriteBatchIoVec, FastWriteItem};
#[cfg(feature = "embedded")]
use super::transactions::{TransactionCoordinator, TransactionState};
#[cfg(feature = "embedded")]
use super::wire::RespProtocolVersion;
use super::*;

pub(super) struct MultiDirectAddress;

impl MultiDirectAddress {
    pub(super) fn direct_base_port(base: SocketAddr, shard_count: usize) -> Result<u16> {
        let configured = std::env::var("SHARDCACHE_DIRECT_SHARD_BASE_PORT")
            .ok()
            .map(|value| {
                value.parse::<u16>().map_err(|error| {
                    crate::ShardCacheError::Config(format!(
                        "invalid SHARDCACHE_DIRECT_SHARD_BASE_PORT={value}: {error}"
                    ))
                })
            })
            .transpose()?;
        let direct_base = match configured {
            Some(port) => port,
            None => base.port().checked_add(1).ok_or_else(|| {
                crate::ShardCacheError::Config("direct shard port range overflows u16".into())
            })?,
        };
        let last_offset = u16::try_from(shard_count.saturating_sub(1)).map_err(|_| {
            crate::ShardCacheError::Config("direct shard port range overflows u16".into())
        })?;
        let direct_last = direct_base.checked_add(last_offset).ok_or_else(|| {
            crate::ShardCacheError::Config("direct shard port range overflows u16".into())
        })?;
        if (direct_base..=direct_last).contains(&base.port()) {
            return Err(crate::ShardCacheError::Config(format!(
                "direct shard port range {direct_base}-{direct_last} overlaps fanout port {}",
                base.port()
            )));
        }
        Ok(direct_base)
    }

    pub(super) fn direct_worker_bind_addr(
        fanout: SocketAddr,
        direct_base_port: u16,
        worker_id: usize,
    ) -> Result<SocketAddr> {
        let port_offset = u16::try_from(worker_id)
            .map_err(|_| crate::ShardCacheError::Config("shard port range overflows u16".into()))?;
        let port = direct_base_port.checked_add(port_offset).ok_or_else(|| {
            crate::ShardCacheError::Config("shard port range overflows u16".into())
        })?;
        let mut addr = fanout;
        addr.set_port(port);
        Ok(addr)
    }
}

pub(super) struct MultiDirectWorker;

#[cfg(feature = "embedded")]
pub(super) struct TokioHybridWorkerConfig {
    pub(super) worker_id: usize,
    pub(super) direct_bind_addr: SocketAddr,
    pub(super) core_id: Option<core_affinity::CoreId>,
    pub(super) single_threaded: bool,
    pub(super) owned_shard_id: usize,
    pub(super) started_at: Instant,
    pub(super) transaction_coordinator: Option<Arc<TransactionCoordinator>>,
}

#[cfg(feature = "embedded")]
pub(super) struct TokioWorkerConfig {
    pub(super) worker_id: usize,
    pub(super) core_id: Option<core_affinity::CoreId>,
    pub(super) single_threaded: bool,
    pub(super) started_at: Instant,
    pub(super) transaction_coordinator: Option<Arc<TransactionCoordinator>>,
}

#[cfg(feature = "embedded")]
impl MultiDirectWorker {
    pub(super) fn run(
        config: TokioWorkerConfig,
        store: Arc<EmbeddedStore>,
        limiter: Arc<Semaphore>,
        rx: flume::Receiver<std::net::TcpStream>,
    ) {
        let TokioWorkerConfig {
            worker_id,
            core_id,
            single_threaded,
            started_at,
            transaction_coordinator,
        } = config;

        if let Some(core) = core_id
            && !core_affinity::set_for_current(core)
        {
            tracing::warn!("worker {worker_id} failed to pin to core {:?}", core);
        }

        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(error) => {
                tracing::error!("worker {worker_id} runtime build failed: {error}");
                return;
            }
        };

        let local = LocalSet::new();
        runtime.block_on(local.run_until(async move {
            while let Ok(std_stream) = rx.recv_async().await {
                if std_stream.set_nonblocking(true).is_err() {
                    continue;
                }
                let stream = match TcpStream::from_std(std_stream) {
                    Ok(s) => s,
                    Err(error) => {
                        tracing::warn!("worker {worker_id} from_std failed: {error}");
                        continue;
                    }
                };
                let permit = match limiter.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        let _ = ConnectionRejector::reject(stream).await;
                        continue;
                    }
                };
                let store = store.clone();
                let transaction_coordinator = transaction_coordinator.clone();
                spawn_local(async move {
                    if let Err(error) = MultiDirectConnection::handle(
                        stream,
                        store,
                        permit,
                        single_threaded,
                        None,
                        started_at,
                        transaction_coordinator,
                    )
                    .await
                    {
                        tracing::warn!("multi-direct connection closed with error: {error}");
                    }
                });
            }
        }));
    }

    pub(super) fn run_hybrid(
        config: TokioHybridWorkerConfig,
        store: Arc<EmbeddedStore>,
        limiter: Arc<Semaphore>,
        rx: flume::Receiver<std::net::TcpStream>,
    ) {
        let TokioHybridWorkerConfig {
            worker_id,
            direct_bind_addr,
            core_id,
            single_threaded,
            owned_shard_id,
            started_at,
            transaction_coordinator,
        } = config;

        if let Some(core) = core_id
            && !core_affinity::set_for_current(core)
        {
            tracing::warn!("worker {worker_id} failed to pin to core {:?}", core);
        }

        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(error) => {
                tracing::error!("worker {worker_id} runtime build failed: {error}");
                return;
            }
        };

        let local = LocalSet::new();
        runtime.block_on(local.run_until(async move {
            let direct_listener = match TcpListener::bind(direct_bind_addr).await {
                Ok(listener) => listener,
                Err(error) => {
                    tracing::error!("worker {worker_id} bind {direct_bind_addr} failed: {error}");
                    return;
                }
            };
            tracing::info!(
                "worker {worker_id} accepting shard-owned SCNP on {}",
                direct_bind_addr
            );

            let direct_store = store.clone();
            let direct_limiter = limiter.clone();
            let direct_transaction_coordinator = transaction_coordinator.clone();
            spawn_local(async move {
                loop {
                    let (stream, _addr) = match direct_listener.accept().await {
                        Ok(pair) => pair,
                        Err(error) => {
                            tracing::warn!("worker {worker_id} direct accept error: {error}");
                            continue;
                        }
                    };
                    let _ = stream.set_nodelay(true);
                    let permit = match direct_limiter.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            let _ = ConnectionRejector::reject(stream).await;
                            continue;
                        }
                    };
                    let store = direct_store.clone();
                    let transaction_coordinator = direct_transaction_coordinator.clone();
                    spawn_local(async move {
                        if let Err(error) = MultiDirectConnection::handle(
                            stream,
                            store,
                            permit,
                            single_threaded,
                            Some(owned_shard_id),
                            started_at,
                            transaction_coordinator,
                        )
                        .await
                        {
                            tracing::warn!("multi-direct connection closed with error: {error}");
                        }
                    });
                }
            });

            while let Ok(std_stream) = rx.recv_async().await {
                if std_stream.set_nonblocking(true).is_err() {
                    continue;
                }
                let stream = match TcpStream::from_std(std_stream) {
                    Ok(s) => s,
                    Err(error) => {
                        tracing::warn!("worker {worker_id} from_std failed: {error}");
                        continue;
                    }
                };
                let permit = match limiter.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        let _ = ConnectionRejector::reject(stream).await;
                        continue;
                    }
                };
                let store = store.clone();
                let transaction_coordinator = transaction_coordinator.clone();
                spawn_local(async move {
                    if let Err(error) = MultiDirectConnection::handle(
                        stream,
                        store,
                        permit,
                        single_threaded,
                        None,
                        started_at,
                        transaction_coordinator,
                    )
                    .await
                    {
                        tracing::warn!("multi-direct connection closed with error: {error}");
                    }
                });
            }
        }));
    }
}

#[cfg(not(feature = "embedded"))]
impl MultiDirectWorker {
    pub(super) fn run(
        _config: (),
        _store: Arc<()>,
        _limiter: Arc<Semaphore>,
        _rx: flume::Receiver<std::net::TcpStream>,
    ) {
        panic!("multi-direct requires the `embedded` feature");
    }

    pub(super) fn run_hybrid(
        _config: (),
        _store: Arc<()>,
        _limiter: Arc<Semaphore>,
        _rx: flume::Receiver<std::net::TcpStream>,
    ) {
        panic!("multi-direct requires the `embedded` feature");
    }
}

pub(super) struct ReusePortListener;

#[allow(dead_code)]
impl ReusePortListener {
    pub(super) fn build(addr: SocketAddr) -> Result<TcpListener> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP)).map_err(|error| {
            crate::ShardCacheError::Config(format!("socket create failed: {error}"))
        })?;
        socket.set_reuse_address(true).map_err(|error| {
            crate::ShardCacheError::Config(format!("SO_REUSEADDR failed: {error}"))
        })?;
        #[cfg(unix)]
        socket.set_reuse_port(true).map_err(|error| {
            crate::ShardCacheError::Config(format!("SO_REUSEPORT failed: {error}"))
        })?;
        socket.set_nonblocking(true).map_err(|error| {
            crate::ShardCacheError::Config(format!("set_nonblocking failed: {error}"))
        })?;
        socket
            .bind(&addr.into())
            .map_err(|error| crate::ShardCacheError::Config(format!("bind failed: {error}")))?;
        socket
            .listen(1024)
            .map_err(|error| crate::ShardCacheError::Config(format!("listen failed: {error}")))?;
        let std_listener: StdTcpListener = socket.into();
        TcpListener::from_std(std_listener).map_err(|error| {
            crate::ShardCacheError::Config(format!("TcpListener::from_std failed: {error}"))
        })
    }
}

pub(super) struct MultiDirectConnection;

#[cfg(feature = "embedded")]
impl MultiDirectConnection {
    pub(super) async fn handle<S>(
        stream: S,
        store: Arc<EmbeddedStore>,
        _permit: OwnedSemaphorePermit,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        started_at: Instant,
        transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        match TokioResponseWriterMode::configured() {
            TokioResponseWriterMode::Inline => {
                Self::handle_inline(
                    stream,
                    store,
                    _permit,
                    single_threaded,
                    owned_shard_id,
                    started_at,
                    transaction_coordinator,
                )
                .await
            }
            TokioResponseWriterMode::Split => {
                Self::handle_split(
                    stream,
                    store,
                    _permit,
                    single_threaded,
                    owned_shard_id,
                    started_at,
                    transaction_coordinator,
                )
                .await
            }
        }
    }

    async fn handle_split<S>(
        stream: S,
        store: Arc<EmbeddedStore>,
        _permit: OwnedSemaphorePermit,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        started_at: Instant,
        transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let (mut read_half, mut write_half) = tokio::io::split(stream);
        let (write_tx, mut write_rx) =
            tokio::sync::mpsc::channel::<bytes::Bytes>(WRITE_HANDOFF_MAX_ITEMS);

        let writer = spawn_local(async move {
            while let Some(bytes) = write_rx.recv().await {
                if write_half.write_all(&bytes).await.is_err() {
                    break;
                }
            }
        });

        let mut frame_buffer = HandoffBuffer::with_config(HandoffConfig::buffer());
        // Reuse a single BytesMut across iterations: split().freeze() hands off
        // the encoded bytes to the writer while leaving the underlying buffer's
        // remaining capacity for the next batch (zero-realloc steady state).
        let mut write_buffer = BytesMut::with_capacity(CONNECTION_BUFFER_CAPACITY);
        let mut transaction_state = TransactionState::default();
        let mut resp_protocol = RespProtocolVersion::default();

        let read_loop = async {
            loop {
                let read = frame_buffer
                    .read_available(&mut read_half)
                    .await
                    .map_err(|error| {
                        crate::ShardCacheError::Protocol(format!("handoff read error: {error}"))
                    })?;
                if read == 0 {
                    return Ok::<(), crate::ShardCacheError>(());
                }

                let consumed_total = DirectProtocol::process_shared_request_buffer_with_context(
                    frame_buffer.peek(),
                    &store,
                    &mut write_buffer,
                    None,
                    SharedRequestBufferContext {
                        single_threaded,
                        owned_shard_id,
                        started_at,
                        transaction_coordinator: transaction_coordinator.as_deref(),
                        transaction_state: &mut transaction_state,
                        resp_protocol: &mut resp_protocol,
                    },
                )?;

                if !write_buffer.is_empty() {
                    let bytes = write_buffer.split().freeze();
                    if write_tx.send(bytes).await.is_err() {
                        return Ok(());
                    }
                    if write_buffer.capacity() < READ_RESERVE_THRESHOLD {
                        write_buffer.reserve(CONNECTION_BUFFER_CAPACITY);
                    }
                }
                if consumed_total > 0 {
                    frame_buffer.advance(consumed_total).map_err(|error| {
                        crate::ShardCacheError::Protocol(format!("handoff advance error: {error}"))
                    })?;
                }
            }
        };

        let result = read_loop.await;
        transaction_state.close(transaction_coordinator.as_deref());
        drop(write_tx);
        let _ = writer.await;
        result
    }

    async fn handle_inline<S>(
        mut stream: S,
        store: Arc<EmbeddedStore>,
        _permit: OwnedSemaphorePermit,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        started_at: Instant,
        transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let mut frame_buffer = HandoffBuffer::with_config(HandoffConfig::buffer());
        let mut write_buffer = BytesMut::with_capacity(CONNECTION_BUFFER_CAPACITY);
        let mut fast_write_queue = FastWriteQueue::default();
        let use_fast_write_queue = store.shard_count() > 1;
        let mut transaction_state = TransactionState::default();
        let mut resp_protocol = RespProtocolVersion::default();

        loop {
            let read = frame_buffer
                .read_available(&mut stream)
                .await
                .map_err(|error| {
                    crate::ShardCacheError::Protocol(format!("handoff read error: {error}"))
                })?;
            if read == 0 {
                break;
            }

            let consumed_total = DirectProtocol::process_shared_request_buffer_with_context(
                frame_buffer.peek(),
                &store,
                &mut write_buffer,
                use_fast_write_queue.then_some(&mut fast_write_queue),
                SharedRequestBufferContext {
                    single_threaded,
                    owned_shard_id,
                    started_at,
                    transaction_coordinator: transaction_coordinator.as_deref(),
                    transaction_state: &mut transaction_state,
                    resp_protocol: &mut resp_protocol,
                },
            )?;

            if use_fast_write_queue && !fast_write_queue.is_empty() {
                fast_write_queue.flush_bytes(&mut write_buffer);
                fast_write_queue.write_pending_tokio(&mut stream).await?;
                if write_buffer.capacity() < READ_RESERVE_THRESHOLD {
                    write_buffer.reserve(CONNECTION_BUFFER_CAPACITY);
                }
            } else if !write_buffer.is_empty() {
                stream.write_all(&write_buffer).await?;
                write_buffer.clear();
                if write_buffer.capacity() < READ_RESERVE_THRESHOLD {
                    write_buffer.reserve(CONNECTION_BUFFER_CAPACITY);
                }
            }
            if consumed_total > 0 {
                frame_buffer.advance(consumed_total).map_err(|error| {
                    crate::ShardCacheError::Protocol(format!("handoff advance error: {error}"))
                })?;
            }
        }

        transaction_state.close(transaction_coordinator.as_deref());
        Ok(())
    }
}

#[cfg(feature = "embedded")]
#[derive(Clone, Copy)]
enum TokioResponseWriterMode {
    Split,
    Inline,
}

#[cfg(feature = "embedded")]
impl TokioResponseWriterMode {
    fn configured() -> Self {
        match std::env::var("SHARDCACHE_TOKIO_WRITER_MODE") {
            Ok(value) if value.eq_ignore_ascii_case("split") => Self::Split,
            Ok(value)
                if value.eq_ignore_ascii_case("inline") || value.eq_ignore_ascii_case("direct") =>
            {
                Self::Inline
            }
            _ => Self::Inline,
        }
    }
}

#[cfg(not(feature = "embedded"))]
impl MultiDirectConnection {
    pub(super) async fn handle<S>(
        _stream: S,
        _store: Arc<()>,
        _permit: OwnedSemaphorePermit,
        _single_threaded: bool,
        _owned_shard_id: Option<usize>,
        _started_at: Instant,
        _transaction_coordinator: Option<Arc<()>>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        Err(crate::ShardCacheError::Config(
            "multi-direct requires the `embedded` feature".into(),
        ))
    }
}

// =============================================================================
// monoio worker — Linux only, opt-in via SHARDCACHE_USE_MONOIO=1
// =============================================================================

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
pub(super) struct MonoioMultiDirectWorker;

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
pub(super) struct MonoioWorkerConfig {
    pub(super) worker_id: usize,
    pub(super) worker_count: usize,
    pub(super) fanout_bind_addr: SocketAddr,
    pub(super) direct_bind_addr: Option<SocketAddr>,
    pub(super) core_id: Option<core_affinity::CoreId>,
    pub(super) single_threaded: bool,
    pub(super) started_at: Instant,
    pub(super) transaction_coordinator: Option<Arc<TransactionCoordinator>>,
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl MonoioMultiDirectWorker {
    pub(super) fn run(
        config: MonoioWorkerConfig,
        store: Arc<EmbeddedStore>,
        limiter: Arc<Semaphore>,
    ) {
        let MonoioWorkerConfig {
            worker_id,
            worker_count,
            fanout_bind_addr,
            direct_bind_addr,
            core_id,
            single_threaded,
            started_at,
            transaction_coordinator,
        } = config;

        if let Some(core) = core_id
            && !core_affinity::set_for_current(core)
        {
            tracing::warn!("monoio worker {worker_id} failed to pin to core {:?}", core);
        }

        let accept_config = MonoioAcceptLoopConfig {
            worker_id,
            fanout_bind_addr,
            direct_bind_addr,
            single_threaded,
            started_at,
            transaction_coordinator,
        };

        let driver_mode = MonoioDriverMode::configured(worker_count);
        tracing::info!(
            "monoio worker {worker_id}: using {driver_mode:?} driver ({worker_count} workers)"
        );
        match driver_mode {
            MonoioDriverMode::IoUring => {
                let mut runtime = match monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .with_entries(MonoioDriverMode::runtime_entries())
                    .build()
                {
                    Ok(rt) => rt,
                    Err(error) => {
                        tracing::error!(
                            "monoio worker {worker_id} io_uring runtime build failed: {error}"
                        );
                        return;
                    }
                };
                Self::block_on_accept_loop(&mut runtime, accept_config, store, limiter);
            }
            MonoioDriverMode::Legacy => {
                let mut runtime = match monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
                    .with_entries(MonoioDriverMode::runtime_entries())
                    .build()
                {
                    Ok(rt) => rt,
                    Err(error) => {
                        tracing::error!(
                            "monoio worker {worker_id} legacy runtime build failed: {error}"
                        );
                        return;
                    }
                };
                Self::block_on_accept_loop(&mut runtime, accept_config, store, limiter);
            }
        }
    }

    fn block_on_accept_loop<D>(
        runtime: &mut monoio::Runtime<D>,
        config: MonoioAcceptLoopConfig,
        store: Arc<EmbeddedStore>,
        limiter: Arc<Semaphore>,
    ) where
        D: monoio::Driver,
    {
        let MonoioAcceptLoopConfig {
            worker_id,
            fanout_bind_addr,
            direct_bind_addr,
            single_threaded,
            started_at,
            transaction_coordinator,
        } = config;

        runtime.block_on(async move {
            let fanout_listener = match MonoioListener::open(worker_id, fanout_bind_addr, "fanout")
            {
                Some(listener) => listener,
                None => return,
            };

            if let Some(bind_addr) = direct_bind_addr {
                let direct_listener =
                    match MonoioListener::open(worker_id, bind_addr, "direct shard") {
                        Some(listener) => listener,
                        None => return,
                    };
                let direct_config = MonoioListenerConfig {
                    worker_id,
                    bind_addr,
                    single_threaded,
                    owned_shard_id: Some(worker_id),
                    started_at,
                    transaction_coordinator: transaction_coordinator.clone(),
                    label: "shard-owned SCNP",
                };
                monoio::spawn(MonoioListener::accept_loop(
                    direct_listener,
                    direct_config,
                    store.clone(),
                    limiter.clone(),
                ));
            }

            let fanout_config = MonoioListenerConfig {
                worker_id,
                bind_addr: fanout_bind_addr,
                single_threaded,
                owned_shard_id: None,
                started_at,
                transaction_coordinator,
                label: "shared SCNP/RESP",
            };
            MonoioListener::accept_loop(fanout_listener, fanout_config, store, limiter).await;
        });
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
#[derive(Clone, Debug)]
struct MonoioAcceptLoopConfig {
    worker_id: usize,
    fanout_bind_addr: SocketAddr,
    direct_bind_addr: Option<SocketAddr>,
    single_threaded: bool,
    started_at: Instant,
    transaction_coordinator: Option<Arc<TransactionCoordinator>>,
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
#[derive(Clone, Debug)]
struct MonoioListenerConfig {
    worker_id: usize,
    bind_addr: SocketAddr,
    single_threaded: bool,
    owned_shard_id: Option<usize>,
    started_at: Instant,
    transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    label: &'static str,
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MonoioDriverMode {
    IoUring,
    Legacy,
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl MonoioDriverMode {
    fn configured(worker_count: usize) -> Self {
        match std::env::var("SHARDCACHE_MONOIO_DRIVER") {
            Ok(value) if value.eq_ignore_ascii_case("auto") => Self::default_for(worker_count),
            Ok(value) if value.eq_ignore_ascii_case("legacy") => Self::Legacy,
            Ok(value) if value.eq_ignore_ascii_case("iouring") => Self::IoUring,
            Ok(value) if value.eq_ignore_ascii_case("io_uring") => Self::IoUring,
            Ok(value) => {
                tracing::warn!("unknown SHARDCACHE_MONOIO_DRIVER={value}; using auto");
                Self::default_for(worker_count)
            }
            Err(_) => Self::default_for(worker_count),
        }
    }

    #[inline(always)]
    fn default_for(worker_count: usize) -> Self {
        if worker_count > 1 {
            Self::Legacy
        } else {
            Self::IoUring
        }
    }

    fn runtime_entries() -> u32 {
        std::env::var("SHARDCACHE_MONOIO_ENTRIES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(8192)
    }
}

#[cfg(all(
    target_os = "linux",
    feature = "embedded",
    feature = "monoio",
    not(feature = "unsafe")
))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MonoioSafeWriterMode {
    Inline,
    Split,
    Writev,
}

#[cfg(all(
    target_os = "linux",
    feature = "embedded",
    feature = "monoio",
    not(feature = "unsafe")
))]
impl MonoioSafeWriterMode {
    fn configured() -> Self {
        match std::env::var("SHARDCACHE_MONOIO_SAFE_WRITER") {
            Ok(value) if value.eq_ignore_ascii_case("inline") => Self::Inline,
            Ok(value) if value.eq_ignore_ascii_case("split") => Self::Split,
            Ok(value)
                if value.eq_ignore_ascii_case("writev")
                    || value.eq_ignore_ascii_case("vectored")
                    || value.eq_ignore_ascii_case("queued") =>
            {
                Self::Writev
            }
            Ok(value) => {
                tracing::warn!(
                    "unknown SHARDCACHE_MONOIO_SAFE_WRITER={value}; using inline writer"
                );
                Self::Inline
            }
            Err(_) => Self::configured_legacy(),
        }
    }

    fn configured_legacy() -> Self {
        match std::env::var("SHARDCACHE_MONOIO_WRITEV") {
            Ok(value) if value == "1" || value.eq_ignore_ascii_case("true") => Self::Writev,
            _ => match std::env::var("SHARDCACHE_MONOIO_SPLIT_WRITER") {
                Ok(value) if value == "1" || value.eq_ignore_ascii_case("true") => Self::Split,
                _ => Self::Inline,
            },
        }
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
struct MonoioListener;

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl MonoioListener {
    fn open(
        worker_id: usize,
        bind_addr: SocketAddr,
        label: &'static str,
    ) -> Option<monoio::net::TcpListener> {
        let std_listener = match Self::build(bind_addr) {
            Ok(listener) => listener,
            Err(error) => {
                tracing::error!(
                    "monoio worker {worker_id} {label} listener build failed on {bind_addr}: {error}"
                );
                return None;
            }
        };
        match monoio::net::TcpListener::from_std(std_listener) {
            Ok(listener) => Some(listener),
            Err(error) => {
                tracing::error!(
                    "monoio worker {worker_id} {label} from_std failed on {bind_addr}: {error}"
                );
                None
            }
        }
    }

    async fn accept_loop(
        listener: monoio::net::TcpListener,
        config: MonoioListenerConfig,
        store: Arc<EmbeddedStore>,
        limiter: Arc<Semaphore>,
    ) {
        let MonoioListenerConfig {
            worker_id,
            bind_addr,
            single_threaded,
            owned_shard_id,
            started_at,
            transaction_coordinator,
            label,
        } = config;

        tracing::info!("monoio worker {worker_id} accepting {label} on {bind_addr}");
        loop {
            let (stream, _addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(error) => {
                    tracing::warn!("monoio worker {worker_id} {label} accept error: {error}");
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            TcpBufferTuning::apply_monoio(&stream);
            let permit = match limiter.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => continue,
            };
            let store = store.clone();
            let transaction_coordinator = transaction_coordinator.clone();
            monoio::spawn(MonoioMultiDirectConnection::handle(
                stream,
                store,
                permit,
                single_threaded,
                owned_shard_id,
                started_at,
                transaction_coordinator,
            ));
        }
    }

    fn build(addr: SocketAddr) -> Result<StdTcpListener> {
        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP)).map_err(|error| {
            crate::ShardCacheError::Config(format!("monoio socket create failed: {error}"))
        })?;
        socket.set_reuse_address(true).map_err(|error| {
            crate::ShardCacheError::Config(format!("monoio SO_REUSEADDR failed: {error}"))
        })?;
        socket.set_reuse_port(true).map_err(|error| {
            crate::ShardCacheError::Config(format!("monoio SO_REUSEPORT failed: {error}"))
        })?;
        socket.set_nonblocking(true).map_err(|error| {
            crate::ShardCacheError::Config(format!("monoio set_nonblocking failed: {error}"))
        })?;
        if let Some(buffer_bytes) = TcpBufferTuning::configured_bytes() {
            let _ = socket.set_send_buffer_size(buffer_bytes);
            let _ = socket.set_recv_buffer_size(buffer_bytes);
        }
        socket.bind(&addr.into()).map_err(|error| {
            crate::ShardCacheError::Config(format!("monoio bind failed: {error}"))
        })?;
        socket.listen(1024).map_err(|error| {
            crate::ShardCacheError::Config(format!("monoio listen failed: {error}"))
        })?;
        Ok(socket.into())
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
struct TcpBufferTuning;

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl TcpBufferTuning {
    fn configured_bytes() -> Option<usize> {
        static VALUE: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
        *VALUE.get_or_init(|| {
            std::env::var("SHARDCACHE_TCP_BUFFER_BYTES")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value > 0)
        })
    }

    fn apply_monoio(stream: &monoio::net::TcpStream) {
        if let Some(value) =
            Self::configured_bytes().and_then(|bytes| libc::c_int::try_from(bytes).ok())
        {
            Self::apply_monoio_value(stream, value);
        }
    }

    fn apply_monoio_value(stream: &monoio::net::TcpStream, value: libc::c_int) {
        use std::os::fd::AsRawFd;
        let fd = stream.as_raw_fd();
        let value_ptr = (&value as *const libc::c_int).cast::<libc::c_void>();
        let value_len = std::mem::size_of_val(&value) as libc::socklen_t;

        // Best-effort tuning knob for bandwidth experiments. Linux may clamp the
        // requested size to net.core.{r,w}mem_max, which is still fine for A/B runs.
        unsafe {
            let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, value_ptr, value_len);
            let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, value_ptr, value_len);
        }
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
struct MonoioMultiDirectConnection;

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
enum MonoioDrainError {
    Buffer,
    Protocol,
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl From<bytes_handoff::BufferError> for MonoioDrainError {
    fn from(_: bytes_handoff::BufferError) -> Self {
        Self::Buffer
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl MonoioDrainError {
    fn protocol(_: crate::ShardCacheError) -> Self {
        Self::Protocol
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
struct MonoioRequestDrain;

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
struct MonoioRequestDrainContext<'a> {
    store: &'a EmbeddedStore,
    write_buffer: &'a mut BytesMut,
    fast_write_queue: Option<&'a mut FastWriteQueue>,
    single_threaded: bool,
    owned_shard_id: Option<usize>,
    started_at: Instant,
    transaction_coordinator: Option<&'a TransactionCoordinator>,
    transaction_state: &'a mut TransactionState,
    resp_protocol: &'a mut RespProtocolVersion,
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl MonoioRequestDrain {
    #[inline(always)]
    fn process(
        cursor: &mut bytes_handoff::HandoffDrainCursor<'_>,
        context: MonoioRequestDrainContext<'_>,
    ) -> std::result::Result<usize, MonoioDrainError> {
        let consumed = DirectProtocol::process_shared_request_buffer_with_context(
            cursor.remaining(),
            context.store,
            context.write_buffer,
            context.fast_write_queue,
            SharedRequestBufferContext {
                single_threaded: context.single_threaded,
                owned_shard_id: context.owned_shard_id,
                started_at: context.started_at,
                transaction_coordinator: context.transaction_coordinator,
                transaction_state: context.transaction_state,
                resp_protocol: context.resp_protocol,
            },
        )
        .map_err(MonoioDrainError::protocol)?;
        cursor.consume(consumed)?;
        Ok(consumed)
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl MonoioMultiDirectConnection {
    async fn handle(
        stream: monoio::net::TcpStream,
        store: Arc<EmbeddedStore>,
        permit: OwnedSemaphorePermit,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        started_at: Instant,
        transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    ) {
        #[cfg(feature = "unsafe")]
        {
            Self::handle_writev(
                stream,
                store,
                permit,
                single_threaded,
                owned_shard_id,
                started_at,
                transaction_coordinator,
            )
            .await;
        }

        #[cfg(not(feature = "unsafe"))]
        {
            match MonoioSafeWriterMode::configured() {
                MonoioSafeWriterMode::Inline => {
                    Self::handle_inline_writer(
                        stream,
                        store,
                        permit,
                        single_threaded,
                        owned_shard_id,
                        started_at,
                        transaction_coordinator,
                    )
                    .await;
                }
                MonoioSafeWriterMode::Split => {
                    Self::handle_split_writer(
                        stream,
                        store,
                        permit,
                        single_threaded,
                        owned_shard_id,
                        started_at,
                        transaction_coordinator,
                    )
                    .await;
                }
                MonoioSafeWriterMode::Writev => {
                    Self::handle_writev(
                        stream,
                        store,
                        permit,
                        single_threaded,
                        owned_shard_id,
                        started_at,
                        transaction_coordinator,
                    )
                    .await;
                }
            }
        }
    }

    #[cfg(not(feature = "unsafe"))]
    async fn handle_inline_writer(
        mut stream: monoio::net::TcpStream,
        store: Arc<EmbeddedStore>,
        _permit: OwnedSemaphorePermit,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        started_at: Instant,
        transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    ) {
        let mut frame_buffer = HandoffBuffer::with_config(HandoffConfig::buffer());
        let mut write_buffer = BytesMut::with_capacity(CONNECTION_BUFFER_CAPACITY);
        let mut transaction_state = TransactionState::default();
        let mut resp_protocol = RespProtocolVersion::default();

        loop {
            match frame_buffer
                .read_and_drain_monoio(&mut stream, |cursor| {
                    MonoioRequestDrain::process(
                        cursor,
                        MonoioRequestDrainContext {
                            store: &store,
                            write_buffer: &mut write_buffer,
                            fast_write_queue: None,
                            single_threaded,
                            owned_shard_id,
                            started_at,
                            transaction_coordinator: transaction_coordinator.as_deref(),
                            transaction_state: &mut transaction_state,
                            resp_protocol: &mut resp_protocol,
                        },
                    )
                })
                .await
            {
                Ok((0, _)) => break,
                Ok((_, _)) => {}
                Err(_) => break,
            }

            if !MonoioResponseWriter::write_bytes(&mut stream, &mut write_buffer).await {
                break;
            }
        }

        transaction_state.close(transaction_coordinator.as_deref());
    }

    #[cfg(not(feature = "unsafe"))]
    async fn handle_split_writer(
        stream: monoio::net::TcpStream,
        store: Arc<EmbeddedStore>,
        _permit: OwnedSemaphorePermit,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        started_at: Instant,
        transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    ) {
        use monoio::io::Splitable;

        let (mut read_half, mut write_half) = stream.into_split();
        let (write_tx, write_rx) = flume::bounded::<bytes::Bytes>(WRITE_HANDOFF_MAX_ITEMS);

        let writer = monoio::spawn(async move {
            while let Ok(bytes) = write_rx.recv_async().await {
                if !MonoioResponseWriter::write_owned_bytes(&mut write_half, bytes).await {
                    break;
                }
            }
        });

        let mut frame_buffer = HandoffBuffer::with_config(HandoffConfig::buffer());
        let mut write_buffer = BytesMut::with_capacity(CONNECTION_BUFFER_CAPACITY);
        let mut transaction_state = TransactionState::default();
        let mut resp_protocol = RespProtocolVersion::default();

        loop {
            match frame_buffer
                .read_and_drain_monoio(&mut read_half, |cursor| {
                    MonoioRequestDrain::process(
                        cursor,
                        MonoioRequestDrainContext {
                            store: &store,
                            write_buffer: &mut write_buffer,
                            fast_write_queue: None,
                            single_threaded,
                            owned_shard_id,
                            started_at,
                            transaction_coordinator: transaction_coordinator.as_deref(),
                            transaction_state: &mut transaction_state,
                            resp_protocol: &mut resp_protocol,
                        },
                    )
                })
                .await
            {
                Ok((0, _)) => break,
                Ok((_, _)) => {}
                Err(_) => break,
            }

            if !write_buffer.is_empty() {
                let bytes = write_buffer.split().freeze();
                if write_tx.send_async(bytes).await.is_err() {
                    break;
                }
                MonoioResponseWriter::reserve_response_capacity(&mut write_buffer);
            }
        }

        transaction_state.close(transaction_coordinator.as_deref());
        drop(write_tx);
        let _ = writer.await;
    }

    async fn handle_writev(
        mut stream: monoio::net::TcpStream,
        store: Arc<EmbeddedStore>,
        _permit: OwnedSemaphorePermit,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        started_at: Instant,
        transaction_coordinator: Option<Arc<TransactionCoordinator>>,
    ) {
        let mut frame_buffer = HandoffBuffer::with_config(HandoffConfig::buffer());
        let mut write_buffer = BytesMut::with_capacity(CONNECTION_BUFFER_CAPACITY);
        let mut fast_write_queue = FastWriteQueue::default();
        let mut transaction_state = TransactionState::default();
        let mut resp_protocol = RespProtocolVersion::default();

        loop {
            match frame_buffer
                .read_and_drain_monoio(&mut stream, |cursor| {
                    MonoioRequestDrain::process(
                        cursor,
                        MonoioRequestDrainContext {
                            store: &store,
                            write_buffer: &mut write_buffer,
                            fast_write_queue: Some(&mut fast_write_queue),
                            single_threaded,
                            owned_shard_id,
                            started_at,
                            transaction_coordinator: transaction_coordinator.as_deref(),
                            transaction_state: &mut transaction_state,
                            resp_protocol: &mut resp_protocol,
                        },
                    )
                })
                .await
            {
                Ok((0, _)) => break,
                Ok((_, _)) => {}
                Err(_) => break,
            }

            fast_write_queue.flush_bytes(&mut write_buffer);
            if !MonoioResponseWriter::write_pending(
                &mut stream,
                &mut write_buffer,
                &mut fast_write_queue,
            )
            .await
            {
                break;
            }
        }

        transaction_state.close(transaction_coordinator.as_deref());
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
struct MonoioResponseWriter;

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl MonoioResponseWriter {
    async fn write_pending(
        stream: &mut monoio::net::TcpStream,
        out: &mut BytesMut,
        queue: &mut FastWriteQueue,
    ) -> bool {
        if queue.is_empty() {
            return true;
        }

        const MAX_WRITEV_IOVECS: usize = 1024;
        while !queue.is_empty() {
            let mut batch = queue.drain_iovec_batch(MAX_WRITEV_IOVECS);
            if batch.len() == 1 {
                match batch.pop().expect("single-item write batch") {
                    FastWriteItem::Bytes(bytes) => {
                        if !Self::write_owned_bytes(stream, bytes).await {
                            return false;
                        }
                        continue;
                    }
                    item => batch.push(item),
                }
            }
            if FastWriteBatchIoVec::write_all(stream, FastWriteBatchIoVec::new(batch))
                .await
                .is_err()
            {
                return false;
            }
        }
        Self::reserve_response_capacity(out);
        true
    }

    async fn write_owned_bytes<W>(stream: &mut W, bytes: bytes::Bytes) -> bool
    where
        W: monoio::io::AsyncWriteRent,
    {
        use monoio::io::AsyncWriteRentExt;

        let (result, _) = stream.write_all(bytes).await;
        result.is_ok()
    }

    #[cfg(not(feature = "unsafe"))]
    async fn write_bytes<W>(stream: &mut W, out: &mut BytesMut) -> bool
    where
        W: monoio::io::AsyncWriteRent,
    {
        if out.is_empty() {
            return true;
        }

        let bytes = out.split().freeze();
        let written = Self::write_owned_bytes(stream, bytes).await;
        Self::reserve_response_capacity(out);
        written
    }

    fn reserve_response_capacity(out: &mut BytesMut) {
        if out.capacity() < READ_RESERVE_THRESHOLD {
            out.reserve(CONNECTION_BUFFER_CAPACITY);
        }
    }
}
