use std::cell::RefCell;

use super::commands::{DirectCommandContext, DirectFastCommandDispatcher};
use super::connection::HandoffConfig;
#[cfg(feature = "embedded")]
use super::direct_protocol::DirectProtocol;
#[cfg(feature = "embedded")]
use super::wire::{RespProtocolVersion, ServerWire};
use super::*;
#[cfg(feature = "embedded")]
use crate::storage::{ShardArcEmbeddedStore, hash_key};

thread_local! {
    static DIRECT_STATE: RefCell<Option<DirectServerState>> = const { RefCell::new(None) };
}

pub(super) struct DirectServer;

impl DirectServer {
    pub(super) fn initialize(config: &ShardCacheConfig) -> Result<()> {
        #[cfg(feature = "embedded")]
        {
            let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::FullKey);
            store.configure_memory_policy(
                config.per_shard_memory_limit_bytes(),
                config.eviction_policy,
            );
            store.configure_object_overflow(ObjectOverflowRuntime::from_config(
                &config.object_overflow,
            )?)?;
            #[cfg(feature = "redis")]
            store.configure_vector_memory_policy(
                config.total_memory_limit_bytes(),
                config.eviction_policy,
            );
            let store = store
                .into_local_stores(1)
                .into_iter()
                .next()
                .expect("direct mode must create one local embedded store");
            store.install_local().map_err(|error| {
                crate::ShardCacheError::Config(format!(
                    "failed to install direct local embedded store: {error}"
                ))
            })?;
        }

        DIRECT_STATE.with(|cell| {
            *cell.borrow_mut() = Some(DirectServerState::new(true));
        });
        Ok(())
    }

    pub(super) fn initialize_thread_local(_config: &ShardCacheConfig) -> Result<()> {
        #[cfg(feature = "embedded")]
        crate::storage::with_local_embedded_store(|_| ()).map_err(|error| {
            crate::ShardCacheError::Config(format!(
                "thread-local embedded server requires an installed local store: {error}"
            ))
        })?;

        DIRECT_STATE.with(|cell| {
            *cell.borrow_mut() = Some(DirectServerState::new(false));
        });
        Ok(())
    }

    pub(super) fn clear() {
        let owns_thread_local_store = DIRECT_STATE.with(|cell| {
            cell.borrow_mut()
                .take()
                .is_some_and(|state| state.owns_thread_local_store)
        });
        #[cfg(feature = "embedded")]
        if owns_thread_local_store {
            let _ = crate::storage::take_local_embedded_store();
        }
    }
}

#[derive(Debug)]
pub(super) struct DirectServerState {
    #[cfg(not(feature = "embedded"))]
    map: FlatMap,
    owns_thread_local_store: bool,
    pub(super) reads: u64,
    pub(super) writes: u64,
    expired: u64,
    maintenance_runs: u64,
}

impl DirectServerState {
    fn new(owns_thread_local_store: bool) -> Self {
        Self {
            #[cfg(not(feature = "embedded"))]
            map: FlatMap::new(),
            owns_thread_local_store,
            reads: 0,
            writes: 0,
            expired: 0,
            maintenance_runs: 0,
        }
    }

    pub(super) fn get(&mut self, key: &[u8], now_ms: u64) -> Option<Bytes> {
        self.try_get(key, now_ms).ok().flatten()
    }

    pub(super) fn try_get(&mut self, key: &[u8], now_ms: u64) -> crate::Result<Option<Bytes>> {
        #[cfg(feature = "embedded")]
        {
            let _ = now_ms;
            self.with_local_store(|store| store.try_get_if_local(key))
        }
        #[cfg(not(feature = "embedded"))]
        {
            Ok(self.map.get(key, now_ms))
        }
    }

    pub(super) fn getex(
        &mut self,
        key: &[u8],
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) -> Option<Bytes> {
        let value = self.get(key, now_ms);
        if value.is_some() {
            match expire_at_ms {
                Some(expire_at_ms) => {
                    self.expire_at(key, expire_at_ms, now_ms);
                }
                None => {
                    self.persist(key, now_ms);
                }
            }
        }
        value
    }

    pub(super) fn exists(&mut self, key: &[u8], now_ms: u64) -> bool {
        #[cfg(feature = "embedded")]
        {
            let _ = now_ms;
            self.with_local_store(|store| store.exists_if_local(key).unwrap_or(false))
        }
        #[cfg(not(feature = "embedded"))]
        {
            self.map.exists(key, now_ms)
        }
    }

    pub(super) fn ttl(&mut self, key: &[u8], millis: bool, now_ms: u64) -> i64 {
        #[cfg(feature = "embedded")]
        {
            let _ = now_ms;
            match millis {
                true => {
                    self.with_local_store(|store| store.pttl_millis_if_local(key).unwrap_or(-2))
                }
                false => {
                    self.with_local_store(|store| store.ttl_seconds_if_local(key).unwrap_or(-2))
                }
            }
        }
        #[cfg(not(feature = "embedded"))]
        {
            match millis {
                true => self.map.ttl_millis(key, now_ms),
                false => self.map.ttl_seconds(key, now_ms),
            }
        }
    }

    pub(super) fn set_owned(&mut self, key: Bytes, value: Bytes, ttl_ms: Option<u64>, now_ms: u64) {
        #[cfg(feature = "embedded")]
        {
            let _ = now_ms;
            let _ = self.with_local_store(|store| store.set_if_local(key, value, ttl_ms));
        }
        #[cfg(not(feature = "embedded"))]
        {
            let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
            self.map.set(key, value, expire_at_ms, now_ms);
        }
    }

    pub(super) fn delete(&mut self, key: &[u8], now_ms: u64) -> bool {
        #[cfg(feature = "embedded")]
        {
            let _ = now_ms;
            self.with_local_store(|store| store.delete_if_local(key).unwrap_or(false))
        }
        #[cfg(not(feature = "embedded"))]
        {
            self.map.delete(key, now_ms)
        }
    }

    pub(super) fn expire_at(&mut self, key: &[u8], expire_at_ms: u64, now_ms: u64) -> bool {
        #[cfg(feature = "embedded")]
        {
            let _ = now_ms;
            self.with_local_store(|store| store.expire_if_local(key, expire_at_ms).unwrap_or(false))
        }
        #[cfg(not(feature = "embedded"))]
        {
            self.map.expire(key, expire_at_ms, now_ms)
        }
    }

    pub(super) fn persist(&mut self, key: &[u8], now_ms: u64) -> bool {
        #[cfg(feature = "embedded")]
        {
            let _ = now_ms;
            self.with_local_store(|store| store.persist_if_local(key).unwrap_or(false))
        }
        #[cfg(not(feature = "embedded"))]
        {
            self.map.persist(key, now_ms)
        }
    }

    fn process_maintenance(&mut self, now_ms: u64) -> usize {
        #[cfg(feature = "embedded")]
        {
            let _ = now_ms;
            self.with_local_store(|store| store.process_maintenance())
        }
        #[cfg(not(feature = "embedded"))]
        {
            self.map.process_maintenance(now_ms)
        }
    }

    #[cfg(feature = "embedded")]
    fn with_local_store<R>(&mut self, op: impl FnOnce(&mut LocalEmbeddedStore) -> R) -> R {
        crate::storage::with_local_embedded_store(op)
            .expect("direct server local embedded store is not installed")
    }
}

impl DirectCommandContext {
    pub(crate) fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.try_get(key).ok().flatten()
    }

    pub(crate) fn try_get(&self, key: &[u8]) -> crate::Result<Option<Bytes>> {
        DirectServer::with_state(|state| {
            state.reads = state.reads.saturating_add(1);
            state.try_get(key, self.now_ms)
        })
    }

    pub(crate) fn getex(&self, key: &[u8], expire_at_ms: Option<u64>) -> Option<Bytes> {
        DirectServer::with_state(|state| {
            state.reads = state.reads.saturating_add(1);
            state.writes = state.writes.saturating_add(1);
            state.getex(key, expire_at_ms, self.now_ms)
        })
    }

    pub(crate) fn exists(&self, key: &[u8]) -> bool {
        DirectServer::with_state(|state| {
            state.reads = state.reads.saturating_add(1);
            state.exists(key, self.now_ms)
        })
    }

    pub(crate) fn ttl(&self, key: &[u8], millis: bool) -> i64 {
        DirectServer::with_state(|state| {
            state.reads = state.reads.saturating_add(1);
            state.ttl(key, millis, self.now_ms)
        })
    }

    pub(crate) fn set_owned(&self, key: Bytes, value: Bytes, ttl_ms: Option<u64>) {
        DirectServer::with_state(|state| {
            state.writes = state.writes.saturating_add(1);
            state.set_owned(key, value, ttl_ms, self.now_ms);
        });
    }

    pub(crate) fn delete(&self, key: &[u8]) -> bool {
        DirectServer::with_state(|state| {
            state.writes = state.writes.saturating_add(1);
            state.delete(key, self.now_ms)
        })
    }

    pub(crate) fn expire_at(&self, key: &[u8], expire_at_ms: u64) -> bool {
        DirectServer::with_state(|state| {
            state.writes = state.writes.saturating_add(1);
            state.expire_at(key, expire_at_ms, self.now_ms)
        })
    }

    pub(crate) fn persist(&self, key: &[u8]) -> bool {
        DirectServer::with_state(|state| {
            state.writes = state.writes.saturating_add(1);
            state.persist(key, self.now_ms)
        })
    }
}

impl DirectServer {
    pub(super) fn with_state<R>(op: impl FnOnce(&mut DirectServerState) -> R) -> R {
        DIRECT_STATE.with(|cell| {
            let mut state = cell.borrow_mut();
            let state = state.as_mut().expect("direct server state not initialized");
            op(state)
        })
    }

    pub(super) fn process_maintenance() {
        Self::with_state(|state| {
            state.expired = state
                .expired
                .saturating_add(state.process_maintenance(now_millis()) as u64);
            state.maintenance_runs = state.maintenance_runs.saturating_add(1);
        });
    }

    fn execute_borrowed(command: BorrowedCommand<'_>) -> Frame {
        let now_ms = now_millis();
        command.execute_direct_borrowed(DirectCommandContext::new(now_ms))
    }

    fn execute_fast(request: FastRequest<'_>) -> FastResponse {
        let now_ms = now_millis();
        DirectFastCommandDispatcher::execute(DirectCommandContext::new(now_ms), request)
            .unwrap_or_else(|| FastResponse::Error(b"ERR unsupported command".to_vec()))
    }
}

pub(super) struct DirectConnection;

impl DirectConnection {
    pub(super) async fn handle<S>(stream: S, _permit: OwnedSemaphorePermit) -> Result<()>
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

                let mut write_buffer = Vec::with_capacity(CONNECTION_BUFFER_CAPACITY);
                let mut consumed_total = 0usize;

                loop {
                    let slice = &frame_buffer.peek()[consumed_total..];
                    if slice.is_empty() {
                        break;
                    }

                    if FastCodec::is_fast_request_prefix(slice[0]) {
                        let decoded = FastCodec::decode_request(slice)?;
                        let Some((request, consumed)) = decoded else {
                            break;
                        };
                        consumed_total += consumed;
                        let response = DirectServer::execute_fast(request);
                        FastCodec::encode_response(&response, &mut write_buffer);
                    } else {
                        let decoded = RespCodec::decode_command(slice)?;
                        let Some((frame, consumed)) = decoded else {
                            break;
                        };
                        consumed_total += consumed;

                        let response = match BorrowedCommand::from_frame(frame) {
                            Ok(command) => DirectServer::execute_borrowed(command),
                            Err(error) => Frame::Error(format!("ERR {error}")),
                        };

                        RespCodec::encode(&response, &mut write_buffer);
                    }
                }

                if !write_buffer.is_empty()
                    && write_tx
                        .send(bytes::Bytes::from(write_buffer))
                        .await
                        .is_err()
                {
                    return Ok(());
                }
                if consumed_total > 0 {
                    frame_buffer.advance(consumed_total).map_err(|error| {
                        crate::ShardCacheError::Protocol(format!("handoff advance error: {error}"))
                    })?;
                }
            }
        };

        let result = read_loop.await;
        drop(write_tx);
        let _ = writer.await;
        result
    }
}

#[cfg(feature = "embedded")]
pub(super) struct ShardArcConnection;

#[cfg(feature = "embedded")]
impl ShardArcConnection {
    pub(super) async fn handle(
        mut stream: TcpStream,
        store: Arc<ShardArcEmbeddedStore>,
        _permit: OwnedSemaphorePermit,
    ) -> Result<()> {
        let mut frame_buffer = HandoffBuffer::with_config(HandoffConfig::buffer());
        let mut write_buffer = BytesMut::with_capacity(CONNECTION_BUFFER_CAPACITY);
        let mut resp_protocol = RespProtocolVersion::Resp2;

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

            let consumed_total = Self::process_buffer(
                frame_buffer.peek(),
                &store,
                &mut write_buffer,
                &mut resp_protocol,
            );

            if !write_buffer.is_empty() {
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

        Ok(())
    }

    fn process_buffer(
        buf: &[u8],
        store: &ShardArcEmbeddedStore,
        out: &mut BytesMut,
        resp_protocol: &mut RespProtocolVersion,
    ) -> usize {
        let mut consumed_total = 0usize;
        loop {
            let slice = &buf[consumed_total..];
            if slice.is_empty() {
                break;
            }
            let Some((consumed, command, args)) = DirectProtocol::try_resp_command_parts(slice)
            else {
                break;
            };
            Self::execute_resp(store, command, args.as_slice(), out, resp_protocol);
            consumed_total += consumed;
        }
        consumed_total
    }

    fn execute_resp(
        store: &ShardArcEmbeddedStore,
        command: &[u8],
        args: &[&[u8]],
        out: &mut BytesMut,
        resp_protocol: &mut RespProtocolVersion,
    ) {
        match command.len() {
            3 if command.eq_ignore_ascii_case(b"GET") => match args {
                [key] => {
                    if !store.get_blob_string_hashed_into(hash_key(key), key, out) {
                        ServerWire::write_resp_null(out, *resp_protocol);
                    }
                }
                _ => ServerWire::write_resp_error(out, "ERR wrong number of arguments for GET"),
            },
            3 if command.eq_ignore_ascii_case(b"SET") => match args {
                [key, value] => {
                    store.set_slice_prehashed(hash_key(key), key, value, None);
                    out.extend_from_slice(b"+OK\r\n");
                }
                _ => ServerWire::write_resp_error(out, "ERR wrong number of arguments for SET"),
            },
            5 if command.eq_ignore_ascii_case(b"HELLO") => {
                if let Some(protocol) = args
                    .first()
                    .and_then(|arg| RespProtocolVersion::from_hello_argument(arg))
                {
                    *resp_protocol = protocol;
                }
                ServerWire::write_resp_hello(out, *resp_protocol);
            }
            _ => ServerWire::write_resp_error(
                out,
                "ERR shard-arc benchmark server only supports GET and SET",
            ),
        }
    }
}
