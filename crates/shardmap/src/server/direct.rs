use std::cell::RefCell;

use super::commands::{DirectCommandContext, DirectFastCommandDispatcher};
use super::connection::HandoffConfig;
use super::*;

thread_local! {
    static DIRECT_STATE: RefCell<Option<DirectServerState>> = const { RefCell::new(None) };
}

pub(super) struct DirectServer;

impl DirectServer {
    pub(super) fn initialize(config: &ShardCacheConfig) {
        DIRECT_STATE.with(|cell| {
            *cell.borrow_mut() = Some(DirectServerState::new(config));
        });
    }

    pub(super) fn clear() {
        DIRECT_STATE.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }
}

#[derive(Debug)]
pub(super) struct DirectServerState {
    #[cfg(feature = "embedded")]
    store: LocalEmbeddedStore,
    #[cfg(not(feature = "embedded"))]
    map: FlatMap,
    pub(super) reads: u64,
    pub(super) writes: u64,
    expired: u64,
    maintenance_runs: u64,
}

impl DirectServerState {
    fn new(_config: &ShardCacheConfig) -> Self {
        Self {
            #[cfg(feature = "embedded")]
            store: {
                let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::FullKey);
                store.configure_memory_policy(
                    _config.per_shard_memory_limit_bytes(),
                    _config.eviction_policy,
                );
                store
                    .into_local_stores(1)
                    .into_iter()
                    .next()
                    .expect("direct mode must create one local embedded store")
            },
            #[cfg(not(feature = "embedded"))]
            map: FlatMap::new(),
            reads: 0,
            writes: 0,
            expired: 0,
            maintenance_runs: 0,
        }
    }

    pub(super) fn get(&mut self, key: &[u8], now_ms: u64) -> Option<Bytes> {
        #[cfg(feature = "embedded")]
        {
            let _ = now_ms;
            self.store.get(key)
        }
        #[cfg(not(feature = "embedded"))]
        {
            self.map.get(key, now_ms)
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
            self.store.exists(key)
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
                true => self.store.pttl_millis(key),
                false => self.store.ttl_seconds(key),
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
            self.store.set(key, value, ttl_ms);
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
            self.store.delete(key)
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
            self.store.expire(key, expire_at_ms)
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
            self.store.persist(key)
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
            self.store.process_maintenance()
        }
        #[cfg(not(feature = "embedded"))]
        {
            self.map.process_maintenance(now_ms)
        }
    }
}

impl DirectCommandContext {
    pub(crate) fn get(&self, key: &[u8]) -> Option<Bytes> {
        DirectServer::with_state(|state| {
            state.reads = state.reads.saturating_add(1);
            state.get(key, self.now_ms)
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
