use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes as SharedBytes;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::commands::EngineCommandCatalog;
use crate::config::ShardCacheConfig;
use crate::persistence::{PersistenceRuntime, load_recovery_state};
use crate::protocol::{CommandSpanFrame, FastRequest, FastResponse, Frame, RespCodec};
use crate::replication::{ReplicationBatchBuilder, ReplicationMutation, ReplicationPrimary};
use crate::storage::command::{BorrowedCommand, Command};
use crate::storage::stats::{GlobalStatsSnapshot, ShardStatsSnapshot};
use crate::storage::{
    Bytes, FlatMap, MutationOp, MutationRecord, StoredEntry, hash_key, now_millis, shift_for,
    stripe_index,
};
#[cfg(feature = "telemetry")]
use crate::storage::{CacheTelemetry, CacheTelemetryHandle};
use crate::{Result, ShardCacheError};

#[derive(Clone)]
pub struct EngineHandle {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    config: ShardCacheConfig,
    started_at: Instant,
    shift: u32,
    shard_senders: Vec<Sender<ShardMessage>>,
    shard_threads: Mutex<Vec<JoinHandle<()>>>,
    persistence: PersistenceRuntime,
    replication: Option<Arc<ReplicationPrimary>>,
    #[cfg(feature = "telemetry")]
    metrics: Option<Arc<CacheTelemetry>>,
}

const MULTIKEY_INLINE_KEY_MAX: usize = 32;
const MULTIKEY_INLINE_VALUE_MAX: usize = 64;
pub(crate) const RESP_SPANNED_VALUE_MIN: usize = 2 * 1024;

type InlineKey = smallvec::SmallVec<[u8; MULTIKEY_INLINE_KEY_MAX]>;
type InlineValue = smallvec::SmallVec<[u8; MULTIKEY_INLINE_VALUE_MAX]>;
pub(crate) type EngineFrameFuture<'a> = Pin<Box<dyn Future<Output = Result<Frame>> + Send + 'a>>;
pub(crate) type EngineFastFuture<'a> =
    Pin<Box<dyn Future<Output = Result<FastResponse>> + Send + 'a>>;
pub(crate) type EngineRespSpanFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

#[derive(Clone, Copy)]
pub(crate) struct EngineCommandContext<'engine> {
    engine: &'engine EngineHandle,
}

impl<'engine> EngineCommandContext<'engine> {
    #[inline(always)]
    pub(crate) fn new(engine: &'engine EngineHandle) -> Self {
        Self { engine }
    }

    pub(crate) fn route_key(self, key: &[u8]) -> usize {
        self.engine.route(key)
    }

    pub(crate) fn route_key_hash(self, key_hash: u64) -> usize {
        self.engine.route_hash_to_shard(key_hash)
    }

    pub(crate) async fn request(self, shard_id: usize, op: ShardOperation) -> Result<ShardReply> {
        self.engine.request(shard_id, op).await
    }
}

pub(crate) enum ShardKey {
    Inline(InlineKey),
    Shared(SharedBytes),
}

impl ShardKey {
    pub(crate) fn inline(key: &[u8]) -> Self {
        Self::Inline(InlineKey::from_slice(key))
    }

    pub(crate) fn from_owner(owner: &SharedBytes, range: Range<usize>) -> Self {
        if range.len() <= MULTIKEY_INLINE_KEY_MAX {
            Self::Inline(InlineKey::from_slice(&owner[range]))
        } else {
            Self::Shared(owner.slice(range))
        }
    }

    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Inline(key) => key.as_ref(),
            Self::Shared(key) => key.as_ref(),
        }
    }

    fn into_shared(self) -> SharedBytes {
        match self {
            Self::Inline(key) => SharedBytes::from(key.into_vec()),
            Self::Shared(key) => key,
        }
    }
}

pub(crate) enum ShardValue {
    Inline(InlineValue),
    Shared(SharedBytes),
}

impl ShardValue {
    pub(crate) fn inline(value: &[u8]) -> Self {
        Self::Inline(InlineValue::from_slice(value))
    }

    pub(crate) fn from_owner(owner: &SharedBytes, range: Range<usize>) -> Self {
        if range.len() >= RESP_SPANNED_VALUE_MIN {
            Self::Shared(owner.slice(range))
        } else {
            Self::Inline(InlineValue::from_slice(&owner[range]))
        }
    }

    fn into_shared(self) -> SharedBytes {
        match self {
            Self::Inline(value) => SharedBytes::from(value.into_vec()),
            Self::Shared(value) => value,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpirationChange {
    Keep,
    ExpireAt(u64),
    Persist,
}

impl ExpirationChange {
    fn expire_at_ms(self) -> Option<u64> {
        match self {
            Self::Keep | Self::Persist => None,
            Self::ExpireAt(expire_at_ms) => Some(expire_at_ms),
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum ShardMessage {
    Execute {
        op: ShardOperation,
        reply: oneshot::Sender<Result<ShardReply>>,
    },
    Snapshot {
        reply: oneshot::Sender<Vec<StoredEntry>>,
    },
    Stats {
        reply: oneshot::Sender<ShardStatsSnapshot>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ShardOperation {
    Get(Vec<u8>),
    GetEx {
        key_hash: u64,
        key: ShardKey,
        expiration: ExpirationChange,
    },
    Set {
        key_hash: u64,
        key: ShardKey,
        value: ShardValue,
        expire_at_ms: Option<u64>,
    },
    Delete {
        key_hash: u64,
        key: ShardKey,
    },
    Exists(Vec<u8>),
    Ttl {
        key: Vec<u8>,
        millis: bool,
    },
    Expire {
        key_hash: u64,
        key: ShardKey,
        expire_at_ms: Option<u64>,
    },
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ShardReply {
    Value(Option<Bytes>),
    Integer(i64),
    Ok,
}

struct ShardState {
    shard_id: usize,
    map: FlatMap,
    wal: Option<crate::persistence::WalAppender>,
    replication: Option<Arc<ReplicationPrimary>>,
    replication_batch: Option<ReplicationBatchBuilder>,
    sequence: u64,
}

impl EngineHandle {
    pub fn open(config: ShardCacheConfig) -> Result<Self> {
        #[cfg(feature = "telemetry")]
        {
            Self::open_with_metrics(config, None)
        }
        #[cfg(not(feature = "telemetry"))]
        {
            config.validate()?;
            config.ensure_paths()?;

            let recovery = load_recovery_state(&config.persistence)?;
            let persistence =
                PersistenceRuntime::start(config.shard_count, config.persistence.clone())?;
            let replication = start_replication_primary(&config)?;
            let shift = shift_for(config.shard_count);
            let recovered = RecoveredShardEntries::partition(&recovery.entries, shift);

            let mut shard_senders = Vec::with_capacity(config.shard_count);
            let mut shard_threads = Vec::with_capacity(config.shard_count);
            for shard_id in 0..config.shard_count {
                let (tx, rx) = unbounded::<ShardMessage>();
                let shard_entries = recovered.get(&shard_id).cloned().unwrap_or_default();
                let shard_config = config.clone();
                let wal = persistence.appender(shard_id);
                let replication = replication.clone();
                let join = thread::Builder::new()
                    .name(format!("shardcache-shard-{shard_id}"))
                    .spawn(move || {
                        ShardWorker::run(
                            shard_id,
                            rx,
                            shard_config,
                            shard_entries,
                            wal,
                            replication,
                        )
                    })
                    .map_err(|error| {
                        ShardCacheError::Config(format!(
                            "failed to start shard {shard_id}: {error}"
                        ))
                    })?;
                shard_senders.push(tx);
                shard_threads.push(join);
            }

            Ok(Self {
                inner: Arc::new(EngineInner {
                    config,
                    started_at: Instant::now(),
                    shift,
                    shard_senders,
                    shard_threads: Mutex::new(shard_threads),
                    persistence,
                    replication,
                }),
            })
        }
    }

    #[cfg(feature = "telemetry")]
    pub fn open_with_metrics(
        config: ShardCacheConfig,
        metrics: Option<Arc<CacheTelemetry>>,
    ) -> Result<Self> {
        config.validate()?;
        config.ensure_paths()?;

        let recovery = load_recovery_state(&config.persistence)?;
        let persistence = PersistenceRuntime::start_with_metrics(
            config.shard_count,
            config.persistence.clone(),
            metrics.clone(),
        )?;
        let replication = start_replication_primary(&config)?;
        let shift = shift_for(config.shard_count);
        let recovered = RecoveredShardEntries::partition(&recovery.entries, shift);

        let mut shard_senders = Vec::with_capacity(config.shard_count);
        let mut shard_threads = Vec::with_capacity(config.shard_count);
        for shard_id in 0..config.shard_count {
            let (tx, rx) = unbounded::<ShardMessage>();
            let shard_entries = recovered.get(&shard_id).cloned().unwrap_or_default();
            let shard_config = config.clone();
            let wal = persistence.appender(shard_id);
            let replication = replication.clone();
            let shard_metrics = metrics.clone();
            let join = thread::Builder::new()
                .name(format!("shardcache-shard-{shard_id}"))
                .spawn(move || {
                    ShardWorker::run(
                        shard_id,
                        rx,
                        shard_config,
                        shard_entries,
                        wal,
                        replication,
                        shard_metrics,
                    )
                })
                .map_err(|error| {
                    ShardCacheError::Config(format!("failed to start shard {shard_id}: {error}"))
                })?;
            shard_senders.push(tx);
            shard_threads.push(join);
        }

        Ok(Self {
            inner: Arc::new(EngineInner {
                config,
                started_at: Instant::now(),
                shift,
                shard_senders,
                shard_threads: Mutex::new(shard_threads),
                persistence,
                replication,
                metrics,
            }),
        })
    }

    pub fn config(&self) -> &ShardCacheConfig {
        &self.inner.config
    }

    pub async fn execute(&self, command: Command) -> Result<Frame> {
        self.execute_owned(command).await
    }

    pub async fn execute_fast<'a>(&'a self, request: FastRequest<'a>) -> Result<FastResponse> {
        EngineCommandCatalog::execute_fast(EngineCommandContext::new(self), request)
            .await
            .unwrap_or_else(|| Ok(FastResponse::Error(b"ERR unsupported command".to_vec())))
    }

    pub async fn execute_borrowed<'a>(&'a self, command: BorrowedCommand<'a>) -> Result<Frame> {
        command
            .execute_engine(EngineCommandContext::new(self))
            .await
    }

    pub(crate) async fn execute_resp_borrowed_into<'a>(
        &'a self,
        command: BorrowedCommand<'a>,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let response = self.execute_borrowed(command).await?;
        RespCodec::encode(&response, out);
        Ok(())
    }

    pub(crate) fn should_use_spanned_resp(command: &BorrowedCommand<'_>) -> bool {
        command.supports_spanned_resp()
    }

    pub(crate) async fn execute_resp_spanned_into(
        &self,
        frame: CommandSpanFrame,
        owner: SharedBytes,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        if frame.parts.is_empty() {
            return Err(ShardCacheError::Command("empty command".into()));
        }
        let name_span = &frame.parts[0];
        let name = String::from_utf8_lossy(&owner[name_span.start..name_span.end]).into_owned();
        if let Some(result) = EngineCommandCatalog::execute_resp_spanned(
            EngineCommandContext::new(self),
            frame,
            owner,
            out,
        )
        .await
        {
            return result;
        }
        Err(ShardCacheError::Command(format!(
            "unsupported spanned command: {name}",
        )))
    }

    async fn execute_owned(&self, command: Command) -> Result<Frame> {
        self.execute_borrowed(command.to_borrowed_command()).await
    }

    pub async fn snapshot(&self) -> Result<std::path::PathBuf> {
        let now_ms = now_millis();
        let mut entries = Vec::new();
        for shard in 0..self.inner.shard_senders.len() {
            let (tx, rx) = oneshot::channel();
            self.inner.shard_senders[shard]
                .send(ShardMessage::Snapshot { reply: tx })
                .map_err(|_| ShardCacheError::ChannelClosed("snapshot request"))?;
            entries.extend(
                rx.await
                    .map_err(|_| ShardCacheError::ChannelClosed("snapshot response"))?,
            );
        }
        self.inner.persistence.snapshot(&entries, now_ms)
    }

    pub async fn stats_snapshot(&self) -> Result<GlobalStatsSnapshot> {
        let mut shards = Vec::with_capacity(self.inner.shard_senders.len());
        for shard in 0..self.inner.shard_senders.len() {
            let (tx, rx) = oneshot::channel();
            self.inner.shard_senders[shard]
                .send(ShardMessage::Stats { reply: tx })
                .map_err(|_| ShardCacheError::ChannelClosed("stats request"))?;
            shards.push(
                rx.await
                    .map_err(|_| ShardCacheError::ChannelClosed("stats response"))?,
            );
        }

        let total_keys = shards.iter().map(|shard| shard.key_count).sum();
        let total_reads = shards.iter().map(|shard| shard.reads).sum();
        let total_writes = shards.iter().map(|shard| shard.writes).sum();
        let total_deletes = shards.iter().map(|shard| shard.deletes).sum();
        let total_expired = shards.iter().map(|shard| shard.expired).sum();

        Ok(GlobalStatsSnapshot {
            uptime_ms: self.inner.started_at.elapsed().as_millis() as u64,
            shard_count: self.inner.shard_senders.len(),
            total_keys,
            total_reads,
            total_writes,
            total_deletes,
            total_expired,
            shards,
            wal: self.inner.persistence.stats_snapshot(),
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        for shard in &self.inner.shard_senders {
            let (tx, rx) = oneshot::channel();
            shard
                .send(ShardMessage::Shutdown { reply: tx })
                .map_err(|_| ShardCacheError::ChannelClosed("shutdown request"))?;
            rx.await
                .map_err(|_| ShardCacheError::ChannelClosed("shutdown response"))?;
        }
        while let Some(join) = self.inner.shard_threads.lock().pop() {
            join.join()
                .map_err(|_| ShardCacheError::TaskJoin("shard thread panicked".into()))?;
        }
        self.inner.persistence.shutdown()?;
        if let Some(replication) = &self.inner.replication {
            replication.shutdown()?;
        }
        Ok(())
    }

    pub fn snapshot_interval(&self) -> Duration {
        self.inner.config.snapshot_interval()
    }

    pub fn snapshot_min_writes(&self) -> u64 {
        self.inner.config.persistence.snapshot_min_writes
    }

    #[cfg(feature = "telemetry")]
    pub fn metrics(&self) -> Option<Arc<CacheTelemetry>> {
        self.inner.metrics.clone()
    }

    fn route(&self, key: &[u8]) -> usize {
        stripe_index(hash_key(key), self.inner.shift)
    }

    fn route_hash_to_shard(&self, route_hash: u64) -> usize {
        stripe_index(route_hash, self.inner.shift)
    }

    async fn request(&self, shard_id: usize, op: ShardOperation) -> Result<ShardReply> {
        self.request_pending(shard_id, op)?
            .await
            .map_err(|_| ShardCacheError::ChannelClosed("shard response"))?
    }

    fn request_pending(
        &self,
        shard_id: usize,
        op: ShardOperation,
    ) -> Result<oneshot::Receiver<Result<ShardReply>>> {
        let (tx, rx) = oneshot::channel();
        self.inner.shard_senders[shard_id]
            .send(ShardMessage::Execute { op, reply: tx })
            .map_err(|_| ShardCacheError::ChannelClosed("shard request"))?;
        Ok(rx)
    }
}

struct RecoveredShardEntries;

impl RecoveredShardEntries {
    fn partition(entries: &[StoredEntry], shift: u32) -> HashMap<usize, Vec<StoredEntry>> {
        let mut shards = HashMap::<usize, Vec<StoredEntry>>::new();
        for entry in entries {
            let shard = stripe_index(hash_key(entry.key.as_ref()), shift);
            shards.entry(shard).or_default().push(entry.clone());
        }
        shards
    }
}

struct ShardWorker;

fn start_replication_primary(config: &ShardCacheConfig) -> Result<Option<Arc<ReplicationPrimary>>> {
    if !config.replication.enabled {
        return Ok(None);
    }
    if config.replication.role != crate::config::ReplicationRole::Primary {
        return Ok(None);
    }
    Ok(Some(Arc::new(ReplicationPrimary::start(
        config.shard_count,
        config.replication.clone(),
    )?)))
}

impl ShardWorker {
    fn run(
        shard_id: usize,
        receiver: Receiver<ShardMessage>,
        config: ShardCacheConfig,
        recovered_entries: Vec<StoredEntry>,
        wal: Option<crate::persistence::WalAppender>,
        replication: Option<Arc<ReplicationPrimary>>,
        #[cfg(feature = "telemetry")] metrics: Option<Arc<CacheTelemetry>>,
    ) {
        Self::pin_current_thread(shard_id);

        #[allow(unused_mut)]
        let mut map = FlatMap::from_entries(recovered_entries, now_millis());
        map.configure_memory_policy(
            config.per_shard_memory_limit_bytes(),
            config.eviction_policy,
            now_millis(),
        );
        #[cfg(feature = "telemetry")]
        if let Some(metrics) = &metrics {
            map.attach_metrics(CacheTelemetryHandle::from_arc(metrics), shard_id);
        }

        let mut state = ShardState {
            shard_id,
            map,
            wal,
            replication_batch: replication
                .as_ref()
                .map(|_| ReplicationBatchBuilder::new(config.replication.clone())),
            replication,
            sequence: 0,
        };
        let maintenance_interval = config.ttl_sweep_interval();
        let mut next_maintenance = Instant::now() + maintenance_interval;

        loop {
            state.flush_replication_due();
            let timeout = state.next_worker_timeout(next_maintenance);
            match receiver.recv_timeout(timeout) {
                Ok(ShardMessage::Execute { op, reply }) => {
                    let _ = reply.send(state.execute(op));
                    state.flush_replication_due();
                }
                Ok(ShardMessage::Snapshot { reply }) => {
                    let _ = reply.send(state.map.snapshot_entries(now_millis()));
                    state.flush_replication_due();
                }
                Ok(ShardMessage::Stats { reply }) => {
                    let _ = reply.send(state.stats_snapshot());
                    state.flush_replication_due();
                }
                Ok(ShardMessage::Shutdown { reply }) => {
                    state.flush_replication();
                    let _ = reply.send(());
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {
                    state.flush_replication_due();
                    if Instant::now() >= next_maintenance {
                        state.map.process_maintenance(now_millis());
                        next_maintenance = Instant::now() + maintenance_interval;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        state.flush_replication();
    }

    fn pin_current_thread(shard_id: usize) {
        if let Some(cores) = core_affinity::get_core_ids()
            && let Some(core) = cores.get(shard_id % cores.len())
        {
            core_affinity::set_for_current(*core);
        }
    }
}

impl ShardState {
    fn execute(&mut self, op: ShardOperation) -> Result<ShardReply> {
        let now_ms = now_millis();
        let reply = match op {
            ShardOperation::Get(key) => ShardReply::Value(self.map.get(&key, now_ms)),
            ShardOperation::GetEx {
                key_hash,
                key,
                expiration,
            } => {
                let value = self.map.get(key.as_ref(), now_ms);
                if value.is_some() && expiration != ExpirationChange::Keep {
                    self.apply_expiration_change(key_hash, key, expiration, now_ms)?;
                }
                ShardReply::Value(value)
            }
            ShardOperation::Set {
                key_hash,
                key,
                value,
                expire_at_ms,
            } => {
                self.set_and_append_wal(key_hash, key, value, expire_at_ms, now_ms)?;
                ShardReply::Ok
            }
            ShardOperation::Delete { key_hash, key } => {
                ShardReply::Integer(self.delete_and_append_wal(key_hash, key, now_ms)? as i64)
            }
            ShardOperation::Exists(key) => {
                ShardReply::Integer(self.map.exists(&key, now_ms) as i64)
            }
            ShardOperation::Ttl { key, millis } => {
                let ttl = match millis {
                    true => self.map.ttl_millis(&key, now_ms),
                    false => self.map.ttl_seconds(&key, now_ms),
                };
                ShardReply::Integer(ttl)
            }
            ShardOperation::Expire {
                key_hash,
                key,
                expire_at_ms,
            } => {
                let expiration = expire_at_ms
                    .map(ExpirationChange::ExpireAt)
                    .unwrap_or(ExpirationChange::Persist);
                ShardReply::Integer(
                    self.apply_expiration_change(key_hash, key, expiration, now_ms)? as i64,
                )
            }
        };
        Ok(reply)
    }

    fn set_and_append_wal(
        &mut self,
        key_hash: u64,
        key: ShardKey,
        value: ShardValue,
        expire_at_ms: Option<u64>,
        timestamp_ms: u64,
    ) -> Result<()> {
        if self.wal.is_some() || self.replication.is_some() {
            self.sequence = self.sequence.saturating_add(1);
            let key = key.into_shared();
            let value = value.into_shared();
            self.map.set_bytes_hashed(
                key_hash,
                key.as_ref(),
                value.clone(),
                expire_at_ms,
                timestamp_ms,
            );
            let record = MutationRecord {
                shard_id: self.shard_id,
                sequence: self.sequence,
                timestamp_ms,
                op: MutationOp::Set,
                key,
                value,
                expire_at_ms,
            };
            if let Some(wal) = &self.wal {
                wal.append(record.clone())?;
            }
            self.emit_replication(ReplicationMutation::from_record_with_key_hash(
                &record, key_hash,
            ));
        } else {
            match value {
                ShardValue::Inline(value) => {
                    self.map.set_slice_hashed(
                        key_hash,
                        key.as_ref(),
                        value.as_ref(),
                        expire_at_ms,
                        timestamp_ms,
                    );
                }
                ShardValue::Shared(value) => {
                    self.map.set_bytes_hashed(
                        key_hash,
                        key.as_ref(),
                        value,
                        expire_at_ms,
                        timestamp_ms,
                    );
                }
            }
        }
        Ok(())
    }

    fn delete_and_append_wal(
        &mut self,
        key_hash: u64,
        key: ShardKey,
        timestamp_ms: u64,
    ) -> Result<bool> {
        if self.wal.is_some() || self.replication.is_some() {
            self.sequence = self.sequence.saturating_add(1);
            let key = key.into_shared();
            let deleted = self.map.delete_hashed(key_hash, key.as_ref(), timestamp_ms);
            let record = MutationRecord {
                shard_id: self.shard_id,
                sequence: self.sequence,
                timestamp_ms,
                op: MutationOp::Del,
                key,
                value: SharedBytes::new(),
                expire_at_ms: None,
            };
            if let Some(wal) = &self.wal {
                wal.append(record.clone())?;
            }
            self.emit_replication(ReplicationMutation::from_record_with_key_hash(
                &record, key_hash,
            ));
            return Ok(deleted);
        }

        Ok(self.map.delete_hashed(key_hash, key.as_ref(), timestamp_ms))
    }

    fn apply_expiration_change(
        &mut self,
        key_hash: u64,
        key: ShardKey,
        expiration: ExpirationChange,
        timestamp_ms: u64,
    ) -> Result<bool> {
        let changed = match expiration {
            ExpirationChange::Keep => false,
            ExpirationChange::ExpireAt(expire_at_ms) => {
                self.map.expire(key.as_ref(), expire_at_ms, timestamp_ms)
            }
            ExpirationChange::Persist => self.map.persist(key.as_ref(), timestamp_ms),
        };
        if !changed {
            return Ok(false);
        }
        if self.wal.is_some() || self.replication.is_some() {
            self.sequence = self.sequence.saturating_add(1);
            let key = key.into_shared();
            let record = MutationRecord {
                shard_id: self.shard_id,
                sequence: self.sequence,
                timestamp_ms,
                op: MutationOp::Expire,
                key,
                value: SharedBytes::new(),
                expire_at_ms: expiration.expire_at_ms(),
            };
            if let Some(wal) = &self.wal {
                wal.append(record.clone())?;
            }
            self.emit_replication(ReplicationMutation::from_record_with_key_hash(
                &record, key_hash,
            ));
        }
        Ok(true)
    }

    fn emit_replication(&mut self, mutation: ReplicationMutation) {
        let Some(replication) = &self.replication else {
            return;
        };
        let Some(batch_builder) = &mut self.replication_batch else {
            replication.emit(mutation);
            return;
        };
        if let Some(batch) = batch_builder.push(mutation) {
            replication.export_batch_direct(batch);
        }
    }

    fn flush_replication_due(&mut self) {
        let Some(replication) = &self.replication else {
            return;
        };
        let Some(batch_builder) = &mut self.replication_batch else {
            return;
        };
        if let Some(batch) = batch_builder.flush_due() {
            replication.export_batch_direct(batch);
        }
    }

    fn flush_replication(&mut self) {
        let Some(replication) = &self.replication else {
            return;
        };
        let Some(batch_builder) = &mut self.replication_batch else {
            return;
        };
        if let Some(batch) = batch_builder.flush() {
            replication.export_batch_direct(batch);
        }
    }

    fn next_worker_timeout(&self, next_maintenance: Instant) -> Duration {
        let maintenance_timeout = next_maintenance
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        match self
            .replication_batch
            .as_ref()
            .and_then(ReplicationBatchBuilder::next_timeout)
        {
            Some(replication_timeout) => maintenance_timeout.min(replication_timeout),
            None => maintenance_timeout,
        }
    }

    fn stats_snapshot(&self) -> ShardStatsSnapshot {
        let (hot, warm, cold) = self.map.stats_snapshot();
        ShardStatsSnapshot {
            shard_id: self.shard_id,
            key_count: self.map.len(),
            reads: 0,
            writes: 0,
            deletes: 0,
            expired: 0,
            maintenance_runs: 0,
            hot,
            warm,
            cold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ShardCacheConfig;

    #[tokio::test(flavor = "multi_thread")]
    async fn large_resp_set_uses_spanned_owner_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut config = ShardCacheConfig {
            shard_count: 4,
            ..ShardCacheConfig::default()
        };
        config.persistence.enabled = false;
        config.persistence.data_dir = temp_dir.path().to_path_buf();

        let engine = EngineHandle::open(config).expect("engine");
        let value = vec![b'x'; RESP_SPANNED_VALUE_MIN];
        let frame = Frame::Array(vec![
            Frame::BlobString(b"SET".to_vec()),
            Frame::BlobString(b"large".to_vec()),
            Frame::BlobString(value.clone()),
        ]);
        let mut encoded = Vec::new();
        RespCodec::encode(&frame, &mut encoded);

        {
            let (borrowed_frame, borrowed_consumed) = RespCodec::decode_command(&encoded)
                .expect("borrowed decode")
                .expect("borrowed frame");
            assert_eq!(borrowed_consumed, encoded.len());
            let command = BorrowedCommand::from_frame(borrowed_frame).expect("borrowed command");
            assert!(EngineHandle::should_use_spanned_resp(&command));
        }

        let (span_frame, span_consumed) = RespCodec::decode_command_spans(&encoded)
            .expect("span decode")
            .expect("span frame");
        assert_eq!(span_consumed, encoded.len());

        let mut out = Vec::new();
        engine
            .execute_resp_spanned_into(span_frame, SharedBytes::from(encoded), &mut out)
            .await
            .expect("spanned SET");
        assert_eq!(out, b"+OK\r\n");

        let get = BorrowedCommand::from_parts(&[b"GET".as_slice(), b"large".as_slice()])
            .expect("GET command");
        let response = engine.execute_borrowed(get).await.expect("GET response");
        assert_eq!(response, Frame::BlobString(value));

        engine.shutdown().await.expect("shutdown");
    }
}
