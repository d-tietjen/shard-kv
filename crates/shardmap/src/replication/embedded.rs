#[cfg(feature = "redis")]
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use parking_lot::Mutex;

use crate::config::ReplicationConfig;
#[cfg(feature = "redis")]
use crate::storage::VectorMutationKind;
use crate::storage::{
    Bytes, EmbeddedRouteMode, EmbeddedStore, MutationOp, hash_key_tag_from_hash, now_millis,
    shift_for, stripe_index, ttl_now_millis,
};
use crate::{Result, ShardCacheError};

use super::ReplicationFrameBytes;
use super::backlog::BacklogCatchUp;
use super::batcher::{
    EncodedReplicationBatch, EncodedReplicationBatchBuilder, ReplicationBatch,
    ReplicationBatchBuilder, ReplicationPrimary,
};
use super::metrics::{ReplicationMetrics, ReplicationMetricsSnapshot};
use super::protocol::{
    BorrowedReplicationMutation, FrameBackedReplicationMutation, FrameKind,
    ReplicationFrameBytesPayload, ReplicationFramePayload, ReplicationMutation,
    ReplicationMutationOp, ReplicationSnapshot, ShardWatermarks, decode_frame_payload,
    decode_frame_payload_bytes, mutation_batch_record_count, visit_mutation_batch_payload,
    visit_mutation_batch_payload_bytes,
};
use super::transport::{ReplicationPrimaryServer, SnapshotProvider};

const DIRECT_ENCODED_SET_MAX_VALUE_LEN: usize = 128;

#[derive(Debug)]
pub struct ReplicatedEmbeddedStore {
    store: Arc<EmbeddedStore>,
    primary: Arc<ReplicationPrimary>,
    emitters: Arc<ReplicatedEmbeddedEmitters>,
}

#[derive(Debug)]
pub struct ReplicationReplica {
    store: EmbeddedStore,
    watermarks: ShardWatermarks,
    physical_topology_initialized: bool,
    source_shard_count: Option<usize>,
    metrics: ReplicationMetrics,
}

#[derive(Debug)]
struct ReplicatedEmbeddedEmitters {
    primary: Arc<ReplicationPrimary>,
    shards: Vec<Mutex<ReplicatedEmbeddedShardEmitter>>,
    flusher_stop: AtomicBool,
    exporter_stop: Arc<AtomicBool>,
    flusher_join: Mutex<Option<JoinHandle<()>>>,
    exporter_joins: Mutex<Vec<JoinHandle<()>>>,
    flush_wake_tx: Sender<()>,
    flush_wake_rx: Receiver<()>,
    #[cfg(feature = "redis")]
    vector_pending: Mutex<VectorPendingState>,
    #[cfg(feature = "redis")]
    vector_pending_capacity: usize,
    #[cfg(feature = "redis")]
    vector_pending_max_bytes: usize,
    #[cfg(feature = "redis")]
    vector_flush_interval: Duration,
}

#[cfg(feature = "redis")]
#[derive(Debug)]
struct VectorPendingState {
    mutations: HashMap<bytes::Bytes, ReplicationMutation>,
    retained_bytes: usize,
    last_flush: Instant,
}

#[derive(Debug)]
struct ReplicatedEmbeddedShardEmitter {
    sequence: u64,
    batch: ReplicationBatchBuilder,
    encoded_batch: EncodedReplicationBatchBuilder,
    tx: Sender<ReplicatedEmbeddedBatch>,
}

#[derive(Debug)]
enum ReplicatedEmbeddedBatch {
    Owned(ReplicationBatch),
    Encoded(EncodedReplicationBatch),
}

#[derive(Debug, Clone, Copy)]
struct BorrowedSetReplication<'a> {
    shard_id: usize,
    timestamp_ms: u64,
    key_hash: u64,
    key_tag: u64,
    key: &'a [u8],
    value: &'a [u8],
    expire_at_ms: Option<u64>,
    governance: Option<&'a [u8]>,
}

impl ReplicatedEmbeddedStore {
    pub fn new(shard_count: usize, config: ReplicationConfig) -> Result<Self> {
        Self::with_route_mode(shard_count, EmbeddedRouteMode::FullKey, config)
    }

    pub fn with_route_mode(
        shard_count: usize,
        route_mode: EmbeddedRouteMode,
        config: ReplicationConfig,
    ) -> Result<Self> {
        if !config.enabled {
            return Err(ShardCacheError::Config(
                "ReplicatedEmbeddedStore requires replication.enabled = true".into(),
            ));
        }
        let store = Arc::new(EmbeddedStore::with_route_mode(shard_count, route_mode));
        let primary = Arc::new(ReplicationPrimary::start(shard_count, config.clone())?);
        let emitters =
            ReplicatedEmbeddedEmitters::start(Arc::clone(&primary), shard_count, config)?;
        #[cfg(feature = "redis")]
        {
            let vector_emitters = Arc::clone(&emitters);
            store.configure_vector_mutation_observer(Some(Arc::new(
                move |kind, key, value, expire_at_ms| {
                    let op = match kind {
                        VectorMutationKind::Set => ReplicationMutationOp::Set,
                        VectorMutationKind::Delete => ReplicationMutationOp::Del,
                        VectorMutationKind::Expire => ReplicationMutationOp::Expire,
                    };
                    let value = match (kind, value) {
                        (VectorMutationKind::Set, Some(value)) => value,
                        (VectorMutationKind::Set, None) => {
                            tracing::error!(
                                "rejecting vector replication event without canonical state"
                            );
                            return;
                        }
                        (_, _) => bytes::Bytes::new(),
                    };
                    let key_hash = crate::storage::hash_key(key);
                    let shard_id = vector_emitters.shard_id_for_hash(key_hash);
                    let timestamp_ms = ttl_now_millis();
                    vector_emitters.queue_vector_mutation(ReplicationMutation {
                        shard_id,
                        sequence: 0,
                        timestamp_ms,
                        op,
                        key_hash,
                        key_tag: hash_key_tag_from_hash(key_hash),
                        key: bytes::Bytes::copy_from_slice(key),
                        value,
                        expire_at_ms,
                        governance: None,
                    });
                },
            )));
        }
        Ok(Self {
            store,
            primary,
            emitters,
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.store.get(key)
    }

    pub fn get_with_governance_filter<F>(&self, key: &[u8], authorize: F) -> Option<Bytes>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        self.store
            .get_value_bytes_with_governance_filter(key, authorize)
            .map(|value| value.to_vec())
    }

    pub fn set(&self, key: Bytes, value: Bytes, ttl_ms: Option<u64>) {
        let route = self.store.route_key(&key);
        #[cfg(feature = "redis")]
        let was_vector = self.store.clone_vector_value(&key).is_some();
        #[cfg(feature = "redis")]
        if was_vector {
            self.store.delete(&key);
        }
        #[cfg(feature = "redis")]
        self.emitters.flush_vector_key(&key);
        let replication_shard_id = route.shard_id;
        let key = bytes::Bytes::from(key);
        let value = bytes::Bytes::from(value);
        let direct_encode = direct_encoded_set_enabled(value.len());
        match ttl_ms {
            Some(ttl_ms) => {
                let now_ms = now_millis();
                let expire_at_ms = Some(now_ms.saturating_add(ttl_ms));
                self.store.set_value_bytes_routed_expire_at_then(
                    route,
                    key.as_ref(),
                    value.clone(),
                    expire_at_ms,
                    now_ms,
                    || match direct_encode {
                        true => self.emitters.emit_borrowed_set(BorrowedSetReplication {
                            shard_id: replication_shard_id,
                            timestamp_ms: now_ms,
                            key_hash: route.key_hash,
                            key_tag: hash_key_tag_from_hash(route.key_hash),
                            key: key.as_ref(),
                            value: value.as_ref(),
                            expire_at_ms,
                            governance: None,
                        }),
                        false => self.emitters.emit(
                            replication_shard_id,
                            ReplicationMutation {
                                shard_id: replication_shard_id,
                                sequence: 0,
                                timestamp_ms: now_ms,
                                op: ReplicationMutationOp::Set,
                                key_hash: route.key_hash,
                                key_tag: hash_key_tag_from_hash(route.key_hash),
                                key: key.clone(),
                                value: value.clone(),
                                expire_at_ms,
                                governance: None,
                            },
                        ),
                    },
                );
            }
            None => {
                let timestamp_ms = ttl_now_millis();
                self.store.set_value_bytes_routed_no_ttl_then(
                    route,
                    key.as_ref(),
                    value.clone(),
                    || match direct_encode {
                        true => self.emitters.emit_borrowed_set(BorrowedSetReplication {
                            shard_id: replication_shard_id,
                            timestamp_ms,
                            key_hash: route.key_hash,
                            key_tag: hash_key_tag_from_hash(route.key_hash),
                            key: key.as_ref(),
                            value: value.as_ref(),
                            expire_at_ms: None,
                            governance: None,
                        }),
                        false => self.emitters.emit(
                            replication_shard_id,
                            ReplicationMutation {
                                shard_id: replication_shard_id,
                                sequence: 0,
                                timestamp_ms,
                                op: ReplicationMutationOp::Set,
                                key_hash: route.key_hash,
                                key_tag: hash_key_tag_from_hash(route.key_hash),
                                key: key.clone(),
                                value: value.clone(),
                                expire_at_ms: None,
                                governance: None,
                            },
                        ),
                    },
                );
            }
        }
    }

    pub fn set_with_governance(
        &self,
        key: Bytes,
        value: Bytes,
        ttl_ms: Option<u64>,
        governance: Bytes,
    ) {
        let route = self.store.route_key(&key);
        #[cfg(feature = "redis")]
        let was_vector = self.store.clone_vector_value(&key).is_some();
        #[cfg(feature = "redis")]
        if was_vector {
            self.store.delete(&key);
        }
        #[cfg(feature = "redis")]
        self.emitters.flush_vector_key(&key);
        let replication_shard_id = route.shard_id;
        let key = bytes::Bytes::from(key);
        let value = bytes::Bytes::from(value);
        let governance = bytes::Bytes::from(governance);
        let direct_encode = direct_encoded_set_enabled(value.len());
        let now_ms = ttl_now_millis();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        self.store.set_value_bytes_routed_with_governance_then(
            route,
            key.as_ref(),
            value.clone(),
            Some(governance.clone()),
            expire_at_ms,
            now_ms,
            || match direct_encode {
                true => self.emitters.emit_borrowed_set(BorrowedSetReplication {
                    shard_id: replication_shard_id,
                    timestamp_ms: now_ms,
                    key_hash: route.key_hash,
                    key_tag: hash_key_tag_from_hash(route.key_hash),
                    key: key.as_ref(),
                    value: value.as_ref(),
                    expire_at_ms,
                    governance: Some(governance.as_ref()),
                }),
                false => self.emitters.emit(
                    replication_shard_id,
                    ReplicationMutation {
                        shard_id: replication_shard_id,
                        sequence: 0,
                        timestamp_ms: now_ms,
                        op: ReplicationMutationOp::Set,
                        key_hash: route.key_hash,
                        key_tag: hash_key_tag_from_hash(route.key_hash),
                        key: key.clone(),
                        value: value.clone(),
                        expire_at_ms,
                        governance: Some(governance.clone()),
                    },
                ),
            },
        );
    }

    pub fn delete(&self, key: &[u8]) -> bool {
        #[cfg(feature = "redis")]
        if self.store.clone_vector_value(key).is_some() {
            return self.store.delete(key);
        }
        let route = self.store.route_key(key);
        let now_ms = now_millis();
        self.store.delete_routed_then(route, key, now_ms, || {
            self.emitters.emit(
                route.shard_id,
                ReplicationMutation {
                    shard_id: route.shard_id,
                    sequence: 0,
                    timestamp_ms: now_ms,
                    op: ReplicationMutationOp::Del,
                    key_hash: route.key_hash,
                    key_tag: hash_key_tag_from_hash(route.key_hash),
                    key: bytes::Bytes::copy_from_slice(key),
                    value: bytes::Bytes::new(),
                    expire_at_ms: None,
                    governance: None,
                },
            );
        })
    }

    pub fn expire(&self, key: &[u8], expire_at_ms: u64) -> bool {
        #[cfg(feature = "redis")]
        if self.store.clone_vector_value(key).is_some() {
            return self.store.expire(key, expire_at_ms);
        }
        let route = self.store.route_key(key);
        let now_ms = now_millis();
        self.store
            .expire_routed_then(route, key, expire_at_ms, now_ms, || {
                self.emitters.emit(
                    route.shard_id,
                    ReplicationMutation {
                        shard_id: route.shard_id,
                        sequence: 0,
                        timestamp_ms: now_ms,
                        op: ReplicationMutationOp::Expire,
                        key_hash: route.key_hash,
                        key_tag: hash_key_tag_from_hash(route.key_hash),
                        key: bytes::Bytes::copy_from_slice(key),
                        value: bytes::Bytes::new(),
                        expire_at_ms: Some(expire_at_ms),
                        governance: None,
                    },
                );
            })
    }

    /// Captures a consistent snapshot for replica bootstrap.
    ///
    /// Watermarks are taken first so any mutation that lands between the
    /// watermark read and the entry read is reflected in the entries. Catch-up
    /// from this watermark may re-deliver those mutations, but `apply_mutation`
    /// deduplicates by sequence so re-delivery is a no-op.
    pub fn snapshot(&self) -> ReplicationSnapshot {
        self.emitters.flush_all_and_wait();
        let watermarks = self.primary.current_watermarks();
        ReplicationSnapshot {
            entries: self.store.entry_snapshot(),
            watermarks,
        }
    }

    pub fn catch_up_replica(&self, replica: &mut ReplicationReplica) -> Result<()> {
        self.emitters.flush_all_and_wait();
        replica.ensure_source_topology(self.primary.shard_count())?;
        // Attempt backlog-only catch-up first.
        match self.primary.catch_up_since(&replica.watermarks)? {
            BacklogCatchUp::Available(frames) => {
                replica.apply_frames(&frames)?;
                replica.metrics.record_backlog_catch_up();
                return Ok(());
            }
            BacklogCatchUp::NeedsSnapshot => {}
        }

        // Fall back to a full snapshot, then drain whatever new mutations the
        // backlog has accumulated since.
        let mut attempts = 0;
        loop {
            attempts += 1;
            replica.try_replace_with_snapshot(self.snapshot())?;
            let watermarks = replica.watermarks.clone();
            match self.primary.catch_up_since(&watermarks)? {
                BacklogCatchUp::Available(frames) => {
                    replica.apply_frames(&frames)?;
                    replica.metrics.record_snapshot_catch_up();
                    return Ok(());
                }
                BacklogCatchUp::NeedsSnapshot if attempts >= MAX_SNAPSHOT_CATCH_UP_ATTEMPTS => {
                    return Err(ShardCacheError::Protocol(format!(
                        "replication backlog could not catch up after {attempts} snapshot attempts"
                    )));
                }
                BacklogCatchUp::NeedsSnapshot => {}
            }
        }
    }

    pub fn primary(&self) -> Arc<ReplicationPrimary> {
        Arc::clone(&self.primary)
    }

    /// Starts a TCP replication listener for this embedded primary.
    ///
    /// This is a convenience wrapper around [`ReplicationPrimaryServer::start`]
    /// that uses the embedded store as the consistent snapshot provider.
    pub fn serve_replicas(
        self: &Arc<Self>,
        config: ReplicationConfig,
    ) -> Result<ReplicationPrimaryServer> {
        let snapshots: Arc<dyn SnapshotProvider> = self.clone();
        ReplicationPrimaryServer::start(config, self.primary(), snapshots)
    }

    pub fn metrics_snapshot(&self) -> ReplicationMetricsSnapshot {
        self.emitters.flush_all_and_wait();
        self.primary.metrics_snapshot()
    }

    pub fn inner(&self) -> &EmbeddedStore {
        self.store.as_ref()
    }

    /// Returns the shared store used by protocol servers.
    ///
    /// Keep this `ReplicatedEmbeddedStore` alive while serving the returned
    /// handle so its replication exporters and vector mutation observer remain
    /// active.
    pub fn shared_inner(&self) -> Arc<EmbeddedStore> {
        Arc::clone(&self.store)
    }
}

fn direct_encoded_set_enabled(value_len: usize) -> bool {
    value_len <= DIRECT_ENCODED_SET_MAX_VALUE_LEN
}

impl Drop for ReplicatedEmbeddedStore {
    fn drop(&mut self) {
        #[cfg(feature = "redis")]
        self.store.configure_vector_mutation_observer(None);
        self.emitters.shutdown();
    }
}

impl ReplicatedEmbeddedEmitters {
    fn start(
        primary: Arc<ReplicationPrimary>,
        shard_count: usize,
        config: ReplicationConfig,
    ) -> Result<Arc<Self>> {
        let shard_count = shard_count.max(1);
        let exporter_stop = Arc::new(AtomicBool::new(false));
        let (flush_wake_tx, flush_wake_rx) = bounded(1);
        let mut shards = Vec::with_capacity(shard_count);
        let mut exporter_joins = Vec::with_capacity(shard_count);
        for shard_id in 0..shard_count {
            let (tx, rx) = bounded(config.queue_capacity.max(1));
            shards.push(Mutex::new(ReplicatedEmbeddedShardEmitter {
                sequence: 0,
                batch: ReplicationBatchBuilder::new_clockless(config.clone()),
                encoded_batch: EncodedReplicationBatchBuilder::new_clockless(
                    config.clone(),
                    shard_id,
                ),
                tx,
            }));
            exporter_joins.push(start_embedded_exporter(
                shard_id,
                Arc::clone(&primary),
                rx,
                Arc::clone(&exporter_stop),
            )?);
        }
        let emitters = Arc::new(Self {
            primary,
            shards,
            flusher_stop: AtomicBool::new(false),
            exporter_stop,
            flusher_join: Mutex::new(None),
            exporter_joins: Mutex::new(exporter_joins),
            flush_wake_tx,
            flush_wake_rx,
            #[cfg(feature = "redis")]
            vector_pending: Mutex::new(VectorPendingState {
                mutations: HashMap::new(),
                retained_bytes: 0,
                last_flush: Instant::now(),
            }),
            #[cfg(feature = "redis")]
            vector_pending_capacity: config.queue_capacity.max(1),
            #[cfg(feature = "redis")]
            vector_pending_max_bytes: config.vector_state_pending_max_bytes.max(1),
            #[cfg(feature = "redis")]
            vector_flush_interval: Duration::from_millis(config.vector_state_flush_ms.max(1)),
        });
        let flusher = Arc::clone(&emitters);
        let join = thread::Builder::new()
            .name("shardcache-replicated-embedded-flusher".into())
            .spawn(move || flusher.run_flusher())
            .map_err(|error| {
                ShardCacheError::Config(format!(
                    "failed to start replicated embedded flusher: {error}"
                ))
            })?;
        *emitters.flusher_join.lock() = Some(join);
        Ok(emitters)
    }

    fn emit(&self, shard_id: usize, mutation: ReplicationMutation) {
        let Some(shard) = self.shards.get(shard_id) else {
            self.primary.emit(mutation);
            return;
        };
        {
            let mut emitter = shard.lock();
            emitter.emit(mutation);
        }
        let _ = self.flush_wake_tx.try_send(());
    }

    #[cfg(feature = "redis")]
    fn queue_vector_mutation(&self, mutation: ReplicationMutation) {
        let mut forced = Vec::new();
        {
            let mut pending = self.vector_pending.lock();
            match mutation.op {
                ReplicationMutationOp::Set | ReplicationMutationOp::Del => {
                    if let Some(replaced) = pending.mutations.remove(&mutation.key) {
                        pending.retained_bytes = pending
                            .retained_bytes
                            .saturating_sub(vector_mutation_retained_bytes(&replaced));
                    }
                    let mutation_bytes = vector_mutation_retained_bytes(&mutation);
                    if mutation_bytes > self.vector_pending_max_bytes {
                        forced.push(mutation);
                    } else {
                        if pending.mutations.len() >= self.vector_pending_capacity
                            || pending.retained_bytes.saturating_add(mutation_bytes)
                                > self.vector_pending_max_bytes
                        {
                            forced.extend(pending.mutations.drain().map(|(_, mutation)| mutation));
                            pending.retained_bytes = 0;
                            pending.last_flush = Instant::now();
                        }
                        pending.retained_bytes =
                            pending.retained_bytes.saturating_add(mutation_bytes);
                        pending.mutations.insert(mutation.key.clone(), mutation);
                    }
                }
                ReplicationMutationOp::Expire => match pending.mutations.get_mut(&mutation.key) {
                    Some(existing) if existing.op == ReplicationMutationOp::Set => {
                        existing.timestamp_ms = mutation.timestamp_ms;
                        existing.expire_at_ms = mutation.expire_at_ms;
                    }
                    Some(existing) if existing.op == ReplicationMutationOp::Del => {}
                    _ => {
                        let mutation_bytes = vector_mutation_retained_bytes(&mutation);
                        if mutation_bytes > self.vector_pending_max_bytes {
                            forced.push(mutation);
                        } else {
                            if pending.mutations.len() >= self.vector_pending_capacity
                                || pending.retained_bytes.saturating_add(mutation_bytes)
                                    > self.vector_pending_max_bytes
                            {
                                forced.extend(
                                    pending.mutations.drain().map(|(_, mutation)| mutation),
                                );
                                pending.retained_bytes = 0;
                                pending.last_flush = Instant::now();
                            }
                            pending.retained_bytes =
                                pending.retained_bytes.saturating_add(mutation_bytes);
                            pending.mutations.insert(mutation.key.clone(), mutation);
                        }
                    }
                },
            }
        }
        self.emit_vector_mutations(forced);
        let _ = self.flush_wake_tx.try_send(());
    }

    #[cfg(feature = "redis")]
    fn flush_vector_key(&self, key: &[u8]) -> bool {
        let mutation = {
            let mut pending = self.vector_pending.lock();
            let mutation = pending.mutations.remove(key);
            if let Some(mutation) = mutation.as_ref() {
                pending.retained_bytes = pending
                    .retained_bytes
                    .saturating_sub(vector_mutation_retained_bytes(mutation));
            }
            mutation
        };
        if let Some(mutation) = mutation {
            self.emit(mutation.shard_id, mutation);
            true
        } else {
            false
        }
    }

    #[cfg(feature = "redis")]
    fn flush_vector_due(&self) {
        let mutations = {
            let mut pending = self.vector_pending.lock();
            if pending.last_flush.elapsed() < self.vector_flush_interval {
                return;
            }
            pending.last_flush = Instant::now();
            pending.retained_bytes = 0;
            pending
                .mutations
                .drain()
                .map(|(_, mutation)| mutation)
                .collect()
        };
        self.emit_vector_mutations(mutations);
    }

    #[cfg(feature = "redis")]
    fn flush_vector_pending(&self) {
        let mutations = {
            let mut pending = self.vector_pending.lock();
            pending.last_flush = Instant::now();
            pending.retained_bytes = 0;
            pending
                .mutations
                .drain()
                .map(|(_, mutation)| mutation)
                .collect()
        };
        self.emit_vector_mutations(mutations);
    }

    #[cfg(feature = "redis")]
    fn emit_vector_mutations(&self, mut mutations: Vec<ReplicationMutation>) {
        mutations.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        for mutation in mutations {
            self.emit(mutation.shard_id, mutation);
        }
    }

    #[cfg(feature = "redis")]
    fn shard_id_for_hash(&self, key_hash: u64) -> usize {
        stripe_index(key_hash, shift_for(self.shards.len()))
    }

    fn emit_borrowed_set(&self, set: BorrowedSetReplication<'_>) {
        let Some(shard) = self.shards.get(set.shard_id) else {
            self.primary.emit(ReplicationMutation {
                shard_id: set.shard_id,
                sequence: 0,
                timestamp_ms: set.timestamp_ms,
                op: ReplicationMutationOp::Set,
                key_hash: set.key_hash,
                key_tag: set.key_tag,
                key: bytes::Bytes::copy_from_slice(set.key),
                value: bytes::Bytes::copy_from_slice(set.value),
                expire_at_ms: set.expire_at_ms,
                governance: set.governance.map(bytes::Bytes::copy_from_slice),
            });
            return;
        };
        let mut emitter = shard.lock();
        emitter.emit_borrowed_set(set);
        let _ = self.flush_wake_tx.try_send(());
    }

    fn run_flusher(&self) {
        while !self.flusher_stop.load(Ordering::Relaxed) {
            self.flush_due();
            let timeout = self
                .next_flush_timeout()
                .unwrap_or(Duration::from_millis(100));
            match self.flush_wake_rx.recv_timeout(timeout) {
                Ok(()) | Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        self.flush_all();
    }

    fn next_flush_timeout(&self) -> Option<Duration> {
        let mut timeout = None;
        for shard in &self.shards {
            let emitter = shard.lock();
            timeout = min_timeout(timeout, emitter.next_timeout());
        }
        #[cfg(feature = "redis")]
        {
            let pending = self.vector_pending.lock();
            if !pending.mutations.is_empty() {
                let vector_timeout = self
                    .vector_flush_interval
                    .checked_sub(pending.last_flush.elapsed())
                    .unwrap_or_default();
                timeout = min_timeout(timeout, Some(vector_timeout));
            }
        }
        timeout
    }

    fn flush_due(&self) {
        #[cfg(feature = "redis")]
        self.flush_vector_due();
        for shard in &self.shards {
            if let Some(mut emitter) = shard.try_lock() {
                emitter.flush_due();
            }
        }
    }

    fn flush_all(&self) -> Vec<u64> {
        #[cfg(feature = "redis")]
        self.flush_vector_pending();
        let mut targets = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let mut emitter = shard.lock();
            emitter.flush();
            targets.push(emitter.sequence);
        }
        targets
    }

    fn flush_all_and_wait(&self) {
        let targets = self.flush_all();
        let deadline = Instant::now() + Duration::from_millis(250);
        while !replication_watermarks_reached(&self.primary, &targets) {
            if Instant::now() >= deadline {
                break;
            }
            thread::yield_now();
        }
    }

    fn shutdown(&self) {
        self.flusher_stop.store(true, Ordering::Relaxed);
        let _ = self.flush_wake_tx.try_send(());
        if let Some(join) = self.flusher_join.lock().take()
            && join.thread().id() != thread::current().id()
        {
            let _ = join.join();
        }
        self.flush_all_and_wait();
        self.exporter_stop.store(true, Ordering::Relaxed);
        for join in self.exporter_joins.lock().drain(..) {
            if join.thread().id() != thread::current().id() {
                let _ = join.join();
            }
        }
    }
}

impl ReplicatedEmbeddedShardEmitter {
    fn emit(&mut self, mut mutation: ReplicationMutation) {
        self.flush_encoded();
        self.sequence = self.sequence.saturating_add(1);
        mutation.sequence = self.sequence;
        if let Some(batch) = self.batch.push(mutation) {
            self.send_owned_batch(batch);
        }
    }

    fn emit_borrowed_set(&mut self, set: BorrowedSetReplication<'_>) {
        self.flush_owned();
        self.sequence = self.sequence.saturating_add(1);
        let mutation = BorrowedReplicationMutation {
            shard_id: self.encoded_batch.shard_id(),
            sequence: self.sequence,
            timestamp_ms: set.timestamp_ms,
            op: ReplicationMutationOp::Set,
            key_hash: set.key_hash,
            key_tag: set.key_tag,
            key: set.key,
            value: set.value,
            expire_at_ms: set.expire_at_ms,
            governance: set.governance,
        };
        if let Some(batch) = self.encoded_batch.push(mutation) {
            self.send_encoded_batch(batch);
        }
    }

    fn flush_due(&mut self) {
        self.batch.arm_clockless_delay();
        self.encoded_batch.arm_clockless_delay();
        if let Some(batch) = self.batch.flush_due() {
            self.send_owned_batch(batch);
        }
        if let Some(batch) = self.encoded_batch.flush_due() {
            self.send_encoded_batch(batch);
        }
    }

    fn next_timeout(&self) -> Option<Duration> {
        min_timeout(self.batch.next_timeout(), self.encoded_batch.next_timeout())
    }

    fn flush(&mut self) {
        self.flush_owned();
        self.flush_encoded();
    }

    fn flush_owned(&mut self) {
        if let Some(batch) = self.batch.flush() {
            self.send_owned_batch(batch);
        }
    }

    fn flush_encoded(&mut self) {
        if let Some(batch) = self.encoded_batch.flush() {
            self.send_encoded_batch(batch);
        }
    }

    fn send_owned_batch(&self, batch: ReplicationBatch) {
        self.send_batch(ReplicatedEmbeddedBatch::Owned(batch));
    }

    fn send_encoded_batch(&self, batch: EncodedReplicationBatch) {
        self.send_batch(ReplicatedEmbeddedBatch::Encoded(batch));
    }

    fn send_batch(&self, batch: ReplicatedEmbeddedBatch) {
        if self.tx.send(batch).is_err() {
            tracing::warn!("dropping replicated embedded batch because exporter stopped");
        }
    }
}

fn min_timeout(left: Option<Duration>, right: Option<Duration>) -> Option<Duration> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(timeout), None) | (None, Some(timeout)) => Some(timeout),
        (None, None) => None,
    }
}

#[cfg(feature = "redis")]
fn vector_mutation_retained_bytes(mutation: &ReplicationMutation) -> usize {
    mutation
        .key
        .len()
        .saturating_add(mutation.value.len())
        .saturating_add(mutation.governance.as_ref().map_or(0, bytes::Bytes::len))
}

fn start_embedded_exporter(
    shard_id: usize,
    primary: Arc<ReplicationPrimary>,
    rx: Receiver<ReplicatedEmbeddedBatch>,
    stop: Arc<AtomicBool>,
) -> Result<JoinHandle<()>> {
    thread::Builder::new()
        .name(format!("shardcache-replicated-embedded-export-{shard_id}"))
        .spawn(move || run_embedded_exporter(primary, rx, stop))
        .map_err(|error| {
            ShardCacheError::Config(format!(
                "failed to start replicated embedded exporter {shard_id}: {error}"
            ))
        })
}

fn run_embedded_exporter(
    primary: Arc<ReplicationPrimary>,
    rx: Receiver<ReplicatedEmbeddedBatch>,
    stop: Arc<AtomicBool>,
) {
    loop {
        match rx.recv_timeout(Duration::from_millis(1)) {
            Ok(batch) => {
                export_embedded_batch(&primary, batch);
                while let Ok(batch) = rx.try_recv() {
                    export_embedded_batch(&primary, batch);
                }
            }
            Err(RecvTimeoutError::Timeout) if stop.load(Ordering::Relaxed) && rx.is_empty() => {
                break;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    while let Ok(batch) = rx.try_recv() {
        export_embedded_batch(&primary, batch);
    }
}

fn export_embedded_batch(primary: &ReplicationPrimary, batch: ReplicatedEmbeddedBatch) {
    match batch {
        ReplicatedEmbeddedBatch::Owned(batch) => primary.export_batch_direct(batch),
        ReplicatedEmbeddedBatch::Encoded(batch) => primary.export_encoded_batch_direct(batch),
    }
}

fn replication_watermarks_reached(primary: &ReplicationPrimary, targets: &[u64]) -> bool {
    let watermarks = primary.current_watermarks();
    targets
        .iter()
        .enumerate()
        .all(|(shard_id, target)| watermarks.get(shard_id) >= *target)
}

const MAX_SNAPSHOT_CATCH_UP_ATTEMPTS: usize = 4;

impl ReplicationReplica {
    pub fn new(shard_count: usize) -> Self {
        Self::with_route_mode(shard_count, EmbeddedRouteMode::FullKey)
    }

    pub fn with_route_mode(shard_count: usize, route_mode: EmbeddedRouteMode) -> Self {
        Self {
            store: EmbeddedStore::with_route_mode(shard_count, route_mode),
            watermarks: ShardWatermarks::new(shard_count),
            physical_topology_initialized: true,
            source_shard_count: None,
            metrics: ReplicationMetrics::default(),
        }
    }

    pub(crate) fn uninitialized() -> Self {
        Self {
            store: EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::FullKey),
            watermarks: ShardWatermarks::new(0),
            physical_topology_initialized: false,
            source_shard_count: None,
            metrics: ReplicationMetrics::default(),
        }
    }

    pub(crate) fn ensure_topology(&mut self, shard_count: usize) -> Result<()> {
        if shard_count == 0 || !shard_count.is_power_of_two() {
            return Err(ShardCacheError::Protocol(format!(
                "replication primary advertised invalid shard count {shard_count}"
            )));
        }
        if !self.physical_topology_initialized {
            let route_mode = self.store.route_mode();
            self.store = EmbeddedStore::with_route_mode(shard_count, route_mode);
            self.physical_topology_initialized = true;
        }
        self.ensure_source_topology(shard_count)
    }

    fn ensure_source_topology(&mut self, shard_count: usize) -> Result<()> {
        self.validate_source_topology(shard_count)?;
        if self.watermarks.as_slice().len() != shard_count {
            self.watermarks = ShardWatermarks::new(shard_count);
        }
        self.source_shard_count = Some(shard_count);
        Ok(())
    }

    fn validate_source_topology(&self, shard_count: usize) -> Result<()> {
        if shard_count == 0 || !shard_count.is_power_of_two() {
            return Err(ShardCacheError::Protocol(format!(
                "replication primary advertised invalid shard count {shard_count}"
            )));
        }
        if let Some(current) = self.source_shard_count {
            if current != shard_count || self.watermarks.as_slice().len() != shard_count {
                return Err(ShardCacheError::Protocol(format!(
                    "replication source topology changed from {current} to {shard_count} shards"
                )));
            }
            return Ok(());
        }
        if self.watermarks.as_slice().len() != shard_count
            && self
                .watermarks
                .as_slice()
                .iter()
                .any(|watermark| *watermark != 0)
        {
            return Err(ShardCacheError::Protocol(
                "cannot change replication source topology after applying mutations".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn topology_initialized(&self) -> bool {
        self.source_shard_count.is_some()
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.store.get(key)
    }

    pub fn watermarks(&self) -> &ShardWatermarks {
        &self.watermarks
    }

    pub fn metrics_snapshot(&self) -> ReplicationMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn apply_frame_bytes(&mut self, frame: &[u8]) -> Result<()> {
        let frame = decode_frame_payload(frame)?;
        self.apply_frame_payload(frame)
    }

    pub fn apply_frame(&mut self, frame: ReplicationFrameBytes) -> Result<()> {
        let frame = decode_frame_payload_bytes(frame)?;
        self.apply_frame_bytes_payload(frame)
    }

    pub fn apply_frames(&mut self, frames: &[ReplicationFrameBytes]) -> Result<()> {
        for frame in frames {
            self.apply_frame(frame.clone())?;
        }
        Ok(())
    }

    pub fn apply_decoded_frame(&mut self, frame: super::protocol::ReplicationFrame) -> Result<()> {
        match frame.kind {
            FrameKind::MutationBatch => {
                self.apply_owned_mutation_batch_payload(bytes::Bytes::from(frame.payload))
            }
            other => Err(ShardCacheError::Protocol(format!(
                "replica cannot apply FCRP frame kind: {other:?}"
            ))),
        }
    }

    pub fn apply_frame_payload(&mut self, frame: ReplicationFramePayload<'_>) -> Result<()> {
        match frame.kind {
            FrameKind::MutationBatch => self.apply_mutation_batch_payload(frame.payload.as_ref()),
            other => Err(ShardCacheError::Protocol(format!(
                "replica cannot apply FCRP frame kind: {other:?}"
            ))),
        }
    }

    pub fn apply_frame_bytes_payload(&mut self, frame: ReplicationFrameBytesPayload) -> Result<()> {
        match frame.kind {
            FrameKind::MutationBatch => self.apply_owned_mutation_batch_payload(frame.payload),
            other => Err(ShardCacheError::Protocol(format!(
                "replica cannot apply FCRP frame kind: {other:?}"
            ))),
        }
    }

    fn apply_owned_mutation_batch_payload(&mut self, payload: bytes::Bytes) -> Result<()> {
        match mutation_batch_record_count(payload.as_ref())? {
            1 => self.apply_mutation_batch_payload_bytes(payload),
            _ => self.apply_mutation_batch_payload(payload.as_ref()),
        }
    }

    fn apply_mutation_batch_payload(&mut self, payload: &[u8]) -> Result<()> {
        let started = Instant::now();
        let mut now_ms = None;
        let mut applied = 0_u64;
        let mut skipped = 0_u64;
        visit_mutation_batch_payload(payload, |mutation| {
            if self.apply_borrowed_mutation_inner(mutation, &mut now_ms)? {
                applied += 1;
            } else {
                skipped += 1;
            }
            Ok(())
        })?;
        self.metrics.record_replica_apply_batch(
            applied,
            skipped,
            started.elapsed().as_nanos() as u64,
        );
        Ok(())
    }

    fn apply_mutation_batch_payload_bytes(&mut self, payload: bytes::Bytes) -> Result<()> {
        let started = Instant::now();
        let mut now_ms = None;
        let mut applied = 0_u64;
        let mut skipped = 0_u64;
        visit_mutation_batch_payload_bytes(payload, |mutation| {
            if self.apply_frame_backed_mutation_inner(mutation, &mut now_ms)? {
                applied += 1;
            } else {
                skipped += 1;
            }
            Ok(())
        })?;
        self.metrics.record_replica_apply_batch(
            applied,
            skipped,
            started.elapsed().as_nanos() as u64,
        );
        Ok(())
    }

    pub fn apply_mutation(&mut self, mutation: ReplicationMutation) {
        let _ = self.try_apply_mutation(mutation);
    }

    pub fn try_apply_mutation(&mut self, mutation: ReplicationMutation) -> Result<()> {
        let started = Instant::now();
        let applied = self.apply_mutation_inner(mutation, &mut None)?;
        self.metrics
            .record_replica_apply(applied, started.elapsed().as_nanos() as u64);
        Ok(())
    }

    fn apply_mutation_inner(
        &mut self,
        mutation: ReplicationMutation,
        now_ms: &mut Option<u64>,
    ) -> Result<bool> {
        if !self.validate_mutation(
            mutation.shard_id,
            mutation.sequence,
            mutation.key_hash,
            Some(mutation.key_tag),
            mutation.key.as_ref(),
        )? {
            return Ok(false);
        }

        match mutation.op {
            ReplicationMutationOp::Set => {
                self.apply_set(
                    mutation.key_hash,
                    mutation.key.as_ref(),
                    mutation.value,
                    mutation.expire_at_ms,
                    mutation.governance,
                    now_ms,
                )?;
            }
            ReplicationMutationOp::Del => {
                self.store.delete(&mutation.key);
            }
            ReplicationMutationOp::Expire => match mutation.expire_at_ms {
                Some(expire_at_ms) => {
                    self.store.expire(&mutation.key, expire_at_ms);
                }
                None => {
                    self.store.persist(&mutation.key);
                }
            },
        }
        self.watermarks
            .observe(mutation.shard_id, mutation.sequence);
        Ok(true)
    }

    fn apply_frame_backed_mutation_inner(
        &mut self,
        mutation: FrameBackedReplicationMutation<'_>,
        now_ms: &mut Option<u64>,
    ) -> Result<bool> {
        if !self.validate_mutation(
            mutation.shard_id,
            mutation.sequence,
            mutation.key_hash,
            Some(mutation.key_tag),
            mutation.key,
        )? {
            return Ok(false);
        }

        match mutation.op {
            ReplicationMutationOp::Set => {
                self.apply_set(
                    mutation.key_hash,
                    mutation.key,
                    mutation.value,
                    mutation.expire_at_ms,
                    mutation.governance,
                    now_ms,
                )?;
            }
            ReplicationMutationOp::Del => {
                self.store.delete(mutation.key);
            }
            ReplicationMutationOp::Expire => match mutation.expire_at_ms {
                Some(expire_at_ms) => {
                    self.store.expire(mutation.key, expire_at_ms);
                }
                None => {
                    self.store.persist(mutation.key);
                }
            },
        }
        self.watermarks
            .observe(mutation.shard_id, mutation.sequence);
        Ok(true)
    }

    fn apply_borrowed_mutation_inner(
        &mut self,
        mutation: BorrowedReplicationMutation<'_>,
        now_ms: &mut Option<u64>,
    ) -> Result<bool> {
        if !self.validate_mutation(
            mutation.shard_id,
            mutation.sequence,
            mutation.key_hash,
            Some(mutation.key_tag),
            mutation.key,
        )? {
            return Ok(false);
        }

        match mutation.op {
            ReplicationMutationOp::Set => {
                self.apply_set(
                    mutation.key_hash,
                    mutation.key,
                    bytes::Bytes::copy_from_slice(mutation.value),
                    mutation.expire_at_ms,
                    mutation.governance.map(bytes::Bytes::copy_from_slice),
                    now_ms,
                )?;
            }
            ReplicationMutationOp::Del => {
                self.store.delete(mutation.key);
            }
            ReplicationMutationOp::Expire => match mutation.expire_at_ms {
                Some(expire_at_ms) => {
                    self.store.expire(mutation.key, expire_at_ms);
                }
                None => {
                    self.store.persist(mutation.key);
                }
            },
        }
        self.watermarks
            .observe(mutation.shard_id, mutation.sequence);
        Ok(true)
    }

    fn apply_set(
        &mut self,
        key_hash: u64,
        key: &[u8],
        value: bytes::Bytes,
        expire_at_ms: Option<u64>,
        governance: Option<bytes::Bytes>,
        now_ms: &mut Option<u64>,
    ) -> Result<()> {
        #[cfg(feature = "redis")]
        let is_vector = value.starts_with(crate::storage::VECTOR_SET_PREFIX);
        #[cfg(feature = "redis")]
        let route = if is_vector {
            crate::commands::vector_set::validate_vector_set_bytes(&value).map_err(|_| {
                ShardCacheError::Protocol("replica rejected malformed vector-set state".into())
            })?;
            let primary_route = self.store.route_key_prehashed(key_hash, key);
            let vector_route = self.store.route_vector_key(key);
            if primary_route.shard_id != vector_route.shard_id {
                self.store
                    .delete_routed_then(primary_route, key, now_millis(), || {});
            }
            vector_route
        } else {
            let vector_route = self.store.route_vector_key(key);
            if self.store.clone_vector_value(key).is_some() {
                self.store
                    .delete_routed_then(vector_route, key, now_millis(), || {});
            }
            self.store.route_key_prehashed(key_hash, key)
        };
        #[cfg(not(feature = "redis"))]
        let route = self.store.route_key_prehashed(key_hash, key);
        match expire_at_ms {
            Some(expire_at_ms) => {
                let now_ms = *now_ms.get_or_insert_with(now_millis);
                if expire_at_ms <= now_ms {
                    return Ok(());
                }
                self.store.set_value_bytes_routed_expire_at_with_governance(
                    route,
                    key,
                    value,
                    governance,
                    Some(expire_at_ms),
                    now_ms,
                );
            }
            None => self.store.set_value_bytes_routed_with_governance_then(
                route,
                key,
                value,
                governance,
                None,
                now_millis(),
                || {},
            ),
        }
        Ok(())
    }

    fn validate_mutation(
        &self,
        shard_id: usize,
        sequence: u64,
        key_hash: u64,
        key_tag: Option<u64>,
        key: &[u8],
    ) -> Result<bool> {
        if !self.physical_topology_initialized {
            return Err(ShardCacheError::Protocol(
                "replication mutation arrived before topology negotiation".into(),
            ));
        }
        let shard_count = self.watermarks.as_slice().len();
        if shard_id >= shard_count {
            return Err(ShardCacheError::Protocol(format!(
                "replication mutation shard {shard_id} exceeds topology size {shard_count}"
            )));
        }
        let actual_hash = crate::storage::hash_key(key);
        if key_hash != actual_hash {
            return Err(ShardCacheError::Protocol(
                "replication mutation key hash does not match key bytes".into(),
            ));
        }
        if key_tag.is_some_and(|tag| tag != hash_key_tag_from_hash(actual_hash)) {
            return Err(ShardCacheError::Protocol(
                "replication mutation key tag does not match key hash".into(),
            ));
        }
        if self.source_shard_count.is_some() {
            let expected_shard = stripe_index(actual_hash, shift_for(shard_count));
            if shard_id != expected_shard {
                return Err(ShardCacheError::Protocol(format!(
                    "replication mutation shard {shard_id} does not own key; expected shard {expected_shard}"
                )));
            }
        }

        let current = self.watermarks.get(shard_id);
        if sequence <= current {
            return Ok(false);
        }
        let expected = current.checked_add(1).ok_or_else(|| {
            ShardCacheError::Protocol("replication sequence exhausted u64 range".into())
        })?;
        if sequence != expected {
            return Err(ShardCacheError::Protocol(format!(
                "replication sequence gap on shard {shard_id}: expected {expected}, received {sequence}"
            )));
        }
        Ok(true)
    }

    pub fn replace_with_snapshot(&mut self, snapshot: ReplicationSnapshot) {
        if let Err(error) = self.try_replace_with_snapshot(snapshot) {
            tracing::warn!(%error, "replication snapshot replacement rejected");
        }
    }

    pub fn try_replace_with_snapshot(&mut self, mut snapshot: ReplicationSnapshot) -> Result<()> {
        let source_shard_count = snapshot.watermarks.as_slice().len();
        if !self.physical_topology_initialized {
            return Err(ShardCacheError::Protocol(
                "replication snapshot arrived before topology negotiation".into(),
            ));
        }
        self.validate_source_topology(source_shard_count)?;
        snapshot
            .entries
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if snapshot
            .entries
            .windows(2)
            .any(|entries| entries[0].key == entries[1].key)
        {
            return Err(ShardCacheError::Protocol(
                "replication snapshot contains duplicate logical keys".into(),
            ));
        }
        #[cfg(feature = "redis")]
        for entry in &snapshot.entries {
            if entry.value.starts_with(crate::storage::VECTOR_SET_PREFIX) {
                crate::commands::vector_set::validate_vector_set_bytes(&entry.value).map_err(
                    |_| {
                        ShardCacheError::Protocol(
                            "replica rejected malformed vector-set snapshot state".into(),
                        )
                    },
                )?;
            }
        }
        let route_mode = self.store.route_mode();
        let shard_count = self.store.shard_count();
        let store = EmbeddedStore::with_route_mode(shard_count, route_mode);
        store.restore_entries(snapshot.entries);
        self.store = store;
        self.watermarks = snapshot.watermarks;
        self.source_shard_count = Some(source_shard_count);
        Ok(())
    }

    pub fn inner(&self) -> &EmbeddedStore {
        &self.store
    }
}

impl From<&ReplicationMutation> for MutationOp {
    fn from(value: &ReplicationMutation) -> Self {
        match value.op {
            ReplicationMutationOp::Set => MutationOp::Set,
            ReplicationMutationOp::Del => MutationOp::Del,
            ReplicationMutationOp::Expire => MutationOp::Expire,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    #[cfg(feature = "redis")]
    use crate::commands::redis::RedisCommand;
    #[cfg(feature = "redis")]
    use crate::commands::vector_set::{VAdd, VCard, VSim};
    #[cfg(feature = "redis")]
    use crate::commands::{copy::Copy, rename::Rename};
    use crate::config::{ReplicationCompression, ReplicationConfig, ReplicationSendPolicy};
    #[cfg(feature = "redis")]
    use crate::protocol::Frame;
    use crate::storage::StoredEntry;

    use super::*;

    fn config(send_policy: ReplicationSendPolicy) -> ReplicationConfig {
        ReplicationConfig {
            enabled: true,
            compression: ReplicationCompression::None,
            send_policy,
            batch_max_records: 2,
            batch_max_delay_us: 1_000,
            ..ReplicationConfig::default()
        }
    }

    #[cfg(feature = "redis")]
    fn vector_key_outside_shard_zero(store: &EmbeddedStore) -> Vec<u8> {
        (0_u64..)
            .map(|index| format!("vectors:{index}").into_bytes())
            .find(|key| store.route_key(key).shard_id != store.vector_shard_id())
            .expect("key outside vector shard")
    }

    #[cfg(feature = "redis")]
    fn add_vector(store: &EmbeddedStore, key: &[u8], element: &[u8]) {
        assert_eq!(
            VAdd::execute(store, &[key, b"VALUES", b"2", b"1", b"0", element]),
            Frame::Integer(1)
        );
    }

    #[cfg(feature = "redis")]
    fn vector_governance(store: &EmbeddedStore, key: &[u8]) -> Option<Vec<u8>> {
        match VSim::execute(
            store,
            &[
                key,
                b"VALUES",
                b"2",
                b"1",
                b"0",
                b"COUNT",
                b"1",
                b"WITHGOVERNANCE",
                b"GOVERNANCE",
                b"tenant=acme",
                b"TRUTH",
            ],
        ) {
            Frame::Array(items) => match items.get(1) {
                Some(Frame::BlobString(governance)) => Some(governance.clone()),
                Some(Frame::Null) | None => None,
                frame => panic!("unexpected vector governance response: {frame:?}"),
            },
            frame => panic!("unexpected VSIM response: {frame:?}"),
        }
    }

    #[cfg(feature = "redis")]
    fn vector_count(store: &EmbeddedStore, key: &[u8]) -> i64 {
        match VCard::execute(store, &[key]) {
            Frame::Integer(count) => count,
            frame => panic!("unexpected VCARD response: {frame:?}"),
        }
    }

    #[test]
    fn embedded_replica_applies_immediate_mutation() {
        let primary = ReplicatedEmbeddedStore::new(4, config(ReplicationSendPolicy::Immediate))
            .expect("primary");
        let mut replica = ReplicationReplica::new(4);
        let subscriber = primary.primary().subscribe(8);
        primary.set(b"alpha".to_vec(), b"one".to_vec(), None);
        let frame = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("replication frame");
        replica.apply_frame(frame.clone()).expect("apply");
        assert_eq!(replica.get(b"alpha"), Some(b"one".to_vec()));
        let stored = replica
            .inner()
            .get_value_bytes(b"alpha")
            .expect("stored value");
        assert!(bytes_points_inside(&stored, &frame));
    }

    #[cfg(feature = "redis")]
    #[test]
    fn vector_mutations_use_the_key_shard_sequence_and_pinned_storage() {
        let primary = ReplicatedEmbeddedStore::new(4, config(ReplicationSendPolicy::Immediate))
            .expect("primary");
        let mut replica = ReplicationReplica::new(4);
        let subscriber = primary.primary().subscribe(8);
        let key = vector_key_outside_shard_zero(primary.inner());

        assert_eq!(
            VAdd::execute(
                primary.inner(),
                &[
                    &key,
                    b"VALUES",
                    b"2",
                    b"1",
                    b"0",
                    b"document-1",
                    b"GOVERNANCE",
                    b"tenant=acme",
                ],
            ),
            Frame::Integer(1)
        );
        let frame = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("vector replication frame");
        let source_shard = primary.inner().route_key(&key).shard_id;
        assert_ne!(source_shard, primary.inner().vector_shard_id());
        assert_eq!(primary.primary().current_watermarks().get(source_shard), 1);
        assert_eq!(primary.primary().current_watermarks().get(0), 0);
        replica.apply_frame(frame).expect("apply vector mutation");

        assert_eq!(vector_count(replica.inner(), &key), 1);
        assert_eq!(
            vector_governance(replica.inner(), &key).as_deref(),
            Some(b"tenant=acme".as_slice())
        );
        assert_eq!(replica.inner().route_vector_key(&key).shard_id, 0);

        let expire_at_ms = now_millis().saturating_add(60_000);
        assert!(primary.inner().expire(&key, expire_at_ms));
        let frame = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("vector expiry frame");
        replica.apply_frame(frame).expect("apply vector expiry");
        assert!(replica.inner().pttl_millis(&key) > 0);

        assert!(primary.inner().delete(&key));
        let frame = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("vector delete frame");
        replica.apply_frame(frame).expect("apply vector delete");
        assert_eq!(vector_count(replica.inner(), &key), 0);
    }

    #[cfg(feature = "redis")]
    #[test]
    fn vector_snapshot_restore_preserves_the_pinned_route() {
        let primary = ReplicatedEmbeddedStore::new(4, config(ReplicationSendPolicy::Immediate))
            .expect("primary");
        let key = vector_key_outside_shard_zero(primary.inner());
        add_vector(primary.inner(), &key, b"document-1");

        let mut replica = ReplicationReplica::new(4);
        replica.replace_with_snapshot(primary.snapshot());

        assert_eq!(vector_count(replica.inner(), &key), 1);
        assert!(replica.inner().clone_vector_value(&key).is_some());
    }

    #[cfg(feature = "redis")]
    #[test]
    fn malformed_replicated_vector_state_is_rejected_without_advancing_watermark() {
        let source = EmbeddedStore::new(1);
        add_vector(&source, b"vectors", b"document-1");
        let mut malformed = source
            .clone_vector_value(b"vectors")
            .expect("canonical vector state")
            .to_vec();
        malformed.pop();

        let key_hash = crate::storage::hash_key(b"vectors");
        let mut replica = ReplicationReplica::new(1);
        let mutation = ReplicationMutation {
            shard_id: 0,
            sequence: 1,
            timestamp_ms: now_millis(),
            op: ReplicationMutationOp::Set,
            key_hash,
            key_tag: hash_key_tag_from_hash(key_hash),
            key: bytes::Bytes::from_static(b"vectors"),
            value: bytes::Bytes::from(malformed.clone()),
            expire_at_ms: None,
            governance: None,
        };
        assert!(replica.try_apply_mutation(mutation).is_err());
        assert_eq!(replica.watermarks().get(0), 0);
        assert!(replica.inner().clone_vector_value(b"vectors").is_none());

        replica
            .inner()
            .set_value_bytes(b"sentinel", bytes::Bytes::from_static(b"preserved"), None);
        let snapshot = ReplicationSnapshot {
            entries: vec![StoredEntry {
                key: b"vectors".to_vec(),
                value: malformed,
                expire_at_ms: None,
                governance: None,
            }],
            watermarks: ShardWatermarks::from_vec(vec![1]),
        };
        assert!(replica.try_replace_with_snapshot(snapshot).is_err());
        assert_eq!(replica.get(b"sentinel"), Some(b"preserved".to_vec()));
        assert_eq!(replica.watermarks().get(0), 0);
    }

    #[test]
    fn apply_mutation_preserves_the_infallible_public_signature() {
        let _: fn(&mut ReplicationReplica, ReplicationMutation) =
            ReplicationReplica::apply_mutation;
    }

    #[test]
    fn replace_with_snapshot_preserves_state_on_invalid_input() {
        let mut replica = ReplicationReplica::new(1);
        replica
            .inner()
            .set_value_bytes(b"sentinel", bytes::Bytes::from_static(b"preserved"), None);
        let duplicate = StoredEntry {
            key: b"duplicate".to_vec(),
            value: b"one".to_vec(),
            expire_at_ms: None,
            governance: None,
        };
        replica.replace_with_snapshot(ReplicationSnapshot {
            entries: vec![duplicate.clone(), duplicate],
            watermarks: ShardWatermarks::from_vec(vec![0]),
        });

        assert_eq!(replica.get(b"sentinel"), Some(b"preserved".to_vec()));
        assert!(!replica.topology_initialized());
    }

    #[test]
    fn replica_rejects_malformed_mutation_identity_and_sequence_without_state_change() {
        let mut replica = ReplicationReplica::new(4);
        let key = bytes::Bytes::from_static(b"identity-key");
        let key_hash = crate::storage::hash_key(&key);
        let mutation = ReplicationMutation {
            shard_id: 4,
            sequence: 1,
            timestamp_ms: 1,
            op: ReplicationMutationOp::Set,
            key_hash,
            key_tag: hash_key_tag_from_hash(key_hash),
            key: key.clone(),
            value: bytes::Bytes::from_static(b"value"),
            expire_at_ms: None,
            governance: None,
        };
        assert!(replica.try_apply_mutation(mutation.clone()).is_err());
        assert_eq!(replica.watermarks().as_slice().len(), 4);

        let mut bad_hash = mutation.clone();
        bad_hash.shard_id = 0;
        bad_hash.key_hash ^= 1;
        assert!(replica.try_apply_mutation(bad_hash).is_err());

        let mut bad_tag = mutation.clone();
        bad_tag.shard_id = 0;
        bad_tag.key_tag ^= 1;
        assert!(replica.try_apply_mutation(bad_tag).is_err());

        let mut gap = mutation;
        gap.shard_id = 0;
        gap.sequence = 2;
        assert!(replica.try_apply_mutation(gap).is_err());
        assert_eq!(replica.watermarks().get(0), 0);
        assert_eq!(replica.get(&key), None);
    }

    #[test]
    fn snapshot_rejects_duplicate_keys_and_wrong_topology_atomically() {
        let mut replica = ReplicationReplica::new(2);
        let existing = ReplicationMutation {
            shard_id: 0,
            sequence: 1,
            timestamp_ms: 1,
            op: ReplicationMutationOp::Set,
            key_hash: crate::storage::hash_key(b"existing"),
            key_tag: crate::storage::hash_key_tag(b"existing"),
            key: bytes::Bytes::from_static(b"existing"),
            value: bytes::Bytes::from_static(b"safe"),
            expire_at_ms: None,
            governance: None,
        };
        replica.try_apply_mutation(existing).unwrap();

        let duplicate = StoredEntry {
            key: b"duplicate".to_vec(),
            value: b"one".to_vec(),
            expire_at_ms: None,
            governance: None,
        };
        let snapshot = ReplicationSnapshot {
            entries: vec![
                duplicate.clone(),
                StoredEntry {
                    value: b"two".to_vec(),
                    ..duplicate
                },
            ],
            watermarks: ShardWatermarks::from_vec(vec![1, 0]),
        };
        assert!(replica.try_replace_with_snapshot(snapshot).is_err());
        assert_eq!(replica.get(b"existing"), Some(b"safe".to_vec()));

        let wrong_topology = ReplicationSnapshot {
            entries: Vec::new(),
            watermarks: ShardWatermarks::from_vec(vec![1]),
        };
        assert!(replica.try_replace_with_snapshot(wrong_topology).is_err());
        assert_eq!(replica.get(b"existing"), Some(b"safe".to_vec()));
    }

    #[test]
    fn network_replica_topology_is_initialized_once() {
        let mut replica = ReplicationReplica::uninitialized();
        assert!(!replica.topology_initialized());
        replica.ensure_topology(8).unwrap();
        assert_eq!(replica.inner().shard_count(), 8);
        assert_eq!(replica.watermarks().as_slice().len(), 8);
        assert!(replica.ensure_topology(4).is_err());
    }

    #[test]
    fn negotiated_replica_rejects_an_in_range_non_owner_shard() {
        let mut replica = ReplicationReplica::new(4);
        replica.ensure_source_topology(4).unwrap();
        let key = bytes::Bytes::from_static(b"owner-check");
        let key_hash = crate::storage::hash_key(&key);
        let owner = stripe_index(key_hash, shift_for(4));
        let mutation = ReplicationMutation {
            shard_id: (owner + 1) % 4,
            sequence: 1,
            timestamp_ms: 1,
            op: ReplicationMutationOp::Set,
            key_hash,
            key_tag: hash_key_tag_from_hash(key_hash),
            key: key.clone(),
            value: bytes::Bytes::from_static(b"value"),
            expire_at_ms: None,
            governance: None,
        };

        assert!(replica.try_apply_mutation(mutation).is_err());
        assert_eq!(replica.get(&key), None);
        assert_eq!(replica.watermarks().get(owner), 0);
    }

    #[cfg(feature = "redis")]
    #[test]
    fn vector_updates_coalesce_to_the_latest_snapshot_state() {
        let mut replication = config(ReplicationSendPolicy::Immediate);
        replication.vector_state_flush_ms = 60_000;
        let primary = ReplicatedEmbeddedStore::new(4, replication).expect("primary");
        let subscriber = primary.primary().subscribe(8);
        let key = vector_key_outside_shard_zero(primary.inner());

        add_vector(primary.inner(), &key, b"document-1");
        add_vector(primary.inner(), &key, b"document-2");
        add_vector(primary.inner(), &key, b"document-3");
        assert!(subscriber.try_recv().is_err());

        let snapshot = primary.snapshot();
        let mut replica = ReplicationReplica::new(4);
        replica
            .apply_frame(
                subscriber
                    .recv_timeout(Duration::from_secs(2))
                    .expect("coalesced vector frame"),
            )
            .expect("apply coalesced vector state");
        assert!(subscriber.try_recv().is_err());
        assert_eq!(vector_count(replica.inner(), &key), 3);

        let mut snapshot_replica = ReplicationReplica::new(4);
        snapshot_replica.replace_with_snapshot(snapshot);
        assert_eq!(vector_count(snapshot_replica.inner(), &key), 3);
    }

    #[cfg(feature = "redis")]
    #[test]
    fn oversized_vector_state_bypasses_the_bounded_coalescer() {
        let mut replication = config(ReplicationSendPolicy::Immediate);
        replication.vector_state_flush_ms = 60_000;
        replication.vector_state_pending_max_bytes = 1;
        let primary = ReplicatedEmbeddedStore::new(4, replication).expect("primary");
        let subscriber = primary.primary().subscribe(8);
        let key = vector_key_outside_shard_zero(primary.inner());

        add_vector(primary.inner(), &key, b"document-1");
        let frame = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("oversized vector frame");
        let mut replica = ReplicationReplica::new(4);
        replica.apply_frame(frame).expect("apply vector state");

        assert_eq!(vector_count(replica.inner(), &key), 1);
        assert!(primary.emitters.vector_pending.lock().mutations.is_empty());
    }

    #[cfg(feature = "redis")]
    #[test]
    fn vector_update_after_expire_preserves_the_absolute_deadline() {
        let mut replication = config(ReplicationSendPolicy::Immediate);
        replication.vector_state_flush_ms = 60_000;
        let primary = ReplicatedEmbeddedStore::new(4, replication).expect("primary");
        let subscriber = primary.primary().subscribe(8);
        let key = vector_key_outside_shard_zero(primary.inner());

        add_vector(primary.inner(), &key, b"document-1");
        let expire_at_ms = now_millis().saturating_add(60_000);
        assert!(primary.inner().expire(&key, expire_at_ms));
        add_vector(primary.inner(), &key, b"document-2");

        let mut replica = ReplicationReplica::new(4);
        let _snapshot = primary.snapshot();
        replica
            .apply_frame(
                subscriber
                    .recv_timeout(Duration::from_secs(2))
                    .expect("coalesced vector frame"),
            )
            .expect("apply vector state");

        assert_eq!(vector_count(replica.inner(), &key), 2);
        assert!(replica.inner().pttl_millis(&key) > 0);
    }

    #[cfg(feature = "redis")]
    #[test]
    fn vector_copy_and_rename_replicate_state_and_ttl() {
        let mut replication = config(ReplicationSendPolicy::Immediate);
        replication.vector_state_flush_ms = 60_000;
        let primary = ReplicatedEmbeddedStore::new(4, replication).expect("primary");
        let subscriber = primary.primary().subscribe(8);
        let source = (0_u64..)
            .map(|index| format!("vector-shard-zero:{index}").into_bytes())
            .find(|key| primary.inner().route_key(key).shard_id == 0)
            .expect("key on vector shard");
        let copied = vector_key_outside_shard_zero(primary.inner());
        let moved = b"vector-moved".to_vec();

        assert_eq!(
            VAdd::execute(
                primary.inner(),
                &[
                    &source,
                    b"VALUES",
                    b"2",
                    b"1",
                    b"0",
                    b"document-1",
                    b"GOVERNANCE",
                    b"tenant=acme",
                ],
            ),
            Frame::Integer(1)
        );
        let expire_at_ms = now_millis().saturating_add(60_000);
        assert!(primary.inner().expire(&source, expire_at_ms));
        assert_eq!(
            Copy::execute(primary.inner(), &[&source, &copied]),
            Frame::Integer(1)
        );
        assert_eq!(
            Rename::execute(primary.inner(), &[&copied, &moved]),
            Frame::SimpleString("OK".to_string())
        );
        let _snapshot = primary.snapshot();

        let mut replica = ReplicationReplica::new(4);
        replica
            .inner()
            .set_value_bytes(&moved, bytes::Bytes::from_static(b"stale"), None);
        let first = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("first lifecycle frame");
        replica
            .apply_frame(first)
            .expect("apply first lifecycle frame");
        while let Ok(frame) = subscriber.try_recv() {
            replica.apply_frame(frame).expect("apply lifecycle frame");
        }

        assert_eq!(vector_count(replica.inner(), &source), 1);
        assert_eq!(vector_count(replica.inner(), &copied), 0);
        assert_eq!(vector_count(replica.inner(), &moved), 1);
        assert_eq!(
            vector_governance(replica.inner(), &moved).as_deref(),
            Some(b"tenant=acme".as_slice())
        );
        assert!(replica.inner().pttl_millis(&moved) > 0);
        assert!(replica.inner().get_value_bytes(&moved).is_none());
    }

    #[cfg(feature = "redis")]
    #[test]
    fn vector_to_string_replacement_keeps_one_ordering_stream() {
        let primary = ReplicatedEmbeddedStore::new(4, config(ReplicationSendPolicy::Immediate))
            .expect("primary");
        let mut replica = ReplicationReplica::new(4);
        let subscriber = primary.primary().subscribe(8);
        let key = vector_key_outside_shard_zero(primary.inner());

        add_vector(primary.inner(), &key, b"document-1");
        replica
            .apply_frame(
                subscriber
                    .recv_timeout(Duration::from_secs(2))
                    .expect("vector set frame"),
            )
            .expect("apply vector set");

        primary.set(key.clone(), b"plain-string".to_vec(), None);
        let delete = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("vector delete frame");
        let replacement = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("string replacement frame");

        // FCRP is ordered: a future sequence is rejected instead of silently
        // advancing the watermark past a missing mutation.
        assert!(replica.apply_frame(replacement.clone()).is_err());
        replica.apply_frame(delete).expect("apply vector delete");
        replica.apply_frame(replacement).expect("apply replacement");
        assert_eq!(replica.get(&key), Some(b"plain-string".to_vec()));
        assert!(replica.inner().clone_vector_value(&key).is_none());
    }

    #[test]
    fn embedded_replica_preserves_governance_and_fails_closed() {
        let primary = ReplicatedEmbeddedStore::new(4, config(ReplicationSendPolicy::Immediate))
            .expect("primary");
        let mut replica = ReplicationReplica::new(4);
        let subscriber = primary.primary().subscribe(8);
        primary.set_with_governance(
            b"private".to_vec(),
            b"model-state".to_vec(),
            None,
            b"tenant-a/repo-private".to_vec(),
        );
        let frame = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("replication frame");
        replica.apply_frame(frame).expect("apply");

        assert_eq!(primary.get(b"private"), None);
        assert_eq!(replica.get(b"private"), None);
        assert_eq!(
            replica
                .inner()
                .get_value_bytes_with_governance_filter(b"private", |metadata| {
                    metadata == Some(b"tenant-a/repo-private".as_slice())
                })
                .as_deref(),
            Some(b"model-state".as_slice())
        );
    }

    #[test]
    fn embedded_replica_applies_batched_mutations() {
        let mut replication = config(ReplicationSendPolicy::Batch);
        replication.batch_max_delay_us = 100_000;
        let primary = ReplicatedEmbeddedStore::new(4, replication).expect("primary");
        let mut replica = ReplicationReplica::new(4);
        let subscriber = primary.primary().subscribe(8);
        let (first_key, second_key) = same_source_shard_keys(&primary);
        primary.set(first_key.clone(), b"one".to_vec(), None);
        thread::sleep(Duration::from_millis(10));
        assert!(subscriber.try_recv().is_err());
        primary.set(second_key.clone(), b"two".to_vec(), None);
        let frame = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("replication frame");
        replica.apply_frame(frame.clone()).expect("apply");
        assert_eq!(replica.get(&first_key), Some(b"one".to_vec()));
        assert_eq!(replica.get(&second_key), Some(b"two".to_vec()));
        let stored = replica
            .inner()
            .get_value_bytes(&first_key)
            .expect("stored value");
        assert!(!bytes_points_inside(&stored, &frame));
    }

    #[test]
    fn idle_flusher_wakes_and_honors_a_single_batch_deadline() {
        let mut replication = config(ReplicationSendPolicy::Batch);
        replication.batch_max_records = 512;
        replication.batch_max_delay_us = 5_000;
        let primary = ReplicatedEmbeddedStore::new(4, replication).expect("primary");
        let subscriber = primary.primary().subscribe(8);

        primary.set(b"single".to_vec(), b"value".to_vec(), None);
        let frame = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("single delayed batch");
        let mut replica = ReplicationReplica::new(4);
        replica.apply_frame(frame).expect("apply delayed batch");
        assert_eq!(replica.get(b"single"), Some(b"value".to_vec()));
    }

    #[test]
    fn backlog_catch_up_replays_missing_mutations() {
        let primary = ReplicatedEmbeddedStore::new(4, config(ReplicationSendPolicy::Immediate))
            .expect("primary");
        primary.set(b"alpha".to_vec(), b"one".to_vec(), None);
        thread::sleep(Duration::from_millis(20));
        let mut replica = ReplicationReplica::new(4);
        primary.catch_up_replica(&mut replica).expect("catch up");
        assert_eq!(replica.get(b"alpha"), Some(b"one".to_vec()));
    }

    #[test]
    fn snapshot_catch_up_restores_when_backlog_is_insufficient() {
        let mut cfg = config(ReplicationSendPolicy::Immediate);
        cfg.backlog_bytes = 1;
        let primary = ReplicatedEmbeddedStore::new(4, cfg).expect("primary");
        primary.set(b"alpha".to_vec(), b"one".to_vec(), None);
        thread::sleep(Duration::from_millis(20));
        let mut replica = ReplicationReplica::new(4);
        primary.catch_up_replica(&mut replica).expect("catch up");
        assert_eq!(replica.get(b"alpha"), Some(b"one".to_vec()));
        assert_eq!(replica.metrics_snapshot().catch_up_snapshot_count, 1);
    }

    #[test]
    fn snapshot_atomicity_under_concurrent_writes() {
        let primary = ReplicatedEmbeddedStore::new(4, config(ReplicationSendPolicy::Immediate))
            .expect("primary");
        let writer = Arc::new(primary);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer_clone = Arc::clone(&writer);
        let stop_clone = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut i = 0u64;
            while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
                let key = format!("key-{i}");
                writer_clone.set(key.into_bytes(), b"v".to_vec(), None);
                i += 1;
            }
        });
        thread::sleep(Duration::from_millis(20));
        let snapshot = writer.snapshot();
        let entry_count = snapshot.entries.len();
        let max_watermark = snapshot
            .watermarks
            .as_slice()
            .iter()
            .copied()
            .max()
            .unwrap_or(0);
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        handle.join().expect("writer");
        // Watermark must be captured no later than entry read, so it cannot
        // exceed the count of writes that landed in the snapshot.
        assert!(
            max_watermark as usize <= entry_count,
            "watermark {max_watermark} exceeds entry count {entry_count}"
        );
    }

    #[test]
    fn multi_shard_catch_up_via_backlog() {
        let primary = ReplicatedEmbeddedStore::new(4, config(ReplicationSendPolicy::Immediate))
            .expect("primary");
        for i in 0..16 {
            primary.set(format!("key-{i}").into_bytes(), b"v".to_vec(), None);
        }
        thread::sleep(Duration::from_millis(20));
        let mut replica = ReplicationReplica::new(4);
        primary.catch_up_replica(&mut replica).expect("catch up");
        for i in 0..16 {
            assert_eq!(
                replica.get(format!("key-{i}").as_bytes()),
                Some(b"v".to_vec())
            );
        }
    }

    #[test]
    fn replica_applies_mutation_using_its_local_route() {
        let primary = ReplicatedEmbeddedStore::new(2, config(ReplicationSendPolicy::Immediate))
            .expect("primary");
        let mut replica = ReplicationReplica::new(4);
        let key = (0..10_000)
            .map(|index| format!("key-{index}").into_bytes())
            .find(|key| {
                let source = primary.inner().route_key(key);
                let target = replica.inner().route_key(key);
                source.shard_id != target.shard_id
            })
            .expect("key with different source and target shard routes");

        let subscriber = primary.primary().subscribe(8);
        primary.set(key.clone(), b"one".to_vec(), None);
        let frame = subscriber
            .recv_timeout(Duration::from_secs(2))
            .expect("replication frame");
        replica.apply_frame_bytes(&frame).expect("apply");

        assert_eq!(replica.get(&key), Some(b"one".to_vec()));
    }

    #[test]
    fn snapshot_catch_up_keeps_replica_physical_topology() {
        let mut cfg = config(ReplicationSendPolicy::Immediate);
        cfg.backlog_bytes = 1;
        let primary = ReplicatedEmbeddedStore::new(2, cfg).expect("primary");
        for index in 0..32 {
            primary.set(
                format!("snapshot-key-{index}").into_bytes(),
                b"value".to_vec(),
                None,
            );
        }
        let mut replica = ReplicationReplica::new(4);

        primary
            .catch_up_replica(&mut replica)
            .expect("snapshot catch-up");

        assert_eq!(replica.inner().shard_count(), 4);
        assert_eq!(replica.watermarks().as_slice().len(), 2);
        for index in 0..32 {
            assert_eq!(
                replica.get(format!("snapshot-key-{index}").as_bytes()),
                Some(b"value".to_vec())
            );
        }
    }

    #[test]
    fn new_requires_enabled_config() {
        let cfg = ReplicationConfig {
            enabled: false,
            ..ReplicationConfig::default()
        };
        let err = ReplicatedEmbeddedStore::new(2, cfg).expect_err("should reject disabled config");
        assert!(matches!(err, ShardCacheError::Config(_)));
    }

    fn bytes_points_inside(value: &bytes::Bytes, owner: &bytes::Bytes) -> bool {
        let value_start = value.as_ptr() as usize;
        let value_end = value_start + value.len();
        let owner_start = owner.as_ptr() as usize;
        let owner_end = owner_start + owner.len();
        value_start >= owner_start && value_end <= owner_end
    }

    fn same_source_shard_keys(primary: &ReplicatedEmbeddedStore) -> (Bytes, Bytes) {
        let first = b"batch-key-0".to_vec();
        let first_shard = primary.inner().route_key(&first).shard_id;
        let second = (1..10_000)
            .map(|index| format!("batch-key-{index}").into_bytes())
            .find(|key| primary.inner().route_key(key).shard_id == first_shard)
            .expect("second key on same source shard");
        (first, second)
    }
}
