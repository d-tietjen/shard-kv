use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use parking_lot::Mutex;

use crate::config::ReplicationConfig;
use crate::storage::{
    Bytes, EmbeddedRouteMode, EmbeddedStore, MutationOp, hash_key_tag_from_hash, now_millis,
    ttl_now_millis,
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

const DIRECT_ENCODED_SET_MAX_VALUE_LEN: usize = 128;

#[derive(Debug)]
pub struct ReplicatedEmbeddedStore {
    store: EmbeddedStore,
    primary: Arc<ReplicationPrimary>,
    emitters: Arc<ReplicatedEmbeddedEmitters>,
}

#[derive(Debug)]
pub struct ReplicationReplica {
    store: EmbeddedStore,
    watermarks: ShardWatermarks,
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
    flush_interval: Duration,
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
        let store = EmbeddedStore::with_route_mode(shard_count, route_mode);
        let primary = Arc::new(ReplicationPrimary::start(shard_count, config.clone())?);
        let emitters =
            ReplicatedEmbeddedEmitters::start(Arc::clone(&primary), shard_count, config)?;
        Ok(Self {
            store,
            primary,
            emitters,
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.store.get(key)
    }

    pub fn set(&self, key: Bytes, value: Bytes, ttl_ms: Option<u64>) {
        let route = self.store.route_key(&key);
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
                            shard_id: route.shard_id,
                            timestamp_ms: now_ms,
                            key_hash: route.key_hash,
                            key_tag: hash_key_tag_from_hash(route.key_hash),
                            key: key.as_ref(),
                            value: value.as_ref(),
                            expire_at_ms,
                        }),
                        false => self.emitters.emit(
                            route.shard_id,
                            ReplicationMutation {
                                shard_id: route.shard_id,
                                sequence: 0,
                                timestamp_ms: now_ms,
                                op: ReplicationMutationOp::Set,
                                key_hash: route.key_hash,
                                key_tag: hash_key_tag_from_hash(route.key_hash),
                                key: key.clone(),
                                value: value.clone(),
                                expire_at_ms,
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
                            shard_id: route.shard_id,
                            timestamp_ms,
                            key_hash: route.key_hash,
                            key_tag: hash_key_tag_from_hash(route.key_hash),
                            key: key.as_ref(),
                            value: value.as_ref(),
                            expire_at_ms: None,
                        }),
                        false => self.emitters.emit(
                            route.shard_id,
                            ReplicationMutation {
                                shard_id: route.shard_id,
                                sequence: 0,
                                timestamp_ms,
                                op: ReplicationMutationOp::Set,
                                key_hash: route.key_hash,
                                key_tag: hash_key_tag_from_hash(route.key_hash),
                                key: key.clone(),
                                value: value.clone(),
                                expire_at_ms: None,
                            },
                        ),
                    },
                );
            }
        }
    }

    pub fn delete(&self, key: &[u8]) -> bool {
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
                },
            );
        })
    }

    pub fn expire(&self, key: &[u8], expire_at_ms: u64) -> bool {
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
            replica.replace_with_snapshot(self.snapshot());
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

    pub fn metrics_snapshot(&self) -> ReplicationMetricsSnapshot {
        self.emitters.flush_all_and_wait();
        self.primary.metrics_snapshot()
    }

    pub fn inner(&self) -> &EmbeddedStore {
        &self.store
    }
}

fn direct_encoded_set_enabled(value_len: usize) -> bool {
    value_len <= DIRECT_ENCODED_SET_MAX_VALUE_LEN
}

impl Drop for ReplicatedEmbeddedStore {
    fn drop(&mut self) {
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
        let flush_interval = Duration::from_micros(config.batch_max_delay_us.max(1));
        let exporter_stop = Arc::new(AtomicBool::new(false));
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
            flush_interval,
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
            });
            return;
        };
        let mut emitter = shard.lock();
        emitter.emit_borrowed_set(set);
    }

    fn run_flusher(&self) {
        while !self.flusher_stop.load(Ordering::Relaxed) {
            thread::sleep(self.flush_interval);
            self.flush_due();
        }
        self.flush_all();
    }

    fn flush_due(&self) {
        for shard in &self.shards {
            if let Some(mut emitter) = shard.try_lock() {
                emitter.flush_due();
            }
        }
    }

    fn flush_all(&self) -> Vec<u64> {
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
        };
        if let Some(batch) = self.encoded_batch.push(mutation) {
            self.send_encoded_batch(batch);
        }
    }

    fn flush_due(&mut self) {
        if let Some(batch) = self.batch.flush_due() {
            self.send_owned_batch(batch);
        }
        if let Some(batch) = self.encoded_batch.flush_due() {
            self.send_encoded_batch(batch);
        }
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
            metrics: ReplicationMetrics::default(),
        }
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
            if self.apply_borrowed_mutation_inner(mutation, &mut now_ms) {
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
            if self.apply_frame_backed_mutation_inner(mutation, &mut now_ms) {
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
        let started = Instant::now();
        let applied = self.apply_mutation_inner(mutation, &mut None);
        self.metrics
            .record_replica_apply(applied, started.elapsed().as_nanos() as u64);
    }

    fn apply_mutation_inner(
        &mut self,
        mutation: ReplicationMutation,
        now_ms: &mut Option<u64>,
    ) -> bool {
        if mutation.sequence <= self.watermarks.get(mutation.shard_id) {
            return false;
        }

        match mutation.op {
            ReplicationMutationOp::Set => {
                self.apply_set(
                    mutation.key_hash,
                    mutation.key.as_ref(),
                    mutation.value,
                    mutation.expire_at_ms,
                    now_ms,
                );
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
        true
    }

    fn apply_frame_backed_mutation_inner(
        &mut self,
        mutation: FrameBackedReplicationMutation<'_>,
        now_ms: &mut Option<u64>,
    ) -> bool {
        if mutation.sequence <= self.watermarks.get(mutation.shard_id) {
            return false;
        }

        match mutation.op {
            ReplicationMutationOp::Set => {
                self.apply_set(
                    mutation.key_hash,
                    mutation.key,
                    mutation.value,
                    mutation.expire_at_ms,
                    now_ms,
                );
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
        true
    }

    fn apply_borrowed_mutation_inner(
        &mut self,
        mutation: BorrowedReplicationMutation<'_>,
        now_ms: &mut Option<u64>,
    ) -> bool {
        if mutation.sequence <= self.watermarks.get(mutation.shard_id) {
            return false;
        }

        match mutation.op {
            ReplicationMutationOp::Set => {
                self.apply_set(
                    mutation.key_hash,
                    mutation.key,
                    bytes::Bytes::copy_from_slice(mutation.value),
                    mutation.expire_at_ms,
                    now_ms,
                );
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
        true
    }

    fn apply_set(
        &mut self,
        key_hash: u64,
        key: &[u8],
        value: bytes::Bytes,
        expire_at_ms: Option<u64>,
        now_ms: &mut Option<u64>,
    ) {
        let route = self.store.route_key_prehashed(key_hash, key);
        match expire_at_ms {
            Some(expire_at_ms) => {
                let now_ms = *now_ms.get_or_insert_with(now_millis);
                if expire_at_ms <= now_ms {
                    return;
                }
                self.store.set_value_bytes_routed_expire_at(
                    route,
                    key,
                    value,
                    Some(expire_at_ms),
                    now_ms,
                );
            }
            None => self
                .store
                .set_value_bytes_routed_no_ttl_then(route, key, value, || {}),
        }
    }

    pub fn replace_with_snapshot(&mut self, snapshot: ReplicationSnapshot) {
        let route_mode = self.store.route_mode();
        let shard_count = self.store.shard_count();
        let store = EmbeddedStore::with_route_mode(shard_count, route_mode);
        store.restore_entries(snapshot.entries);
        self.store = store;
        self.watermarks = snapshot.watermarks;
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

    use crate::config::{ReplicationCompression, ReplicationConfig, ReplicationSendPolicy};

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

    #[test]
    fn embedded_replica_applies_batched_mutations() {
        let primary =
            ReplicatedEmbeddedStore::new(4, config(ReplicationSendPolicy::Batch)).expect("primary");
        let mut replica = ReplicationReplica::new(4);
        let subscriber = primary.primary().subscribe(8);
        let (first_key, second_key) = same_source_shard_keys(&primary);
        primary.set(first_key.clone(), b"one".to_vec(), None);
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
