use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Select, Sender, TrySendError, bounded};
use parking_lot::Mutex;

use crate::config::{ReplicationConfig, ReplicationSendPolicy};
use crate::{FastCacheError, Result};

use super::ReplicationFrameBytes;
use super::backlog::{BacklogCatchUp, ReplicationBacklog};
use super::metrics::{ReplicationMetrics, ReplicationMetricsSnapshot};
use super::protocol::{
    BorrowedReplicationMutation, FRAME_HEADER_LEN, FrameKind, ReplicationCompressionMode,
    ReplicationFrame, ReplicationMutation, ShardWatermarks, borrowed_mutation_record_payload_len,
    decode_frame, encode_frame, encode_mutation_batch_frame_with_payload_len,
    write_borrowed_mutation_payload_record, write_uncompressed_frame_header_at,
};

type SubscriberTx = Sender<ReplicationFrameBytes>;
type SubscriberRef = Arc<ReplicationShardSubscriber>;
type SubscriberList = Mutex<Vec<SubscriberRef>>;

#[derive(Debug)]
pub struct ReplicationPrimary {
    config: ReplicationConfig,
    shard_count: usize,
    sequences: Vec<AtomicU64>,
    metrics: ReplicationMetrics,
    shards: Vec<ReplicationShardExport>,
}

#[derive(Debug)]
struct ReplicationShardExport {
    backlog: Mutex<ReplicationBacklog>,
    subscribers: SubscriberList,
    emitted_watermark: AtomicU64,
}

#[derive(Debug)]
struct ReplicationShardSubscriber {
    tx: SubscriberTx,
    active: Arc<AtomicBool>,
}

#[derive(Debug)]
pub(crate) struct ReplicationBatch {
    pending: Vec<ReplicationMutation>,
    pending_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct ReplicationBatchBuilder {
    config: ReplicationConfig,
    pending: Vec<ReplicationMutation>,
    pending_bytes: usize,
    first_pending_at: Option<Instant>,
    track_delay: bool,
}

#[derive(Debug)]
pub(crate) struct EncodedReplicationBatch {
    shard_id: usize,
    min_sequence: u64,
    max_sequence: u64,
    record_count: usize,
    uncompressed_len: usize,
    frame: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct EncodedReplicationBatchBuilder {
    config: ReplicationConfig,
    shard_id: usize,
    frame: Vec<u8>,
    record_count: usize,
    pending_bytes: usize,
    min_sequence: u64,
    max_sequence: u64,
    first_pending_at: Option<Instant>,
    track_delay: bool,
}

impl ReplicationBatch {
    fn single(mutation: ReplicationMutation) -> Self {
        let pending_bytes = mutation.estimated_uncompressed_len();
        Self {
            pending: vec![mutation],
            pending_bytes,
        }
    }

    fn shard_id(&self) -> Option<usize> {
        self.pending.first().map(|mutation| mutation.shard_id)
    }

    fn payload_len(&self) -> usize {
        4 + self.pending_bytes
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

impl ReplicationBatchBuilder {
    pub(crate) fn new(config: ReplicationConfig) -> Self {
        Self::with_delay_tracking(config, true)
    }

    pub(crate) fn new_clockless(config: ReplicationConfig) -> Self {
        Self::with_delay_tracking(config, false)
    }

    fn with_delay_tracking(config: ReplicationConfig, track_delay: bool) -> Self {
        let capacity = batch_record_capacity(&config);
        Self {
            config,
            pending: Vec::with_capacity(capacity),
            pending_bytes: 0,
            first_pending_at: None,
            track_delay,
        }
    }

    pub(crate) fn push(&mut self, mutation: ReplicationMutation) -> Option<ReplicationBatch> {
        if self.track_delay && self.pending.is_empty() {
            self.first_pending_at = Some(Instant::now());
        }
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(mutation.estimated_uncompressed_len());
        self.pending.push(mutation);
        self.should_flush_after_push()
            .then(|| self.flush())
            .flatten()
    }

    pub(crate) fn flush_due(&mut self) -> Option<ReplicationBatch> {
        self.should_flush_due().then(|| self.flush()).flatten()
    }

    pub(crate) fn flush(&mut self) -> Option<ReplicationBatch> {
        if self.pending.is_empty() {
            return None;
        }
        let capacity = batch_record_capacity(&self.config);
        let pending = std::mem::replace(&mut self.pending, Vec::with_capacity(capacity));
        let pending_bytes = self.pending_bytes;
        self.pending_bytes = 0;
        self.first_pending_at = None;
        Some(ReplicationBatch {
            pending,
            pending_bytes,
        })
    }

    pub(crate) fn next_timeout(&self) -> Option<Duration> {
        match (self.pending.is_empty(), self.first_pending_at) {
            (true, _) => None,
            (false, None) => Some(Duration::ZERO),
            (false, Some(start)) => Some(
                self.max_delay()
                    .checked_sub(start.elapsed())
                    .unwrap_or_default(),
            ),
        }
    }

    fn should_flush_after_push(&self) -> bool {
        if self.pending.is_empty() {
            return false;
        }
        if self.config.send_policy == ReplicationSendPolicy::Immediate {
            return true;
        }
        self.pending.len() >= self.config.batch_max_records
            || self.pending_bytes >= self.config.batch_max_bytes
    }

    fn should_flush_due(&self) -> bool {
        if self.pending.is_empty() {
            return false;
        }
        if self.config.send_policy == ReplicationSendPolicy::Immediate {
            return true;
        }
        if !self.track_delay {
            return true;
        }
        self.first_pending_at
            .is_some_and(|start| start.elapsed() >= self.max_delay())
    }

    fn max_delay(&self) -> Duration {
        Duration::from_micros(self.config.batch_max_delay_us)
    }
}

impl EncodedReplicationBatch {
    fn shard_id(&self) -> usize {
        self.shard_id
    }

    fn min_sequence(&self) -> u64 {
        self.min_sequence
    }

    fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    fn record_count(&self) -> usize {
        self.record_count
    }

    fn uncompressed_len(&self) -> usize {
        self.uncompressed_len
    }

    fn into_frame(
        self,
        compression: ReplicationCompressionMode,
        zstd_level: i32,
    ) -> Result<Vec<u8>> {
        match compression {
            ReplicationCompressionMode::None => Ok(self.frame),
            ReplicationCompressionMode::Zstd => encode_frame(
                FrameKind::MutationBatch,
                compression,
                zstd_level,
                &self.frame[FRAME_HEADER_LEN..],
            ),
        }
    }
}

impl EncodedReplicationBatchBuilder {
    pub(crate) fn new_clockless(config: ReplicationConfig, shard_id: usize) -> Self {
        Self::with_delay_tracking(config, shard_id, false)
    }

    pub(crate) fn shard_id(&self) -> usize {
        self.shard_id
    }

    fn with_delay_tracking(config: ReplicationConfig, shard_id: usize, track_delay: bool) -> Self {
        Self {
            frame: empty_encoded_frame(&config),
            config,
            shard_id,
            record_count: 0,
            pending_bytes: 0,
            min_sequence: u64::MAX,
            max_sequence: 0,
            first_pending_at: None,
            track_delay,
        }
    }

    pub(crate) fn push(
        &mut self,
        mutation: BorrowedReplicationMutation<'_>,
    ) -> Option<EncodedReplicationBatch> {
        if self.track_delay && self.record_count == 0 {
            self.first_pending_at = Some(Instant::now());
        }
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(borrowed_mutation_record_payload_len(mutation));
        self.record_count += 1;
        self.min_sequence = self.min_sequence.min(mutation.sequence);
        self.max_sequence = self.max_sequence.max(mutation.sequence);
        write_borrowed_mutation_payload_record(&mut self.frame, mutation);
        self.should_flush_after_push()
            .then(|| self.flush())
            .flatten()
    }

    pub(crate) fn flush_due(&mut self) -> Option<EncodedReplicationBatch> {
        self.should_flush_due().then(|| self.flush()).flatten()
    }

    pub(crate) fn flush(&mut self) -> Option<EncodedReplicationBatch> {
        if self.record_count == 0 {
            return None;
        }

        let record_count = self.record_count;
        let pending_bytes = self.pending_bytes;
        let min_sequence = self.min_sequence;
        let max_sequence = self.max_sequence;
        let mut frame = std::mem::replace(&mut self.frame, empty_encoded_frame(&self.config));
        let uncompressed_len = 4 + pending_bytes;
        write_uncompressed_frame_header_at(&mut frame, FrameKind::MutationBatch, uncompressed_len);
        frame[FRAME_HEADER_LEN..FRAME_HEADER_LEN + 4]
            .copy_from_slice(&(record_count as u32).to_le_bytes());

        self.record_count = 0;
        self.pending_bytes = 0;
        self.min_sequence = u64::MAX;
        self.max_sequence = 0;
        self.first_pending_at = None;
        Some(EncodedReplicationBatch {
            shard_id: self.shard_id,
            min_sequence,
            max_sequence,
            record_count,
            uncompressed_len,
            frame,
        })
    }

    fn should_flush_after_push(&self) -> bool {
        if self.record_count == 0 {
            return false;
        }
        if self.config.send_policy == ReplicationSendPolicy::Immediate {
            return true;
        }
        self.record_count >= self.config.batch_max_records
            || self.pending_bytes >= self.config.batch_max_bytes
    }

    fn should_flush_due(&self) -> bool {
        if self.record_count == 0 {
            return false;
        }
        if self.config.send_policy == ReplicationSendPolicy::Immediate {
            return true;
        }
        if !self.track_delay {
            return true;
        }
        self.first_pending_at
            .is_some_and(|start| start.elapsed() >= self.max_delay())
    }

    fn max_delay(&self) -> Duration {
        Duration::from_micros(self.config.batch_max_delay_us)
    }
}

fn empty_encoded_frame(config: &ReplicationConfig) -> Vec<u8> {
    let payload_capacity = config.batch_max_bytes.clamp(4, 64 * 1024);
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload_capacity);
    frame.resize(FRAME_HEADER_LEN, 0);
    frame.extend_from_slice(&0_u32.to_le_bytes());
    frame
}

fn batch_record_capacity(config: &ReplicationConfig) -> usize {
    match config.send_policy {
        ReplicationSendPolicy::Immediate => 1,
        ReplicationSendPolicy::Batch => config.batch_max_records.clamp(1, 1024),
    }
}

fn start_subscriber_fan_in(
    out_tx: SubscriberTx,
    shard_receivers: Vec<Receiver<ReplicationFrameBytes>>,
    active: Arc<AtomicBool>,
) {
    let thread_active = Arc::clone(&active);
    match thread::Builder::new()
        .name("fast-cache-replication-subscriber-fan-in".into())
        .spawn(move || run_subscriber_fan_in(out_tx, shard_receivers, thread_active))
    {
        Ok(_) => {}
        Err(error) => {
            active.store(false, Ordering::Relaxed);
            tracing::error!("failed to start replication subscriber fan-in thread: {error}");
        }
    }
}

fn run_subscriber_fan_in(
    out_tx: SubscriberTx,
    mut shard_receivers: Vec<Receiver<ReplicationFrameBytes>>,
    active: Arc<AtomicBool>,
) {
    while active.load(Ordering::Relaxed) && !shard_receivers.is_empty() {
        let mut select = Select::new();
        for receiver in &shard_receivers {
            select.recv(receiver);
        }

        let operation = select.select();
        let index = operation.index();
        match operation.recv(&shard_receivers[index]) {
            Ok(frame) => match out_tx.send(frame) {
                Ok(()) => {}
                Err(_) => {
                    active.store(false, Ordering::Relaxed);
                    break;
                }
            },
            Err(_) => {
                shard_receivers.swap_remove(index);
            }
        }
    }
    active.store(false, Ordering::Relaxed);
}

impl ReplicationPrimary {
    pub fn start(shard_count: usize, config: ReplicationConfig) -> Result<Self> {
        if !config.enabled {
            return Err(FastCacheError::Config(
                "replication primary requires replication.enabled = true".into(),
            ));
        }
        let shard_count = shard_count.max(1);
        let metrics = ReplicationMetrics::default();
        let shards = (0..shard_count)
            .map(|_| ReplicationShardExport {
                backlog: Mutex::new(ReplicationBacklog::new(config.backlog_bytes, shard_count)),
                subscribers: Mutex::new(Vec::new()),
                emitted_watermark: AtomicU64::new(0),
            })
            .collect();
        Ok(Self {
            config,
            shard_count,
            sequences: (0..shard_count).map(|_| AtomicU64::new(0)).collect(),
            metrics,
            shards,
        })
    }

    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Allocates the next sequence for `shard_id` from the primary-owned
    /// counter. Callers using an external sequence source (such as the engine
    /// shard worker, which already mints sequences for the WAL) should pass
    /// fully-formed mutations to [`Self::emit`] instead.
    pub fn next_sequence(&self, shard_id: usize) -> u64 {
        debug_assert!(
            shard_id < self.sequences.len(),
            "shard_id {shard_id} out of range for {} primary sequences",
            self.sequences.len()
        );
        let Some(slot) = self.sequences.get(shard_id) else {
            return 0;
        };
        slot.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Enqueues a mutation for callers that do not own a shard-local batch
    /// buffer.
    ///
    /// The sharded engine path uses `ReplicationBatchBuilder` so it can
    /// append mutations to an ordered per-shard `Vec` and hand off ready
    /// batches. This fallback keeps direct embedded callers correct without
    /// adding shared batching state.
    pub fn emit(&self, mutation: ReplicationMutation) {
        if mutation.shard_id >= self.shards.len() {
            self.metrics.record_drop();
            tracing::warn!(
                "dropping replication mutation for shard {} outside configured shard_count {}",
                mutation.shard_id,
                self.shard_count
            );
            return;
        }
        self.export_batch_direct(ReplicationBatch::single(mutation));
    }

    /// Encodes and publishes a shard-owned batch on the caller's thread.
    ///
    /// Storage shard workers already own ordering and batching, so routing
    /// those batches through a second exporter thread only adds channel and
    /// scheduler overhead. Direct export still keeps socket writes out of the
    /// storage shard; it publishes immutable frames into subscriber queues.
    pub(crate) fn export_batch_direct(&self, batch: ReplicationBatch) {
        if batch.is_empty() {
            return;
        }
        let Some(shard_id) = batch.shard_id() else {
            return;
        };
        if shard_id >= self.shards.len() {
            self.metrics.record_drop();
            tracing::warn!(
                "dropping replication batch for shard {} outside configured shard_count {}",
                shard_id,
                self.shard_count
            );
            return;
        }
        let payload_len = batch.payload_len();
        let record_count = batch.pending.len();
        let compression = ReplicationCompressionMode::from(self.config.compression);
        let compression_started = Instant::now();
        let (frame, uncompressed_len) = match encode_mutation_batch_frame_with_payload_len(
            &batch.pending,
            payload_len,
            compression,
            self.config.zstd_level,
        ) {
            Ok(encoded) => encoded,
            Err(error) => {
                tracing::error!("failed to encode replication batch: {error}");
                return;
            }
        };
        let compression_ns = compression_started.elapsed().as_nanos() as u64;
        self.metrics.record_emit_count(record_count, 0);
        self.metrics
            .record_batch(record_count, uncompressed_len, frame.len(), compression_ns);
        let frame = ReplicationFrameBytes::from(frame);
        self.shards[shard_id]
            .backlog
            .lock()
            .push_encoded(frame.clone(), &batch.pending);
        self.observe_emitted_watermarks(&batch.pending);
        self.broadcast(shard_id, frame);
    }

    pub(crate) fn export_encoded_batch_direct(&self, batch: EncodedReplicationBatch) {
        let shard_id = batch.shard_id();
        if shard_id >= self.shards.len() {
            self.metrics.record_drop();
            tracing::warn!(
                "dropping encoded replication batch for shard {} outside configured shard_count {}",
                shard_id,
                self.shard_count
            );
            return;
        }

        let record_count = batch.record_count();
        let min_sequence = batch.min_sequence();
        let max_sequence = batch.max_sequence();
        let uncompressed_len = batch.uncompressed_len();
        let compression = ReplicationCompressionMode::from(self.config.compression);
        let compression_started = Instant::now();
        let frame = match batch.into_frame(compression, self.config.zstd_level) {
            Ok(frame) => frame,
            Err(error) => {
                tracing::error!("failed to encode direct replication batch: {error}");
                return;
            }
        };
        let compression_ns = compression_started.elapsed().as_nanos() as u64;
        self.metrics.record_emit_count(record_count, 0);
        self.metrics
            .record_batch(record_count, uncompressed_len, frame.len(), compression_ns);
        let frame = ReplicationFrameBytes::from(frame);
        self.shards[shard_id].backlog.lock().push_encoded_span(
            frame.clone(),
            shard_id,
            min_sequence,
            max_sequence,
        );
        self.shards[shard_id]
            .emitted_watermark
            .fetch_max(max_sequence, Ordering::Relaxed);
        self.broadcast(shard_id, frame);
    }

    pub fn queue_depths(&self) -> Vec<usize> {
        vec![0; self.shard_count]
    }

    pub fn max_queue_depth(&self) -> usize {
        0
    }

    pub fn total_queue_depth(&self) -> usize {
        0
    }

    pub fn per_shard_export_enabled(&self) -> bool {
        self.shards.len() == self.shard_count
    }

    pub fn lane_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    pub fn subscribe(&self, channel_capacity: usize) -> Receiver<ReplicationFrameBytes> {
        let channel_capacity = channel_capacity.max(1);
        let (out_tx, out_rx) = bounded(channel_capacity);
        let active = Arc::new(AtomicBool::new(true));
        let mut shard_receivers = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let (tx, rx) = bounded(channel_capacity);
            shard
                .subscribers
                .lock()
                .push(Arc::new(ReplicationShardSubscriber {
                    tx,
                    active: Arc::clone(&active),
                }));
            shard_receivers.push(rx);
        }
        start_subscriber_fan_in(out_tx, shard_receivers, active);
        out_rx
    }

    pub fn catch_up_since(&self, watermarks: &ShardWatermarks) -> Result<BacklogCatchUp> {
        let mut frames = Vec::new();
        for shard in &self.shards {
            match shard.backlog.lock().catch_up_since(watermarks)? {
                BacklogCatchUp::Available(mut shard_frames) => frames.append(&mut shard_frames),
                BacklogCatchUp::NeedsSnapshot => return Ok(BacklogCatchUp::NeedsSnapshot),
            }
        }
        Ok(BacklogCatchUp::Available(frames))
    }

    /// Returns the watermarks for batches that have been emitted to subscribers
    /// and the backlog. Mutations that are still pending in shard-local batch
    /// builders or ready-batch channels are not reflected here.
    pub fn current_watermarks(&self) -> ShardWatermarks {
        ShardWatermarks::from_vec(
            self.shards
                .iter()
                .map(|shard| shard.emitted_watermark.load(Ordering::Relaxed))
                .collect(),
        )
    }

    pub fn latest_backlog_watermarks(&self) -> ShardWatermarks {
        self.current_watermarks()
    }

    pub fn metrics_snapshot(&self) -> ReplicationMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn decode_subscriber_frame(bytes: &[u8]) -> Result<ReplicationFrame> {
        decode_frame(bytes)
    }

    fn observe_emitted_watermarks(&self, mutations: &[ReplicationMutation]) {
        for mutation in mutations {
            if let Some(shard) = self.shards.get(mutation.shard_id) {
                shard
                    .emitted_watermark
                    .fetch_max(mutation.sequence, Ordering::Relaxed);
            }
        }
    }

    fn broadcast(&self, shard_id: usize, frame: ReplicationFrameBytes) {
        let Some(shard) = self.shards.get(shard_id) else {
            return;
        };
        let mut subscribers = shard.subscribers.lock();
        subscribers.retain(|subscriber| {
            subscriber.active.load(Ordering::Relaxed)
                && match subscriber.tx.try_send(frame.clone()) {
                    Ok(()) => true,
                    Err(TrySendError::Full(_)) => {
                        subscriber.active.store(false, Ordering::Relaxed);
                        self.metrics.record_backpressure();
                        self.metrics.record_drop();
                        false
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        subscriber.active.store(false, Ordering::Relaxed);
                        self.metrics.record_drop();
                        false
                    }
                }
        });
    }
}

impl Drop for ReplicationPrimary {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
