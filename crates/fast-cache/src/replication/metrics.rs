use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Clone, Default)]
pub struct ReplicationMetrics {
    inner: Arc<ReplicationMetricsInner>,
}

#[derive(Debug, Default)]
struct ReplicationMetricsInner {
    emitted_mutations: AtomicU64,
    sent_batches: AtomicU64,
    sent_records: AtomicU64,
    sent_bytes_compressed: AtomicU64,
    sent_bytes_uncompressed: AtomicU64,
    compression_ns: AtomicU64,
    queue_depth: AtomicUsize,
    queue_high_watermark: AtomicUsize,
    drops: AtomicU64,
    backpressure_events: AtomicU64,
    replica_applied: AtomicU64,
    replica_skipped: AtomicU64,
    replica_apply_ns: AtomicU64,
    catch_up_backlog_count: AtomicU64,
    catch_up_snapshot_count: AtomicU64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReplicationMetricsSnapshot {
    pub emitted_mutations: u64,
    pub sent_batches: u64,
    pub sent_records: u64,
    pub sent_bytes_compressed: u64,
    pub sent_bytes_uncompressed: u64,
    pub compression_ns: u64,
    pub queue_depth: usize,
    pub queue_high_watermark: usize,
    pub drops: u64,
    pub backpressure_events: u64,
    pub replica_applied: u64,
    pub replica_skipped: u64,
    pub replica_apply_ns: u64,
    pub catch_up_backlog_count: u64,
    pub catch_up_snapshot_count: u64,
}

impl ReplicationMetrics {
    pub fn record_emit(&self, queue_depth: usize) {
        self.record_emit_count(1, queue_depth);
    }

    pub fn record_emit_count(&self, count: usize, queue_depth: usize) {
        self.inner
            .emitted_mutations
            .fetch_add(count as u64, Ordering::Relaxed);
        self.set_queue_depth(queue_depth);
    }

    pub fn record_drop(&self) {
        self.inner.drops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_backpressure(&self) {
        self.inner
            .backpressure_events
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_batch(
        &self,
        records: usize,
        uncompressed_bytes: usize,
        compressed_bytes: usize,
        compression_ns: u64,
    ) {
        self.inner.sent_batches.fetch_add(1, Ordering::Relaxed);
        self.inner
            .sent_records
            .fetch_add(records as u64, Ordering::Relaxed);
        self.inner
            .sent_bytes_uncompressed
            .fetch_add(uncompressed_bytes as u64, Ordering::Relaxed);
        self.inner
            .sent_bytes_compressed
            .fetch_add(compressed_bytes as u64, Ordering::Relaxed);
        self.inner
            .compression_ns
            .fetch_add(compression_ns, Ordering::Relaxed);
    }

    pub fn record_queue_depth(&self, queue_depth: usize) {
        self.set_queue_depth(queue_depth);
    }

    pub fn record_replica_apply(&self, applied: bool, apply_ns: u64) {
        if applied {
            self.inner.replica_applied.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.replica_skipped.fetch_add(1, Ordering::Relaxed);
        }
        self.inner
            .replica_apply_ns
            .fetch_add(apply_ns, Ordering::Relaxed);
    }

    pub fn record_replica_apply_batch(&self, applied: u64, skipped: u64, apply_ns: u64) {
        self.inner
            .replica_applied
            .fetch_add(applied, Ordering::Relaxed);
        self.inner
            .replica_skipped
            .fetch_add(skipped, Ordering::Relaxed);
        self.inner
            .replica_apply_ns
            .fetch_add(apply_ns, Ordering::Relaxed);
    }

    pub fn record_backlog_catch_up(&self) {
        self.inner
            .catch_up_backlog_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_snapshot_catch_up(&self) {
        self.inner
            .catch_up_snapshot_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ReplicationMetricsSnapshot {
        ReplicationMetricsSnapshot {
            emitted_mutations: self.inner.emitted_mutations.load(Ordering::Relaxed),
            sent_batches: self.inner.sent_batches.load(Ordering::Relaxed),
            sent_records: self.inner.sent_records.load(Ordering::Relaxed),
            sent_bytes_compressed: self.inner.sent_bytes_compressed.load(Ordering::Relaxed),
            sent_bytes_uncompressed: self.inner.sent_bytes_uncompressed.load(Ordering::Relaxed),
            compression_ns: self.inner.compression_ns.load(Ordering::Relaxed),
            queue_depth: self.inner.queue_depth.load(Ordering::Relaxed),
            queue_high_watermark: self.inner.queue_high_watermark.load(Ordering::Relaxed),
            drops: self.inner.drops.load(Ordering::Relaxed),
            backpressure_events: self.inner.backpressure_events.load(Ordering::Relaxed),
            replica_applied: self.inner.replica_applied.load(Ordering::Relaxed),
            replica_skipped: self.inner.replica_skipped.load(Ordering::Relaxed),
            replica_apply_ns: self.inner.replica_apply_ns.load(Ordering::Relaxed),
            catch_up_backlog_count: self.inner.catch_up_backlog_count.load(Ordering::Relaxed),
            catch_up_snapshot_count: self.inner.catch_up_snapshot_count.load(Ordering::Relaxed),
        }
    }

    fn set_queue_depth(&self, queue_depth: usize) {
        self.inner.queue_depth.store(queue_depth, Ordering::Relaxed);
        let mut current = self.inner.queue_high_watermark.load(Ordering::Relaxed);
        while queue_depth > current {
            match self.inner.queue_high_watermark.compare_exchange_weak(
                current,
                queue_depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }
}
