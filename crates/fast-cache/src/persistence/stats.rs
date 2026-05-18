use fast_telemetry::{Counter, Gauge};

use crate::storage::WalStatsSnapshot;

pub(super) struct WalStats {
    enabled: Gauge,
    entries_written: Counter,
    segments_rotated: Counter,
    bytes_written: Counter,
    last_flush_ms: Gauge,
    recoveries: Counter,
    snapshots_written: Counter,
    tcp_export_enabled: Gauge,
    tcp_export_frames_queued: Counter,
    tcp_export_frames_sent: Counter,
    tcp_export_bytes_sent: Counter,
    tcp_export_frames_dropped: Counter,
    tcp_export_connect_failures: Counter,
    tcp_export_write_failures: Counter,
    tcp_export_active_subscribers: Gauge,
    tcp_export_subscribers_accepted: Counter,
    tcp_export_subscribers_rejected: Counter,
}

impl WalStats {
    pub(super) fn enabled() -> Self {
        Self::new(true)
    }

    pub(super) fn disabled() -> Self {
        Self::new(false)
    }

    pub(super) fn snapshot(&self) -> WalStatsSnapshot {
        WalStatsSnapshot {
            enabled: gauge_bool(&self.enabled),
            entries_written: counter_u64(&self.entries_written),
            segments_rotated: counter_u64(&self.segments_rotated),
            bytes_written: counter_u64(&self.bytes_written),
            last_flush_ms: gauge_u64(&self.last_flush_ms),
            recoveries: counter_u64(&self.recoveries),
            snapshots_written: counter_u64(&self.snapshots_written),
            tcp_export_enabled: gauge_bool(&self.tcp_export_enabled),
            tcp_export_frames_queued: counter_u64(&self.tcp_export_frames_queued),
            tcp_export_frames_sent: counter_u64(&self.tcp_export_frames_sent),
            tcp_export_bytes_sent: counter_u64(&self.tcp_export_bytes_sent),
            tcp_export_frames_dropped: counter_u64(&self.tcp_export_frames_dropped),
            tcp_export_connect_failures: counter_u64(&self.tcp_export_connect_failures),
            tcp_export_write_failures: counter_u64(&self.tcp_export_write_failures),
            tcp_export_active_subscribers: gauge_usize(&self.tcp_export_active_subscribers),
            tcp_export_subscribers_accepted: counter_u64(&self.tcp_export_subscribers_accepted),
            tcp_export_subscribers_rejected: counter_u64(&self.tcp_export_subscribers_rejected),
        }
    }

    pub(super) fn record_append(&self, bytes: usize, rotations: u64) {
        self.entries_written.inc();
        counter_add_u64(&self.bytes_written, bytes as u64);
        counter_add_u64(&self.segments_rotated, rotations);
    }

    pub(super) fn record_flush(&self, timestamp_ms: u64) {
        self.last_flush_ms
            .set(i64_saturating_from_u64(timestamp_ms));
    }

    pub(super) fn record_snapshot_written(&self) {
        self.snapshots_written.inc();
    }

    pub(super) fn enable_tcp_export(&self) {
        self.tcp_export_enabled.set(1);
    }

    pub(super) fn record_tcp_export_queued(&self) {
        self.tcp_export_frames_queued.inc();
    }

    pub(super) fn record_tcp_export_dropped(&self) {
        self.tcp_export_frames_dropped.inc();
    }

    pub(super) fn record_tcp_export_sent(&self, frames: u64, bytes: u64) {
        counter_add_u64(&self.tcp_export_frames_sent, frames);
        counter_add_u64(&self.tcp_export_bytes_sent, bytes);
    }

    pub(super) fn record_tcp_export_write_failures(&self, failures: u64) {
        counter_add_u64(&self.tcp_export_write_failures, failures);
    }

    pub(super) fn record_tcp_export_connect_failure(&self) {
        self.tcp_export_connect_failures.inc();
    }

    pub(super) fn record_tcp_export_subscriber_accepted(&self, active: usize) {
        self.tcp_export_subscribers_accepted.inc();
        self.set_tcp_export_active_subscribers(active);
    }

    pub(super) fn record_tcp_export_subscriber_rejected(&self) {
        self.tcp_export_subscribers_rejected.inc();
    }

    pub(super) fn set_tcp_export_active_subscribers(&self, active: usize) {
        self.tcp_export_active_subscribers
            .set(i64_saturating_from_usize(active));
    }

    fn new(enabled: bool) -> Self {
        let shards = metric_shards();
        Self {
            enabled: Gauge::with_value(i64::from(enabled)),
            entries_written: Counter::new(shards),
            segments_rotated: Counter::new(shards),
            bytes_written: Counter::new(shards),
            last_flush_ms: Gauge::new(),
            recoveries: Counter::new(shards),
            snapshots_written: Counter::new(shards),
            tcp_export_enabled: Gauge::new(),
            tcp_export_frames_queued: Counter::new(shards),
            tcp_export_frames_sent: Counter::new(shards),
            tcp_export_bytes_sent: Counter::new(shards),
            tcp_export_frames_dropped: Counter::new(shards),
            tcp_export_connect_failures: Counter::new(shards),
            tcp_export_write_failures: Counter::new(shards),
            tcp_export_active_subscribers: Gauge::new(),
            tcp_export_subscribers_accepted: Counter::new(shards),
            tcp_export_subscribers_rejected: Counter::new(shards),
        }
    }
}

fn metric_shards() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
}

fn counter_add_u64(counter: &Counter, value: u64) {
    if value <= isize::MAX as u64 {
        counter.add(value as isize);
        return;
    }

    let mut remaining = value;
    while remaining > 0 {
        let chunk = remaining.min(isize::MAX as u64);
        counter.add(chunk as isize);
        remaining -= chunk;
    }
}

fn counter_u64(counter: &Counter) -> u64 {
    u64::try_from(counter.sum()).unwrap_or_default()
}

fn gauge_bool(gauge: &Gauge) -> bool {
    gauge.get() != 0
}

fn gauge_u64(gauge: &Gauge) -> u64 {
    u64::try_from(gauge.get()).unwrap_or_default()
}

fn gauge_usize(gauge: &Gauge) -> usize {
    usize::try_from(gauge.get()).unwrap_or_default()
}

fn i64_saturating_from_u64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn i64_saturating_from_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::WalStats;

    #[test]
    fn snapshot_reflects_lock_free_updates() {
        let stats = WalStats::enabled();
        stats.record_append(128, 2);
        stats.record_flush(42);
        stats.enable_tcp_export();
        stats.record_tcp_export_queued();
        stats.record_tcp_export_dropped();
        stats.record_tcp_export_sent(3, 384);
        stats.record_tcp_export_write_failures(1);
        stats.record_tcp_export_connect_failure();
        stats.record_tcp_export_subscriber_accepted(2);
        stats.record_tcp_export_subscriber_rejected();
        stats.record_snapshot_written();

        let snapshot = stats.snapshot();
        assert!(snapshot.enabled);
        assert!(snapshot.tcp_export_enabled);
        assert_eq!(snapshot.entries_written, 1);
        assert_eq!(snapshot.bytes_written, 128);
        assert_eq!(snapshot.segments_rotated, 2);
        assert_eq!(snapshot.last_flush_ms, 42);
        assert_eq!(snapshot.snapshots_written, 1);
        assert_eq!(snapshot.tcp_export_frames_queued, 1);
        assert_eq!(snapshot.tcp_export_frames_dropped, 1);
        assert_eq!(snapshot.tcp_export_frames_sent, 3);
        assert_eq!(snapshot.tcp_export_bytes_sent, 384);
        assert_eq!(snapshot.tcp_export_write_failures, 1);
        assert_eq!(snapshot.tcp_export_connect_failures, 1);
        assert_eq!(snapshot.tcp_export_active_subscribers, 2);
        assert_eq!(snapshot.tcp_export_subscribers_accepted, 1);
        assert_eq!(snapshot.tcp_export_subscribers_rejected, 1);
    }
}
