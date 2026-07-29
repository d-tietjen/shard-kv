use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use crate::storage::WalStatsSnapshot;

pub(super) struct WalStats {
    enabled: AtomicBool,
    blocks_merged: AtomicU64,
    entries_written: AtomicU64,
    segments_rotated: AtomicU64,
    bytes_written: AtomicU64,
    last_flush_ms: AtomicU64,
    recoveries: AtomicU64,
    snapshots_written: AtomicU64,
    tcp_export_enabled: AtomicBool,
    tcp_export_frames_queued: AtomicU64,
    tcp_export_frames_sent: AtomicU64,
    tcp_export_bytes_sent: AtomicU64,
    tcp_export_frames_dropped: AtomicU64,
    tcp_export_connect_failures: AtomicU64,
    tcp_export_write_failures: AtomicU64,
    tcp_export_active_subscribers: AtomicUsize,
    tcp_export_subscribers_accepted: AtomicU64,
    tcp_export_subscribers_rejected: AtomicU64,
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
            enabled: self.enabled.load(Ordering::Relaxed),
            blocks_merged: self.blocks_merged.load(Ordering::Relaxed),
            entries_written: self.entries_written.load(Ordering::Relaxed),
            segments_rotated: self.segments_rotated.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            last_flush_ms: self.last_flush_ms.load(Ordering::Relaxed),
            recoveries: self.recoveries.load(Ordering::Relaxed),
            snapshots_written: self.snapshots_written.load(Ordering::Relaxed),
            tcp_export_enabled: self.tcp_export_enabled.load(Ordering::Relaxed),
            tcp_export_frames_queued: self.tcp_export_frames_queued.load(Ordering::Relaxed),
            tcp_export_frames_sent: self.tcp_export_frames_sent.load(Ordering::Relaxed),
            tcp_export_bytes_sent: self.tcp_export_bytes_sent.load(Ordering::Relaxed),
            tcp_export_frames_dropped: self.tcp_export_frames_dropped.load(Ordering::Relaxed),
            tcp_export_connect_failures: self.tcp_export_connect_failures.load(Ordering::Relaxed),
            tcp_export_write_failures: self.tcp_export_write_failures.load(Ordering::Relaxed),
            tcp_export_active_subscribers: self
                .tcp_export_active_subscribers
                .load(Ordering::Relaxed),
            tcp_export_subscribers_accepted: self
                .tcp_export_subscribers_accepted
                .load(Ordering::Relaxed),
            tcp_export_subscribers_rejected: self
                .tcp_export_subscribers_rejected
                .load(Ordering::Relaxed),
        }
    }

    pub(super) fn record_append(&self, bytes: usize, rotations: u64) {
        atomic_add_u64(&self.entries_written, 1);
        atomic_add_u64(&self.bytes_written, bytes as u64);
        atomic_add_u64(&self.segments_rotated, rotations);
    }

    pub(super) fn record_block_merged(&self) {
        atomic_add_u64(&self.blocks_merged, 1);
    }

    pub(super) fn record_flush(&self, timestamp_ms: u64) {
        self.last_flush_ms.store(timestamp_ms, Ordering::Relaxed);
    }

    pub(super) fn record_snapshot_written(&self) {
        atomic_add_u64(&self.snapshots_written, 1);
    }

    pub(super) fn enable_tcp_export(&self) {
        self.tcp_export_enabled.store(true, Ordering::Relaxed);
    }

    pub(super) fn record_tcp_export_queued(&self) {
        atomic_add_u64(&self.tcp_export_frames_queued, 1);
    }

    pub(super) fn record_tcp_export_dropped(&self) {
        atomic_add_u64(&self.tcp_export_frames_dropped, 1);
    }

    pub(super) fn record_tcp_export_sent(&self, frames: u64, bytes: u64) {
        atomic_add_u64(&self.tcp_export_frames_sent, frames);
        atomic_add_u64(&self.tcp_export_bytes_sent, bytes);
    }

    pub(super) fn record_tcp_export_write_failures(&self, failures: u64) {
        atomic_add_u64(&self.tcp_export_write_failures, failures);
    }

    pub(super) fn record_tcp_export_connect_failure(&self) {
        atomic_add_u64(&self.tcp_export_connect_failures, 1);
    }

    pub(super) fn record_tcp_export_subscriber_accepted(&self, active: usize) {
        atomic_add_u64(&self.tcp_export_subscribers_accepted, 1);
        self.set_tcp_export_active_subscribers(active);
    }

    pub(super) fn record_tcp_export_subscriber_rejected(&self) {
        atomic_add_u64(&self.tcp_export_subscribers_rejected, 1);
    }

    pub(super) fn set_tcp_export_active_subscribers(&self, active: usize) {
        self.tcp_export_active_subscribers
            .store(active, Ordering::Relaxed);
    }

    fn new(enabled: bool) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
            blocks_merged: AtomicU64::new(0),
            entries_written: AtomicU64::new(0),
            segments_rotated: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            last_flush_ms: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
            snapshots_written: AtomicU64::new(0),
            tcp_export_enabled: AtomicBool::new(false),
            tcp_export_frames_queued: AtomicU64::new(0),
            tcp_export_frames_sent: AtomicU64::new(0),
            tcp_export_bytes_sent: AtomicU64::new(0),
            tcp_export_frames_dropped: AtomicU64::new(0),
            tcp_export_connect_failures: AtomicU64::new(0),
            tcp_export_write_failures: AtomicU64::new(0),
            tcp_export_active_subscribers: AtomicUsize::new(0),
            tcp_export_subscribers_accepted: AtomicU64::new(0),
            tcp_export_subscribers_rejected: AtomicU64::new(0),
        }
    }
}

fn atomic_add_u64(target: &AtomicU64, value: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

#[cfg(test)]
mod tests {
    use super::WalStats;

    #[test]
    fn snapshot_reflects_lock_free_updates() {
        let stats = WalStats::enabled();
        stats.record_block_merged();
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
        assert_eq!(snapshot.blocks_merged, 1);
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
