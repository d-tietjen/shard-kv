use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize)]
pub struct TierStatsSnapshot {
    pub name: &'static str,
    pub len: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub promotions: u64,
    pub demotions: u64,
    pub evictions: u64,
    pub expirations: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ShardStatsSnapshot {
    pub shard_id: usize,
    pub key_count: usize,
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub expired: u64,
    pub maintenance_runs: u64,
    pub hot: TierStatsSnapshot,
    pub warm: TierStatsSnapshot,
    pub cold: TierStatsSnapshot,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WalStatsSnapshot {
    pub enabled: bool,
    pub entries_written: u64,
    pub segments_rotated: u64,
    pub bytes_written: u64,
    pub last_flush_ms: u64,
    pub recoveries: u64,
    pub snapshots_written: u64,
    pub tcp_export_enabled: bool,
    pub tcp_export_frames_queued: u64,
    pub tcp_export_frames_sent: u64,
    pub tcp_export_bytes_sent: u64,
    pub tcp_export_frames_dropped: u64,
    pub tcp_export_connect_failures: u64,
    pub tcp_export_write_failures: u64,
    pub tcp_export_active_subscribers: usize,
    pub tcp_export_subscribers_accepted: u64,
    pub tcp_export_subscribers_rejected: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct GlobalStatsSnapshot {
    pub uptime_ms: u64,
    pub shard_count: usize,
    pub total_keys: usize,
    pub total_reads: u64,
    pub total_writes: u64,
    pub total_deletes: u64,
    pub total_expired: u64,
    pub shards: Vec<ShardStatsSnapshot>,
    pub wal: WalStatsSnapshot,
}
