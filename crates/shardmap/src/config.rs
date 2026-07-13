//! Runtime configuration for the embedded store and optional server.
//!
//! [`ShardCacheConfig`] can be loaded from TOML with
//! [`ShardCacheConfig::load_from_path`] or serialized with
//! [`ShardCacheConfig::store_to_path`]. The defaults are suitable for local
//! development and derive the shard count and tier sizes from the host when
//! possible.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

mod geometry;
mod validation;

use crate::Result;
use crate::cuda::CudaConfig;

use geometry::{CacheGeometryDetector, DefaultShardCount, HotTierCapacity};
use validation::ConfigValidator;

/// Top-level configuration for `shardcache`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShardCacheConfig {
    /// Socket address the server should bind, for example `127.0.0.1:6380`.
    pub bind_addr: String,
    /// Maximum number of accepted client connections.
    pub max_connections: usize,
    /// Number of storage shards to create. Must be a non-zero power of two.
    pub shard_count: usize,
    /// Global memory budget in bytes. `0` disables memory-limit eviction.
    pub max_memory_bytes: u64,
    /// Policy used when a memory limit is configured.
    pub eviction_policy: EvictionPolicy,
    /// Interval between TTL maintenance sweeps.
    pub ttl_sweep_interval_ms: u64,
    /// Interval between periodic stats reports.
    pub stats_interval_ms: u64,
    /// Per-shard tier sizing.
    pub tiers: TierConfig,
    /// GPU-facing configuration values.
    pub cuda: CudaConfig,
    /// WAL and snapshot configuration.
    pub persistence: PersistenceConfig,
    /// Object-storage overflow configuration for cold values.
    pub object_overflow: ObjectOverflowConfig,
    /// Partitioned key-value overflow configuration for cold values.
    pub kv_overflow: KvOverflowConfig,
    /// Native mutation-stream replication configuration.
    pub replication: ReplicationConfig,
    /// Redis transaction execution mode.
    pub transaction_mode: TransactionMode,
    /// Public server endpoint topology.
    ///
    /// `Fanout` exposes one listener. Caller-owned embedded stores route
    /// fanout requests to shard owners; standalone direct servers keep their
    /// compatibility fanout behavior. `DirectShard` also exposes shard-owned
    /// ports for clients that can route directly.
    pub server_endpoint_mode: ServerEndpointMode,
}

/// Memory-limit eviction policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvictionPolicy {
    /// Do not evict entries because of memory pressure.
    #[default]
    None,
    /// Evict least-recently-used entries first.
    Lru,
    /// Evict least-frequently-used entries first.
    Lfu,
    /// Evict cold prefix groups first, then cold suffix blocks inside that group.
    #[cfg(feature = "prefix-eviction")]
    Prefix,
}

/// Redis-compatible transaction execution policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TransactionMode {
    /// Reject MULTI/EXEC/DISCARD.
    Disabled,
    /// Allow transactions only when all queued keys route to one shard.
    #[default]
    ShardLocal,
    /// Coordinate transactions across all affected shards using router-level gates.
    CoordinatedCrossShard,
}

/// Public server endpoint topology.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ServerEndpointMode {
    /// One public listener.
    #[default]
    Fanout,
    /// Expose shard-owned direct ports in addition to the fanout listener.
    DirectShard,
}

/// Capacity settings for the hot, warm, and cold in-memory tiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TierConfig {
    /// Target number of entries in the CPU-cache-sized hot tier.
    pub hot_capacity: usize,
    /// Target number of entries in the warm tier.
    pub warm_capacity: usize,
    /// Target number of entries in the cold tier.
    pub cold_capacity: usize,
    /// Maximum number of entries promoted during one maintenance pass.
    pub promotion_batch: usize,
}

/// WAL and snapshot persistence settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PersistenceConfig {
    /// Enable WAL and snapshot persistence.
    pub enabled: bool,
    /// Directory used for WAL segments and snapshots.
    pub data_dir: PathBuf,
    /// Approximate WAL segment size before rotation.
    pub segment_size_bytes: u64,
    /// Maximum interval between WAL fsync calls.
    pub fsync_interval_ms: u64,
    /// Snapshot cadence in seconds.
    pub snapshot_every_seconds: u64,
    /// Minimum writes before a periodic snapshot is considered.
    pub snapshot_min_writes: u64,
    /// Compress snapshot files.
    pub compress_snapshots: bool,
    /// Compress WAL segments.
    pub compress_wal: bool,
    /// Bounded channel capacity for WAL append requests.
    pub wal_channel_capacity: usize,
    /// Optional live WAL export over TCP.
    pub tcp_export: WalTcpExportConfig,
}

/// Object storage overflow settings.
///
/// When enabled, memory-pressure eviction can move cold byte-string values out
/// of resident memory while keeping key metadata in the shard. The existing
/// local WAL/snapshot path remains the authoritative durability source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ObjectOverflowConfig {
    /// Enable object-storage overflow.
    pub enabled: bool,
    /// Object overflow backend.
    pub backend: ObjectOverflowBackend,
    /// Object store endpoint. V1 accepts a filesystem path or `file://` URL for
    /// the built-in adapter used by tests and local deployments. The S3 backend
    /// treats this as an S3-compatible endpoint for RustFS/MinIO-compatible
    /// deployments.
    pub endpoint: String,
    /// Object bucket or namespace.
    pub bucket: String,
    /// Object key prefix under the bucket.
    pub prefix: String,
    /// Required stable writer identity when cleanup is enabled. Writers sharing
    /// a prefix must use distinct node IDs.
    pub node_id: Option<String>,
    /// S3 region for RustFS/S3-compatible stores.
    pub region: String,
    /// Use path-style requests for S3-compatible stores.
    pub force_path_style: bool,
    /// Permit HTTP endpoints for local RustFS/S3-compatible stores.
    pub allow_http: bool,
    /// Verify TLS certificates for HTTPS S3-compatible stores.
    pub tls_verify: bool,
    /// Optional server-side encryption algorithm or key reference.
    pub server_side_encryption: Option<String>,
    /// Environment variable that contains the access key for rust-fs/S3 stores.
    pub access_key_env: Option<String>,
    /// Environment variable that contains the secret key for rust-fs/S3 stores.
    pub secret_key_env: Option<String>,
    /// Minimum value size eligible for offload.
    pub min_value_bytes: usize,
    /// Minimum access-clock idle ticks before a resident value can be offloaded.
    pub offload_min_idle_ticks: u64,
    /// Maximum recorded access frequency eligible for offload.
    pub offload_max_frequency: u32,
    /// Compression codec for offloaded values.
    pub compression: ObjectOverflowCompression,
    /// zstd compression level when `compression = "zstd"`.
    pub zstd_level: i32,
    /// Failure behavior when object offload cannot store a value.
    pub failure_policy: ObjectOverflowFailurePolicy,
    /// Maximum object-store retries after the first attempt.
    pub max_retries: usize,
    /// Delay between retry attempts.
    pub retry_backoff_ms: u64,
    /// Operation timeout budget for object-store adapters that support it.
    pub operation_timeout_ms: u64,
    /// Number of bounded object-overflow worker threads.
    pub worker_threads: usize,
    /// Maximum queued object-overflow jobs before offload backpressure applies.
    pub queue_capacity: usize,
    /// Consecutive failures before pausing new offloads.
    pub degraded_failure_threshold: usize,
    /// Cooldown for the degraded state after repeated failures.
    pub degraded_cooldown_ms: u64,
    /// Remove stale generation objects during startup.
    pub cleanup_on_start: bool,
    /// Background cleanup interval. `0` disables periodic cleanup.
    pub cleanup_interval_seconds: u64,
    /// Minimum stale generation age before cleanup may delete objects.
    pub cleanup_grace_seconds: u64,
    /// Fetch cold values on owned GET.
    pub fetch_on_get: bool,
    /// Delete remote payloads when a key is overwritten or removed.
    pub delete_on_overwrite: bool,
}

/// Object-overflow backend.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObjectOverflowBackend {
    /// Local filesystem-backed object store.
    #[default]
    File,
    /// RustFS/S3-compatible object store.
    S3,
}

/// Compression codec for object-overflow payloads.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObjectOverflowCompression {
    /// Store payloads without compression.
    None,
    /// Compress payloads with lz4 size-prepended compression.
    Lz4,
    /// Compress payloads with zstd.
    #[default]
    Zstd,
}

/// Failure policy for object-overflow offload writes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObjectOverflowFailurePolicy {
    /// Keep the resident value if offload fails. Memory may temporarily exceed
    /// the configured target, but data remains resident.
    #[default]
    RetainResident,
    /// Fall back to normal cache eviction if offload fails.
    EvictResident,
}

/// Partitioned key-value overflow settings.
///
/// Each key is mirrored to one deterministic shardcache server. The embedded
/// primary may then evict acknowledged cold values and fault them back from
/// their owner. Overflow servers hold disjoint key partitions rather than full
/// read-replica copies.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KvOverflowConfig {
    /// Enable the embedded key-value overflow wrapper.
    pub enabled: bool,
    /// Stable SCNP addresses for the current overflow membership.
    pub endpoints: Vec<String>,
    /// Previous membership retained during an online slot handoff.
    ///
    /// Keep this empty for steady state. During horizontal expansion, set it
    /// to the membership that previously owned the logical slots until the
    /// authoritative primary has resynchronized the new membership.
    pub previous_endpoints: Vec<String>,
    /// Fixed logical slot count used for overflow ownership.
    ///
    /// This must remain unchanged for the lifetime of the overflow data.
    pub slot_count: u32,
    /// Total resident-byte target for the in-memory primary.
    pub max_memory_bytes: u64,
    /// Policy used to choose acknowledged resident values for offload.
    pub eviction_policy: EvictionPolicy,
    /// TCP connections retained per overflow node.
    pub connections_per_endpoint: usize,
    /// Maximum TCP connect duration.
    pub connect_timeout_ms: u64,
    /// Maximum duration for one SCNP operation.
    pub operation_timeout_ms: u64,
    /// Retries after the first failed SCNP attempt.
    pub max_retries: usize,
    /// Delay between retries.
    pub retry_backoff_ms: u64,
    /// Interval for deleting expired values from overflow nodes.
    pub cleanup_interval_ms: u64,
    /// Promote remotely found values back into primary memory.
    pub fetch_on_miss: bool,
    /// Number of ordered remote replication workers.
    pub worker_threads: usize,
    /// Maximum total queued and active remote replication jobs.
    pub queue_capacity: usize,
}

/// Optional live WAL export settings.
///
/// The TCP exporter streams the same framed WAL records used on disk. It is a
/// live feed, not a replay service; disk WAL segments remain the authoritative
/// recovery source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalTcpExportConfig {
    /// Enable the TCP WAL exporter.
    pub enabled: bool,
    /// TCP export mode.
    pub mode: WalTcpExportMode,
    /// Address the exporter connects to or listens on, for example `127.0.0.1:7630`.
    pub addr: String,
    /// Optional plaintext authentication token.
    ///
    /// In `connect` mode, the token is sent to the configured collector before
    /// WAL frames. In `listen` mode, subscribers must send the token before they
    /// receive WAL frames.
    pub auth_token: Option<String>,
    /// Bounded queue between the disk WAL writer and the TCP exporter.
    pub channel_capacity: usize,
    /// Maximum accepted subscribers in `listen` mode.
    pub max_subscribers: usize,
    /// Maximum time spent opening one TCP connection attempt.
    pub connect_timeout_ms: u64,
    /// Maximum time spent writing a frame before reconnecting.
    pub write_timeout_ms: u64,
    /// Delay between reconnect attempts after connect or write failure.
    pub reconnect_backoff_ms: u64,
    /// If true, a full TCP export queue backpressures the WAL writer.
    ///
    /// If false, frames are dropped from the live TCP export when the exporter
    /// cannot keep up; disk WAL append still continues.
    pub backpressure_on_full: bool,
}

/// Native replication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationConfig {
    /// Enable native mutation-stream replication.
    pub enabled: bool,
    /// Runtime role for this process.
    pub role: ReplicationRole,
    /// Address a primary listens on for replicas and service subscribers.
    pub bind_addr: String,
    /// Primary address a replica connects to.
    pub replica_of: Option<String>,
    /// Optional plaintext authentication token for native replication.
    pub auth_token: Option<String>,
    /// Compression algorithm for mutation batches and snapshot chunks.
    pub compression: ReplicationCompression,
    /// zstd compression level used when `compression = "zstd"`.
    pub zstd_level: i32,
    /// Send policy for primary mutation flushes.
    pub send_policy: ReplicationSendPolicy,
    /// Maximum records in one mutation batch.
    pub batch_max_records: usize,
    /// Maximum uncompressed bytes in one mutation batch.
    pub batch_max_bytes: usize,
    /// Maximum time a non-empty batch may wait before flush.
    pub batch_max_delay_us: u64,
    /// Approximate retained in-memory backlog size for partial catch-up.
    pub backlog_bytes: usize,
    /// Snapshot chunk size before compression.
    pub snapshot_chunk_bytes: usize,
    /// Per-shard bounded queue capacity for ready replication batches.
    ///
    /// The shard worker builds ordered mutation batches locally. When this
    /// queue is full, that shard's emitting thread blocks until its exporter
    /// drains a batch. Increase this if export lanes cannot keep up with
    /// bursty writes.
    pub queue_capacity: usize,
    /// Maximum simultaneously-connected replicas in `listen` mode.
    pub max_replicas: usize,
    /// Maximum time spent opening one TCP connect attempt from a replica.
    pub connect_timeout_ms: u64,
    /// Per-write timeout for replication TCP I/O.
    pub write_timeout_ms: u64,
    /// Delay between reconnect attempts after a replica disconnect.
    pub reconnect_backoff_ms: u64,
    /// Per-subscriber outbound channel capacity.
    pub subscriber_channel_capacity: usize,
}

/// Native replication role.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationRole {
    /// Emit local writes and serve replicas.
    #[default]
    Primary,
    /// Receive and apply writes from a primary.
    Replica,
}

/// Native replication compression.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationCompression {
    /// Do not compress replication payloads.
    None,
    /// Compress replication payloads with zstd.
    #[default]
    Zstd,
}

/// Native replication send policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationSendPolicy {
    /// Flush each mutation as a one-record batch.
    Immediate,
    /// Accumulate mutations until a record, byte, or delay threshold is reached.
    #[default]
    Batch,
}

/// TCP WAL export topology.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WalTcpExportMode {
    /// Connect to a single downstream collector and push live WAL frames.
    Connect,
    /// Listen for authenticated subscribers and fan out live WAL frames.
    Listen,
}

/// Host CPU cache geometry used to derive tier defaults.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CacheGeometry {
    /// L1 data cache size in bytes.
    pub l1d_bytes: usize,
    /// L2 cache size in bytes.
    pub l2_bytes: usize,
    /// L3 cache size in bytes.
    pub l3_bytes: usize,
}

struct ConfigFile<'a> {
    path: &'a Path,
}

enum ConfigFileParent<'a> {
    Present(&'a Path),
    Missing,
}

impl Default for ShardCacheConfig {
    fn default() -> Self {
        let shard_count = Self::default_shard_count();
        Self {
            bind_addr: "127.0.0.1:6380".to_string(),
            max_connections: 4_096,
            shard_count,
            max_memory_bytes: 0,
            eviction_policy: EvictionPolicy::None,
            ttl_sweep_interval_ms: 1_000,
            stats_interval_ms: 5_000,
            tiers: TierConfig::from_geometry(CacheGeometry::detect_current_host(), shard_count),
            cuda: CudaConfig::default(),
            persistence: PersistenceConfig::default(),
            object_overflow: ObjectOverflowConfig::default(),
            kv_overflow: KvOverflowConfig::default(),
            replication: ReplicationConfig::default(),
            transaction_mode: TransactionMode::default(),
            server_endpoint_mode: ServerEndpointMode::default(),
        }
    }
}

impl Default for TierConfig {
    fn default() -> Self {
        Self::from_geometry(
            CacheGeometry::detect_current_host(),
            ShardCacheConfig::default_shard_count(),
        )
    }
}

impl TierConfig {
    /// Builds tier capacities from CPU cache geometry and shard count.
    pub fn from_geometry(geometry: CacheGeometry, shard_count: usize) -> Self {
        let hot_capacity = HotTierCapacity::from_l1(geometry.l1d_bytes);
        let warm_capacity = (geometry.l2_bytes / 160).clamp(1_024, 131_072);
        let cold_bytes_per_shard = usize::max(
            geometry.l3_bytes / usize::max(shard_count, 1),
            2 * 1024 * 1024,
        );
        let cold_capacity = (cold_bytes_per_shard / 192).clamp(8_192, 1_000_000);

        Self {
            hot_capacity,
            warm_capacity,
            cold_capacity,
            promotion_batch: 256,
        }
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            data_dir: PathBuf::from("./var/shardcache"),
            segment_size_bytes: 64 * 1024 * 1024,
            fsync_interval_ms: 100,
            snapshot_every_seconds: 300,
            snapshot_min_writes: 1_000,
            compress_snapshots: true,
            compress_wal: true,
            wal_channel_capacity: 16_384,
            tcp_export: WalTcpExportConfig::default(),
        }
    }
}

impl Default for ObjectOverflowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: ObjectOverflowBackend::default(),
            endpoint: String::new(),
            bucket: String::new(),
            prefix: "shardcache/overflow".to_string(),
            node_id: None,
            region: "us-east-1".to_string(),
            force_path_style: true,
            allow_http: false,
            tls_verify: true,
            server_side_encryption: None,
            access_key_env: None,
            secret_key_env: None,
            min_value_bytes: 4 * 1024,
            offload_min_idle_ticks: 1024,
            offload_max_frequency: u32::MAX,
            compression: ObjectOverflowCompression::default(),
            zstd_level: 3,
            failure_policy: ObjectOverflowFailurePolicy::default(),
            max_retries: 2,
            retry_backoff_ms: 10,
            operation_timeout_ms: 500,
            worker_threads: 2,
            queue_capacity: 1024,
            degraded_failure_threshold: 8,
            degraded_cooldown_ms: 5_000,
            cleanup_on_start: false,
            cleanup_interval_seconds: 0,
            cleanup_grace_seconds: 86_400,
            fetch_on_get: true,
            delete_on_overwrite: true,
        }
    }
}

impl Default for KvOverflowConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoints: Vec::new(),
            previous_endpoints: Vec::new(),
            slot_count: 16_384,
            max_memory_bytes: 0,
            eviction_policy: EvictionPolicy::Lru,
            connections_per_endpoint: 2,
            connect_timeout_ms: 250,
            operation_timeout_ms: 500,
            max_retries: 2,
            retry_backoff_ms: 10,
            cleanup_interval_ms: 1_000,
            fetch_on_miss: true,
            worker_threads: 2,
            queue_capacity: 1_024,
        }
    }
}

impl Default for WalTcpExportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: WalTcpExportMode::Connect,
            addr: "127.0.0.1:7630".to_string(),
            auth_token: None,
            channel_capacity: 16_384,
            max_subscribers: 64,
            connect_timeout_ms: 250,
            write_timeout_ms: 250,
            reconnect_backoff_ms: 100,
            backpressure_on_full: false,
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            role: ReplicationRole::Primary,
            bind_addr: "127.0.0.1:7631".to_string(),
            replica_of: None,
            auth_token: None,
            compression: ReplicationCompression::None,
            zstd_level: 3,
            send_policy: ReplicationSendPolicy::Batch,
            batch_max_records: 512,
            batch_max_bytes: 1024 * 1024,
            batch_max_delay_us: 750,
            backlog_bytes: 64 * 1024 * 1024,
            snapshot_chunk_bytes: 1024 * 1024,
            queue_capacity: 16_384,
            max_replicas: 16,
            connect_timeout_ms: 500,
            write_timeout_ms: 500,
            reconnect_backoff_ms: 200,
            subscriber_channel_capacity: 1_024,
        }
    }
}

impl ShardCacheConfig {
    /// Returns the default shard count for the current host.
    pub fn default_shard_count() -> usize {
        DefaultShardCount::current()
    }

    /// Loads and validates a TOML configuration file.
    pub fn load_from_path(path: &Path) -> Result<Self> {
        let config = ConfigFile::new(path).load()?;
        config.validate()?;
        Ok(config)
    }

    /// Writes the configuration as pretty TOML, creating parent directories.
    pub fn store_to_path(&self, path: &Path) -> Result<()> {
        ConfigFile::new(path).store(self)
    }

    /// Creates directories required by the enabled persistence settings.
    pub fn ensure_paths(&self) -> Result<()> {
        self.persistence.ensure_paths()
    }

    /// Validates cross-field constraints.
    pub fn validate(&self) -> Result<()> {
        ConfigValidator::new(self).validate()
    }

    /// Returns the TTL sweep interval, clamped to at least 1 ms.
    pub fn ttl_sweep_interval(&self) -> Duration {
        Duration::from_millis(self.ttl_sweep_interval_ms.max(1))
    }

    /// Returns the stats interval, clamped to at least 250 ms.
    pub fn stats_interval(&self) -> Duration {
        Duration::from_millis(self.stats_interval_ms.max(250))
    }

    /// Returns the per-shard memory limit implied by `max_memory_bytes`.
    pub fn per_shard_memory_limit_bytes(&self) -> Option<usize> {
        match self.max_memory_bytes {
            0 => None,
            bytes => {
                let shard_count = self.shard_count as u64;
                Some(bytes.div_ceil(shard_count) as usize)
            }
        }
    }

    /// Returns the total memory limit when one is configured.
    pub fn total_memory_limit_bytes(&self) -> Option<usize> {
        match self.max_memory_bytes {
            0 => None,
            bytes => Some(bytes as usize),
        }
    }

    /// Returns the snapshot interval, clamped to at least 1 second.
    pub fn snapshot_interval(&self) -> Duration {
        Duration::from_secs(self.persistence.snapshot_every_seconds.max(1))
    }
}

impl CacheGeometry {
    /// Detects CPU cache geometry from the current host when possible.
    pub fn detect_current_host() -> Self {
        CacheGeometryDetector::detect()
    }
}

impl<'a> ConfigFile<'a> {
    fn new(path: &'a Path) -> Self {
        ConfigFile { path }
    }

    fn load(&self) -> Result<ShardCacheConfig> {
        let contents = std::fs::read_to_string(self.path)?;
        Ok(toml::from_str(&contents)?)
    }

    fn store(&self, config: &ShardCacheConfig) -> Result<()> {
        self.ensure_parent()?;
        let contents = toml::to_string_pretty(config)?;
        std::fs::write(self.path, contents)?;
        Ok(())
    }

    fn ensure_parent(&self) -> Result<()> {
        match ConfigFileParent::from_path(self.path) {
            ConfigFileParent::Present(parent) => {
                std::fs::create_dir_all(parent)?;
                Ok(())
            }
            ConfigFileParent::Missing => Ok(()),
        }
    }
}

impl<'a> ConfigFileParent<'a> {
    fn from_path(path: &'a Path) -> Self {
        match path.parent() {
            Some(parent) => Self::Present(parent),
            None => Self::Missing,
        }
    }
}

impl PersistenceConfig {
    fn ensure_paths(&self) -> Result<()> {
        match self.enabled {
            true => {
                std::fs::create_dir_all(&self.data_dir)?;
                Ok(())
            }
            false => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "kv-overflow")]
    use super::{EvictionPolicy, KvOverflowConfig};
    use super::{ServerEndpointMode, ShardCacheConfig, geometry::CacheSizeParser};

    #[test]
    fn parses_cache_sizes() {
        assert_eq!(CacheSizeParser::parse("32K"), Some(32 * 1024));
        assert_eq!(CacheSizeParser::parse("4M"), Some(4 * 1024 * 1024));
        assert_eq!(CacheSizeParser::parse("65536"), Some(65_536));
    }

    #[test]
    fn validates_power_of_two_shard_count() {
        for shard_count in [0, 3, 10, 12] {
            let config = ShardCacheConfig {
                shard_count,
                ..ShardCacheConfig::default()
            };
            assert!(config.validate().is_err(), "{shard_count} should fail");
        }

        for shard_count in [1, 2, 4, 8, 16, 32] {
            let config = ShardCacheConfig {
                shard_count,
                ..ShardCacheConfig::default()
            };
            assert!(config.validate().is_ok(), "{shard_count} should pass");
        }
    }

    #[test]
    fn default_shard_count_is_power_of_two() {
        let shard_count = ShardCacheConfig::default_shard_count();
        assert!(shard_count > 0);
        assert!(shard_count.is_power_of_two());
    }

    #[test]
    fn default_server_endpoint_mode_is_fanout() {
        assert_eq!(
            ShardCacheConfig::default().server_endpoint_mode,
            ServerEndpointMode::Fanout
        );
    }

    #[test]
    fn parses_server_endpoint_mode_from_toml() {
        let config: ShardCacheConfig =
            toml::from_str(r#"server_endpoint_mode = "direct_shard""#).unwrap();

        assert_eq!(config.server_endpoint_mode, ServerEndpointMode::DirectShard);
    }

    #[cfg(feature = "kv-overflow")]
    #[test]
    fn validates_key_value_overflow_membership() {
        let mut config = ShardCacheConfig {
            kv_overflow: KvOverflowConfig {
                enabled: true,
                endpoints: vec!["127.0.0.1:6381".into(), "127.0.0.1:6382".into()],
                max_memory_bytes: 1024,
                eviction_policy: EvictionPolicy::Lfu,
                ..KvOverflowConfig::default()
            },
            ..ShardCacheConfig::default()
        };
        assert!(config.validate().is_ok());

        config.kv_overflow.endpoints[1] = config.kv_overflow.endpoints[0].clone();
        assert!(config.validate().is_err());

        config.kv_overflow.endpoints[1] = "127.0.0.1:6382".into();
        config.kv_overflow.previous_endpoints =
            vec!["127.0.0.1:6381".into(), "127.0.0.1:6381".into()];
        assert!(config.validate().is_err());

        config.kv_overflow.previous_endpoints.pop();
        config.kv_overflow.slot_count = 10_000;
        assert!(config.validate().is_err());
    }
}
