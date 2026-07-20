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
    /// Enables this server as a shard-addressable key-value overflow replica.
    pub kv_overflow_replica: KvOverflowReplicaServerConfig,
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

/// Server-side role configuration for a dedicated key-value overflow replica.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct KvOverflowReplicaServerConfig {
    /// Enable the overflow-slot routing contract on this server.
    pub enabled: bool,
    /// Stable node identity reported to overflow primaries.
    pub node_id: String,
    /// Environment variable containing the SCNP authentication token.
    pub auth_token_env: Option<String>,
    /// File containing a reloadable SCNP authentication token.
    pub auth_token_path: Option<PathBuf>,
    /// Interval between authentication token file reloads.
    ///
    /// Zero uses a 30 second default.
    pub auth_token_reload_interval_ms: u64,
    /// Native TLS and optional client-certificate verification.
    pub tls: ScnpTlsServerConfig,
    /// Permit unauthenticated SCNP on a non-loopback listener.
    ///
    /// Authentication does not encrypt traffic. Production deployments should
    /// still use a private network or an encrypted transport overlay.
    pub allow_insecure_scnp: bool,
    /// Permit ordinary LRU/LFU eviction without object overflow.
    ///
    /// This makes the overflow replica intentionally lossy.
    pub allow_lossy_eviction: bool,
    /// Confirm that the persistence directory is protected by encrypted storage.
    ///
    /// Overflow replicas persist value envelopes in WAL and snapshots. Set
    /// this only when the filesystem or attached volume encrypts data at rest.
    pub encrypted_persistence: bool,
}

/// Server-side TLS identity for SCNP overflow listeners.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScnpTlsServerConfig {
    /// Require TLS on the overflow replica's SCNP listeners.
    pub enabled: bool,
    /// PEM certificate chain presented by the replica.
    pub cert_path: PathBuf,
    /// PEM private key for `cert_path`.
    pub key_path: PathBuf,
    /// PEM CA bundle used to require and verify client certificates.
    pub client_ca_path: Option<PathBuf>,
    /// Allowed SHA-256 fingerprints for mTLS leaf certificates.
    ///
    /// When configured, a CA-valid client certificate must also match one of
    /// these identities. Multiple entries permit overlap during rotation.
    pub client_cert_sha256: Vec<String>,
    /// Maximum TLS handshake duration.
    pub handshake_timeout_ms: u64,
    /// Interval between certificate, key, and CA reload checks.
    pub reload_interval_ms: u64,
    /// Maximum simultaneous TLS handshakes accepted by this process.
    pub max_concurrent_handshakes: usize,
}

impl Default for ScnpTlsServerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            client_ca_path: None,
            client_cert_sha256: Vec::new(),
            handshake_timeout_ms: 5_000,
            reload_interval_ms: 30_000,
            max_concurrent_handshakes: 256,
        }
    }
}

/// Client-side trust and identity for SCNP overflow connections.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ScnpTlsClientConfig {
    /// Verify and encrypt SCNP connections with TLS.
    pub enabled: bool,
    /// PEM CA bundle used to verify replica certificates.
    pub ca_path: PathBuf,
    /// Optional PEM client certificate chain for mTLS.
    pub client_cert_path: Option<PathBuf>,
    /// Optional PEM client private key for mTLS.
    pub client_key_path: Option<PathBuf>,
    /// TLS server name for legacy address-only endpoint configuration.
    pub server_name: Option<String>,
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
    /// Approximate per-shard record capacity of the bounded WAL block queue.
    pub wal_channel_capacity: usize,
    /// Maximum records accumulated by one shard before publishing a WAL block.
    pub wal_block_max_records: usize,
    /// Approximate maximum bytes accumulated in one shard-local WAL block.
    pub wal_block_max_bytes: usize,
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

/// Largest fixed slot table accepted by key-value overflow.
pub const MAX_KV_OVERFLOW_SLOT_COUNT: u32 = 1_048_576;

/// Partitioned key-value overflow settings.
///
/// Each key is mirrored to one deterministic shardcache or Redis-compatible
/// endpoint. The embedded primary may then evict acknowledged cold values and
/// fault them back from their owner. Overflow endpoints hold disjoint key
/// partitions rather than full read-replica copies.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KvOverflowConfig {
    /// Enable the embedded key-value overflow wrapper.
    pub enabled: bool,
    /// Protocol used by every endpoint in this overflow membership.
    pub backend: KvOverflowBackend,
    /// Stable replica membership. New configurations should use this instead
    /// of `endpoints` so network addresses can change without moving slots.
    pub replicas: Vec<KvOverflowReplica>,
    /// Previous stable membership retained while ranges are handed off.
    pub previous_replicas: Vec<KvOverflowReplica>,
    /// Namespace included in internal overflow keys.
    pub cluster_id: String,
    /// SCNP transport topology.
    pub transport: KvOverflowTransport,
    /// Stable SCNP addresses for the current overflow membership.
    pub endpoints: Vec<String>,
    /// Previous membership retained during an online slot handoff.
    ///
    /// Keep this empty for steady state. During horizontal expansion, set it
    /// to the membership that previously owned the logical slots until the
    /// authoritative primary has resynchronized the new membership.
    pub previous_endpoints: Vec<String>,
    /// Embedded primary shard count that owned the previous membership.
    ///
    /// Set this only when changing `shard_count` during a restart handoff.
    /// When omitted, the previous membership uses the current primary shard
    /// count.
    pub previous_primary_shard_count: Option<usize>,
    /// Fixed logical slot count used for overflow ownership.
    ///
    /// This must remain unchanged for the lifetime of the overflow data and
    /// cannot exceed [`MAX_KV_OVERFLOW_SLOT_COUNT`].
    pub slot_count: u32,
    /// Prefix prepended to binary keys stored in a Redis-compatible backend.
    pub redis_key_prefix: String,
    /// Environment variable containing an optional Redis ACL username.
    pub redis_username_env: Option<String>,
    /// Environment variable containing the Redis password.
    pub redis_password_env: Option<String>,
    /// Environment variable containing the SCNP authentication token.
    pub scnp_auth_token_env: Option<String>,
    /// File containing a reloadable SCNP authentication token.
    pub scnp_auth_token_path: Option<PathBuf>,
    /// Interval between primary-side token file reloads; zero uses 30 seconds.
    pub scnp_auth_token_reload_interval_ms: u64,
    /// Native SCNP TLS trust and optional mTLS identity.
    pub scnp_tls: ScnpTlsClientConfig,
    /// Permit unauthenticated SCNP connections to non-loopback replicas.
    ///
    /// Authentication does not encrypt traffic. Production deployments should
    /// still use a private network or an encrypted transport overlay.
    pub allow_insecure_scnp: bool,
    /// Total resident-byte target for the in-memory primary.
    pub max_memory_bytes: u64,
    /// Maximum primary memory reserved for tracking logical overflow keys.
    /// Zero derives a limit equal to 25% of `max_memory_bytes`.
    pub max_metadata_bytes: u64,
    /// Maximum key length admitted into the overflow metadata index.
    pub max_key_bytes: usize,
    /// Policy used to choose acknowledged resident values for offload.
    pub eviction_policy: EvictionPolicy,
    /// TCP connections retained per overflow node.
    pub connections_per_endpoint: usize,
    /// Maximum queued plus active jobs for each primary shard.
    pub queue_capacity_per_shard: usize,
    /// Maximum requests emitted in one ordered transport pipeline.
    pub pipeline_max_items: usize,
    /// Maximum key and value bytes emitted in one ordered transport pipeline.
    pub pipeline_max_bytes: usize,
    /// Maximum coalescing delay before a partial pipeline is flushed.
    pub pipeline_flush_micros: u64,
    /// Maximum concurrently active batches for one remote target.
    pub max_inflight_per_target: usize,
    /// Adjust pipeline item limits from observed target RTT.
    pub adaptive_pipeline: bool,
    /// RTT target used by adaptive pipeline sizing.
    pub pipeline_target_micros: u64,
    /// Pause a target after this many consecutive transport failures.
    pub circuit_breaker_failure_threshold: usize,
    /// Duration a failed target remains open before a half-open probe.
    pub circuit_breaker_cooldown_ms: u64,
    /// Optional compression applied only by overflow workers.
    pub compression: KvOverflowCompression,
    /// Permit writes using the v2 compressed value envelope.
    ///
    /// Enable only after every primary that may read this cluster supports v2.
    pub allow_envelope_v2_writes: bool,
    /// Minimum raw value size eligible for KV overflow compression.
    pub compression_min_value_bytes: usize,
    /// Minimum percentage reduction required to retain compression.
    pub compression_min_savings_percent: u8,
    /// Maximum decoded value accepted from an overflow replica.
    pub max_value_bytes: usize,
    /// Maximum permitted decoded-to-stored ratio for compressed values.
    pub compression_max_expansion_ratio: usize,
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
    /// Forget remote metadata after a clean miss.
    ///
    /// Keep this false for durable caches so a missing replica value makes the
    /// next snapshot fail loudly instead of silently omitting the key.
    pub forget_remote_misses: bool,
    /// Aggregate handoff bandwidth limit per primary shard; zero is unlimited.
    pub handoff_max_bytes_per_second: u64,
    /// Maximum primary shards migrated concurrently during a handoff.
    pub handoff_max_concurrency: usize,
    /// Maximum keys retained in memory per primary shard during handoff.
    pub handoff_batch_items: usize,
    /// Maximum capacity retained by reusable per-connection buffers.
    pub retained_buffer_bytes: usize,
    /// Legacy pre-0.6 worker setting. Shard-owned overflow always starts
    /// exactly one network drain for each primary shard.
    pub worker_threads: usize,
    /// Legacy pre-0.6 queue setting. Configured stores derive total capacity
    /// from `queue_capacity_per_shard`.
    pub queue_capacity: usize,
}

/// Stable identity and network paths for one overflow replica.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KvOverflowReplica {
    /// Stable membership identity. It must not contain credentials or an
    /// ephemeral IP address.
    pub id: String,
    /// Bootstrap addresses for the same replica, ordered by preference.
    pub addresses: Vec<String>,
    /// Expected SCNP shard count. Redis ignores this field.
    pub shard_count: usize,
    /// First direct-shard port. Zero derives it as bootstrap port plus one.
    pub direct_shard_base_port: u16,
    /// Certificate DNS/IP name. Defaults to the stable replica ID.
    pub tls_server_name: Option<String>,
}

impl Default for KvOverflowReplica {
    fn default() -> Self {
        Self {
            id: String::new(),
            addresses: Vec::new(),
            shard_count: 1,
            direct_shard_base_port: 0,
            tls_server_name: None,
        }
    }
}

/// Compression used for values stored on KV overflow replicas.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KvOverflowCompression {
    /// Preserve the existing zero-compression envelope.
    #[default]
    None,
    /// Use worker-side LZ4 when it clears the configured savings threshold.
    Lz4,
}

/// Transport used between a primary and shardcache overflow replicas.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KvOverflowTransport {
    /// Connect to the shard-owned port selected by overflow-slot routing.
    #[default]
    DirectShard,
    /// Compatibility path through the replica fanout listener.
    Fanout,
}

/// Wire protocol used by key-value overflow nodes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KvOverflowBackend {
    /// Shardcache's native SCNP protocol.
    #[default]
    Scnp,
    /// RESP connection to a Redis/Valkey-compatible database.
    Redis,
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
#[derive(Clone, Serialize, Deserialize)]
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
    /// File containing the current reloadable replication authentication token.
    pub auth_token_path: Option<PathBuf>,
    /// Optional previous-token file retained during a bounded rolling rotation.
    pub previous_auth_token_path: Option<PathBuf>,
    /// TLS identity used while serving native replication connections.
    pub tls_server: ScnpTlsServerConfig,
    /// TLS trust and mTLS identity used while connecting to a primary.
    pub tls_client: ScnpTlsClientConfig,
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
    /// Maximum time canonical vector-set updates may be coalesced per key.
    ///
    /// Coalescing prevents repeated `VADD` calls from replicating every
    /// increasingly large intermediate HNSW payload. Deletes, type changes,
    /// and snapshots force an ordered flush before continuing.
    pub vector_state_flush_ms: u64,
    /// Maximum canonical vector-state bytes retained by the coalescer.
    ///
    /// A single state larger than this limit bypasses coalescing and is sent
    /// immediately. Zero uses a one-byte limit, effectively disabling retained
    /// vector state without disabling vector replication.
    pub vector_state_pending_max_bytes: usize,
    /// Approximate retained in-memory backlog size for partial catch-up.
    pub backlog_bytes: usize,
    /// Snapshot chunk size before compression.
    pub snapshot_chunk_bytes: usize,
    /// Maximum retained bytes while receiving one replication snapshot.
    pub snapshot_receive_max_bytes: usize,
    /// Maximum entries accepted in one replication snapshot.
    pub snapshot_receive_max_entries: usize,
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

impl std::fmt::Debug for ReplicationConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReplicationConfig")
            .field("enabled", &self.enabled)
            .field("role", &self.role)
            .field("bind_addr", &self.bind_addr)
            .field("replica_of", &self.replica_of)
            .field("auth_token_configured", &self.auth_token.is_some())
            .field("auth_token_path", &self.auth_token_path)
            .field("previous_auth_token_path", &self.previous_auth_token_path)
            .field("tls_server", &self.tls_server)
            .field("tls_client", &self.tls_client)
            .field("compression", &self.compression)
            .field("zstd_level", &self.zstd_level)
            .field("send_policy", &self.send_policy)
            .field("batch_max_records", &self.batch_max_records)
            .field("batch_max_bytes", &self.batch_max_bytes)
            .field("batch_max_delay_us", &self.batch_max_delay_us)
            .field("vector_state_flush_ms", &self.vector_state_flush_ms)
            .field(
                "vector_state_pending_max_bytes",
                &self.vector_state_pending_max_bytes,
            )
            .field("backlog_bytes", &self.backlog_bytes)
            .field("snapshot_chunk_bytes", &self.snapshot_chunk_bytes)
            .field(
                "snapshot_receive_max_bytes",
                &self.snapshot_receive_max_bytes,
            )
            .field(
                "snapshot_receive_max_entries",
                &self.snapshot_receive_max_entries,
            )
            .field("queue_capacity", &self.queue_capacity)
            .field("max_replicas", &self.max_replicas)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("write_timeout_ms", &self.write_timeout_ms)
            .field("reconnect_backoff_ms", &self.reconnect_backoff_ms)
            .field(
                "subscriber_channel_capacity",
                &self.subscriber_channel_capacity,
            )
            .finish()
    }
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
            kv_overflow_replica: KvOverflowReplicaServerConfig::default(),
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
            wal_block_max_records: 64,
            wal_block_max_bytes: 256 * 1024,
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
            backend: KvOverflowBackend::Scnp,
            replicas: Vec::new(),
            previous_replicas: Vec::new(),
            cluster_id: "default".into(),
            transport: KvOverflowTransport::default(),
            endpoints: Vec::new(),
            previous_endpoints: Vec::new(),
            previous_primary_shard_count: None,
            slot_count: 16_384,
            redis_key_prefix: "shardcache:overflow:".into(),
            redis_username_env: None,
            redis_password_env: None,
            scnp_auth_token_env: None,
            scnp_auth_token_path: None,
            scnp_auth_token_reload_interval_ms: 30_000,
            scnp_tls: ScnpTlsClientConfig::default(),
            allow_insecure_scnp: false,
            max_memory_bytes: 0,
            max_metadata_bytes: 0,
            max_key_bytes: 1024 * 1024,
            eviction_policy: EvictionPolicy::Lru,
            connections_per_endpoint: 2,
            queue_capacity_per_shard: 1_024,
            pipeline_max_items: 64,
            pipeline_max_bytes: 256 * 1024,
            pipeline_flush_micros: 200,
            max_inflight_per_target: 1,
            adaptive_pipeline: true,
            pipeline_target_micros: 1_000,
            circuit_breaker_failure_threshold: 8,
            circuit_breaker_cooldown_ms: 5_000,
            compression: KvOverflowCompression::None,
            allow_envelope_v2_writes: false,
            compression_min_value_bytes: 4 * 1024,
            compression_min_savings_percent: 12,
            max_value_bytes: 64 * 1024 * 1024,
            compression_max_expansion_ratio: 256,
            connect_timeout_ms: 250,
            operation_timeout_ms: 500,
            max_retries: 2,
            retry_backoff_ms: 10,
            cleanup_interval_ms: 1_000,
            fetch_on_miss: true,
            forget_remote_misses: false,
            handoff_max_bytes_per_second: 0,
            handoff_max_concurrency: 16,
            handoff_batch_items: 1_024,
            retained_buffer_bytes: 16 * 1024,
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
            auth_token_path: None,
            previous_auth_token_path: None,
            tls_server: ScnpTlsServerConfig::default(),
            tls_client: ScnpTlsClientConfig::default(),
            compression: ReplicationCompression::None,
            zstd_level: 3,
            send_policy: ReplicationSendPolicy::Batch,
            batch_max_records: 512,
            batch_max_bytes: 1024 * 1024,
            batch_max_delay_us: 750,
            vector_state_flush_ms: 10,
            vector_state_pending_max_bytes: 16 * 1024 * 1024,
            backlog_bytes: 64 * 1024 * 1024,
            snapshot_chunk_bytes: 1024 * 1024,
            snapshot_receive_max_bytes: 1024 * 1024 * 1024,
            snapshot_receive_max_entries: 10_000_000,
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
    #[cfg(all(feature = "kv-overflow", feature = "kv-overflow-redis"))]
    use super::KvOverflowBackend;
    #[cfg(feature = "kv-overflow")]
    use super::{
        EvictionPolicy, KvOverflowConfig, KvOverflowReplica, KvOverflowReplicaServerConfig,
        MAX_KV_OVERFLOW_SLOT_COUNT, ScnpTlsClientConfig,
    };
    use super::{
        ReplicationConfig, ServerEndpointMode, ShardCacheConfig, geometry::CacheSizeParser,
    };
    use std::path::PathBuf;

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
    fn persistence_requires_nonzero_wal_block_limits() {
        let mut config = ShardCacheConfig::default();
        config.persistence.wal_block_max_records = 0;
        assert!(config.validate().is_err());

        config.persistence.wal_block_max_records = 64;
        config.persistence.wal_block_max_bytes = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn replication_token_rotation_configuration_requires_a_current_file() {
        let config = ShardCacheConfig {
            replication: ReplicationConfig {
                enabled: true,
                previous_auth_token_path: Some(PathBuf::from("/run/secrets/old-token")),
                ..ReplicationConfig::default()
            },
            ..ShardCacheConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[cfg(feature = "scnp-tls")]
    #[test]
    fn non_loopback_replication_requires_mtls_and_token_authentication() {
        let mut config = ShardCacheConfig {
            replication: ReplicationConfig {
                enabled: true,
                role: super::ReplicationRole::Replica,
                replica_of: Some("10.0.0.10:7631".into()),
                ..ReplicationConfig::default()
            },
            ..ShardCacheConfig::default()
        };
        assert!(config.validate().is_err());

        config.replication.auth_token_path = Some(PathBuf::from("/run/secrets/token"));
        config.replication.tls_client = ScnpTlsClientConfig {
            enabled: true,
            ca_path: PathBuf::from("/run/secrets/ca.pem"),
            client_cert_path: Some(PathBuf::from("/run/secrets/client.pem")),
            client_key_path: Some(PathBuf::from("/run/secrets/client-key.pem")),
            server_name: Some("primary.internal".into()),
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn parses_server_endpoint_mode_from_toml() {
        let config: ShardCacheConfig =
            toml::from_str(r#"server_endpoint_mode = "direct_shard""#).unwrap();

        assert_eq!(config.server_endpoint_mode, ServerEndpointMode::DirectShard);
    }

    #[test]
    fn example_configuration_parses_and_validates() {
        let config: ShardCacheConfig =
            toml::from_str(include_str!("../shardcache.toml.example")).unwrap();
        config.validate().unwrap();
    }

    #[cfg(feature = "kv-overflow")]
    #[test]
    fn validates_key_value_overflow_membership() {
        let mut config = ShardCacheConfig {
            shard_count: 16,
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

        config.kv_overflow.slot_count = MAX_KV_OVERFLOW_SLOT_COUNT * 2;
        assert!(config.validate().is_err());

        config.kv_overflow.slot_count = 16_384;
        config.kv_overflow.previous_endpoints.clear();
        config.kv_overflow.previous_primary_shard_count = Some(2);
        assert!(config.validate().is_err());

        config.kv_overflow.previous_endpoints = vec!["127.0.0.1:6381".into()];
        config.kv_overflow.previous_primary_shard_count = Some(3);
        assert!(config.validate().is_err());

        config.kv_overflow.previous_primary_shard_count = Some(2);
        assert!(config.validate().is_ok());

        config.kv_overflow.previous_primary_shard_count = None;
        config.kv_overflow.previous_endpoints.clear();
        config.kv_overflow.endpoints = vec!["10.0.0.10:6380".into()];
        assert!(config.validate().is_err());
        config.kv_overflow.scnp_auth_token_env = Some("OVERFLOW_SCNP_TOKEN".into());
        assert!(config.validate().is_err());
        config.kv_overflow.allow_insecure_scnp = true;
        assert!(config.validate().is_ok());
    }

    #[cfg(all(feature = "kv-overflow", feature = "scnp-tls"))]
    #[test]
    fn non_loopback_scnp_accepts_tls_with_authenticated_clients() {
        let config = ShardCacheConfig {
            shard_count: 1,
            kv_overflow: KvOverflowConfig {
                enabled: true,
                endpoints: vec!["10.0.0.10:6380".into()],
                scnp_auth_token_env: Some("OVERFLOW_SCNP_TOKEN".into()),
                scnp_tls: ScnpTlsClientConfig {
                    enabled: true,
                    ca_path: PathBuf::from("/run/secrets/overflow-ca.pem"),
                    server_name: Some("overflow.internal".into()),
                    ..ScnpTlsClientConfig::default()
                },
                max_memory_bytes: 1024,
                ..KvOverflowConfig::default()
            },
            ..ShardCacheConfig::default()
        };
        assert!(config.validate().is_ok());
    }

    #[cfg(feature = "kv-overflow")]
    #[test]
    fn overflow_replica_requires_capacity_preserving_eviction_by_default() {
        let mut config = ShardCacheConfig {
            bind_addr: "127.0.0.1:6380".into(),
            max_memory_bytes: 1024,
            eviction_policy: EvictionPolicy::Lru,
            server_endpoint_mode: ServerEndpointMode::DirectShard,
            kv_overflow_replica: KvOverflowReplicaServerConfig {
                enabled: true,
                node_id: "replica-a".into(),
                encrypted_persistence: true,
                ..KvOverflowReplicaServerConfig::default()
            },
            ..ShardCacheConfig::default()
        };
        assert!(config.validate().is_err());
        config.kv_overflow_replica.allow_lossy_eviction = true;
        assert!(config.validate().is_ok());
    }

    #[cfg(feature = "kv-overflow")]
    #[test]
    fn validates_structured_overflow_replica_identity_and_ports() {
        let mut config = ShardCacheConfig {
            shard_count: 16,
            kv_overflow: KvOverflowConfig {
                enabled: true,
                replicas: vec![KvOverflowReplica {
                    id: "replica-a".into(),
                    addresses: vec!["127.0.0.1:6380".into()],
                    shard_count: 16,
                    direct_shard_base_port: 6381,
                    tls_server_name: None,
                }],
                max_memory_bytes: 1024,
                ..KvOverflowConfig::default()
            },
            ..ShardCacheConfig::default()
        };
        assert!(config.validate().is_ok());

        config.kv_overflow.previous_replicas = vec![
            config.kv_overflow.replicas[0].clone(),
            config.kv_overflow.replicas[0].clone(),
        ];
        assert!(config.validate().is_err());

        config.kv_overflow.previous_replicas.clear();
        config.kv_overflow.replicas[0].direct_shard_base_port = u16::MAX;
        assert!(config.validate().is_err());
    }

    #[cfg(all(feature = "kv-overflow", feature = "kv-overflow-redis"))]
    #[test]
    fn validates_redis_key_value_overflow_endpoints_and_credentials() {
        let mut config = ShardCacheConfig {
            kv_overflow: KvOverflowConfig {
                enabled: true,
                backend: KvOverflowBackend::Redis,
                endpoints: vec!["redis://127.0.0.1:6379/0".into()],
                max_memory_bytes: 1024,
                ..KvOverflowConfig::default()
            },
            ..ShardCacheConfig::default()
        };
        assert!(config.validate().is_ok());

        config.kv_overflow.endpoints[0] = "127.0.0.1:6379".into();
        assert!(config.validate().is_err());

        config.kv_overflow.endpoints[0] = "redis://:secret@127.0.0.1:6379/0".into();
        assert!(config.validate().is_err());

        config.kv_overflow.endpoints[0] = "redis://user:super-secret@/".into();
        let error = config.validate().unwrap_err().to_string();
        assert!(!error.contains("super-secret"));

        config.kv_overflow.endpoints[0] = "redis://127.0.0.1:6379/0".into();
        config.kv_overflow.redis_username_env = Some("REDIS_USER".into());
        assert!(config.validate().is_err());
    }
}
