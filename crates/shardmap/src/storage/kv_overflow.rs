use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::Bytes as SharedBytes;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded};
use hashbrown::HashMap as HashBrownMap;
use parking_lot::{Mutex, RwLock};
#[cfg(feature = "scnp-tls")]
use shardcache_client_rs::ScnpTlsClientConfig as RuntimeScnpTlsClientConfig;
use shardcache_client_rs::{
    ShardCacheClient, ShardCacheClientError, ShardCacheDirectRouter, ShardCacheDirectShardClient,
    ShardCacheRouteMode,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream as AsyncTcpStream;
use tokio::sync::{mpsc, oneshot};
use xxhash_rust::xxh3::xxh3_64;

use crate::config::{
    EvictionPolicy, KvOverflowBackend, KvOverflowCompression, KvOverflowConfig, KvOverflowReplica,
    KvOverflowTransport, MAX_KV_OVERFLOW_SLOT_COUNT,
};
use crate::persistence::{PersistenceRuntime, SnapshotCompression, SnapshotStore};
use crate::storage::{
    Bytes, EmbeddedStore, FastHashBuilder, GovernedRead, StoredEntry, local_table_hash,
    local_table_hasher, now_millis, shift_for, stripe_index,
};
use crate::{Result, ShardCacheError};

const KV_OVERFLOW_MAGIC: &[u8; 8] = b"SCKVOV01";
const KV_OVERFLOW_COMPRESSED_MAGIC: &[u8; 8] = b"SCKVOV02";
const KV_OVERFLOW_GOVERNED_MAGIC: &[u8; 8] = b"SCKVOV03";
const KV_OVERFLOW_GOVERNED_COMPRESSED_MAGIC: &[u8; 8] = b"SCKVOV04";
const KV_OVERFLOW_HEADER_LEN: usize = KV_OVERFLOW_MAGIC.len() + 8 + 8 + 4;
const KV_OVERFLOW_COMPRESSED_HEADER_LEN: usize = KV_OVERFLOW_HEADER_LEN + 1;
const KV_OVERFLOW_GOVERNED_HEADER_LEN: usize = KV_OVERFLOW_HEADER_LEN + 4;
const KV_OVERFLOW_GOVERNED_COMPRESSED_HEADER_LEN: usize = KV_OVERFLOW_GOVERNED_HEADER_LEN + 1;
const KV_OVERFLOW_CODEC_LZ4: u8 = 1;
const KV_OVERFLOW_KEY_GATES: usize = 64;
const KV_OVERFLOW_EVICTION_BATCH_PER_SHARD: usize = 16;
const KV_OVERFLOW_CLEANUP_BATCH_PER_STRIPE: usize = 256;
const KV_OVERFLOW_METADATA_ENTRY_OVERHEAD: usize = 128;
const KV_OVERFLOW_STORAGE_KEY_MAGIC: &[u8; 8] = b"SCKVKEY1";
pub const DEFAULT_KV_OVERFLOW_SLOT_COUNT: u32 = 16_384;

/// Value returned by a key-value overflow node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvOverflowValue {
    pub value: SharedBytes,
    pub ttl_ms: Option<u64>,
    /// Opaque policy metadata. Presence marks the value as protected.
    pub governance: Option<SharedBytes>,
}

/// One absolute-expiry write in an overflow-node batch.
#[derive(Debug, Clone, Copy)]
pub struct KvOverflowPutRequest<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub expire_at_ms: Option<u64>,
    pub governance: Option<&'a [u8]>,
}

/// One independently addressable member of a partitioned overflow cluster.
pub trait KvOverflowNode: Send + Sync + 'static {
    /// Stable target identity used by deterministic slot ownership.
    fn id(&self) -> &str;
    /// Stable physical replica identity. Multiple target shards can share it.
    fn replica_id(&self) -> &str {
        self.id()
    }
    /// Remote shard represented by this independently owned target.
    fn remote_shard(&self) -> usize {
        0
    }
    /// Returns direct SCNP mutation metadata when this target can be driven by
    /// the shard-owned asynchronous I/O runtime.
    #[doc(hidden)]
    fn direct_scnp_target(&self) -> Option<ScnpDirectTarget> {
        None
    }
    /// Returns `(open, open_events, suppressed_requests)` for this target.
    #[doc(hidden)]
    fn circuit_breaker_health(&self) -> (bool, u64, u64) {
        (false, 0, 0)
    }
    /// Returns `(raw_bytes, stored_bytes, compressed_values)` for this target.
    #[doc(hidden)]
    fn storage_health(&self) -> (u64, u64, u64) {
        (0, 0, 0)
    }
    fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()>;
    /// Stores a value using an absolute primary expiry deadline.
    ///
    /// Implementations with native expiry support should override this so
    /// queueing and request latency cannot restart the value's TTL.
    fn put_until(&self, key: &[u8], value: &[u8], expire_at_ms: Option<u64>) -> Result<()> {
        let ttl_ms = expire_at_ms.map(|deadline| deadline.saturating_sub(now_millis()));
        if ttl_ms == Some(0) {
            self.delete(key)
        } else {
            self.put(key, value, ttl_ms)
        }
    }
    /// Stores a value and its opaque governance metadata atomically.
    fn put_until_with_governance(
        &self,
        key: &[u8],
        value: &[u8],
        governance: Option<&[u8]>,
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        if governance.is_some() {
            return Err(ShardCacheError::Protocol(
                "key-value overflow node does not support governance metadata".into(),
            ));
        }
        self.put_until(key, value, expire_at_ms)
    }
    /// Stores an ordered group of writes. Remote adapters can override this
    /// to pipeline a worker batch without changing per-key ordering.
    fn put_batch_until(&self, requests: &[KvOverflowPutRequest<'_>]) -> Result<()> {
        for request in requests {
            self.put_until_with_governance(
                request.key,
                request.value,
                request.governance,
                request.expire_at_ms,
            )?;
        }
        Ok(())
    }
    /// Stores a batch and reports each mutation independently.
    ///
    /// The default retries items individually after an ambiguous batch failure.
    /// Implementations with native per-item results can override this directly.
    fn put_batch_outcomes_until(
        &self,
        requests: &[KvOverflowPutRequest<'_>],
        outcomes: &mut Vec<bool>,
    ) {
        outcomes.clear();
        if self.put_batch_until(requests).is_ok() {
            outcomes.resize(requests.len(), true);
            return;
        }
        outcomes.extend(requests.iter().map(|request| {
            self.put_until_with_governance(
                request.key,
                request.value,
                request.governance,
                request.expire_at_ms,
            )
            .is_ok()
        }));
    }
    /// Stores a value on the connection dedicated to `primary_shard`.
    fn put_until_on_shard(
        &self,
        _primary_shard: usize,
        key: &[u8],
        value: &[u8],
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        self.put_until(key, value, expire_at_ms)
    }
    /// Stores a governed value on the connection dedicated to `primary_shard`.
    fn put_until_with_governance_on_shard(
        &self,
        _primary_shard: usize,
        key: &[u8],
        value: &[u8],
        governance: Option<&[u8]>,
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        self.put_until_with_governance(key, value, governance, expire_at_ms)
    }
    /// Stores a batch on the connection dedicated to `primary_shard`.
    fn put_batch_outcomes_until_on_shard(
        &self,
        _primary_shard: usize,
        requests: &[KvOverflowPutRequest<'_>],
        outcomes: &mut Vec<bool>,
    ) {
        self.put_batch_outcomes_until(requests, outcomes);
    }
    fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>>;
    /// Fetches a value on the connection dedicated to `primary_shard`.
    fn get_on_shard(&self, _primary_shard: usize, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        self.get(key)
    }
    fn delete(&self, key: &[u8]) -> Result<()>;
    /// Deletes a value on the connection dedicated to `primary_shard`.
    fn delete_on_shard(&self, _primary_shard: usize, key: &[u8]) -> Result<()> {
        self.delete(key)
    }
}

/// Runtime options for partitioned key-value overflow.
#[derive(Debug, Clone)]
pub struct KvOverflowOptions {
    pub max_memory_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_key_bytes: usize,
    pub eviction_policy: EvictionPolicy,
    pub fetch_on_miss: bool,
    pub cleanup_interval: Duration,
    pub queue_capacity: usize,
    pub pipeline_max_items: usize,
    pub pipeline_max_bytes: usize,
    pub pipeline_flush: Duration,
    pub max_inflight_per_target: usize,
    pub forget_remote_misses: bool,
    pub adaptive_pipeline: bool,
    pub pipeline_target: Duration,
    pub circuit_breaker_failure_threshold: usize,
    pub circuit_breaker_cooldown: Duration,
    pub compression: KvOverflowCompression,
    pub compression_min_value_bytes: usize,
    pub compression_min_savings_percent: u8,
    pub max_value_bytes: usize,
    pub compression_max_expansion_ratio: usize,
    pub handoff_max_bytes_per_second: u64,
    pub handoff_max_concurrency: usize,
    pub handoff_batch_items: usize,
    pub retained_buffer_bytes: usize,
}

impl KvOverflowOptions {
    fn validate(&self) -> Result<()> {
        if self.max_memory_bytes == 0
            || self.max_metadata_bytes <= KV_OVERFLOW_METADATA_ENTRY_OVERHEAD
            || self.max_key_bytes == 0
            || !matches!(
                self.eviction_policy,
                EvictionPolicy::Lru | EvictionPolicy::Lfu
            )
        {
            return Err(ShardCacheError::Config(
                "kv overflow requires a positive memory target and lru or lfu eviction".into(),
            ));
        }
        if self.queue_capacity == 0
            || self.pipeline_max_items == 0
            || self.pipeline_max_bytes == 0
            || self.max_inflight_per_target == 0
            || self.pipeline_target.is_zero()
            || self.circuit_breaker_failure_threshold == 0
            || self.circuit_breaker_cooldown.is_zero()
            || self.compression_min_value_bytes == 0
            || self.compression_min_savings_percent > 100
            || self.max_value_bytes == 0
            || self.compression_max_expansion_ratio == 0
            || self.handoff_max_concurrency == 0
            || self.handoff_batch_items == 0
            || self.retained_buffer_bytes == 0
        {
            return Err(ShardCacheError::Config(
                "kv overflow queue and pipeline limits must be > 0".into(),
            ));
        }
        if self.cleanup_interval.is_zero() {
            return Err(ShardCacheError::Config(
                "kv overflow cleanup_interval must be > 0".into(),
            ));
        }
        Ok(())
    }
}

impl TryFrom<&KvOverflowConfig> for KvOverflowOptions {
    type Error = ShardCacheError;

    fn try_from(config: &KvOverflowConfig) -> Result<Self> {
        let max_memory_bytes = usize::try_from(config.max_memory_bytes).map_err(|_| {
            ShardCacheError::Config(
                "kv_overflow.max_memory_bytes exceeds platform addressable size".into(),
            )
        })?;
        if max_memory_bytes == 0 {
            return Err(ShardCacheError::Config(
                "kv_overflow.max_memory_bytes must be > 0".into(),
            ));
        }
        let configured_metadata_bytes =
            usize::try_from(config.max_metadata_bytes).map_err(|_| {
                ShardCacheError::Config(
                    "kv_overflow.max_metadata_bytes exceeds platform addressable size".into(),
                )
            })?;
        let max_metadata_bytes = if configured_metadata_bytes == 0 {
            max_memory_bytes.saturating_div(4).max(
                config
                    .max_key_bytes
                    .saturating_add(KV_OVERFLOW_METADATA_ENTRY_OVERHEAD),
            )
        } else {
            configured_metadata_bytes
        };
        if !matches!(
            config.eviction_policy,
            EvictionPolicy::Lru | EvictionPolicy::Lfu
        ) {
            return Err(ShardCacheError::Config(
                "kv_overflow.eviction_policy must be lru or lfu".into(),
            ));
        }
        let options = Self {
            max_memory_bytes,
            max_metadata_bytes,
            max_key_bytes: config.max_key_bytes,
            eviction_policy: config.eviction_policy,
            fetch_on_miss: config.fetch_on_miss,
            cleanup_interval: Duration::from_millis(config.cleanup_interval_ms),
            queue_capacity: config.queue_capacity_per_shard,
            pipeline_max_items: config.pipeline_max_items,
            pipeline_max_bytes: config.pipeline_max_bytes,
            pipeline_flush: Duration::from_micros(config.pipeline_flush_micros),
            max_inflight_per_target: config.max_inflight_per_target,
            forget_remote_misses: config.forget_remote_misses,
            adaptive_pipeline: config.adaptive_pipeline,
            pipeline_target: Duration::from_micros(config.pipeline_target_micros),
            circuit_breaker_failure_threshold: config.circuit_breaker_failure_threshold,
            circuit_breaker_cooldown: Duration::from_millis(config.circuit_breaker_cooldown_ms),
            compression: config.compression,
            compression_min_value_bytes: config.compression_min_value_bytes,
            compression_min_savings_percent: config.compression_min_savings_percent,
            max_value_bytes: config.max_value_bytes,
            compression_max_expansion_ratio: config.compression_max_expansion_ratio,
            handoff_max_bytes_per_second: config.handoff_max_bytes_per_second,
            handoff_max_concurrency: config.handoff_max_concurrency,
            handoff_batch_items: config.handoff_batch_items,
            retained_buffer_bytes: config.retained_buffer_bytes,
        };
        options.validate()?;
        Ok(options)
    }
}

/// Immutable direct-SCNP connection metadata used by a shard-owned runtime.
#[doc(hidden)]
#[derive(Clone)]
pub struct ScnpDirectTarget {
    addresses: Box<[SocketAddr]>,
    remote_shard: usize,
    connect_timeout: Duration,
    operation_timeout: Duration,
    max_retries: usize,
    retry_backoff: Duration,
    auth_token: Option<Arc<ReloadableScnpAuthToken>>,
    circuit: Arc<CircuitBreaker>,
    #[cfg(feature = "scnp-tls")]
    tls: Option<Arc<RuntimeScnpTlsClientConfig>>,
}

struct ReloadableScnpAuthToken {
    current: Arc<RwLock<Arc<[u8]>>>,
    shutdown: Option<Sender<()>>,
    reload_thread: Mutex<Option<JoinHandle<()>>>,
}

impl ReloadableScnpAuthToken {
    fn new_static(token: Arc<[u8]>) -> Self {
        Self {
            current: Arc::new(RwLock::new(token)),
            shutdown: None,
            reload_thread: Mutex::new(None),
        }
    }

    fn new_file(path: PathBuf, interval: Duration) -> Result<Self> {
        let current = Arc::new(RwLock::new(read_scnp_auth_token_file(&path)?));
        let (shutdown, shutdown_rx) = bounded(1);
        let reload_current = Arc::clone(&current);
        let interval = interval.max(Duration::from_millis(100));
        let reload_thread = thread::Builder::new()
            .name("shardmap-kv-overflow-auth-reload".into())
            .spawn(move || {
                while shutdown_rx.recv_timeout(interval).is_err() {
                    match read_scnp_auth_token_file(&path) {
                        Ok(token) => *reload_current.write() = token,
                        Err(error) => tracing::warn!(
                            "SCNP primary authentication token reload failed; retaining prior token: {error}"
                        ),
                    }
                }
            })
            .map_err(|error| {
                ShardCacheError::Config(format!(
                    "failed to start SCNP primary token reload thread: {error}"
                ))
            })?;
        Ok(Self {
            current,
            shutdown: Some(shutdown),
            reload_thread: Mutex::new(Some(reload_thread)),
        })
    }

    fn current(&self) -> Arc<[u8]> {
        Arc::clone(&self.current.read())
    }
}

impl Drop for ReloadableScnpAuthToken {
    fn drop(&mut self) {
        if let Some(shutdown) = &self.shutdown {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.reload_thread.lock().take() {
            let _ = thread.join();
        }
    }
}

impl std::fmt::Debug for ScnpDirectTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScnpDirectTarget")
            .field("addresses", &self.addresses)
            .field("remote_shard", &self.remote_shard)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("max_retries", &self.max_retries)
            .field("authenticated", &self.auth_token.is_some())
            .field("tls", &{
                #[cfg(feature = "scnp-tls")]
                {
                    self.tls.is_some()
                }
                #[cfg(not(feature = "scnp-tls"))]
                {
                    false
                }
            })
            .finish()
    }
}

enum ScnpTargetConnection {
    Fanout(ShardCacheClient),
    Direct(ShardCacheDirectShardClient),
}

struct CircuitBreaker {
    started: Instant,
    consecutive_failures: AtomicUsize,
    failure_threshold: usize,
    cooldown: Duration,
    open_until_ns: AtomicU64,
    open_events: AtomicU64,
    suppressed_requests: AtomicU64,
}

#[derive(Debug, Default)]
struct OverflowNodeStorageMetrics {
    raw_bytes: AtomicU64,
    stored_bytes: AtomicU64,
    compressed_values: AtomicU64,
}

impl OverflowNodeStorageMetrics {
    fn record(&self, raw_len: usize, encoded: &[u8]) {
        self.raw_bytes.fetch_add(raw_len as u64, Ordering::Relaxed);
        self.stored_bytes
            .fetch_add(encoded.len() as u64, Ordering::Relaxed);
        if encoded.starts_with(KV_OVERFLOW_COMPRESSED_MAGIC) {
            self.compressed_values.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.raw_bytes.load(Ordering::Relaxed),
            self.stored_bytes.load(Ordering::Relaxed),
            self.compressed_values.load(Ordering::Relaxed),
        )
    }
}

impl CircuitBreaker {
    fn new(failure_threshold: usize, cooldown: Duration) -> Self {
        Self {
            started: Instant::now(),
            consecutive_failures: AtomicUsize::new(0),
            failure_threshold,
            cooldown,
            open_until_ns: AtomicU64::new(0),
            open_events: AtomicU64::new(0),
            suppressed_requests: AtomicU64::new(0),
        }
    }

    fn now_ns(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX - 1)
    }

    fn allow_request(&self) -> bool {
        const HALF_OPEN: u64 = u64::MAX;
        let deadline = self.open_until_ns.load(Ordering::Acquire);
        if deadline == 0 {
            return true;
        }
        if deadline != HALF_OPEN
            && deadline <= self.now_ns()
            && self
                .open_until_ns
                .compare_exchange(deadline, HALF_OPEN, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            return true;
        }
        self.suppressed_requests.fetch_add(1, Ordering::Relaxed);
        false
    }

    fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.open_until_ns.store(0, Ordering::Release);
    }

    fn record_failure(&self) {
        let failures = self
            .consecutive_failures
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if failures >= self.failure_threshold {
            let cooldown_ns = u64::try_from(self.cooldown.as_nanos()).unwrap_or(u64::MAX / 2);
            let deadline = self.now_ns().saturating_add(cooldown_ns).min(u64::MAX - 1);
            let previous = self.open_until_ns.swap(deadline, Ordering::AcqRel);
            if previous == 0 || previous == u64::MAX {
                self.open_events.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn health(&self) -> (bool, u64, u64) {
        (
            self.open_until_ns.load(Ordering::Acquire) != 0,
            self.open_events.load(Ordering::Relaxed),
            self.suppressed_requests.load(Ordering::Relaxed),
        )
    }
}

struct AdaptivePipeline {
    enabled: bool,
    current_items: usize,
    max_items: usize,
    target: Duration,
    ewma_latency_ns: Option<u64>,
    healthy_samples: u8,
}

impl AdaptivePipeline {
    fn new(options: &KvOverflowOptions) -> Self {
        Self {
            enabled: options.adaptive_pipeline,
            current_items: options.pipeline_max_items,
            max_items: options.pipeline_max_items,
            target: options.pipeline_target,
            ewma_latency_ns: None,
            healthy_samples: 0,
        }
    }

    fn item_limit(&self) -> usize {
        self.current_items.max(1)
    }

    fn record(
        &mut self,
        elapsed: Duration,
        succeeded: bool,
        batch_bytes: usize,
        max_batch_bytes: usize,
    ) {
        if !self.enabled || self.max_items <= 1 {
            return;
        }
        if !succeeded {
            self.current_items = (self.current_items / 2).max(1);
            self.healthy_samples = 0;
            return;
        }
        let sample = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let average = self.ewma_latency_ns.map_or(sample, |previous| {
            previous.saturating_mul(7).saturating_add(sample) / 8
        });
        self.ewma_latency_ns = Some(average);
        let target = u64::try_from(self.target.as_nanos()).unwrap_or(u64::MAX);
        let byte_saturated = batch_bytes.saturating_mul(4) >= max_batch_bytes.saturating_mul(3);
        if !byte_saturated && average > target.saturating_mul(3) / 2 {
            self.current_items = (self.current_items / 2).max(1);
            self.healthy_samples = 0;
            self.ewma_latency_ns = None;
        } else if average <= target {
            self.healthy_samples = self.healthy_samples.saturating_add(1);
            if self.healthy_samples < 4 {
                return;
            }
            self.healthy_samples = 0;
            let increase = (self.max_items / 8).max(1);
            self.current_items = self
                .current_items
                .saturating_add(increase)
                .min(self.max_items);
        }
    }
}

struct ScnpConnectionSlot {
    connection: Option<ScnpTargetConnection>,
    circuit: Arc<CircuitBreaker>,
}

impl ScnpTargetConnection {
    fn set(&mut self, key: &[u8], value: &[u8]) -> shardcache_client_rs::Result<()> {
        match self {
            Self::Fanout(client) => client.set(key, value),
            Self::Direct(client) => client.set(key, value),
        }
    }

    fn get_into_limited(
        &mut self,
        key: &[u8],
        out: &mut Vec<u8>,
        max_body_len: usize,
    ) -> shardcache_client_rs::Result<bool> {
        match self {
            Self::Fanout(client) => client.get_into_limited(key, out, max_body_len),
            Self::Direct(client) => client.get_into_limited(key, out, max_body_len),
        }
    }

    fn del(&mut self, key: &[u8]) -> shardcache_client_rs::Result<bool> {
        match self {
            Self::Fanout(client) => client.del(key),
            Self::Direct(client) => client.del(key),
        }
    }

    fn begin_set(&mut self, key: &[u8], value: &[u8]) -> shardcache_client_rs::Result<()> {
        match self {
            Self::Fanout(client) => client.begin_pipeline_set(key, value),
            Self::Direct(client) => client.begin_pipeline_set(key, value),
        }
    }

    fn begin_del(&mut self, key: &[u8]) -> shardcache_client_rs::Result<()> {
        match self {
            Self::Fanout(client) => client.begin_pipeline_del(key),
            Self::Direct(client) => client.begin_pipeline_del(key),
        }
    }

    fn flush(&mut self) -> shardcache_client_rs::Result<()> {
        match self {
            Self::Fanout(client) => client.flush_pipeline(),
            Self::Direct(client) => client.flush_pipeline(),
        }
    }

    fn finish_set(&mut self) -> shardcache_client_rs::Result<()> {
        match self {
            Self::Fanout(client) => client.finish_pipeline_set(),
            Self::Direct(client) => client.finish_pipeline_set(),
        }
    }

    fn finish_del(&mut self) -> shardcache_client_rs::Result<bool> {
        match self {
            Self::Fanout(client) => client.finish_pipeline_del(),
            Self::Direct(client) => client.finish_pipeline_del(),
        }
    }
}

/// SCNP-backed overflow target with separate read and mutation connections.
pub struct ScnpKvOverflowNode {
    id: String,
    replica_id: String,
    endpoints: Box<[String]>,
    remote_shard: usize,
    direct_routers: Box<[ShardCacheDirectRouter]>,
    direct_target: Option<ScnpDirectTarget>,
    mutation_connection: Mutex<ScnpConnectionSlot>,
    read_connection: Mutex<ScnpConnectionSlot>,
    connect_timeout: Duration,
    operation_timeout: Duration,
    max_retries: usize,
    retry_backoff: Duration,
    auth_token: Option<Arc<ReloadableScnpAuthToken>>,
    compression: KvOverflowCompression,
    compression_min_value_bytes: usize,
    compression_min_savings_percent: u8,
    max_value_bytes: usize,
    compression_max_expansion_ratio: usize,
    storage_metrics: OverflowNodeStorageMetrics,
    #[cfg(feature = "scnp-tls")]
    tls: Option<Arc<RuntimeScnpTlsClientConfig>>,
}

impl std::fmt::Debug for ScnpKvOverflowNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScnpKvOverflowNode")
            .field("id", &self.id)
            .field("replica_id", &self.replica_id)
            .field("path_count", &self.endpoints.len())
            .field("remote_shard", &self.remote_shard)
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

impl ScnpKvOverflowNode {
    pub fn from_config(endpoint: String, config: &KvOverflowConfig) -> Result<Self> {
        Self::from_config_for_shards(endpoint, config, 1)
    }

    /// Creates one mutation socket and one read socket for this target.
    pub fn from_config_for_shards(
        endpoint: String,
        config: &KvOverflowConfig,
        _primary_shard_count: usize,
    ) -> Result<Self> {
        let circuit = Arc::new(CircuitBreaker::new(
            config.circuit_breaker_failure_threshold,
            Duration::from_millis(config.circuit_breaker_cooldown_ms),
        ));
        Ok(Self {
            id: endpoint.clone(),
            replica_id: endpoint.clone(),
            endpoints: vec![endpoint].into_boxed_slice(),
            remote_shard: 0,
            direct_routers: Box::default(),
            direct_target: None,
            mutation_connection: Mutex::new(ScnpConnectionSlot {
                connection: None,
                circuit: Arc::clone(&circuit),
            }),
            read_connection: Mutex::new(ScnpConnectionSlot {
                connection: None,
                circuit,
            }),
            connect_timeout: Duration::from_millis(config.connect_timeout_ms.max(1)),
            operation_timeout: Duration::from_millis(config.operation_timeout_ms.max(1)),
            max_retries: config.max_retries,
            retry_backoff: Duration::from_millis(config.retry_backoff_ms.max(1)),
            auth_token: read_scnp_auth_token(config)?,
            compression: config.compression,
            compression_min_value_bytes: config.compression_min_value_bytes,
            compression_min_savings_percent: config.compression_min_savings_percent,
            max_value_bytes: config.max_value_bytes,
            compression_max_expansion_ratio: config.compression_max_expansion_ratio,
            storage_metrics: OverflowNodeStorageMetrics::default(),
            #[cfg(feature = "scnp-tls")]
            tls: load_scnp_tls_config(config, config.scnp_tls.server_name.as_deref())?,
        })
    }

    fn from_replica_target(
        replica: &KvOverflowReplica,
        remote_shard: usize,
        config: &KvOverflowConfig,
        _primary_shard_count: usize,
        auth_token: Option<Arc<ReloadableScnpAuthToken>>,
        #[cfg(feature = "scnp-tls")] tls: Option<Arc<RuntimeScnpTlsClientConfig>>,
    ) -> Result<Self> {
        replica.addresses.first().ok_or_else(|| {
            ShardCacheError::Config(format!(
                "kv overflow replica {} has no addresses",
                replica.id
            ))
        })?;
        let direct_routers = match config.transport {
            KvOverflowTransport::Fanout => Vec::new(),
            KvOverflowTransport::DirectShard => {
                let mut routers = Vec::with_capacity(replica.addresses.len());
                for endpoint in &replica.addresses {
                    let mut address = resolve_address(endpoint)?;
                    let base_port = if replica.direct_shard_base_port == 0 {
                        address.port().checked_add(1).ok_or_else(|| {
                            ShardCacheError::Config(format!(
                                "direct shard base port overflows for {}",
                                replica.id
                            ))
                        })?
                    } else {
                        replica.direct_shard_base_port
                    };
                    address.set_port(base_port);
                    routers.push(
                        ShardCacheDirectRouter::new(address, replica.shard_count)
                            .map_err(|error| client_error(endpoint, error))?
                            .with_route_mode(ShardCacheRouteMode::OverflowSlot),
                    );
                }
                routers
            }
        };
        let circuit = Arc::new(CircuitBreaker::new(
            config.circuit_breaker_failure_threshold,
            Duration::from_millis(config.circuit_breaker_cooldown_ms),
        ));
        let direct_target = if direct_routers.is_empty() {
            None
        } else {
            Some(ScnpDirectTarget {
                addresses: direct_routers
                    .iter()
                    .map(|router| {
                        router
                            .shard_addr(remote_shard)
                            .map_err(|error| client_error(&replica.id, error))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_boxed_slice(),
                remote_shard,
                connect_timeout: Duration::from_millis(config.connect_timeout_ms.max(1)),
                operation_timeout: Duration::from_millis(config.operation_timeout_ms.max(1)),
                max_retries: config.max_retries,
                retry_backoff: Duration::from_millis(config.retry_backoff_ms.max(1)),
                auth_token: auth_token.clone(),
                circuit: Arc::clone(&circuit),
                #[cfg(feature = "scnp-tls")]
                tls: tls.clone(),
            })
        };
        Ok(Self {
            id: format!("{}#{remote_shard}", replica.id),
            replica_id: replica.id.clone(),
            endpoints: replica.addresses.clone().into_boxed_slice(),
            remote_shard,
            direct_routers: direct_routers.into_boxed_slice(),
            direct_target,
            mutation_connection: Mutex::new(ScnpConnectionSlot {
                connection: None,
                circuit: Arc::clone(&circuit),
            }),
            read_connection: Mutex::new(ScnpConnectionSlot {
                connection: None,
                circuit,
            }),
            connect_timeout: Duration::from_millis(config.connect_timeout_ms.max(1)),
            operation_timeout: Duration::from_millis(config.operation_timeout_ms.max(1)),
            max_retries: config.max_retries,
            retry_backoff: Duration::from_millis(config.retry_backoff_ms.max(1)),
            auth_token,
            compression: config.compression,
            compression_min_value_bytes: config.compression_min_value_bytes,
            compression_min_savings_percent: config.compression_min_savings_percent,
            max_value_bytes: config.max_value_bytes,
            compression_max_expansion_ratio: config.compression_max_expansion_ratio,
            storage_metrics: OverflowNodeStorageMetrics::default(),
            #[cfg(feature = "scnp-tls")]
            tls,
        })
    }

    fn connect(&self, path: usize) -> shardcache_client_rs::Result<ScnpTargetConnection> {
        let auth_token = self.auth_token.as_ref().map(|token| token.current());
        #[cfg(feature = "scnp-tls")]
        if let Some(tls) = self.tls.as_deref() {
            return match self.direct_routers.get(path) {
                Some(router) => router
                    .connect_shard_with_timeouts_auth_and_tls(
                        self.remote_shard,
                        self.connect_timeout,
                        self.operation_timeout,
                        auth_token.as_deref(),
                        tls,
                    )
                    .map(ScnpTargetConnection::Direct),
                None => ShardCacheClient::connect_with_timeouts_auth_and_tls(
                    self.endpoints[path].as_str(),
                    self.connect_timeout,
                    self.operation_timeout,
                    auth_token.as_deref(),
                    tls,
                )
                .map(ScnpTargetConnection::Fanout),
            };
        }
        match self.direct_routers.get(path) {
            Some(router) => router
                .connect_shard_with_timeouts_and_auth(
                    self.remote_shard,
                    self.connect_timeout,
                    self.operation_timeout,
                    auth_token.as_deref(),
                )
                .map(ScnpTargetConnection::Direct),
            None => ShardCacheClient::connect_with_timeouts_and_auth(
                self.endpoints[path].as_str(),
                self.connect_timeout,
                self.operation_timeout,
                auth_token.as_deref(),
            )
            .map(ScnpTargetConnection::Fanout),
        }
    }

    fn execute_on_shard<T>(
        &self,
        primary_shard: usize,
        read: bool,
        mut operation: impl FnMut(&mut ScnpTargetConnection) -> shardcache_client_rs::Result<T>,
    ) -> Result<T> {
        let connection = if read {
            &self.read_connection
        } else {
            &self.mutation_connection
        };
        let mut last_error = None;
        let total_attempts = self.max_retries.saturating_add(1).max(self.endpoints.len());
        let first_path = primary_shard.wrapping_add(self.remote_shard) % self.endpoints.len();
        for attempt in 0..total_attempts {
            let mut slot = connection.lock();
            if !slot.circuit.allow_request() {
                return Err(ShardCacheError::Protocol(format!(
                    "kv overflow target {} circuit breaker is open",
                    self.id
                )));
            }
            if slot.connection.is_none() {
                let path = first_path.wrapping_add(attempt) % self.endpoints.len();
                match self.connect(path) {
                    Ok(client) => slot.connection = Some(client),
                    Err(error) => {
                        slot.circuit.record_failure();
                        last_error = Some(error);
                        drop(slot);
                        self.retry_delay(attempt, total_attempts);
                        continue;
                    }
                }
            }
            let result = operation(slot.connection.as_mut().expect("connection initialized"));
            match result {
                Ok(value) => {
                    slot.circuit.record_success();
                    return Ok(value);
                }
                Err(error) => {
                    slot.connection = None;
                    slot.circuit.record_failure();
                    last_error = Some(error);
                }
            }
            drop(slot);
            self.retry_delay(attempt, total_attempts);
        }
        Err(client_error(
            self.id.as_str(),
            last_error.expect("at least one connection attempt"),
        ))
    }

    fn retry_delay(&self, attempt: usize, total_attempts: usize) {
        if attempt + 1 < total_attempts {
            thread::sleep(self.retry_backoff);
        }
    }
}

impl KvOverflowNode for ScnpKvOverflowNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn replica_id(&self) -> &str {
        &self.replica_id
    }

    fn remote_shard(&self) -> usize {
        self.remote_shard
    }

    fn direct_scnp_target(&self) -> Option<ScnpDirectTarget> {
        self.direct_target.clone()
    }

    fn circuit_breaker_health(&self) -> (bool, u64, u64) {
        self.mutation_connection.lock().circuit.health()
    }

    fn storage_health(&self) -> (u64, u64, u64) {
        self.storage_metrics.snapshot()
    }

    fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
        let expire_at_ms = ttl_ms.map(|ttl| now_millis().saturating_add(ttl));
        self.put_until(key, value, expire_at_ms)
    }

    fn put_until(&self, key: &[u8], value: &[u8], expire_at_ms: Option<u64>) -> Result<()> {
        self.put_until_on_shard(0, key, value, expire_at_ms)
    }

    fn put_until_on_shard(
        &self,
        primary_shard: usize,
        key: &[u8],
        value: &[u8],
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        self.put_until_with_governance_on_shard(primary_shard, key, value, None, expire_at_ms)
    }

    fn put_until_with_governance_on_shard(
        &self,
        primary_shard: usize,
        key: &[u8],
        value: &[u8],
        governance: Option<&[u8]>,
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        if value.len() > self.max_value_bytes {
            return Err(ShardCacheError::Protocol(
                "key-value overflow value exceeds configured maximum".into(),
            ));
        }
        if expire_at_ms.is_some_and(|deadline| deadline <= now_millis()) {
            return self.delete_on_shard(primary_shard, key);
        }
        let encoded = encode_overflow_value_until(
            value,
            governance,
            expire_at_ms,
            self.compression,
            self.compression_min_value_bytes,
            self.compression_min_savings_percent,
        );
        self.execute_on_shard(primary_shard, false, |client| client.set(key, &encoded))?;
        self.storage_metrics.record(value.len(), &encoded);
        Ok(())
    }

    fn put_batch_outcomes_until_on_shard(
        &self,
        primary_shard: usize,
        requests: &[KvOverflowPutRequest<'_>],
        outcomes: &mut Vec<bool>,
    ) {
        if requests
            .iter()
            .any(|request| request.value.len() > self.max_value_bytes)
        {
            outcomes.clear();
            outcomes.extend(requests.iter().map(|request| {
                self.put_until_on_shard(
                    primary_shard,
                    request.key,
                    request.value,
                    request.expire_at_ms,
                )
                .is_ok()
            }));
            return;
        }
        let encoded = requests
            .iter()
            .map(|request| {
                encode_overflow_value_until(
                    request.value,
                    request.governance,
                    request.expire_at_ms,
                    self.compression,
                    self.compression_min_value_bytes,
                    self.compression_min_savings_percent,
                )
            })
            .collect::<Vec<_>>();
        let pipeline = self.execute_on_shard(primary_shard, false, |client| {
            let now_ms = now_millis();
            for (request, value) in requests.iter().zip(&encoded) {
                if request
                    .expire_at_ms
                    .is_some_and(|deadline| deadline <= now_ms)
                {
                    client.begin_del(request.key)?;
                } else {
                    client.begin_set(request.key, value)?;
                }
            }
            client.flush()?;
            requests
                .iter()
                .map(|request| {
                    if request
                        .expire_at_ms
                        .is_some_and(|deadline| deadline <= now_ms)
                    {
                        client.finish_del().map(|_| true)
                    } else {
                        client.finish_set().map(|()| true)
                    }
                })
                .collect::<shardcache_client_rs::Result<Vec<_>>>()
        });
        match pipeline {
            Ok(results) => {
                outcomes.clear();
                outcomes.extend(results);
                for ((request, encoded), succeeded) in
                    requests.iter().zip(&encoded).zip(outcomes.iter())
                {
                    if *succeeded {
                        self.storage_metrics.record(request.value.len(), encoded);
                    }
                }
            }
            Err(_) => {
                outcomes.clear();
                outcomes.extend(requests.iter().map(|request| {
                    self.put_until_on_shard(
                        primary_shard,
                        request.key,
                        request.value,
                        request.expire_at_ms,
                    )
                    .is_ok()
                }));
            }
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        self.get_on_shard(0, key)
    }

    fn get_on_shard(&self, primary_shard: usize, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        let encoded = self.execute_on_shard(primary_shard, true, |client| {
            let mut value = Vec::new();
            if !client.get_into_limited(
                key,
                &mut value,
                self.max_value_bytes
                    .saturating_add(KV_OVERFLOW_COMPRESSED_HEADER_LEN),
            )? {
                return Ok(None::<Vec<u8>>);
            }
            Ok(Some(value))
        })?;
        encoded
            .map(|value| {
                decode_overflow_value(
                    &value,
                    self.max_value_bytes,
                    self.compression_max_expansion_ratio,
                )
                .map_err(|message| {
                    ShardCacheError::Protocol(format!("kv overflow node {}: {message}", self.id))
                })
            })
            .transpose()
            .map(Option::flatten)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_on_shard(0, key)
    }

    fn delete_on_shard(&self, primary_shard: usize, key: &[u8]) -> Result<()> {
        self.execute_on_shard(primary_shard, false, |client| client.del(key).map(|_| ()))
    }
}

fn resolve_address(endpoint: &str) -> Result<SocketAddr> {
    endpoint
        .to_socket_addrs()
        .map_err(ShardCacheError::Io)?
        .next()
        .ok_or_else(|| ShardCacheError::Config(format!("{endpoint} resolved to no addresses")))
}

fn read_scnp_auth_token(config: &KvOverflowConfig) -> Result<Option<Arc<ReloadableScnpAuthToken>>> {
    if let Some(path) = config.scnp_auth_token_path.as_ref() {
        let reload_ms = match config.scnp_auth_token_reload_interval_ms {
            0 => 30_000,
            configured => configured,
        };
        return ReloadableScnpAuthToken::new_file(path.clone(), Duration::from_millis(reload_ms))
            .map(Arc::new)
            .map(Some);
    }
    let Some(name) = config.scnp_auth_token_env.as_deref() else {
        return Ok(None);
    };
    let value = std::env::var(name).map_err(|error| {
        ShardCacheError::Config(format!(
            "failed to read SCNP auth token env {name:?}: {error}"
        ))
    })?;
    let token = validate_scnp_auth_token(value.into_bytes(), name)?;
    Ok(Some(Arc::new(ReloadableScnpAuthToken::new_static(token))))
}

fn read_scnp_auth_token_file(path: &Path) -> Result<Arc<[u8]>> {
    let mut value = std::fs::read(path)?;
    while value
        .last()
        .is_some_and(|byte| matches!(byte, b'\n' | b'\r'))
    {
        value.pop();
    }
    validate_scnp_auth_token(value, &path.display().to_string())
}

fn validate_scnp_auth_token(value: Vec<u8>, source: &str) -> Result<Arc<[u8]>> {
    if value.is_empty() || value.len() > u16::MAX as usize {
        return Err(ShardCacheError::Config(format!(
            "SCNP auth token {source:?} must contain 1..=65535 bytes"
        )));
    }
    Ok(Arc::from(value))
}

#[cfg(feature = "scnp-tls")]
fn load_scnp_tls_config(
    config: &KvOverflowConfig,
    server_name: Option<&str>,
) -> Result<Option<Arc<RuntimeScnpTlsClientConfig>>> {
    if !config.scnp_tls.enabled {
        return Ok(None);
    }
    let server_name = server_name.ok_or_else(|| {
        ShardCacheError::Config("SCNP TLS requires a certificate server name".into())
    })?;
    RuntimeScnpTlsClientConfig::from_pem_files(
        server_name,
        &config.scnp_tls.ca_path,
        config.scnp_tls.client_cert_path.as_deref(),
        config.scnp_tls.client_key_path.as_deref(),
    )
    .map(Arc::new)
    .map(Some)
    .map_err(|error| ShardCacheError::Config(format!("invalid SCNP TLS config: {error}")))
}

fn validate_replica_topology(
    replica: &KvOverflowReplica,
    config: &KvOverflowConfig,
) -> Result<u16> {
    let mut advertised_base_port = None;
    let auth_token = read_scnp_auth_token(config)?;
    let auth_token = auth_token.as_ref().map(|token| token.current());
    #[cfg(feature = "scnp-tls")]
    let tls = load_scnp_tls_config(
        config,
        replica.tls_server_name.as_deref().or(Some(&replica.id)),
    )?;
    for address in &replica.addresses {
        #[cfg(feature = "scnp-tls")]
        let client = if let Some(tls) = tls.as_deref() {
            ShardCacheClient::connect_with_timeouts_auth_and_tls(
                address.as_str(),
                Duration::from_millis(config.connect_timeout_ms.max(1)),
                Duration::from_millis(config.operation_timeout_ms.max(1)),
                auth_token.as_deref(),
                tls,
            )
        } else {
            ShardCacheClient::connect_with_timeouts_and_auth(
                address.as_str(),
                Duration::from_millis(config.connect_timeout_ms.max(1)),
                Duration::from_millis(config.operation_timeout_ms.max(1)),
                auth_token.as_deref(),
            )
        };
        #[cfg(not(feature = "scnp-tls"))]
        let client = ShardCacheClient::connect_with_timeouts_and_auth(
            address.as_str(),
            Duration::from_millis(config.connect_timeout_ms.max(1)),
            Duration::from_millis(config.operation_timeout_ms.max(1)),
            auth_token.as_deref(),
        );
        let mut client = client.map_err(|error| client_error(address, error))?;
        let topology = client
            .topology()
            .map_err(|error| client_error(address, error))?;
        if topology.node_id != replica.id
            || topology.shard_count != replica.shard_count
            || topology.route_mode != "overflow_slot"
            || topology.direct_shard_base_port == 0
            || !topology
                .capabilities
                .iter()
                .any(|capability| capability == "overflow_slot_v1")
            || (config.compression == KvOverflowCompression::Lz4
                && !topology
                    .capabilities
                    .iter()
                    .any(|capability| capability == "value_envelope_v2"))
        {
            return Err(ShardCacheError::Config(format!(
                "overflow replica {} topology mismatch at {}: id={}, shards={}, route={}, capabilities={:?}",
                replica.id,
                address,
                topology.node_id,
                topology.shard_count,
                topology.route_mode,
                topology.capabilities,
            )));
        }
        if replica.direct_shard_base_port != 0
            && replica.direct_shard_base_port != topology.direct_shard_base_port
        {
            return Err(ShardCacheError::Config(format!(
                "overflow replica {} direct port mismatch at {}: configured={}, advertised={}",
                replica.id,
                address,
                replica.direct_shard_base_port,
                topology.direct_shard_base_port,
            )));
        }
        if advertised_base_port
            .replace(topology.direct_shard_base_port)
            .is_some_and(|port| port != topology.direct_shard_base_port)
        {
            return Err(ShardCacheError::Config(format!(
                "overflow replica {} paths advertise different direct port bases",
                replica.id
            )));
        }
    }
    advertised_base_port.ok_or_else(|| {
        ShardCacheError::Config(format!("overflow replica {} has no addresses", replica.id))
    })
}

/// Redis/Valkey-compatible overflow node with pooled blocking connections.
#[cfg(feature = "kv-overflow-redis")]
pub struct RedisKvOverflowNode {
    id: String,
    replica_id: String,
    remote_shard: usize,
    endpoint: String,
    key_prefix: Box<[u8]>,
    client: redis_client::Client,
    connections: Box<[Mutex<Option<redis_client::Connection>>]>,
    circuits: Box<[CircuitBreaker]>,
    connect_timeout: Duration,
    operation_timeout: Duration,
    max_retries: usize,
    retry_backoff: Duration,
    compression: KvOverflowCompression,
    compression_min_value_bytes: usize,
    compression_min_savings_percent: u8,
    max_value_bytes: usize,
    compression_max_expansion_ratio: usize,
    storage_metrics: OverflowNodeStorageMetrics,
}

#[cfg(feature = "kv-overflow-redis")]
impl std::fmt::Debug for RedisKvOverflowNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedisKvOverflowNode")
            .field("id", &self.id)
            .field("replica_id", &self.replica_id)
            .field("remote_shard", &self.remote_shard)
            .field("endpoint", &self.endpoint)
            .field("key_prefix", &String::from_utf8_lossy(&self.key_prefix))
            .field("connections", &self.connections.len())
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

#[cfg(feature = "kv-overflow-redis")]
impl RedisKvOverflowNode {
    pub fn from_config(endpoint: String, config: &KvOverflowConfig) -> Result<Self> {
        Self::from_config_for_shards(endpoint, config, 1)
    }

    /// Creates one persistent connection slot per primary shard and endpoint.
    pub fn from_config_for_shards(
        endpoint: String,
        config: &KvOverflowConfig,
        primary_shard_count: usize,
    ) -> Result<Self> {
        use redis_client::IntoConnectionInfo;

        if config.redis_key_prefix.is_empty() {
            return Err(ShardCacheError::Config(
                "kv_overflow.redis_key_prefix must not be empty for the Redis backend".into(),
            ));
        }
        let mut connection_info = endpoint
            .as_str()
            .into_connection_info()
            .map_err(redis_config_error)?;
        if connection_info.redis.username.is_some() || connection_info.redis.password.is_some() {
            return Err(ShardCacheError::Config(
                "Redis overflow credentials must use configured environment variables, not endpoint URLs"
                    .into(),
            ));
        }
        connection_info.redis.username = read_redis_credential_env(
            config.redis_username_env.as_deref(),
            "kv_overflow.redis_username_env",
        )?;
        connection_info.redis.password = read_redis_credential_env(
            config.redis_password_env.as_deref(),
            "kv_overflow.redis_password_env",
        )?;
        if connection_info.redis.username.is_some() && connection_info.redis.password.is_none() {
            return Err(ShardCacheError::Config(
                "kv_overflow.redis_password_env is required with redis_username_env".into(),
            ));
        }
        let client = redis_client::Client::open(connection_info).map_err(redis_config_error)?;
        let connection_count = config
            .connections_per_endpoint
            .max(primary_shard_count)
            .max(1);
        Ok(Self {
            id: endpoint.clone(),
            replica_id: endpoint.clone(),
            remote_shard: 0,
            endpoint,
            key_prefix: config.redis_key_prefix.as_bytes().into(),
            client,
            connections: (0..connection_count)
                .map(|_| Mutex::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            circuits: (0..connection_count)
                .map(|_| {
                    CircuitBreaker::new(
                        config.circuit_breaker_failure_threshold,
                        Duration::from_millis(config.circuit_breaker_cooldown_ms),
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            connect_timeout: Duration::from_millis(config.connect_timeout_ms.max(1)),
            operation_timeout: Duration::from_millis(config.operation_timeout_ms.max(1)),
            max_retries: config.max_retries,
            retry_backoff: Duration::from_millis(config.retry_backoff_ms.max(1)),
            compression: config.compression,
            compression_min_value_bytes: config.compression_min_value_bytes,
            compression_min_savings_percent: config.compression_min_savings_percent,
            max_value_bytes: config.max_value_bytes,
            compression_max_expansion_ratio: config.compression_max_expansion_ratio,
            storage_metrics: OverflowNodeStorageMetrics::default(),
        })
    }

    fn from_replica_target(
        replica_id: String,
        remote_shard: usize,
        endpoint: String,
        config: &KvOverflowConfig,
        _primary_shard_count: usize,
    ) -> Result<Self> {
        let mut node = Self::from_config_for_shards(endpoint, config, 1)?;
        node.id = format!("{replica_id}#{remote_shard}");
        node.replica_id = replica_id;
        node.remote_shard = remote_shard;
        Ok(node)
    }

    fn execute_on_shard<T>(
        &self,
        primary_shard: usize,
        mut operation: impl FnMut(&mut redis_client::Connection) -> redis_client::RedisResult<T>,
    ) -> Result<T> {
        let slot = primary_shard % self.connections.len();
        let circuit = &self.circuits[slot];
        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            if !circuit.allow_request() {
                return Err(ShardCacheError::Protocol(format!(
                    "Redis overflow target {} circuit breaker is open",
                    self.id
                )));
            }
            let mut connection = self.connections[slot].lock();
            if connection.is_none() {
                match self.connect() {
                    Ok(client) => *connection = Some(client),
                    Err(error) => {
                        circuit.record_failure();
                        last_error = Some(error);
                        drop(connection);
                        self.retry_delay(attempt);
                        continue;
                    }
                }
            }
            let result = operation(connection.as_mut().expect("connection initialized"));
            match result {
                Ok(value) => {
                    circuit.record_success();
                    return Ok(value);
                }
                Err(error) => {
                    circuit.record_failure();
                    *connection = None;
                    last_error = Some(error);
                }
            }
            drop(connection);
            self.retry_delay(attempt);
        }
        Err(redis_node_error(
            &self.endpoint,
            last_error.expect("at least one connection attempt"),
        ))
    }

    fn connect(&self) -> redis_client::RedisResult<redis_client::Connection> {
        let connection = self
            .client
            .get_connection_with_timeout(self.connect_timeout)?;
        connection.set_read_timeout(Some(self.operation_timeout))?;
        connection.set_write_timeout(Some(self.operation_timeout))?;
        Ok(connection)
    }

    fn retry_delay(&self, attempt: usize) {
        if attempt < self.max_retries {
            thread::sleep(self.retry_backoff);
        }
    }

    fn storage_key(&self, key: &[u8]) -> Vec<u8> {
        let mut storage_key = Vec::with_capacity(self.key_prefix.len().saturating_add(key.len()));
        storage_key.extend_from_slice(&self.key_prefix);
        storage_key.extend_from_slice(key);
        storage_key
    }

    fn put_until_for_shard(
        &self,
        primary_shard: usize,
        key: &[u8],
        value: &[u8],
        governance: Option<&[u8]>,
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        if value.len() > self.max_value_bytes {
            return Err(ShardCacheError::Protocol(
                "key-value overflow value exceeds configured maximum".into(),
            ));
        }
        let storage_key = self.storage_key(key);
        let encoded = encode_overflow_value_until(
            value,
            governance,
            expire_at_ms,
            self.compression,
            self.compression_min_value_bytes,
            self.compression_min_savings_percent,
        );
        self.execute_on_shard(primary_shard, |connection| {
            let ttl_ms = expire_at_ms.map(|deadline| deadline.saturating_sub(now_millis()));
            if ttl_ms == Some(0) {
                return redis_client::cmd("DEL")
                    .arg(&storage_key)
                    .query::<u64>(connection)
                    .map(|_| ());
            }
            let mut command = redis_client::cmd("SET");
            command.arg(&storage_key).arg(&encoded);
            if let Some(ttl_ms) = ttl_ms {
                command.arg("PX").arg(ttl_ms);
            }
            command.query(connection)
        })?;
        self.storage_metrics.record(value.len(), &encoded);
        Ok(())
    }

    fn put_batch_until_for_shard(
        &self,
        primary_shard: usize,
        requests: &[KvOverflowPutRequest<'_>],
    ) -> Result<()> {
        if requests
            .iter()
            .any(|request| request.value.len() > self.max_value_bytes)
        {
            return Err(ShardCacheError::Protocol(
                "key-value overflow value exceeds configured maximum".into(),
            ));
        }
        struct EncodedPut {
            key: Vec<u8>,
            value: Vec<u8>,
            expire_at_ms: Option<u64>,
        }

        let encoded = requests
            .iter()
            .map(|request| EncodedPut {
                key: self.storage_key(request.key),
                value: encode_overflow_value_until(
                    request.value,
                    request.governance,
                    request.expire_at_ms,
                    self.compression,
                    self.compression_min_value_bytes,
                    self.compression_min_savings_percent,
                ),
                expire_at_ms: request.expire_at_ms,
            })
            .collect::<Vec<_>>();
        self.execute_on_shard(primary_shard, |connection| {
            let now_ms = now_millis();
            let mut pipeline = redis_client::pipe();
            for request in &encoded {
                let ttl_ms = request
                    .expire_at_ms
                    .map(|deadline| deadline.saturating_sub(now_ms));
                if ttl_ms == Some(0) {
                    pipeline.cmd("DEL").arg(&request.key).ignore();
                    continue;
                }
                let command = pipeline.cmd("SET").arg(&request.key).arg(&request.value);
                if let Some(ttl_ms) = ttl_ms {
                    command.arg("PX").arg(ttl_ms);
                }
                command.ignore();
            }
            pipeline.query::<()>(connection)
        })?;
        for (request, encoded) in requests.iter().zip(&encoded) {
            self.storage_metrics
                .record(request.value.len(), &encoded.value);
        }
        Ok(())
    }

    fn get_for_shard(&self, primary_shard: usize, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        let storage_key = self.storage_key(key);
        let encoded: Option<Vec<u8>> = self.execute_on_shard(primary_shard, |connection| {
            redis_client::cmd("GET").arg(&storage_key).query(connection)
        })?;
        encoded
            .map(|value| {
                decode_overflow_value(
                    &value,
                    self.max_value_bytes,
                    self.compression_max_expansion_ratio,
                )
                .map_err(|message| {
                    ShardCacheError::Protocol(format!(
                        "Redis overflow node {}: {message}",
                        self.endpoint
                    ))
                })
            })
            .transpose()
            .map(Option::flatten)
    }

    fn delete_for_shard(&self, primary_shard: usize, key: &[u8]) -> Result<()> {
        let storage_key = self.storage_key(key);
        self.execute_on_shard(primary_shard, |connection| {
            redis_client::cmd("DEL")
                .arg(&storage_key)
                .query::<u64>(connection)
                .map(|_| ())
        })
    }
}

#[cfg(feature = "kv-overflow-redis")]
impl KvOverflowNode for RedisKvOverflowNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn replica_id(&self) -> &str {
        &self.replica_id
    }

    fn remote_shard(&self) -> usize {
        self.remote_shard
    }

    fn circuit_breaker_health(&self) -> (bool, u64, u64) {
        self.circuits.iter().fold(
            (false, 0u64, 0u64),
            |(open, events, suppressed), circuit| {
                let (circuit_open, circuit_events, circuit_suppressed) = circuit.health();
                (
                    open || circuit_open,
                    events.saturating_add(circuit_events),
                    suppressed.saturating_add(circuit_suppressed),
                )
            },
        )
    }

    fn storage_health(&self) -> (u64, u64, u64) {
        self.storage_metrics.snapshot()
    }

    fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
        let expire_at_ms = ttl_ms.map(|ttl| now_millis().saturating_add(ttl));
        self.put_until(key, value, expire_at_ms)
    }

    fn put_until(&self, key: &[u8], value: &[u8], expire_at_ms: Option<u64>) -> Result<()> {
        self.put_until_for_shard(0, key, value, None, expire_at_ms)
    }

    fn put_until_on_shard(
        &self,
        primary_shard: usize,
        key: &[u8],
        value: &[u8],
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        self.put_until_for_shard(primary_shard, key, value, None, expire_at_ms)
    }

    fn put_until_with_governance_on_shard(
        &self,
        primary_shard: usize,
        key: &[u8],
        value: &[u8],
        governance: Option<&[u8]>,
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        self.put_until_for_shard(primary_shard, key, value, governance, expire_at_ms)
    }

    fn put_batch_until(&self, requests: &[KvOverflowPutRequest<'_>]) -> Result<()> {
        self.put_batch_until_for_shard(0, requests)
    }

    fn put_batch_outcomes_until_on_shard(
        &self,
        primary_shard: usize,
        requests: &[KvOverflowPutRequest<'_>],
        outcomes: &mut Vec<bool>,
    ) {
        outcomes.clear();
        if self
            .put_batch_until_for_shard(primary_shard, requests)
            .is_ok()
        {
            outcomes.resize(requests.len(), true);
            return;
        }
        outcomes.extend(requests.iter().map(|request| {
            self.put_until_for_shard(
                primary_shard,
                request.key,
                request.value,
                request.governance,
                request.expire_at_ms,
            )
            .is_ok()
        }));
    }

    fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        self.get_for_shard(0, key)
    }

    fn get_on_shard(&self, primary_shard: usize, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        self.get_for_shard(primary_shard, key)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_for_shard(0, key)
    }

    fn delete_on_shard(&self, primary_shard: usize, key: &[u8]) -> Result<()> {
        self.delete_for_shard(primary_shard, key)
    }
}

#[cfg(feature = "kv-overflow-redis")]
fn read_redis_credential_env(name: Option<&str>, field: &str) -> Result<Option<String>> {
    let Some(name) = name else {
        return Ok(None);
    };
    if name.is_empty() {
        return Err(ShardCacheError::Config(format!(
            "{field} must not be empty"
        )));
    }
    let value = std::env::var(name).map_err(|error| {
        ShardCacheError::Config(format!(
            "failed to read Redis credential env {name:?}: {error}"
        ))
    })?;
    if value.is_empty() {
        return Err(ShardCacheError::Config(format!(
            "Redis credential env {name:?} must not be empty"
        )));
    }
    Ok(Some(value))
}

#[cfg(feature = "kv-overflow-redis")]
fn redis_config_error(error: redis_client::RedisError) -> ShardCacheError {
    ShardCacheError::Config(format!("invalid Redis overflow endpoint: {error}"))
}

#[cfg(feature = "kv-overflow-redis")]
fn redis_node_error(endpoint: &str, error: redis_client::RedisError) -> ShardCacheError {
    ShardCacheError::Protocol(format!("Redis overflow node {endpoint}: {error}"))
}

fn client_error(endpoint: &str, error: ShardCacheClientError) -> ShardCacheError {
    ShardCacheError::Protocol(format!("kv overflow node {endpoint}: {error}"))
}

fn encode_overflow_value_until(
    value: &[u8],
    governance: Option<&[u8]>,
    expire_at_ms: Option<u64>,
    compression: KvOverflowCompression,
    compression_min_value_bytes: usize,
    compression_min_savings_percent: u8,
) -> Vec<u8> {
    let mut encoded = Vec::new();
    let mut compressed = Vec::new();
    encode_overflow_value_until_into(
        value,
        governance,
        expire_at_ms,
        compression,
        compression_min_value_bytes,
        compression_min_savings_percent,
        &mut encoded,
        &mut compressed,
    );
    encoded
}

#[allow(clippy::too_many_arguments)]
fn encode_overflow_value_until_into(
    value: &[u8],
    governance: Option<&[u8]>,
    expire_at_ms: Option<u64>,
    compression: KvOverflowCompression,
    compression_min_value_bytes: usize,
    compression_min_savings_percent: u8,
    encoded: &mut Vec<u8>,
    compressed: &mut Vec<u8>,
) {
    let expire_at_ms = expire_at_ms.unwrap_or(0);
    let governance_len = governance.map_or(0, <[u8]>::len);
    let governed = governance.is_some();
    let raw_header_len = if governed {
        KV_OVERFLOW_GOVERNED_HEADER_LEN
    } else {
        KV_OVERFLOW_HEADER_LEN
    };
    let compressed_header_len = if governed {
        KV_OVERFLOW_GOVERNED_COMPRESSED_HEADER_LEN
    } else {
        KV_OVERFLOW_COMPRESSED_HEADER_LEN
    };
    encoded.clear();
    if compression == KvOverflowCompression::Lz4 && value.len() >= compression_min_value_bytes {
        compressed.clear();
        compressed.resize(lz4_flex::block::get_maximum_output_size(value.len()), 0);
        let compressed_len = lz4_flex::block::compress_into(value, compressed)
            .expect("maximum LZ4 output buffer is sufficient");
        compressed.truncate(compressed_len);
        let raw_size = raw_header_len
            .saturating_add(value.len())
            .saturating_add(governance_len);
        let compressed_size = compressed_header_len
            .saturating_add(compressed.len())
            .saturating_add(governance_len);
        let savings = raw_size.saturating_sub(compressed_size);
        if compressed_size < raw_size
            && savings.saturating_mul(100)
                >= raw_size.saturating_mul(compression_min_savings_percent as usize)
        {
            encoded.reserve(compressed_size);
            encoded.extend_from_slice(if governed {
                KV_OVERFLOW_GOVERNED_COMPRESSED_MAGIC
            } else {
                KV_OVERFLOW_COMPRESSED_MAGIC
            });
            encoded.extend_from_slice(&expire_at_ms.to_le_bytes());
            encoded.extend_from_slice(&(value.len() as u64).to_le_bytes());
            encoded.extend_from_slice(&overflow_checksum(value, governance).to_le_bytes());
            if governed {
                encoded.extend_from_slice(&(governance_len as u32).to_le_bytes());
            }
            encoded.push(KV_OVERFLOW_CODEC_LZ4);
            encoded.extend_from_slice(compressed);
            if let Some(governance) = governance {
                encoded.extend_from_slice(governance);
            }
            return;
        }
    }
    encoded.reserve(
        raw_header_len
            .saturating_add(value.len())
            .saturating_add(governance_len),
    );
    encoded.extend_from_slice(if governed {
        KV_OVERFLOW_GOVERNED_MAGIC
    } else {
        KV_OVERFLOW_MAGIC
    });
    encoded.extend_from_slice(&expire_at_ms.to_le_bytes());
    encoded.extend_from_slice(&(value.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&overflow_checksum(value, governance).to_le_bytes());
    if governed {
        encoded.extend_from_slice(&(governance_len as u32).to_le_bytes());
    }
    encoded.extend_from_slice(value);
    if let Some(governance) = governance {
        encoded.extend_from_slice(governance);
    }
}

fn overflow_checksum(value: &[u8], governance: Option<&[u8]>) -> u32 {
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(value);
    if let Some(governance) = governance {
        checksum.update(governance);
    }
    checksum.finalize()
}

fn decode_overflow_value(
    value: &[u8],
    max_value_bytes: usize,
    compression_max_expansion_ratio: usize,
) -> std::result::Result<Option<KvOverflowValue>, &'static str> {
    if value.len() < KV_OVERFLOW_HEADER_LEN {
        return Err("invalid key-value overflow envelope");
    }
    let magic = &value[..KV_OVERFLOW_MAGIC.len()];
    let compressed =
        magic == KV_OVERFLOW_COMPRESSED_MAGIC || magic == KV_OVERFLOW_GOVERNED_COMPRESSED_MAGIC;
    let governed =
        magic == KV_OVERFLOW_GOVERNED_MAGIC || magic == KV_OVERFLOW_GOVERNED_COMPRESSED_MAGIC;
    if magic != KV_OVERFLOW_MAGIC
        && magic != KV_OVERFLOW_COMPRESSED_MAGIC
        && magic != KV_OVERFLOW_GOVERNED_MAGIC
        && magic != KV_OVERFLOW_GOVERNED_COMPRESSED_MAGIC
    {
        return Err("invalid key-value overflow envelope");
    }
    let expire_at_ms = u64::from_le_bytes(value[8..16].try_into().expect("fixed expiry field"));
    let payload_len = u64::from_le_bytes(
        value[16..24]
            .try_into()
            .expect("fixed payload length field"),
    );
    let checksum = u32::from_le_bytes(value[24..28].try_into().expect("fixed checksum field"));
    let payload_len =
        usize::try_from(payload_len).map_err(|_| "invalid key-value overflow payload length")?;
    if payload_len > max_value_bytes {
        return Err("key-value overflow payload exceeds configured maximum");
    }
    let governance_len = if governed {
        if value.len() < KV_OVERFLOW_GOVERNED_HEADER_LEN {
            return Err("invalid key-value overflow governance envelope");
        }
        let len = u32::from_le_bytes(
            value[28..32]
                .try_into()
                .expect("fixed governance length field"),
        ) as usize;
        if len > max_value_bytes {
            return Err("key-value overflow governance exceeds configured maximum");
        }
        len
    } else {
        0
    };
    let raw_header_len = if governed {
        KV_OVERFLOW_GOVERNED_HEADER_LEN
    } else {
        KV_OVERFLOW_HEADER_LEN
    };
    let compressed_header_len = if governed {
        KV_OVERFLOW_GOVERNED_COMPRESSED_HEADER_LEN
    } else {
        KV_OVERFLOW_COMPRESSED_HEADER_LEN
    };
    let payload = if compressed {
        if value.len() < compressed_header_len.saturating_add(governance_len)
            || value[raw_header_len] != KV_OVERFLOW_CODEC_LZ4
        {
            return Err("invalid key-value overflow compression codec");
        }
        let stored_len = value.len() - compressed_header_len - governance_len;
        if payload_len
            > stored_len
                .checked_mul(compression_max_expansion_ratio)
                .ok_or("invalid key-value overflow expansion ratio")?
        {
            return Err("key-value overflow payload exceeds configured expansion ratio");
        }
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(payload_len)
            .map_err(|_| "key-value overflow payload allocation failed")?;
        decoded.resize(payload_len, 0);
        let decoded_len = lz4_flex::block::decompress_into(
            &value[compressed_header_len..compressed_header_len + stored_len],
            &mut decoded,
        )
        .map_err(|_| "invalid key-value overflow compressed payload")?;
        let governance = governed.then(|| &value[value.len() - governance_len..]);
        if decoded_len != payload_len || overflow_checksum(&decoded, governance) != checksum {
            return Err("invalid key-value overflow payload integrity");
        }
        SharedBytes::from(decoded)
    } else {
        let expected_len = raw_header_len
            .checked_add(payload_len)
            .and_then(|len| len.checked_add(governance_len))
            .ok_or("invalid key-value overflow payload length")?;
        if value.len() != expected_len {
            return Err("invalid key-value overflow payload integrity");
        }
        let payload = &value[raw_header_len..raw_header_len + payload_len];
        let governance = governed.then(|| &value[raw_header_len + payload_len..]);
        if overflow_checksum(payload, governance) != checksum {
            return Err("invalid key-value overflow payload integrity");
        }
        SharedBytes::copy_from_slice(payload)
    };
    let governance = governed.then(|| {
        SharedBytes::copy_from_slice(&value[value.len().saturating_sub(governance_len)..])
    });
    let now_ms = now_millis();
    if expire_at_ms != 0 && expire_at_ms <= now_ms {
        return Ok(None);
    }
    Ok(Some(KvOverflowValue {
        value: payload,
        ttl_ms: (expire_at_ms != 0).then_some(expire_at_ms.saturating_sub(now_ms)),
        governance,
    }))
}

#[derive(Debug, Default)]
struct KvOverflowMetrics {
    gets: AtomicU64,
    get_hits: AtomicU64,
    get_failures: AtomicU64,
    deletes: AtomicU64,
    delete_failures: AtomicU64,
    offloads: AtomicU64,
    fault_ins: AtomicU64,
    handoff_reads: AtomicU64,
    handoff_hits: AtomicU64,
    handoff_failures: AtomicU64,
    handoff_migrated: AtomicU64,
    oversized_rejections: AtomicU64,
    metadata_rejections: AtomicU64,
    resident_backpressure: AtomicU64,
    shards: Box<[KvOverflowShardMetrics]>,
}

#[repr(align(64))]
#[derive(Debug, Default)]
struct KvOverflowShardMetrics {
    primary: KvOverflowPrimaryMetrics,
    worker: KvOverflowWorkerMetrics,
}

#[repr(align(64))]
#[derive(Debug, Default)]
struct KvOverflowPrimaryMetrics {
    enqueued_puts: AtomicU64,
    enqueue_failures: AtomicU64,
}

#[repr(align(64))]
#[derive(Debug, Default)]
struct KvOverflowWorkerMetrics {
    puts: AtomicU64,
    put_failures: AtomicU64,
    replicated_puts: AtomicU64,
    replication_failures: AtomicU64,
    pipeline_batches: AtomicU64,
    pipeline_items: AtomicU64,
    pipeline_bytes: AtomicU64,
    pipeline_latency_ns: AtomicU64,
    adaptive_pipeline_items: AtomicUsize,
    object_bytes_raw: AtomicU64,
    object_bytes_stored: AtomicU64,
    compressed_values: AtomicU64,
    active_workers: AtomicUsize,
}

#[repr(align(64))]
#[derive(Debug)]
struct KvOverflowSequence(AtomicU64);

impl Default for KvOverflowSequence {
    fn default() -> Self {
        Self(AtomicU64::new(1))
    }
}

impl KvOverflowMetrics {
    fn for_primary_shards(primary_shard_count: usize) -> Self {
        Self {
            shards: (0..primary_shard_count)
                .map(|_| KvOverflowShardMetrics::default())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            ..Self::default()
        }
    }

    #[inline]
    fn shard(&self, primary_shard: usize) -> &KvOverflowShardMetrics {
        &self.shards[primary_shard]
    }
}

/// Deterministic targets and logical slots owned by one primary shard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvOverflowPrimaryOwnershipSnapshot {
    pub primary_shard: usize,
    pub slot_start: u32,
    pub slot_end_exclusive: u32,
    pub target_ids: Vec<String>,
}

/// Health and activity counters for a partitioned overflow cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvOverflowHealthSnapshot {
    pub backend: Option<KvOverflowBackend>,
    pub transport: Option<KvOverflowTransport>,
    pub tls_enabled: bool,
    pub compression: KvOverflowCompression,
    pub adaptive_pipeline: bool,
    pub max_value_bytes: usize,
    pub max_key_bytes: usize,
    pub max_metadata_bytes: usize,
    pub metadata_bytes: usize,
    pub metadata_rejections: u64,
    pub resident_backpressure: u64,
    pub retained_buffer_bytes: usize,
    pub circuit_open_targets: usize,
    pub circuit_open_events: u64,
    pub circuit_suppressed_requests: u64,
    pub node_count: usize,
    pub previous_node_count: usize,
    pub primary_shard_count: usize,
    pub slot_count: u32,
    pub resident_keys: usize,
    pub remote_keys: usize,
    pub resident_bytes: usize,
    pub puts: u64,
    pub put_failures: u64,
    pub gets: u64,
    pub get_hits: u64,
    pub get_failures: u64,
    pub deletes: u64,
    pub delete_failures: u64,
    pub offloads: u64,
    pub fault_ins: u64,
    pub queue_depth: usize,
    pub queue_capacity: usize,
    pub shard_queue_depths: Vec<usize>,
    pub shard_queue_capacities: Vec<usize>,
    pub completion_backlog: usize,
    pub shard_completion_backlogs: Vec<usize>,
    pub drains_per_shard: usize,
    pub pending_keys: usize,
    pub failed_pending_keys: usize,
    pub active_workers: usize,
    pub enqueued_puts: u64,
    pub enqueue_failures: u64,
    pub replicated_puts: u64,
    pub replication_failures: u64,
    pub handoff_reads: u64,
    pub handoff_hits: u64,
    pub handoff_failures: u64,
    pub handoff_migrated: u64,
    pub handoff_pending: usize,
    pub oversized_rejections: u64,
    pub pipeline_batches: u64,
    pub pipeline_items: u64,
    pub pipeline_bytes: u64,
    pub pipeline_latency_ns: u64,
    pub shard_pipeline_item_limits: Vec<usize>,
    pub object_bytes_raw: u64,
    pub object_bytes_stored: u64,
    pub compressed_values: u64,
    pub ownership: Vec<KvOverflowPrimaryOwnershipSnapshot>,
}

/// Fixed-slot collection of disjoint key-value overflow nodes.
pub struct KvOverflowCluster {
    nodes: Box<[Arc<dyn KvOverflowNode>]>,
    previous_nodes: Box<[Arc<dyn KvOverflowNode>]>,
    slot_count: u32,
    primary_shard_count: usize,
    slots_per_primary_shard: u32,
    slot_owners: Box<[usize]>,
    previous_slot_owners: Box<[usize]>,
    cluster_id: Option<Box<[u8]>>,
    backend: Option<KvOverflowBackend>,
    transport: Option<KvOverflowTransport>,
    tls_enabled: bool,
    metrics: Arc<KvOverflowMetrics>,
}

impl std::fmt::Debug for KvOverflowCluster {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvOverflowCluster")
            .field("nodes", &self.node_ids())
            .field("previous_nodes", &self.previous_node_ids())
            .field("primary_shard_count", &self.primary_shard_count)
            .field("slot_count", &self.slot_count)
            .field("cluster_id", &self.cluster_id.as_deref())
            .finish()
    }
}

impl KvOverflowCluster {
    pub fn from_config(config: &KvOverflowConfig) -> Result<Self> {
        Self::from_config_for_primary_shards(config, 1)
    }

    /// Creates a configured cluster with independent slot ranges and
    /// connection affinity for each embedded primary shard.
    pub fn from_config_for_primary_shards(
        config: &KvOverflowConfig,
        primary_shard_count: usize,
    ) -> Result<Self> {
        if config.replicas.is_empty() && config.endpoints.is_empty() {
            return Err(ShardCacheError::Config(
                "kv_overflow.replicas must contain at least one server".into(),
            ));
        }
        if !config.replicas.is_empty() {
            let scnp_auth_token = (config.backend == KvOverflowBackend::Scnp)
                .then(|| read_scnp_auth_token(config))
                .transpose()?
                .flatten();
            let normalize_replicas =
                |replicas: &[KvOverflowReplica]| -> Result<Vec<KvOverflowReplica>> {
                    replicas
                        .iter()
                        .map(|replica| {
                            let mut replica = replica.clone();
                            if config.backend == KvOverflowBackend::Scnp {
                                let advertised = validate_replica_topology(&replica, config)?;
                                if replica.direct_shard_base_port == 0 {
                                    replica.direct_shard_base_port = advertised;
                                }
                            }
                            Ok(replica)
                        })
                        .collect()
                };
            let replicas = normalize_replicas(&config.replicas)?;
            let previous_replicas = normalize_replicas(&config.previous_replicas)?;
            let make_targets =
                |replicas: &[KvOverflowReplica]| -> Result<Vec<Arc<dyn KvOverflowNode>>> {
                    let mut nodes = Vec::<Arc<dyn KvOverflowNode>>::new();
                    for replica in replicas {
                        match config.backend {
                            KvOverflowBackend::Scnp => {
                                #[cfg(feature = "scnp-tls")]
                                let tls = load_scnp_tls_config(
                                    config,
                                    replica.tls_server_name.as_deref().or(Some(&replica.id)),
                                )?;
                                for remote_shard in 0..replica.shard_count {
                                    nodes.push(Arc::new(ScnpKvOverflowNode::from_replica_target(
                                        replica,
                                        remote_shard,
                                        config,
                                        primary_shard_count,
                                        scnp_auth_token.clone(),
                                        #[cfg(feature = "scnp-tls")]
                                        tls.clone(),
                                    )?)
                                        as Arc<dyn KvOverflowNode>);
                                }
                            }
                            KvOverflowBackend::Redis => {
                                #[cfg(feature = "kv-overflow-redis")]
                                {
                                    let endpoint =
                                        replica.addresses.first().cloned().ok_or_else(|| {
                                            ShardCacheError::Config(format!(
                                                "kv overflow replica {} has no addresses",
                                                replica.id
                                            ))
                                        })?;
                                    for virtual_shard in 0..primary_shard_count {
                                        nodes.push(Arc::new(
                                            RedisKvOverflowNode::from_replica_target(
                                                replica.id.clone(),
                                                virtual_shard,
                                                endpoint.clone(),
                                                config,
                                                primary_shard_count,
                                            )?,
                                        )
                                            as Arc<dyn KvOverflowNode>);
                                    }
                                }
                                #[cfg(not(feature = "kv-overflow-redis"))]
                            return Err(ShardCacheError::Config(
                                "kv_overflow.backend = \"redis\" requires the kv-overflow-redis feature".into(),
                            ));
                            }
                        }
                    }
                    Ok(nodes)
                };
            let mut cluster = Self::with_topology(
                make_targets(&replicas)?,
                make_targets(&previous_replicas)?,
                config.slot_count,
                primary_shard_count,
                config.previous_primary_shard_count,
                Some(config.cluster_id.as_bytes().into()),
                true,
            )?;
            cluster.backend = Some(config.backend);
            cluster.transport = Some(config.transport);
            cluster.tls_enabled =
                config.backend == KvOverflowBackend::Scnp && config.scnp_tls.enabled;
            return Ok(cluster);
        }
        let make_nodes = |endpoints: &[String]| -> Result<Vec<Arc<dyn KvOverflowNode>>> {
            endpoints
                .iter()
                .cloned()
                .map(|endpoint| match config.backend {
                    KvOverflowBackend::Scnp => Ok(Arc::new(
                        ScnpKvOverflowNode::from_config_for_shards(
                            endpoint,
                            config,
                            primary_shard_count,
                        )?,
                    ) as Arc<dyn KvOverflowNode>),
                    KvOverflowBackend::Redis => {
                        #[cfg(feature = "kv-overflow-redis")]
                        {
                            Ok(Arc::new(RedisKvOverflowNode::from_config_for_shards(
                                endpoint,
                                config,
                                primary_shard_count,
                            )?) as Arc<dyn KvOverflowNode>)
                        }
                        #[cfg(not(feature = "kv-overflow-redis"))]
                        {
                            let _ = endpoint;
                            Err(ShardCacheError::Config(
                                "kv_overflow.backend = \"redis\" requires the kv-overflow-redis feature"
                                    .into(),
                            ))
                        }
                    }
                })
                .collect()
        };
        let mut cluster = Self::with_topology(
            make_nodes(&config.endpoints)?,
            make_nodes(&config.previous_endpoints)?,
            config.slot_count,
            primary_shard_count,
            config.previous_primary_shard_count,
            Some(config.cluster_id.as_bytes().into()),
            false,
        )?;
        cluster.backend = Some(config.backend);
        cluster.transport = Some(KvOverflowTransport::Fanout);
        cluster.tls_enabled = config.backend == KvOverflowBackend::Scnp && config.scnp_tls.enabled;
        Ok(cluster)
    }

    pub fn new(nodes: Vec<Arc<dyn KvOverflowNode>>) -> Result<Self> {
        Self::with_previous(nodes, Vec::new(), DEFAULT_KV_OVERFLOW_SLOT_COUNT)
    }

    /// Creates a fixed-slot membership with an optional previous ring.
    ///
    /// The previous ring is consulted only when the current owner returns a
    /// clean miss. Migration is performed only by ordered writes or an
    /// authoritative resynchronization, never by an uncoordinated read.
    pub fn with_previous(
        nodes: Vec<Arc<dyn KvOverflowNode>>,
        previous_nodes: Vec<Arc<dyn KvOverflowNode>>,
        slot_count: u32,
    ) -> Result<Self> {
        Self::with_previous_for_primary_shards(nodes, previous_nodes, slot_count, 1)
    }

    /// Creates a fixed-slot membership split into one contiguous slot range
    /// per embedded primary shard.
    pub fn with_previous_for_primary_shards(
        nodes: Vec<Arc<dyn KvOverflowNode>>,
        previous_nodes: Vec<Arc<dyn KvOverflowNode>>,
        slot_count: u32,
        primary_shard_count: usize,
    ) -> Result<Self> {
        Self::with_topology(
            nodes,
            previous_nodes,
            slot_count,
            primary_shard_count,
            None,
            None,
            false,
        )
    }

    fn with_topology(
        mut nodes: Vec<Arc<dyn KvOverflowNode>>,
        mut previous_nodes: Vec<Arc<dyn KvOverflowNode>>,
        slot_count: u32,
        primary_shard_count: usize,
        previous_primary_shard_count: Option<usize>,
        cluster_id: Option<Box<[u8]>>,
        strict_targets: bool,
    ) -> Result<Self> {
        nodes.sort_by(|left, right| {
            left.replica_id()
                .cmp(right.replica_id())
                .then_with(|| left.remote_shard().cmp(&right.remote_shard()))
                .then_with(|| left.id().cmp(right.id()))
        });
        previous_nodes.sort_by(|left, right| {
            left.replica_id()
                .cmp(right.replica_id())
                .then_with(|| left.remote_shard().cmp(&right.remote_shard()))
                .then_with(|| left.id().cmp(right.id()))
        });
        if nodes.is_empty() {
            return Err(ShardCacheError::Config(
                "kv overflow cluster requires at least one node".into(),
            ));
        }
        if nodes.windows(2).any(|pair| pair[0].id() == pair[1].id()) {
            return Err(ShardCacheError::Config(
                "kv overflow node IDs must be unique".into(),
            ));
        }
        if previous_nodes
            .windows(2)
            .any(|pair| pair[0].id() == pair[1].id())
        {
            return Err(ShardCacheError::Config(
                "previous kv overflow node IDs must be unique".into(),
            ));
        }
        if slot_count == 0 || !slot_count.is_power_of_two() {
            return Err(ShardCacheError::Config(
                "kv overflow slot count must be a non-zero power of two".into(),
            ));
        }
        if slot_count > MAX_KV_OVERFLOW_SLOT_COUNT {
            return Err(ShardCacheError::Config(format!(
                "kv overflow slot count cannot exceed {MAX_KV_OVERFLOW_SLOT_COUNT}"
            )));
        }
        if primary_shard_count == 0 || !primary_shard_count.is_power_of_two() {
            return Err(ShardCacheError::Config(
                "kv overflow primary shard count must be a non-zero power of two".into(),
            ));
        }
        if primary_shard_count > slot_count as usize {
            return Err(ShardCacheError::Config(
                "kv overflow slot count must be at least the primary shard count".into(),
            ));
        }
        let slots_per_primary_shard = slot_count / primary_shard_count as u32;
        let slot_owners =
            shard_owned_slot_table(&nodes, slot_count, primary_shard_count, strict_targets)?;
        let previous_slot_owners = if previous_nodes.is_empty() {
            Box::default()
        } else {
            shard_owned_slot_table(
                &previous_nodes,
                slot_count,
                previous_primary_shard_count.unwrap_or(primary_shard_count),
                strict_targets,
            )?
        };
        Ok(Self {
            nodes: nodes.into_boxed_slice(),
            previous_nodes: previous_nodes.into_boxed_slice(),
            slot_count,
            primary_shard_count,
            slots_per_primary_shard,
            slot_owners,
            previous_slot_owners,
            cluster_id,
            backend: None,
            transport: None,
            tls_enabled: false,
            metrics: Arc::new(KvOverflowMetrics::for_primary_shards(primary_shard_count)),
        })
    }

    pub fn node_ids(&self) -> Vec<&str> {
        self.nodes.iter().map(|node| node.id()).collect()
    }

    pub fn previous_node_ids(&self) -> Vec<&str> {
        self.previous_nodes.iter().map(|node| node.id()).collect()
    }

    pub fn ownership_snapshot(&self) -> Vec<KvOverflowPrimaryOwnershipSnapshot> {
        (0..self.primary_shard_count)
            .map(|primary_shard| {
                let slot_start = primary_shard as u32 * self.slots_per_primary_shard;
                let slot_end_exclusive = slot_start + self.slots_per_primary_shard;
                let mut target_ids = Vec::new();
                for slot in slot_start..slot_end_exclusive {
                    let target = self.nodes[self.slot_owners[slot as usize]].id();
                    if target_ids.last().is_none_or(|previous| previous != target) {
                        target_ids.push(target.to_string());
                    }
                }
                KvOverflowPrimaryOwnershipSnapshot {
                    primary_shard,
                    slot_start,
                    slot_end_exclusive,
                    target_ids,
                }
            })
            .collect()
    }

    /// Returns the stable logical overflow slot for `key`.
    pub fn slot_for_key(&self, key: &[u8]) -> u32 {
        let key_hash = xxh3_64(key);
        self.slot_for_hash_on_shard(self.primary_shard_for_hash(key_hash), key_hash)
    }

    #[inline]
    fn slot_for_hash_on_shard(&self, primary_shard: usize, key_hash: u64) -> u32 {
        debug_assert!(primary_shard < self.primary_shard_count);
        let slot = stripe_index(key_hash, shift_for(self.slot_count as usize)) as u32;
        debug_assert_eq!(
            primary_shard,
            slot as usize / self.slots_per_primary_shard as usize
        );
        slot
    }

    #[inline]
    fn primary_shard_for_hash(&self, key_hash: u64) -> usize {
        stripe_index(key_hash, shift_for(self.primary_shard_count))
    }

    pub fn slot_count(&self) -> u32 {
        self.slot_count
    }

    pub fn primary_shard_count(&self) -> usize {
        self.primary_shard_count
    }

    fn for_primary_shards(&self, primary_shard_count: usize) -> Result<Self> {
        let mut cluster = Self::with_topology(
            self.nodes.to_vec(),
            self.previous_nodes.to_vec(),
            self.slot_count,
            primary_shard_count,
            None,
            self.cluster_id.clone(),
            false,
        )?;
        cluster.backend = self.backend;
        cluster.transport = self.transport;
        Ok(cluster)
    }

    pub fn owner_index(&self, key: &[u8]) -> usize {
        self.owner_index_for_hash(xxh3_64(key))
    }

    #[inline]
    fn owner_index_for_hash(&self, key_hash: u64) -> usize {
        self.owner_index_for_hash_on_shard(self.primary_shard_for_hash(key_hash), key_hash)
    }

    #[inline]
    fn owner_index_for_hash_on_shard(&self, primary_shard: usize, key_hash: u64) -> usize {
        self.slot_owners[self.slot_for_hash_on_shard(primary_shard, key_hash) as usize]
    }

    fn direct_targets_for_shard(
        &self,
        primary_shard: usize,
    ) -> Option<Vec<(usize, ScnpDirectTarget)>> {
        if self.transport != Some(KvOverflowTransport::DirectShard)
            || !self.previous_nodes.is_empty()
        {
            return None;
        }
        let slot_start = primary_shard as u32 * self.slots_per_primary_shard;
        let slot_end = slot_start + self.slots_per_primary_shard;
        let mut owners = self.slot_owners[slot_start as usize..slot_end as usize].to_vec();
        owners.sort_unstable();
        owners.dedup();
        owners
            .into_iter()
            .map(|owner| {
                self.nodes[owner]
                    .direct_scnp_target()
                    .map(|target| (owner, target))
            })
            .collect()
    }

    pub fn owner_id(&self, key: &[u8]) -> &str {
        self.nodes[self.owner_index(key)].id()
    }

    /// Returns the current owner for a logical slot.
    pub fn slot_owner_id(&self, slot: u32) -> Result<&str> {
        if slot >= self.slot_count {
            return Err(ShardCacheError::Config(format!(
                "kv overflow slot {slot} is outside 0..{}",
                self.slot_count
            )));
        }
        Ok(self.nodes[self.slot_owners[slot as usize]].id())
    }

    fn previous_owner(
        &self,
        primary_shard: usize,
        key_hash: u64,
    ) -> Option<&Arc<dyn KvOverflowNode>> {
        self.previous_owner_index(primary_shard, key_hash)
            .map(|index| &self.previous_nodes[index])
    }

    fn previous_owner_index(&self, primary_shard: usize, key_hash: u64) -> Option<usize> {
        if self.previous_nodes.is_empty() {
            return None;
        }
        let slot = self.slot_for_hash_on_shard(primary_shard, key_hash) as usize;
        let owner_index = self.previous_slot_owners[slot];
        let owner = &self.previous_nodes[owner_index];
        let current = &self.nodes[self.slot_owners[slot]];
        (owner.id() != current.id()).then_some(owner_index)
    }

    fn storage_key_for<'a>(
        &'a self,
        owner_index: usize,
        primary_shard: usize,
        key_hash: u64,
        key: &'a [u8],
    ) -> Cow<'a, [u8]> {
        let Some(cluster_id) = &self.cluster_id else {
            return Cow::Borrowed(key);
        };
        let slot = self.slot_for_hash_on_shard(primary_shard, key_hash);
        Cow::Owned(encode_overflow_storage_key(
            cluster_id,
            slot,
            self.nodes[owner_index].remote_shard(),
            key,
        ))
    }

    fn previous_storage_key_for<'a>(
        &'a self,
        owner_index: usize,
        primary_shard: usize,
        key_hash: u64,
        key: &'a [u8],
    ) -> Cow<'a, [u8]> {
        let Some(cluster_id) = &self.cluster_id else {
            return Cow::Borrowed(key);
        };
        let slot = self.slot_for_hash_on_shard(primary_shard, key_hash);
        Cow::Owned(encode_overflow_storage_key(
            cluster_id,
            slot,
            self.previous_nodes[owner_index].remote_shard(),
            key,
        ))
    }

    pub fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
        let expire_at_ms = ttl_ms.map(|ttl| now_millis().saturating_add(ttl));
        let key_hash = xxh3_64(key);
        self.put_until_on_shard(
            self.primary_shard_for_hash(key_hash),
            key_hash,
            key,
            value,
            None,
            expire_at_ms,
        )
    }

    fn put_until_on_shard(
        &self,
        primary_shard: usize,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
        governance: Option<&[u8]>,
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        let metrics = self.metrics.shard(primary_shard);
        metrics.worker.puts.fetch_add(1, Ordering::Relaxed);
        let owner_index = self.owner_index_for_hash_on_shard(primary_shard, key_hash);
        let storage_key = self.storage_key_for(owner_index, primary_shard, key_hash, key);
        let result = self.nodes[owner_index]
            .put_until_with_governance_on_shard(
                primary_shard,
                &storage_key,
                value,
                governance,
                expire_at_ms,
            )
            .and_then(|()| {
                let Some(previous_index) = self.previous_owner_index(primary_shard, key_hash)
                else {
                    return Ok(());
                };
                let previous_key =
                    self.previous_storage_key_for(previous_index, primary_shard, key_hash, key);
                self.previous_nodes[previous_index]
                    .delete_on_shard(primary_shard, &previous_key)
                    .inspect_err(|_| {
                        self.metrics
                            .handoff_failures
                            .fetch_add(1, Ordering::Relaxed);
                    })
            });
        if result.is_err() {
            metrics.worker.put_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn handoff_value_on_shard(
        &self,
        primary_shard: usize,
        key_hash: u64,
        key: &[u8],
        value: &KvOverflowValue,
        expire_at_ms: Option<u64>,
    ) -> Result<()> {
        let owner_index = self.owner_index_for_hash_on_shard(primary_shard, key_hash);
        let storage_key = self.storage_key_for(owner_index, primary_shard, key_hash, key);
        let current = &self.nodes[owner_index];
        current.put_until_with_governance_on_shard(
            primary_shard,
            &storage_key,
            &value.value,
            value.governance.as_deref(),
            expire_at_ms,
        )?;
        let verified = current
            .get_on_shard(primary_shard, &storage_key)?
            .is_some_and(|stored| {
                stored.value == value.value && stored.governance == value.governance
            });
        if !verified {
            self.metrics
                .handoff_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(ShardCacheError::Protocol(
                "kv overflow handoff could not verify the new owner".into(),
            ));
        }
        if let Some(previous_index) = self.previous_owner_index(primary_shard, key_hash) {
            let previous_key =
                self.previous_storage_key_for(previous_index, primary_shard, key_hash, key);
            self.previous_nodes[previous_index]
                .delete_on_shard(primary_shard, &previous_key)
                .inspect_err(|_| {
                    self.metrics
                        .handoff_failures
                        .fetch_add(1, Ordering::Relaxed);
                })?;
        }
        Ok(())
    }

    fn put_batch_until(
        &self,
        primary_shard: usize,
        owner_hint: Option<usize>,
        key_hashes: &[u64],
        requests: &[KvOverflowPutRequest<'_>],
        succeeded: &mut Vec<bool>,
        by_owner: &mut Vec<(usize, usize)>,
    ) {
        debug_assert_eq!(key_hashes.len(), requests.len());
        let metrics = self.metrics.shard(primary_shard);
        metrics
            .worker
            .puts
            .fetch_add(requests.len() as u64, Ordering::Relaxed);
        succeeded.clear();
        succeeded.resize(requests.len(), false);

        by_owner.clear();
        for (index, key_hash) in key_hashes.iter().copied().enumerate() {
            let owner = owner_hint
                .unwrap_or_else(|| self.owner_index_for_hash_on_shard(primary_shard, key_hash));
            by_owner.push((owner, index));
        }
        if owner_hint.is_none() {
            by_owner.sort_unstable_by_key(|(owner, _)| *owner);
        }
        let mut owner_outcomes = Vec::new();
        let mut begin = 0;
        while begin < by_owner.len() {
            let owner_index = by_owner[begin].0;
            let end = by_owner[begin..]
                .iter()
                .position(|(owner, _)| *owner != owner_index)
                .map_or(by_owner.len(), |offset| begin + offset);
            let indexes = &by_owner[begin..end];
            let storage_keys = indexes
                .iter()
                .map(|(_, index)| {
                    self.storage_key_for(
                        owner_index,
                        primary_shard,
                        key_hashes[*index],
                        requests[*index].key,
                    )
                })
                .collect::<Vec<_>>();
            let owner_requests = indexes
                .iter()
                .zip(&storage_keys)
                .map(|((_, index), key)| KvOverflowPutRequest {
                    key,
                    value: requests[*index].value,
                    expire_at_ms: requests[*index].expire_at_ms,
                    governance: requests[*index].governance,
                })
                .collect::<Vec<_>>();
            let pipeline_bytes = owner_requests.iter().fold(0usize, |total, request| {
                total
                    .saturating_add(request.key.len())
                    .saturating_add(request.value.len())
            });
            let pipeline_started = Instant::now();
            self.nodes[owner_index].put_batch_outcomes_until_on_shard(
                primary_shard,
                &owner_requests,
                &mut owner_outcomes,
            );
            metrics
                .worker
                .pipeline_batches
                .fetch_add(1, Ordering::Relaxed);
            metrics
                .worker
                .pipeline_items
                .fetch_add(owner_requests.len() as u64, Ordering::Relaxed);
            metrics
                .worker
                .pipeline_bytes
                .fetch_add(pipeline_bytes as u64, Ordering::Relaxed);
            metrics.worker.pipeline_latency_ns.fetch_add(
                u64::try_from(pipeline_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            normalize_outcomes(&mut owner_outcomes, owner_requests.len());
            for ((_, index), outcome) in indexes.iter().zip(owner_outcomes.drain(..)) {
                if !outcome {
                    continue;
                }
                let request = requests[*index];
                succeeded[*index] = self
                    .previous_owner_index(primary_shard, key_hashes[*index])
                    .is_none_or(|previous_index| {
                        let previous_key = self.previous_storage_key_for(
                            previous_index,
                            primary_shard,
                            key_hashes[*index],
                            request.key,
                        );
                        self.previous_nodes[previous_index]
                            .delete_on_shard(primary_shard, &previous_key)
                            .is_ok()
                            || {
                                self.metrics
                                    .handoff_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                false
                            }
                    });
            }
            begin = end;
        }

        self.record_batch_failures(primary_shard, succeeded);
    }

    fn record_batch_failures(&self, primary_shard: usize, succeeded: &[bool]) {
        let failures = succeeded.iter().filter(|result| !**result).count();
        self.metrics
            .shard(primary_shard)
            .worker
            .put_failures
            .fetch_add(failures as u64, Ordering::Relaxed);
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        let key_hash = xxh3_64(key);
        self.get_on_shard(self.primary_shard_for_hash(key_hash), key_hash, key)
            .map(|value| value.filter(|value| value.governance.is_none()))
    }

    /// Reads a value only when `authorize` accepts its borrowed metadata.
    pub fn get_with_governance_filter<F>(
        &self,
        key: &[u8],
        authorize: F,
    ) -> Result<Option<KvOverflowValue>>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        let key_hash = xxh3_64(key);
        let value = self.get_on_shard(self.primary_shard_for_hash(key_hash), key_hash, key)?;
        Ok(value.filter(|value| authorize(value.governance.as_deref())))
    }

    fn get_on_shard(
        &self,
        primary_shard: usize,
        key_hash: u64,
        key: &[u8],
    ) -> Result<Option<KvOverflowValue>> {
        self.metrics.gets.fetch_add(1, Ordering::Relaxed);
        let owner_index = self.owner_index_for_hash_on_shard(primary_shard, key_hash);
        let storage_key = self.storage_key_for(owner_index, primary_shard, key_hash, key);
        let current = &self.nodes[owner_index];
        let result = match current.get_on_shard(primary_shard, &storage_key) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => self.get_from_previous(primary_shard, key_hash, key),
            Err(error) => Err(error),
        };
        match &result {
            Ok(Some(_)) => {
                self.metrics.get_hits.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.metrics.get_failures.fetch_add(1, Ordering::Relaxed);
            }
            Ok(None) => {}
        }
        result
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let key_hash = xxh3_64(key);
        self.delete_on_shard(self.primary_shard_for_hash(key_hash), key_hash, key)
    }

    fn delete_on_shard(&self, primary_shard: usize, key_hash: u64, key: &[u8]) -> Result<()> {
        self.metrics.deletes.fetch_add(1, Ordering::Relaxed);
        let owner_index = self.owner_index_for_hash_on_shard(primary_shard, key_hash);
        let storage_key = self.storage_key_for(owner_index, primary_shard, key_hash, key);
        let current_result = self.nodes[owner_index].delete_on_shard(primary_shard, &storage_key);
        let previous_result =
            self.previous_owner_index(primary_shard, key_hash)
                .map_or(Ok(()), |previous_index| {
                    let previous_key =
                        self.previous_storage_key_for(previous_index, primary_shard, key_hash, key);
                    self.previous_nodes[previous_index]
                        .delete_on_shard(primary_shard, &previous_key)
                        .inspect_err(|_| {
                            self.metrics
                                .handoff_failures
                                .fetch_add(1, Ordering::Relaxed);
                        })
                });
        let result = current_result.and(previous_result);
        if result.is_err() {
            self.metrics.delete_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn get_from_previous(
        &self,
        primary_shard: usize,
        key_hash: u64,
        key: &[u8],
    ) -> Result<Option<KvOverflowValue>> {
        let Some(previous) = self.previous_owner(primary_shard, key_hash) else {
            return Ok(None);
        };
        let previous_index = self
            .previous_owner_index(primary_shard, key_hash)
            .expect("previous owner checked");
        let previous_key =
            self.previous_storage_key_for(previous_index, primary_shard, key_hash, key);
        self.metrics.handoff_reads.fetch_add(1, Ordering::Relaxed);
        let result = previous
            .get_on_shard(primary_shard, &previous_key)
            .inspect_err(|_| {
                self.metrics
                    .handoff_failures
                    .fetch_add(1, Ordering::Relaxed);
            });
        if matches!(result, Ok(Some(_))) {
            self.metrics.handoff_hits.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

fn shard_owned_slot_table(
    nodes: &[Arc<dyn KvOverflowNode>],
    slot_count: u32,
    primary_shard_count: usize,
    strict_targets: bool,
) -> Result<Box<[usize]>> {
    let mut replica_ranges = Vec::<std::ops::Range<usize>>::new();
    let mut start = 0;
    while start < nodes.len() {
        let replica_id = nodes[start].replica_id();
        let mut end = start + 1;
        while end < nodes.len() && nodes[end].replica_id() == replica_id {
            end += 1;
        }
        replica_ranges.push(start..end);
        start = end;
    }

    if strict_targets && nodes.len() < primary_shard_count {
        return Err(ShardCacheError::Config(format!(
            "kv overflow has {} remote shard targets for {primary_shard_count} primary shards",
            nodes.len()
        )));
    }

    let mut targets_by_primary = vec![Vec::<usize>::new(); primary_shard_count];
    if replica_ranges.len() >= primary_shard_count {
        for (primary_shard, targets) in targets_by_primary.iter_mut().enumerate() {
            let begin = primary_shard * replica_ranges.len() / primary_shard_count;
            let end = (primary_shard + 1) * replica_ranges.len() / primary_shard_count;
            for range in &replica_ranges[begin..end] {
                targets.extend(range.clone());
            }
        }
    } else if nodes.len() >= primary_shard_count {
        for (primary_shard, targets) in targets_by_primary.iter_mut().enumerate() {
            let begin = primary_shard * nodes.len() / primary_shard_count;
            let end = (primary_shard + 1) * nodes.len() / primary_shard_count;
            targets.extend(begin..end);
        }
    } else {
        for (primary_shard, targets) in targets_by_primary.iter_mut().enumerate() {
            targets.push(primary_shard % nodes.len());
        }
    }

    let slots_per_primary = slot_count as usize / primary_shard_count;
    let mut owners = vec![0; slot_count as usize];
    for (primary_shard, targets) in targets_by_primary.iter().enumerate() {
        if targets.is_empty() || targets.len() > slots_per_primary {
            return Err(ShardCacheError::Config(format!(
                "primary shard {primary_shard} has {} targets for {slots_per_primary} logical slots",
                targets.len()
            )));
        }
        let slot_start = primary_shard * slots_per_primary;
        for offset in 0..slots_per_primary {
            owners[slot_start + offset] = targets[offset * targets.len() / slots_per_primary];
        }
    }
    Ok(owners.into_boxed_slice())
}

fn encode_overflow_storage_key(
    cluster_id: &[u8],
    slot: u32,
    remote_shard: usize,
    key: &[u8],
) -> Vec<u8> {
    let cluster_len = u16::try_from(cluster_id.len()).expect("cluster ID validated");
    let remote_shard = u32::try_from(remote_shard).expect("remote shard count validated");
    let mut encoded = Vec::with_capacity(18 + cluster_id.len() + key.len());
    encoded.extend_from_slice(KV_OVERFLOW_STORAGE_KEY_MAGIC);
    encoded.extend_from_slice(&remote_shard.to_le_bytes());
    encoded.extend_from_slice(&slot.to_le_bytes());
    encoded.extend_from_slice(&cluster_len.to_le_bytes());
    encoded.extend_from_slice(cluster_id);
    encoded.extend_from_slice(key);
    encoded
}

fn normalize_outcomes(outcomes: &mut Vec<bool>, expected: usize) {
    outcomes.truncate(expected);
    outcomes.resize(expected, false);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RemoteKeyMeta {
    primary_shard: usize,
    expire_at_ms: Option<u64>,
    generation: u64,
    handoff_complete: bool,
    protected: bool,
}

#[derive(Debug, Clone)]
enum PendingMutation {
    Put {
        value: Option<SharedBytes>,
        governance: Option<SharedBytes>,
        expire_at_ms: Option<u64>,
    },
    Delete {
        retry_on_failure: bool,
    },
}

#[derive(Debug, Clone)]
struct PendingKeyMeta {
    primary_shard: usize,
    generation: u64,
    queued: bool,
    tracks_key: bool,
    mutation: PendingMutation,
}

#[derive(Debug)]
struct KvOverflowMetadataBudget {
    used: AtomicUsize,
    limit: usize,
}

impl KvOverflowMetadataBudget {
    fn new(limit: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit,
        }
    }

    fn charge(key_len: usize) -> Option<usize> {
        key_len.checked_add(KV_OVERFLOW_METADATA_ENTRY_OVERHEAD)
    }

    fn try_reserve(&self, key_len: usize) -> bool {
        let Some(charge) = Self::charge(key_len) else {
            return false;
        };
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(charge).filter(|next| *next <= self.limit)
            })
            .is_ok()
    }

    fn release(&self, key_len: usize) {
        let Some(charge) = Self::charge(key_len) else {
            return;
        };
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(charge))
            });
    }

    fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

struct MetadataMap<V> {
    entries: HashBrownMap<MetadataKey, V, FastHashBuilder>,
}

#[derive(Eq, PartialEq)]
struct MetadataKey(SharedBytes);

impl Hash for MetadataKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(xxh3_64(&self.0));
    }
}

impl<V> Default for MetadataMap<V> {
    fn default() -> Self {
        Self {
            entries: HashBrownMap::with_hasher(local_table_hasher()),
        }
    }
}

impl<V> MetadataMap<V> {
    #[inline]
    fn get(&self, key: &[u8]) -> Option<&V> {
        self.get_hashed(xxh3_64(key), key)
    }

    #[inline]
    fn get_hashed(&self, hash: u64, key: &[u8]) -> Option<&V> {
        self.entries
            .raw_entry()
            .from_hash(local_table_hash(hash), |entry_key| {
                entry_key.0.as_ref() == key
            })
            .map(|(_, value)| value)
    }

    #[inline]
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        self.get_mut_hashed(xxh3_64(key), key)
    }

    #[inline]
    fn get_mut_hashed(&mut self, hash: u64, key: &[u8]) -> Option<&mut V> {
        match self
            .entries
            .raw_entry_mut()
            .from_hash(local_table_hash(hash), |entry_key| {
                entry_key.0.as_ref() == key
            }) {
            hashbrown::hash_map::RawEntryMut::Occupied(occupied) => Some(occupied.into_mut()),
            hashbrown::hash_map::RawEntryMut::Vacant(_) => None,
        }
    }

    #[inline]
    fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    #[inline]
    fn insert(&mut self, key: SharedBytes, value: V) -> Option<V> {
        let hash = xxh3_64(&key);
        self.insert_hashed(hash, key, value)
    }

    #[inline]
    fn insert_hashed(&mut self, hash: u64, key: SharedBytes, value: V) -> Option<V> {
        match self
            .entries
            .raw_entry_mut()
            .from_hash(local_table_hash(hash), |entry_key| entry_key.0 == key)
        {
            hashbrown::hash_map::RawEntryMut::Occupied(mut occupied) => {
                Some(std::mem::replace(occupied.get_mut(), value))
            }
            hashbrown::hash_map::RawEntryMut::Vacant(vacant) => {
                vacant.insert_hashed_nocheck(local_table_hash(hash), MetadataKey(key), value);
                None
            }
        }
    }

    #[inline]
    fn remove_hashed(&mut self, hash: u64, key: &[u8]) -> Option<V> {
        match self
            .entries
            .raw_entry_mut()
            .from_hash(local_table_hash(hash), |entry_key| {
                entry_key.0.as_ref() == key
            }) {
            hashbrown::hash_map::RawEntryMut::Occupied(occupied) => Some(occupied.remove_entry().1),
            hashbrown::hash_map::RawEntryMut::Vacant(_) => None,
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&SharedBytes, &V)> {
        self.entries.iter().map(|(key, value)| (&key.0, value))
    }

    fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.values()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

type RemoteKeyShards = Arc<[RwLock<MetadataMap<RemoteKeyMeta>>]>;
type PendingKeyShards = Arc<[RwLock<MetadataMap<PendingKeyMeta>>]>;

enum KvOverflowJob {
    Put {
        primary_shard: usize,
        key: SharedBytes,
        key_hash: u64,
        value: SharedBytes,
        governance: Option<SharedBytes>,
        expire_at_ms: Option<u64>,
        generation: u64,
    },
    Delete {
        primary_shard: usize,
        key: SharedBytes,
        key_hash: u64,
        generation: u64,
        completion: Option<Sender<Result<()>>>,
    },
    Barrier(Sender<Result<()>>),
    Shutdown,
}

struct PendingPutJob {
    primary_shard: usize,
    key: SharedBytes,
    key_hash: u64,
    value: SharedBytes,
    governance: Option<SharedBytes>,
    expire_at_ms: Option<u64>,
    generation: u64,
}

enum KvOverflowCompletion {
    Put {
        primary_shard: usize,
        key_hash: u64,
        retry_value: Option<SharedBytes>,
        retry_governance: Option<SharedBytes>,
        protected: bool,
        expire_at_ms: Option<u64>,
        generation: u64,
        succeeded: bool,
    },
    Delete {
        key_hash: u64,
        generation: u64,
        succeeded: bool,
    },
}

struct DirectPutJob {
    put: PendingPutJob,
    storage_key: Vec<u8>,
}

#[derive(Default)]
struct DirectPipelineBuffers {
    request: Vec<u8>,
    responses: Vec<DirectResponseKind>,
    compressed: Vec<u8>,
}

enum DirectMutationJob {
    Put(DirectPutJob),
    Delete {
        key: SharedBytes,
        key_hash: u64,
        storage_key: Vec<u8>,
        generation: u64,
        completion: Option<Sender<Result<()>>>,
    },
    Barrier(oneshot::Sender<Result<()>>),
    Shutdown,
}

#[derive(Clone, Copy)]
enum DirectResponseKind {
    Ok,
    Integer,
}

struct AsyncScnpMutationConnection {
    target: ScnpDirectTarget,
    primary_shard: usize,
    lane: usize,
    stream: Option<AsyncScnpStream>,
    circuit: Arc<CircuitBreaker>,
}

trait AsyncScnpIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send {}

impl<T> AsyncScnpIo for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send {}

type AsyncScnpStream = Pin<Box<dyn AsyncScnpIo>>;

async fn wrap_async_scnp_stream(
    stream: AsyncTcpStream,
    _target: &ScnpDirectTarget,
) -> Result<AsyncScnpStream> {
    #[cfg(feature = "scnp-tls")]
    if let Some(tls) = _target.tls.as_deref() {
        let connector = tokio_rustls::TlsConnector::from(tls.rustls_config());
        let stream = tokio::time::timeout(
            _target.operation_timeout,
            connector.connect(tls.server_name(), stream),
        )
        .await
        .map_err(|_| {
            ShardCacheError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "SCNP TLS handshake deadline exceeded",
            ))
        })?
        .map_err(|error| {
            ShardCacheError::Protocol(format!("SCNP TLS handshake failed: {error}"))
        })?;
        return Ok(Box::pin(stream));
    }
    Ok(Box::pin(stream))
}

impl AsyncScnpMutationConnection {
    fn new(target: ScnpDirectTarget, primary_shard: usize, lane: usize) -> Self {
        let circuit = Arc::clone(&target.circuit);
        Self {
            target,
            primary_shard,
            lane,
            stream: None,
            circuit,
        }
    }

    async fn execute(&mut self, request: &[u8], responses: &[DirectResponseKind]) -> Result<()> {
        if !self.circuit.allow_request() {
            return Err(ShardCacheError::Protocol(
                "SCNP target circuit breaker is open".into(),
            ));
        }
        let attempts = self
            .target
            .max_retries
            .saturating_add(1)
            .max(self.target.addresses.len());
        let first_path = self
            .primary_shard
            .wrapping_add(self.target.remote_shard)
            .wrapping_add(self.lane)
            % self.target.addresses.len();
        let mut last_error = None;
        for attempt in 0..attempts {
            if attempt > 0 && !self.circuit.allow_request() {
                break;
            }
            if self.stream.is_none() {
                let path = first_path.wrapping_add(attempt) % self.target.addresses.len();
                let address = self.target.addresses[path];
                match tokio::time::timeout(
                    self.target.connect_timeout,
                    AsyncTcpStream::connect(address),
                )
                .await
                {
                    Ok(Ok(stream)) => {
                        stream.set_nodelay(true)?;
                        let mut stream = match wrap_async_scnp_stream(stream, &self.target).await {
                            Ok(stream) => stream,
                            Err(error) => {
                                last_error = Some(error);
                                self.circuit.record_failure();
                                continue;
                            }
                        };
                        if let Some(token) =
                            self.target.auth_token.as_ref().map(|token| token.current())
                        {
                            let token_len = u16::try_from(token.len()).map_err(|_| {
                                ShardCacheError::Config(
                                    "SCNP authentication token exceeds 65535 bytes".into(),
                                )
                            })?;
                            let authentication = async {
                                stream.write_all(b"SCAUTH01").await?;
                                stream.write_all(&token_len.to_le_bytes()).await?;
                                stream.write_all(&token).await?;
                                let mut response = [0u8; 1];
                                stream.read_exact(&mut response).await?;
                                if response != [1] {
                                    return Err(ShardCacheError::Protocol(
                                        "SCNP authentication rejected".into(),
                                    ));
                                }
                                Ok(())
                            };
                            match tokio::time::timeout(
                                self.target.operation_timeout,
                                authentication,
                            )
                            .await
                            {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => {
                                    last_error = Some(error);
                                    self.circuit.record_failure();
                                    continue;
                                }
                                Err(_) => {
                                    last_error = Some(ShardCacheError::Io(std::io::Error::new(
                                        std::io::ErrorKind::TimedOut,
                                        "SCNP authentication deadline exceeded",
                                    )));
                                    self.circuit.record_failure();
                                    continue;
                                }
                            }
                        }
                        self.stream = Some(stream);
                    }
                    Ok(Err(error)) => last_error = Some(ShardCacheError::Io(error)),
                    Err(_) => {
                        last_error = Some(ShardCacheError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!("SCNP connect to {address} timed out"),
                        )));
                    }
                }
            }
            if let Some(stream) = self.stream.as_mut() {
                let operation = async {
                    stream.write_all(request).await?;
                    for response in responses {
                        read_direct_response(stream, *response).await?;
                    }
                    Result::<()>::Ok(())
                };
                match tokio::time::timeout(self.target.operation_timeout, operation).await {
                    Ok(Ok(())) => {
                        self.circuit.record_success();
                        return Ok(());
                    }
                    Ok(Err(error)) => last_error = Some(error),
                    Err(_) => {
                        last_error = Some(ShardCacheError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "SCNP mutation deadline exceeded",
                        )));
                    }
                }
                self.stream = None;
            }
            self.circuit.record_failure();
            if !self.circuit.allow_request() {
                break;
            }
            if attempt + 1 < attempts {
                tokio::time::sleep(self.target.retry_backoff).await;
            }
        }
        Err(last_error.unwrap_or_else(|| {
            ShardCacheError::Protocol("SCNP mutation failed without an attempt".into())
        }))
    }
}

async fn read_direct_response(
    stream: &mut (impl tokio::io::AsyncRead + Unpin),
    expected: DirectResponseKind,
) -> Result<()> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;
    if header[0] != 0xFB || header[1] != 2 {
        return Err(ShardCacheError::Protocol(format!(
            "invalid SCNP response header: magic=0x{:02x}, version={}",
            header[0], header[1]
        )));
    }
    let body_len = u32::from_le_bytes(header[4..8].try_into().expect("fixed response length"));
    if body_len > 64 * 1024 {
        return Err(ShardCacheError::Protocol(format!(
            "SCNP mutation response exceeds 64 KiB: {body_len}"
        )));
    }
    let valid = match expected {
        DirectResponseKind::Ok => header[2] == 0 && body_len == 0,
        DirectResponseKind::Integer => header[2] == 3 && body_len == 8,
    };
    if !valid {
        let mut body = vec![0; body_len as usize];
        stream.read_exact(&mut body).await?;
        return Err(ShardCacheError::Protocol(format!(
            "unexpected SCNP mutation response: status={}, body_len={body_len}",
            header[2]
        )));
    }
    if body_len != 0 {
        let mut body = [0u8; 8];
        stream.read_exact(&mut body).await?;
    }
    Ok(())
}

fn write_direct_header(out: &mut Vec<u8>, opcode: u8, body_len: usize) -> Result<()> {
    let body_len = u32::try_from(body_len)
        .map_err(|_| ShardCacheError::Protocol("SCNP mutation body exceeds u32".into()))?;
    out.extend_from_slice(&[0xFA, 2, opcode, 0x07]);
    out.extend_from_slice(&body_len.to_le_bytes());
    Ok(())
}

fn write_direct_route(out: &mut Vec<u8>, key: &[u8], remote_shard: usize) -> Result<()> {
    let key_hash = xxh3_64(key);
    let remote_shard = u32::try_from(remote_shard)
        .map_err(|_| ShardCacheError::Protocol("SCNP remote shard exceeds u32".into()))?;
    out.extend_from_slice(&key_hash.to_le_bytes());
    out.extend_from_slice(&remote_shard.to_le_bytes());
    out.extend_from_slice(&(key_hash >> 56).to_le_bytes());
    Ok(())
}

fn write_direct_set_prefix(
    out: &mut Vec<u8>,
    key: &[u8],
    value_len: usize,
    remote_shard: usize,
) -> Result<()> {
    let body_len = 28usize
        .checked_add(key.len())
        .and_then(|size| size.checked_add(value_len))
        .ok_or_else(|| ShardCacheError::Protocol("SCNP SET body length overflow".into()))?;
    let key_len = u32::try_from(key.len())
        .map_err(|_| ShardCacheError::Protocol("SCNP SET key exceeds u32".into()))?;
    let value_len = u32::try_from(value_len)
        .map_err(|_| ShardCacheError::Protocol("SCNP SET value exceeds u32".into()))?;
    write_direct_header(out, 2, body_len)?;
    write_direct_route(out, key, remote_shard)?;
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(&value_len.to_le_bytes());
    out.extend_from_slice(key);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_direct_overflow_set(
    out: &mut Vec<u8>,
    key: &[u8],
    value: &[u8],
    governance: Option<&[u8]>,
    expire_at_ms: Option<u64>,
    remote_shard: usize,
    compression: KvOverflowCompression,
    compression_min_value_bytes: usize,
    compression_min_savings_percent: u8,
    compressed: &mut Vec<u8>,
) -> Result<(usize, bool)> {
    let expire_at_ms = expire_at_ms.unwrap_or(0);
    let governance_len = governance.map_or(0, <[u8]>::len);
    let governed = governance.is_some();
    let raw_header_len = if governed {
        KV_OVERFLOW_GOVERNED_HEADER_LEN
    } else {
        KV_OVERFLOW_HEADER_LEN
    };
    let compressed_header_len = if governed {
        KV_OVERFLOW_GOVERNED_COMPRESSED_HEADER_LEN
    } else {
        KV_OVERFLOW_COMPRESSED_HEADER_LEN
    };
    let use_compressed = if compression == KvOverflowCompression::Lz4
        && value.len() >= compression_min_value_bytes
    {
        compressed.clear();
        compressed.resize(lz4_flex::block::get_maximum_output_size(value.len()), 0);
        let compressed_len = lz4_flex::block::compress_into(value, compressed)
            .expect("maximum LZ4 output buffer is sufficient");
        compressed.truncate(compressed_len);
        let raw_size = raw_header_len
            .saturating_add(value.len())
            .saturating_add(governance_len);
        let compressed_size = compressed_header_len
            .saturating_add(compressed.len())
            .saturating_add(governance_len);
        let savings = raw_size.saturating_sub(compressed_size);
        compressed_size < raw_size
            && savings.saturating_mul(100)
                >= raw_size.saturating_mul(compression_min_savings_percent as usize)
    } else {
        false
    };
    let envelope_len = if use_compressed {
        compressed_header_len
            .saturating_add(compressed.len())
            .saturating_add(governance_len)
    } else {
        raw_header_len
            .saturating_add(value.len())
            .saturating_add(governance_len)
    };
    write_direct_set_prefix(out, key, envelope_len, remote_shard)?;
    out.extend_from_slice(if use_compressed && governed {
        KV_OVERFLOW_GOVERNED_COMPRESSED_MAGIC
    } else if use_compressed {
        KV_OVERFLOW_COMPRESSED_MAGIC
    } else if governed {
        KV_OVERFLOW_GOVERNED_MAGIC
    } else {
        KV_OVERFLOW_MAGIC
    });
    out.extend_from_slice(&expire_at_ms.to_le_bytes());
    out.extend_from_slice(&(value.len() as u64).to_le_bytes());
    out.extend_from_slice(&overflow_checksum(value, governance).to_le_bytes());
    if governed {
        out.extend_from_slice(&(governance_len as u32).to_le_bytes());
    }
    if use_compressed {
        out.push(KV_OVERFLOW_CODEC_LZ4);
        out.extend_from_slice(compressed);
    } else {
        out.extend_from_slice(value);
    }
    if let Some(governance) = governance {
        out.extend_from_slice(governance);
    }
    Ok((envelope_len, use_compressed))
}

fn write_direct_delete(out: &mut Vec<u8>, key: &[u8], remote_shard: usize) -> Result<()> {
    let body_len = 24usize
        .checked_add(key.len())
        .ok_or_else(|| ShardCacheError::Protocol("SCNP DEL body length overflow".into()))?;
    let key_len = u32::try_from(key.len())
        .map_err(|_| ShardCacheError::Protocol("SCNP DEL key exceeds u32".into()))?;
    write_direct_header(out, 5, body_len)?;
    write_direct_route(out, key, remote_shard)?;
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(key);
    Ok(())
}

enum FaultInOutcome {
    Retry,
    Denied,
    Return(Option<Bytes>),
    Loaded(Bytes),
}

struct KvOverflowWorkerPool {
    senders: Box<[KvOverflowJobSender]>,
    completion_receivers: Arc<[Receiver<(SharedBytes, KvOverflowCompletion)>]>,
    shard_in_flight: Box<[Arc<AtomicUsize>]>,
    capacity_per_shard: usize,
    drains_per_shard: usize,
    metrics: Arc<KvOverflowMetrics>,
    joins: Mutex<Vec<JoinHandle<()>>>,
}

#[derive(Clone)]
enum KvOverflowJobSender {
    Blocking(Sender<KvOverflowJob>),
    Async(mpsc::Sender<KvOverflowJob>),
}

impl KvOverflowJobSender {
    fn send(&self, job: KvOverflowJob) -> std::result::Result<(), ()> {
        match self {
            Self::Blocking(sender) => sender.send(job).map_err(|_| ()),
            Self::Async(sender) => sender.blocking_send(job).map_err(|_| ()),
        }
    }

    fn try_send(&self, job: KvOverflowJob) -> std::result::Result<(), ()> {
        match self {
            Self::Blocking(sender) => sender.try_send(job).map_err(|_| ()),
            Self::Async(sender) => sender.try_send(job).map_err(|_| ()),
        }
    }
}

enum KvOverflowJobReceiver {
    Blocking(Receiver<KvOverflowJob>),
    Async(mpsc::Receiver<KvOverflowJob>),
}

enum KvOverflowTryRecvError {
    Empty,
    Disconnected,
}

impl KvOverflowJobReceiver {
    fn blocking_recv(&mut self) -> Option<KvOverflowJob> {
        match self {
            Self::Blocking(receiver) => receiver.recv().ok(),
            Self::Async(receiver) => receiver.blocking_recv(),
        }
    }

    fn try_recv(&mut self) -> std::result::Result<KvOverflowJob, KvOverflowTryRecvError> {
        match self {
            Self::Blocking(receiver) => receiver.try_recv().map_err(|error| match error {
                crossbeam_channel::TryRecvError::Empty => KvOverflowTryRecvError::Empty,
                crossbeam_channel::TryRecvError::Disconnected => {
                    KvOverflowTryRecvError::Disconnected
                }
            }),
            Self::Async(receiver) => receiver.try_recv().map_err(|error| match error {
                mpsc::error::TryRecvError::Empty => KvOverflowTryRecvError::Empty,
                mpsc::error::TryRecvError::Disconnected => KvOverflowTryRecvError::Disconnected,
            }),
        }
    }
}

impl std::fmt::Debug for KvOverflowWorkerPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvOverflowWorkerPool")
            .field("workers", &self.senders.len())
            .field("queue_depth", &self.queue_depth())
            .field("capacity", &self.queue_capacity())
            .field("drains_per_shard", &self.drains_per_shard)
            .finish()
    }
}

struct KvOverflowCleanupTask {
    shutdown: Sender<()>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for KvOverflowCleanupTask {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvOverflowCleanupTask")
            .finish_non_exhaustive()
    }
}

/// Embedded primary with a partitioned, remotely readable overflow tier.
pub struct KvOverflowStore {
    inner: Arc<EmbeddedStore>,
    cluster: Arc<KvOverflowCluster>,
    options: KvOverflowOptions,
    metadata_budget: Arc<KvOverflowMetadataBudget>,
    remote_keys: RemoteKeyShards,
    pending_keys: PendingKeyShards,
    key_gates: Arc<[Mutex<()>]>,
    fault_gates: Mutex<HashMap<SharedBytes, Arc<Mutex<()>>>>,
    maintenance: Arc<[Mutex<()>]>,
    admission: Arc<[RwLock<()>]>,
    flush_gate: Mutex<()>,
    sequence: Arc<[KvOverflowSequence]>,
    workers: KvOverflowWorkerPool,
    cleanup: KvOverflowCleanupTask,
}

impl std::fmt::Debug for KvOverflowStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvOverflowStore")
            .field("inner", &self.inner)
            .field("cluster", &self.cluster)
            .field("options", &self.options)
            .field("remote_keys", &metadata_len(&self.remote_keys))
            .field("pending_keys", &metadata_len(&self.pending_keys))
            .field("workers", &self.workers)
            .finish()
    }
}

impl KvOverflowStore {
    pub fn from_config(inner: EmbeddedStore, config: &KvOverflowConfig) -> Result<Self> {
        if !config.enabled {
            return Err(ShardCacheError::Config(
                "KvOverflowStore requires kv_overflow.enabled = true".into(),
            ));
        }
        let cluster = Arc::new(KvOverflowCluster::from_config_for_primary_shards(
            config,
            inner.shard_count(),
        )?);
        let options = KvOverflowOptions::try_from(config)?;
        Self::new(inner, cluster, options)
    }

    pub fn new(
        inner: EmbeddedStore,
        cluster: Arc<KvOverflowCluster>,
        options: KvOverflowOptions,
    ) -> Result<Self> {
        options.validate()?;
        let cluster = if cluster.primary_shard_count() == inner.shard_count() {
            cluster
        } else {
            Arc::new(cluster.for_primary_shards(inner.shard_count())?)
        };
        inner.configure_memory_policy(None, options.eviction_policy);
        let inner = Arc::new(inner);
        let remote_keys: RemoteKeyShards = (0..KV_OVERFLOW_KEY_GATES)
            .map(|_| RwLock::new(MetadataMap::default()))
            .collect::<Vec<_>>()
            .into();
        let pending_keys: PendingKeyShards = (0..KV_OVERFLOW_KEY_GATES)
            .map(|_| RwLock::new(MetadataMap::default()))
            .collect::<Vec<_>>()
            .into();
        let key_gates: Arc<[Mutex<()>]> = (0..KV_OVERFLOW_KEY_GATES)
            .map(|_| Mutex::new(()))
            .collect::<Vec<_>>()
            .into();
        let metadata_budget = Arc::new(KvOverflowMetadataBudget::new(options.max_metadata_bytes));
        let maintenance: Arc<[Mutex<()>]> = (0..inner.shard_count())
            .map(|_| Mutex::new(()))
            .collect::<Vec<_>>()
            .into();
        let sequence: Arc<[KvOverflowSequence]> = (0..inner.shard_count())
            .map(|_| KvOverflowSequence::default())
            .collect::<Vec<_>>()
            .into();
        let workers = KvOverflowWorkerPool::start(
            options.queue_capacity,
            Arc::clone(&cluster),
            options.clone(),
        )?;
        let admission: Arc<[RwLock<()>]> = (0..workers.worker_count())
            .map(|_| RwLock::new(()))
            .collect::<Vec<_>>()
            .into();
        let cleanup = KvOverflowCleanupTask::start(
            &workers,
            Arc::clone(&remote_keys),
            Arc::clone(&pending_keys),
            Arc::clone(&key_gates),
            Arc::clone(&inner),
            Arc::clone(&cluster),
            Arc::clone(&maintenance),
            options.clone(),
            Arc::clone(&admission),
            Arc::clone(&sequence),
            Arc::clone(&metadata_budget),
            options.cleanup_interval,
        )?;
        let store = Self {
            inner,
            cluster,
            options,
            metadata_budget,
            remote_keys,
            pending_keys,
            key_gates,
            fault_gates: Mutex::new(HashMap::new()),
            maintenance,
            admission,
            flush_gate: Mutex::new(()),
            sequence,
            workers,
            cleanup,
        };
        store.synchronize_resident()?;
        Ok(store)
    }

    pub fn inner(&self) -> &EmbeddedStore {
        self.inner.as_ref()
    }

    pub fn cluster(&self) -> Arc<KvOverflowCluster> {
        Arc::clone(&self.cluster)
    }

    pub fn set<K, V>(&self, key: K, value: V, ttl_ms: Option<u64>) -> Result<()>
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        self.set_with_optional_governance(key.into(), value.into(), ttl_ms, None)
    }

    /// Stores an exact value with opaque governance metadata. Ordinary
    /// [`Self::get`] calls cannot return the protected value.
    pub fn set_with_governance<K, V, G>(
        &self,
        key: K,
        value: V,
        ttl_ms: Option<u64>,
        governance: G,
    ) -> Result<()>
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
        G: Into<Bytes>,
    {
        self.set_with_optional_governance(
            key.into(),
            value.into(),
            ttl_ms,
            Some(SharedBytes::from(governance.into())),
        )
    }

    fn set_with_optional_governance(
        &self,
        key: Bytes,
        value: Bytes,
        ttl_ms: Option<u64>,
        governance: Option<SharedBytes>,
    ) -> Result<()> {
        let value = SharedBytes::from(value);
        if value.len() > self.options.max_value_bytes {
            self.cluster
                .metrics
                .oversized_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(ShardCacheError::Protocol(format!(
                "key-value overflow value is {} bytes, exceeding max_value_bytes {}",
                value.len(),
                self.options.max_value_bytes
            )));
        }
        if governance
            .as_ref()
            .is_some_and(|metadata| metadata.len() > self.options.max_value_bytes)
        {
            return Err(ShardCacheError::Protocol(format!(
                "key-value overflow governance exceeds max_value_bytes {}",
                self.options.max_value_bytes
            )));
        }
        let key = SharedBytes::from(key);
        self.validate_key_len(&key)?;
        let key_hash = xxh3_64(&key);
        let route = self.inner.route_key_prehashed(key_hash, &key);
        let primary_shard = route.shard_id;
        let shard_target = (self.options.max_memory_bytes != usize::MAX)
            .then(|| shard_memory_target(&self.options, self.inner.shard_count(), primary_shard));
        let lane = self.workers.lane_for_hash(primary_shard, key_hash);
        let _admission = self.admission[lane].read();
        self.reserve_worker_slot(primary_shard)?;
        let _key_gate = self.key_gate_for_hash(key_hash);
        let metadata_index = key_gate_index_for_hash(key_hash, self.remote_keys.len());
        let newly_tracked = self.options.max_metadata_bytes != usize::MAX
            && !(self.inner.exists(&key)
                || self
                    .inner
                    .is_value_protected_routed(route, &key, now_millis())
                || self.remote_keys[metadata_index]
                    .read()
                    .get_hashed(key_hash, &key)
                    .is_some()
                || self.pending_keys[metadata_index]
                    .read()
                    .get_hashed(key_hash, &key)
                    .is_some_and(|meta| meta.tracks_key));
        if newly_tracked && !self.metadata_budget.try_reserve(key.len()) {
            self.workers.shard_in_flight[primary_shard].fetch_sub(1, Ordering::Release);
            self.cluster
                .metrics
                .metadata_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(ShardCacheError::Backpressure(
                "kv overflow metadata budget exhausted",
            ));
        }
        let generation = self.sequence[primary_shard]
            .0
            .fetch_add(1, Ordering::Relaxed);
        let now_ms = ttl_ms.map(|_| now_millis());
        let expire_at_ms = ttl_ms.zip(now_ms).map(|(ttl, now)| now.saturating_add(ttl));
        let hard_limit = shard_target.map_or(usize::MAX, |shard_target| {
            shard_target
                .saturating_add(self.options.max_key_bytes)
                .saturating_add(self.options.max_value_bytes)
                .saturating_add(governance.as_ref().map_or(0, SharedBytes::len))
        });
        let inserted = self.inner.try_set_value_bytes_routed_overflow(
            route,
            &key,
            value.clone(),
            governance.clone(),
            expire_at_ms,
            generation,
            hard_limit,
        );
        if !inserted {
            if newly_tracked {
                self.metadata_budget.release(key.len());
            }
            self.workers.shard_in_flight[primary_shard].fetch_sub(1, Ordering::Release);
            self.cluster
                .metrics
                .resident_backpressure
                .fetch_add(1, Ordering::Relaxed);
            return Err(ShardCacheError::Backpressure(
                "kv overflow resident memory hard limit is exceeded",
            ));
        }
        self.remote_keys[metadata_index]
            .write()
            .remove_hashed(key_hash, key.as_ref());
        if self.workers.enqueue_reserved_to_lane(
            lane,
            KvOverflowJob::Put {
                primary_shard,
                key: key.clone(),
                key_hash,
                value,
                governance,
                expire_at_ms,
                generation,
            },
        ) {
            drop(_key_gate);
            self.cluster
                .metrics
                .shard(primary_shard)
                .primary
                .enqueued_puts
                .fetch_add(1, Ordering::Relaxed);
            self.apply_completions_under_pressure(primary_shard);
            Ok(())
        } else {
            Err(ShardCacheError::ChannelClosed("kv overflow workers"))
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.validate_key_len(key)?;
        if let Some(value) = self.inner.get(key) {
            return Ok(Some(value));
        }
        if !self.options.fetch_on_miss {
            return Ok(None);
        }
        let key_hash = xxh3_64(key);
        let metadata_index = key_gate_index_for_hash(key_hash, self.remote_keys.len());
        loop {
            let Some(expected) = self.remote_keys[metadata_index]
                .read()
                .get_hashed(key_hash, key)
                .copied()
            else {
                return Ok(None);
            };
            if expected.protected {
                return Ok(None);
            }
            let fault_gate = {
                let mut gates = self.fault_gates.lock();
                Arc::clone(
                    gates
                        .entry(SharedBytes::copy_from_slice(key))
                        .or_insert_with(|| Arc::new(Mutex::new(()))),
                )
            };
            let outcome = {
                let _fault_gate = fault_gate.lock();
                self.fault_in_once(key, key_hash, metadata_index, expected)
            };
            self.release_fault_gate(key, &fault_gate);
            match outcome? {
                FaultInOutcome::Retry => continue,
                FaultInOutcome::Denied => return Ok(None),
                FaultInOutcome::Return(value) => return Ok(value),
                FaultInOutcome::Loaded(value) => {
                    self.enforce_memory_target(expected.primary_shard);
                    return Ok(Some(value));
                }
            }
        }
    }

    /// Returns a resident or overflow value only after `authorize` accepts
    /// its borrowed opaque metadata. Rejection is indistinguishable from miss.
    pub fn get_with_governance_filter<F>(&self, key: &[u8], authorize: F) -> Result<Option<Bytes>>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        self.validate_key_len(key)?;
        let key_hash = xxh3_64(key);
        let route = self.inner.route_key_prehashed(key_hash, key);
        let mut authorize = Some(authorize);
        match self
            .inner
            .try_get_value_bytes_routed_with_governance_filter(
                route,
                key,
                now_millis(),
                |metadata| {
                    authorize
                        .take()
                        .expect("authorization filter is called at most once")(
                        metadata
                    )
                },
            )? {
            GovernedRead::Authorized(value) => return Ok(Some(value.to_vec())),
            GovernedRead::Denied => return Ok(None),
            GovernedRead::Missing => {}
        }
        if !self.options.fetch_on_miss {
            return Ok(None);
        }
        let metadata_index = key_gate_index_for_hash(key_hash, self.remote_keys.len());
        loop {
            let Some(expected) = self.remote_keys[metadata_index]
                .read()
                .get_hashed(key_hash, key)
                .copied()
            else {
                return Ok(None);
            };
            let fault_gate = {
                let mut gates = self.fault_gates.lock();
                Arc::clone(
                    gates
                        .entry(SharedBytes::copy_from_slice(key))
                        .or_insert_with(|| Arc::new(Mutex::new(()))),
                )
            };
            let outcome = {
                let _fault_gate = fault_gate.lock();
                self.fault_in_once_with_governance(
                    key,
                    key_hash,
                    metadata_index,
                    expected,
                    &mut authorize,
                )
            };
            self.release_fault_gate(key, &fault_gate);
            match outcome? {
                FaultInOutcome::Retry => continue,
                FaultInOutcome::Denied | FaultInOutcome::Return(None) => return Ok(None),
                FaultInOutcome::Return(value) => return Ok(value),
                FaultInOutcome::Loaded(value) => {
                    self.enforce_memory_target(expected.primary_shard);
                    return Ok(Some(value));
                }
            }
        }
    }

    fn fault_in_once_with_governance<F>(
        &self,
        key: &[u8],
        key_hash: u64,
        metadata_index: usize,
        expected: RemoteKeyMeta,
        authorize: &mut Option<F>,
    ) -> Result<FaultInOutcome>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        let route = self.inner.route_key_prehashed(key_hash, key);
        let check_resident = |authorize: &mut Option<F>| {
            self.inner
                .try_get_value_bytes_routed_with_governance_filter(
                    route,
                    key,
                    now_millis(),
                    |metadata| {
                        authorize
                            .take()
                            .expect("authorization filter is called at most once")(
                            metadata
                        )
                    },
                )
        };
        match check_resident(authorize)? {
            GovernedRead::Authorized(value) => {
                return Ok(FaultInOutcome::Return(Some(value.to_vec())));
            }
            GovernedRead::Denied => return Ok(FaultInOutcome::Denied),
            GovernedRead::Missing => {}
        }
        if self.remote_keys[metadata_index]
            .read()
            .get_hashed(key_hash, key)
            .copied()
            != Some(expected)
        {
            return Ok(FaultInOutcome::Retry);
        }
        let remote = self
            .cluster
            .get_on_shard(expected.primary_shard, key_hash, key)?;
        let _key_gate = self.key_gate_for_hash(key_hash);
        match check_resident(authorize)? {
            GovernedRead::Authorized(value) => {
                return Ok(FaultInOutcome::Return(Some(value.to_vec())));
            }
            GovernedRead::Denied => return Ok(FaultInOutcome::Denied),
            GovernedRead::Missing => {}
        }
        if self.remote_keys[metadata_index]
            .read()
            .get_hashed(key_hash, key)
            .copied()
            != Some(expected)
        {
            return Ok(FaultInOutcome::Retry);
        }
        let Some(remote) = remote else {
            if self.options.forget_remote_misses
                || expected
                    .expire_at_ms
                    .is_some_and(|expiry| expiry <= now_millis())
            {
                let removed = self.remote_keys[metadata_index]
                    .write()
                    .remove_hashed(key_hash, key);
                if removed.is_some() {
                    self.metadata_budget.release(key.len());
                }
            }
            return Ok(FaultInOutcome::Return(None));
        };
        if !authorize
            .take()
            .expect("authorization filter is called at most once")(
            remote.governance.as_deref()
        ) {
            return Ok(FaultInOutcome::Denied);
        }
        let shard_target = shard_memory_target(
            &self.options,
            self.inner.shard_count(),
            expected.primary_shard,
        );
        let hard_limit = shard_target
            .saturating_add(self.options.max_key_bytes)
            .saturating_add(self.options.max_value_bytes)
            .saturating_add(remote.governance.as_ref().map_or(0, SharedBytes::len));
        if !self.inner.try_set_value_bytes_routed_overflow(
            route,
            key,
            remote.value.clone(),
            remote.governance.clone(),
            expected.expire_at_ms,
            expected.generation,
            hard_limit,
        ) {
            self.cluster
                .metrics
                .resident_backpressure
                .fetch_add(1, Ordering::Relaxed);
            return Ok(FaultInOutcome::Return(Some(remote.value.to_vec())));
        }
        self.cluster
            .metrics
            .fault_ins
            .fetch_add(1, Ordering::Relaxed);
        Ok(FaultInOutcome::Loaded(remote.value.to_vec()))
    }

    fn fault_in_once(
        &self,
        key: &[u8],
        key_hash: u64,
        metadata_index: usize,
        expected: RemoteKeyMeta,
    ) -> Result<FaultInOutcome> {
        if let Some(value) = self.inner.get(key) {
            return Ok(FaultInOutcome::Return(Some(value)));
        }
        if self.remote_keys[metadata_index]
            .read()
            .get_hashed(key_hash, key)
            .copied()
            != Some(expected)
        {
            return Ok(FaultInOutcome::Retry);
        }
        let remote = self
            .cluster
            .get_on_shard(expected.primary_shard, key_hash, key)?;
        let _key_gate = self.key_gate_for_hash(key_hash);
        if let Some(value) = self.inner.get(key) {
            return Ok(FaultInOutcome::Return(Some(value)));
        }
        if self.remote_keys[metadata_index]
            .read()
            .get_hashed(key_hash, key)
            .copied()
            != Some(expected)
        {
            return Ok(FaultInOutcome::Retry);
        }
        let Some(remote) = remote else {
            if self.options.forget_remote_misses
                || expected
                    .expire_at_ms
                    .is_some_and(|expiry| expiry <= now_millis())
            {
                let removed = self.remote_keys[metadata_index]
                    .write()
                    .remove_hashed(key_hash, key);
                if removed.is_some() {
                    self.metadata_budget.release(key.len());
                }
            }
            return Ok(FaultInOutcome::Return(None));
        };
        if remote.governance.is_some() {
            if let Some(meta) = self.remote_keys[metadata_index]
                .write()
                .get_mut_hashed(key_hash, key)
                .filter(|meta| **meta == expected)
            {
                meta.protected = true;
            }
            return Ok(FaultInOutcome::Return(None));
        }
        let route = self.inner.route_key_prehashed(key_hash, key);
        let shard_target = shard_memory_target(
            &self.options,
            self.inner.shard_count(),
            expected.primary_shard,
        );
        let hard_limit = shard_target
            .saturating_add(self.options.max_key_bytes)
            .saturating_add(self.options.max_value_bytes);
        if !self.inner.try_set_value_bytes_routed_overflow(
            route,
            key,
            remote.value.clone(),
            remote.governance.clone(),
            expected.expire_at_ms,
            expected.generation,
            hard_limit,
        ) {
            self.cluster
                .metrics
                .resident_backpressure
                .fetch_add(1, Ordering::Relaxed);
            return Ok(FaultInOutcome::Return(Some(remote.value.to_vec())));
        }
        self.cluster
            .metrics
            .fault_ins
            .fetch_add(1, Ordering::Relaxed);
        Ok(FaultInOutcome::Loaded(remote.value.as_ref().to_vec()))
    }

    fn release_fault_gate(&self, key: &[u8], fault_gate: &Arc<Mutex<()>>) {
        let mut gates = self.fault_gates.lock();
        if Arc::strong_count(fault_gate) == 2
            && gates
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, fault_gate))
        {
            gates.remove(key);
        }
    }

    /// Reads the deterministic overflow owner without touching primary memory.
    pub fn get_remote(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        self.validate_key_len(key)?;
        let key_hash = xxh3_64(key);
        let primary_shard = self.inner.route_key_prehashed(key_hash, key).shard_id;
        self.cluster
            .get_on_shard(primary_shard, key_hash, key)
            .map(|value| value.filter(|value| value.governance.is_none()))
    }

    /// Reads the deterministic overflow owner after applying `authorize`,
    /// without promoting the value into primary memory.
    pub fn get_remote_with_governance_filter<F>(
        &self,
        key: &[u8],
        authorize: F,
    ) -> Result<Option<KvOverflowValue>>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        self.validate_key_len(key)?;
        let key_hash = xxh3_64(key);
        let primary_shard = self.inner.route_key_prehashed(key_hash, key).shard_id;
        let value = self.cluster.get_on_shard(primary_shard, key_hash, key)?;
        Ok(value.filter(|value| authorize(value.governance.as_deref())))
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        self.validate_key_len(key)?;
        let key_hash = xxh3_64(key);
        let primary_shard = self.inner.route_key_prehashed(key_hash, key).shard_id;
        let lane = self.workers.lane_for_hash(primary_shard, key_hash);
        let admission = self.admission[lane].read();
        self.reserve_worker_slot(primary_shard)?;
        let generation = self.sequence[primary_shard]
            .0
            .fetch_add(1, Ordering::Relaxed);
        let (completion, result) = bounded(1);
        let _key_gate = self.key_gate_for_hash(key_hash);
        let metadata_index = key_gate_index_for_hash(key_hash, self.remote_keys.len());
        let present = self.inner.exists(key)
            || self.remote_keys[metadata_index]
                .read()
                .get_hashed(key_hash, key)
                .is_some()
            || self.pending_keys[metadata_index]
                .read()
                .get_hashed(key_hash, key)
                .is_some_and(|meta| meta.tracks_key);
        self.pending_keys[metadata_index].write().insert_hashed(
            key_hash,
            SharedBytes::copy_from_slice(key),
            PendingKeyMeta {
                primary_shard,
                generation,
                queued: true,
                tracks_key: present,
                mutation: PendingMutation::Delete {
                    retry_on_failure: false,
                },
            },
        );
        if !self.workers.enqueue_reserved_to_lane(
            lane,
            KvOverflowJob::Delete {
                primary_shard,
                key: SharedBytes::copy_from_slice(key),
                key_hash,
                generation,
                completion: Some(completion),
            },
        ) {
            self.pending_keys[metadata_index]
                .write()
                .remove_hashed(key_hash, key);
            return Err(ShardCacheError::ChannelClosed("kv overflow workers"));
        }
        drop(_key_gate);
        drop(admission);
        let remote_result = result
            .recv()
            .map_err(|_| ShardCacheError::ChannelClosed("kv overflow delete completion"))?;
        apply_delete_completion(
            &self.inner,
            &self.remote_keys,
            &self.pending_keys,
            &self.key_gates,
            &self.metadata_budget,
            SharedBytes::copy_from_slice(key),
            key_hash,
            generation,
            remote_result.is_ok(),
        );
        remote_result?;
        Ok(present)
    }

    fn validate_key_len(&self, key: &[u8]) -> Result<()> {
        if key.len() <= self.options.max_key_bytes {
            return Ok(());
        }
        self.cluster
            .metrics
            .metadata_rejections
            .fetch_add(1, Ordering::Relaxed);
        Err(ShardCacheError::Protocol(format!(
            "key-value overflow key is {} bytes, exceeding max_key_bytes {}",
            key.len(),
            self.options.max_key_bytes
        )))
    }

    /// Waits until all remote mutations admitted before this call are complete.
    pub fn flush_remote(&self) -> Result<()> {
        let _flush = self.flush_gate.lock();
        let (cutoff, completions) = {
            let _admission = self.lock_all_admissions();
            let cutoff = self
                .sequence
                .iter()
                .map(|sequence| sequence.0.load(Ordering::Acquire))
                .collect::<Vec<_>>();
            (cutoff, self.workers.enqueue_barriers())
        };
        let (completions, mut first_error) = completions;
        if let Err(error) = drain_barriers(completions)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        self.drain_all_completions();

        let (retried, retry_error) = self.retry_pending_before(&cutoff);
        if first_error.is_none() {
            first_error = retry_error;
        }
        if retried > 0 {
            let (completions, enqueue_error) = {
                let _admission = self.lock_all_admissions();
                self.workers.enqueue_barriers()
            };
            if first_error.is_none() {
                first_error = enqueue_error;
            }
            if let Err(error) = drain_barriers(completions)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
            self.drain_all_completions();
        }

        if self.pending_before(&cutoff) > 0 {
            return Err(first_error.unwrap_or_else(|| {
                ShardCacheError::Protocol(
                    "one or more key-value overflow mutations remain unreplicated".into(),
                )
            }));
        }
        if first_error.is_none()
            && let Err(error) = self.synchronize_remote_handoff()
        {
            first_error = Some(error);
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Migrates every known cold key whose exact target changed between the
    /// previous and current memberships. Each key is stabilized independently,
    /// so unrelated primary writes continue while the handoff runs.
    pub fn synchronize_remote_handoff(&self) -> Result<usize> {
        if self.cluster.previous_nodes.is_empty() {
            return Ok(0);
        }
        let mut migrated = 0usize;
        loop {
            let by_primary = self.collect_handoff_batch();
            if by_primary.iter().all(Vec::is_empty) {
                break;
            }
            for group in by_primary
                .into_iter()
                .filter(|candidates| !candidates.is_empty())
                .collect::<Vec<_>>()
                .chunks_mut(self.options.handoff_max_concurrency)
            {
                let results = thread::scope(|scope| {
                    group
                        .iter_mut()
                        .map(std::mem::take)
                        .map(|candidates| {
                            scope.spawn(move || self.synchronize_handoff_shard(candidates))
                        })
                        .collect::<Vec<_>>()
                        .into_iter()
                        .map(|handle| {
                            handle.join().map_err(|_| {
                                ShardCacheError::Protocol(
                                    "kv overflow handoff worker panicked".into(),
                                )
                            })?
                        })
                        .collect::<Result<Vec<_>>>()
                })?;
                migrated = migrated.saturating_add(results.into_iter().sum::<usize>());
            }
        }
        self.cluster
            .metrics
            .handoff_migrated
            .fetch_add(migrated as u64, Ordering::Relaxed);
        Ok(migrated)
    }

    fn collect_handoff_batch(&self) -> Vec<Vec<(SharedBytes, RemoteKeyMeta)>> {
        let mut by_primary = (0..self.inner.shard_count())
            .map(|_| Vec::with_capacity(self.options.handoff_batch_items))
            .collect::<Vec<_>>();
        for metadata in self.remote_keys.iter().map(RwLock::read) {
            for (key, meta) in metadata.iter() {
                let candidates = &mut by_primary[meta.primary_shard];
                if candidates.len() >= self.options.handoff_batch_items
                    || meta.handoff_complete
                    || self
                        .cluster
                        .previous_owner_index(meta.primary_shard, xxh3_64(key))
                        .is_none()
                {
                    continue;
                }
                candidates.push((key.clone(), *meta));
            }
        }
        by_primary
    }

    fn synchronize_handoff_shard(
        &self,
        candidates: Vec<(SharedBytes, RemoteKeyMeta)>,
    ) -> Result<usize> {
        let started = Instant::now();
        let mut transferred_bytes = 0u64;
        let mut migrated = 0;
        for (key, meta) in candidates {
            let key_hash = xxh3_64(&key);
            let metadata_index = key_gate_index_for_hash(key_hash, self.remote_keys.len());
            {
                let _key_gate = self.key_gate_for_hash(key_hash);
                let current = self.remote_keys[metadata_index]
                    .read()
                    .get_hashed(key_hash, &key)
                    .copied();
                if current != Some(meta) {
                    continue;
                }
            }
            let value = self
                .cluster
                .get_on_shard(meta.primary_shard, key_hash, &key)?
                .ok_or_else(|| {
                    ShardCacheError::Protocol(
                        "kv overflow handoff source no longer contains a cold key".into(),
                    )
                })?;
            transferred_bytes = transferred_bytes.saturating_add(value.value.len() as u64);
            if self.options.handoff_max_bytes_per_second > 0 {
                let budget = Duration::from_secs_f64(
                    transferred_bytes as f64 / self.options.handoff_max_bytes_per_second as f64,
                );
                if let Some(delay) = budget.checked_sub(started.elapsed()) {
                    thread::sleep(delay);
                }
            }
            let _key_gate = self.key_gate_for_hash(key_hash);
            let current = self.remote_keys[metadata_index]
                .read()
                .get_hashed(key_hash, &key)
                .copied();
            if current != Some(meta) {
                continue;
            }
            self.cluster.handoff_value_on_shard(
                meta.primary_shard,
                key_hash,
                &key,
                &value,
                meta.expire_at_ms,
            )?;
            if let Some(current) = self.remote_keys[metadata_index]
                .write()
                .get_mut_hashed(key_hash, &key)
                .filter(|current| current.generation == meta.generation)
            {
                current.handoff_complete = true;
            }
            migrated += 1;
        }
        Ok(migrated)
    }

    fn retry_pending_before(&self, cutoff: &[u64]) -> (usize, Option<ShardCacheError>) {
        let candidates = self
            .pending_keys
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .iter()
                    .filter(|(_, meta)| {
                        meta.generation < cutoff[meta.primary_shard] && !meta.queued
                    })
                    .map(|(key, meta)| (key.clone(), meta.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut retried = 0;
        let mut first_error = None;
        for (key, candidate) in candidates {
            match enqueue_pending_retry(
                key,
                candidate,
                &self.workers.senders,
                &self.workers.shard_in_flight,
                self.workers.capacity_per_shard,
                self.workers.drains_per_shard,
                &self.workers.metrics,
                &self.pending_keys,
                &self.key_gates,
                &self.admission,
            ) {
                Ok(true) => retried += 1,
                Ok(false) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        (retried, first_error)
    }

    fn pending_before(&self, cutoff: &[u64]) -> usize {
        self.pending_keys
            .iter()
            .map(|shard| {
                shard
                    .read()
                    .values()
                    .filter(|meta| meta.generation < cutoff[meta.primary_shard])
                    .count()
            })
            .sum()
    }

    fn lock_all_admissions(&self) -> Vec<parking_lot::RwLockWriteGuard<'_, ()>> {
        self.admission.iter().map(RwLock::write).collect()
    }

    /// Mirrors all resident values, used after authoritative local recovery.
    pub fn synchronize_resident(&self) -> Result<()> {
        let _admission = self.lock_all_admissions();
        let _key_gates = self.lock_all_keys();
        for entry in self.inner.try_entry_snapshot()? {
            let key_hash = xxh3_64(&entry.key);
            let metadata_index = key_gate_index_for_hash(key_hash, self.remote_keys.len());
            if entry.key.len() > self.options.max_key_bytes {
                return Err(ShardCacheError::Persistence(format!(
                    "recovered key is {} bytes, exceeding kv_overflow.max_key_bytes {}",
                    entry.key.len(),
                    self.options.max_key_bytes
                )));
            }
            let already_tracked = self.remote_keys[metadata_index]
                .read()
                .get_hashed(key_hash, &entry.key)
                .is_some();
            if !already_tracked && !self.metadata_budget.try_reserve(entry.key.len()) {
                self.cluster
                    .metrics
                    .metadata_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Err(ShardCacheError::Persistence(
                    "recovered keyspace exceeds kv_overflow.max_metadata_bytes".into(),
                ));
            }
            let primary_shard = self
                .inner
                .route_key_prehashed(key_hash, &entry.key)
                .shard_id;
            if let Err(error) = self.cluster.put_until_on_shard(
                primary_shard,
                key_hash,
                &entry.key,
                &entry.value,
                entry.governance.as_deref(),
                entry.expire_at_ms,
            ) {
                if !already_tracked {
                    self.metadata_budget.release(entry.key.len());
                }
                return Err(error);
            }
            let generation = self.sequence[primary_shard]
                .0
                .fetch_add(1, Ordering::Relaxed);
            self.remote_keys[metadata_index].write().insert(
                SharedBytes::from(entry.key),
                RemoteKeyMeta {
                    primary_shard,
                    expire_at_ms: entry.expire_at_ms,
                    generation,
                    handoff_complete: true,
                    protected: entry.governance.is_some(),
                },
            );
        }
        for primary_shard in 0..self.inner.shard_count() {
            self.enforce_memory_target(primary_shard);
        }
        Ok(())
    }

    /// Materializes resident and remotely offloaded values for persistence.
    pub fn try_entry_snapshot(&self) -> Result<Vec<StoredEntry>> {
        let mut entries = Vec::new();
        self.try_visit_entry_snapshot(|entry| {
            entries.push(entry);
            Ok(())
        })?;
        entries.sort_by_key(|entry| xxh3_64(&entry.key));
        Ok(entries)
    }

    /// Visits a persistence snapshot while retaining at most one remote
    /// metadata stripe and one remote value beyond the bounded resident tier.
    pub fn try_visit_entry_snapshot(
        &self,
        mut visit: impl FnMut(StoredEntry) -> Result<()>,
    ) -> Result<()> {
        let resident_entries = self.inner.try_entry_snapshot()?;
        let resident = resident_entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<HashSet<_>>();
        for entry in resident_entries {
            visit(entry)?;
        }
        let now_ms = now_millis();
        for metadata in self.remote_keys.iter() {
            let remote_entries = metadata
                .read()
                .iter()
                .map(|(key, meta)| (key.clone(), *meta))
                .collect::<Vec<_>>();
            for (key, meta) in remote_entries {
                if resident.contains(key.as_ref())
                    || meta.expire_at_ms.is_some_and(|expiry| expiry <= now_ms)
                {
                    continue;
                }
                let key_hash = xxh3_64(&key);
                let metadata_index = key_gate_index_for_hash(key_hash, self.remote_keys.len());
                let _key_gate = self.key_gate_for_hash(key_hash);
                if self.remote_keys[metadata_index]
                    .read()
                    .get_hashed(key_hash, &key)
                    .copied()
                    != Some(meta)
                {
                    return Err(ShardCacheError::Persistence(
                        "kv overflow snapshot metadata changed while streaming".into(),
                    ));
                }
                let remote = self
                    .cluster
                    .get_on_shard(meta.primary_shard, key_hash, &key)?
                    .ok_or_else(|| {
                        ShardCacheError::Persistence(format!(
                            "kv overflow snapshot could not materialize key in primary shard {}",
                            meta.primary_shard
                        ))
                    })?;
                visit(StoredEntry {
                    key: key.as_ref().to_vec(),
                    value: remote.value.as_ref().to_vec(),
                    expire_at_ms: meta.expire_at_ms,
                    governance: remote.governance.as_deref().map(<[u8]>::to_vec),
                })?;
            }
        }
        Ok(())
    }

    /// Writes a bounded-memory, atomic persistence snapshot.
    pub fn write_snapshot(
        &self,
        snapshots: &SnapshotStore,
        timestamp_ms: u64,
        compression: SnapshotCompression,
    ) -> Result<std::path::PathBuf> {
        snapshots.write_snapshot_streaming(timestamp_ms, compression, |sink| {
            self.try_visit_entry_snapshot(sink)
        })
    }

    /// Writes through the authoritative persistence runtime and prunes WAL
    /// only after the streamed snapshot is durable.
    pub fn write_persistence_snapshot(
        &self,
        persistence: &PersistenceRuntime,
        timestamp_ms: u64,
    ) -> Result<std::path::PathBuf> {
        persistence.snapshot_streaming(timestamp_ms, |sink| self.try_visit_entry_snapshot(sink))
    }

    /// Restores the newest snapshot incrementally and mirrors it to overflow.
    pub fn restore_latest_snapshot(&self, snapshots: &SnapshotStore) -> Result<Option<u64>> {
        let mut restored = 0usize;
        let flush_interval = (self.options.queue_capacity / 2).max(1);
        let loaded = snapshots.visit_latest_snapshot(|entry| {
            let now_ms = now_millis();
            if entry
                .expire_at_ms
                .is_some_and(|deadline| deadline <= now_ms)
            {
                return Ok(());
            }
            let ttl_ms = entry
                .expire_at_ms
                .map(|deadline| deadline.saturating_sub(now_ms));
            self.set(entry.key, entry.value, ttl_ms)?;
            restored = restored.saturating_add(1);
            if restored.is_multiple_of(flush_interval) {
                self.flush_remote()?;
            }
            Ok(())
        })?;
        if loaded.is_some() {
            self.flush_remote()?;
        }
        Ok(loaded.map(|(_, timestamp_ms)| timestamp_ms))
    }

    pub fn health_snapshot(&self) -> KvOverflowHealthSnapshot {
        let metrics = &self.cluster.metrics;
        let circuit_health = self.cluster.nodes.iter().fold(
            (0usize, 0u64, 0u64),
            |(open, events, suppressed), node| {
                let (node_open, node_events, node_suppressed) = node.circuit_breaker_health();
                (
                    open.saturating_add(usize::from(node_open)),
                    events.saturating_add(node_events),
                    suppressed.saturating_add(node_suppressed),
                )
            },
        );
        let node_storage_health = self.cluster.nodes.iter().fold(
            (0u64, 0u64, 0u64),
            |(raw, stored, compressed), node| {
                let (node_raw, node_stored, node_compressed) = node.storage_health();
                (
                    raw.saturating_add(node_raw),
                    stored.saturating_add(node_stored),
                    compressed.saturating_add(node_compressed),
                )
            },
        );
        let shard_completion_backlogs = self
            .workers
            .completion_receivers
            .iter()
            .map(Receiver::len)
            .collect::<Vec<_>>();
        KvOverflowHealthSnapshot {
            backend: self.cluster.backend,
            transport: self.cluster.transport,
            tls_enabled: self.cluster.tls_enabled,
            compression: self.options.compression,
            adaptive_pipeline: self.options.adaptive_pipeline,
            max_value_bytes: self.options.max_value_bytes,
            max_key_bytes: self.options.max_key_bytes,
            max_metadata_bytes: self.options.max_metadata_bytes,
            metadata_bytes: self.metadata_budget.used(),
            metadata_rejections: metrics.metadata_rejections.load(Ordering::Relaxed),
            resident_backpressure: metrics.resident_backpressure.load(Ordering::Relaxed),
            retained_buffer_bytes: self.options.retained_buffer_bytes,
            circuit_open_targets: circuit_health.0,
            circuit_open_events: circuit_health.1,
            circuit_suppressed_requests: circuit_health.2,
            node_count: self.cluster.nodes.len(),
            previous_node_count: self.cluster.previous_nodes.len(),
            primary_shard_count: self.cluster.primary_shard_count,
            slot_count: self.cluster.slot_count,
            resident_keys: self.inner.len(),
            remote_keys: metadata_len(&self.remote_keys),
            resident_bytes: self.inner.stored_bytes(),
            puts: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.puts.load(Ordering::Relaxed))
                .sum(),
            put_failures: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.put_failures.load(Ordering::Relaxed))
                .sum(),
            gets: metrics.gets.load(Ordering::Relaxed),
            get_hits: metrics.get_hits.load(Ordering::Relaxed),
            get_failures: metrics.get_failures.load(Ordering::Relaxed),
            deletes: metrics.deletes.load(Ordering::Relaxed),
            delete_failures: metrics.delete_failures.load(Ordering::Relaxed),
            offloads: metrics.offloads.load(Ordering::Relaxed),
            fault_ins: metrics.fault_ins.load(Ordering::Relaxed),
            queue_depth: self.workers.queue_depth(),
            queue_capacity: self.workers.queue_capacity(),
            shard_queue_depths: self.workers.shard_queue_depths(),
            shard_queue_capacities: vec![
                self.workers.capacity_per_shard;
                self.cluster.primary_shard_count
            ],
            completion_backlog: shard_completion_backlogs.iter().sum(),
            shard_completion_backlogs,
            drains_per_shard: self.workers.drains_per_shard,
            pending_keys: metadata_len(&self.pending_keys),
            failed_pending_keys: self
                .pending_keys
                .iter()
                .map(|shard| shard.read().values().filter(|meta| !meta.queued).count())
                .sum(),
            active_workers: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.active_workers.load(Ordering::Relaxed))
                .sum(),
            enqueued_puts: metrics
                .shards
                .iter()
                .map(|metrics| metrics.primary.enqueued_puts.load(Ordering::Relaxed))
                .sum(),
            enqueue_failures: metrics
                .shards
                .iter()
                .map(|metrics| metrics.primary.enqueue_failures.load(Ordering::Relaxed))
                .sum(),
            replicated_puts: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.replicated_puts.load(Ordering::Relaxed))
                .sum(),
            replication_failures: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.replication_failures.load(Ordering::Relaxed))
                .sum(),
            handoff_reads: metrics.handoff_reads.load(Ordering::Relaxed),
            handoff_hits: metrics.handoff_hits.load(Ordering::Relaxed),
            handoff_failures: metrics.handoff_failures.load(Ordering::Relaxed),
            handoff_migrated: metrics.handoff_migrated.load(Ordering::Relaxed),
            handoff_pending: self
                .remote_keys
                .iter()
                .map(|shard| {
                    shard
                        .read()
                        .iter()
                        .filter(|(key, meta)| {
                            !meta.handoff_complete
                                && self
                                    .cluster
                                    .previous_owner_index(meta.primary_shard, xxh3_64(key))
                                    .is_some()
                        })
                        .count()
                })
                .sum(),
            oversized_rejections: metrics.oversized_rejections.load(Ordering::Relaxed),
            pipeline_batches: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.pipeline_batches.load(Ordering::Relaxed))
                .sum(),
            pipeline_items: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.pipeline_items.load(Ordering::Relaxed))
                .sum(),
            pipeline_bytes: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.pipeline_bytes.load(Ordering::Relaxed))
                .sum(),
            pipeline_latency_ns: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.pipeline_latency_ns.load(Ordering::Relaxed))
                .sum(),
            shard_pipeline_item_limits: metrics
                .shards
                .iter()
                .map(|metrics| {
                    metrics
                        .worker
                        .adaptive_pipeline_items
                        .load(Ordering::Relaxed)
                })
                .collect(),
            object_bytes_raw: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.object_bytes_raw.load(Ordering::Relaxed))
                .sum::<u64>()
                .saturating_add(node_storage_health.0),
            object_bytes_stored: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.object_bytes_stored.load(Ordering::Relaxed))
                .sum::<u64>()
                .saturating_add(node_storage_health.1),
            compressed_values: metrics
                .shards
                .iter()
                .map(|metrics| metrics.worker.compressed_values.load(Ordering::Relaxed))
                .sum::<u64>()
                .saturating_add(node_storage_health.2),
            ownership: self.cluster.ownership_snapshot(),
        }
    }

    fn enforce_memory_target(&self, primary_shard: usize) {
        enforce_memory_target(
            &self.inner,
            &self.cluster,
            &self.options,
            &self.remote_keys,
            primary_shard,
            &self.maintenance[primary_shard],
        );
    }

    fn drain_completions(&self, primary_shard: usize, max_items: usize) {
        if drain_completion_queue(
            &self.workers.completion_receivers[primary_shard],
            &self.workers.shard_in_flight[primary_shard],
            &self.inner,
            &self.remote_keys,
            &self.pending_keys,
            &self.key_gates,
            &self.metadata_budget,
            max_items,
        ) {
            self.enforce_memory_target(primary_shard);
        }
    }

    fn reserve_worker_slot(&self, primary_shard: usize) -> Result<()> {
        let in_flight = &self.workers.shard_in_flight[primary_shard];
        if try_reserve_in_flight(in_flight, self.workers.capacity_per_shard) {
            return Ok(());
        }
        self.drain_completions(primary_shard, self.options.pipeline_max_items);
        if try_reserve_in_flight(in_flight, self.workers.capacity_per_shard) {
            return Ok(());
        }
        self.workers
            .metrics
            .shard(primary_shard)
            .primary
            .enqueue_failures
            .fetch_add(1, Ordering::Relaxed);
        Err(ShardCacheError::Backpressure(
            "kv overflow replication queue is full",
        ))
    }

    fn drain_all_completions(&self) {
        for primary_shard in 0..self.cluster.primary_shard_count {
            self.drain_completions(primary_shard, usize::MAX);
        }
    }

    fn apply_completions_under_pressure(&self, primary_shard: usize) {
        if self.inner.stored_bytes_in_shard(primary_shard)
            > shard_memory_target(&self.options, self.inner.shard_count(), primary_shard)
        {
            self.drain_completions(primary_shard, self.options.pipeline_max_items);
        }
    }

    fn key_gate_for_hash(&self, key_hash: u64) -> parking_lot::MutexGuard<'_, ()> {
        let index = key_gate_index_for_hash(key_hash, self.key_gates.len());
        self.key_gates[index].lock()
    }

    #[cfg(test)]
    fn key_gate(&self, key: &[u8]) -> parking_lot::MutexGuard<'_, ()> {
        self.key_gate_for_hash(xxh3_64(key))
    }

    fn lock_all_keys(&self) -> Vec<parking_lot::MutexGuard<'_, ()>> {
        self.key_gates.iter().map(Mutex::lock).collect()
    }
}

impl Drop for KvOverflowStore {
    fn drop(&mut self) {
        self.cleanup.shutdown();
        self.workers.shutdown();
    }
}

async fn run_direct_shard_worker(
    primary_shard: usize,
    mut receiver: mpsc::Receiver<KvOverflowJob>,
    targets: Vec<(usize, ScnpDirectTarget)>,
    cluster: Arc<KvOverflowCluster>,
    options: KvOverflowOptions,
    completions: Sender<(SharedBytes, KvOverflowCompletion)>,
    in_flight: Arc<AtomicUsize>,
) {
    let lane_count = targets
        .len()
        .saturating_mul(options.max_inflight_per_target);
    let lane_capacity = options.queue_capacity / lane_count.max(1);
    let mut target_lanes = (0..cluster.nodes.len())
        .map(|_| Vec::new())
        .collect::<Vec<Vec<mpsc::Sender<DirectMutationJob>>>>();
    let mut joins = Vec::new();
    for (owner, target) in targets {
        for lane in 0..options.max_inflight_per_target {
            let (sender, lane_receiver) = mpsc::channel(lane_capacity);
            target_lanes[owner].push(sender);
            joins.push(tokio::spawn(run_direct_target_lane(
                primary_shard,
                lane,
                target.clone(),
                lane_receiver,
                options.clone(),
                Arc::clone(&cluster.metrics),
                completions.clone(),
                Arc::clone(&in_flight),
            )));
        }
    }

    while let Some(job) = receiver.recv().await {
        match job {
            KvOverflowJob::Put {
                primary_shard: job_primary_shard,
                key,
                key_hash,
                value,
                governance,
                expire_at_ms,
                generation,
            } => {
                debug_assert_eq!(job_primary_shard, primary_shard);
                let owner = cluster.owner_index_for_hash_on_shard(primary_shard, key_hash);
                let storage_key = cluster
                    .storage_key_for(owner, primary_shard, key_hash, &key)
                    .into_owned();
                let lanes = &target_lanes[owner];
                let lane = key_hash as usize % lanes.len();
                let direct_job = DirectMutationJob::Put(DirectPutJob {
                    put: PendingPutJob {
                        primary_shard,
                        key,
                        key_hash,
                        value,
                        governance,
                        expire_at_ms,
                        generation,
                    },
                    storage_key,
                });
                if let Err(error) = lanes[lane].try_send(direct_job)
                    && let DirectMutationJob::Put(job) = error.into_inner()
                {
                    let worker_metrics = &cluster.metrics.shard(primary_shard).worker;
                    worker_metrics.puts.fetch_add(1, Ordering::Relaxed);
                    worker_metrics
                        .replication_failures
                        .fetch_add(1, Ordering::Relaxed);
                    worker_metrics.put_failures.fetch_add(1, Ordering::Relaxed);
                    complete_direct_put(&completions, job, false);
                }
            }
            KvOverflowJob::Delete {
                primary_shard: job_primary_shard,
                key,
                key_hash,
                generation,
                completion,
            } => {
                debug_assert_eq!(job_primary_shard, primary_shard);
                let owner = cluster.owner_index_for_hash_on_shard(primary_shard, key_hash);
                let storage_key = cluster
                    .storage_key_for(owner, primary_shard, key_hash, &key)
                    .into_owned();
                let lanes = &target_lanes[owner];
                let lane = key_hash as usize % lanes.len();
                let direct_job = DirectMutationJob::Delete {
                    key,
                    key_hash,
                    storage_key,
                    generation,
                    completion,
                };
                if let Err(error) = lanes[lane].try_send(direct_job)
                    && let DirectMutationJob::Delete {
                        key,
                        key_hash,
                        generation,
                        completion,
                        ..
                    } = error.into_inner()
                {
                    cluster.metrics.deletes.fetch_add(1, Ordering::Relaxed);
                    cluster
                        .metrics
                        .delete_failures
                        .fetch_add(1, Ordering::Relaxed);
                    complete_direct_delete(
                        &completions,
                        &in_flight,
                        key,
                        key_hash,
                        generation,
                        completion,
                        Err(ShardCacheError::ChannelClosed("SCNP target lane")),
                    );
                }
            }
            KvOverflowJob::Barrier(completion) => {
                let mut barriers = Vec::new();
                let mut first_error = None;
                for lanes in &target_lanes {
                    for lane in lanes {
                        let (sender, receiver) = oneshot::channel();
                        if lane.send(DirectMutationJob::Barrier(sender)).await.is_ok() {
                            barriers.push(receiver);
                        } else {
                            first_error
                                .get_or_insert(ShardCacheError::ChannelClosed("SCNP target lane"));
                        }
                    }
                }
                for barrier in barriers {
                    match barrier.await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            first_error.get_or_insert(error);
                        }
                        Err(_) => {
                            first_error.get_or_insert(ShardCacheError::ChannelClosed(
                                "SCNP target barrier",
                            ));
                        }
                    }
                }
                let _ = completion.send(first_error.map_or(Ok(()), Err));
            }
            KvOverflowJob::Shutdown => break,
        }
    }
    for lanes in target_lanes {
        for lane in lanes {
            let _ = lane.send(DirectMutationJob::Shutdown).await;
        }
    }
    for join in joins {
        let _ = join.await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_direct_target_lane(
    primary_shard: usize,
    lane: usize,
    target: ScnpDirectTarget,
    mut receiver: mpsc::Receiver<DirectMutationJob>,
    options: KvOverflowOptions,
    metrics: Arc<KvOverflowMetrics>,
    completions: Sender<(SharedBytes, KvOverflowCompletion)>,
    in_flight: Arc<AtomicUsize>,
) {
    let remote_shard = target.remote_shard;
    let mut connection = AsyncScnpMutationConnection::new(target, primary_shard, lane);
    let mut buffers = DirectPipelineBuffers::default();
    let mut deferred = None;
    let mut puts = Vec::with_capacity(options.pipeline_max_items);
    let mut adaptive = AdaptivePipeline::new(&options);
    metrics
        .shard(primary_shard)
        .worker
        .adaptive_pipeline_items
        .store(adaptive.item_limit(), Ordering::Relaxed);
    loop {
        let job = match deferred.take() {
            Some(job) => job,
            None => match receiver.recv().await {
                Some(job) => job,
                None => break,
            },
        };
        match job {
            DirectMutationJob::Put(put) => {
                puts.clear();
                let mut pipeline_bytes = direct_put_bytes(&put);
                puts.push(put);
                let mut waited = false;
                while puts.len() < adaptive.item_limit() {
                    let next = match receiver.try_recv() {
                        Ok(job) => Some(job),
                        Err(mpsc::error::TryRecvError::Disconnected) => None,
                        Err(mpsc::error::TryRecvError::Empty)
                            if !waited && !options.pipeline_flush.is_zero() =>
                        {
                            waited = true;
                            tokio::time::timeout(options.pipeline_flush, receiver.recv())
                                .await
                                .ok()
                                .flatten()
                        }
                        Err(mpsc::error::TryRecvError::Empty) => None,
                    };
                    let Some(next) = next else {
                        break;
                    };
                    match next {
                        DirectMutationJob::Put(put) => {
                            let job_bytes = direct_put_bytes(&put);
                            if pipeline_bytes.saturating_add(job_bytes) > options.pipeline_max_bytes
                            {
                                deferred = Some(DirectMutationJob::Put(put));
                                break;
                            }
                            pipeline_bytes = pipeline_bytes.saturating_add(job_bytes);
                            puts.push(put);
                        }
                        job => {
                            deferred = Some(job);
                            break;
                        }
                    }
                }
                let (elapsed, succeeded) = execute_direct_puts(
                    primary_shard,
                    remote_shard,
                    &mut connection,
                    &mut puts,
                    &mut buffers,
                    &options,
                    &metrics,
                    &completions,
                )
                .await;
                adaptive.record(
                    elapsed,
                    succeeded,
                    pipeline_bytes,
                    options.pipeline_max_bytes,
                );
                metrics
                    .shard(primary_shard)
                    .worker
                    .adaptive_pipeline_items
                    .store(adaptive.item_limit(), Ordering::Relaxed);
            }
            DirectMutationJob::Delete {
                key,
                key_hash,
                storage_key,
                generation,
                completion,
            } => {
                metrics.deletes.fetch_add(1, Ordering::Relaxed);
                let mut request = Vec::with_capacity(storage_key.len().saturating_add(32));
                let result = write_direct_delete(&mut request, &storage_key, remote_shard);
                let result = match result {
                    Ok(()) => {
                        connection
                            .execute(&request, &[DirectResponseKind::Integer])
                            .await
                    }
                    Err(error) => Err(error),
                };
                if result.is_err() {
                    metrics.delete_failures.fetch_add(1, Ordering::Relaxed);
                }
                complete_direct_delete(
                    &completions,
                    &in_flight,
                    key,
                    key_hash,
                    generation,
                    completion,
                    result,
                );
            }
            DirectMutationJob::Barrier(completion) => {
                let _ = completion.send(Ok(()));
            }
            DirectMutationJob::Shutdown => break,
        }
    }
}

fn direct_put_bytes(job: &DirectPutJob) -> usize {
    job.storage_key
        .len()
        .saturating_add(job.put.value.len())
        .saturating_add(job.put.governance.as_ref().map_or(0, SharedBytes::len))
        .saturating_add(if job.put.governance.is_some() {
            KV_OVERFLOW_GOVERNED_HEADER_LEN
        } else {
            KV_OVERFLOW_HEADER_LEN
        })
}

#[allow(clippy::too_many_arguments)]
async fn execute_direct_puts(
    primary_shard: usize,
    remote_shard: usize,
    connection: &mut AsyncScnpMutationConnection,
    puts: &mut Vec<DirectPutJob>,
    buffers: &mut DirectPipelineBuffers,
    options: &KvOverflowOptions,
    metrics: &KvOverflowMetrics,
    completions: &Sender<(SharedBytes, KvOverflowCompletion)>,
) -> (Duration, bool) {
    let total_started = Instant::now();
    let shard_metrics = metrics.shard(primary_shard);
    shard_metrics
        .worker
        .puts
        .fetch_add(puts.len() as u64, Ordering::Relaxed);
    buffers.request.clear();
    buffers.responses.clear();
    buffers.request.reserve(
        puts.iter()
            .map(direct_put_bytes)
            .fold(0usize, usize::saturating_add),
    );
    buffers.responses.reserve(puts.len());
    let now_ms = now_millis();
    let mut encode_result = Ok(());
    let mut encoded_metrics = vec![None; puts.len()];
    for (index, job) in puts.iter().enumerate() {
        let result = if job
            .put
            .expire_at_ms
            .is_none_or(|deadline| deadline > now_ms)
        {
            buffers.responses.push(DirectResponseKind::Ok);
            if job.put.value.len() > options.max_value_bytes {
                Err(ShardCacheError::Protocol(
                    "key-value overflow value exceeds configured maximum".into(),
                ))
            } else {
                write_direct_overflow_set(
                    &mut buffers.request,
                    &job.storage_key,
                    &job.put.value,
                    job.put.governance.as_deref(),
                    job.put.expire_at_ms,
                    remote_shard,
                    options.compression,
                    options.compression_min_value_bytes,
                    options.compression_min_savings_percent,
                    &mut buffers.compressed,
                )
                .map(|(stored_bytes, compressed)| {
                    encoded_metrics[index] = Some((job.put.value.len(), stored_bytes, compressed));
                })
            }
        } else {
            buffers.responses.push(DirectResponseKind::Integer);
            write_direct_delete(&mut buffers.request, &job.storage_key, remote_shard)
        };
        if let Err(error) = result {
            encode_result = Err(error);
            break;
        }
    }
    let started = Instant::now();
    shard_metrics
        .worker
        .active_workers
        .fetch_add(1, Ordering::Relaxed);
    let result = match encode_result {
        Ok(()) => {
            connection
                .execute(&buffers.request, &buffers.responses)
                .await
        }
        Err(error) => Err(error),
    };
    shard_metrics
        .worker
        .active_workers
        .fetch_sub(1, Ordering::Relaxed);
    shard_metrics
        .worker
        .pipeline_batches
        .fetch_add(1, Ordering::Relaxed);
    shard_metrics
        .worker
        .pipeline_items
        .fetch_add(puts.len() as u64, Ordering::Relaxed);
    shard_metrics
        .worker
        .pipeline_bytes
        .fetch_add(buffers.request.len() as u64, Ordering::Relaxed);
    shard_metrics.worker.pipeline_latency_ns.fetch_add(
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    let outcomes = if result.is_ok() {
        vec![true; puts.len()]
    } else if puts.len() > 1 {
        let mut outcomes = Vec::with_capacity(puts.len());
        for (index, job) in puts.iter().enumerate() {
            buffers.request.clear();
            buffers.responses.clear();
            let result = if job
                .put
                .expire_at_ms
                .is_none_or(|deadline| deadline > now_millis())
            {
                buffers.responses.push(DirectResponseKind::Ok);
                if job.put.value.len() > options.max_value_bytes {
                    Err(ShardCacheError::Protocol(
                        "key-value overflow value exceeds configured maximum".into(),
                    ))
                } else {
                    write_direct_overflow_set(
                        &mut buffers.request,
                        &job.storage_key,
                        &job.put.value,
                        job.put.governance.as_deref(),
                        job.put.expire_at_ms,
                        remote_shard,
                        options.compression,
                        options.compression_min_value_bytes,
                        options.compression_min_savings_percent,
                        &mut buffers.compressed,
                    )
                    .map(|(stored_bytes, compressed)| {
                        encoded_metrics[index] =
                            Some((job.put.value.len(), stored_bytes, compressed));
                    })
                }
            } else {
                buffers.responses.push(DirectResponseKind::Integer);
                write_direct_delete(&mut buffers.request, &job.storage_key, remote_shard)
            };
            outcomes.push(match result {
                Ok(()) => connection
                    .execute(&buffers.request, &buffers.responses)
                    .await
                    .is_ok(),
                Err(_) => false,
            });
        }
        outcomes
    } else {
        vec![false]
    };
    let succeeded = outcomes.iter().filter(|outcome| **outcome).count() as u64;
    let failed = outcomes.len() as u64 - succeeded;
    shard_metrics
        .worker
        .replicated_puts
        .fetch_add(succeeded, Ordering::Relaxed);
    shard_metrics
        .worker
        .replication_failures
        .fetch_add(failed, Ordering::Relaxed);
    for (succeeded, encoded) in outcomes.iter().zip(encoded_metrics) {
        if !succeeded {
            continue;
        }
        if let Some((raw_bytes, stored_bytes, compressed)) = encoded {
            shard_metrics
                .worker
                .object_bytes_raw
                .fetch_add(raw_bytes as u64, Ordering::Relaxed);
            shard_metrics
                .worker
                .object_bytes_stored
                .fetch_add(stored_bytes as u64, Ordering::Relaxed);
            if compressed {
                shard_metrics
                    .worker
                    .compressed_values
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    shard_metrics
        .worker
        .put_failures
        .fetch_add(failed, Ordering::Relaxed);
    for (job, succeeded) in puts.drain(..).zip(outcomes) {
        complete_direct_put(completions, job, succeeded);
    }
    buffers.request.clear();
    buffers.compressed.clear();
    if buffers.request.capacity() > options.retained_buffer_bytes {
        buffers.request.shrink_to(options.retained_buffer_bytes);
    }
    if buffers.compressed.capacity() > options.retained_buffer_bytes {
        buffers.compressed.shrink_to(options.retained_buffer_bytes);
    }
    (total_started.elapsed(), failed == 0)
}

fn complete_direct_put(
    completions: &Sender<(SharedBytes, KvOverflowCompletion)>,
    job: DirectPutJob,
    succeeded: bool,
) {
    let protected = job.put.governance.is_some();
    completions
        .send((
            job.put.key,
            KvOverflowCompletion::Put {
                primary_shard: job.put.primary_shard,
                key_hash: job.put.key_hash,
                retry_value: (!succeeded).then_some(job.put.value),
                retry_governance: (!succeeded).then_some(job.put.governance).flatten(),
                protected,
                expire_at_ms: job.put.expire_at_ms,
                generation: job.put.generation,
                succeeded,
            },
        ))
        .expect("primary shard completion receiver remains live");
}

#[allow(clippy::too_many_arguments)]
fn complete_direct_delete(
    completions: &Sender<(SharedBytes, KvOverflowCompletion)>,
    in_flight: &AtomicUsize,
    key: SharedBytes,
    key_hash: u64,
    generation: u64,
    completion: Option<Sender<Result<()>>>,
    result: Result<()>,
) {
    let succeeded = result.is_ok();
    if let Some(completion) = completion {
        in_flight.fetch_sub(1, Ordering::Release);
        let _ = completion.send(result);
    } else {
        completions
            .send((
                key,
                KvOverflowCompletion::Delete {
                    key_hash,
                    generation,
                    succeeded,
                },
            ))
            .expect("primary shard completion receiver remains live");
    }
}

impl KvOverflowWorkerPool {
    #[allow(clippy::too_many_arguments)]
    fn start(
        capacity: usize,
        cluster: Arc<KvOverflowCluster>,
        options: KvOverflowOptions,
    ) -> Result<Self> {
        let primary_shard_count = cluster.primary_shard_count();
        let drains_per_shard = 1;
        let worker_count = primary_shard_count;
        let capacity_per_shard = capacity;
        primary_shard_count
            .checked_mul(capacity_per_shard)
            .ok_or_else(|| {
                ShardCacheError::Config("kv overflow queue capacity exceeds platform limits".into())
            })?;
        let shard_in_flight = (0..primary_shard_count)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let (completion_senders, completion_receivers): (Vec<_>, Vec<_>) = (0..primary_shard_count)
            .map(|_| bounded(capacity_per_shard))
            .unzip();
        let completion_receivers: Arc<[Receiver<(SharedBytes, KvOverflowCompletion)>]> =
            completion_receivers.into();
        for primary_shard in 0..primary_shard_count {
            if let Some(targets) = cluster.direct_targets_for_shard(primary_shard)
                && (targets.len() > 1 || options.max_inflight_per_target > 1)
            {
                let lanes = targets
                    .len()
                    .checked_mul(options.max_inflight_per_target)
                    .ok_or_else(|| {
                        ShardCacheError::Config(
                            "kv overflow target lane count exceeds platform limits".into(),
                        )
                    })?;
                if lanes > capacity_per_shard {
                    return Err(ShardCacheError::Config(format!(
                        "primary shard {primary_shard} requires {lanes} target lanes but queue_capacity_per_shard is {capacity_per_shard}"
                    )));
                }
            }
        }
        let mut senders: Vec<KvOverflowJobSender> = Vec::with_capacity(worker_count);
        let mut joins: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let primary_shard = worker_id;
            let worker_cluster = Arc::clone(&cluster);
            let worker_completions = completion_senders[primary_shard].clone();
            let worker_options = options.clone();
            let worker_in_flight = Arc::clone(&shard_in_flight[primary_shard]);
            let direct_targets = worker_cluster
                .direct_targets_for_shard(primary_shard)
                .filter(|targets| targets.len() > 1 || worker_options.max_inflight_per_target > 1);
            let (sender, mut receiver) = if direct_targets.is_some() {
                let (sender, receiver) = mpsc::channel(capacity_per_shard);
                (
                    KvOverflowJobSender::Async(sender),
                    KvOverflowJobReceiver::Async(receiver),
                )
            } else {
                let (sender, receiver) = bounded(capacity_per_shard);
                (
                    KvOverflowJobSender::Blocking(sender),
                    KvOverflowJobReceiver::Blocking(receiver),
                )
            };
            let direct_runtime = direct_targets
                .as_ref()
                .map(|_| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                })
                .transpose()
                .map_err(|error| {
                    ShardCacheError::Config(format!(
                        "failed to create key-value overflow runtime: {error}"
                    ))
                })?;
            let join = match thread::Builder::new()
                .name(format!(
                    "shardmap-kv-overflow-{primary_shard}-{}",
                    worker_id % drains_per_shard
                ))
                .spawn(move || {
                    if let (Some(runtime), Some(targets)) = (direct_runtime, direct_targets) {
                        let KvOverflowJobReceiver::Async(receiver) = receiver else {
                            unreachable!("direct runtime requires an asynchronous ingress queue")
                        };
                        runtime.block_on(run_direct_shard_worker(
                            primary_shard,
                            receiver,
                            targets,
                            worker_cluster,
                            worker_options,
                            worker_completions,
                            worker_in_flight,
                        ));
                        return;
                    }
                    let mut deferred = None;
                    let mut puts = Vec::with_capacity(worker_options.pipeline_max_items);
                    let mut outcomes = Vec::with_capacity(worker_options.pipeline_max_items);
                    let mut key_hashes = Vec::with_capacity(worker_options.pipeline_max_items);
                    let mut by_owner = Vec::with_capacity(worker_options.pipeline_max_items);
                    let shard_metrics = worker_cluster.metrics.shard(primary_shard);
                    let mut adaptive = AdaptivePipeline::new(&worker_options);
                    shard_metrics
                        .worker
                        .adaptive_pipeline_items
                        .store(adaptive.item_limit(), Ordering::Relaxed);
                    loop {
                        let job = match deferred.take() {
                            Some(job) => job,
                            None => match receiver.blocking_recv() {
                                Some(job) => job,
                                None => break,
                            },
                        };
                        match job {
                            KvOverflowJob::Put {
                                primary_shard: job_primary_shard,
                                key,
                                key_hash,
                                value,
                                governance,
                                expire_at_ms,
                                generation,
                            } => {
                                debug_assert_eq!(job_primary_shard, primary_shard);
                                puts.clear();
                                let mut pipeline_bytes = key.len().saturating_add(value.len());
                                puts.push(PendingPutJob {
                                    primary_shard,
                                    key,
                                    key_hash,
                                    value,
                                    governance,
                                    expire_at_ms,
                                    generation,
                                });
                                for phase in 0..2 {
                                    while puts.len() < adaptive.item_limit() {
                                        let next = match receiver.try_recv() {
                                            Ok(job) => job,
                                            Err(KvOverflowTryRecvError::Empty) => break,
                                            Err(KvOverflowTryRecvError::Disconnected) => break,
                                        };
                                        match next {
                                            KvOverflowJob::Put {
                                                primary_shard: job_primary_shard,
                                                key,
                                                key_hash,
                                                value,
                                                governance,
                                                expire_at_ms,
                                                generation,
                                            } if job_primary_shard == primary_shard => {
                                                let job_bytes =
                                                    key.len().saturating_add(value.len());
                                                if pipeline_bytes.saturating_add(job_bytes)
                                                    > worker_options.pipeline_max_bytes
                                                {
                                                    deferred = Some(KvOverflowJob::Put {
                                                        primary_shard,
                                                        key,
                                                        key_hash,
                                                        value,
                                                        governance,
                                                        expire_at_ms,
                                                        generation,
                                                    });
                                                    break;
                                                }
                                                pipeline_bytes =
                                                    pipeline_bytes.saturating_add(job_bytes);
                                                puts.push(PendingPutJob {
                                                    primary_shard,
                                                    key,
                                                    key_hash,
                                                    value,
                                                    governance,
                                                    expire_at_ms,
                                                    generation,
                                                });
                                            }
                                            job => {
                                                deferred = Some(job);
                                                break;
                                            }
                                        }
                                    }
                                    if phase == 0
                                        && puts.len() < adaptive.item_limit()
                                        && pipeline_bytes < worker_options.pipeline_max_bytes
                                        && deferred.is_none()
                                        && !worker_options.pipeline_flush.is_zero()
                                    {
                                        thread::sleep(worker_options.pipeline_flush);
                                    } else {
                                        break;
                                    }
                                }
                                let requests = puts
                                    .iter()
                                    .map(|put| KvOverflowPutRequest {
                                        key: &put.key,
                                        value: &put.value,
                                        expire_at_ms: put.expire_at_ms,
                                        governance: put.governance.as_deref(),
                                    })
                                    .collect::<Vec<_>>();
                                let shard_metrics = worker_cluster.metrics.shard(primary_shard);
                                shard_metrics
                                    .worker
                                    .active_workers
                                    .fetch_add(1, Ordering::Relaxed);
                                let first_owner = worker_cluster
                                    .owner_index_for_hash_on_shard(primary_shard, puts[0].key_hash);
                                let owner_hint = puts
                                    .iter()
                                    .all(|put| {
                                        worker_cluster.owner_index_for_hash_on_shard(
                                            primary_shard,
                                            put.key_hash,
                                        ) == first_owner
                                    })
                                    .then_some(first_owner);
                                key_hashes.clear();
                                key_hashes.extend(puts.iter().map(|put| put.key_hash));
                                let batch_started = Instant::now();
                                worker_cluster.put_batch_until(
                                    primary_shard,
                                    owner_hint,
                                    &key_hashes,
                                    &requests,
                                    &mut outcomes,
                                    &mut by_owner,
                                );
                                let batch_elapsed = batch_started.elapsed();
                                drop(requests);
                                shard_metrics
                                    .worker
                                    .active_workers
                                    .fetch_sub(1, Ordering::Relaxed);
                                let mut replicated = 0u64;
                                let mut failed = 0u64;
                                for (put, succeeded) in puts.drain(..).zip(outcomes.drain(..)) {
                                    let protected = put.governance.is_some();
                                    replicated += u64::from(succeeded);
                                    failed += u64::from(!succeeded);
                                    worker_completions
                                        .send((
                                            put.key,
                                            KvOverflowCompletion::Put {
                                                primary_shard: put.primary_shard,
                                                key_hash: put.key_hash,
                                                retry_value: (!succeeded).then_some(put.value),
                                                retry_governance: (!succeeded)
                                                    .then_some(put.governance)
                                                    .flatten(),
                                                protected,
                                                expire_at_ms: put.expire_at_ms,
                                                generation: put.generation,
                                                succeeded,
                                            },
                                        ))
                                        .expect("primary shard completion receiver remains live");
                                }
                                adaptive.record(
                                    batch_elapsed,
                                    failed == 0,
                                    pipeline_bytes,
                                    worker_options.pipeline_max_bytes,
                                );
                                shard_metrics
                                    .worker
                                    .adaptive_pipeline_items
                                    .store(adaptive.item_limit(), Ordering::Relaxed);
                                shard_metrics
                                    .worker
                                    .replicated_puts
                                    .fetch_add(replicated, Ordering::Relaxed);
                                shard_metrics
                                    .worker
                                    .replication_failures
                                    .fetch_add(failed, Ordering::Relaxed);
                            }
                            KvOverflowJob::Delete {
                                primary_shard: job_primary_shard,
                                key,
                                key_hash,
                                generation,
                                completion,
                            } => {
                                debug_assert_eq!(job_primary_shard, primary_shard);
                                let shard_metrics = worker_cluster.metrics.shard(primary_shard);
                                shard_metrics
                                    .worker
                                    .active_workers
                                    .fetch_add(1, Ordering::Relaxed);
                                let result =
                                    worker_cluster.delete_on_shard(primary_shard, key_hash, &key);
                                shard_metrics
                                    .worker
                                    .active_workers
                                    .fetch_sub(1, Ordering::Relaxed);
                                let succeeded = result.is_ok();
                                if let Some(completion) = completion {
                                    worker_in_flight.fetch_sub(1, Ordering::Release);
                                    let _ = completion.send(result);
                                } else {
                                    worker_completions
                                        .send((
                                            key,
                                            KvOverflowCompletion::Delete {
                                                key_hash,
                                                generation,
                                                succeeded,
                                            },
                                        ))
                                        .expect("primary shard completion receiver remains live");
                                }
                            }
                            KvOverflowJob::Barrier(completion) => {
                                let _ = completion.send(Ok(()));
                            }
                            KvOverflowJob::Shutdown => break,
                        }
                    }
                }) {
                Ok(join) => join,
                Err(error) => {
                    for sender in &senders {
                        let _ = sender.send(KvOverflowJob::Shutdown);
                    }
                    for join in joins {
                        let _ = join.join();
                    }
                    return Err(ShardCacheError::Config(format!(
                        "failed to start key-value overflow worker: {error}"
                    )));
                }
            };
            senders.push(sender);
            joins.push(join);
        }
        Ok(Self {
            senders: senders.into_boxed_slice(),
            completion_receivers,
            shard_in_flight,
            capacity_per_shard,
            drains_per_shard,
            metrics: Arc::clone(&cluster.metrics),
            joins: Mutex::new(joins),
        })
    }

    fn enqueue_reserved_to_lane(&self, lane: usize, job: KvOverflowJob) -> bool {
        if self.senders[lane].try_send(job).is_ok() {
            true
        } else {
            let primary_shard = lane / self.drains_per_shard;
            self.shard_in_flight[primary_shard].fetch_sub(1, Ordering::Release);
            false
        }
    }

    #[cfg(test)]
    fn lane_for_key(&self, key: &[u8]) -> usize {
        let key_hash = xxh3_64(key);
        let primary_shard = stripe_index(key_hash, shift_for(self.shard_in_flight.len()));
        self.lane_for_hash(primary_shard, key_hash)
    }

    #[inline]
    fn lane_for_hash(&self, primary_shard: usize, key_hash: u64) -> usize {
        lane_for_hash(primary_shard, self.drains_per_shard, key_hash)
    }

    fn enqueue_barriers(
        &self,
    ) -> (
        Vec<crossbeam_channel::Receiver<Result<()>>>,
        Option<ShardCacheError>,
    ) {
        let mut completions = Vec::with_capacity(self.senders.len());
        let mut first_error = None;
        for sender in &self.senders {
            let (completion, result) = bounded(1);
            if sender.send(KvOverflowJob::Barrier(completion)).is_ok() {
                completions.push(result);
            } else {
                first_error.get_or_insert(ShardCacheError::ChannelClosed("kv overflow workers"));
            }
        }
        (completions, first_error)
    }

    fn queue_depth(&self) -> usize {
        self.shard_in_flight
            .iter()
            .map(|depth| depth.load(Ordering::Acquire))
            .sum()
    }

    fn shard_queue_depths(&self) -> Vec<usize> {
        self.shard_in_flight
            .iter()
            .map(|depth| depth.load(Ordering::Acquire))
            .collect()
    }

    fn queue_capacity(&self) -> usize {
        self.capacity_per_shard * self.shard_in_flight.len()
    }

    fn worker_count(&self) -> usize {
        self.senders.len()
    }

    fn shutdown(&self) {
        for sender in &self.senders {
            let _ = sender.send(KvOverflowJob::Shutdown);
        }
        for join in self.joins.lock().drain(..) {
            if join.thread().id() != thread::current().id() {
                let _ = join.join();
            }
        }
    }
}

fn reserve_in_flight(
    in_flight: &AtomicUsize,
    capacity: usize,
    enqueue_failures: &AtomicU64,
) -> Result<()> {
    if try_reserve_in_flight(in_flight, capacity) {
        return Ok(());
    }
    enqueue_failures.fetch_add(1, Ordering::Relaxed);
    Err(ShardCacheError::Backpressure(
        "kv overflow replication queue is full",
    ))
}

fn try_reserve_in_flight(in_flight: &AtomicUsize, capacity: usize) -> bool {
    in_flight
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |depth| {
            (depth < capacity).then_some(depth + 1)
        })
        .is_ok()
}

#[allow(clippy::too_many_arguments)]
fn enqueue_pending_retry(
    key: SharedBytes,
    candidate: PendingKeyMeta,
    senders: &[KvOverflowJobSender],
    shard_in_flight: &[Arc<AtomicUsize>],
    capacity_per_shard: usize,
    drains_per_shard: usize,
    metrics: &KvOverflowMetrics,
    pending_keys: &[RwLock<MetadataMap<PendingKeyMeta>>],
    key_gates: &[Mutex<()>],
    admission: &[RwLock<()>],
) -> Result<bool> {
    let key_hash = xxh3_64(&key);
    let primary_shard = candidate.primary_shard;
    let lane = lane_for_hash(primary_shard, drains_per_shard, key_hash);
    let in_flight = &shard_in_flight[primary_shard];
    let _admission = admission[lane].read();
    let metadata_index = key_gate_index_for_hash(key_hash, pending_keys.len());
    let _key_gate = key_gates[metadata_index].lock();
    let is_current = pending_keys[metadata_index]
        .read()
        .get_hashed(key_hash, key.as_ref())
        .is_some_and(|meta| meta.generation == candidate.generation && !meta.queued);
    if !is_current {
        return Ok(false);
    }
    reserve_in_flight(
        in_flight,
        capacity_per_shard,
        &metrics.shard(primary_shard).primary.enqueue_failures,
    )?;
    pending_keys[metadata_index]
        .write()
        .get_mut_hashed(key_hash, key.as_ref())
        .expect("pending mutation checked under key gate")
        .queued = true;
    let job = match candidate.mutation {
        PendingMutation::Put {
            value: Some(value),
            governance,
            expire_at_ms,
        } => KvOverflowJob::Put {
            primary_shard,
            key: key.clone(),
            key_hash,
            value,
            governance,
            expire_at_ms,
            generation: candidate.generation,
        },
        PendingMutation::Put { value: None, .. } => {
            in_flight.fetch_sub(1, Ordering::Release);
            pending_keys[metadata_index]
                .write()
                .get_mut_hashed(key_hash, key.as_ref())
                .expect("pending mutation checked under key gate")
                .queued = false;
            return Err(ShardCacheError::Protocol(
                "failed key-value overflow mutation has no retry payload".into(),
            ));
        }
        PendingMutation::Delete { .. } => KvOverflowJob::Delete {
            primary_shard,
            key: key.clone(),
            key_hash,
            generation: candidate.generation,
            completion: None,
        },
    };
    if senders[lane].try_send(job).is_ok() {
        return Ok(true);
    }
    in_flight.fetch_sub(1, Ordering::Release);
    if let Some(meta) = pending_keys[metadata_index]
        .write()
        .get_mut_hashed(key_hash, key.as_ref())
        .filter(|meta| meta.generation == candidate.generation)
    {
        meta.queued = false;
    }
    Err(ShardCacheError::ChannelClosed("kv overflow workers"))
}

#[inline]
fn lane_for_hash(primary_shard: usize, drains_per_shard: usize, key_hash: u64) -> usize {
    primary_shard * drains_per_shard + (key_hash as usize % drains_per_shard)
}

fn drain_barriers(completions: Vec<crossbeam_channel::Receiver<Result<()>>>) -> Result<()> {
    let mut first_error = None;
    for completion in completions {
        match completion.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(_) => {
                first_error.get_or_insert(ShardCacheError::ChannelClosed("kv overflow flush"));
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[allow(clippy::too_many_arguments)]
fn drain_completion_queue(
    completion_receiver: &Receiver<(SharedBytes, KvOverflowCompletion)>,
    in_flight: &AtomicUsize,
    inner: &EmbeddedStore,
    remote_keys: &[RwLock<MetadataMap<RemoteKeyMeta>>],
    pending_keys: &[RwLock<MetadataMap<PendingKeyMeta>>],
    key_gates: &[Mutex<()>],
    metadata_budget: &KvOverflowMetadataBudget,
    max_items: usize,
) -> bool {
    let mut enforce_memory = false;
    let completions = completion_receiver
        .try_iter()
        .take(max_items)
        .collect::<Vec<_>>();
    in_flight.fetch_sub(completions.len(), Ordering::Release);
    for (key, completion) in completions {
        match completion {
            KvOverflowCompletion::Put {
                primary_shard,
                key_hash,
                retry_value,
                retry_governance,
                protected,
                expire_at_ms,
                generation,
                succeeded,
            } => {
                enforce_memory |= apply_put_completion(
                    inner,
                    remote_keys,
                    pending_keys,
                    key_gates,
                    primary_shard,
                    key,
                    key_hash,
                    retry_value,
                    retry_governance,
                    protected,
                    expire_at_ms,
                    generation,
                    succeeded,
                );
            }
            KvOverflowCompletion::Delete {
                key_hash,
                generation,
                succeeded,
            } => apply_delete_completion(
                inner,
                remote_keys,
                pending_keys,
                key_gates,
                metadata_budget,
                key,
                key_hash,
                generation,
                succeeded,
            ),
        }
    }
    enforce_memory
}

#[allow(clippy::too_many_arguments)]
fn apply_put_completion(
    inner: &EmbeddedStore,
    remote_keys: &[RwLock<MetadataMap<RemoteKeyMeta>>],
    pending_keys: &[RwLock<MetadataMap<PendingKeyMeta>>],
    key_gates: &[Mutex<()>],
    primary_shard: usize,
    key: SharedBytes,
    key_hash: u64,
    retry_value: Option<SharedBytes>,
    retry_governance: Option<SharedBytes>,
    protected: bool,
    expire_at_ms: Option<u64>,
    generation: u64,
    succeeded: bool,
) -> bool {
    let gate_index = key_gate_index_for_hash(key_hash, key_gates.len());
    let _key_gate = key_gates[gate_index].lock();
    let route = inner.route_key_prehashed(key_hash, key.as_ref());
    let is_current = inner.overflow_generation_matches(route, key.as_ref(), generation);
    if !is_current {
        return false;
    }
    if succeeded {
        pending_keys[gate_index]
            .write()
            .remove_hashed(key_hash, key.as_ref());
        remote_keys[gate_index].write().insert_hashed(
            key_hash,
            key,
            RemoteKeyMeta {
                primary_shard,
                expire_at_ms,
                generation,
                handoff_complete: true,
                protected,
            },
        );
        true
    } else {
        if let Some(value) = retry_value {
            pending_keys[gate_index].write().insert_hashed(
                key_hash,
                key,
                PendingKeyMeta {
                    primary_shard,
                    generation,
                    queued: false,
                    tracks_key: true,
                    mutation: PendingMutation::Put {
                        value: Some(value),
                        governance: retry_governance,
                        expire_at_ms,
                    },
                },
            );
        }
        false
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_delete_completion(
    inner: &EmbeddedStore,
    remote_keys: &[RwLock<MetadataMap<RemoteKeyMeta>>],
    pending_keys: &[RwLock<MetadataMap<PendingKeyMeta>>],
    key_gates: &[Mutex<()>],
    metadata_budget: &KvOverflowMetadataBudget,
    key: SharedBytes,
    key_hash: u64,
    generation: u64,
    succeeded: bool,
) {
    let gate_index = key_gate_index_for_hash(key_hash, key_gates.len());
    let _key_gate = key_gates[gate_index].lock();
    let is_current = pending_keys[gate_index]
        .read()
        .get_hashed(key_hash, key.as_ref())
        .is_some_and(|meta| meta.generation == generation);
    if !is_current {
        return;
    }
    if succeeded {
        let tracks_key = pending_keys[gate_index]
            .write()
            .remove_hashed(key_hash, key.as_ref())
            .is_some_and(|meta| meta.tracks_key);
        remote_keys[gate_index]
            .write()
            .remove_hashed(key_hash, key.as_ref());
        inner.delete(&key);
        if tracks_key {
            metadata_budget.release(key.len());
        }
        return;
    }
    let mut pending = pending_keys[gate_index].write();
    let retry = pending
        .get_hashed(key_hash, key.as_ref())
        .is_some_and(|meta| {
            matches!(
                meta.mutation,
                PendingMutation::Delete {
                    retry_on_failure: true
                }
            )
        });
    if retry {
        if let Some(meta) = pending.get_mut_hashed(key_hash, key.as_ref()) {
            meta.queued = false;
        }
    } else {
        pending.remove_hashed(key_hash, key.as_ref());
    }
}

fn enforce_memory_target(
    inner: &EmbeddedStore,
    cluster: &KvOverflowCluster,
    options: &KvOverflowOptions,
    remote_keys: &[RwLock<MetadataMap<RemoteKeyMeta>>],
    primary_shard: usize,
    maintenance: &Mutex<()>,
) {
    let _maintenance = maintenance.lock();
    let shard_count = inner.shard_count();
    let shard_target = shard_memory_target(options, shard_count, primary_shard);
    while inner.stored_bytes_in_shard(primary_shard) > shard_target {
        let remote_keys = remote_keys.iter().map(RwLock::read).collect::<Vec<_>>();
        let now_ms = now_millis();
        let mut evicted = false;
        for _ in 0..KV_OVERFLOW_EVICTION_BATCH_PER_SHARD {
            let victim =
                inner.evict_one_point_in_shard_if(primary_shard, options.eviction_policy, |key| {
                    let metadata_index = key_gate_index(key, remote_keys.len());
                    remote_keys[metadata_index].get(key).is_some_and(|meta| {
                        meta.primary_shard == primary_shard
                            && meta.expire_at_ms.is_none_or(|expiry| expiry > now_ms)
                    })
                });
            if victim.is_none() {
                break;
            }
            cluster.metrics.offloads.fetch_add(1, Ordering::Relaxed);
            evicted = true;
            if inner.stored_bytes_in_shard(primary_shard) <= shard_target {
                break;
            }
        }
        if !evicted {
            break;
        }
    }
}

#[inline]
fn shard_memory_target(
    options: &KvOverflowOptions,
    shard_count: usize,
    primary_shard: usize,
) -> usize {
    let base_target = options.max_memory_bytes / shard_count;
    let remainder = options.max_memory_bytes % shard_count;
    base_target + usize::from(primary_shard < remainder)
}

fn key_gate_index(key: &[u8], count: usize) -> usize {
    key_gate_index_for_hash(xxh3_64(key), count)
}

#[inline]
fn key_gate_index_for_hash(key_hash: u64, count: usize) -> usize {
    debug_assert!(count > 0);
    (key_hash as usize) % count
}

fn metadata_len<V>(shards: &[RwLock<MetadataMap<V>>]) -> usize {
    shards.iter().map(|shard| shard.read().len()).sum()
}

impl KvOverflowCleanupTask {
    #[allow(clippy::too_many_arguments)]
    fn start(
        workers: &KvOverflowWorkerPool,
        remote_keys: RemoteKeyShards,
        pending_keys: PendingKeyShards,
        key_gates: Arc<[Mutex<()>]>,
        inner: Arc<EmbeddedStore>,
        cluster: Arc<KvOverflowCluster>,
        maintenance: Arc<[Mutex<()>]>,
        options: KvOverflowOptions,
        admission: Arc<[RwLock<()>]>,
        sequence: Arc<[KvOverflowSequence]>,
        metadata_budget: Arc<KvOverflowMetadataBudget>,
        interval: Duration,
    ) -> Result<Self> {
        let senders = workers.senders.iter().cloned().collect::<Box<[_]>>();
        let completion_receivers = Arc::clone(&workers.completion_receivers);
        let shard_in_flight = workers
            .shard_in_flight
            .iter()
            .cloned()
            .collect::<Box<[_]>>();
        let capacity_per_shard = workers.capacity_per_shard;
        let drains_per_shard = workers.drains_per_shard;
        let metrics = Arc::clone(&workers.metrics);
        let (shutdown, receiver) = bounded(1);
        let join = std::thread::Builder::new()
            .name("shardmap-kv-overflow-cleanup".into())
            .spawn(move || {
                loop {
                    match receiver.recv_timeout(interval) {
                        Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                    let dirty = pending_keys
                        .iter()
                        .flat_map(|shard| {
                            shard
                                .read()
                                .iter()
                                .filter(|(_, meta)| !meta.queued)
                                .take(KV_OVERFLOW_CLEANUP_BATCH_PER_STRIPE)
                                .map(|(key, meta)| (key.clone(), meta.clone()))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    for (key, candidate) in dirty {
                        let _ = enqueue_pending_retry(
                            key,
                            candidate,
                            &senders,
                            &shard_in_flight,
                            capacity_per_shard,
                            drains_per_shard,
                            &metrics,
                            &pending_keys,
                            &key_gates,
                            &admission,
                        );
                    }
                    let now_ms = now_millis();
                    let expired = remote_keys
                        .iter()
                        .flat_map(|shard| {
                            shard
                                .read()
                                .iter()
                                .filter(|(_key, meta)| {
                                    meta.expire_at_ms.is_some_and(|expiry| expiry <= now_ms)
                                })
                                .take(KV_OVERFLOW_CLEANUP_BATCH_PER_STRIPE)
                                .map(|(key, meta)| (key.clone(), *meta))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    for (key, expected) in expired {
                        let key_hash = xxh3_64(&key);
                        let primary_shard = expected.primary_shard;
                        let lane = lane_for_hash(primary_shard, drains_per_shard, key_hash);
                        let in_flight = &shard_in_flight[primary_shard];
                        let _admission = admission[lane].read();
                        let gate_index = key_gate_index_for_hash(key_hash, key_gates.len());
                        let _key_gate = key_gates[gate_index].lock();
                        let still_expired = remote_keys[gate_index]
                            .read()
                            .get(key.as_ref())
                            .is_some_and(|meta| *meta == expected);
                        if !still_expired {
                            continue;
                        }
                        let existing_generation = pending_keys[gate_index]
                            .read()
                            .get(key.as_ref())
                            .and_then(|meta| {
                                (!meta.queued
                                    && matches!(
                                        meta.mutation,
                                        PendingMutation::Delete {
                                            retry_on_failure: true
                                        }
                                    ))
                                .then_some(meta.generation)
                            });
                        if pending_keys[gate_index].read().contains_key(key.as_ref())
                            && existing_generation.is_none()
                        {
                            continue;
                        }
                        if reserve_in_flight(
                            in_flight,
                            capacity_per_shard,
                            &metrics.shard(primary_shard).primary.enqueue_failures,
                        )
                        .is_err()
                        {
                            continue;
                        }
                        let generation = existing_generation.unwrap_or_else(|| {
                            sequence[primary_shard].0.fetch_add(1, Ordering::Relaxed)
                        });
                        if let Some(meta) = pending_keys[gate_index].write().get_mut(key.as_ref()) {
                            meta.queued = true;
                        } else {
                            pending_keys[gate_index].write().insert(
                                key.clone(),
                                PendingKeyMeta {
                                    primary_shard,
                                    generation,
                                    queued: true,
                                    tracks_key: true,
                                    mutation: PendingMutation::Delete {
                                        retry_on_failure: true,
                                    },
                                },
                            );
                        }
                        if senders[lane]
                            .try_send(KvOverflowJob::Delete {
                                primary_shard,
                                key: key.clone(),
                                key_hash,
                                generation,
                                completion: None,
                            })
                            .is_err()
                        {
                            in_flight.fetch_sub(1, Ordering::Release);
                            if let Some(meta) = pending_keys[gate_index]
                                .write()
                                .get_mut(key.as_ref())
                                .filter(|meta| meta.generation == generation)
                            {
                                meta.queued = false;
                            }
                        }
                    }
                    for (primary_shard, completions) in completion_receivers.iter().enumerate() {
                        if drain_completion_queue(
                            completions,
                            &shard_in_flight[primary_shard],
                            &inner,
                            &remote_keys,
                            &pending_keys,
                            &key_gates,
                            &metadata_budget,
                            usize::MAX,
                        ) {
                            enforce_memory_target(
                                &inner,
                                &cluster,
                                &options,
                                &remote_keys,
                                primary_shard,
                                &maintenance[primary_shard],
                            );
                        }
                    }
                }
            })
            .map_err(|error| {
                ShardCacheError::Config(format!(
                    "failed to start key-value overflow cleanup: {error}"
                ))
            })?;
        Ok(Self {
            shutdown,
            join: Mutex::new(Some(join)),
        })
    }

    fn shutdown(&self) {
        let _ = self.shutdown.try_send(());
        if let Some(join) = self.join.lock().take()
            && join.thread().id() != thread::current().id()
        {
            let _ = join.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::SnapshotRepository;
    use crossbeam_channel::Receiver;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::AtomicBool;

    #[derive(Debug)]
    struct MemoryNode {
        id: String,
        replica_id: String,
        remote_shard: usize,
        values: RwLock<HashMap<Bytes, KvOverflowValue>>,
        fail_puts: AtomicBool,
        fail_gets: AtomicBool,
        fail_deletes: AtomicBool,
    }

    impl MemoryNode {
        fn new(id: &str) -> Self {
            Self {
                id: id.into(),
                replica_id: id.into(),
                remote_shard: 0,
                values: RwLock::new(HashMap::new()),
                fail_puts: AtomicBool::new(false),
                fail_gets: AtomicBool::new(false),
                fail_deletes: AtomicBool::new(false),
            }
        }

        fn target(replica_id: &str, remote_shard: usize) -> Self {
            let mut node = Self::new(&format!("{replica_id}#{remote_shard}"));
            node.replica_id = replica_id.into();
            node.remote_shard = remote_shard;
            node
        }
    }

    impl KvOverflowNode for MemoryNode {
        fn id(&self) -> &str {
            &self.id
        }

        fn replica_id(&self) -> &str {
            &self.replica_id
        }

        fn remote_shard(&self) -> usize {
            self.remote_shard
        }

        fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
            if self.fail_puts.load(Ordering::Relaxed) {
                return Err(ShardCacheError::Protocol("injected put failure".into()));
            }
            self.values.write().insert(
                key.to_vec(),
                KvOverflowValue {
                    value: SharedBytes::copy_from_slice(value),
                    ttl_ms,
                    governance: None,
                },
            );
            Ok(())
        }

        fn put_until_with_governance_on_shard(
            &self,
            _primary_shard: usize,
            key: &[u8],
            value: &[u8],
            governance: Option<&[u8]>,
            expire_at_ms: Option<u64>,
        ) -> Result<()> {
            if self.fail_puts.load(Ordering::Relaxed) {
                return Err(ShardCacheError::Protocol("injected put failure".into()));
            }
            self.values.write().insert(
                key.to_vec(),
                KvOverflowValue {
                    value: SharedBytes::copy_from_slice(value),
                    ttl_ms: expire_at_ms.map(|deadline| deadline.saturating_sub(now_millis())),
                    governance: governance.map(SharedBytes::copy_from_slice),
                },
            );
            Ok(())
        }

        fn put_until_with_governance(
            &self,
            key: &[u8],
            value: &[u8],
            governance: Option<&[u8]>,
            expire_at_ms: Option<u64>,
        ) -> Result<()> {
            self.put_until_with_governance_on_shard(0, key, value, governance, expire_at_ms)
        }

        fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
            if self.fail_gets.load(Ordering::Relaxed) {
                return Err(ShardCacheError::Protocol("injected get failure".into()));
            }
            Ok(self.values.read().get(key).cloned())
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            if self.fail_deletes.load(Ordering::Relaxed) {
                return Err(ShardCacheError::Protocol("injected delete failure".into()));
            }
            self.values.write().remove(key);
            Ok(())
        }
    }

    struct DirectWireNode {
        id: String,
        target: ScnpDirectTarget,
    }

    impl KvOverflowNode for DirectWireNode {
        fn id(&self) -> &str {
            &self.id
        }

        fn direct_scnp_target(&self) -> Option<ScnpDirectTarget> {
            Some(self.target.clone())
        }

        fn put(&self, _key: &[u8], _value: &[u8], _ttl_ms: Option<u64>) -> Result<()> {
            Err(ShardCacheError::Protocol(
                "direct wire node used through blocking path".into(),
            ))
        }

        fn get(&self, _key: &[u8]) -> Result<Option<KvOverflowValue>> {
            Ok(None)
        }

        fn delete(&self, _key: &[u8]) -> Result<()> {
            Err(ShardCacheError::Protocol(
                "direct wire node used through blocking path".into(),
            ))
        }
    }

    struct DirectWireServer {
        address: SocketAddr,
        shutdown: Arc<AtomicBool>,
        join: Option<JoinHandle<()>>,
    }

    impl DirectWireServer {
        fn start(response_delay: Duration) -> Self {
            Self::start_with_failure(response_delay, None)
        }

        fn start_with_failure(response_delay: Duration, fail_request_once: Option<usize>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind direct wire server");
            listener
                .set_nonblocking(true)
                .expect("set direct wire listener nonblocking");
            let address = listener.local_addr().expect("direct wire address");
            let shutdown = Arc::new(AtomicBool::new(false));
            let request_count = Arc::new(AtomicUsize::new(0));
            let thread_shutdown = Arc::clone(&shutdown);
            let join = thread::spawn(move || {
                while !thread_shutdown.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let connection_shutdown = Arc::clone(&thread_shutdown);
                            let request_count = Arc::clone(&request_count);
                            thread::spawn(move || {
                                serve_direct_wire_connection(
                                    stream,
                                    response_delay,
                                    fail_request_once,
                                    &request_count,
                                    &connection_shutdown,
                                );
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => break,
                    }
                }
            });
            Self {
                address,
                shutdown,
                join: Some(join),
            }
        }

        fn node(&self, id: &str) -> Arc<dyn KvOverflowNode> {
            Arc::new(DirectWireNode {
                id: id.into(),
                target: ScnpDirectTarget {
                    addresses: vec![self.address].into_boxed_slice(),
                    remote_shard: 0,
                    connect_timeout: Duration::from_secs(1),
                    operation_timeout: Duration::from_secs(2),
                    max_retries: 0,
                    retry_backoff: Duration::from_millis(1),
                    auth_token: None,
                    circuit: Arc::new(CircuitBreaker::new(8, Duration::from_secs(1))),
                    #[cfg(feature = "scnp-tls")]
                    tls: None,
                },
            })
        }
    }

    impl Drop for DirectWireServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Release);
            let _ = std::net::TcpStream::connect(self.address);
            if let Some(join) = self.join.take() {
                join.join().expect("direct wire server thread");
            }
        }
    }

    fn serve_direct_wire_connection(
        mut stream: std::net::TcpStream,
        response_delay: Duration,
        fail_request_once: Option<usize>,
        request_count: &AtomicUsize,
        shutdown: &AtomicBool,
    ) {
        stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("set direct wire read timeout");
        while !shutdown.load(Ordering::Acquire) {
            let mut header = [0u8; 8];
            match stream.read_exact(&mut header) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(_) => break,
            }
            let body_len = u32::from_le_bytes(header[4..8].try_into().expect("wire body length"));
            let mut body = vec![0; body_len as usize];
            if stream.read_exact(&mut body).is_err() {
                break;
            }
            thread::sleep(response_delay);
            let request = request_count.fetch_add(1, Ordering::Relaxed) + 1;
            let response = if fail_request_once == Some(request) {
                let mut response = vec![0xFB, 2, 1, 0, 8, 0, 0, 0];
                response.extend_from_slice(b"injected");
                response
            } else {
                match header[2] {
                    2 => vec![0xFB, 2, 0, 0, 0, 0, 0, 0],
                    5 => {
                        let mut response = vec![0xFB, 2, 3, 0, 8, 0, 0, 0];
                        response.extend_from_slice(&1i64.to_le_bytes());
                        response
                    }
                    opcode => panic!("unexpected direct wire opcode {opcode}"),
                }
            };
            if stream.write_all(&response).is_err() {
                break;
            }
        }
    }

    struct BlockingNode {
        id: String,
        started: Sender<()>,
        release: Receiver<()>,
        values: RwLock<HashMap<Bytes, KvOverflowValue>>,
        block_once: AtomicBool,
    }

    struct BatchingNode {
        id: String,
        started: Sender<()>,
        release: Receiver<()>,
        values: RwLock<HashMap<Bytes, KvOverflowValue>>,
        batch_sizes: Mutex<Vec<usize>>,
        block_once: AtomicBool,
    }

    struct BlockingGetNode {
        id: String,
        values: RwLock<HashMap<Bytes, KvOverflowValue>>,
        block_get: AtomicBool,
        get_calls: AtomicUsize,
        started: Sender<()>,
        release: Receiver<()>,
    }

    struct ShardRecordingNode {
        id: String,
        values: RwLock<HashMap<Bytes, KvOverflowValue>>,
        put_shards: Mutex<Vec<usize>>,
        get_shards: Mutex<Vec<usize>>,
        delete_shards: Mutex<Vec<usize>>,
    }

    impl KvOverflowNode for ShardRecordingNode {
        fn id(&self) -> &str {
            &self.id
        }

        fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
            self.values.write().insert(
                key.to_vec(),
                KvOverflowValue {
                    value: SharedBytes::copy_from_slice(value),
                    ttl_ms,
                    governance: None,
                },
            );
            Ok(())
        }

        fn put_batch_outcomes_until_on_shard(
            &self,
            primary_shard: usize,
            requests: &[KvOverflowPutRequest<'_>],
            outcomes: &mut Vec<bool>,
        ) {
            self.put_shards.lock().push(primary_shard);
            outcomes.clear();
            outcomes.extend(
                requests
                    .iter()
                    .map(|request| self.put(request.key, request.value, None).is_ok()),
            );
        }

        fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
            Ok(self.values.read().get(key).cloned())
        }

        fn get_on_shard(
            &self,
            primary_shard: usize,
            key: &[u8],
        ) -> Result<Option<KvOverflowValue>> {
            self.get_shards.lock().push(primary_shard);
            self.get(key)
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            self.values.write().remove(key);
            Ok(())
        }

        fn delete_on_shard(&self, primary_shard: usize, key: &[u8]) -> Result<()> {
            self.delete_shards.lock().push(primary_shard);
            self.delete(key)
        }
    }

    impl KvOverflowNode for BlockingGetNode {
        fn id(&self) -> &str {
            &self.id
        }

        fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
            self.values.write().insert(
                key.to_vec(),
                KvOverflowValue {
                    value: SharedBytes::copy_from_slice(value),
                    ttl_ms,
                    governance: None,
                },
            );
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            if self.block_get.swap(false, Ordering::Relaxed) {
                let _ = self.started.send(());
                self.release
                    .recv()
                    .map_err(|_| ShardCacheError::ChannelClosed("blocking get test node"))?;
            }
            Ok(self.values.read().get(key).cloned())
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            self.values.write().remove(key);
            Ok(())
        }
    }

    struct PartialBatchNode {
        id: String,
        values: RwLock<HashMap<Bytes, KvOverflowValue>>,
        fail_batch_once: AtomicBool,
    }

    impl KvOverflowNode for PartialBatchNode {
        fn id(&self) -> &str {
            &self.id
        }

        fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
            self.values.write().insert(
                key.to_vec(),
                KvOverflowValue {
                    value: SharedBytes::copy_from_slice(value),
                    ttl_ms,
                    governance: None,
                },
            );
            Ok(())
        }

        fn put_batch_until(&self, requests: &[KvOverflowPutRequest<'_>]) -> Result<()> {
            if self.fail_batch_once.swap(false, Ordering::Relaxed) {
                if let Some(request) = requests.first() {
                    self.put(request.key, request.value, None)?;
                }
                return Err(ShardCacheError::Protocol(
                    "injected partial batch failure".into(),
                ));
            }
            for request in requests {
                self.put(request.key, request.value, None)?;
            }
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
            Ok(self.values.read().get(key).cloned())
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            self.values.write().remove(key);
            Ok(())
        }
    }

    impl KvOverflowNode for BatchingNode {
        fn id(&self) -> &str {
            &self.id
        }

        fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
            self.values.write().insert(
                key.to_vec(),
                KvOverflowValue {
                    value: SharedBytes::copy_from_slice(value),
                    ttl_ms,
                    governance: None,
                },
            );
            Ok(())
        }

        fn put_batch_until(&self, requests: &[KvOverflowPutRequest<'_>]) -> Result<()> {
            if self.block_once.swap(false, Ordering::Relaxed) {
                let _ = self.started.send(());
                self.release
                    .recv()
                    .map_err(|_| ShardCacheError::ChannelClosed("batching test node"))?;
            }
            self.batch_sizes.lock().push(requests.len());
            for request in requests {
                let ttl_ms = request
                    .expire_at_ms
                    .map(|deadline| deadline.saturating_sub(now_millis()));
                self.put(request.key, request.value, ttl_ms)?;
            }
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
            Ok(self.values.read().get(key).cloned())
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            self.values.write().remove(key);
            Ok(())
        }
    }

    impl KvOverflowNode for BlockingNode {
        fn id(&self) -> &str {
            &self.id
        }

        fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
            if self.block_once.swap(false, Ordering::Relaxed) {
                let _ = self.started.send(());
                self.release
                    .recv()
                    .map_err(|_| ShardCacheError::ChannelClosed("blocking test node"))?;
            }
            self.values.write().insert(
                key.to_vec(),
                KvOverflowValue {
                    value: SharedBytes::copy_from_slice(value),
                    ttl_ms,
                    governance: None,
                },
            );
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
            Ok(self.values.read().get(key).cloned())
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            self.values.write().remove(key);
            Ok(())
        }
    }

    fn blocking_node() -> (Arc<BlockingNode>, Receiver<()>, Sender<()>) {
        let (started, started_rx) = bounded(1);
        let (release, release_rx) = bounded(1);
        (
            Arc::new(BlockingNode {
                id: "blocking-node".into(),
                started,
                release: release_rx,
                values: RwLock::new(HashMap::new()),
                block_once: AtomicBool::new(true),
            }),
            started_rx,
            release,
        )
    }

    fn options(max_memory_bytes: usize) -> KvOverflowOptions {
        KvOverflowOptions {
            max_memory_bytes,
            max_metadata_bytes: usize::MAX,
            max_key_bytes: usize::MAX - KV_OVERFLOW_METADATA_ENTRY_OVERHEAD,
            eviction_policy: EvictionPolicy::Lfu,
            fetch_on_miss: true,
            cleanup_interval: Duration::from_secs(60),
            queue_capacity: 64,
            pipeline_max_items: 64,
            pipeline_max_bytes: 256 * 1024,
            pipeline_flush: Duration::ZERO,
            max_inflight_per_target: 1,
            forget_remote_misses: false,
            adaptive_pipeline: true,
            pipeline_target: Duration::from_millis(1),
            circuit_breaker_failure_threshold: 8,
            circuit_breaker_cooldown: Duration::from_secs(1),
            compression: KvOverflowCompression::None,
            compression_min_value_bytes: 4 * 1024,
            compression_min_savings_percent: 12,
            max_value_bytes: 64 * 1024 * 1024,
            compression_max_expansion_ratio: 256,
            handoff_max_bytes_per_second: 0,
            handoff_max_concurrency: 16,
            handoff_batch_items: 1_024,
            retained_buffer_bytes: 16 * 1024,
        }
    }

    #[test]
    fn lz4_overflow_envelope_round_trips_and_preserves_v1_compatibility() {
        let value = vec![b'x'; 32 * 1024];
        let encoded =
            encode_overflow_value_until(&value, None, None, KvOverflowCompression::Lz4, 1024, 12);
        assert_eq!(
            &encoded[..KV_OVERFLOW_COMPRESSED_MAGIC.len()],
            KV_OVERFLOW_COMPRESSED_MAGIC
        );
        assert!(encoded.len() < value.len());
        assert_eq!(
            decode_overflow_value(&encoded, 64 * 1024 * 1024, 256)
                .unwrap()
                .unwrap()
                .value
                .as_ref(),
            value
        );

        let legacy = encode_overflow_value_until(
            b"legacy",
            None,
            None,
            KvOverflowCompression::None,
            1024,
            12,
        );
        assert_eq!(&legacy[..KV_OVERFLOW_MAGIC.len()], KV_OVERFLOW_MAGIC);
        assert_eq!(
            decode_overflow_value(&legacy, 64 * 1024 * 1024, 256)
                .unwrap()
                .unwrap()
                .value
                .as_ref(),
            b"legacy"
        );
    }

    #[test]
    fn governed_overflow_envelopes_preserve_metadata_and_integrity() {
        let value = vec![b'x'; 32 * 1024];
        let governance = b"tenant-a/repo-private";
        for compression in [KvOverflowCompression::None, KvOverflowCompression::Lz4] {
            let encoded =
                encode_overflow_value_until(&value, Some(governance), None, compression, 1024, 1);
            let decoded = decode_overflow_value(&encoded, 64 * 1024 * 1024, 256)
                .unwrap()
                .unwrap();
            assert_eq!(decoded.value.as_ref(), value);
            assert_eq!(decoded.governance.as_deref(), Some(governance.as_slice()));

            let mut corrupted = encoded;
            *corrupted.last_mut().unwrap() ^= 1;
            assert!(decode_overflow_value(&corrupted, 64 * 1024 * 1024, 256).is_err());
        }

        let empty = encode_overflow_value_until(
            b"value",
            Some(b""),
            None,
            KvOverflowCompression::None,
            1024,
            1,
        );
        assert_eq!(
            decode_overflow_value(&empty, 1024, 256)
                .unwrap()
                .unwrap()
                .governance,
            Some(SharedBytes::new())
        );
    }

    #[test]
    fn compressed_overflow_envelope_rejects_corruption() {
        let mut encoded = encode_overflow_value_until(
            &[7; 16 * 1024],
            None,
            None,
            KvOverflowCompression::Lz4,
            1024,
            1,
        );
        *encoded.last_mut().unwrap() ^= 0xff;
        assert!(decode_overflow_value(&encoded, 64 * 1024 * 1024, 256).is_err());
    }

    #[test]
    fn compressed_overflow_envelope_rejects_unbounded_lengths_before_allocation() {
        let mut encoded = encode_overflow_value_until(
            &[7; 16 * 1024],
            None,
            None,
            KvOverflowCompression::Lz4,
            1024,
            1,
        );
        encoded[16..24].copy_from_slice(&(128 * 1024 * 1024u64).to_le_bytes());
        assert_eq!(
            decode_overflow_value(&encoded, 64 * 1024 * 1024, 256),
            Err("key-value overflow payload exceeds configured maximum")
        );

        encoded[16..24].copy_from_slice(&(1024 * 1024u64).to_le_bytes());
        assert_eq!(
            decode_overflow_value(&encoded, 2 * 1024 * 1024, 2),
            Err("key-value overflow payload exceeds configured expansion ratio")
        );
    }

    #[test]
    fn circuit_breaker_opens_and_allows_a_half_open_probe() {
        let breaker = CircuitBreaker::new(2, Duration::from_millis(5));
        assert!(breaker.allow_request());
        breaker.record_failure();
        assert!(breaker.allow_request());
        breaker.record_failure();
        assert!(!breaker.allow_request());
        thread::sleep(Duration::from_millis(8));
        assert!(breaker.allow_request());
        breaker.record_success();
        assert!(breaker.allow_request());
    }

    #[test]
    fn circuit_breaker_allows_only_one_concurrent_half_open_probe() {
        let breaker = Arc::new(CircuitBreaker::new(1, Duration::from_millis(5)));
        breaker.record_failure();
        thread::sleep(Duration::from_millis(8));
        let allowed = (0..16)
            .map(|_| {
                let breaker = Arc::clone(&breaker);
                thread::spawn(move || usize::from(breaker.allow_request()))
            })
            .map(|thread| thread.join().unwrap())
            .sum::<usize>();
        assert_eq!(allowed, 1);
    }

    #[test]
    fn adaptive_pipeline_reduces_slow_batches_and_recovers() {
        let mut options = options(1024);
        options.pipeline_max_items = 64;
        options.pipeline_target = Duration::from_millis(1);
        let mut adaptive = AdaptivePipeline::new(&options);
        adaptive.record(Duration::from_millis(3), true, 1024, 256 * 1024);
        assert_eq!(adaptive.item_limit(), 32);
        for _ in 0..4 {
            adaptive.record(Duration::from_micros(500), true, 1024, 256 * 1024);
        }
        assert_eq!(adaptive.item_limit(), 40);
        adaptive.record(Duration::from_micros(500), false, 1024, 256 * 1024);
        assert_eq!(adaptive.item_limit(), 20);
    }

    fn keys_for_primary_shards(store: &KvOverflowStore, keys_per_shard: usize) -> Vec<Vec<Bytes>> {
        let mut keys = (0..store.inner.shard_count())
            .map(|_| Vec::with_capacity(keys_per_shard))
            .collect::<Vec<_>>();
        for index in 0..1_000_000 {
            let key = format!("primary-shard-key-{index}").into_bytes();
            let key_hash = xxh3_64(&key);
            let primary_shard = store.inner.route_key_prehashed(key_hash, &key).shard_id;
            if keys[primary_shard].len() < keys_per_shard {
                keys[primary_shard].push(key);
                if keys.iter().all(|shard| shard.len() == keys_per_shard) {
                    return keys;
                }
            }
        }
        panic!("failed to find keys for every primary shard");
    }

    #[test]
    fn public_constructor_rejects_invalid_worker_options() {
        for invalid in [
            KvOverflowOptions {
                queue_capacity: 0,
                ..options(1024)
            },
            KvOverflowOptions {
                pipeline_max_items: 0,
                ..options(1024)
            },
            KvOverflowOptions {
                pipeline_max_bytes: 0,
                ..options(1024)
            },
            KvOverflowOptions {
                max_inflight_per_target: 0,
                ..options(1024)
            },
            KvOverflowOptions {
                cleanup_interval: Duration::ZERO,
                ..options(1024)
            },
        ] {
            let node = Arc::new(MemoryNode::new("node-a"));
            let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
            assert!(matches!(
                KvOverflowStore::new(EmbeddedStore::new(1), cluster, invalid),
                Err(ShardCacheError::Config(_))
            ));
        }

        let config = KvOverflowConfig {
            cleanup_interval_ms: 0,
            ..KvOverflowConfig::default()
        };
        assert!(matches!(
            KvOverflowOptions::try_from(&config),
            Err(ShardCacheError::Config(_))
        ));

        let config = KvOverflowConfig {
            max_memory_bytes: 1024,
            queue_capacity_per_shard: 1,
            worker_threads: 0,
            queue_capacity: 0,
            ..KvOverflowConfig::default()
        };
        assert_eq!(
            KvOverflowOptions::try_from(&config)
                .expect("legacy worker settings are ignored")
                .queue_capacity,
            1
        );
    }

    #[test]
    fn ambiguous_batch_failure_is_resolved_per_item() {
        let node = Arc::new(PartialBatchNode {
            id: "node-a".into(),
            values: RwLock::new(HashMap::new()),
            fail_batch_once: AtomicBool::new(true),
        });
        let cluster = KvOverflowCluster::new(vec![node.clone()]).unwrap();
        let requests = [
            KvOverflowPutRequest {
                key: b"first",
                value: b"one",
                expire_at_ms: None,
                governance: None,
            },
            KvOverflowPutRequest {
                key: b"second",
                value: b"two",
                expire_at_ms: None,
                governance: None,
            },
        ];
        let mut outcomes = Vec::new();
        let key_hashes = requests
            .iter()
            .map(|request| xxh3_64(request.key))
            .collect::<Vec<_>>();
        let mut by_owner = Vec::new();

        cluster.put_batch_until(
            0,
            Some(0),
            &key_hashes,
            &requests,
            &mut outcomes,
            &mut by_owner,
        );

        assert_eq!(outcomes, [true, true]);
        assert_eq!(node.values.read().len(), 2);
    }

    #[test]
    fn flush_drains_every_lane_before_reporting_failure() {
        let failing = Arc::new(MemoryNode::new("a-failing"));
        failing.fail_puts.store(true, Ordering::Relaxed);
        let (blocking, started, release) = blocking_node();
        let cluster = Arc::new(
            KvOverflowCluster::new(vec![failing, blocking as Arc<dyn KvOverflowNode>]).unwrap(),
        );
        let parallel = options(usize::MAX);
        let store = Arc::new(
            KvOverflowStore::new(EmbeddedStore::new(2), cluster.clone(), parallel).unwrap(),
        );
        let routed_cluster = store.cluster();
        let failing_key = (0..10_000)
            .map(|index| format!("failing-{index}").into_bytes())
            .find(|key| routed_cluster.owner_id(key) == "a-failing")
            .unwrap();
        let blocking_key = (0..10_000)
            .map(|index| format!("blocking-{index}").into_bytes())
            .find(|key| routed_cluster.owner_id(key) == "blocking-node")
            .unwrap();

        store.set(blocking_key, b"value".to_vec(), None).unwrap();
        started.recv_timeout(Duration::from_secs(1)).unwrap();
        store.set(failing_key, b"value".to_vec(), None).unwrap();
        let (done, result) = bounded(1);
        let flush_store = Arc::clone(&store);
        let flush = thread::spawn(move || done.send(flush_store.flush_remote()).unwrap());

        assert!(result.recv_timeout(Duration::from_millis(50)).is_err());
        release.send(()).unwrap();
        assert!(
            result
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .is_err()
        );
        flush.join().unwrap();
    }

    #[test]
    fn failed_put_remains_pending_until_a_retry_succeeds() {
        let node = Arc::new(MemoryNode::new("node-a"));
        node.fail_puts.store(true, Ordering::Relaxed);
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1)).unwrap();

        store.set(b"key".to_vec(), b"new".to_vec(), None).unwrap();
        assert!(store.flush_remote().is_err());
        assert!(store.flush_remote().is_err());
        assert_eq!(store.health_snapshot().pending_keys, 1);
        assert_eq!(store.inner().get(b"key"), Some(b"new".to_vec()));

        node.fail_puts.store(false, Ordering::Relaxed);
        store.flush_remote().unwrap();
        assert_eq!(store.health_snapshot().pending_keys, 0);
        assert_eq!(node.values.read()[b"key".as_slice()].value.as_ref(), b"new");
    }

    #[test]
    fn maintenance_retries_failed_puts_without_an_explicit_flush() {
        let node = Arc::new(MemoryNode::new("node-a"));
        node.fail_puts.store(true, Ordering::Relaxed);
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut retrying = options(1024);
        retrying.cleanup_interval = Duration::from_millis(10);
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, retrying).unwrap();

        store.set(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while store.health_snapshot().failed_pending_keys == 0
            && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(store.health_snapshot().failed_pending_keys, 1);

        node.fail_puts.store(false, Ordering::Relaxed);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while store.health_snapshot().pending_keys != 0 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(store.health_snapshot().pending_keys, 0);
        assert_eq!(
            node.values.read()[b"key".as_slice()].value.as_ref(),
            b"value"
        );
    }

    #[test]
    fn fault_in_does_not_hold_a_striped_key_gate_during_network_io() {
        let (started, started_rx) = bounded(1);
        let (release, release_rx) = bounded(1);
        let node = Arc::new(BlockingGetNode {
            id: "node-a".into(),
            values: RwLock::new(HashMap::new()),
            block_get: AtomicBool::new(false),
            get_calls: AtomicUsize::new(0),
            started,
            release: release_rx,
        });
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let store =
            Arc::new(KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1024)).unwrap());
        let remote_key = b"remote-key";
        node.put(remote_key, &[1; 32], None).unwrap();
        let remote_hash = xxh3_64(remote_key);
        let remote_index = key_gate_index_for_hash(remote_hash, store.remote_keys.len());
        store.remote_keys[remote_index].write().insert(
            SharedBytes::from_static(remote_key),
            RemoteKeyMeta {
                primary_shard: 0,
                expire_at_ms: None,
                generation: 1,
                handoff_complete: true,
                protected: false,
            },
        );
        let gate = key_gate_index(remote_key, store.key_gates.len());
        let colliding_key = (0..100_000)
            .map(|index| format!("collision-{index}").into_bytes())
            .find(|key| key_gate_index(key, store.key_gates.len()) == gate)
            .unwrap();

        node.block_get.store(true, Ordering::Relaxed);
        let get_store = Arc::clone(&store);
        let get = thread::spawn(move || get_store.get(remote_key));
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let second_get_store = Arc::clone(&store);
        let second_get = thread::spawn(move || second_get_store.get(remote_key));
        let (set_done, set_result) = bounded(1);
        let set_store = Arc::clone(&store);
        let set = thread::spawn(move || {
            set_done
                .send(set_store.set(colliding_key, b"value".to_vec(), None))
                .unwrap();
        });

        set_result
            .recv_timeout(Duration::from_millis(100))
            .expect("colliding set should not wait for remote get")
            .unwrap();
        release.send(()).unwrap();
        assert_eq!(get.join().unwrap().unwrap(), Some(vec![1; 32]));
        assert_eq!(second_get.join().unwrap().unwrap(), Some(vec![1; 32]));
        assert_eq!(node.get_calls.load(Ordering::Relaxed), 1);
        set.join().unwrap();
    }

    #[test]
    fn snapshot_remote_io_does_not_block_unrelated_key_stripes() {
        let (started, started_rx) = bounded(1);
        let (release, release_rx) = bounded(1);
        let node = Arc::new(BlockingGetNode {
            id: "node-a".into(),
            values: RwLock::new(HashMap::new()),
            block_get: AtomicBool::new(false),
            get_calls: AtomicUsize::new(0),
            started,
            release: release_rx,
        });
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let store =
            Arc::new(KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1)).unwrap());
        store
            .set(b"remote".to_vec(), b"value".to_vec(), None)
            .unwrap();
        store.flush_remote().unwrap();
        assert!(!store.inner().exists(b"remote"));
        let remote_gate = key_gate_index(b"remote", store.key_gates.len());
        let unrelated = (0..100_000)
            .map(|index| format!("snapshot-write-{index}").into_bytes())
            .find(|key| key_gate_index(key, store.key_gates.len()) != remote_gate)
            .unwrap();

        node.block_get.store(true, Ordering::Relaxed);
        let snapshot_store = Arc::clone(&store);
        let snapshot = thread::spawn(move || snapshot_store.try_entry_snapshot());
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        store.set(unrelated, b"new".to_vec(), None).unwrap();
        release.send(()).unwrap();
        assert!(snapshot.join().unwrap().is_ok());
    }

    #[test]
    fn exact_slot_ownership_is_stable_across_node_order() {
        let first = Arc::new(MemoryNode::new("node-a"));
        let second = Arc::new(MemoryNode::new("node-b"));
        let cluster_a = KvOverflowCluster::new(vec![first.clone(), second.clone()]).unwrap();
        let cluster_b = KvOverflowCluster::new(vec![second, first]).unwrap();
        for index in 0..1_000 {
            let key = format!("key-{index}");
            assert_eq!(
                cluster_a.owner_id(key.as_bytes()),
                cluster_b.owner_id(key.as_bytes())
            );
        }
    }

    #[test]
    fn oversized_slot_table_is_rejected_before_allocation() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let result = KvOverflowCluster::with_previous(
            vec![node],
            Vec::new(),
            MAX_KV_OVERFLOW_SLOT_COUNT * 2,
        );
        assert!(matches!(result, Err(ShardCacheError::Config(_))));
    }

    #[test]
    fn exact_horizontal_rebalance_moves_only_complete_logical_slots() {
        let first = Arc::new(MemoryNode::new("node-a"));
        let second = Arc::new(MemoryNode::new("node-b"));
        let added = Arc::new(MemoryNode::new("node-c"));
        let original = KvOverflowCluster::new(vec![first.clone(), second.clone()]).unwrap();
        let expanded = KvOverflowCluster::with_previous(
            vec![first.clone(), second.clone(), added],
            vec![first, second],
            DEFAULT_KV_OVERFLOW_SLOT_COUNT,
        )
        .unwrap();

        let mut moved_slots = 0;
        for slot in 0..DEFAULT_KV_OVERFLOW_SLOT_COUNT {
            let old_owner = original.slot_owner_id(slot).unwrap();
            let new_owner = expanded.slot_owner_id(slot).unwrap();
            if old_owner != new_owner {
                moved_slots += 1;
                assert!(matches!(new_owner, "node-a" | "node-b" | "node-c"));
            }
        }
        assert!(moved_slots > 0);

        for index in 0..1_000 {
            let key = format!("key-{index}");
            assert_eq!(
                original.slot_for_key(key.as_bytes()),
                expanded.slot_for_key(key.as_bytes())
            );
        }
    }

    #[test]
    fn worker_lanes_are_dedicated_to_primary_shards() {
        let nodes: Vec<Arc<dyn KvOverflowNode>> = (0..4)
            .map(|index| {
                Arc::new(MemoryNode::new(&format!("node-{index}"))) as Arc<dyn KvOverflowNode>
            })
            .collect::<Vec<_>>();
        let cluster = Arc::new(KvOverflowCluster::new(nodes).unwrap());
        let parallel = options(usize::MAX);
        let store = KvOverflowStore::new(EmbeddedStore::new(4), cluster, parallel).unwrap();
        let mut lane_shards = [None; 4];

        for index in 0..10_000 {
            let key = format!("key-{index}");
            let key_hash = xxh3_64(key.as_bytes());
            let primary_shard = store
                .inner
                .route_key_prehashed(key_hash, key.as_bytes())
                .shard_id;
            let lane = store.workers.lane_for_key(key.as_bytes());
            match lane_shards[lane] {
                Some(existing) => assert_eq!(existing, primary_shard),
                None => lane_shards[lane] = Some(primary_shard),
            }
        }

        assert!(lane_shards.iter().all(Option::is_some));
        for primary_shard in 0..4 {
            assert_eq!(
                lane_shards
                    .iter()
                    .filter(|lane_shard| **lane_shard == Some(primary_shard))
                    .count(),
                1
            );
        }
    }

    #[test]
    fn primary_shards_have_disjoint_slot_ranges_and_replica_sets() {
        let nodes = (0..4)
            .map(|index| {
                Arc::new(MemoryNode::new(&format!("node-{index}"))) as Arc<dyn KvOverflowNode>
            })
            .collect::<Vec<_>>();
        let cluster =
            KvOverflowCluster::with_previous_for_primary_shards(nodes, Vec::new(), 64, 4).unwrap();
        let key_hashes = (0..10_000)
            .map(|index| xxh3_64(format!("range-key-{index}").as_bytes()))
            .collect::<Vec<_>>();

        for primary_shard in 0..4 {
            let range_start = primary_shard as u32 * 16;
            let range_end = range_start + 16;
            let shard_hashes = key_hashes
                .iter()
                .copied()
                .filter(|hash| cluster.primary_shard_for_hash(*hash) == primary_shard)
                .collect::<Vec<_>>();
            assert!(!shard_hashes.is_empty());
            for key_hash in shard_hashes {
                let slot = cluster.slot_for_hash_on_shard(primary_shard, key_hash);
                assert!((range_start..range_end).contains(&slot));
            }
            let owners = (range_start..range_end)
                .map(|slot| cluster.slot_owner_id(slot).unwrap())
                .collect::<HashSet<_>>();
            assert_eq!(owners.len(), 1);
            let expected = format!("node-{primary_shard}");
            assert_eq!(owners.into_iter().next(), Some(expected.as_str()));
        }
    }

    fn topology_targets(
        replica_count: usize,
        shards_per_replica: usize,
    ) -> Vec<Arc<dyn KvOverflowNode>> {
        (0..replica_count)
            .flat_map(|replica| {
                (0..shards_per_replica).map(move |remote_shard| {
                    Arc::new(MemoryNode::target(
                        &format!("replica-{replica:04}"),
                        remote_shard,
                    )) as Arc<dyn KvOverflowNode>
                })
            })
            .collect()
    }

    fn owned_targets(cluster: &KvOverflowCluster, primary_shard: usize) -> HashSet<usize> {
        let start = primary_shard * cluster.slots_per_primary_shard as usize;
        let end = start + cluster.slots_per_primary_shard as usize;
        cluster.slot_owners[start..end].iter().copied().collect()
    }

    #[test]
    fn logical_slots_remain_stable_when_primary_shards_are_added() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let clusters = [1, 2, 4, 8, 16]
            .into_iter()
            .map(|primary_shards| {
                KvOverflowCluster::with_previous_for_primary_shards(
                    vec![node.clone()],
                    Vec::new(),
                    DEFAULT_KV_OVERFLOW_SLOT_COUNT,
                    primary_shards,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        for index in 0..10_000 {
            let key = format!("stable-slot-{index}");
            let expected = clusters[0].slot_for_key(key.as_bytes());
            for cluster in &clusters[1..] {
                assert_eq!(cluster.slot_for_key(key.as_bytes()), expected);
                assert_eq!(
                    cluster.primary_shard_for_hash(xxh3_64(key.as_bytes())),
                    expected as usize / cluster.slots_per_primary_shard as usize
                );
            }
        }
    }

    #[test]
    fn previous_primary_geometry_finds_owner_after_shard_count_change() {
        let targets = topology_targets(3, 1);
        let old = KvOverflowCluster::with_topology(
            targets.clone(),
            Vec::new(),
            DEFAULT_KV_OVERFLOW_SLOT_COUNT,
            1,
            None,
            Some(Box::from(&b"scale-primary"[..])),
            true,
        )
        .unwrap();
        let scaled = KvOverflowCluster::with_topology(
            targets.clone(),
            targets,
            DEFAULT_KV_OVERFLOW_SLOT_COUNT,
            2,
            Some(1),
            Some(Box::from(&b"scale-primary"[..])),
            true,
        )
        .unwrap();
        let key = (0..100_000)
            .map(|index| format!("primary-scale-{index}"))
            .find(|key| old.owner_id(key.as_bytes()) != scaled.owner_id(key.as_bytes()))
            .expect("changing primary geometry must move a slot for this topology");
        let key_hash = xxh3_64(key.as_bytes());
        let primary_shard = scaled.primary_shard_for_hash(key_hash);

        assert_eq!(
            old.slot_for_key(key.as_bytes()),
            scaled.slot_for_key(key.as_bytes())
        );
        assert!(
            scaled
                .previous_owner_index(primary_shard, key_hash)
                .is_some()
        );
        old.put(key.as_bytes(), b"old-owner-value", None).unwrap();
        assert_eq!(
            scaled.get(key.as_bytes()).unwrap().unwrap().value.as_ref(),
            b"old-owner-value"
        );
    }

    #[test]
    fn one_sixteen_shard_replica_assigns_one_unique_target_per_primary() {
        let cluster = KvOverflowCluster::with_topology(
            topology_targets(1, 16),
            Vec::new(),
            16_384,
            16,
            None,
            Some(Box::from(&b"test"[..])),
            true,
        )
        .unwrap();

        for primary_shard in 0..16 {
            let targets = owned_targets(&cluster, primary_shard);
            assert_eq!(targets, HashSet::from([primary_shard]));
            assert_eq!(cluster.nodes[primary_shard].remote_shard(), primary_shard);
        }
    }

    #[test]
    fn sixteen_replicas_assign_one_complete_replica_per_primary() {
        let cluster = KvOverflowCluster::with_topology(
            topology_targets(16, 16),
            Vec::new(),
            16_384,
            16,
            None,
            Some(Box::from(&b"test"[..])),
            true,
        )
        .unwrap();

        for primary_shard in 0..16 {
            let targets = owned_targets(&cluster, primary_shard);
            assert_eq!(targets.len(), 16);
            assert!(targets.iter().all(|target| {
                cluster.nodes[*target].replica_id() == format!("replica-{primary_shard:04}")
            }));
        }
    }

    #[test]
    fn five_hundred_replicas_are_exclusive_and_balanced_by_complete_node() {
        let cluster = KvOverflowCluster::with_topology(
            topology_targets(500, 16),
            Vec::new(),
            32_768,
            16,
            None,
            Some(Box::from(&b"test"[..])),
            true,
        )
        .unwrap();
        let mut all_targets = HashSet::new();
        let mut replica_counts = Vec::new();
        for primary_shard in 0..16 {
            let targets = owned_targets(&cluster, primary_shard);
            assert!(all_targets.is_disjoint(&targets));
            let replicas = targets
                .iter()
                .map(|target| cluster.nodes[*target].replica_id())
                .collect::<HashSet<_>>();
            assert!(targets.len() == replicas.len() * 16);
            replica_counts.push(replicas.len());
            all_targets.extend(targets);
        }
        assert_eq!(all_targets.len(), 500 * 16);
        assert_eq!(replica_counts.iter().min(), Some(&31));
        assert_eq!(replica_counts.iter().max(), Some(&32));
    }

    #[test]
    fn overflow_storage_key_routes_and_restores_to_encoded_shard() {
        let key = encode_overflow_storage_key(b"cluster-a", 42, 7, b"logical-key");
        let store =
            EmbeddedStore::with_route_mode(16, crate::storage::EmbeddedRouteMode::OverflowSlot);
        let route = store.route_key(&key);
        assert_eq!(route.shard_id, 7);
        store.restore_entries([StoredEntry {
            key: key.clone(),
            value: b"value".to_vec(),
            expire_at_ms: None,
            governance: None,
        }]);
        assert_eq!(store.get(&key).unwrap(), b"value");
    }

    #[test]
    fn saturated_shard_drain_does_not_backpressure_other_shards() {
        let (node, started, release) = blocking_node();
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let mut isolated = options(usize::MAX);
        isolated.queue_capacity = 1;
        let store = KvOverflowStore::new(EmbeddedStore::new(2), cluster, isolated).unwrap();
        let keys = keys_for_primary_shards(&store, 2);

        store.set(keys[0][0].as_slice(), b"blocked", None).unwrap();
        started.recv_timeout(Duration::from_secs(1)).unwrap();
        let saturated_result = store.set(keys[0][1].as_slice(), b"rejected", None);
        let independent_result = store.set(keys[1][0].as_slice(), b"accepted", None);
        release.send(()).unwrap();

        assert!(matches!(
            saturated_result,
            Err(ShardCacheError::Backpressure(_))
        ));
        independent_result.unwrap();
        store.flush_remote().unwrap();
        assert!(!store.inner.exists(&keys[0][1]));
        let health = store.health_snapshot();
        assert_eq!(health.shard_queue_capacities, [1, 1]);
        assert_eq!(health.drains_per_shard, 1);
    }

    #[test]
    fn network_operations_keep_primary_shard_affinity() {
        let node = Arc::new(ShardRecordingNode {
            id: "node-a".into(),
            values: RwLock::new(HashMap::new()),
            put_shards: Mutex::new(Vec::new()),
            get_shards: Mutex::new(Vec::new()),
            delete_shards: Mutex::new(Vec::new()),
        });
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let store =
            KvOverflowStore::new(EmbeddedStore::new(4), cluster, options(usize::MAX)).unwrap();
        let keys = keys_for_primary_shards(&store, 1);

        for key in keys.iter().flatten() {
            store.set(key.as_slice(), b"value", None).unwrap();
        }
        store.flush_remote().unwrap();
        for key in keys.iter().flatten() {
            store.get_remote(key).unwrap();
            store.delete(key).unwrap();
        }

        for recorded in [&node.put_shards, &node.get_shards, &node.delete_shards] {
            let mut shards = recorded.lock().clone();
            shards.sort_unstable();
            shards.dedup();
            assert_eq!(shards, [0, 1, 2, 3]);
        }
    }

    #[test]
    fn previous_membership_fallback_is_read_only_until_ordered_write() {
        let first = Arc::new(MemoryNode::new("node-a"));
        let second = Arc::new(MemoryNode::new("node-b"));
        let added = Arc::new(MemoryNode::new("node-c"));
        let original = KvOverflowCluster::new(vec![first.clone(), second.clone()]).unwrap();
        let expanded = KvOverflowCluster::with_previous(
            vec![first.clone(), second.clone(), added.clone()],
            vec![first.clone(), second.clone()],
            DEFAULT_KV_OVERFLOW_SLOT_COUNT,
        )
        .unwrap();
        let key = (0..100_000)
            .map(|index| format!("moving-key-{index}"))
            .find(|key| expanded.owner_id(key.as_bytes()) == "node-c")
            .expect("expansion must move at least one slot");
        let old_owner = original.owner_id(key.as_bytes()).to_owned();

        original.put(key.as_bytes(), b"value", None).unwrap();
        assert_eq!(expanded.owner_id(key.as_bytes()), "node-c");
        assert_eq!(
            expanded
                .get(key.as_bytes())
                .unwrap()
                .unwrap()
                .value
                .as_ref(),
            b"value"
        );
        let previous = if old_owner == "node-a" { first } else { second };
        assert!(!added.values.read().contains_key(key.as_bytes()));
        assert!(previous.values.read().contains_key(key.as_bytes()));
        assert_eq!(expanded.metrics.handoff_reads.load(Ordering::Relaxed), 1);
        assert_eq!(expanded.metrics.handoff_hits.load(Ordering::Relaxed), 1);

        expanded.put(key.as_bytes(), b"new-value", None).unwrap();
        assert_eq!(
            added.values.read()[key.as_bytes()].value.as_ref(),
            b"new-value"
        );
        assert!(!previous.values.read().contains_key(key.as_bytes()));
    }

    #[test]
    fn remote_only_handoff_copies_verifies_and_deletes_previous_owner() {
        let first = Arc::new(MemoryNode::new("node-a"));
        let second = Arc::new(MemoryNode::new("node-b"));
        let added = Arc::new(MemoryNode::new("node-c"));
        let cluster = Arc::new(
            KvOverflowCluster::with_topology(
                vec![first.clone(), second.clone(), added.clone()],
                vec![first.clone(), second.clone()],
                DEFAULT_KV_OVERFLOW_SLOT_COUNT,
                1,
                None,
                Some(Box::from(&b"handoff-test"[..])),
                false,
            )
            .unwrap(),
        );
        let key = (0..100_000)
            .map(|index| format!("remote-only-{index}").into_bytes())
            .find(|key| cluster.previous_owner_index(0, xxh3_64(key)).is_some())
            .expect("membership change must move at least one slot");
        let key_hash = xxh3_64(&key);
        let previous_index = cluster.previous_owner_index(0, key_hash).unwrap();
        let previous_key = cluster.previous_storage_key_for(previous_index, 0, key_hash, &key);
        cluster.previous_nodes[previous_index]
            .put(&previous_key, b"cold-value", None)
            .unwrap();

        let store = KvOverflowStore::new(
            EmbeddedStore::new(1),
            Arc::clone(&cluster),
            options(usize::MAX),
        )
        .unwrap();
        let metadata_index = key_gate_index(&key, store.remote_keys.len());
        store.remote_keys[metadata_index].write().insert(
            SharedBytes::copy_from_slice(&key),
            RemoteKeyMeta {
                primary_shard: 0,
                expire_at_ms: None,
                generation: 1,
                handoff_complete: false,
                protected: false,
            },
        );

        assert_eq!(store.synchronize_remote_handoff().unwrap(), 1);
        assert_eq!(
            cluster.get(&key).unwrap().unwrap().value.as_ref(),
            b"cold-value"
        );
        assert!(
            cluster.previous_nodes[previous_index]
                .get(&previous_key)
                .unwrap()
                .is_none()
        );
        assert_eq!(store.health_snapshot().handoff_migrated, 1);
        assert_eq!(store.health_snapshot().handoff_pending, 0);
        assert_eq!(store.synchronize_remote_handoff().unwrap(), 0);
        assert_eq!(store.health_snapshot().handoff_migrated, 1);
    }

    #[test]
    fn remote_handoff_does_not_pause_unrelated_primary_admissions() {
        let (started, started_rx) = bounded(1);
        let (release, release_rx) = bounded(1);
        let previous = Arc::new(BlockingGetNode {
            id: "previous".into(),
            values: RwLock::new(HashMap::new()),
            block_get: AtomicBool::new(false),
            get_calls: AtomicUsize::new(0),
            started,
            release: release_rx,
        });
        let current = Arc::new(MemoryNode::new("current"));
        let cluster = Arc::new(
            KvOverflowCluster::with_previous(
                vec![current],
                vec![previous.clone()],
                DEFAULT_KV_OVERFLOW_SLOT_COUNT,
            )
            .unwrap(),
        );
        let store = Arc::new(
            KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(usize::MAX)).unwrap(),
        );
        let remote_key = b"handoff-key";
        previous.put(remote_key, b"value", None).unwrap();
        let key_hash = xxh3_64(remote_key);
        let metadata_index = key_gate_index_for_hash(key_hash, store.remote_keys.len());
        store.remote_keys[metadata_index].write().insert_hashed(
            key_hash,
            SharedBytes::from_static(remote_key),
            RemoteKeyMeta {
                primary_shard: 0,
                expire_at_ms: None,
                generation: 1,
                handoff_complete: false,
                protected: false,
            },
        );
        let remote_gate = key_gate_index(remote_key, store.key_gates.len());
        let unrelated = (0..100_000)
            .map(|index| format!("handoff-write-{index}").into_bytes())
            .find(|key| key_gate_index(key, store.key_gates.len()) != remote_gate)
            .unwrap();

        previous.block_get.store(true, Ordering::Relaxed);
        let handoff_store = Arc::clone(&store);
        let handoff = thread::spawn(move || handoff_store.synchronize_remote_handoff());
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        store.set(unrelated, b"new".to_vec(), None).unwrap();
        release.send(()).unwrap();
        assert_eq!(handoff.join().unwrap().unwrap(), 1);
    }

    #[test]
    fn durable_remote_miss_keeps_metadata_and_fails_snapshot() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1)).unwrap();
        store.set(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        store.flush_remote().unwrap();
        node.values.write().clear();

        assert_eq!(store.get(b"key").unwrap(), None);
        assert_eq!(store.health_snapshot().remote_keys, 1);
        assert!(store.try_entry_snapshot().is_err());
    }

    #[test]
    fn previous_owner_failure_is_reported_without_falling_through() {
        let first_node = MemoryNode::new("node-a");
        first_node.fail_gets.store(true, Ordering::Relaxed);
        let first = Arc::new(first_node);
        let second_node = MemoryNode::new("node-b");
        second_node.fail_gets.store(true, Ordering::Relaxed);
        let second = Arc::new(second_node);
        let added = Arc::new(MemoryNode::new("node-c"));
        let expanded = KvOverflowCluster::with_previous(
            vec![first.clone(), second.clone(), added],
            vec![first, second],
            DEFAULT_KV_OVERFLOW_SLOT_COUNT,
        )
        .unwrap();
        let key = (0..100_000)
            .map(|index| format!("moving-key-{index}"))
            .find(|key| expanded.owner_id(key.as_bytes()) == "node-c")
            .expect("expansion must move at least one slot");

        assert!(expanded.get(key.as_bytes()).is_err());
        assert_eq!(expanded.metrics.handoff_reads.load(Ordering::Relaxed), 1);
        assert_eq!(expanded.metrics.handoff_failures.load(Ordering::Relaxed), 1);
        assert_eq!(expanded.metrics.get_failures.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn primary_set_does_not_wait_for_remote_io() {
        let (node, started, release) = blocking_node();
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1024)).unwrap();

        store.set(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        started
            .recv_timeout(Duration::from_secs(1))
            .expect("worker started remote write");
        assert_eq!(store.inner().get(b"key"), Some(b"value".to_vec()));
        assert_eq!(store.health_snapshot().pending_keys, 0);

        release.send(()).unwrap();
        store.flush_remote().unwrap();
        assert_eq!(store.health_snapshot().pending_keys, 0);
    }

    #[test]
    fn network_worker_only_emits_primary_applied_completions() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut isolated = options(usize::MAX);
        isolated.cleanup_interval = Duration::from_secs(60);
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, isolated).unwrap();

        store
            .set(b"first".to_vec(), b"value".to_vec(), None)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !node.values.read().contains_key(b"first".as_slice()) && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(node.values.read().contains_key(b"first".as_slice()));
        assert_eq!(store.health_snapshot().pending_keys, 0);
        assert_eq!(store.health_snapshot().remote_keys, 0);

        store
            .set(b"second".to_vec(), b"value".to_vec(), None)
            .unwrap();
        assert_eq!(store.health_snapshot().remote_keys, 0);
        store.flush_remote().unwrap();
        assert_eq!(store.health_snapshot().remote_keys, 2);
    }

    #[test]
    fn worker_batches_queued_puts_without_losing_acknowledgements() {
        let (started, started_rx) = bounded(1);
        let (release, release_rx) = bounded(1);
        let node = Arc::new(BatchingNode {
            id: "batching-node".into(),
            started,
            release: release_rx,
            values: RwLock::new(HashMap::new()),
            batch_sizes: Mutex::new(Vec::new()),
            block_once: AtomicBool::new(true),
        });
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut serial = options(usize::MAX);
        serial.queue_capacity = 256;
        let store = KvOverflowStore::new(EmbeddedStore::new(4), cluster, serial).unwrap();

        store
            .set(b"key-0".to_vec(), b"value".to_vec(), None)
            .unwrap();
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first batch reached overflow node");
        for index in 1..129 {
            store
                .set(format!("key-{index}").into_bytes(), b"value".to_vec(), None)
                .unwrap();
        }
        release.send(()).unwrap();
        store.flush_remote().unwrap();

        let batch_sizes = node.batch_sizes.lock();
        assert_eq!(batch_sizes.iter().sum::<usize>(), 129);
        assert!(batch_sizes.iter().any(|size| *size > 1));
        assert!(batch_sizes.iter().all(|size| *size <= 64));
        assert_eq!(node.values.read().len(), 129);
        let health = store.health_snapshot();
        assert_eq!(health.pending_keys, 0);
        assert_eq!(health.remote_keys, 129);
        assert_eq!(health.replicated_puts, 129);
    }

    #[test]
    fn worker_limits_pipeline_bytes_for_large_values() {
        let (started, _started_rx) = bounded(1);
        let (_release, release_rx) = bounded(1);
        let node = Arc::new(BatchingNode {
            id: "byte-limited-node".into(),
            started,
            release: release_rx,
            values: RwLock::new(HashMap::new()),
            batch_sizes: Mutex::new(Vec::new()),
            block_once: AtomicBool::new(false),
        });
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut byte_limited = options(usize::MAX);
        byte_limited.pipeline_max_bytes = 150;
        byte_limited.pipeline_flush = Duration::from_millis(1);
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, byte_limited).unwrap();

        for index in 0..20 {
            store
                .set(
                    format!("key-{index}").into_bytes(),
                    vec![index as u8; 64],
                    None,
                )
                .unwrap();
        }
        store.flush_remote().unwrap();

        let batch_sizes = node.batch_sizes.lock();
        assert!(batch_sizes.iter().all(|size| *size <= 2));
        assert_eq!(batch_sizes.iter().sum::<usize>(), 20);
    }

    #[test]
    fn delayed_direct_target_does_not_block_healthy_target() {
        let slow = DirectWireServer::start(Duration::from_millis(300));
        let healthy = DirectWireServer::start(Duration::ZERO);
        let mut cluster = KvOverflowCluster::new(vec![
            slow.node("slow-target"),
            healthy.node("healthy-target"),
        ])
        .unwrap();
        cluster.transport = Some(KvOverflowTransport::DirectShard);
        let cluster = Arc::new(cluster);
        let mut slow_key = None;
        let mut healthy_key = None;
        for index in 0..10_000 {
            let key = format!("target-isolation-{index}").into_bytes();
            match cluster.owner_id(&key) {
                "slow-target" if slow_key.is_none() => slow_key = Some(key),
                "healthy-target" if healthy_key.is_none() => healthy_key = Some(key),
                _ => {}
            }
            if slow_key.is_some() && healthy_key.is_some() {
                break;
            }
        }
        let store = KvOverflowStore::new(
            EmbeddedStore::new(1),
            cluster,
            KvOverflowOptions {
                pipeline_flush: Duration::ZERO,
                ..options(usize::MAX)
            },
        )
        .unwrap();
        store
            .set(slow_key.expect("slow key"), b"slow".to_vec(), None)
            .unwrap();
        let started = Instant::now();
        store
            .set(healthy_key.expect("healthy key"), b"healthy".to_vec(), None)
            .unwrap();

        let deadline = Instant::now() + Duration::from_millis(150);
        while store.health_snapshot().replicated_puts == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(store.health_snapshot().replicated_puts, 1);
        assert!(started.elapsed() < Duration::from_millis(200));
        store.flush_remote().unwrap();
        assert_eq!(store.health_snapshot().replicated_puts, 2);
    }

    #[test]
    fn direct_pipeline_retries_items_after_one_response_fails() {
        let failing = DirectWireServer::start_with_failure(Duration::ZERO, Some(2));
        let other = DirectWireServer::start(Duration::ZERO);
        let mut cluster = KvOverflowCluster::new(vec![
            failing.node("failing-target"),
            other.node("other-target"),
        ])
        .unwrap();
        cluster.transport = Some(KvOverflowTransport::DirectShard);
        let cluster = Arc::new(cluster);
        let keys = (0..100_000)
            .map(|index| format!("partial-pipeline-{index}").into_bytes())
            .filter(|key| cluster.owner_id(key) == "failing-target")
            .take(3)
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 3);
        let mut pipelined = options(usize::MAX);
        pipelined.pipeline_flush = Duration::from_millis(10);
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, pipelined).unwrap();
        for key in keys {
            store.set(key, b"value".to_vec(), None).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        while store.health_snapshot().replicated_puts != 3 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(store.health_snapshot().replicated_puts, 3);
        store.flush_remote().unwrap();
    }

    #[test]
    fn saturated_direct_target_does_not_consume_the_shard_queue() {
        let slow = DirectWireServer::start(Duration::from_millis(250));
        let healthy = DirectWireServer::start(Duration::ZERO);
        let mut cluster = KvOverflowCluster::new(vec![
            slow.node("slow-target"),
            healthy.node("healthy-target"),
        ])
        .unwrap();
        cluster.transport = Some(KvOverflowTransport::DirectShard);
        let cluster = Arc::new(cluster);
        let mut slow_keys = Vec::new();
        let mut healthy_key = None;
        for index in 0..100_000 {
            let key = format!("target-capacity-{index}").into_bytes();
            match cluster.owner_id(&key) {
                "slow-target" if slow_keys.len() < 8 => slow_keys.push(key),
                "healthy-target" if healthy_key.is_none() => healthy_key = Some(key),
                _ => {}
            }
            if slow_keys.len() == 8 && healthy_key.is_some() {
                break;
            }
        }
        let mut constrained = options(usize::MAX);
        constrained.queue_capacity = 8;
        constrained.pipeline_max_items = 1;
        constrained.pipeline_flush = Duration::ZERO;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();
        for key in slow_keys {
            store.set(key, b"slow".to_vec(), None).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        while store.health_snapshot().replication_failures == 0 && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(store.health_snapshot().replication_failures > 0);

        let started = Instant::now();
        store
            .set(healthy_key.expect("healthy key"), b"healthy".to_vec(), None)
            .unwrap();
        let deadline = Instant::now() + Duration::from_millis(150);
        while store.health_snapshot().replicated_puts == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(store.health_snapshot().replicated_puts, 1);
        assert!(started.elapsed() < Duration::from_millis(200));
    }

    #[test]
    fn queued_replication_does_not_restart_primary_ttl() {
        let (node, started, release) = blocking_node();
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut serial = options(1024);
        serial.queue_capacity = 2;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, serial).unwrap();

        store
            .set(b"blocker".to_vec(), b"value".to_vec(), None)
            .unwrap();
        started
            .recv_timeout(Duration::from_secs(1))
            .expect("worker started blocking write");
        store
            .set(b"expiring".to_vec(), b"value".to_vec(), Some(50))
            .unwrap();
        thread::sleep(Duration::from_millis(100));

        release.send(()).unwrap();
        store.flush_remote().unwrap();

        assert!(!node.values.read().contains_key(b"expiring".as_slice()));
        assert_eq!(store.get(b"expiring").unwrap(), None);
        let metadata_index = key_gate_index(b"expiring", store.remote_keys.len());
        assert!(
            !store.remote_keys[metadata_index]
                .read()
                .contains_key(b"expiring".as_slice())
        );
    }

    #[test]
    fn flush_waits_for_reserved_mutation_to_enqueue() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let serial = options(1024);
        let store = Arc::new(KvOverflowStore::new(EmbeddedStore::new(1), cluster, serial).unwrap());
        let key_gate = store.key_gate(b"key");
        let writer_store = Arc::clone(&store);
        let writer =
            thread::spawn(move || writer_store.set(b"key".to_vec(), b"value".to_vec(), None));
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while store.health_snapshot().queue_depth == 0 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(store.health_snapshot().queue_depth, 1);

        let (flush_done, flush_result) = bounded(1);
        let flush_store = Arc::clone(&store);
        let flush = thread::spawn(move || {
            let _ = flush_done.send(flush_store.flush_remote());
        });
        assert!(
            flush_result
                .recv_timeout(Duration::from_millis(50))
                .is_err()
        );

        drop(key_gate);
        writer.join().unwrap().unwrap();
        flush_result
            .recv_timeout(Duration::from_secs(1))
            .expect("flush completed after admitted write enqueued")
            .unwrap();
        flush.join().unwrap();
        assert_eq!(
            node.values.read()[b"key".as_slice()].value.as_ref(),
            b"value"
        );
    }

    #[test]
    fn full_queue_rejects_without_mutating_primary() {
        let (node, started, release) = blocking_node();
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let mut constrained = options(1024);
        constrained.queue_capacity = 1;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();

        store
            .set(b"first".to_vec(), b"value".to_vec(), None)
            .unwrap();
        started
            .recv_timeout(Duration::from_secs(1))
            .expect("worker started remote write");
        let error = store
            .set(b"rejected".to_vec(), b"value".to_vec(), None)
            .expect_err("full queue must apply backpressure");
        assert!(matches!(error, ShardCacheError::Backpressure(_)));
        assert!(!store.inner().exists(b"rejected"));
        assert_eq!(store.health_snapshot().enqueue_failures, 1);

        release.send(()).unwrap();
        store.flush_remote().unwrap();
    }

    #[test]
    fn metadata_budget_bounds_unique_keys_and_is_reusable_after_delete() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let mut constrained = options(usize::MAX);
        constrained.max_metadata_bytes = KV_OVERFLOW_METADATA_ENTRY_OVERHEAD + b"a".len();
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();

        store.set(b"a", b"one", None).unwrap();
        store.set(b"a", b"updated", None).unwrap();
        let error = store
            .set(b"b", b"two", None)
            .expect_err("a second unique key must exceed the metadata budget");
        assert!(matches!(error, ShardCacheError::Backpressure(_)));
        assert!(!store.inner().exists(b"b"));
        assert_eq!(
            store.health_snapshot().metadata_bytes,
            KV_OVERFLOW_METADATA_ENTRY_OVERHEAD + 1
        );

        store.flush_remote().unwrap();
        assert!(store.delete(b"a").unwrap());
        assert_eq!(store.health_snapshot().metadata_bytes, 0);
        store.set(b"b", b"two", None).unwrap();
        store.flush_remote().unwrap();
        assert_eq!(store.get(b"b").unwrap().as_deref(), Some(b"two".as_slice()));
        assert_eq!(store.health_snapshot().metadata_rejections, 1);
    }

    #[test]
    fn oversized_keys_are_rejected_before_primary_mutation() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let mut constrained = options(usize::MAX);
        constrained.max_key_bytes = 3;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();

        assert!(matches!(
            store.set(b"four", b"value", None),
            Err(ShardCacheError::Protocol(_))
        ));
        assert!(store.get(b"four").is_err());
        assert!(store.get_remote(b"four").is_err());
        assert!(store.delete(b"four").is_err());
        assert!(!store.inner().exists(b"four"));
        assert_eq!(store.health_snapshot().metadata_rejections, 4);
    }

    #[test]
    fn completed_acknowledgement_reclaims_its_shard_admission_slot() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut constrained = options(usize::MAX);
        constrained.queue_capacity = 1;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();

        store
            .set(b"first".to_vec(), b"value".to_vec(), None)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while store.workers.completion_receivers[0].is_empty() && Instant::now() < deadline {
            thread::yield_now();
        }
        assert_eq!(store.workers.completion_receivers[0].len(), 1);

        store
            .set(b"second".to_vec(), b"value".to_vec(), None)
            .unwrap();
        store.flush_remote().unwrap();

        assert_eq!(node.values.read().len(), 2);
        assert_eq!(store.health_snapshot().enqueue_failures, 0);
    }

    #[test]
    fn pending_value_is_not_evictable_until_acknowledged() {
        let (node, started, release) = blocking_node();
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1)).unwrap();

        store.set(b"key".to_vec(), vec![7; 32], None).unwrap();
        started
            .recv_timeout(Duration::from_secs(1))
            .expect("worker started remote write");
        assert!(store.inner().exists(b"key"));
        assert_eq!(store.health_snapshot().offloads, 0);

        release.send(()).unwrap();
        store.flush_remote().unwrap();
        assert!(!store.inner().exists(b"key"));
        assert_eq!(store.health_snapshot().offloads, 1);
    }

    #[test]
    fn same_key_overwrites_reach_replica_in_order() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut ordered = options(1024);
        ordered.queue_capacity = 128;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, ordered).unwrap();

        for version in 0..100u8 {
            store.set(b"key".to_vec(), vec![version], None).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        while (node
            .values
            .read()
            .get(b"key".as_slice())
            .is_none_or(|value| value.value.as_ref() != [99])
            || store.workers.completion_receivers[0].len() != 100)
            && Instant::now() < deadline
        {
            thread::yield_now();
        }
        assert_eq!(store.workers.completion_receivers[0].len(), 100);
        store.flush_remote().unwrap();

        assert_eq!(node.values.read()[b"key".as_slice()].value.as_ref(), &[99]);
    }

    #[test]
    fn concurrent_producers_preserve_all_acknowledgements() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut concurrent = options(usize::MAX);
        concurrent.queue_capacity = 2_048;
        let store =
            Arc::new(KvOverflowStore::new(EmbeddedStore::new(8), cluster, concurrent).unwrap());
        let mut producers = Vec::new();
        for producer in 0..8 {
            let store = Arc::clone(&store);
            producers.push(thread::spawn(move || {
                for index in 0..128 {
                    let key = format!("producer-{producer}-key-{index}").into_bytes();
                    store.set(key, vec![producer as u8; 32], None).unwrap();
                }
            }));
        }
        for producer in producers {
            producer.join().unwrap();
        }
        store.flush_remote().unwrap();

        let health = store.health_snapshot();
        assert_eq!(health.pending_keys, 0);
        assert_eq!(health.remote_keys, 1_024);
        assert_eq!(health.replicated_puts, 1_024);
        assert_eq!(node.values.read().len(), 1_024);
    }

    #[test]
    fn acknowledged_cold_values_offload_and_fault_back() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(25)).unwrap();

        store.set(b"cold".to_vec(), vec![1; 8], None).unwrap();
        store.set(b"hot".to_vec(), vec![2; 8], None).unwrap();
        for _ in 0..2_048 {
            assert!(store.get(b"hot").unwrap().is_some());
        }
        store.set(b"new".to_vec(), vec![3; 8], None).unwrap();
        store.flush_remote().unwrap();

        assert!(!store.inner().exists(b"cold"));
        assert!(store.inner().exists(b"hot"));
        assert_eq!(store.get(b"cold").unwrap(), Some(vec![1; 8]));
        assert!(store.health_snapshot().fault_ins >= 1);
    }

    #[test]
    fn failed_mirror_remains_resident_and_ineligible() {
        let node = MemoryNode::new("node-a");
        node.fail_puts.store(true, Ordering::Relaxed);
        let cluster = Arc::new(KvOverflowCluster::new(vec![Arc::new(node)]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1)).unwrap();

        store.set(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        assert!(store.flush_remote().is_err());
        assert_eq!(store.inner().get(b"key"), Some(b"value".to_vec()));
        assert_eq!(store.health_snapshot().offloads, 0);
        assert_eq!(store.health_snapshot().replication_failures, 2);
    }

    #[test]
    fn failed_replica_backpressures_resident_growth_above_target() {
        let node = MemoryNode::new("node-a");
        node.fail_puts.store(true, Ordering::Relaxed);
        let cluster = Arc::new(KvOverflowCluster::new(vec![Arc::new(node)]).unwrap());
        let mut constrained = options(4);
        constrained.max_key_bytes = 1;
        constrained.max_value_bytes = 5;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();

        store.set(b"a", b"value", None).unwrap();
        assert!(store.flush_remote().is_err());
        let retained_bytes = store.inner().stored_bytes();
        let error = store
            .set(b"b", b"value", None)
            .expect_err("unavailable overflow must not permit unbounded resident growth");

        assert!(matches!(error, ShardCacheError::Backpressure(_)));
        assert_eq!(store.inner().stored_bytes(), retained_bytes);
        assert!(!store.inner().exists(b"b"));
        assert_eq!(store.health_snapshot().resident_backpressure, 1);
    }

    #[test]
    fn resident_hard_limit_accounts_for_replaced_value_under_shard_lock() {
        let node = MemoryNode::new("node-a");
        node.fail_puts.store(true, Ordering::Relaxed);
        let cluster = Arc::new(KvOverflowCluster::new(vec![Arc::new(node)]).unwrap());
        let mut constrained = options(4);
        constrained.max_key_bytes = 1;
        constrained.max_value_bytes = 5;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();

        store.set(b"a", b"x", None).unwrap();
        store.set(b"a", b"value", None).unwrap();
        let retained_bytes = store.inner().stored_bytes();

        assert_eq!(store.inner().get(b"a"), Some(b"value".to_vec()));
        assert!(matches!(
            store.set(b"b", b"value", None),
            Err(ShardCacheError::Backpressure(_))
        ));
        assert_eq!(store.inner().stored_bytes(), retained_bytes);
        assert!(!store.inner().exists(b"b"));
    }

    #[test]
    fn resident_hard_limit_accounts_for_governance_bytes() {
        let node = MemoryNode::new("node-a");
        node.fail_puts.store(true, Ordering::Relaxed);
        let cluster = Arc::new(KvOverflowCluster::new(vec![Arc::new(node)]).unwrap());
        let mut constrained = options(4);
        constrained.max_key_bytes = 1;
        constrained.max_value_bytes = 5;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();

        store
            .set_with_governance(b"a", b"value", None, b"rules")
            .unwrap();
        let retained_bytes = store.inner().stored_bytes();
        assert_eq!(retained_bytes, b"a".len() + b"value".len() + b"rules".len());

        assert!(matches!(
            store.set_with_governance(b"b", b"value", None, b"rules"),
            Err(ShardCacheError::Backpressure(_))
        ));
        assert_eq!(store.inner().stored_bytes(), retained_bytes);
        assert_eq!(
            store.get_with_governance_filter(b"b", |_| true).unwrap(),
            None
        );
    }

    #[test]
    fn governed_cold_value_is_fail_closed_and_stays_protected_after_promotion() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1)).unwrap();
        store
            .set_with_governance(
                b"known-prefix",
                b"private-model-state",
                None,
                b"tenant-a/repo-private",
            )
            .unwrap();
        store.flush_remote().unwrap();

        assert_eq!(store.get(b"known-prefix").unwrap(), None);
        assert_eq!(store.get_remote(b"known-prefix").unwrap(), None);
        assert_eq!(
            store
                .get_with_governance_filter(b"known-prefix", |metadata| {
                    metadata == Some(b"tenant-b/repo-public".as_slice())
                })
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .get_with_governance_filter(b"known-prefix", |metadata| {
                    metadata == Some(b"tenant-a/repo-private".as_slice())
                })
                .unwrap()
                .as_deref(),
            Some(b"private-model-state".as_slice())
        );
        assert_eq!(store.get(b"known-prefix").unwrap(), None);

        let snapshot = store.try_entry_snapshot().unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot[0].governance.as_deref(),
            Some(b"tenant-a/repo-private".as_slice())
        );
    }

    #[test]
    fn fault_in_bypasses_primary_when_resident_hard_limit_is_full() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut constrained = options(4);
        constrained.max_key_bytes = 1;
        constrained.max_value_bytes = 5;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();

        store.set(b"a", b"value", None).unwrap();
        store.flush_remote().unwrap();
        assert!(!store.inner().exists(b"a"));

        node.fail_puts.store(true, Ordering::Relaxed);
        store.set(b"b", b"value", None).unwrap();
        assert!(store.flush_remote().is_err());
        assert_eq!(
            store.get(b"a").unwrap().as_deref(),
            Some(b"value".as_slice())
        );
        assert!(!store.inner().exists(b"a"));
        assert_eq!(store.health_snapshot().resident_backpressure, 1);
    }

    #[test]
    fn snapshot_materializes_remote_only_values() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1)).unwrap();
        store.set(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        store.flush_remote().unwrap();

        assert!(!store.inner().exists(b"key"));
        assert_eq!(
            store.try_entry_snapshot().unwrap(),
            vec![StoredEntry {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                expire_at_ms: None,
                governance: None,
            }]
        );
    }

    #[test]
    fn construction_mirrors_preloaded_recovery_values() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let inner = EmbeddedStore::new(1);
        inner.set(b"recovered".to_vec(), b"value".to_vec(), None);

        let store = KvOverflowStore::new(inner, cluster.clone(), options(1)).unwrap();

        assert!(!store.inner().exists(b"recovered"));
        assert_eq!(
            cluster.get(b"recovered").unwrap().unwrap().value.as_ref(),
            b"value"
        );
    }

    #[test]
    fn delete_failure_retains_primary_value() {
        let node = MemoryNode::new("node-a");
        node.fail_deletes.store(true, Ordering::Relaxed);
        let cluster = Arc::new(KvOverflowCluster::new(vec![Arc::new(node)]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1024)).unwrap();
        store.set(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        store.flush_remote().unwrap();

        assert!(store.delete(b"key").is_err());
        assert_eq!(store.inner().get(b"key"), Some(b"value".to_vec()));
    }

    #[test]
    fn snapshot_fails_when_remote_value_is_missing() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1)).unwrap();
        store.set(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        store.flush_remote().unwrap();
        node.values.write().clear();

        assert!(store.try_entry_snapshot().is_err());
    }

    #[test]
    fn oversized_value_is_rejected_before_admission_or_retry() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let mut constrained = options(1);
        constrained.max_value_bytes = 4;
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, constrained).unwrap();

        assert!(store.set(b"large", b"12345", None).is_err());
        let health = store.health_snapshot();
        assert_eq!(health.oversized_rejections, 1);
        assert_eq!(health.pending_keys, 0);
        assert_eq!(health.queue_depth, 0);
        assert!(!store.inner().exists(b"large"));
        assert!(!node.values.read().contains_key(b"large".as_slice()));
    }

    #[test]
    fn streaming_snapshot_persists_resident_and_remote_values() {
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(16)).unwrap();
        store.set(b"cold", vec![1; 32], None).unwrap();
        store.set(b"resident", vec![2; 4], None).unwrap();
        store.flush_remote().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let snapshots = SnapshotStore::new(directory.path());

        store
            .write_snapshot(&snapshots, 11, SnapshotCompression::Lz4)
            .unwrap();
        let mut entries = snapshots.load_latest_snapshot().unwrap().unwrap().entries;
        entries.sort_by(|left, right| left.key.cmp(&right.key));

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key.as_slice(), b"cold");
        assert_eq!(entries[0].value.as_slice(), &[1; 32]);
        assert_eq!(entries[1].key.as_slice(), b"resident");
        assert_eq!(entries[1].value.as_slice(), &[2; 4]);
    }

    #[test]
    fn streaming_snapshot_restore_rebuilds_overflow_incrementally() {
        let directory = tempfile::tempdir().unwrap();
        let snapshots = SnapshotStore::new(directory.path());
        snapshots
            .write_snapshot_streaming(12, SnapshotCompression::Lz4, |sink| {
                for index in 0..32u8 {
                    sink(StoredEntry {
                        key: vec![index],
                        value: vec![index; 8],
                        expire_at_ms: None,
                        governance: None,
                    })?;
                }
                Ok(())
            })
            .unwrap();
        let node = Arc::new(MemoryNode::new("node-a"));
        let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(16)).unwrap();

        assert_eq!(store.restore_latest_snapshot(&snapshots).unwrap(), Some(12));
        assert_eq!(node.values.read().len(), 32);
        assert!(store.inner().stored_bytes() <= 16);
        for index in 0..32u8 {
            assert_eq!(
                store.get_remote(&[index]).unwrap().unwrap().value,
                vec![index; 8]
            );
        }
    }

    #[test]
    fn primary_auth_token_file_reloads_without_exposing_token() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        std::fs::write(&path, b"first\n").unwrap();
        let token =
            ReloadableScnpAuthToken::new_file(path.clone(), Duration::from_millis(100)).unwrap();
        assert_eq!(token.current().as_ref(), b"first");

        std::fs::write(path, b"second\n").unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while token.current().as_ref() != b"second" && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(token.current().as_ref(), b"second");
    }
}
