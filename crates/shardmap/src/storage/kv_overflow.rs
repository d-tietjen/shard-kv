use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes as SharedBytes;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError, bounded, unbounded};
use parking_lot::{Mutex, RwLock};
use shardcache_client_rs::{ShardCacheClient, ShardCacheClientError};
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

use crate::config::{EvictionPolicy, KvOverflowBackend, KvOverflowConfig};
use crate::storage::{Bytes, EmbeddedStore, StoredEntry, now_millis};
use crate::{Result, ShardCacheError};

const KV_OVERFLOW_MAGIC: &[u8; 8] = b"SCKVOV01";
const KV_OVERFLOW_HEADER_LEN: usize = KV_OVERFLOW_MAGIC.len() + 8 + 8 + 4;
const KV_OVERFLOW_KEY_GATES: usize = 64;
pub const DEFAULT_KV_OVERFLOW_SLOT_COUNT: u32 = 16_384;

/// Value returned by a key-value overflow node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvOverflowValue {
    pub value: SharedBytes,
    pub ttl_ms: Option<u64>,
}

/// One independently addressable member of a partitioned overflow cluster.
pub trait KvOverflowNode: Send + Sync + 'static {
    /// Stable node identity used by rendezvous hashing.
    fn id(&self) -> &str;
    fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()>;
    fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>>;
    fn delete(&self, key: &[u8]) -> Result<()>;
}

/// Runtime options for partitioned key-value overflow.
#[derive(Debug, Clone)]
pub struct KvOverflowOptions {
    pub max_memory_bytes: usize,
    pub eviction_policy: EvictionPolicy,
    pub fetch_on_miss: bool,
    pub cleanup_interval: Duration,
    pub worker_threads: usize,
    pub queue_capacity: usize,
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
        if !matches!(
            config.eviction_policy,
            EvictionPolicy::Lru | EvictionPolicy::Lfu
        ) {
            return Err(ShardCacheError::Config(
                "kv_overflow.eviction_policy must be lru or lfu".into(),
            ));
        }
        if config.worker_threads == 0 || config.queue_capacity == 0 {
            return Err(ShardCacheError::Config(
                "kv_overflow.worker_threads and queue_capacity must be > 0".into(),
            ));
        }
        Ok(Self {
            max_memory_bytes,
            eviction_policy: config.eviction_policy,
            fetch_on_miss: config.fetch_on_miss,
            cleanup_interval: Duration::from_millis(config.cleanup_interval_ms.max(1)),
            worker_threads: config.worker_threads,
            queue_capacity: config.queue_capacity,
        })
    }
}

/// SCNP-backed overflow node with persistent connection pooling and retries.
pub struct ScnpKvOverflowNode {
    endpoint: String,
    connections: Box<[Mutex<Option<ShardCacheClient>>]>,
    next_connection: AtomicUsize,
    connect_timeout: Duration,
    operation_timeout: Duration,
    max_retries: usize,
    retry_backoff: Duration,
}

impl std::fmt::Debug for ScnpKvOverflowNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScnpKvOverflowNode")
            .field("endpoint", &self.endpoint)
            .field("connections", &self.connections.len())
            .field("connect_timeout", &self.connect_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

impl ScnpKvOverflowNode {
    pub fn from_config(endpoint: String, config: &KvOverflowConfig) -> Self {
        let connection_count = config.connections_per_endpoint.max(1);
        Self {
            endpoint,
            connections: (0..connection_count)
                .map(|_| Mutex::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next_connection: AtomicUsize::new(0),
            connect_timeout: Duration::from_millis(config.connect_timeout_ms.max(1)),
            operation_timeout: Duration::from_millis(config.operation_timeout_ms.max(1)),
            max_retries: config.max_retries,
            retry_backoff: Duration::from_millis(config.retry_backoff_ms.max(1)),
        }
    }

    fn execute<T>(
        &self,
        mut operation: impl FnMut(&mut ShardCacheClient) -> shardcache_client_rs::Result<T>,
    ) -> Result<T> {
        let slot = self.next_connection.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            let mut connection = self.connections[slot].lock();
            if connection.is_none() {
                match ShardCacheClient::connect_with_timeouts(
                    self.endpoint.as_str(),
                    self.connect_timeout,
                    self.operation_timeout,
                ) {
                    Ok(client) => *connection = Some(client),
                    Err(error) => {
                        last_error = Some(error);
                        drop(connection);
                        self.retry_delay(attempt);
                        continue;
                    }
                }
            }
            let result = operation(connection.as_mut().expect("connection initialized"));
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    *connection = None;
                    last_error = Some(error);
                }
            }
            drop(connection);
            self.retry_delay(attempt);
        }
        Err(client_error(
            self.endpoint.as_str(),
            last_error.expect("at least one connection attempt"),
        ))
    }

    fn retry_delay(&self, attempt: usize) {
        if attempt < self.max_retries {
            thread::sleep(self.retry_backoff);
        }
    }
}

impl KvOverflowNode for ScnpKvOverflowNode {
    fn id(&self) -> &str {
        &self.endpoint
    }

    fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
        let encoded = encode_overflow_value(value, ttl_ms);
        self.execute(|client| client.set(key, &encoded))
    }

    fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        self.execute(|client| {
            let mut value = Vec::new();
            if !client.get_into(key, &mut value)? {
                return Ok(None);
            }
            decode_overflow_value(&value)
                .map_err(|message| ShardCacheClientError::Protocol(message.to_string()))
        })
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.execute(|client| client.del(key).map(|_| ()))
    }
}

/// Redis/Valkey-compatible overflow node with pooled blocking connections.
#[cfg(feature = "kv-overflow-redis")]
pub struct RedisKvOverflowNode {
    endpoint: String,
    key_prefix: Box<[u8]>,
    client: redis_client::Client,
    connections: Box<[Mutex<Option<redis_client::Connection>>]>,
    next_connection: AtomicUsize,
    connect_timeout: Duration,
    operation_timeout: Duration,
    max_retries: usize,
    retry_backoff: Duration,
}

#[cfg(feature = "kv-overflow-redis")]
impl std::fmt::Debug for RedisKvOverflowNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedisKvOverflowNode")
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
        use redis_client::IntoConnectionInfo;

        if config.redis_key_prefix.is_empty() {
            return Err(ShardCacheError::Config(
                "kv_overflow.redis_key_prefix must not be empty for the Redis backend".into(),
            ));
        }
        let mut connection_info = endpoint
            .as_str()
            .into_connection_info()
            .map_err(|error| redis_config_error(&endpoint, error))?;
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
        let client = redis_client::Client::open(connection_info)
            .map_err(|error| redis_config_error(&endpoint, error))?;
        let connection_count = config.connections_per_endpoint.max(1);
        Ok(Self {
            endpoint,
            key_prefix: config.redis_key_prefix.as_bytes().into(),
            client,
            connections: (0..connection_count)
                .map(|_| Mutex::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            next_connection: AtomicUsize::new(0),
            connect_timeout: Duration::from_millis(config.connect_timeout_ms.max(1)),
            operation_timeout: Duration::from_millis(config.operation_timeout_ms.max(1)),
            max_retries: config.max_retries,
            retry_backoff: Duration::from_millis(config.retry_backoff_ms.max(1)),
        })
    }

    fn execute<T>(
        &self,
        mut operation: impl FnMut(&mut redis_client::Connection) -> redis_client::RedisResult<T>,
    ) -> Result<T> {
        let slot = self.next_connection.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            let mut connection = self.connections[slot].lock();
            if connection.is_none() {
                match self.connect() {
                    Ok(client) => *connection = Some(client),
                    Err(error) => {
                        last_error = Some(error);
                        drop(connection);
                        self.retry_delay(attempt);
                        continue;
                    }
                }
            }
            let result = operation(connection.as_mut().expect("connection initialized"));
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
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
}

#[cfg(feature = "kv-overflow-redis")]
impl KvOverflowNode for RedisKvOverflowNode {
    fn id(&self) -> &str {
        &self.endpoint
    }

    fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
        let storage_key = self.storage_key(key);
        let encoded = encode_overflow_value(value, ttl_ms);
        self.execute(|connection| {
            let mut command = redis_client::cmd("SET");
            command.arg(&storage_key).arg(&encoded);
            if let Some(ttl_ms) = ttl_ms {
                command.arg("PX").arg(ttl_ms.max(1));
            }
            command.query(connection)
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        let storage_key = self.storage_key(key);
        let encoded: Option<Vec<u8>> = self
            .execute(|connection| redis_client::cmd("GET").arg(&storage_key).query(connection))?;
        encoded
            .map(|value| {
                decode_overflow_value(&value).map_err(|message| {
                    ShardCacheError::Protocol(format!(
                        "Redis overflow node {}: {message}",
                        self.endpoint
                    ))
                })
            })
            .transpose()
            .map(Option::flatten)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let storage_key = self.storage_key(key);
        self.execute(|connection| {
            redis_client::cmd("DEL")
                .arg(&storage_key)
                .query::<u64>(connection)
                .map(|_| ())
        })
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
fn redis_config_error(endpoint: &str, error: redis_client::RedisError) -> ShardCacheError {
    ShardCacheError::Config(format!(
        "invalid Redis overflow endpoint {endpoint:?}: {error}"
    ))
}

#[cfg(feature = "kv-overflow-redis")]
fn redis_node_error(endpoint: &str, error: redis_client::RedisError) -> ShardCacheError {
    ShardCacheError::Protocol(format!("Redis overflow node {endpoint}: {error}"))
}

fn client_error(endpoint: &str, error: ShardCacheClientError) -> ShardCacheError {
    ShardCacheError::Protocol(format!("kv overflow node {endpoint}: {error}"))
}

fn encode_overflow_value(value: &[u8], ttl_ms: Option<u64>) -> Vec<u8> {
    let expire_at_ms = ttl_ms.map_or(0, |ttl| now_millis().saturating_add(ttl));
    let mut encoded = Vec::with_capacity(KV_OVERFLOW_HEADER_LEN.saturating_add(value.len()));
    encoded.extend_from_slice(KV_OVERFLOW_MAGIC);
    encoded.extend_from_slice(&expire_at_ms.to_le_bytes());
    encoded.extend_from_slice(&(value.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&crc32fast::hash(value).to_le_bytes());
    encoded.extend_from_slice(value);
    encoded
}

fn decode_overflow_value(
    value: &[u8],
) -> std::result::Result<Option<KvOverflowValue>, &'static str> {
    if value.len() < KV_OVERFLOW_HEADER_LEN
        || &value[..KV_OVERFLOW_MAGIC.len()] != KV_OVERFLOW_MAGIC
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
    let payload = &value[KV_OVERFLOW_HEADER_LEN..];
    if usize::try_from(payload_len).ok() != Some(payload.len())
        || crc32fast::hash(payload) != checksum
    {
        return Err("invalid key-value overflow payload integrity");
    }
    let now_ms = now_millis();
    if expire_at_ms != 0 && expire_at_ms <= now_ms {
        return Ok(None);
    }
    Ok(Some(KvOverflowValue {
        value: SharedBytes::copy_from_slice(payload),
        ttl_ms: (expire_at_ms != 0).then_some(expire_at_ms.saturating_sub(now_ms)),
    }))
}

#[derive(Debug, Default)]
struct KvOverflowMetrics {
    puts: AtomicU64,
    put_failures: AtomicU64,
    gets: AtomicU64,
    get_hits: AtomicU64,
    get_failures: AtomicU64,
    deletes: AtomicU64,
    delete_failures: AtomicU64,
    offloads: AtomicU64,
    fault_ins: AtomicU64,
    enqueued_puts: AtomicU64,
    enqueue_failures: AtomicU64,
    replicated_puts: AtomicU64,
    replication_failures: AtomicU64,
    handoff_reads: AtomicU64,
    handoff_hits: AtomicU64,
    handoff_failures: AtomicU64,
    active_workers: AtomicUsize,
}

/// Health and activity counters for a partitioned overflow cluster.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvOverflowHealthSnapshot {
    pub node_count: usize,
    pub previous_node_count: usize,
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
    pub pending_keys: usize,
    pub active_workers: usize,
    pub enqueued_puts: u64,
    pub enqueue_failures: u64,
    pub replicated_puts: u64,
    pub replication_failures: u64,
    pub handoff_reads: u64,
    pub handoff_hits: u64,
    pub handoff_failures: u64,
}

/// Fixed-slot collection of disjoint key-value overflow nodes.
pub struct KvOverflowCluster {
    nodes: Box<[Arc<dyn KvOverflowNode>]>,
    previous_nodes: Box<[Arc<dyn KvOverflowNode>]>,
    slot_count: u32,
    metrics: Arc<KvOverflowMetrics>,
}

impl std::fmt::Debug for KvOverflowCluster {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvOverflowCluster")
            .field("nodes", &self.node_ids())
            .field("previous_nodes", &self.previous_node_ids())
            .field("slot_count", &self.slot_count)
            .finish()
    }
}

impl KvOverflowCluster {
    pub fn from_config(config: &KvOverflowConfig) -> Result<Self> {
        if config.endpoints.is_empty() {
            return Err(ShardCacheError::Config(
                "kv_overflow.endpoints must contain at least one server".into(),
            ));
        }
        let make_nodes = |endpoints: &[String]| -> Result<Vec<Arc<dyn KvOverflowNode>>> {
            endpoints
                .iter()
                .cloned()
                .map(|endpoint| match config.backend {
                    KvOverflowBackend::Scnp => Ok(Arc::new(
                        ScnpKvOverflowNode::from_config(endpoint, config),
                    ) as Arc<dyn KvOverflowNode>),
                    KvOverflowBackend::Redis => {
                        #[cfg(feature = "kv-overflow-redis")]
                        {
                            Ok(Arc::new(RedisKvOverflowNode::from_config(endpoint, config)?)
                                as Arc<dyn KvOverflowNode>)
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
        Self::with_previous(
            make_nodes(&config.endpoints)?,
            make_nodes(&config.previous_endpoints)?,
            config.slot_count,
        )
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
        mut nodes: Vec<Arc<dyn KvOverflowNode>>,
        mut previous_nodes: Vec<Arc<dyn KvOverflowNode>>,
        slot_count: u32,
    ) -> Result<Self> {
        nodes.sort_by(|left, right| left.id().cmp(right.id()));
        previous_nodes.sort_by(|left, right| left.id().cmp(right.id()));
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
        Ok(Self {
            nodes: nodes.into_boxed_slice(),
            previous_nodes: previous_nodes.into_boxed_slice(),
            slot_count,
            metrics: Arc::new(KvOverflowMetrics::default()),
        })
    }

    pub fn node_ids(&self) -> Vec<&str> {
        self.nodes.iter().map(|node| node.id()).collect()
    }

    pub fn previous_node_ids(&self) -> Vec<&str> {
        self.previous_nodes.iter().map(|node| node.id()).collect()
    }

    /// Returns the stable logical overflow slot for `key`.
    pub fn slot_for_key(&self, key: &[u8]) -> u32 {
        (xxh3_64(key) as u32) & (self.slot_count - 1)
    }

    pub fn slot_count(&self) -> u32 {
        self.slot_count
    }

    pub fn owner_index(&self, key: &[u8]) -> usize {
        owner_index_for_slot(&self.nodes, self.slot_for_key(key))
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
        Ok(self.nodes[owner_index_for_slot(&self.nodes, slot)].id())
    }

    fn previous_owner(&self, key: &[u8]) -> Option<&Arc<dyn KvOverflowNode>> {
        if self.previous_nodes.is_empty() {
            return None;
        }
        let owner = &self.previous_nodes
            [owner_index_for_slot(&self.previous_nodes, self.slot_for_key(key))];
        (owner.id() != self.owner_id(key)).then_some(owner)
    }

    pub fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
        self.metrics.puts.fetch_add(1, Ordering::Relaxed);
        let result = self.nodes[self.owner_index(key)]
            .put(key, value, ttl_ms)
            .and_then(|()| {
                let Some(previous) = self.previous_owner(key) else {
                    return Ok(());
                };
                previous.delete(key).inspect_err(|_| {
                    self.metrics
                        .handoff_failures
                        .fetch_add(1, Ordering::Relaxed);
                })
            });
        if result.is_err() {
            self.metrics.put_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        self.metrics.gets.fetch_add(1, Ordering::Relaxed);
        let current = &self.nodes[self.owner_index(key)];
        let result = match current.get(key) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => self.get_from_previous(key),
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
        self.metrics.deletes.fetch_add(1, Ordering::Relaxed);
        let current_result = self.nodes[self.owner_index(key)].delete(key);
        let previous_result = self.previous_owner(key).map_or(Ok(()), |previous| {
            previous.delete(key).inspect_err(|_| {
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

    fn get_from_previous(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        let Some(previous) = self.previous_owner(key) else {
            return Ok(None);
        };
        self.metrics.handoff_reads.fetch_add(1, Ordering::Relaxed);
        let result = previous.get(key).inspect_err(|_| {
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

fn owner_index_for_slot(nodes: &[Arc<dyn KvOverflowNode>], slot: u32) -> usize {
    nodes
        .iter()
        .enumerate()
        .max_by(|(left_index, left), (right_index, right)| {
            rendezvous_score(slot, left.id())
                .cmp(&rendezvous_score(slot, right.id()))
                .then_with(|| right_index.cmp(left_index))
        })
        .map(|(index, _)| index)
        .expect("membership is non-empty")
}

fn rendezvous_score(slot: u32, node_id: &str) -> u64 {
    xxh3_64_with_seed(&slot.to_le_bytes(), xxh3_64(node_id.as_bytes()))
}

#[derive(Debug, Clone, Copy)]
struct RemoteKeyMeta {
    expire_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct PendingKeyMeta {
    generation: u64,
}

enum KvOverflowJob {
    Put {
        key: Bytes,
        value: SharedBytes,
        ttl_ms: Option<u64>,
        expire_at_ms: Option<u64>,
        generation: u64,
    },
    Delete {
        key: Bytes,
        generation: u64,
        completion: Sender<Result<()>>,
    },
    Barrier(Sender<Result<()>>),
    Shutdown,
}

struct KvOverflowWorkerPool {
    senders: Box<[Sender<KvOverflowJob>]>,
    available: Receiver<()>,
    permits: Sender<()>,
    capacity: usize,
    metrics: Arc<KvOverflowMetrics>,
    joins: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for KvOverflowWorkerPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvOverflowWorkerPool")
            .field("workers", &self.senders.len())
            .field("queue_depth", &self.queue_depth())
            .field("capacity", &self.capacity)
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
    remote_keys: Arc<RwLock<HashMap<Bytes, RemoteKeyMeta>>>,
    pending_keys: Arc<RwLock<HashMap<Bytes, PendingKeyMeta>>>,
    key_gates: Arc<[Mutex<()>]>,
    maintenance: Arc<Mutex<()>>,
    flush_gate: Mutex<()>,
    sequence: AtomicU64,
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
            .field("remote_keys", &self.remote_keys.read().len())
            .field("pending_keys", &self.pending_keys.read().len())
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
        let cluster = Arc::new(KvOverflowCluster::from_config(config)?);
        Self::new(inner, cluster, KvOverflowOptions::try_from(config)?)
    }

    pub fn new(
        inner: EmbeddedStore,
        cluster: Arc<KvOverflowCluster>,
        options: KvOverflowOptions,
    ) -> Result<Self> {
        if options.max_memory_bytes == 0
            || !matches!(
                options.eviction_policy,
                EvictionPolicy::Lru | EvictionPolicy::Lfu
            )
        {
            return Err(ShardCacheError::Config(
                "kv overflow requires a positive memory target and lru or lfu eviction".into(),
            ));
        }
        inner.configure_memory_policy(None, options.eviction_policy);
        let inner = Arc::new(inner);
        let remote_keys = Arc::new(RwLock::new(HashMap::new()));
        let pending_keys = Arc::new(RwLock::new(HashMap::new()));
        let key_gates: Arc<[Mutex<()>]> = (0..KV_OVERFLOW_KEY_GATES)
            .map(|_| Mutex::new(()))
            .collect::<Vec<_>>()
            .into();
        let maintenance = Arc::new(Mutex::new(()));
        let workers = KvOverflowWorkerPool::start(
            options.worker_threads,
            options.queue_capacity,
            Arc::clone(&inner),
            Arc::clone(&cluster),
            Arc::clone(&remote_keys),
            Arc::clone(&pending_keys),
            Arc::clone(&key_gates),
            Arc::clone(&maintenance),
            options.clone(),
        )?;
        let cleanup = KvOverflowCleanupTask::start(
            Arc::clone(&cluster),
            Arc::clone(&remote_keys),
            Arc::clone(&key_gates),
            options.cleanup_interval,
        )?;
        let store = Self {
            inner,
            cluster,
            options,
            remote_keys,
            pending_keys,
            key_gates,
            maintenance,
            flush_gate: Mutex::new(()),
            sequence: AtomicU64::new(1),
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
        let key = key.into();
        self.workers.reserve()?;
        let value = SharedBytes::from(value.into());
        let _key_gate = self.key_gate(&key);
        let generation = self.sequence.fetch_add(1, Ordering::Relaxed);
        let expire_at_ms = ttl_ms.map(|ttl| now_millis().saturating_add(ttl));
        {
            let mut remote_keys = self.remote_keys.write();
            remote_keys.remove(key.as_slice());
            self.inner.set_value_bytes(&key, value.clone(), ttl_ms);
        }
        self.pending_keys
            .write()
            .insert(key.clone(), PendingKeyMeta { generation });
        if self.workers.enqueue_reserved(KvOverflowJob::Put {
            key: key.clone(),
            value,
            ttl_ms,
            expire_at_ms,
            generation,
        }) {
            self.cluster
                .metrics
                .enqueued_puts
                .fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            self.pending_keys.write().remove(key.as_slice());
            Err(ShardCacheError::ChannelClosed("kv overflow workers"))
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(value) = self.inner.get(key) {
            return Ok(Some(value));
        }
        if !self.options.fetch_on_miss || !self.remote_keys.read().contains_key(key) {
            return Ok(None);
        }
        let _key_gate = self.key_gate(key);
        if let Some(value) = self.inner.get(key) {
            return Ok(Some(value));
        }
        let Some(remote) = self.cluster.get(key)? else {
            self.remote_keys.write().remove(key);
            return Ok(None);
        };
        self.inner
            .set_value_bytes(key, remote.value.clone(), remote.ttl_ms);
        self.cluster
            .metrics
            .fault_ins
            .fetch_add(1, Ordering::Relaxed);
        self.enforce_memory_target();
        Ok(Some(remote.value.as_ref().to_vec()))
    }

    /// Reads the deterministic overflow owner without touching primary memory.
    pub fn get_remote(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
        self.cluster.get(key)
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        self.workers.reserve()?;
        let generation = self.sequence.fetch_add(1, Ordering::Relaxed);
        let (completion, result) = bounded(1);
        let _key_gate = self.key_gate(key);
        let present = self.inner.exists(key)
            || self.remote_keys.read().contains_key(key)
            || self.pending_keys.read().contains_key(key);
        self.pending_keys
            .write()
            .insert(key.to_vec(), PendingKeyMeta { generation });
        if !self.workers.enqueue_reserved(KvOverflowJob::Delete {
            key: key.to_vec(),
            generation,
            completion,
        }) {
            self.pending_keys.write().remove(key);
            return Err(ShardCacheError::ChannelClosed("kv overflow workers"));
        }
        drop(_key_gate);
        result
            .recv()
            .map_err(|_| ShardCacheError::ChannelClosed("kv overflow delete completion"))??;
        Ok(present)
    }

    /// Waits until all remote mutations admitted before this call are complete.
    pub fn flush_remote(&self) -> Result<()> {
        let _flush = self.flush_gate.lock();
        let mut completions = Vec::with_capacity(self.workers.senders.len());
        for sender in &self.workers.senders {
            let (completion, result) = bounded(1);
            sender
                .send(KvOverflowJob::Barrier(completion))
                .map_err(|_| ShardCacheError::ChannelClosed("kv overflow workers"))?;
            completions.push(result);
        }
        for completion in completions {
            completion
                .recv()
                .map_err(|_| ShardCacheError::ChannelClosed("kv overflow flush"))??;
        }
        Ok(())
    }

    /// Mirrors all resident values, used after authoritative local recovery.
    pub fn synchronize_resident(&self) -> Result<()> {
        let _key_gates = self.lock_all_keys();
        let now_ms = now_millis();
        for entry in self.inner.try_entry_snapshot()? {
            let ttl_ms = entry
                .expire_at_ms
                .map(|deadline| deadline.saturating_sub(now_ms));
            self.cluster.put(&entry.key, &entry.value, ttl_ms)?;
            self.remote_keys.write().insert(
                entry.key,
                RemoteKeyMeta {
                    expire_at_ms: entry.expire_at_ms,
                },
            );
        }
        self.enforce_memory_target();
        Ok(())
    }

    /// Materializes resident and remotely offloaded values for persistence.
    pub fn try_entry_snapshot(&self) -> Result<Vec<StoredEntry>> {
        let _key_gates = self.lock_all_keys();
        let mut entries = self.inner.try_entry_snapshot()?;
        let resident = entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<HashSet<_>>();
        let now_ms = now_millis();
        for (key, meta) in self.remote_keys.read().iter() {
            if resident.contains(key) || meta.expire_at_ms.is_some_and(|expiry| expiry <= now_ms) {
                continue;
            }
            let remote = self.cluster.get(key)?.ok_or_else(|| {
                ShardCacheError::Persistence(format!(
                    "kv overflow snapshot could not materialize key owned by {}",
                    self.cluster.owner_id(key)
                ))
            })?;
            entries.push(StoredEntry {
                key: key.clone(),
                value: remote.value.as_ref().to_vec(),
                expire_at_ms: meta.expire_at_ms,
            });
        }
        entries.sort_by_key(|entry| xxh3_64(&entry.key));
        Ok(entries)
    }

    pub fn health_snapshot(&self) -> KvOverflowHealthSnapshot {
        let metrics = &self.cluster.metrics;
        KvOverflowHealthSnapshot {
            node_count: self.cluster.nodes.len(),
            previous_node_count: self.cluster.previous_nodes.len(),
            slot_count: self.cluster.slot_count,
            resident_keys: self.inner.len(),
            remote_keys: self.remote_keys.read().len(),
            resident_bytes: self.inner.stored_bytes(),
            puts: metrics.puts.load(Ordering::Relaxed),
            put_failures: metrics.put_failures.load(Ordering::Relaxed),
            gets: metrics.gets.load(Ordering::Relaxed),
            get_hits: metrics.get_hits.load(Ordering::Relaxed),
            get_failures: metrics.get_failures.load(Ordering::Relaxed),
            deletes: metrics.deletes.load(Ordering::Relaxed),
            delete_failures: metrics.delete_failures.load(Ordering::Relaxed),
            offloads: metrics.offloads.load(Ordering::Relaxed),
            fault_ins: metrics.fault_ins.load(Ordering::Relaxed),
            queue_depth: self.workers.queue_depth(),
            queue_capacity: self.workers.capacity,
            pending_keys: self.pending_keys.read().len(),
            active_workers: metrics.active_workers.load(Ordering::Relaxed),
            enqueued_puts: metrics.enqueued_puts.load(Ordering::Relaxed),
            enqueue_failures: metrics.enqueue_failures.load(Ordering::Relaxed),
            replicated_puts: metrics.replicated_puts.load(Ordering::Relaxed),
            replication_failures: metrics.replication_failures.load(Ordering::Relaxed),
            handoff_reads: metrics.handoff_reads.load(Ordering::Relaxed),
            handoff_hits: metrics.handoff_hits.load(Ordering::Relaxed),
            handoff_failures: metrics.handoff_failures.load(Ordering::Relaxed),
        }
    }

    fn enforce_memory_target(&self) {
        enforce_memory_target(
            &self.inner,
            &self.cluster,
            &self.options,
            &self.remote_keys,
            &self.maintenance,
        );
    }

    fn key_gate(&self, key: &[u8]) -> parking_lot::MutexGuard<'_, ()> {
        let index = (xxh3_64(key) as usize) & (self.key_gates.len() - 1);
        self.key_gates[index].lock()
    }

    fn lock_all_keys(&self) -> Vec<parking_lot::MutexGuard<'_, ()>> {
        self.key_gates.iter().map(Mutex::lock).collect()
    }
}

impl Drop for KvOverflowStore {
    fn drop(&mut self) {
        self.workers.shutdown();
        self.cleanup.shutdown();
    }
}

impl KvOverflowWorkerPool {
    #[allow(clippy::too_many_arguments)]
    fn start(
        worker_threads: usize,
        capacity: usize,
        inner: Arc<EmbeddedStore>,
        cluster: Arc<KvOverflowCluster>,
        remote_keys: Arc<RwLock<HashMap<Bytes, RemoteKeyMeta>>>,
        pending_keys: Arc<RwLock<HashMap<Bytes, PendingKeyMeta>>>,
        key_gates: Arc<[Mutex<()>]>,
        maintenance: Arc<Mutex<()>>,
        options: KvOverflowOptions,
    ) -> Result<Self> {
        let (permits, available) = bounded(capacity);
        for _ in 0..capacity {
            permits
                .send(())
                .expect("new permit channel must remain connected");
        }
        let mut senders: Vec<Sender<KvOverflowJob>> = Vec::with_capacity(worker_threads);
        let mut joins: Vec<JoinHandle<()>> = Vec::with_capacity(worker_threads);
        for worker_id in 0..worker_threads {
            let (sender, receiver) = unbounded();
            let worker_inner = Arc::clone(&inner);
            let worker_cluster = Arc::clone(&cluster);
            let worker_remote_keys = Arc::clone(&remote_keys);
            let worker_pending_keys = Arc::clone(&pending_keys);
            let worker_key_gates = Arc::clone(&key_gates);
            let worker_maintenance = Arc::clone(&maintenance);
            let worker_options = options.clone();
            let worker_permits = permits.clone();
            let join = match thread::Builder::new()
                .name(format!("shardmap-kv-overflow-{worker_id}"))
                .spawn(move || {
                    let mut failed_since_barrier = false;
                    while let Ok(job) = receiver.recv() {
                        match job {
                            KvOverflowJob::Put {
                                key,
                                value,
                                ttl_ms,
                                expire_at_ms,
                                generation,
                            } => {
                                worker_cluster
                                    .metrics
                                    .active_workers
                                    .fetch_add(1, Ordering::Relaxed);
                                let result = worker_cluster.put(&key, &value, ttl_ms);
                                worker_cluster
                                    .metrics
                                    .active_workers
                                    .fetch_sub(1, Ordering::Relaxed);
                                let succeeded = result.is_ok();
                                if succeeded {
                                    worker_cluster
                                        .metrics
                                        .replicated_puts
                                        .fetch_add(1, Ordering::Relaxed);
                                } else {
                                    failed_since_barrier = true;
                                    worker_cluster
                                        .metrics
                                        .replication_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                let gate_index = key_gate_index(&key, worker_key_gates.len());
                                let _key_gate = worker_key_gates[gate_index].lock();
                                let is_current = worker_pending_keys
                                    .read()
                                    .get(key.as_slice())
                                    .is_some_and(|meta| meta.generation == generation);
                                if is_current {
                                    worker_pending_keys.write().remove(key.as_slice());
                                    if succeeded {
                                        worker_remote_keys
                                            .write()
                                            .insert(key, RemoteKeyMeta { expire_at_ms });
                                        enforce_memory_target(
                                            &worker_inner,
                                            &worker_cluster,
                                            &worker_options,
                                            &worker_remote_keys,
                                            &worker_maintenance,
                                        );
                                    }
                                }
                                let _ = worker_permits.send(());
                            }
                            KvOverflowJob::Delete {
                                key,
                                generation,
                                completion,
                            } => {
                                worker_cluster
                                    .metrics
                                    .active_workers
                                    .fetch_add(1, Ordering::Relaxed);
                                let result = worker_cluster.delete(&key);
                                worker_cluster
                                    .metrics
                                    .active_workers
                                    .fetch_sub(1, Ordering::Relaxed);
                                let succeeded = result.is_ok();
                                if !succeeded {
                                    failed_since_barrier = true;
                                }
                                let gate_index = key_gate_index(&key, worker_key_gates.len());
                                let _key_gate = worker_key_gates[gate_index].lock();
                                let is_current = worker_pending_keys
                                    .read()
                                    .get(key.as_slice())
                                    .is_some_and(|meta| meta.generation == generation);
                                if is_current {
                                    worker_pending_keys.write().remove(key.as_slice());
                                    if succeeded {
                                        let mut remote_keys = worker_remote_keys.write();
                                        remote_keys.remove(key.as_slice());
                                        worker_inner.delete(&key);
                                    }
                                }
                                let _ = completion.send(result);
                                let _ = worker_permits.send(());
                            }
                            KvOverflowJob::Barrier(completion) => {
                                let result = if failed_since_barrier {
                                    Err(ShardCacheError::Protocol(
                                        "one or more key-value overflow mutations failed".into(),
                                    ))
                                } else {
                                    Ok(())
                                };
                                failed_since_barrier = false;
                                let _ = completion.send(result);
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
            available,
            permits,
            capacity,
            metrics: Arc::clone(&cluster.metrics),
            joins: Mutex::new(joins),
        })
    }

    fn reserve(&self) -> Result<()> {
        match self.available.try_recv() {
            Ok(()) => Ok(()),
            Err(TryRecvError::Empty) => {
                self.metrics
                    .enqueue_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err(ShardCacheError::Backpressure(
                    "kv overflow replication queue is full",
                ))
            }
            Err(TryRecvError::Disconnected) => {
                Err(ShardCacheError::ChannelClosed("kv overflow permits"))
            }
        }
    }

    fn enqueue_reserved(&self, job: KvOverflowJob) -> bool {
        let lane = match &job {
            KvOverflowJob::Put { key, .. } | KvOverflowJob::Delete { key, .. } => {
                key_gate_index(key, self.senders.len())
            }
            KvOverflowJob::Barrier(_) | KvOverflowJob::Shutdown => 0,
        };
        if self.senders[lane].send(job).is_ok() {
            true
        } else {
            let _ = self.permits.send(());
            false
        }
    }

    fn queue_depth(&self) -> usize {
        self.capacity.saturating_sub(self.available.len())
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

fn enforce_memory_target(
    inner: &EmbeddedStore,
    cluster: &KvOverflowCluster,
    options: &KvOverflowOptions,
    remote_keys: &RwLock<HashMap<Bytes, RemoteKeyMeta>>,
    maintenance: &Mutex<()>,
) {
    let _maintenance = maintenance.lock();
    while inner.stored_bytes() > options.max_memory_bytes {
        let remote_keys = remote_keys.read();
        let now_ms = now_millis();
        let mut evicted = false;
        for shard_id in 0..inner.shard_count() {
            let victim =
                inner.evict_one_point_in_shard_if(shard_id, options.eviction_policy, |key| {
                    remote_keys
                        .get(key)
                        .is_some_and(|meta| meta.expire_at_ms.is_none_or(|expiry| expiry > now_ms))
                });
            if victim.is_some() {
                cluster.metrics.offloads.fetch_add(1, Ordering::Relaxed);
                evicted = true;
                if inner.stored_bytes() <= options.max_memory_bytes {
                    break;
                }
            }
        }
        if !evicted {
            break;
        }
    }
}

fn key_gate_index(key: &[u8], count: usize) -> usize {
    debug_assert!(count > 0);
    (xxh3_64(key) as usize) % count
}

impl KvOverflowCleanupTask {
    fn start(
        cluster: Arc<KvOverflowCluster>,
        remote_keys: Arc<RwLock<HashMap<Bytes, RemoteKeyMeta>>>,
        key_gates: Arc<[Mutex<()>]>,
        interval: Duration,
    ) -> Result<Self> {
        let (shutdown, receiver) = bounded(1);
        let join = std::thread::Builder::new()
            .name("shardmap-kv-overflow-cleanup".into())
            .spawn(move || {
                loop {
                    match receiver.recv_timeout(interval) {
                        Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                    let now_ms = now_millis();
                    let expired = remote_keys
                        .read()
                        .iter()
                        .filter(|(_key, meta)| {
                            meta.expire_at_ms.is_some_and(|expiry| expiry <= now_ms)
                        })
                        .map(|(key, meta)| (key.clone(), meta.expire_at_ms))
                        .collect::<Vec<_>>();
                    for (key, expected_expiry) in expired {
                        let gate_index = (xxh3_64(&key) as usize) & (key_gates.len() - 1);
                        let _key_gate = key_gates[gate_index].lock();
                        let still_expired = remote_keys
                            .read()
                            .get(key.as_slice())
                            .is_some_and(|meta| meta.expire_at_ms == expected_expiry);
                        if still_expired && cluster.delete(&key).is_ok() {
                            remote_keys.write().remove(key.as_slice());
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
    use std::sync::atomic::AtomicBool;

    #[derive(Debug)]
    struct MemoryNode {
        id: String,
        values: RwLock<HashMap<Bytes, KvOverflowValue>>,
        fail_puts: bool,
        fail_gets: bool,
        fail_deletes: bool,
    }

    impl MemoryNode {
        fn new(id: &str) -> Self {
            Self {
                id: id.into(),
                values: RwLock::new(HashMap::new()),
                fail_puts: false,
                fail_gets: false,
                fail_deletes: false,
            }
        }
    }

    impl KvOverflowNode for MemoryNode {
        fn id(&self) -> &str {
            &self.id
        }

        fn put(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> Result<()> {
            if self.fail_puts {
                return Err(ShardCacheError::Protocol("injected put failure".into()));
            }
            self.values.write().insert(
                key.to_vec(),
                KvOverflowValue {
                    value: SharedBytes::copy_from_slice(value),
                    ttl_ms,
                },
            );
            Ok(())
        }

        fn get(&self, key: &[u8]) -> Result<Option<KvOverflowValue>> {
            if self.fail_gets {
                return Err(ShardCacheError::Protocol("injected get failure".into()));
            }
            Ok(self.values.read().get(key).cloned())
        }

        fn delete(&self, key: &[u8]) -> Result<()> {
            if self.fail_deletes {
                return Err(ShardCacheError::Protocol("injected delete failure".into()));
            }
            self.values.write().remove(key);
            Ok(())
        }
    }

    struct BlockingNode {
        id: String,
        started: Sender<()>,
        release: Receiver<()>,
        values: RwLock<HashMap<Bytes, KvOverflowValue>>,
        block_once: AtomicBool,
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
            eviction_policy: EvictionPolicy::Lfu,
            fetch_on_miss: true,
            cleanup_interval: Duration::from_secs(60),
            worker_threads: 2,
            queue_capacity: 64,
        }
    }

    #[test]
    fn rendezvous_distribution_is_stable_across_node_order() {
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
    fn horizontal_expansion_moves_whole_slots_only_to_the_added_node() {
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
                assert_eq!(new_owner, "node-c");
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
            .find(|key| original.owner_id(key.as_bytes()) != expanded.owner_id(key.as_bytes()))
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
    fn previous_owner_failure_is_reported_without_falling_through() {
        let mut first_node = MemoryNode::new("node-a");
        first_node.fail_gets = true;
        let first = Arc::new(first_node);
        let mut second_node = MemoryNode::new("node-b");
        second_node.fail_gets = true;
        let second = Arc::new(second_node);
        let added = Arc::new(MemoryNode::new("node-c"));
        let original = KvOverflowCluster::new(vec![first.clone(), second.clone()]).unwrap();
        let expanded = KvOverflowCluster::with_previous(
            vec![first.clone(), second.clone(), added],
            vec![first, second],
            DEFAULT_KV_OVERFLOW_SLOT_COUNT,
        )
        .unwrap();
        let key = (0..100_000)
            .map(|index| format!("moving-key-{index}"))
            .find(|key| original.owner_id(key.as_bytes()) != expanded.owner_id(key.as_bytes()))
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
        assert_eq!(store.health_snapshot().pending_keys, 1);

        release.send(()).unwrap();
        store.flush_remote().unwrap();
        assert_eq!(store.health_snapshot().pending_keys, 0);
    }

    #[test]
    fn full_queue_rejects_without_mutating_primary() {
        let (node, started, release) = blocking_node();
        let cluster = Arc::new(KvOverflowCluster::new(vec![node]).unwrap());
        let mut constrained = options(1024);
        constrained.worker_threads = 1;
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
        store.flush_remote().unwrap();

        assert_eq!(node.values.read()[b"key".as_slice()].value.as_ref(), &[99]);
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
        let mut node = MemoryNode::new("node-a");
        node.fail_puts = true;
        let cluster = Arc::new(KvOverflowCluster::new(vec![Arc::new(node)]).unwrap());
        let store = KvOverflowStore::new(EmbeddedStore::new(1), cluster, options(1)).unwrap();

        store.set(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        assert!(store.flush_remote().is_err());
        assert_eq!(store.inner().get(b"key"), Some(b"value".to_vec()));
        assert_eq!(store.health_snapshot().offloads, 0);
        assert_eq!(store.health_snapshot().replication_failures, 1);
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
        let mut node = MemoryNode::new("node-a");
        node.fail_deletes = true;
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
}
