//! Embedded storage APIs, routing helpers, batch payloads, and runtime stats.
//!
//! Most applications should start with [`crate::ShardMap`], which is a
//! cloneable DashMap-like facade over [`SharedEmbeddedStore`]. This module
//! exposes the lower-level storage surfaces behind that facade. Use
//! [`EmbeddedStore`] for the richer shared sharded engine used by server mode.
//! Use [`LocalEmbeddedStore`] when work is pinned to owner-local worker threads
//! and each worker can access its assigned shards through exclusive `&mut`
//! handles.
//!
//! Keys and values are byte vectors (`Vec<u8>`). TTL arguments accepted by
//! write methods are milliseconds relative to the current time; expiration
//! methods that accept `expire_at_ms` use an absolute Unix timestamp in
//! milliseconds.
//!
//! Read APIs are intentionally split by ownership. `get_ref` borrows the stored
//! value without copying bytes and keeps the returned guard or slice tied to
//! the store borrow. `get` returns an owned, materialized `Vec<u8>` for callers
//! that need to keep the value independently. `get_mut` returns a routed guard
//! for replacing or removing an existing point value while preserving its TTL.
//! With the `mutable-value-slices` feature, mutation guards also expose
//! `value_mut_no_ttl` for in-place `&mut [u8]` access to uniquely-owned no-TTL
//! values. TTL values are rejected by that API; use `set_slice` when expiry
//! preservation matters.

mod command;
mod embedded_store;
#[cfg(feature = "sharded")]
mod embedded_store_sharded;
mod embedded_store_shared;
mod engine;
mod flat_map;
mod records;
#[cfg(feature = "redis")]
#[path = "redis_compat/storage/redis_objects.rs"]
mod redis_objects;
mod semantic;
mod stats;
#[cfg(feature = "telemetry")]
mod telemetry;

pub use command::{BorrowedCommand, Command};
#[cfg(feature = "sharded")]
pub use embedded_store::OwnedEmbeddedSessionPackedView as LocalEmbeddedSessionPackedView;
#[doc(hidden)]
pub use embedded_store::ShardArcEmbeddedStore;
#[cfg(feature = "redis-module-timeseries")]
pub(crate) use embedded_store::TimeSeriesMultiRangeWriter;
#[cfg(feature = "redis-module-topk")]
pub(crate) use embedded_store::TopKError;
#[cfg(feature = "redis")]
pub(crate) use embedded_store::{
    DEFAULT_SCAN_COUNT, RedisHashStore, RedisKeyScanType, RedisKeyStore, RedisListStore,
    RedisObjectStoreAccess, RedisSetStore, RedisStringStore, RedisZSetStore,
};
pub use embedded_store::{
    EmbeddedBatchReadView, EmbeddedKeyRoute, EmbeddedReadSlice, EmbeddedReadView, EmbeddedRef,
    EmbeddedRefMut, EmbeddedRouteMode, EmbeddedSessionBatchView, EmbeddedSessionRoute,
    EmbeddedShardHandle, EmbeddedStore, OwnedEmbeddedBatchReadView, OwnedEmbeddedReadView,
    OwnedEmbeddedRefMut, OwnedEmbeddedSessionBatchView, OwnedEmbeddedSessionPackedView,
    OwnedEmbeddedShard, OwnedEmbeddedWorkerReadSession, OwnedEmbeddedWorkerShards,
    PackedSessionWrite, shift_for, stripe_index,
};
#[cfg(feature = "redis-modules")]
pub use embedded_store::{RedisModuleApi, RedisModuleApiResult, RedisModuleFamily};
#[cfg(feature = "sharded")]
pub use embedded_store_sharded::{
    LocalRouteError, LocalStoreAccessError, LocalStoreInstallError,
    WorkerLocalBatchReadView as LocalEmbeddedBatchReadView,
    WorkerLocalEmbeddedRefMut as LocalEmbeddedRefMut,
    WorkerLocalEmbeddedStore as LocalEmbeddedStore,
    WorkerLocalEmbeddedStoreBootstrap as LocalEmbeddedStoreBootstrap,
    WorkerLocalReadSlice as LocalEmbeddedReadSlice, WorkerLocalReadView as LocalEmbeddedReadView,
    WorkerLocalSessionBatchView as LocalEmbeddedSessionBatchView,
};
pub use embedded_store_shared::{
    Entry as SharedEmbeddedEntry, Ref as SharedEmbeddedRef, RefMut as SharedEmbeddedRefMut,
    SharedEmbeddedConfig, SharedEmbeddedLockPolicy, SharedEmbeddedStore,
    VacantEntry as SharedEmbeddedVacantEntry,
};
#[cfg(feature = "redis")]
pub(crate) use redis_objects::RedisObjectArrayItem;

#[cfg(feature = "sharded")]
pub fn with_local_embedded_store<R>(
    f: impl FnOnce(&mut LocalEmbeddedStore) -> R,
) -> Result<R, LocalStoreAccessError> {
    embedded_store_sharded::with_thread_local_embedded_store(f)
}

#[cfg(feature = "sharded")]
pub fn take_local_embedded_store() -> Option<LocalEmbeddedStore> {
    embedded_store_sharded::take_thread_local_embedded_store()
}
pub use engine::EngineHandle;
pub(crate) use engine::{
    EngineCommandContext, EngineFastFuture, EngineFrameFuture, EngineRespSpanFuture,
    ExpirationChange, RESP_SPANNED_VALUE_MIN, ShardKey, ShardOperation, ShardReply, ShardValue,
};
pub use flat_map::FlatMap;
pub use records::{MutationBytes, MutationOp, MutationRecord, StoredEntry};
#[cfg(feature = "redis")]
pub(crate) use redis_objects::{
    HashFieldExpireCond, HashFieldGetExpireAction, HashFieldSetCondition, HashFieldSetExpireAction,
};
#[cfg(feature = "redis")]
pub(crate) use redis_objects::{
    RedisObjectBucket, RedisObjectReadOutcome, RedisObjectStore, RedisObjectValue,
    RedisObjectWriteAttempt, RedisObjectZSetRangeItem, WRONGTYPE_MESSAGE,
};
#[cfg(feature = "redis")]
pub use redis_objects::{RedisObjectError, RedisObjectResult, RedisStringLookup};
pub use semantic::{SemanticCacheError, SemanticMatch};
pub(crate) use semantic::{
    SemanticEmbedding, SemanticIndex, SemanticIndexCandidate, SemanticIndexToken,
    validate_similarity_threshold,
};
pub use stats::{GlobalStatsSnapshot, ShardStatsSnapshot, TierStatsSnapshot, WalStatsSnapshot};
use std::collections::{HashMap, HashSet};
use std::sync::Once;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(feature = "telemetry")]
pub use telemetry::{CacheMetrics, CacheMetricsSnapshot, CacheTelemetry, CacheTelemetryHandle};

/// Owned byte buffer used for cache keys and values.
pub type Bytes = Vec<u8>;
#[cfg(feature = "redis")]
pub(crate) const VECTOR_SET_PREFIX: &[u8] = b"FC:VSET:v1\0";
/// Hash map with the crate's default XXH3 hasher.
pub type FastHashMap<K, V> = HashMap<K, V, xxhash_rust::xxh3::Xxh3DefaultBuilder>;
/// Hash set with the crate's default XXH3 hasher.
pub type FastHashSet<T> = HashSet<T, xxhash_rust::xxh3::Xxh3DefaultBuilder>;

/// A packed copy-out batch result.
///
/// The payload bytes are concatenated into a single contiguous buffer to keep
/// Python object creation and allocator churn low while we still return owned
/// data. `offsets[index] == usize::MAX` marks a miss.
#[derive(Debug, Clone, Default)]
pub struct PackedBatch {
    /// Concatenated value bytes for all hits.
    pub buffer: Bytes,
    /// Start offset for each requested item, or `usize::MAX` for a miss.
    pub offsets: Vec<usize>,
    /// Value length for each requested item.
    pub lengths: Vec<usize>,
    /// Number of requested keys that were present.
    pub hit_count: usize,
}

/// Precomputed routing and exact-match metadata for repeated point lookups.
///
/// This token owns the original key bytes plus precomputed routing and
/// hash-derived tag state so callers can reuse lookup preparation work while still
/// preserving exact byte-for-byte key semantics on every hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedPointKey {
    pub(crate) route: EmbeddedKeyRoute,
    pub(crate) key_len: usize,
    pub(crate) key_tag: u64,
    pub(crate) key: Bytes,
}

impl PreparedPointKey {
    /// Returns the precomputed route for this key.
    #[inline(always)]
    pub fn route(&self) -> EmbeddedKeyRoute {
        self.route
    }

    /// Returns the original key length in bytes.
    #[inline(always)]
    pub fn key_len(&self) -> usize {
        self.key_len
    }

    /// Returns the precomputed primary-hash-derived key tag.
    #[inline(always)]
    pub fn key_tag(&self) -> u64 {
        self.key_tag
    }

    /// Returns the original key bytes.
    #[inline(always)]
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

impl PackedBatch {
    /// Returns the total number of value bytes copied into `buffer`.
    #[inline(always)]
    pub fn total_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the number of keys requested in the batch.
    #[inline(always)]
    pub fn item_count(&self) -> usize {
        self.offsets.len()
    }

    /// Returns true when every requested key was present.
    #[inline(always)]
    pub fn all_hit(&self) -> bool {
        self.hit_count == self.item_count()
    }
}

/// Computes the crate's primary XXH3 key hash.
#[inline(always)]
pub fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}

/// Computes the key tag associated with an already-computed primary key hash.
///
/// This mirrors hash-table control-byte style filtering without hashing the
/// key bytes a second time. Exact-match paths still compare the key bytes.
#[inline(always)]
pub fn hash_key_tag_from_hash(hash: u64) -> u64 {
    hash >> 56
}

/// Computes the primary-hash-derived key tag used by prepared point lookups.
#[inline(always)]
pub fn hash_key_tag(key: &[u8]) -> u64 {
    hash_key_tag_from_hash(hash_key(key))
}

/// Returns the current Unix timestamp in milliseconds.
pub fn now_millis() -> u64 {
    exact_now_millis()
}

/// Returns a cached Unix timestamp in milliseconds for TTL hot paths.
///
/// Expiration checks only need millisecond granularity, but a full wall-clock
/// read on every cache hit is expensive at saturation. This starts a small
/// process-local updater on first TTL use and lets readers load the current
/// millisecond from an atomic. If the updater cannot start, callers fall back
/// to the exact clock.
#[inline(always)]
pub(crate) fn ttl_now_millis() -> u64 {
    TTL_CLOCK_START.call_once(start_ttl_clock);
    if TTL_CLOCK_RUNNING.load(Ordering::Relaxed) {
        TTL_CLOCK_MS.load(Ordering::Relaxed)
    } else {
        exact_now_millis()
    }
}

static TTL_CLOCK_START: Once = Once::new();
static TTL_CLOCK_RUNNING: AtomicBool = AtomicBool::new(false);
static TTL_CLOCK_MS: AtomicU64 = AtomicU64::new(0);

fn start_ttl_clock() {
    TTL_CLOCK_MS.store(exact_now_millis(), Ordering::Relaxed);
    match thread::Builder::new()
        .name("shardcache-ttl-clock".to_string())
        .spawn(|| {
            loop {
                TTL_CLOCK_MS.store(exact_now_millis(), Ordering::Relaxed);
                thread::sleep(Duration::from_millis(1));
            }
        }) {
        Ok(_handle) => TTL_CLOCK_RUNNING.store(true, Ordering::Relaxed),
        Err(_error) => {}
    }
}

#[inline(always)]
fn exact_now_millis() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_millis() as u64
}
