//! DashMap-like embedded cache facade.
//!
//! [`FastMap`] is the ergonomic cloneable embedded API. It wraps
//! [`SharedEmbeddedStore`], so callers get
//! a cheap handle they can move into many workers while still using the same
//! sharded storage core as server and owner-local embedded modes.

use bytes::Bytes as SharedBytes;

use crate::config::EvictionPolicy;
use crate::storage::{
    EmbeddedKeyRoute, EmbeddedRouteMode, PreparedPointKey, SharedEmbeddedConfig,
    SharedEmbeddedEntry, SharedEmbeddedLockPolicy, SharedEmbeddedRef, SharedEmbeddedRefMut,
    SharedEmbeddedStore, SharedEmbeddedVacantEntry,
};

/// Default number of stripes for [`FastMap`].
///
/// This intentionally over-stripes the common embedded shared-handle case. In
/// benchmarks, one stripe per worker leaves read-heavy shared access
/// under-striped compared with DashMap-style maps.
pub const DEFAULT_CACHE_SHARDS: usize = 64;

/// Configuration for [`FastMap`].
///
/// The shard count is a const generic on [`SharedCache`], keeping the hot path
/// monomorphized while [`FastMap`] gives users a simple default.
#[derive(Debug, Clone)]
pub struct CacheOptions {
    /// Total memory budget for all stripes. `None` disables memory-limit eviction.
    pub total_memory_bytes: Option<usize>,
    /// Eviction policy applied independently inside each stripe.
    pub eviction_policy: EvictionPolicy,
    /// Key routing mode used by point and session APIs.
    pub route_mode: EmbeddedRouteMode,
    /// Approximate total point-key capacity to reserve across all stripes.
    pub capacity_hint: Option<usize>,
    /// Lock policy used by each stripe.
    pub lock_policy: SharedEmbeddedLockPolicy,
}

impl Default for CacheOptions {
    fn default() -> Self {
        let shared = SharedEmbeddedConfig::default();
        Self {
            total_memory_bytes: shared.total_memory_bytes,
            eviction_policy: shared.eviction_policy,
            route_mode: shared.route_mode,
            capacity_hint: shared.flat_map_capacity_hint,
            lock_policy: shared.lock_policy,
        }
    }
}

impl From<CacheOptions> for SharedEmbeddedConfig {
    fn from(options: CacheOptions) -> Self {
        Self {
            total_memory_bytes: options.total_memory_bytes,
            eviction_policy: options.eviction_policy,
            route_mode: options.route_mode,
            flat_map_capacity_hint: options.capacity_hint,
            lock_policy: options.lock_policy,
        }
    }
}

impl From<SharedEmbeddedConfig> for CacheOptions {
    fn from(config: SharedEmbeddedConfig) -> Self {
        Self {
            total_memory_bytes: config.total_memory_bytes,
            eviction_policy: config.eviction_policy,
            route_mode: config.route_mode,
            capacity_hint: config.flat_map_capacity_hint,
            lock_policy: config.lock_policy,
        }
    }
}

/// Borrowed read guard returned by [`SharedCache::get`].
pub type CacheRef<'a> = SharedEmbeddedRef<'a>;
/// Mutable point-key guard returned by [`SharedCache::get_mut`].
pub type CacheRefMut<'a> = SharedEmbeddedRefMut<'a>;
/// Entry API guard returned by [`SharedCache::entry`].
pub type CacheEntry<'a> = SharedEmbeddedEntry<'a>;
/// Vacant entry guard used by [`CacheEntry`].
pub type CacheVacantEntry<'a> = SharedEmbeddedVacantEntry<'a>;

/// Cloneable DashMap-like embedded cache.
///
/// This is the recommended starting point for application code that wants a
/// drop-in in-process cache or lock table. Use
/// [`embedded::ShardedEngine`](crate::embedded::ShardedEngine) and
/// [`embedded::LocalEmbeddedStore`](crate::embedded::LocalEmbeddedStore) when
/// you want the owner-local sharded performance model.
#[derive(Debug, Clone)]
pub struct SharedCache<const SHARDS: usize> {
    inner: SharedEmbeddedStore<SHARDS>,
}

/// Default cloneable DashMap-like embedded map.
pub type FastMap = SharedCache<DEFAULT_CACHE_SHARDS>;

/// Cache-flavored alias for users thinking in TTL/LRU/cache terms.
pub type FastCache = FastMap;

/// Explicit-shard-count alias for map-shaped users.
pub type FastMapWithShards<const SHARDS: usize> = SharedCache<SHARDS>;

/// Explicit-shard-count cache-flavored alias.
pub type FastCacheWithShards<const SHARDS: usize> = SharedCache<SHARDS>;

impl<const SHARDS: usize> Default for SharedCache<SHARDS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const SHARDS: usize> SharedCache<SHARDS> {
    /// Creates a cache with default options.
    #[inline(always)]
    pub fn new() -> Self {
        Self::with_options(CacheOptions::default())
    }

    /// Creates a cache with a total point-key capacity hint.
    #[inline(always)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_options(CacheOptions {
            capacity_hint: Some(capacity),
            ..CacheOptions::default()
        })
    }

    /// Creates a cache with explicit options.
    #[inline(always)]
    pub fn with_options(options: CacheOptions) -> Self {
        Self {
            inner: SharedEmbeddedStore::new(options.into()),
        }
    }

    /// Wraps an existing shared embedded store.
    #[inline(always)]
    pub fn from_shared_store(inner: SharedEmbeddedStore<SHARDS>) -> Self {
        Self { inner }
    }

    /// Returns the underlying shared embedded store.
    #[inline(always)]
    pub fn as_shared_store(&self) -> &SharedEmbeddedStore<SHARDS> {
        &self.inner
    }

    /// Consumes this facade and returns the underlying shared embedded store.
    #[inline(always)]
    pub fn into_shared_store(self) -> SharedEmbeddedStore<SHARDS> {
        self.inner
    }

    /// Returns the configured number of cache stripes.
    #[inline(always)]
    pub const fn shard_count(&self) -> usize {
        SHARDS
    }

    /// Returns the configured route mode.
    #[inline(always)]
    pub fn route_mode(&self) -> EmbeddedRouteMode {
        self.inner.route_mode()
    }

    /// Computes the route for a key.
    #[inline(always)]
    pub fn route_key(&self, key: &[u8]) -> EmbeddedKeyRoute {
        self.inner.route_key(key)
    }

    /// Precomputes route and exact-match metadata for repeated point lookups.
    #[inline(always)]
    pub fn prepare_key(&self, key: &[u8]) -> PreparedPointKey {
        self.inner.prepare_point_key(key)
    }

    /// Returns a borrowed value guard for `key`.
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<CacheRef<'_>> {
        self.inner.get_ref(key)
    }

    /// Returns a borrowed value guard for `key`.
    #[inline(always)]
    pub fn get_ref(&self, key: &[u8]) -> Option<CacheRef<'_>> {
        self.inner.get_ref(key)
    }

    /// Returns a borrowed value guard for a prepared point key.
    #[inline(always)]
    pub fn get_prepared(&self, prepared: &PreparedPointKey) -> Option<CacheRef<'_>> {
        self.inner.get_prepared_ref(prepared)
    }

    /// Returns a refcount-only clone of the stored bytes for `key`.
    ///
    /// This releases the shard read lock before the caller inspects or copies
    /// the value.
    #[inline(always)]
    pub fn get_owned(&self, key: &[u8]) -> Option<SharedBytes> {
        self.inner.get_value_bytes(key)
    }

    /// Returns a refcount-only clone of the stored bytes for a prepared key.
    #[inline(always)]
    pub fn get_prepared_owned(&self, prepared: &PreparedPointKey) -> Option<SharedBytes> {
        self.inner.get_prepared_value_bytes(prepared)
    }

    /// Returns true when `key` is present in point-key storage.
    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    /// Returns true when `key` is present in point-key storage.
    #[inline(always)]
    pub fn exists(&self, key: &[u8]) -> bool {
        self.contains_key(key)
    }

    /// Inserts or replaces a point-key value without a TTL.
    #[inline(always)]
    pub fn insert<K, V>(&self, key: K, value: V)
    where
        K: Into<SharedBytes>,
        V: Into<SharedBytes>,
    {
        self.inner.insert(key.into(), value.into());
    }

    /// Inserts or replaces a point-key value from borrowed slices.
    #[inline(always)]
    pub fn insert_slice(&self, key: &[u8], value: &[u8]) {
        self.inner.insert_slice(key, value);
    }

    /// Inserts or replaces a point-key value with an optional relative TTL.
    #[inline(always)]
    pub fn insert_with_ttl<K, V>(&self, key: K, value: V, ttl_ms: Option<u64>)
    where
        K: Into<SharedBytes>,
        V: Into<SharedBytes>,
    {
        self.inner.insert_with_ttl(key.into(), value.into(), ttl_ms);
    }

    /// Inserts or replaces a point-key value from borrowed bytes with a TTL.
    #[inline(always)]
    pub fn insert_slice_with_ttl(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) {
        self.inner.insert_slice_with_ttl(key, value, ttl_ms);
    }

    /// Inserts a point-key value only when the key is absent or expired.
    #[inline(always)]
    pub fn try_insert<K, V>(&self, key: K, value: V) -> bool
    where
        K: Into<SharedBytes>,
        V: Into<SharedBytes>,
    {
        self.inner.insert_if_absent(key.into(), value.into())
    }

    /// Inserts borrowed bytes only when the key is absent or expired.
    #[inline(always)]
    pub fn try_insert_slice(&self, key: &[u8], value: &[u8]) -> bool {
        self.inner.insert_slice_if_absent(key, value)
    }

    /// Inserts borrowed bytes with an optional TTL only when the key is absent
    /// or expired.
    #[inline(always)]
    pub fn try_insert_slice_with_ttl(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) -> bool {
        self.inner
            .insert_slice_if_absent_with_ttl(key, value, ttl_ms)
    }

    /// Returns a mutable point-key guard for `key`.
    #[inline(always)]
    pub fn get_mut(&self, key: &[u8]) -> Option<CacheRefMut<'_>> {
        self.inner.get_mut(key)
    }

    /// Removes a point-key value and returns the stored bytes when present.
    #[inline(always)]
    pub fn remove(&self, key: &[u8]) -> Option<SharedBytes> {
        self.inner.remove(key)
    }

    /// Locks the routed stripe and returns an occupied or vacant entry.
    #[inline(always)]
    pub fn entry<K>(&self, key: K) -> CacheEntry<'_>
    where
        K: Into<SharedBytes>,
    {
        self.inner.entry(key.into())
    }

    /// Acquires a Redis-style token lock using `SET key token NX PX ttl`.
    ///
    /// This is process-local when used as an embedded cache. Use the server
    /// surface when multiple processes or machines need to coordinate through
    /// one lock table.
    #[inline(always)]
    pub fn try_acquire_lock(&self, key: &[u8], token: &[u8], ttl_ms: u64) -> bool {
        assert!(ttl_ms > 0, "lock ttl_ms must be greater than zero");
        self.inner
            .insert_slice_if_absent_with_ttl(key, token, Some(ttl_ms))
    }

    /// Releases a token lock only when the stored token matches.
    #[inline(always)]
    pub fn release_lock(&self, key: &[u8], token: &[u8]) -> bool {
        self.inner.remove_if_value_eq(key, token)
    }

    /// Renews a token lock only when the stored token matches.
    #[inline(always)]
    pub fn renew_lock(&self, key: &[u8], token: &[u8], ttl_ms: u64) -> bool {
        assert!(ttl_ms > 0, "lock ttl_ms must be greater than zero");
        self.inner.update_ttl_if_value_eq(key, token, ttl_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_map_round_trips_like_a_shared_map() {
        let cache = SharedCache::<4>::with_capacity(16);
        cache.insert_slice(b"alpha", b"one");

        assert_eq!(cache.get(b"alpha").unwrap().value(), b"one");
        assert_eq!(cache.get_owned(b"alpha").unwrap().as_ref(), b"one");
        assert!(cache.contains_key(b"alpha"));

        cache.get_mut(b"alpha").unwrap().set_slice(b"two");
        assert_eq!(cache.remove(b"alpha").unwrap().as_ref(), b"two");
        assert!(!cache.contains_key(b"alpha"));
    }

    #[test]
    fn fast_map_try_insert_only_writes_missing_keys() {
        let cache = SharedCache::<4>::new();

        assert!(cache.try_insert_slice(b"alpha", b"one"));
        assert!(!cache.try_insert_slice(b"alpha", b"two"));
        assert_eq!(cache.get(b"alpha").unwrap().value(), b"one");
    }

    #[cfg(not(feature = "no-ttl"))]
    #[test]
    fn fast_map_token_locks_are_compare_and_delete() {
        let cache = SharedCache::<4>::new();

        assert!(cache.try_acquire_lock(b"lock:alpha", b"token-1", 60_000));
        assert!(!cache.try_acquire_lock(b"lock:alpha", b"token-2", 60_000));
        assert!(!cache.release_lock(b"lock:alpha", b"token-2"));
        assert!(cache.renew_lock(b"lock:alpha", b"token-1", 60_000));
        assert!(cache.release_lock(b"lock:alpha", b"token-1"));
        assert!(cache.try_acquire_lock(b"lock:alpha", b"token-2", 60_000));
    }
}
