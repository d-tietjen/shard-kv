//! Native Rust typed map storage.
//!
//! [`ShardMap`] stores Rust `K` and `V` values directly in sharded hash maps.
//! This is the DashMap-like surface for ordinary embedded users. Use the
//! `codec` feature's `CodecShardMap` when you specifically need a typed facade
//! over the shared byte engine used by Redis/RESP/SCNP paths.

use std::array;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_utils::CachePadded;
use parking_lot::{RwLock, RwLockReadGuard};

use crate::cache::DEFAULT_CACHE_SHARDS;
use crate::storage::EmbeddedKeyRoute;

/// Randomly keyed hasher used by the native typed [`ShardMap`].
pub type ShardMapHasher = crate::storage::FastHashBuilder;

type NativeShard<K, V> = CachePadded<RwLock<HashMap<K, NativeValue<V>, ShardMapHasher>>>;

/// Configuration for the native typed [`ShardMap`].
#[derive(Debug, Clone, Default)]
pub struct ShardMapOptions {
    /// Approximate total point-key capacity to reserve across all shards.
    pub capacity_hint: Option<usize>,
    /// Default relative TTL, in milliseconds, applied by write APIs that do not
    /// pass an explicit TTL.
    pub default_ttl_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct NativeValue<V> {
    value: V,
    expire_at_ms: Option<u64>,
}

impl<V> NativeValue<V> {
    #[inline(always)]
    fn new(value: V, expire_at_ms: Option<u64>) -> Self {
        Self {
            value,
            expire_at_ms,
        }
    }

    #[inline(always)]
    fn is_live(&self, now_ms: u64) -> bool {
        self.expire_at_ms
            .is_none_or(|expire_at_ms| expire_at_ms > now_ms)
    }

    #[inline(always)]
    fn into_live_value(self, now_ms: u64) -> Option<V> {
        self.is_live(now_ms).then_some(self.value)
    }
}

/// Borrowed read guard returned by [`ShardMap::get_ref`].
pub struct ShardMapRef<'a, K, V> {
    guard: RwLockReadGuard<'a, HashMap<K, NativeValue<V>, ShardMapHasher>>,
    value: *const V,
    _not_send: PhantomData<*const ()>,
}

impl<K, V> ShardMapRef<'_, K, V> {
    /// Returns the borrowed value.
    #[inline(always)]
    pub fn value(&self) -> &V {
        let _guard = &self.guard;
        // SAFETY: `value` points into the hash map protected by `guard`, and
        // the read guard is held for this `ShardMapRef`'s full lifetime.
        unsafe { &*self.value }
    }
}

impl<K, V> Deref for ShardMapRef<'_, K, V> {
    type Target = V;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

/// Cloneable native Rust sharded map.
///
/// `ShardMap<K, V>` stores `K` and `V` directly. Keys must implement
/// [`Hash`] and [`Eq`], matching the ordinary Rust `HashMap` contract.
pub struct ShardMap<K, V, const SHARDS: usize = DEFAULT_CACHE_SHARDS> {
    shards: Arc<[NativeShard<K, V>; SHARDS]>,
    hasher: ShardMapHasher,
    default_ttl_ms: Option<u64>,
}

/// Native typed map with an explicit compile-time shard count.
pub type ShardMapWithShards<const SHARDS: usize, K, V> = ShardMap<K, V, SHARDS>;

impl<K, V, const SHARDS: usize> Clone for ShardMap<K, V, SHARDS> {
    fn clone(&self) -> Self {
        Self {
            shards: Arc::clone(&self.shards),
            hasher: self.hasher.clone(),
            default_ttl_ms: self.default_ttl_ms,
        }
    }
}

impl<K, V, const SHARDS: usize> Default for ShardMap<K, V, SHARDS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, const SHARDS: usize> fmt::Debug for ShardMap<K, V, SHARDS> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShardMap")
            .field("shard_count", &SHARDS)
            .field("default_ttl_ms", &self.default_ttl_ms)
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<K, V, const SHARDS: usize> ShardMap<K, V, SHARDS> {
    /// Creates an empty native typed map.
    pub fn new() -> Self {
        Self::with_options(ShardMapOptions::default())
    }

    /// Creates an empty native typed map with an approximate total capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_options(ShardMapOptions {
            capacity_hint: Some(capacity),
            ..ShardMapOptions::default()
        })
    }

    /// Creates an empty native typed map with explicit options.
    pub fn with_options(options: ShardMapOptions) -> Self {
        const {
            assert!(
                SHARDS > 0 && SHARDS.is_power_of_two(),
                "SHARDS must be a non-zero power of two"
            );
        }
        let per_shard_capacity = options
            .capacity_hint
            .map(|capacity| capacity.div_ceil(SHARDS))
            .unwrap_or_default();
        let hasher = ShardMapHasher::default();
        Self {
            shards: Arc::new(array::from_fn(|_| {
                let map = match per_shard_capacity {
                    0 => HashMap::with_hasher(hasher.clone()),
                    capacity => HashMap::with_capacity_and_hasher(capacity, hasher.clone()),
                };
                CachePadded::new(RwLock::new(map))
            })),
            hasher,
            default_ttl_ms: options.default_ttl_ms,
        }
    }

    /// Returns the configured number of shards.
    #[inline(always)]
    pub const fn shard_count(&self) -> usize {
        SHARDS
    }

    /// Computes the route for a key.
    #[inline(always)]
    pub fn route_key<Q>(&self, key: &Q) -> EmbeddedKeyRoute
    where
        Q: Hash + ?Sized,
    {
        let key_hash = self.hash_key(key);
        EmbeddedKeyRoute {
            shard_id: self.shard_index_from_hash(key_hash),
            key_hash,
        }
    }

    /// Returns the default TTL applied by write APIs that do not pass an
    /// explicit TTL.
    #[inline(always)]
    pub const fn default_ttl_ms(&self) -> Option<u64> {
        self.default_ttl_ms
    }

    /// Inserts or replaces a value, returning the old value when present.
    #[inline(always)]
    pub fn insert(&self, key: K, value: V) -> Option<V>
    where
        K: Eq + Hash,
    {
        self.insert_with_ttl(key, value, self.default_ttl_ms)
    }

    /// Inserts or replaces a value with an optional relative TTL, returning
    /// the old live value when present.
    #[inline(always)]
    pub fn insert_with_ttl(&self, key: K, value: V, ttl_ms: Option<u64>) -> Option<V>
    where
        K: Eq + Hash,
    {
        let now_ms = ttl_now_millis();
        let expire_at_ms = ttl_deadline(now_ms, ttl_ms);
        let shard_id = self.route_key(&key).shard_id;
        self.shards[shard_id]
            .write()
            .insert(key, NativeValue::new(value, expire_at_ms))
            .and_then(|value| value.into_live_value(now_ms))
    }

    /// Inserts only when the key is absent.
    #[inline(always)]
    pub fn try_insert(&self, key: K, value: V) -> bool
    where
        K: Eq + Hash,
    {
        self.try_insert_with_ttl(key, value, self.default_ttl_ms)
    }

    /// Inserts only when the key is absent or expired, with an optional
    /// relative TTL.
    #[inline(always)]
    pub fn try_insert_with_ttl(&self, key: K, value: V, ttl_ms: Option<u64>) -> bool
    where
        K: Eq + Hash,
    {
        let now_ms = ttl_now_millis();
        let expire_at_ms = ttl_deadline(now_ms, ttl_ms);
        let shard_id = self.route_key(&key).shard_id;
        let mut shard = self.shards[shard_id].write();
        match shard.entry(key) {
            Entry::Occupied(mut entry) => {
                if entry.get().is_live(now_ms) {
                    false
                } else {
                    entry.insert(NativeValue::new(value, expire_at_ms));
                    true
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(NativeValue::new(value, expire_at_ms));
                true
            }
        }
    }

    /// Returns an owned clone of the value.
    #[inline(always)]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
        V: Clone,
    {
        let now_ms = ttl_now_millis();
        let shard_id = self.route_key(key).shard_id;
        self.shards[shard_id]
            .read()
            .get(key)
            .filter(|value| value.is_live(now_ms))
            .map(|value| value.value.clone())
    }

    /// Returns a borrowed value guard.
    #[inline(always)]
    pub fn get_ref<Q>(&self, key: &Q) -> Option<ShardMapRef<'_, K, V>>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        let now_ms = ttl_now_millis();
        let shard_id = self.route_key(key).shard_id;
        let guard = self.shards[shard_id].read();
        let value = match guard.get(key) {
            Some(value) if value.is_live(now_ms) => &value.value as *const V,
            None => return None,
            Some(_) => return None,
        };
        Some(ShardMapRef {
            guard,
            value,
            _not_send: PhantomData,
        })
    }

    /// Returns true when the key is present.
    #[inline(always)]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        let now_ms = ttl_now_millis();
        let shard_id = self.route_key(key).shard_id;
        self.shards[shard_id]
            .read()
            .get(key)
            .is_some_and(|value| value.is_live(now_ms))
    }

    /// Returns true when the key is present.
    #[inline(always)]
    pub fn exists<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        self.contains_key(key)
    }

    /// Removes a key and returns its value when present.
    #[inline(always)]
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        let now_ms = ttl_now_millis();
        let shard_id = self.route_key(key).shard_id;
        self.shards[shard_id]
            .write()
            .remove(key)
            .and_then(|value| value.into_live_value(now_ms))
    }

    /// Removes a key without returning the value.
    #[inline(always)]
    pub fn delete<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + Eq + Hash,
        Q: Eq + Hash + ?Sized,
    {
        self.remove(key).is_some()
    }

    /// Returns the number of entries currently stored.
    pub fn len(&self) -> usize {
        let now_ms = ttl_now_millis();
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .read()
                    .values()
                    .filter(|value| value.is_live(now_ms))
                    .count()
            })
            .sum::<usize>()
    }

    /// Returns true when the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Visits keys without allocating a snapshot.
    ///
    /// The visitor runs while each shard read lock is held. Keep callbacks
    /// lightweight, and return `false` to stop early.
    pub fn visit_keys(&self, mut visitor: impl FnMut(&K) -> bool) {
        let now_ms = ttl_now_millis();
        for shard in self.shards.iter() {
            let shard = shard.read();
            for (key, value) in shard.iter() {
                if !value.is_live(now_ms) {
                    continue;
                }
                if !visitor(key) {
                    return;
                }
            }
        }
    }

    /// Visits entries without allocating a snapshot.
    ///
    /// The visitor runs while each shard read lock is held. Keep callbacks
    /// lightweight, and return `false` to stop early.
    pub fn visit_entries(&self, mut visitor: impl FnMut(&K, &V) -> bool) {
        let now_ms = ttl_now_millis();
        for shard in self.shards.iter() {
            let shard = shard.read();
            for (key, value) in shard.iter() {
                if !value.is_live(now_ms) {
                    continue;
                }
                if !visitor(key, &value.value) {
                    return;
                }
            }
        }
    }

    /// Returns a snapshot of keys.
    pub fn keys(&self) -> Vec<K>
    where
        K: Clone,
    {
        let mut keys = Vec::new();
        self.visit_keys(|key| {
            keys.push(key.clone());
            true
        });
        keys
    }

    /// Returns a snapshot of entries.
    pub fn entries(&self) -> Vec<(K, V)>
    where
        K: Clone,
        V: Clone,
    {
        let mut entries = Vec::new();
        self.visit_entries(|key, value| {
            entries.push((key.clone(), value.clone()));
            true
        });
        entries
    }

    #[inline(always)]
    fn hash_key<Q>(&self, key: &Q) -> u64
    where
        Q: Hash + ?Sized,
    {
        self.hasher.hash_one(key)
    }

    #[inline(always)]
    const fn shard_index_from_hash(&self, hash: u64) -> usize {
        (hash as usize) & (SHARDS - 1)
    }
}

#[inline(always)]
fn ttl_deadline(now_ms: u64, ttl_ms: Option<u64>) -> Option<u64> {
    ttl_ms.map(|ttl_ms| now_ms.saturating_add(ttl_ms))
}

#[inline(always)]
fn ttl_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_map_round_trips_strings() {
        let map: ShardMap<String, String, 4> = ShardMap::with_capacity(16);

        assert!(
            map.insert("user:42".to_owned(), "ready".to_owned())
                .is_none()
        );
        assert_eq!(map.get("user:42").as_deref(), Some("ready"));

        {
            let value = map.get_ref("user:42").unwrap();
            assert_eq!(value.value(), "ready");
        }

        assert_eq!(
            map.insert("user:42".to_owned(), "done".to_owned())
                .as_deref(),
            Some("ready")
        );
        assert_eq!(map.remove("user:42").as_deref(), Some("done"));
        assert!(!map.contains_key("user:42"));
    }

    #[derive(Debug, Eq, Hash, PartialEq)]
    enum NativeInputKey {
        Text(String),
        Bytes(Vec<u8>),
        Tuple(u64, bool),
        Array([u8; 4]),
    }

    #[derive(Debug, PartialEq)]
    struct NonCloneValue {
        payload: Vec<u8>,
    }

    #[test]
    fn native_map_accepts_broad_rust_key_and_value_shapes() {
        let string_keys: ShardMap<String, Vec<u8>, 4> = ShardMap::new();
        string_keys.insert("unicode:☃".to_owned(), vec![0, 1, 2, 255]);
        assert_eq!(
            string_keys.get("unicode:☃").as_deref(),
            Some([0, 1, 2, 255].as_slice())
        );
        assert_eq!(
            string_keys.remove("unicode:☃").as_deref(),
            Some([0, 1, 2, 255].as_slice())
        );

        let bytes_keys: ShardMap<Vec<u8>, String, 4> = ShardMap::new();
        bytes_keys.insert(b"\0binary-key\xff".to_vec(), "binary value".to_owned());
        assert_eq!(
            bytes_keys.get(b"\0binary-key\xff".as_slice()).as_deref(),
            Some("binary value")
        );

        let tuple_keys: ShardMap<(u64, bool), Option<Result<String, u8>>, 4> = ShardMap::new();
        tuple_keys.insert((u64::MAX, true), Some(Ok("nested value".to_owned())));
        assert_eq!(
            tuple_keys.get(&(u64::MAX, true)),
            Some(Some(Ok("nested value".to_owned())))
        );

        let array_keys: ShardMap<[u8; 4], [u8; 3], 4> = ShardMap::new();
        array_keys.insert([0, 1, 2, 255], [9, 8, 7]);
        assert_eq!(array_keys.get(&[0, 1, 2, 255]), Some([9, 8, 7]));

        let custom_keys: ShardMap<NativeInputKey, NonCloneValue, 4> = ShardMap::new();
        let custom_key = NativeInputKey::Tuple(42, false);
        assert!(
            custom_keys
                .insert(
                    NativeInputKey::Text("tenant:user".to_owned()),
                    NonCloneValue {
                        payload: b"text".to_vec(),
                    },
                )
                .is_none()
        );
        assert!(
            custom_keys
                .insert(
                    NativeInputKey::Bytes(b"\0tenant:user".to_vec()),
                    NonCloneValue {
                        payload: b"bytes".to_vec(),
                    },
                )
                .is_none()
        );
        assert!(
            custom_keys
                .insert(
                    NativeInputKey::Array([1, 2, 3, 4]),
                    NonCloneValue {
                        payload: b"array".to_vec(),
                    },
                )
                .is_none()
        );
        custom_keys.insert(
            custom_key,
            NonCloneValue {
                payload: b"non-clone".to_vec(),
            },
        );

        let borrowed = custom_keys
            .get_ref(&NativeInputKey::Tuple(42, false))
            .unwrap();
        assert_eq!(borrowed.value().payload, b"non-clone");
        drop(borrowed);

        assert_eq!(
            custom_keys
                .remove(&NativeInputKey::Tuple(42, false))
                .unwrap(),
            NonCloneValue {
                payload: b"non-clone".to_vec(),
            }
        );
        assert_eq!(custom_keys.len(), 3);
    }

    #[test]
    fn native_map_default_ttl_expires_plain_writes_and_can_be_overridden() {
        let map: ShardMap<String, String, 4> = ShardMap::with_options(ShardMapOptions {
            capacity_hint: Some(16),
            default_ttl_ms: Some(20),
        });

        assert_eq!(map.default_ttl_ms(), Some(20));
        assert!(
            map.insert("default".to_owned(), "expires".to_owned())
                .is_none()
        );
        assert_eq!(map.get("default").as_deref(), Some("expires"));
        assert_eq!(map.len(), 1);

        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(map.get("default").is_none());
        assert!(map.get_ref("default").is_none());
        assert!(!map.contains_key("default"));
        assert_eq!(map.len(), 0);

        map.insert_with_ttl("durable".to_owned(), "stays".to_owned(), None);
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert_eq!(map.get("durable").as_deref(), Some("stays"));

        map.insert_with_ttl("short".to_owned(), "gone".to_owned(), Some(10));
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(map.get("short").is_none());

        assert!(map.try_insert("default".to_owned(), "reused".to_owned()));
        assert_eq!(map.get("default").as_deref(), Some("reused"));
    }

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    struct UserKey {
        tenant: String,
        id: u64,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct UserValue {
        name: String,
        active: bool,
    }

    #[test]
    fn native_map_supports_custom_struct_keys_and_values() {
        let map: ShardMap<UserKey, UserValue, 4> = ShardMap::new();
        let key = UserKey {
            tenant: "acme".to_owned(),
            id: 7,
        };

        assert!(
            map.insert(
                key.clone(),
                UserValue {
                    name: "Devon".to_owned(),
                    active: true,
                },
            )
            .is_none()
        );

        assert_eq!(
            map.get(&key),
            Some(UserValue {
                name: "Devon".to_owned(),
                active: true,
            })
        );
        assert_eq!(map.keys(), vec![key]);
    }

    #[test]
    fn native_map_routes_by_hash() {
        let map: ShardMap<u64, String, 8> = ShardMap::new();
        let route = map.route_key(&42);

        assert!(route.shard_id < map.shard_count());
    }
}
