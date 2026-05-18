use hashbrown::HashTable;
use std::collections::{BinaryHeap, VecDeque};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "telemetry")]
use std::time::Instant;

use crate::config::EvictionPolicy;
#[cfg(feature = "telemetry")]
use crate::storage::CacheTelemetryHandle;
use crate::storage::stats::TierStatsSnapshot;
use crate::storage::{Bytes, StoredEntry, hash_key, hash_key_tag_from_hash};
use bytes::Bytes as SharedBytes;

#[derive(Debug)]
struct FlatEntry {
    hash: u64,
    key_tag: u64,
    key_len: usize,
    key: Box<[u8]>,
    /// Value held as `bytes::Bytes` for zero-copy `SET` from the read buffer.
    /// `as_ref()` gives `&[u8]`; storage size = `value.len()`.
    value: SharedBytes,
    expire_at_ms: Option<u64>,
    access: EntryAccessMeta,
}

impl FlatEntry {
    #[inline(always)]
    fn matches(&self, hash: u64, key: &[u8]) -> bool {
        self.matches_hashed_key(hash, key)
    }

    #[inline(always)]
    fn matches_hashed_key(&self, hash: u64, key: &[u8]) -> bool {
        self.hash == hash && self.key_len == key.len() && bytes_equal_hot(self.key.as_ref(), key)
    }

    #[inline(always)]
    fn matches_prepared(&self, hash: u64, key: &[u8], _key_tag: u64) -> bool {
        self.matches_hashed_key(hash, key)
    }

    #[inline(always)]
    fn matches_tagged(&self, hash: u64, key_tag: u64, key_len: usize) -> bool {
        self.hash == hash && self.key_tag == key_tag && self.key_len == key_len
    }

    #[inline(always)]
    fn is_expired(&self, now_ms: u64) -> bool {
        self.expire_at_ms.is_some_and(|deadline| deadline <= now_ms)
    }
}

#[cfg(feature = "unsafe")]
#[inline(always)]
unsafe fn copy_hot_value_bytes(dst: *mut u8, src: *const u8, len: usize) {
    // SAFETY: forwarded from this function's caller.
    unsafe { std::ptr::copy_nonoverlapping(src, dst, len) };
}

#[inline(always)]
fn bytes_equal_hot(left: &[u8], right: &[u8]) -> bool {
    left == right
}

#[inline(always)]
fn shared_bytes_from_slice(value: &[u8]) -> SharedBytes {
    if should_reuse_value_buffer(value.len()) {
        SharedBytes::from(value.to_vec())
    } else {
        SharedBytes::copy_from_slice(value)
    }
}

#[inline(always)]
fn should_reuse_value_buffer(value_len: usize) -> bool {
    value_len >= REUSABLE_VALUE_MIN_BYTES
}

fn shared_bytes_from_reusable_pool(
    value: &[u8],
    reusable_values: &mut Vec<SharedBytes>,
    reusable_value_bytes: &mut usize,
) -> SharedBytes {
    let Some(position) = reusable_values
        .iter()
        .position(|candidate| candidate.len() == value.len())
    else {
        return shared_bytes_from_slice(value);
    };

    let reusable = reusable_values.swap_remove(position);
    *reusable_value_bytes = reusable_value_bytes.saturating_sub(reusable.len());
    match reusable.try_into_mut() {
        Ok(mut writable) => {
            writable[..].copy_from_slice(value);
            writable.freeze()
        }
        Err(_reusable) => shared_bytes_from_slice(value),
    }
}

#[inline(always)]
fn recycle_value_into_pool(
    value: SharedBytes,
    reusable_values: &mut Vec<SharedBytes>,
    reusable_value_bytes: &mut usize,
) {
    let value_len = value.len();
    if !should_reuse_value_buffer(value_len) {
        return;
    }
    if reusable_values.len() >= MAX_REUSABLE_VALUE_BUFFERS {
        return;
    }
    if reusable_value_bytes.saturating_add(value_len) > MAX_REUSABLE_VALUE_BYTES {
        return;
    }
    *reusable_value_bytes = reusable_value_bytes.saturating_add(value_len);
    reusable_values.push(value);
}

#[derive(Debug, Clone, Copy, Default)]
struct EntryAccessMeta {
    last_touch: u64,
    frequency: u32,
}

impl EntryAccessMeta {
    #[inline(always)]
    fn record_access(&mut self, tick: u64) {
        self.last_touch = tick;
        self.frequency = self.frequency.saturating_add(1).max(1);
    }

    #[inline(always)]
    fn rank(&self, policy: EvictionPolicy) -> EvictionRank {
        match policy {
            EvictionPolicy::None => EvictionRank {
                primary: u64::MAX,
                secondary: u64::MAX,
            },
            EvictionPolicy::Lru => EvictionRank {
                primary: self.last_touch,
                secondary: 0,
            },
            EvictionPolicy::Lfu => EvictionRank {
                primary: self.frequency as u64,
                secondary: self.last_touch,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct EvictionRank {
    pub(crate) primary: u64,
    pub(crate) secondary: u64,
}

#[derive(Debug)]
struct EvictionCandidate {
    rank: EvictionRank,
    hash: u64,
    key: Bytes,
}

impl PartialEq for EvictionCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.rank == other.rank && self.hash == other.hash
    }
}

impl Eq for EvictionCandidate {}

impl PartialOrd for EvictionCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EvictionCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank
            .cmp(&other.rank)
            .then_with(|| self.hash.cmp(&other.hash))
    }
}

#[derive(Debug)]
struct LruTouch {
    tick: u64,
    hash: u64,
}

const REUSABLE_VALUE_MIN_BYTES: usize = 4096;
const MAX_REUSABLE_VALUE_BUFFERS: usize = 128;
const MAX_REUSABLE_VALUE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Default)]
pub struct FlatMap {
    entries: HashTable<FlatEntry>,
    #[cfg(feature = "fast-point-map")]
    fast_points: FastPointMap,
    ttl_entries: usize,
    active_readers: AtomicUsize,
    retired_values: Vec<SharedBytes>,
    reusable_values: Vec<SharedBytes>,
    reusable_value_bytes: usize,
    stored_bytes: usize,
    memory_limit_bytes: Option<usize>,
    eviction_policy: EvictionPolicy,
    /// Monotonic counter for entry access timestamps. Metadata touches require
    /// `&mut FlatMap`, so this stays shard-local and non-atomic.
    access_clock: u64,
    /// Sampling counter for the lazy LFU/LRU read-touch decision.
    read_sample_counter: u64,
    lru_touch_log: VecDeque<LruTouch>,
    evictions: u64,
    #[cfg(feature = "telemetry")]
    telemetry: Option<FlatMapTelemetry>,
}

#[cfg(feature = "telemetry")]
#[derive(Debug, Clone)]
struct FlatMapTelemetry {
    metrics: CacheTelemetryHandle,
    shard_id: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteReason {
    Explicit,
    Expired,
    Evicted,
}
#[cfg(feature = "fast-point-map")]
mod fast_point;

mod core;
mod lifecycle;
mod read;
mod write;
mod write_hot;
mod write_local;

#[cfg(feature = "fast-point-map")]
use fast_point::FastPointMap;

#[cfg(test)]
mod tests {
    use super::FlatMap;
    #[cfg(feature = "embedded")]
    use super::hash_key_tag_from_hash;
    use super::{REUSABLE_VALUE_MIN_BYTES, hash_key};
    use crate::config::EvictionPolicy;

    #[test]
    fn stores_reads_and_updates_values() {
        let mut map = FlatMap::new();
        map.set(b"alpha".to_vec(), b"one".to_vec(), None, 0);
        assert_eq!(map.get(b"alpha", 0), Some(b"one".to_vec()));

        map.set(b"alpha".to_vec(), b"two".to_vec(), None, 0);
        assert_eq!(map.get(b"alpha", 0), Some(b"two".to_vec()));
    }

    #[test]
    fn expires_values() {
        let mut map = FlatMap::new();
        map.set(b"alpha".to_vec(), b"one".to_vec(), Some(10), 0);

        assert_eq!(map.get(b"alpha", 9), Some(b"one".to_vec()));
        assert_eq!(map.ttl_seconds(b"alpha", 11), -2);
        assert_eq!(map.get(b"alpha", 11), None);
    }

    #[test]
    fn maintenance_removes_expired_entries() {
        let mut map = FlatMap::new();
        map.set(b"alpha".to_vec(), b"one".to_vec(), Some(10), 0);
        map.set(b"beta".to_vec(), b"two".to_vec(), Some(10), 0);

        assert_eq!(map.process_maintenance(11), 2);
        assert!(map.is_empty());
    }

    #[test]
    fn read_epoch_keeps_old_value_alive_across_update() {
        let mut map = FlatMap::new();
        map.set(b"alpha".to_vec(), b"one".to_vec(), None, 0);

        map.begin_read_epoch();
        let read = map.get_ref(b"alpha", 0).unwrap();
        let ptr = read.as_ptr();
        let len = read.len();

        map.set(b"alpha".to_vec(), b"two".to_vec(), None, 0);

        let stale = unsafe { std::slice::from_raw_parts(ptr, len) };
        assert_eq!(stale, b"one");

        map.end_read_epoch();
        assert_eq!(map.get(b"alpha", 0), Some(b"two".to_vec()));
    }

    #[test]
    fn lru_eviction_removes_least_recent_entry_under_cap() {
        let mut map = FlatMap::new();
        map.configure_memory_policy(Some(4), EvictionPolicy::Lru, 0);

        map.set(b"a".to_vec(), b"1".to_vec(), None, 0);
        map.set(b"b".to_vec(), b"2".to_vec(), None, 0);
        map.set(b"a".to_vec(), b"1".to_vec(), None, 0);

        map.set(b"c".to_vec(), b"3".to_vec(), None, 0);

        assert_eq!(map.get(b"a", 0), Some(b"1".to_vec()));
        assert_eq!(map.get(b"b", 0), None);
        assert_eq!(map.get(b"c", 0), Some(b"3".to_vec()));
        assert!(map.stored_bytes() <= 4);
        assert_eq!(map.evictions(), 1);
    }

    #[cfg(feature = "embedded")]
    #[test]
    fn local_lru_reuses_evicted_large_value_buffer() {
        let mut map = FlatMap::new();
        map.configure_memory_policy(Some(5000), EvictionPolicy::Lru, 0);
        let value = vec![7u8; REUSABLE_VALUE_MIN_BYTES];

        let hash_a = hash_key(b"a");
        map.set_slice_hashed_tagged_no_ttl_local(
            hash_a,
            hash_key_tag_from_hash(hash_a),
            b"a",
            &value,
        );
        map.enforce_memory_limit(0);

        let hash_b = hash_key(b"b");
        map.set_slice_hashed_tagged_no_ttl_local(
            hash_b,
            hash_key_tag_from_hash(hash_b),
            b"b",
            &value,
        );
        map.enforce_memory_limit(0);

        assert_eq!(map.reusable_values.len(), 1);
        let reusable_ptr = map.reusable_values[0].as_ptr();

        let hash_c = hash_key(b"c");
        map.set_slice_hashed_tagged_no_ttl_local(
            hash_c,
            hash_key_tag_from_hash(hash_c),
            b"c",
            &value,
        );

        let stored_ptr = map
            .get_shared_value_bytes_hashed_no_ttl(hash_c, b"c")
            .expect("new value is stored")
            .as_ptr();
        assert_eq!(stored_ptr, reusable_ptr);
    }

    #[test]
    fn ttl_lru_reuses_evicted_large_value_buffer() {
        let mut map = FlatMap::new();
        let value = vec![7u8; REUSABLE_VALUE_MIN_BYTES];
        map.configure_memory_policy(Some(value.len() + 2048), EvictionPolicy::Lru, 0);

        let hash_a = hash_key(b"a");
        map.set_slice_hashed(hash_a, b"a", &value, Some(60_000), 0);

        let hash_b = hash_key(b"b");
        map.set_slice_hashed(hash_b, b"b", &value, Some(60_000), 1);

        assert_eq!(map.reusable_values.len(), 1);
        let reusable_ptr = map.reusable_values[0].as_ptr();

        let hash_c = hash_key(b"c");
        map.set_slice_hashed(hash_c, b"c", &value, Some(60_000), 2);

        let stored_ptr = map
            .get_shared_value_bytes_hashed(hash_c, b"c", 2)
            .expect("new value is stored")
            .as_ptr();
        assert_eq!(stored_ptr, reusable_ptr);
    }

    #[test]
    fn ttl_lru_does_not_pool_small_value_buffers() {
        let mut map = FlatMap::new();
        let small_value_len = 512;
        assert!(small_value_len < REUSABLE_VALUE_MIN_BYTES);
        map.configure_memory_policy(Some(small_value_len + 88), EvictionPolicy::Lru, 0);
        let value = vec![7u8; small_value_len];

        map.set_slice_hashed(hash_key(b"a"), b"a", &value, Some(60_000), 0);
        map.set_slice_hashed(hash_key(b"b"), b"b", &value, Some(60_000), 1);

        assert_eq!(map.evictions(), 1);
        assert!(map.reusable_values.is_empty());
        assert_eq!(map.reusable_value_bytes, 0);
    }

    #[test]
    fn lfu_eviction_removes_least_frequent_entry_under_cap() {
        let mut map = FlatMap::new();
        map.configure_memory_policy(Some(4), EvictionPolicy::Lfu, 0);

        map.set(b"a".to_vec(), b"1".to_vec(), None, 0);
        map.set(b"b".to_vec(), b"2".to_vec(), None, 0);
        map.set(b"a".to_vec(), b"1".to_vec(), None, 0);
        map.set(b"a".to_vec(), b"1".to_vec(), None, 0);

        map.set(b"c".to_vec(), b"3".to_vec(), None, 0);

        assert_eq!(map.get(b"a", 0), Some(b"1".to_vec()));
        assert_eq!(map.get(b"b", 0), None);
        assert_eq!(map.get(b"c", 0), Some(b"3".to_vec()));
        assert!(map.stored_bytes() <= 4);
        assert_eq!(map.evictions(), 1);
    }
}
