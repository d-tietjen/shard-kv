use hashbrown::HashTable;
#[cfg(feature = "prefix-eviction")]
use std::collections::HashMap;
use std::collections::{BinaryHeap, VecDeque};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::ShardCacheError;
use crate::config::{EvictionPolicy, ObjectOverflowFailurePolicy};
use crate::storage::stats::{ObjectOverflowStatsSnapshot, TierStatsSnapshot};
use crate::storage::{
    Bytes, FastHashMap, ObjectOverflowRuntime, ObjectValueRef, SemanticCacheError,
    SemanticEmbedding, SemanticIndex, SemanticIndexCandidate, SemanticIndexToken, SemanticMatch,
    StoredEntry, hash_key, hash_key_tag_from_hash,
};
#[cfg(feature = "telemetry")]
use crate::storage::{CacheTelemetryHandle, LatencySampleStart};
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
    semantic_index_token: Option<SemanticIndexToken>,
    /// Opaque policy metadata. Presence marks the value as protected for every
    /// ordinary point read, including entries created through semantic APIs.
    governance: Option<SharedBytes>,
    #[cfg(feature = "kv-overflow")]
    overflow_generation: u64,
    access: EntryAccessMeta,
}

#[derive(Debug)]
struct RemoteEntry {
    hash: u64,
    key_len: usize,
    key: Box<[u8]>,
    object: ObjectValueRef,
    expire_at_ms: Option<u64>,
    governance: Option<SharedBytes>,
}

impl RemoteEntry {
    #[inline(always)]
    fn matches(&self, hash: u64, key: &[u8]) -> bool {
        self.hash == hash && self.key_len == key.len() && bytes_equal_hot(self.key.as_ref(), key)
    }

    #[inline(always)]
    fn is_expired(&self, now_ms: u64) -> bool {
        self.expire_at_ms.is_some_and(|deadline| deadline <= now_ms)
    }

    #[inline(always)]
    fn stored_bytes(&self) -> usize {
        self.key_len
            .saturating_add(self.object.object_key.len())
            .saturating_add(std::mem::size_of::<ObjectValueRef>())
            .saturating_add(self.governance.as_ref().map_or(0, SharedBytes::len))
    }

    #[inline(always)]
    fn is_protected(&self) -> bool {
        self.governance.is_some()
    }
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
    fn matches_readable(&self, hash: u64, key: &[u8]) -> bool {
        !self.is_protected() && self.matches_hashed_key(hash, key)
    }

    #[inline(always)]
    fn matches_readable_prepared(&self, hash: u64, key: &[u8], key_tag: u64) -> bool {
        !self.is_protected() && self.matches_prepared(hash, key, key_tag)
    }

    #[inline(always)]
    fn matches_readable_tagged(&self, hash: u64, key_tag: u64, key_len: usize) -> bool {
        !self.is_protected() && self.matches_tagged(hash, key_tag, key_len)
    }

    #[inline(always)]
    fn matches_tagged(&self, hash: u64, key_tag: u64, key_len: usize) -> bool {
        self.hash == hash && self.key_tag == key_tag && self.key_len == key_len
    }

    #[inline(always)]
    fn is_expired(&self, now_ms: u64) -> bool {
        self.expire_at_ms.is_some_and(|deadline| deadline <= now_ms)
    }

    #[inline(always)]
    fn semantic_bytes(&self) -> usize {
        self.semantic_index_token
            .map_or(0, |token| token.stored_bytes())
            .saturating_add(self.governance.as_ref().map_or(0, SharedBytes::len))
    }

    #[inline(always)]
    fn stored_bytes(&self) -> usize {
        self.key_len
            .saturating_add(self.value.len())
            .saturating_add(self.semantic_bytes())
    }

    #[inline(always)]
    fn clear_semantic_embedding(&mut self) {
        self.semantic_index_token = None;
        self.governance = None;
        #[cfg(feature = "kv-overflow")]
        {
            self.overflow_generation = 0;
        }
    }

    #[inline(always)]
    fn is_protected(&self) -> bool {
        self.governance.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GovernedRead<T> {
    Missing,
    Denied,
    Authorized(T),
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

#[cfg(feature = "mutable-value-slices")]
#[inline(always)]
fn shared_bytes_as_unique_slice_mut(value: &mut SharedBytes) -> Option<&mut [u8]> {
    if !value.is_unique() {
        return None;
    }

    // SAFETY: `Bytes::is_unique` is checked while holding `&mut Bytes`, so no
    // other `Bytes` handle aliases this allocation. The slice is tied to the
    // mutable borrow of the stored value and cannot outlive it.
    Some(unsafe { std::slice::from_raw_parts_mut(value.as_ptr().cast_mut(), value.len()) })
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
            #[cfg(feature = "prefix-eviction")]
            EvictionPolicy::Prefix => EvictionRank {
                primary: self.last_touch,
                secondary: self.frequency as u64,
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

#[cfg(feature = "prefix-eviction")]
#[inline(always)]
fn prefix_eviction_group_rank(access: EntryAccessMeta) -> EvictionRank {
    EvictionRank {
        primary: access.last_touch,
        secondary: access.frequency as u64,
    }
}

#[cfg(feature = "prefix-eviction")]
#[inline(always)]
fn prefix_eviction_member_rank(
    prefix_len: usize,
    key_len: usize,
    access: EntryAccessMeta,
) -> EvictionRank {
    let suffix_depth = key_len.saturating_sub(prefix_len) as u64;
    EvictionRank {
        primary: access.last_touch,
        secondary: u64::MAX.saturating_sub(suffix_depth),
    }
}

#[cfg(feature = "prefix-eviction")]
fn prefix_eviction_key_prefix(key: &[u8]) -> &[u8] {
    if let Some(prefix) = prefix_eviction_session_chunk_prefix(key) {
        return prefix;
    }
    if let Some(prefix) = prefix_eviction_lmcache_session_prefix(key) {
        return prefix;
    }
    if let Some(index) = key
        .iter()
        .rposition(|byte| matches!(*byte, b':' | b'/' | b'|' | b'@'))
        .filter(|index| *index > 0)
    {
        return &key[..index];
    }
    key
}

#[cfg(feature = "prefix-eviction")]
fn prefix_eviction_session_chunk_prefix(key: &[u8]) -> Option<&[u8]> {
    if !key.starts_with(b"s:") {
        return None;
    }
    let marker = b":c:";
    key.windows(marker.len())
        .rposition(|window| window == marker)
        .filter(|index| *index > 0)
        .map(|index| &key[..index])
}

#[cfg(feature = "prefix-eviction")]
fn prefix_eviction_lmcache_session_prefix(key: &[u8]) -> Option<&[u8]> {
    let marker = b"session%";
    let mut start = 0usize;
    while start <= key.len() {
        let remaining = &key[start..];
        let segment_len = remaining
            .iter()
            .position(|byte| *byte == b'@')
            .unwrap_or(remaining.len());
        let segment = &remaining[..segment_len];
        if segment.starts_with(marker) && segment.len() > marker.len() {
            return Some(segment);
        }
        if segment_len == remaining.len() {
            break;
        }
        start = start.saturating_add(segment_len).saturating_add(1);
    }
    None
}

const REUSABLE_VALUE_MIN_BYTES: usize = 4096;
const MAX_REUSABLE_VALUE_BUFFERS: usize = 128;
const MAX_REUSABLE_VALUE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Default)]
pub struct FlatMap {
    entries: HashTable<FlatEntry>,
    remote_entries: FastHashMap<Bytes, RemoteEntry>,
    semantic_index: SemanticIndex,
    #[cfg(feature = "experimental-no-ttl-point-hot-path")]
    fast_points: FastPointMap,
    ttl_entries: usize,
    active_readers: AtomicUsize,
    retired_values: Vec<SharedBytes>,
    reusable_values: Vec<SharedBytes>,
    reusable_value_bytes: usize,
    stored_bytes: usize,
    remote_value_bytes: usize,
    memory_limit_bytes: Option<usize>,
    eviction_policy: EvictionPolicy,
    object_overflow: Option<ObjectOverflowRuntime>,
    object_overflow_shard_id: usize,
    object_overflow_sequence: u64,
    object_overflow_stats: ObjectOverflowStats,
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

#[derive(Debug, Default, Clone)]
pub struct ObjectOverflowStats {
    pub enabled: bool,
    pub backend: String,
    pub degraded: bool,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub pending_jobs: usize,
    pub active_workers: usize,
    pub worker_threads: usize,
    pub remote_entries: usize,
    pub remote_value_bytes: usize,
    pub remote_stored_bytes: usize,
    pub offload_attempts: u64,
    pub offload_successes: u64,
    pub offload_failures: u64,
    pub offload_hot_skips: u64,
    pub fault_attempts: u64,
    pub fault_successes: u64,
    pub fault_failures: u64,
    pub checksum_failures: u64,
    pub remote_delete_attempts: u64,
    pub remote_delete_failures: u64,
    pub encode_ops: u64,
    pub compress_ops: u64,
    pub upload_ops: u64,
    pub download_ops: u64,
    pub decode_ops: u64,
    pub delete_ops: u64,
    pub retries: u64,
    pub timeouts: u64,
    pub auth_config_failures: u64,
    pub unavailable_failures: u64,
    pub integrity_failures: u64,
    pub not_found_failures: u64,
    pub delete_failures: u64,
    pub cleanup_scans: u64,
    pub cleanup_deletes: u64,
    pub object_bytes_raw: u64,
    pub object_bytes_stored: u64,
    pub encode_latency_us: u64,
    pub upload_latency_us: u64,
    pub download_latency_us: u64,
    pub decode_latency_us: u64,
    pub delete_latency_us: u64,
}

impl From<ObjectOverflowStats> for ObjectOverflowStatsSnapshot {
    fn from(value: ObjectOverflowStats) -> Self {
        Self {
            enabled: value.enabled,
            backend: value.backend,
            degraded: value.degraded,
            queue_capacity: value.queue_capacity,
            queue_depth: value.queue_depth,
            pending_jobs: value.pending_jobs,
            active_workers: value.active_workers,
            worker_threads: value.worker_threads,
            remote_entries: value.remote_entries,
            remote_value_bytes: value.remote_value_bytes,
            remote_stored_bytes: value.remote_stored_bytes,
            offload_attempts: value.offload_attempts,
            offload_successes: value.offload_successes,
            offload_failures: value.offload_failures,
            offload_hot_skips: value.offload_hot_skips,
            fault_attempts: value.fault_attempts,
            fault_successes: value.fault_successes,
            fault_failures: value.fault_failures,
            checksum_failures: value.checksum_failures,
            remote_delete_attempts: value.remote_delete_attempts,
            remote_delete_failures: value.remote_delete_failures,
            encode_ops: value.encode_ops,
            compress_ops: value.compress_ops,
            upload_ops: value.upload_ops,
            download_ops: value.download_ops,
            decode_ops: value.decode_ops,
            delete_ops: value.delete_ops,
            retries: value.retries,
            timeouts: value.timeouts,
            auth_config_failures: value.auth_config_failures,
            unavailable_failures: value.unavailable_failures,
            integrity_failures: value.integrity_failures,
            not_found_failures: value.not_found_failures,
            delete_failures: value.delete_failures,
            cleanup_scans: value.cleanup_scans,
            cleanup_deletes: value.cleanup_deletes,
            object_bytes_raw: value.object_bytes_raw,
            object_bytes_stored: value.object_bytes_stored,
            encode_latency_us: value.encode_latency_us,
            upload_latency_us: value.upload_latency_us,
            download_latency_us: value.download_latency_us,
            decode_latency_us: value.decode_latency_us,
            delete_latency_us: value.delete_latency_us,
        }
    }
}

#[cfg(feature = "telemetry")]
#[derive(Debug, Clone)]
struct FlatMapTelemetry {
    metrics: CacheTelemetryHandle,
    shard_id: usize,
    latency_sample_counter: u64,
    latency_sample_mask: u64,
}

#[cfg(feature = "telemetry")]
impl FlatMapTelemetry {
    fn new(metrics: CacheTelemetryHandle, shard_id: usize) -> Self {
        let latency_sample_mask = metrics.latency_sample_mask();
        Self {
            metrics,
            shard_id,
            latency_sample_counter: 0,
            latency_sample_mask,
        }
    }

    #[inline(always)]
    fn start_latency_sample(&mut self) -> Option<LatencySampleStart> {
        let should_sample = self.latency_sample_counter & self.latency_sample_mask == 0;
        self.latency_sample_counter = self.latency_sample_counter.wrapping_add(1);
        should_sample.then(|| self.metrics.start_latency_sample())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteReason {
    Explicit,
    Expired,
    Evicted,
}

enum ObjectOffloadAttempt {
    Offloaded,
    NotEligible,
    HotRetainResident,
    FailedRetainResident,
    FailedEvictResident,
}
#[cfg(feature = "experimental-no-ttl-point-hot-path")]
mod fast_point;

mod core;
mod lifecycle;
mod read;
mod semantic;
mod write;
mod write_hot;
mod write_local;

#[cfg(feature = "experimental-no-ttl-point-hot-path")]
use fast_point::FastPointMap;

#[cfg(test)]
mod tests {
    use super::FlatMap;
    #[cfg(feature = "embedded")]
    use super::hash_key_tag_from_hash;
    use super::{REUSABLE_VALUE_MIN_BYTES, hash_key};
    use crate::config::{
        EvictionPolicy, ObjectOverflowBackend, ObjectOverflowCompression,
        ObjectOverflowFailurePolicy,
    };
    use crate::storage::object_overflow::tests::InMemoryObjectOverflowStore;
    use crate::storage::{ObjectOverflowRuntime, ObjectOverflowRuntimeOptions};
    use std::sync::Arc;

    fn in_memory_overflow(min_value_bytes: usize) -> ObjectOverflowRuntime {
        in_memory_overflow_with_store(min_value_bytes).0
    }

    fn in_memory_overflow_with_cold_gate(
        min_value_bytes: usize,
        offload_min_idle_ticks: u64,
        offload_max_frequency: u32,
    ) -> ObjectOverflowRuntime {
        in_memory_overflow_with_store_and_cold_gate(
            min_value_bytes,
            offload_min_idle_ticks,
            offload_max_frequency,
        )
        .0
    }

    fn in_memory_overflow_with_store(
        min_value_bytes: usize,
    ) -> (ObjectOverflowRuntime, Arc<InMemoryObjectOverflowStore>) {
        in_memory_overflow_with_store_and_cold_gate(min_value_bytes, 0, u32::MAX)
    }

    fn in_memory_overflow_with_store_and_cold_gate(
        min_value_bytes: usize,
        offload_min_idle_ticks: u64,
        offload_max_frequency: u32,
    ) -> (ObjectOverflowRuntime, Arc<InMemoryObjectOverflowStore>) {
        let store = Arc::new(InMemoryObjectOverflowStore::default());
        let runtime = ObjectOverflowRuntime::new(
            store.clone(),
            ObjectOverflowRuntimeOptions {
                backend: ObjectOverflowBackend::File,
                min_value_bytes,
                compression: ObjectOverflowCompression::Zstd,
                zstd_level: 3,
                failure_policy: ObjectOverflowFailurePolicy::RetainResident,
                max_retries: 0,
                retry_backoff: std::time::Duration::from_millis(1),
                operation_timeout: std::time::Duration::from_millis(100),
                worker_threads: 1,
                queue_capacity: 16,
                degraded_failure_threshold: 8,
                degraded_cooldown: std::time::Duration::from_millis(100),
                fetch_on_get: true,
                delete_on_overwrite: true,
                prefix: "test-overflow".to_string(),
                node_id: "test-node".to_string(),
                generation_id: "test-generation".to_string(),
                cleanup_on_start: false,
                cleanup_grace: std::time::Duration::from_secs(60),
                offload_min_idle_ticks,
                offload_max_frequency,
            },
        );
        (runtime, store)
    }

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
    fn no_ttl_slice_write_clears_existing_expiration() {
        let mut map = FlatMap::new();
        let hash = hash_key(b"alpha");
        map.set_slice_hashed(hash, b"alpha", b"one", Some(10), 0);

        map.set_slice_hashed_no_ttl(hash, b"alpha", b"two");

        assert_eq!(map.get(b"alpha", 11), Some(b"two".to_vec()));
        assert!(map.has_no_ttl_entries());
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

    #[cfg(feature = "prefix-eviction")]
    #[test]
    fn prefix_eviction_preserves_refreshed_prefix_group() {
        let mut map = FlatMap::new();
        map.configure_memory_policy(None, EvictionPolicy::Prefix, 0);

        map.set(b"cold:0".to_vec(), b"1".to_vec(), None, 0);
        map.set(b"cold:1".to_vec(), b"2".to_vec(), None, 0);
        map.set(b"hot:0".to_vec(), b"3".to_vec(), None, 0);
        map.set(b"hot:1".to_vec(), b"4".to_vec(), None, 0);
        map.set(b"cold:1".to_vec(), b"2".to_vec(), None, 0);

        assert!(map.evict_with_policy(EvictionPolicy::Prefix, 0));

        assert_eq!(map.get(b"hot:0", 0), None);
        assert_eq!(map.get(b"hot:1", 0), Some(b"4".to_vec()));
        assert_eq!(map.get(b"cold:0", 0), Some(b"1".to_vec()));
        assert_eq!(map.get(b"cold:1", 0), Some(b"2".to_vec()));
    }

    #[test]
    fn object_overflow_offloads_and_faults_owned_get() {
        let mut map = FlatMap::new();
        map.configure_memory_policy(Some(8), EvictionPolicy::Lru, 0);
        map.configure_object_overflow(Some(in_memory_overflow(4)), 2, 0);

        map.set(b"alpha".to_vec(), b"0123456789".to_vec(), None, 0);

        assert_eq!(map.get_ref(b"alpha", 0), None);
        assert!(map.exists(b"alpha", 0));
        assert_eq!(map.remote_value_bytes(), 10);

        assert_eq!(map.get(b"alpha", 0), Some(b"0123456789".to_vec()));
        let stats = map.object_overflow_stats();
        assert_eq!(stats.offload_successes, 2);
        assert_eq!(stats.fault_successes, 1);
    }

    #[test]
    fn object_overflow_snapshot_materializes_remote_entries() {
        let mut map = FlatMap::new();
        map.configure_memory_policy(Some(8), EvictionPolicy::Lru, 0);
        map.configure_object_overflow(Some(in_memory_overflow(4)), 7, 0);

        map.set(b"alpha".to_vec(), b"remote-value".to_vec(), None, 0);

        let snapshot = map.snapshot_entries(0);
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].key, b"alpha".to_vec());
        assert_eq!(snapshot[0].value, b"remote-value".to_vec());
    }

    #[test]
    fn object_overflow_retains_recent_hot_values() {
        let mut map = FlatMap::new();
        map.configure_memory_policy(Some(8), EvictionPolicy::Lru, 0);
        map.configure_object_overflow(
            Some(in_memory_overflow_with_cold_gate(4, 1_024, u32::MAX)),
            2,
            0,
        );

        map.set(b"alpha".to_vec(), b"0123456789".to_vec(), None, 0);

        assert!(map.get_ref(b"alpha", 0).is_some());
        assert_eq!(map.remote_value_bytes(), 0);
        let stats = map.object_overflow_stats();
        assert_eq!(stats.offload_attempts, 0);
        assert_eq!(stats.offload_successes, 0);
        assert!(stats.offload_hot_skips >= 1);
    }

    #[test]
    fn object_overflow_offloads_only_idle_cold_values() {
        let mut map = FlatMap::new();
        map.configure_memory_policy(Some(4096), EvictionPolicy::Lru, 0);
        map.configure_object_overflow(
            Some(in_memory_overflow_with_cold_gate(4, 2, u32::MAX)),
            2,
            0,
        );

        map.set(b"cold".to_vec(), b"cold-value".to_vec(), None, 0);
        map.set(b"middle".to_vec(), b"middle-value".to_vec(), None, 0);
        map.set(b"hot".to_vec(), b"hot-value".to_vec(), None, 0);
        assert_eq!(map.get(b"hot", 0), Some(b"hot-value".to_vec()));

        map.configure_memory_policy(Some(8), EvictionPolicy::Lru, 0);

        assert_eq!(map.get_ref(b"cold", 0), None);
        assert_eq!(map.get_ref(b"hot", 0), Some(b"hot-value".as_slice()));
        let stats = map.object_overflow_stats();
        assert!(stats.offload_successes >= 1);
        assert!(stats.offload_hot_skips >= 1);
    }

    #[test]
    fn object_overflow_keys_include_node_and_generation() {
        let runtime = in_memory_overflow(4);
        let key = runtime.object_key(7, 0xabcd, 3);
        assert!(key.starts_with("test-overflow/test-node/test-generation/shard-7/"));
        assert!(key.ends_with("000000000000abcd-0000000000000003.bin"));
    }

    #[test]
    fn object_overflow_try_snapshot_errors_on_remote_fetch_failure() {
        let mut map = FlatMap::new();
        let (overflow, store) = in_memory_overflow_with_store(4);
        map.configure_memory_policy(Some(8), EvictionPolicy::Lru, 0);
        map.configure_object_overflow(Some(overflow), 7, 0);

        map.set(b"alpha".to_vec(), b"remote-value".to_vec(), None, 0);
        let object_key = map
            .remote_entries
            .get(b"alpha".as_slice())
            .expect("remote entry")
            .object
            .object_key
            .clone();
        store.remove(&object_key);

        assert!(map.try_snapshot_entries(0).is_err());
        assert!(map.snapshot_entries(0).is_empty());
    }

    #[test]
    fn object_overflow_maintenance_removes_expired_remote_entries() {
        let mut map = FlatMap::new();
        map.configure_memory_policy(Some(8), EvictionPolicy::Lru, 0);
        map.configure_object_overflow(Some(in_memory_overflow(4)), 3, 0);

        map.set(b"alpha".to_vec(), b"0123456789".to_vec(), Some(10), 0);
        assert_eq!(map.len(), 1);

        assert_eq!(map.process_maintenance(11), 1);
        assert_eq!(map.len(), 0);
        let stats = map.object_overflow_stats();
        assert_eq!(stats.remote_delete_attempts, 1);
        assert_eq!(stats.remote_delete_failures, 0);
    }

    #[test]
    fn object_overflow_rejects_corrupted_remote_payload() {
        let mut map = FlatMap::new();
        let (overflow, store) = in_memory_overflow_with_store(4);
        map.configure_memory_policy(Some(8), EvictionPolicy::Lru, 0);
        map.configure_object_overflow(Some(overflow), 5, 0);

        map.set(b"alpha".to_vec(), b"0123456789".to_vec(), None, 0);
        let object_key = map
            .remote_entries
            .get(b"alpha".as_slice())
            .expect("remote entry")
            .object
            .object_key
            .clone();
        store.overwrite(&object_key, b"not-a-valid-payload");

        assert_eq!(map.get(b"alpha", 0), None);
        let stats = map.object_overflow_stats();
        assert_eq!(stats.fault_failures, 1);
        assert_eq!(stats.checksum_failures, 1);
    }
}
