use std::array;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes as SharedBytes;
use crossbeam_utils::CachePadded;
use parking_lot::{
    RwLock as FairRwLock, RwLockReadGuard as FairRwLockReadGuard,
    RwLockWriteGuard as FairRwLockWriteGuard,
};
use rblock::{
    RwLock as ReadBiasedRwLock, RwLockReadGuard as ReadBiasedRwLockReadGuard,
    RwLockWriteGuard as ReadBiasedRwLockWriteGuard,
};

use crate::config::EvictionPolicy;
use crate::storage::embedded_store::{
    EmbeddedKeyRoute, EmbeddedRouteMode, EmbeddedSessionRoute, EmbeddedShard,
    assert_valid_shard_count, compute_key_route, compute_session_shard, shift_for,
};
#[cfg(feature = "telemetry")]
use crate::storage::{CacheTelemetry, CacheTelemetryHandle, TelemetryRuntime};
use crate::storage::{
    FastHashMap, PreparedPointKey, SemanticCacheError, SemanticEmbedding, SemanticMatch, hash_key,
    hash_key_tag_from_hash, ttl_now_millis, validate_similarity_threshold,
};

/// Lock policy for [`SharedEmbeddedStore`] stripes.
///
/// `ReadBiased` favors read-heavy cache workloads by allowing new readers to
/// enter while a writer is waiting. `Fair` uses `parking_lot::RwLock`, which is
/// a better fit when writes are frequent or write latency matters more than
/// peak read-side throughput.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedEmbeddedLockPolicy {
    /// Favor readers. Best for read-heavy shared-handle workloads.
    ReadBiased,
    /// Favor bounded writer progress. Best for write-heavy or skewed workloads.
    Fair,
}

fn default_lock_policy() -> SharedEmbeddedLockPolicy {
    if cfg!(feature = "shared-parking-lot-lock") {
        SharedEmbeddedLockPolicy::Fair
    } else {
        SharedEmbeddedLockPolicy::ReadBiased
    }
}

/// Configuration for [`SharedEmbeddedStore`].
#[derive(Debug, Clone)]
pub struct SharedEmbeddedConfig {
    /// Total memory budget for all stripes. `None` disables memory-limit eviction.
    pub total_memory_bytes: Option<usize>,
    /// Eviction policy applied independently inside each stripe.
    pub eviction_policy: EvictionPolicy,
    /// Key routing mode used by point and session APIs.
    pub route_mode: EmbeddedRouteMode,
    /// Approximate total point-key capacity to reserve across all stripes.
    pub flat_map_capacity_hint: Option<usize>,
    /// Lock policy used by each stripe.
    pub lock_policy: SharedEmbeddedLockPolicy,
}

impl Default for SharedEmbeddedConfig {
    fn default() -> Self {
        Self {
            total_memory_bytes: None,
            eviction_policy: EvictionPolicy::None,
            route_mode: EmbeddedRouteMode::FullKey,
            flat_map_capacity_hint: None,
            lock_policy: default_lock_policy(),
        }
    }
}

/// Cloneable, lock-striped embedded cache handle.
///
/// `SharedEmbeddedStore` is the embedded mode for applications that want to
/// clone one cache handle into many workers while still allowing each worker to
/// reach every key. It uses the same `EmbeddedShard` storage primitive as
/// [`crate::storage::EmbeddedStore`], but stores stripes in an `Arc` so clones
/// are cheap and route to cache-padded shard locks.
///
/// ```compile_fail
/// use shardmap::storage::{SharedEmbeddedConfig, SharedEmbeddedStore};
///
/// let _ = SharedEmbeddedStore::<3>::new(SharedEmbeddedConfig::default());
/// ```
#[derive(Debug)]
pub struct SharedEmbeddedStore<const SHARDS: usize> {
    inner: Arc<SharedInner<SHARDS>>,
}

const SEMANTIC_QUERY_CACHE_SLOTS: usize = 1024;

#[derive(Debug)]
struct SemanticQueryCache {
    entries: FastHashMap<u64, Vec<SemanticQueryCacheEntry>>,
    len: usize,
}

#[derive(Debug, Clone)]
struct SemanticQueryCacheEntry {
    fingerprint: u64,
    generation: u64,
    min_score_bits: u32,
    query: Box<[f32]>,
    result: Option<SemanticMatch>,
}

impl Default for SemanticQueryCache {
    fn default() -> Self {
        Self {
            entries: FastHashMap::with_capacity_and_hasher(
                SEMANTIC_QUERY_CACHE_SLOTS,
                Default::default(),
            ),
            len: 0,
        }
    }
}

impl SemanticQueryCache {
    #[inline(always)]
    fn lookup(
        &self,
        query: &SemanticEmbedding,
        min_score: f32,
        generation: u64,
    ) -> Option<Option<SemanticMatch>> {
        let fingerprint = semantic_query_fingerprint(query, min_score);
        let entries = self.entries.get(&fingerprint)?;
        let entry = entries.iter().find(|entry| {
            entry.fingerprint == fingerprint
                && entry.generation == generation
                && entry.min_score_bits == min_score.to_bits()
                && entry.query.as_ref() == query.as_slice()
        })?;
        Some(entry.result.clone())
    }

    #[inline(always)]
    fn insert(
        &mut self,
        query: &SemanticEmbedding,
        min_score: f32,
        generation: u64,
        result: Option<SemanticMatch>,
    ) {
        let fingerprint = semantic_query_fingerprint(query, min_score);
        let min_score_bits = min_score.to_bits();
        if let Some(entries) = self.entries.get_mut(&fingerprint)
            && let Some(entry) = entries.iter_mut().find(|entry| {
                entry.generation == generation
                    && entry.min_score_bits == min_score_bits
                    && entry.query.as_ref() == query.as_slice()
            })
        {
            entry.result = result;
            return;
        }

        if self.len >= SEMANTIC_QUERY_CACHE_SLOTS {
            self.entries.clear();
            self.len = 0;
        }

        self.entries
            .entry(fingerprint)
            .or_default()
            .push(SemanticQueryCacheEntry {
                fingerprint,
                generation,
                min_score_bits,
                query: query.as_slice().to_vec().into_boxed_slice(),
                result,
            });
        self.len = self.len.saturating_add(1);
    }
}

#[inline(always)]
fn semantic_query_fingerprint(query: &SemanticEmbedding, min_score: f32) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    hash = semantic_query_mix(hash, query.as_slice().len() as u64);
    hash = semantic_query_mix(hash, min_score.to_bits() as u64);
    for component in query.as_slice() {
        hash = semantic_query_mix(hash, component.to_bits() as u64);
    }
    hash
}

#[inline(always)]
fn semantic_query_mix(hash: u64, value: u64) -> u64 {
    hash ^ value
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left(27)
        .wrapping_mul(0xc2b2_ae3d_27d4_eb4f)
}

#[derive(Debug)]
struct SharedInner<const SHARDS: usize> {
    shards: [CachePadded<SharedShardLock<EmbeddedShard>>; SHARDS],
    shift: u32,
    route_mode: EmbeddedRouteMode,
    semantic_generation: AtomicU64,
    semantic_data_active: AtomicBool,
    semantic_query_cache_enabled: AtomicBool,
    semantic_query_cache: FairRwLock<SemanticQueryCache>,
}
mod core;
mod guards;
mod lock;
mod point;
mod semantic;
mod session;
#[cfg(test)]
mod tests;

pub use guards::{Entry, Ref, RefMut, VacantEntry};
use lock::{SharedReadGuard, SharedShardLock, SharedWriteGuard};
