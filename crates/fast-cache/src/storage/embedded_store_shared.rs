use std::array;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

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
use crate::storage::{hash_key, ttl_now_millis};

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
/// use fast_cache::storage::{SharedEmbeddedConfig, SharedEmbeddedStore};
///
/// let _ = SharedEmbeddedStore::<3>::new(SharedEmbeddedConfig::default());
/// ```
#[derive(Debug)]
pub struct SharedEmbeddedStore<const SHARDS: usize> {
    inner: Arc<SharedInner<SHARDS>>,
}

#[derive(Debug)]
struct SharedInner<const SHARDS: usize> {
    shards: [CachePadded<SharedShardLock<EmbeddedShard>>; SHARDS],
    shift: u32,
    route_mode: EmbeddedRouteMode,
}
mod core;
mod guards;
mod lock;
mod point;
mod session;
#[cfg(test)]
mod tests;

pub use guards::{Entry, Ref, RefMut, VacantEntry};
use lock::{SharedReadGuard, SharedShardLock, SharedWriteGuard};
