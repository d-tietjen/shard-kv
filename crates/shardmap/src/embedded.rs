//! Embedded API layers.
//!
//! This module gives names to the three intended embedded surfaces:
//!
//! - [`ShardMap`] / [`SharedCache`]: cloneable DashMap-like cache handles.
//! - [`ShardedEngine`]: the richer shared sharded engine used by server mode.
//! - [`LocalEmbeddedStore`]: owner-local stores for pinned workers.

pub use crate::cache::{
    CacheEntry, CacheOptions, CacheRef, CacheRefMut, CacheSemanticError, CacheSemanticMatch,
    CacheVacantEntry, DEFAULT_CACHE_SHARDS, ShardCache, ShardCacheWithShards, ShardMap,
    ShardMapWithShards, SharedCache,
};
pub use crate::storage::SharedEmbeddedStore as SharedCacheStore;
pub use crate::storage::{
    EmbeddedKeyRoute, EmbeddedRouteMode, EmbeddedSessionRoute, PackedSessionWrite,
    PreparedPointKey, SemanticCacheError, SemanticMatch, SharedEmbeddedConfig,
    SharedEmbeddedLockPolicy, SharedEmbeddedStore, shift_for, stripe_index,
};
pub use crate::storage::{EmbeddedStore as ShardedEmbeddedStore, EmbeddedStore as ShardedEngine};

#[cfg(feature = "sharded")]
pub use crate::storage::{
    LocalEmbeddedBatchReadView, LocalEmbeddedReadSlice, LocalEmbeddedReadView, LocalEmbeddedRefMut,
    LocalEmbeddedSessionBatchView, LocalEmbeddedSessionPackedView, LocalEmbeddedStore,
    LocalEmbeddedStoreBootstrap, LocalRouteError, LocalStoreAccessError, LocalStoreInstallError,
    take_local_embedded_store, with_local_embedded_store,
};

#[cfg(all(feature = "embedded", feature = "server"))]
pub use crate::storage::EngineHandle as ServerEngineHandle;
