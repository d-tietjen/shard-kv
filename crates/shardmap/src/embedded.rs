//! Embedded API layers.
//!
//! This module gives names to the three intended embedded surfaces:
//!
//! - [`ShardMap`] / [`SharedCache`]: cloneable DashMap-like cache handles.
//! - [`ShardedEngine`]: the richer shared sharded engine used by server mode.
//! - [`LocalEmbeddedStore`]: owner-local stores for pinned workers.
//! - [`ShardCacheServer`]: an optional TCP protocol server over a caller-owned
//!   [`ShardedEngine`].
//! - [`ReplicatedEmbeddedStore`]: an embedded primary that can serve native
//!   read replicas and FCRP subscribers.

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

#[cfg(feature = "redis")]
pub use crate::redis_embedded::{
    EmbeddedRedis, EmbeddedRedisCommandError, EmbeddedRedisSession, PreparedEmbeddedRedisCommand,
    execute_redis_command, execute_redis_command_slices, try_execute_redis_command,
    try_execute_redis_command_slices,
};

#[cfg(feature = "sharded")]
pub use crate::storage::{
    LocalEmbeddedBatchReadView, LocalEmbeddedReadSlice, LocalEmbeddedReadView, LocalEmbeddedRefMut,
    LocalEmbeddedSessionBatchView, LocalEmbeddedSessionPackedView, LocalEmbeddedStore,
    LocalEmbeddedStoreBootstrap, LocalRouteError, LocalStoreAccessError, LocalStoreInstallError,
    take_local_embedded_store, with_local_embedded_store,
};

#[cfg(all(feature = "embedded", feature = "server"))]
pub use crate::server::{ServerMode, ServerRuntime, ShardCacheServer};
#[cfg(all(feature = "embedded", feature = "server"))]
pub use crate::storage::EngineHandle as ServerEngineHandle;

pub use crate::replication::{
    ReplicatedEmbeddedStore, ReplicationPrimaryServer, ReplicationReplica, ReplicationReplicaClient,
};
