#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "server"), allow(dead_code, unused_imports))]

#[cfg(feature = "embedded")]
pub mod cache;
#[cfg(feature = "codec")]
pub mod codec;
#[cfg(not(feature = "embedded"))]
compile_error!(
    "shardmap currently requires the `embedded` feature; build with default features enabled or enable `embedded`/`sharded` explicitly"
);

#[cfg(feature = "embedded")]
pub mod commands;
#[cfg(feature = "embedded")]
pub mod config;
#[cfg(feature = "embedded")]
pub mod cuda;
#[cfg(feature = "embedded")]
pub mod embedded;
#[cfg(feature = "embedded")]
pub mod native;
#[cfg(feature = "embedded")]
pub mod persistence;
#[cfg(feature = "embedded")]
pub mod protocol;
#[cfg(feature = "redis")]
pub mod redis_embedded;
#[cfg(feature = "embedded")]
pub mod replication;
#[cfg(all(feature = "embedded", feature = "server"))]
pub mod server;
#[cfg(feature = "embedded")]
pub mod storage;

#[cfg(feature = "embedded")]
mod error;
#[cfg(all(target_os = "linux", feature = "monoio"))]
mod monoio_runtime;

#[cfg(feature = "embedded")]
pub use cache::{
    CacheOptions, CacheSemanticError, CacheSemanticMatch, RawShardMap, RawShardMapWithShards,
    ShardCache, ShardCacheWithShards, SharedCache,
};
#[cfg(feature = "codec")]
pub use codec::{
    CodecError, CodecKey, CodecKeyDecode, CodecShardMap, CodecShardMapRef, CodecShardMapWithShards,
    CodecValue, CodecValueEncode, EncodedBytes,
};
#[cfg(feature = "embedded")]
pub use error::{Result, ShardCacheError};
#[cfg(feature = "embedded")]
pub use native::{ShardMap, ShardMapHasher, ShardMapOptions, ShardMapRef, ShardMapWithShards};
#[cfg(all(feature = "telemetry", feature = "embedded"))]
pub use storage::{CacheMetrics, CacheMetricsSnapshot, CacheTelemetry, CacheTelemetryClock};
#[cfg(feature = "embedded")]
pub use storage::{SemanticCacheError, SemanticMatch};
