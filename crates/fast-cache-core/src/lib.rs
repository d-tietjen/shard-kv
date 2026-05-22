#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "server"), allow(dead_code, unused_imports))]

#[cfg(feature = "embedded")]
pub mod cache;
#[cfg(not(feature = "embedded"))]
compile_error!(
    "fast-cache currently requires the `embedded` feature; build with default features enabled or enable `embedded`/`sharded` explicitly"
);

pub mod commands;
#[cfg(feature = "embedded")]
pub mod config;
#[cfg(feature = "embedded")]
pub mod cuda;
#[cfg(feature = "embedded")]
pub mod embedded;
#[cfg(feature = "embedded")]
pub mod persistence;
#[cfg(feature = "embedded")]
pub mod protocol;
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
    CacheOptions, FastCache, FastCacheWithShards, FastMap, FastMapWithShards, SharedCache,
};
#[cfg(feature = "embedded")]
pub use error::{FastCacheError, Result};
#[cfg(all(feature = "telemetry", feature = "embedded"))]
pub use storage::{CacheMetrics, CacheMetricsSnapshot, CacheTelemetry};
