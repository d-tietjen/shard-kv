#![doc = include_str!("../README.md")]
#![cfg_attr(not(feature = "server"), allow(dead_code, unused_imports))]

#[cfg(not(feature = "embedded"))]
compile_error!(
    "fast-cache currently requires the default `embedded` feature; build with default features enabled"
);

pub mod commands;
#[cfg(feature = "embedded")]
pub mod config;
#[cfg(feature = "embedded")]
pub mod cuda;
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
pub use error::{FastCacheError, Result};
#[cfg(all(feature = "telemetry", feature = "embedded"))]
pub use storage::{CacheMetrics, CacheMetricsSnapshot, CacheTelemetry};
