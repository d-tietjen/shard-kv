//! Redis/Valkey compatibility source package for `shardcache`.
//!
//! This crate owns the Redis-only command and Redis object implementation
//! source. `shardmap` currently includes those files by path behind its
//! `redis` feature while the extension boundary is being finished.

/// Version marker for the Redis compatibility source package.
pub const COMPAT_PACKAGE: &str = "shardcache-redis";
