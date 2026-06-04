//! Redis/Valkey compatibility source for `shardcache`.
//!
//! `shardmap` owns the Redis-only command and Redis object implementation
//! source behind its `redis` feature. The public server-facing surface is the
//! `shardcache` crate's Redis feature set.

/// Version marker for the Redis compatibility source surface.
pub const COMPAT_PACKAGE: &str = "shardcache";
