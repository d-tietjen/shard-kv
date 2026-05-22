//! Redis/Valkey compatibility source package for `fast-cache`.
//!
//! This crate owns the Redis-only command and Redis object implementation
//! source. `fast-cache-core` currently includes those files by path behind its
//! `redis-compat` feature while the extension boundary is being finished.

/// Version marker for the Redis compatibility source package.
pub const COMPAT_PACKAGE: &str = "fast-cache-redis";
