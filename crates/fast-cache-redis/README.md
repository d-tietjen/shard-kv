# fast-cache-redis

Redis/Valkey compatibility source package for `fast-cache`.

This crate owns the Redis-only command families and Redis object storage
implementation. Its source root is `crates/fast-cache-redis/src`.

The crate defaults to `redis-compat`, so `cargo check -p fast-cache-redis`
exercises the Redis compatibility source instead of compiling an empty marker
package.

The intended long-term direction is to narrow the remaining internal extension
points until this package can become an ordinary optional dependency instead of
a source-owned compatibility package. During that transition, core still
includes these files by path behind its `redis-compat` feature, but the files no
longer live inside the core crate tree.
