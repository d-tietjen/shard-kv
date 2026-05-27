# fast-cache-redis

Redis/Valkey compatibility source package for `fast-cache`.

This crate owns the Redis-only command families and Redis object storage
implementation. Its source root is `crates/fast-cache-redis/src`.

The crate defaults to `redis`, so `cargo check -p fast-cache-redis`
exercises the Redis compatibility source instead of compiling an empty marker
package.

Compatibility status is generated from the live command benchmark registry in
[`docs/REDIS_COMPATIBILITY.md`](../../docs/REDIS_COMPATIBILITY.md). The 0.2.0
target covers the Redis 5.0.14 command table plus selected later cache-command
extensions, with standalone expected-error behavior and semantic caveats
documented there.

The intended long-term direction is to narrow the remaining internal extension
points until this package can become an ordinary optional dependency instead of
a source-owned compatibility package. During that transition, core still
includes these files by path behind its `redis` feature, but the files no
longer live inside the core crate tree.
