# Changelog

## 0.3.0 - Unreleased

### Added

- Added the native typed embedded API, `ShardMap<K, V>`, for Rust object
  storage with cloneable sharded handles, borrowed `get_ref` access, owned
  `get` reads, typed snapshots, custom key/value structs, and optional
  per-map default TTLs.
- Added the feature-gated `codec` API for typed facades over the shared byte
  engine, including zero-copy borrowed reads where the codec supports it,
  owned decode reads, custom codecs, namespace-local iteration, and
  namespace-local default TTLs.
- Added embedded benchmark backends for native typed maps and codec facades,
  including no-namespace, single-namespace, and multi-namespace shared-engine
  modes.
- Added a 16-vCPU max-out benchmark writeup for typed and codec embedded
  APIs versus DashMap and the owner-local shardcache embedded baseline.
- Added unit coverage for typed keys and values, borrowed reads, custom
  structs, namespace isolation, codec decoding errors, and default TTL
  behavior.

### Changed

- Bumped the workspace and publishable Rust crate surfaces to `0.3.0` because
  the typed `ShardMap<K, V>` and `codec` APIs are new public API surfaces.
- Renamed the workspace to `shard-kv`, with `shardmap` as the embedded Rust
  crate and `shardcache` as the server crate/binary.
- Moved Redis/Valkey compatibility source into `crates/shardcache-redis/src`.
  The unpublished compatibility crate remains path-included by `shardmap`
  behind the `redis` feature until the extension API is complete.
- Simplified public feature defaults around the embedded sharded cache,
  `redis`, and `redis-server`.
- Added transaction runtime coverage for `MULTI`, `EXEC`, `DISCARD`,
  `WATCH`, and `UNWATCH` behavior.
- Expanded Redis command coverage across the Redis 5.0.14 command table,
  including `BITFIELD`, `DUMP`, `RESTORE`, stream, pub/sub, geo, scripting,
  and HyperLogLog compatibility cases.
- Added adaptive monoio driver selection so single-worker deployments use
  io_uring while multi-worker socket deployments use the legacy poll driver by
  default.
- Added a direct RESP fast path for fanout requests without active
  transactions or direct-shard route checks.

### Validation

- Workspace tests, Redis differential tests, rustdoc, `shardmap` packaging, and
  formatting are part of the 0.3.0 release checklist.
- The 16-vCPU typed/codec benchmark run showed `fc-codec-ref` ahead of
  DashMap reference access for every tested pure GET row, while DashMap kept
  the edge on small pure SET rows and large pure SET rows converged around
  payload-copy throughput.
- Redis 5.0.14 coverage is explicit: all Redis 5.0.14 commands are represented
  in the live RESP benchmark registry, with standalone-only expected-error
  behavior documented in `docs/REDIS_COMPATIBILITY.md`.
- Known compatibility caveats remain documented for snapshot-based
  `WATCH`/`UNWATCH`, constrained scripting, lightweight stream group behavior,
  Pub/Sub subscriber fanout, HyperLogLog representation, and long-lived
  blocking command wakeups.
