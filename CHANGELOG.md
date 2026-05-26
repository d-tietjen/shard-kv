# Changelog

## 0.2.0 - Unreleased

### Changed

- Split the public facade crate from `fast-cache-core` so downstream users can
  depend on the stable `fast-cache` entry point while core implementation code
  stays isolated.
- Moved Redis/Valkey compatibility source into `crates/fast-cache-redis/src`.
  The crate now depends on `fast-cache-core`; core still path-includes those
  files behind `redis` until the extension API is complete.
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

- Workspace tests, Redis differential tests, rustdoc, core packaging, and
  formatting are part of the 0.2.0 release checklist.
- Redis 5.0.14 coverage is explicit: all Redis 5.0.14 commands are represented
  in the live RESP benchmark registry, with standalone-only expected-error
  behavior documented in `docs/REDIS_COMPATIBILITY.md`.
- Known compatibility caveats remain documented for snapshot-based
  `WATCH`/`UNWATCH`, constrained scripting, lightweight stream group behavior,
  Pub/Sub subscriber fanout, HyperLogLog representation, and long-lived
  blocking command wakeups.
