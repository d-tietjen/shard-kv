# Changelog

## 0.3.2 - Unreleased

### Added

- Added a shared microsecond telemetry clock for full-rate hot-path latency
  sampling without calling `Instant::now()` for every request.
- Added `CacheTelemetryClock` and
  `CacheTelemetry::new_with_latency_sample_rate_and_clock` so callers can
  choose per-request `Instant` timing, the default 1 microsecond shared clock,
  or a custom shared-clock update interval.
- Added telemetry benchmark backends for 100% latency sampling with
  `Instant::now()` and with the shared clock.

### Changed

- Bumped the workspace and publishable Rust crate surfaces to `0.3.2`.

### Validation

- Ran focused telemetry tests, `cargo fmt`, `cargo clippy` for `shardmap`, and
  the telemetry benchmark crate check.
- Ran server-side saturation comparisons for embedded and shared-reference
  backends at 100% telemetry sampling; the shared clock recovered most SET
  throughput lost to per-request `Instant::now()` while keeping GET throughput
  effectively flat.

## 0.3.1 - 2026-06-04

### Changed

- Reduced embedded telemetry hot-path overhead by sampling latency histograms
  while keeping operation counters, byte counters, key gauges, and memory gauges
  exact.
- Added telemetry-enabled embedded benchmark backends so telemetry cost can be
  measured head-to-head against the ordinary embedded backends.
- Avoided unnecessary semantic shadow/generation work for point-key writes until
  semantic data is actually active.
- Bumped the workspace and publishable Rust crate surfaces to `0.3.1`.

### Validation

- Ran focused telemetry tests, point-key shared-store tests, `cargo fmt`, and
  clippy for `shardmap` and the telemetry benchmark feature.
- Ran Linux `perf` and max-out saturation telemetry comparisons on a 32-logical
  CPU server to validate that telemetry is no longer a general bottleneck.

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
