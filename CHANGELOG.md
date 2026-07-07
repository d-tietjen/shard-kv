# Changelog

## 0.5.0 - Unreleased

### Added

- Added feature-gated object overflow for in-memory primary storage. Cold
  values can now be offloaded behind an `ObjectOverflowStore` boundary while
  logical key metadata remains resident.
- Added the `object-overflow` feature, `[object_overflow]` configuration,
  shard-level object-overflow stats, and a filesystem-backed local adapter for
  tests and local deployments.
- Added owned-read fault-in for offloaded values, remote cleanup on delete,
  overwrite, and TTL expiry, plus snapshot materialization for remote entries.
- Added self-describing object-overflow payload encoding with configurable
  `none`, `lz4`, or `zstd` compression, CRC32 integrity checks, retry knobs,
  and explicit retain-or-evict failure policy.
- Added bounded object-overflow worker execution, generation-scoped object
  names, generation markers, conservative file-backend orphan cleanup,
  fail-loud snapshot materialization, degraded-state health, and expanded
  object-overflow counters.
- Added the `object-overflow-s3` feature with a RustFS/S3-compatible adapter
  backed by `object_store`, including path-style requests, HTTP/TLS controls,
  env-sourced credentials, and server-side encryption configuration.
- Added filesystem-backed object-overflow integration tests that exercise real
  file creation, snapshot materialization, missing-payload failure, remote
  delete, and stale-generation cleanup.
- Added an ignored/env-gated S3/RustFS-compatible object-overflow smoke test
  for live endpoint validation.
- Added explicit cold-only object-overflow gating with configurable idle ticks,
  optional access-frequency limits, and hot-skip accounting so recent hot values
  stay resident under memory pressure.
- Added the `object_overflow_fs_cost` benchmark binary for resident set/get,
  cold filesystem offload, and filesystem fault-in performance measurements.

### Changed

- Bumped the workspace and publishable crate surfaces to `0.5.0`.

### Validation

- Ran focused object-overflow tests for offload/fault-in, snapshot
  materialization, TTL cleanup, and corrupted remote payload rejection.
- Ran filesystem-backed object-overflow integration tests and a release-mode
  filesystem object-overflow benchmark sample.
- Ran `cargo check` for default shardmap, embedded-only shardmap, and
  shardcache with `object-overflow`.
- Ran clippy for the object-overflow feature release.

## 0.4.1 - Unreleased

### Added

- Added a parent-runtime telemetry integration path backed by
  `fast-telemetry` 0.7.1 so embedded shardmap deployments can register metrics
  into an application-owned runtime instead of always creating a shardmap-owned
  runtime.
- Added grouped aggregate telemetry counters for shardmap cache operations and
  WAL append counters using `fast-telemetry::CounterSet`.
- Added a grouped-counter benchmark writeup in
  `benchmarks/SHARDMAP_TELEMETRY_GROUPED_COUNTERS.md` documenting direct
  telemetry update cost before and after making grouped counters authoritative.

### Changed

- Bumped the workspace and publishable crate surfaces to `0.4.1`.
- Bumped `fast-telemetry` to `0.7.1`.
- Made grouped telemetry counters authoritative: snapshots, Prometheus export,
  and runtime visitor export now read from the grouped counter set instead of
  mirroring hot-path updates into legacy individual counters.

### Validation

- Ran focused telemetry tests for snapshots, Prometheus export, runtime visitor
  export, latency sampling exactness, and parent-runtime reuse.
- Ran `cargo check --workspace --all-targets`.
- Benchmarked direct telemetry counter update cost against the mirrored
  grouped-counter implementation; the main hot paths improved by roughly
  30-62% in the single-thread direct-cost run, with an end-to-end saturation
  sanity check documented as too noisy to use as the primary signal.

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
- Removed the separate Redis compatibility package wrapper from the publish set;
  Redis/Valkey compatibility is exposed through `shardcache` and `shardmap`
  feature flags.

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
- Moved Redis/Valkey compatibility source under `crates/shardmap/src/redis_compat`
  behind the `redis` feature.
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
