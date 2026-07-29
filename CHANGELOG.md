# Changelog

## 0.9.0 - 2026-07-29

### Changed

- Narrowed the public workspace to local storage, persistence, overflow,
  protocol, compatibility, and benchmark surfaces.
- Added runtime-neutral embedded-store extension contracts for routed
  operations, mutation admission and observation, ordered callbacks, and
  bounded snapshots.
- Aligned the optional parent telemetry runtime with `fast-telemetry 0.9.0`;
  the 0.9 public crates now require Rust 1.93.

### Removed

- Removed non-OSS distributed deployment implementations, feature flags,
  configuration, transports, examples, tests, formal models, documentation,
  and benchmark drivers from the public crates.

## 0.8.1 - 2026-07-23

### Security

- Re-keyed process-local maps and raw tables with per-process randomized AHash
  before bucket selection. Stable XXH3 key routing and exact-key comparisons
  remain unchanged, while authenticated clients that can choose keys can no
  longer precompute colliding local buckets from the public routing hash.
  Redis-compatible XXH3 `DIGEST` and `IFDEQ`/`IFDNE` tokens remain unchanged;
  they are fast comparison hints, not authorization or cryptographic integrity
  primitives.

## 0.8.0 - 2026-07-20

### Added

- Added typed vector operations, bounded request/response decoding, vector
  governance metadata, fail-closed governance guards, and reproducible Object
  RAG benchmarks.
- Added canonical vector snapshot and restore handling, TTL preservation,
  bounded HNSW search, and malformed-state rejection.

### Fixed

- Hardened object-overflow startup, cleanup, materialization, snapshots,
  filesystem access, S3 private-CA handling, and benchmark acknowledgement.
- Corrected vector routing, TTL preservation, governance filtering, response
  encoding, UID allocation, and non-finite input validation.

## 0.7.x - Archived

- This pre-1.0 experimental release line covered non-OSS distributed
  deployment work and is not part of the 0.9 public crate surface.

## 0.6.0 - 2026-07-14

### Added

- Added fail-closed governance metadata for exact point values through
  `EmbeddedStore`, shared and worker-local stores, WAL and snapshots, object
  overflow, and partitioned KV overflow. Protected entries
  require a governed filter and cannot be released by ordinary GET, mutable,
  visitor, removal-return, or Redis paths.
- Added versioned governed KV-overflow envelopes for SCNP and Redis/Valkey,
  including metadata integrity checks, retry and handoff preservation, remote
  read filtering, and protected promotion back into resident memory.

- Added the `kv-overflow` feature and `KvOverflowStore`, a bounded embedded
  primary backed by disjoint, fixed-slot shardcache server partitions with
  previous-membership handoff for horizontal expansion.
- Added acknowledged-only LRU/LFU eviction, remote fault-in, direct cluster
  reads, TTL-preserving value envelopes, CRC32 integrity checks, SCNP
  connection pooling, operation deadlines, reconnect retries, and health
  counters.
- Added a feature-gated Redis/Valkey-compatible overflow adapter with pooled
  RESP connections, TLS URLs, environment-only ACL credentials, key
  namespacing, and server-side TTL enforcement.
- Added fallible snapshot materialization and recovery synchronization so the
  local persistence path remains authoritative for remote-only values.
- Added unit coverage for placement, cold-only eviction, failed-write
  retention, fault-in, and snapshot materialization, plus a live two-server
  SCNP integration test for disjoint placement and direct reads.
- Added bounded asynchronous overflow mirroring with per-key ordered worker
  lanes, generation-safe acknowledgments, queue backpressure, explicit remote
  flush, and queue/worker health counters so primary writes do not wait on
  SCNP I/O.
- Added ordered 64-write Redis pipelines, O(1) fixed-slot owner lookup, and
  striped acknowledgment metadata to keep primary overhead bounded while
  overflow endpoints scale horizontally.
- Isolated every network drain from primary storage and metadata locks. Remote
  acknowledgements return through bounded per-shard completion lanes, discard
  successful value payloads, and are applied by the primary under admission or
  memory pressure, at explicit flush boundaries, or by periodic maintenance.
  Each primary shard has exactly one network drain; no worker or completion
  mutex is shared across shards.
- Replaced allocation-backed worker queues with preallocated shard-local rings,
  split primary and worker counters onto separate cache lines, made generation
  counters shard-local, aggregated overflow metrics per batch, and replaced
  topology-sized batch allocation with reusable sparse owner grouping.
- Changed pipeline coalescing to wait once and then drain the shard ring instead
  of waking the network worker for each arriving item, substantially reducing
  sparse-producer admission overhead without extending the configured flush
  deadline.
- Reused each operation's xxh3 key hash in striped overflow metadata tables.
  Hashbrown raw-entry lookups now avoid a second key hash on primary admission,
  completion, retry, and delete paths while retaining resize-safe hashing.
- Stored queued generations in resident entries so successful remote completion
  no longer requires a pending-metadata insertion, and limited pipelines by both
  item count and encoded bytes to bound large-value allocation and tail cost.
- Added independent target-local SCNP tasks on one current-thread runtime for
  shards that own multiple remote targets. Single-target shards retain the
  lower-overhead blocking pipeline; stable key-hash lanes preserve per-key
  order when `max_inflight_per_target` is greater than one.
- Reduced idle SCNP read/write buffers from 64 KiB each to 8 KiB each.
- Added replica-side LRU plus filesystem object-overflow integration coverage,
  including transparent SCNP fault-in for envelopes evicted from replica RAM.
- Added the `kv_overflow_primary_cost` benchmark for measuring embedded SET
  versus key-value overflow enqueue overhead and remote drain time.
- Added stable replica identities, topology handshakes, `OverflowSlot` routing,
  and route-checked direct shard ports. Every remote shard target now has one
  primary-shard owner and one shard-owned mutation/read connection pair.
- Added exact online rebalance for replica and primary-shard topology changes.
  Handoff migrates resident and remote-only values through write, verification,
  and old-owner deletion while preserving the fixed logical slot namespace.
- Added native Rustls TLS 1.3 for SCNP overflow, optional mTLS with leaf
  certificate pinning, reloadable certificates and tokens, bounded concurrent
  handshakes, and non-loopback authentication requirements. The all-feature
  dependency graph contains no OpenSSL or native-tls implementation. Published
  `shardmap` and `shardcache` packages expose the `scnp-tls` build feature.
- Added target-local circuit breakers, adaptive byte-bounded pipelines,
  compression negotiation, topology/path failover counters, per-shard queue
  health, and handoff progress reporting.
- Added independent resident-value and key-metadata ceilings, bounded cleanup,
  key/value and slot-table limits, decompression expansion limits, and bounded
  RESP, SCNP, and client response collection decoding.
- Added filesystem-backed cascading-tier, restart, TLS, topology, stalled
  target, malformed-frame, and memory-amplification tests.
- Added a public 0.6 feature and migration guide plus a CI-verified inventory
  of every locked workspace and third-party dependency.

### Changed

- Bumped the workspace and publishable crate surfaces to `0.6.0`.
- Added explicit timeout-capable connections to `shardcache-client-rs`.
- Preserve primary TTL deadlines across queued/retried overflow writes, make
  `flush_remote` include writes already admitted but not yet enqueued, and
  redact malformed Redis endpoints from configuration errors.
- Replaced contended queue permits and global remote-key metadata locks with
  atomic bounded admission and 64 metadata stripes.
- Moved resident hard-limit projection into the existing shard write critical
  section. This removes a second shard read, a maintenance mutex, and an
  accidental object fault-in from bounded primary writes while keeping
  concurrent admission atomic.
- Replaced address-only membership with stable `replicas` and
  `previous_replicas`. Rebalance is exact and may move ranges between existing
  replicas; operators must retain the previous membership until handoff
  completes.

### Validation

- Five-run no-op adapter benchmarks on Adam with 16 shard-local drains, four
  producers, and 1 KiB values admitted a median 1.90M writes/second at 527
  ns/op and completed 1.269M writes/second end to end. Five direct-SCNP runs
  against a separately pinned 16-shard replica sustained a median 1.044M
  end-to-end writes/second with 680 ns/op primary admission. A 256 KiB byte cap
  sustained 58.4K writes/second for 64 KiB values with a 229 us sampled p99.
- Five-run release benchmarks on Adam with four producers and 1 KiB values
  measured 954 ns/op primary admission and 339K end-to-end writes/second for
  one Valkey endpoint. Four disjoint endpoints with four workers each admitted
  writes at 1,309 ns/op and completed 740K writes/second end to end.
- Redis worker pipelining improved a one-endpoint, four-worker production-path
  benchmark from 34.4K to 347K completed writes/second, approximately 10.1x.
- A final interleaved A/B on Adam compared the hardened branch with commit
  `46a4ca8`. The fully bounded KV enqueue path sustained a median 2.05M
  writes/second, 3.5% below the pre-hardening baseline. Embedded, RESP, and
  SCNP median throughput deltas all stayed within 3.6%; the worst measured
  median p99 change was 4.9%. No overflow drops, backpressure, or server
  protocol errors were observed.

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
