# Changelog

## 0.7.2 - 2026-07-19

### Added

- Added the independent `vector` feature to `shardcache-client-rs`, with typed
  SCNP `PING`, `VADD`, `VSIM COUNT ... WITHSCORES WITHATTRIBS`, and `VREM`
  methods for fanout, automatically routed direct, and shard-pinned clients.
- Added FP32 request encoding, typed vector options and results, bounded native
  array decoding, legacy 0.7.1 RESP-envelope compatibility, explicit connect
  and operation deadlines, optional token authentication, and existing Rustls
  client support for typed vector calls.
- Added a reproducible typed Object RAG benchmark plus matching Redis-compatible
  and embedded ShardMap vector cases.

### Fixed

- Return native SCNP value, integer, and array responses for typed `PING`,
  `VADD`, `VREM`, and `VSIM` requests instead of wrapping RESP bytes.
- Route vector opcodes consistently to the dedicated vector shard on fanout and
  direct-shard listeners, including keys whose ordinary hash belongs elsewhere.
- Added command-name/opcode uniqueness coverage to prevent duplicate optional
  Redis command-table entries.

## 0.7.1 - 2026-07-17

### Added

- Added revision-ordered active-sync SET and DELETE operations. Applications
  encode an authoritative, lexicographically sortable revision; the greatest
  revision wins even when an older mutation arrives later or causally observes
  a newer delete. Ordinary active-sync keys retain the 0.7 causal semantics and
  compact metadata layout.

### Changed

- Bumped workspace, publishable crate, and standalone Blossom bridge versions
  to `0.7.1`.
- Bumped the active-sync TLS wire and ALPN versions for revision metadata.
  Revision-aware and 0.7.0 peers must not be mixed in one rolling deployment.

## 0.7.0 - 2026-07-16

### Added

- Added the feature-gated `ActiveShardMap` exact point-value API with per-shard
  causal mutation dots, hybrid logical clocks, remove-wins deletion and expiry,
  deterministic concurrent SET resolution, governance conflict protection, and
  exact cluster-eviction commits.
- Added bounded interval sync blocks, SHA-256 integrity, duplicate and reordered
  delivery handling, compacted-history state transfer, atomic checksummed
  snapshots, explicit bidirectional synchronization, and health snapshots.
- Added independent exact-version local eviction and peer fault-in. Residency
  changes do not create replicated logical deletes, and payloads remain resident
  until an exact recovery source is known.
- Added a dedicated direct-shard Rustls transport behind `active-sync-tls` with
  mandatory mTLS 1.3, ALPN, certificate-fingerprint authorization, cluster and
  shard identity validation, bounded frames and deadlines, credential overlap,
  and immediate node revocation.
- Added convergence, conflict, compaction, corruption, resource-bound, live
  mTLS, credential-rotation, and revocation tests plus active-sync cost,
  guaranteed concurrent-conflict cost, and cross-host mTLS latency benchmarks,
  and a runnable embedded example.
- Added exclusive shard-owned persistence WAL block appenders and one
  background canonical-log merger. Record and byte thresholds bound blocks,
  partial blocks flush on deadlines and shutdown, and real `sync_data` calls
  enforce the configured durability interval without a cross-shard producer
  lock.
- Added optional consensus-ordered eventual conflict handling behind
  `active-sync-consensus-ordered-eventual`. Only ambiguous concurrent mutation
  identities and digests enter the ordering plane; keys, values, and WAL blocks
  continue to replicate exclusively through active-sync. Consensus waits
  release the shard lock, stale decisions are retried, and failed decisions
  leave source blocks unacknowledged. Stable per-candidate finalized-epoch ranks
  impose a transitive total order, preventing three-way conflict cycles under
  reordered WAL delivery.
- Added bounded live conflict batching for consensus-ordered synchronization.
  Independent claims from one shard block are finalized together without
  holding the storage-shard lock; repeated keys preserve exact order, stale
  decisions are revalidated, and failed batches leave the source block
  unacknowledged. The Blossom bridge writes unresolved claims as multiple
  transactions in one signed block and returns exact per-claim certificates.
- Added outcome-oriented active-sync feature flags. Use
  `active-sync-causal-eventual` for deterministic causal/HLC convergence or
  `active-sync-consensus-ordered-eventual` for externally finalized ambiguous
  conflicts; Blossom remains an adapter rather than a public feature name.
- Added a vendored, revision-pinned deterministic Blossom test environment that
  injects consensus outages, invalid certificates, node unavailability,
  latency, duplicate WAL blocks, and different three-way delivery orders, then
  verifies exact version convergence and event-log replay across 32 fixed seeds.
- Added the source-only `shardmap-blossom-bridge` production adapter with
  loopback-only identity-bound endpoints, concurrent supermajority epoch reads,
  exact validator generations, signer-key reload, hard deadlines, bounded
  per-group queues and caches, retry backoff, circuit breaking, and health
  counters. Accepted but omitted claims are resubmitted without sending values
  or WAL data through consensus.
- Added checksummed, fsynced, byte- and entry-bounded candidate-rank recovery,
  a real six-validator Blossom TCP finality/restart test, atomic signer-rotation
  coverage, and an executable three-way convergence model for reordered
  duplicate SETs and remove-wins eviction/deletion behavior.
- Added explicit `CausalEventual` and `ConsensusOrderedEventual` API modes,
  named constructors, and runtime guarantee introspection. The active-sync
  benchmark now separates local versus synchronized execution from causal
  versus consensus conflict ordering and includes the full `consensus-sync`
  profile.
- Added revisioned automatic mTLS peer reconciliation for exact active-sync
  values. One scheduler per local shard performs bounded catch-up outside the
  GET/SET path; stale or conflicting topology revisions fail closed, joins
  require complete shard rounds, and removals require revision-checked drain or
  an explicitly counted force-retirement decision.

### Changed

- Bumped workspace and publishable crate versions to `0.7.0`.
- Kept the private, pre-release Blossom runtime out of the public Cargo
  workspace. The production bridge remains a standalone source integration,
  while the deterministic test harness is vendored and non-published so clean
  builds, CI, and crates.io packaging do not require private Git credentials.

### Validation

- A local release WAL plus TCP-export A/B with four dedicated shard producers
  improved 4 KiB append throughput from 155K/s to 204K/s while reducing CPU
  from 3.47 to 2.32 vCPU. At 64-byte values, throughput remained within 2%
  while CPU fell from 4.62 to 2.60 vCPU. Neither run dropped export frames or
  reported transport failures.
- The final Adam active-sync matrix used eight shards and clients, 1 KiB values,
  pinned CPUs `0-7`, isolated ten-second mode runs, and convergence validation.
  Installing conflict ordering without a conflict stayed within 0.5% of
  `causal-local`. Causal-sync GET measured 15.05M ops/s, SET 2.33M ops/s, and
  80/20 7.94M ops/s; the latter two improved from 1.34M and 5.36M in the prior
  Adam run. Write-heavy modes remain below the 90% release gate and stay
  explicit and disabled by default.
- The full `consensus-sync` profile measured 14.70M GET/s, 2.29M SET/s, and
  7.95M 80/20 ops/s in the ten-second Adam matrix. A 20-second GET repeat
  measured 14.90M causal-sync versus 15.16M consensus-sync with identical
  2.2us p99, confirming no conflict-free read penalty from configuring the
  external orderer.
- The Adam concurrent-conflict benchmark kept local admission effectively
  unchanged. With a synthetic 100us external operation, bounded batching
  improved consensus convergence from 3.20K to 66.0K conflict pairs/s and cut
  sync p99 from 80.2ms to 3.9ms. At zero external latency, batching changed
  convergence by 2.6% and improved p99 from 6.7ms to 5.5ms.
- Default raw-cache and native ShardMap GET, SET, and 80/20 rows remained within
  2.4% of their recorded Adam baselines with zero errors. The all-feature
  workspace suite passed 424 ShardMap unit tests plus integration, differential,
  deterministic-fault, mTLS, overflow, formal-model, and doc tests.
- Three-run Adam medians for the intended read-heavy workload measured 13.78M
  ops/s (77.5% of baseline) at 2.3us p99 for causal-sync 99/1 and 13.34M
  (75.1%) at 2.3us for consensus-sync. At 95/5, causal-sync measured 11.88M
  (67.6%) at 2.7us and consensus-sync measured 11.61M (66.1%) at 2.5us. Every
  synchronized run drained and verified exact convergence.
- Documented the complete opt-in performance impact by consistency profile,
  including baseline-retained throughput, p99 latency, value-size sensitivity,
  post-compaction SET improvement, and deployment guidance. Active sync remains
  absent from the default feature set.

## 0.6.0 - 2026-07-14

### Added

- Added fail-closed governance metadata for exact point values through
  `EmbeddedStore`, shared and worker-local stores, native replication, WAL and
  snapshots, object overflow, and partitioned KV overflow. Protected entries
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
- Added bounded asynchronous replication with per-key ordered worker lanes,
  generation-safe acknowledgments, queue backpressure, explicit remote flush,
  and queue/worker health counters so primary writes do not wait on SCNP I/O.
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
  counters shard-local, aggregated replication metrics per batch, and replaced
  topology-sized batch allocation with reusable sparse owner grouping.
- Changed pipeline coalescing to wait once and then drain the shard ring instead
  of waking the network worker for each arriving item, substantially reducing
  sparse-producer admission overhead without extending the configured flush
  deadline.
- Reused each operation's xxh3 key hash in striped overflow metadata tables.
  Hashbrown raw-entry lookups now avoid a second key hash on primary admission,
  completion, retry, and delete paths while retaining resize-safe hashing.
- Stored queued generations in resident entries so successful replication no
  longer requires a pending-metadata insertion, and limited pipelines by both
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
  RESP, SCNP, FCRP, and client response collection decoding.
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
  writes at 1,309 ns/op and replicated 740K writes/second end to end.
- Redis worker pipelining improved a one-endpoint, four-worker production-path
  benchmark from 34.4K to 347K replicated writes/second, approximately 10.1x.
- A final interleaved A/B on Adam compared the hardened branch with commit
  `46a4ca8`. The fully bounded KV enqueue path sustained a median 2.05M
  writes/second, 3.5% below the pre-hardening baseline. Embedded, local/TCP
  replication, RESP, and SCNP median throughput deltas all stayed within 3.6%;
  the worst measured median p99 change was 4.9%. No replication drops,
  backpressure, or server protocol errors were observed.

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
