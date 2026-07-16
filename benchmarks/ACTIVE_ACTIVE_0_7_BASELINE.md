# Shardcache 0.7 Active-Active Performance

Measured through: 2026-07-16

This note preserves the pre-implementation Adam budget and records the first
`ActiveShardMap` measurements. Results distinguish local causal admission from
caller-driven synchronization; neither mode is enabled in the default build.

## Final 0.7 Candidate On Adam

The 2026-07-16 candidate was built from `dt/active-active-replication` and run
on Adam's CPUs `0-7`. Each mode ran in a separate process with eight shards,
eight clients, 100,000 keys, 1 KiB values, a two-second warmup, a ten-second
measurement, a 100 ms sync interval, and one latency sample per 10,000
operations. The benchmark now propagates background sync errors, drains bounded
final rounds until no transfer remains, and performs a full-key convergence
check. `consensus-local` and `consensus-sync` install the external
conflict-ordering callback but generate no conflicts; they isolate the normal
hot-path cost of making Blossom ordering available.

| Workload | Mode | Ops/s | Baseline retained | p50 | p99 | p999 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GET | baseline | 17.70M | 100.0% | 360ns | 2.1us | 4.7us |
| GET | causal-local | 15.78M | 89.2% | 390ns | 2.3us | 6.7us |
| GET | consensus-local | 15.72M | 88.9% | 380ns | 2.3us | 12.2us |
| GET | causal-sync | 15.05M | 85.0% | 400ns | 2.2us | 8.7us |
| GET | consensus-sync | 14.70M | 83.1% | 390ns | 2.6us | 21.6us |
| SET | baseline | 7.86M | 100.0% | 630ns | 6.0us | 22.8us |
| SET | causal-local | 2.75M | 34.9% | 1.8us | 17.2us | 28.5us |
| SET | consensus-local | 2.73M | 34.8% | 1.8us | 15.5us | 34.4us |
| SET | causal-sync | 2.33M | 29.6% | 1.8us | 19.3us | 119.6us |
| SET | consensus-sync | 2.29M | 29.1% | 1.8us | 19.3us | 49.6us |
| 80% GET / 20% SET | baseline | 14.72M | 100.0% | 390ns | 2.8us | 9.6us |
| 80% GET / 20% SET | causal-local | 9.94M | 67.6% | 450ns | 3.6us | 8.4us |
| 80% GET / 20% SET | consensus-local | 9.90M | 67.3% | 450ns | 3.7us | 18.3us |
| 80% GET / 20% SET | causal-sync | 7.94M | 54.0% | 450ns | 3.8us | 25.2us |
| 80% GET / 20% SET | consensus-sync | 7.95M | 54.0% | 450ns | 3.9us | 24.9us |

### Write-path compaction follow-up

A same-load Adam A/B on 2026-07-16 compared the preserved pre-compaction
binary with the compacted build. Mutation dots now share immutable causal
origins, deadline and residency metadata use niche-backed eight-byte layouts,
and pending-byte accounting includes the actual mutation structure size. On a
64-bit build, `ActiveMutation` decreased from at most 240 bytes to 192 bytes
and `VersionState` decreased from 192 bytes to 144 bytes.

| Workload | Pre-compaction | Compacted | Change | Pre p50 / p99 | Compacted p50 / p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| SET, three-run mean | 2.761M | 3.608M | +30.7% | 1.8us / 18.0us | 1.4us / 15.4us |
| 80% GET / 20% SET | 9.985M | 11.048M | +10.6% | 460ns / 3.7us | 460ns / 3.2us |
| GET | 15.626M | 15.525M | -0.6% | 390ns / 2.3us | 390ns / 2.3us |

The compacted SET run retained 44.5% of its same-run 8.109M embedded baseline.
The small GET difference is within run-to-run noise and had no measured latency
regression. These follow-up runs used 15-second measurement windows; all other
workload parameters match the canonical command below.

Installing the conflict orderer reduced throughput by at most 0.5% relative to
`causal-local` in these canonical rows. Consensus is not called without an
ambiguous conflict. `consensus-sync` remained within 2.4% of `causal-sync` in
the ten-second rows. A 20-second GET repeat measured 14.90M causal-sync and
15.16M consensus-sync ops/s with the same 2.2us p99, confirming that the
conflict-free read path has no directional consensus penalty. Against the
earlier Adam implementation, causal-sync GET
was effectively flat at 15.05M versus 15.11M ops/s, while SET increased from
1.34M to 2.33M and 80/20 increased from 5.36M to 7.94M ops/s. Local and
background write modes still do not meet the original 90% throughput gate, so
active sync remains feature-gated and opt-in.

### Concurrent-conflict cost

The dedicated conflict driver was run on Adam's CPUs `0-7` with eight shards
and 1 KiB values. Both nodes overwrote the same previously synchronized keys in
every round, each sync reported two conflict applications per key, and the
driver verified exact convergence after every round. Admission and convergence
were timed separately.

| Mode | Delay | Batch bound | Conflict pairs | Admission mutations/s | Convergence pairs/s | End-to-end pairs/s | Sync p99 | Claims/batch |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Causal | n/a | 256 | 51,200 | 2.227M | 333.9K | 256.9K | 3.2ms | n/a |
| Consensus | 0 | 1 | 51,200 | 2.136M | 182.2K | 155.6K | 6.7ms | 1 |
| Consensus | 0 | 256 | 51,200 | 2.320M | 186.9K | 160.9K | 5.5ms | 128 |
| Consensus | 100us | 1 | 2,560 | 2.242M | 3.20K | 3.19K | 80.2ms | 1 |
| Consensus | 100us | 256 | 2,560 | 2.624M | 66.0K | 62.9K | 3.9ms | 32 |

`--batch-items 1` preserves the previous per-claim path as the direct control.
At zero external latency, bounded batching changes convergence by only 2.6%
and improves p99 from 6.7ms to 5.5ms. With 100us synthetic latency per external
operation, batching improves convergence by 20.6x and cuts p99 by 95%. The
engine still processes two logical conflict applications per key pair, but it
submits one bounded claim batch per shard block instead of one external request
per claim. Causal eventual remains the cheaper mode when externally finalized
conflict order is not required.

Canonical conflict commands:

```bash
taskset -c 0-7 target/release/active_sync_conflict_cost \
  --modes MODE --shards 8 --conflict-keys 1024 --value-size 1024 \
  --warmup-rounds 5 --rounds 50 --batch-items BATCH \
  --orderer-delay-micros 0

taskset -c 0-7 target/release/active_sync_conflict_cost \
  --modes MODE --shards 8 --conflict-keys 256 --value-size 1024 \
  --warmup-rounds 2 --rounds 10 --batch-items BATCH \
  --orderer-delay-micros 100
```

The 80/20 value-size sensitivity run used the same settings, except the 64 KiB
case used 10,000 keys to bound resident memory:

| Value | Baseline | Causal local | Consensus local | Causal sync |
| --- | ---: | ---: | ---: | ---: |
| 64 B | 15.45M | 9.98M (64.6%) | 10.05M (65.1%) | 8.08M (52.3%) |
| 1 KiB | 14.72M | 9.94M (67.6%) | 9.90M (67.3%) | 7.94M (54.0%) |
| 64 KiB | 3.21M | 3.04M (94.9%) | 3.05M (95.2%) | 2.10M (65.5%) |

Large borrowed values keep local active metadata close to baseline, but full
payload retention and circulation in background interval blocks remains
visible. Durable WAL-offset descriptors are still the planned fix for that
cost.

The default, feature-disabled surfaces were rerun with the existing
`saturation` driver. The 20-second raw-cache GET repeat reached 33.33M ops/s;
the other rows used ten-second measurements. Every row completed with zero
errors and remained within 2.4% of its recorded Adam baseline.

| Workload | Raw cache | Prior raw | Native ShardMap | Prior native |
| --- | ---: | ---: | ---: | ---: |
| GET | 33.33M | 33.98M | 21.94M | 22.21M |
| SET | 10.27M | 10.38M | 9.64M | 9.70M |
| 80% GET / 20% SET | 19.72M | 20.19M | 16.06M | 16.23M |

Canonical command shape:

```bash
taskset -c 0-7 target/release/active_sync_cost \
  --modes MODE --shards 8 --clients 8 --key-count 100000 \
  --value-size 1024 --read-percent MIX --warmup 2 --duration 10 \
  --sync-interval-ms 100 --latency-sample-rate 10000
```

## Initial ActiveShardMap Measurement

The following release-build smoke measurement ran on the Apple M5 Max
development host with four shards, four clients, 10,000 keys, 1 KiB values, a
one-second warmup, a three-second measured phase, and a 100 ms explicit sync
interval. `baseline` is the same `EmbeddedStore` byte API. `active-local` keeps
causal metadata and interval blocks but performs no peer sync. `active-sync`
adds one background in-process peer exchange.

| Workload | Mode | Ops/s | Baseline retained | p99 | Baseline p99 ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| GET | baseline | 14.58M | 100.0% | 1.4us | 1.00x |
| GET | active-local | 11.30M | 77.5% | 2.3us | 1.64x |
| GET | active-sync | 13.12M | 90.0% | 1.6us | 1.14x |
| SET | baseline | 5.58M | 100.0% | 8.5us | 1.00x |
| SET | active-local | 1.90M | 34.0% | 20.7us | 2.44x |
| SET | active-sync | 1.27M | 22.8% | 32.0us | 3.76x |
| 80% GET / 20% SET | baseline | 10.38M | 100.0% | 2.2us | 1.00x |
| 80% GET / 20% SET | active-local | 5.29M | 51.0% | 6.6us | 3.00x |
| 80% GET / 20% SET | active-sync | 6.88M | 66.2% | 4.9us | 2.23x |

These short local rows are diagnostic, not release claims. One local GET row
reached the 90% throughput target, but the canonical Adam run below did not;
causal write admission and mixed tails also do not meet the release gates. The
current implementation creates causal records and retains value payloads
synchronously; moving block construction to durable WAL-offset descriptors
remains required before active sync should be enabled on a write-heavy
production primary.

Command shape:

```bash
target/release/active_sync_cost \
  --modes baseline,active-local,active-sync \
  --shards 4 --clients 4 --key-count 10000 --value-size 1024 \
  --read-percent MIX --warmup 1 --duration 3 \
  --sync-interval-ms 100 --latency-sample-rate 1000
```

## Optimized GET Fast Path

Commit `f009e6d` removes duplicate key routing from active reads, returns
conflict-free readable payloads directly from embedded storage, and adds a
zero-copy TTL setter so baseline and active TTL values have equivalent buffer
ownership. A monotonic per-shard governance-conflict guard keeps the direct
return fail-closed. Expired misses still enter active metadata handling and
produce replicated tombstones.

The following ten-second release measurements used eight shards, eight clients,
100,000 keys, 1 KiB values, a two-second warmup, and a 100 ms sync interval on
the Apple M5 Max development host. Each mode was invoked separately to avoid
the local command runner's duration limit.

| Values | Mode | Ops/s | Baseline retained | p50 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| Plain | baseline | 17.78M | 100.0% | 250ns | 3.6us |
| Plain | active-local | 17.34M | 97.5% | 250ns | 3.4us |
| Plain | active-sync | 17.39M | 97.8% | 291ns | 3.3us |
| TTL | baseline | 17.24M | 100.0% | 250ns | 3.5us |
| TTL | active-local | 17.05M | 98.9% | 291ns | 3.3us |
| TTL | active-sync | 17.07M | 99.0% | 291ns | 3.2us |

These local rows show that plain and live-TTL GET throughput are within two and
a half percent of the embedded baseline, with no p99 regression. They do not
replace the pinned Adam result below; commit `f009e6d` still requires the same
canonical Adam rerun before using these figures as a release claim.

TTL command shape:

```bash
target/release/active_sync_cost \
  --modes MODE --shards 8 --clients 8 --key-count 100000 --value-size 1024 \
  --read-percent 100 --warmup 2 --duration 10 \
  --sync-interval-ms 100 --latency-sample-rate 10000 --ttl-seconds 3600
```

## Optimized Active Write Path

Commit `85a0630` reduces local mutation admission and interval-block overhead.
It reuses the embedded route hash for active version lookup, uses the embedded
store's no-TTL write path for plain values, reads the wall clock once, and
transfers pending record vectors into sealed blocks without relocating their
contents. Retired blocks are released after dropping the shard metadata lock.
The common causal context, mutation kind, and recovery-peer state are also
stored more compactly and checked by layout regression tests.

The following ten-second release measurements used eight shards, eight clients,
100,000 keys, 1 KiB values, a two-second warmup, and a 100 ms sync interval on
the Apple M5 Max development host. Each mode was invoked separately. The GET
row verifies that the write-path changes did not regress the direct read path.

| Workload | Mode | Ops/s | Baseline retained | p50 | p99 | p999 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GET | baseline | 15.35M | 100.0% | 291ns | 3.7us | 17.7us |
| GET | active-local | 15.44M | 100.6% | 292ns | 3.8us | 19.5us |
| GET | active-sync | 15.50M | 101.0% | 292ns | 3.8us | 19.0us |
| SET | baseline | 9.64M | 100.0% | 375ns | 6.6us | 32.0us |
| SET | active-local | 3.19M | 33.1% | 1.1us | 22.5us | 79.4us |
| SET | active-sync | 2.77M | 28.7% | 1.2us | 25.3us | 48.5us |
| 80% GET / 20% SET | baseline | 12.86M | 100.0% | 333ns | 4.7us | 27.2us |
| 80% GET / 20% SET | active-local | 9.56M | 74.3% | 375ns | 6.0us | 26.2us |
| 80% GET / 20% SET | active-sync | 9.70M | 75.4% | 333ns | 7.8us | 33.6us |

Against the pre-change local write smoke measurements, SET throughput improved
by approximately 29% in `active-local` and 25% in `active-sync`. The active
metadata layout now uses 32 bytes for `CausalContext`, 16 bytes for
`MutationKind`, 192 bytes for `VersionState`, and 240 bytes for
`ActiveMutation` on this target.

These rows remain development-host diagnostics. The current interval stream
retains full mutation values in memory; it is not a durable WAL and does not yet
reference value bytes by WAL offset. SET-heavy and mixed modes still miss the
90% release throughput gate, so the optimized code requires the canonical Adam
rerun and further write-path work before active sync is enabled by default.

Command shape:

```bash
target/release/active_sync_cost \
  --modes MODE --shards 8 --clients 8 --key-count 100000 --value-size 1024 \
  --read-percent MIX --warmup 2 --duration 10 \
  --sync-interval-ms 100 --latency-sample-rate 10000
```

## Implemented ActiveShardMap On Adam

Commit `e835105` was built natively in release mode and run on CPUs `0-7` with
eight shards, eight clients, 100,000 keys, 1 KiB values, a two-second warmup,
a ten-second measured phase, one latency sample per 10,000 operations, and a
100 ms explicit sync interval.

| Workload | Mode | Ops/s | Baseline retained | p99 | Baseline p99 ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| GET | baseline | 18.47M | 100.0% | 2.1us | 1.00x |
| GET | active-local | 15.99M | 86.6% | 2.2us | 1.05x |
| GET | active-sync | 15.11M | 81.8% | 2.3us | 1.10x |
| SET | baseline | 7.97M | 100.0% | 5.4us | 1.00x |
| SET | active-local | 1.64M | 20.5% | 33.3us | 6.17x |
| SET | active-sync | 1.34M | 16.8% | 59.8us | 11.07x |
| 80% GET / 20% SET | baseline | 14.66M | 100.0% | 2.8us | 1.00x |
| 80% GET / 20% SET | active-local | 6.63M | 45.2% | 5.2us | 1.86x |
| 80% GET / 20% SET | active-sync | 5.36M | 36.6% | 6.3us | 2.25x |

The active-active read path remains close in p99, but neither throughput mode
meets the 90% gate on Adam. Write admission is the dominant limit, and the
synchronized write-heavy p999 reached `496.6us`. This is not a production-ready
performance result for write-heavy primaries. The feature remains explicit and
off by default while WAL-offset block construction and shard-owned background
drains are implemented and remeasured.

Command shape:

```bash
taskset -c 0-7 target/release/active_sync_cost \
  --modes baseline,active-local,active-sync \
  --shards 8 --clients 8 --key-count 100000 --value-size 1024 \
  --read-percent MIX --warmup 2 --duration 10 \
  --sync-interval-ms 100 --latency-sample-rate 10000
```

## Pre-Implementation Adam Baseline

## Host And Method

| Field | Value |
| --- | --- |
| Host | `adam` |
| CPU | AMD Ryzen 9 3950X, 16 cores / 32 threads |
| OS | Linux 6.8.0-111-generic x86_64 |
| Rust | 1.95.0 |
| CPU set | logical CPUs `0-7` |
| Shards / clients | 8 / 8 |
| Keys / value | 100,000 / 1 KiB |
| Warmup / measured | 2 seconds / 10 seconds |
| Latency sampling | 1 in 10,000 operations |

The cache/map comparison uses borrowed reads for both backends. `fc-shared-ref`
is the raw byte-oriented shared cache used by persistence, eviction, overflow,
and server paths. `fc-typed-ref` is the native typed `ShardMap<Vec<u8>,
Vec<u8>>`. These are different product surfaces, so their absolute numbers must
not be mixed with the separate replication driver.

## Cache Versus Native ShardMap

| Workload | Raw cache ops/s | Native ShardMap ops/s | Raw cache lead | Raw cache p99 | ShardMap p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 33.98M | 22.21M | 53.0% | 680ns | 820ns |
| SET | 10.38M | 9.70M | 7.0% | 2.5us | 2.1us |
| 80% GET / 20% SET | 20.19M | 16.23M | 24.4% | 1.5us | 1.2us |

Command shape:

```bash
taskset -c 0-7 target/release/saturation \
  --backends fc-shared-ref,fc-typed-ref \
  --vcpu-budget 8 --clients 8 \
  --value-size 1024 --mix MIX \
  --key-count 100000 --duration 10 --warmup 2 \
  --latency-sample-rate 10000
```

The 0.7 implementation must benchmark each supported API against its own
disabled baseline. The raw cache is the relevant denominator for the initial
durable, eviction-aware implementation. A native `ShardMap<K, V>` result is
valid only after active sync supports native typed values without routing them
through the byte cache or a codec facade.

## Existing Replication Cost Proxy

The existing `replication_cost` driver emits current single-primary mutation
batches. It does not model causal context, interval block sealing, bidirectional
anti-entropy, conflict resolution, or active-active eviction. It provides a
warning about the cost of copying and applying every mutation on the primary's
CPU budget, not a forecast of the 0.7 design.

| Replica apply | Workload | Baseline ops/s | Batched ops/s | Retained | Baseline p99 | Batched p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Excluded | SET | 7.25M | 2.74M | 37.8% | 4.3us | 27.1us |
| Excluded | 80% GET / 20% SET | 8.90M | 5.99M | 67.4% | 4.0us | 10.2us |
| Same CPU set | SET | 7.03M | 2.27M | 32.3% | 4.9us | 36.4us |
| Same CPU set | 80% GET / 20% SET | 8.97M | 5.15M | 57.4% | 3.9us | 15.0us |

Command shape:

```bash
taskset -c 0-7 target/release/replication_cost \
  --value-sizes 1024 --mixes set,80-20 \
  --modes baseline,batch-none \
  --clients 8 --shards 8 --key-count 100000 \
  --duration 10 --warmup 2 \
  --replica-mode REPLICA_MODE \
  --latency-sample-rate 10000
```

The 0.7 architecture therefore must not enqueue copied values per write or
apply replicas on the primary's shard workers. Its proposed WAL-offset block
builders and shard-owned background I/O must be measured directly before making
an overhead claim.

## Existing LRU Cost Baseline

The raw cache was also measured with a Zipf `0.99` 80/20 workload and a 25%
resident capacity. This measures current local LRU work only; no active-active
nomination or fault-in state exists.

| Mode | Ops/s | Retained | p99 | p999 |
| --- | ---: | ---: | ---: | ---: |
| Resident, no eviction | 22.86M | 100.0% | 1.3us | 2.8us |
| LRU, 25% capacity | 16.12M | 70.5% | 2.2us | 30.3us |

The tail increase makes eviction a required independent benchmark axis. A
single aggregate active-sync throughput number would hide candidate selection,
stub installation, nomination, and fault-in costs.

## Required 0.7 Matrix

After implementation, run the following rows on the same commit and CPU layout:

1. Active sync disabled.
2. Active sync enabled with networking disabled, measuring causal metadata and
   WAL interval admission only.
3. Three active members on disjoint CPU sets with interval block exchange.
4. Local LRU eviction with cluster nomination disabled.
5. Local LRU eviction with majority cold nomination enabled.
6. Exact-version fault-in from local overflow and from a peer.
7. One delayed or partitioned peer while healthy peers continue syncing.

Run GET, SET, and 80/20 at 64 B, 1 KiB, and 64 KiB. Report cache-hit throughput,
write throughput, p50/p99/p999, resident hit rate, bytes copied per mutation,
block bytes per write, queue depth, CPU by shard runtime, eviction-stub bytes,
nomination amplification, and fault-in latency.

The release gate remains the one in
[`ACTIVE_ACTIVE_REPLICATION.md`](../docs/ACTIVE_ACTIVE_REPLICATION.md): local
mode must retain at least 90% of its own embedded-only throughput and p99 must
remain within 1.2x. This baseline shows that the current per-mutation replication
path does not meet that gate and must not be reused unchanged.
