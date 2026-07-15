# Shardcache 0.7 Active-Active Performance

Measured: 2026-07-15

This note preserves the pre-implementation Adam budget and records the first
`ActiveShardMap` measurements. Results distinguish local causal admission from
caller-driven synchronization; neither mode is enabled in the default build.

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
