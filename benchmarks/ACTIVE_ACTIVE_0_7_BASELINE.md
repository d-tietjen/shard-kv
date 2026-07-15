# Shardcache 0.7 Active-Active Performance Baseline

Measured: 2026-07-15

This note establishes the pre-implementation performance budget for the 0.7
active-active work. It is not an active-active benchmark result. Commit
`2cf9518` contains the design, but does not contain `ActiveShardMap`, causal
mutation records, eviction stubs, cold nominations, or interval WAL-block
exchange.

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
