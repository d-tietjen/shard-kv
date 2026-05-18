# fast-cache Native Replication Cost

Published: 2026-05-15

This note measures the primary write-path cost of native fast-cache replication.
It compares an embedded primary with replication disabled against the same
primary exporting native FCRP mutation batches with compression disabled.

## Scope

| Field | Value |
| --- | --- |
| Host | Linux |
| OS | Ubuntu 24.04 LTS family |
| Benchmark driver | `replication_cost` |
| CPU pinning | `taskset -c 0-15` |
| Shards | 16 |
| Clients | 64 |
| Key count | 100k |
| Value pattern | `semi-random` |
| Value pool | 4096 values |
| Duration | 10s measured, 2s warmup |
| Replication mode | `batch-none` |
| Compression | none |

Command shape:

```bash
taskset -c 0-15 target/release/replication_cost \
  --value-sizes 64,512,4096,16384 \
  --mixes set,80-20 \
  --modes baseline,batch-none \
  --clients 64 \
  --shards 16 \
  --duration 10 \
  --warmup 2 \
  --key-count 100000
```

`baseline` is the primary write path with replication disabled. `batch-none`
uses native mutation batches and no compression. The primary, exporter, and
replica all run inside the same benchmark process pinned to the 16-CPU set, so
this measures end-to-end local replication cost under a fixed CPU budget.

## Throughput Cost

| Value | Mix | Baseline ops/s | Replicated ops/s | Retained | Drop |
| ---: | --- | ---: | ---: | ---: | ---: |
| 64B | SET | 13.57M | 6.86M | 50.6% | 49.4% |
| 64B | 80/20 | 16.96M | 11.72M | 69.1% | 30.9% |
| 512B | SET | 12.23M | 5.07M | 41.5% | 58.5% |
| 512B | 80/20 | 10.41M | 7.43M | 71.4% | 28.6% |
| 4KiB | SET | 3.15M | 888.93k | 28.3% | 71.7% |
| 4KiB | 80/20 | 4.00M | 2.35M | 58.7% | 41.3% |
| 16KiB | SET | 765.80k | 254.17k | 33.2% | 66.8% |
| 16KiB | 80/20 | 1.29M | 645.35k | 50.1% | 49.9% |

## Replication Output

| Value | Mix | Wire MiB/s | Replica apply/s | Drops | Backpressure | Queue high-water |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 64B | SET | 1,067.14 | 2.57M | 4 | 863 | 65,536 |
| 64B | 80/20 | 363.47 | 2.81M | 0 | 0 | 2,885 |
| 512B | SET | 3,366.01 | 1.63M | 3 | 3 | 35,510 |
| 512B | 80/20 | 994.36 | 1.78M | 0 | 0 | 1,503 |
| 4KiB | SET | 4,288.93 | 354.15k | 1 | 1 | 1,331 |
| 4KiB | 80/20 | 2,286.03 | 457.54k | 1 | 1 | 899 |
| 16KiB | SET | 4,822.41 | 117.51k | 1 | 1 | 1,616 |
| 16KiB | 80/20 | 2,594.39 | 81.58k | 1 | 1 | 1,289 |

## Raw Matrix

| Mode | Value | Mix | Ops/s | vCPU | ns/op | Emitted | Batches | Raw MiB/s | Wire MiB/s | Drops | Backpressure | Queue hi | Replica apply/s |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 64B | SET | 13.57M | 15.017 | 73.7 | 0 | 0 | 0.00 | 0.00 | 0 | 0 | 0 | 0 |
| batch-none | 64B | SET | 6.86M | 15.813 | 145.7 | 82.69M | 1.30M | 1,065.15 | 1,067.14 | 4 | 863 | 65,536 | 2.57M |
| baseline | 64B | 80/20 | 16.96M | 14.448 | 59.0 | 0 | 0 | 0.00 | 0.00 | 0 | 0 | 0 | 0 |
| batch-none | 64B | 80/20 | 11.72M | 15.504 | 85.3 | 28.13M | 660.42k | 362.46 | 363.47 | 0 | 0 | 2,885 | 2.81M |
| baseline | 512B | SET | 12.23M | 15.534 | 81.8 | 0 | 0 | 0.00 | 0.00 | 0 | 0 | 0 | 0 |
| batch-none | 512B | SET | 5.07M | 15.542 | 197.3 | 60.51M | 958.86k | 3,364.55 | 3,366.01 | 3 | 3 | 35,510 | 1.63M |
| baseline | 512B | 80/20 | 10.41M | 13.847 | 96.0 | 0 | 0 | 0.00 | 0.00 | 0 | 0 | 0 | 0 |
| batch-none | 512B | 80/20 | 7.43M | 15.297 | 134.6 | 17.86M | 638.00k | 993.38 | 994.36 | 0 | 0 | 1,503 | 1.78M |
| baseline | 4KiB | SET | 3.15M | 15.797 | 317.9 | 0 | 0 | 0.00 | 0.00 | 0 | 0 | 0 | 0 |
| batch-none | 4KiB | SET | 888.93k | 15.566 | 1,124.9 | 10.79M | 247.26k | 4,288.56 | 4,288.93 | 1 | 1 | 1,331 | 354.15k |
| baseline | 4KiB | 80/20 | 4.00M | 13.838 | 250.3 | 0 | 0 | 0.00 | 0.00 | 0 | 0 | 0 | 0 |
| batch-none | 4KiB | 80/20 | 2.35M | 13.838 | 426.1 | 5.75M | 517.40k | 2,285.24 | 2,286.03 | 1 | 1 | 899 | 457.54k |
| baseline | 16KiB | SET | 765.80k | 15.525 | 1,305.8 | 0 | 0 | 0.00 | 0.00 | 0 | 0 | 0 | 0 |
| batch-none | 16KiB | SET | 254.17k | 15.537 | 3,934.3 | 3.07M | 211.05k | 4,822.08 | 4,822.41 | 1 | 1 | 1,616 | 117.51k |
| baseline | 16KiB | 80/20 | 1.29M | 13.648 | 775.9 | 0 | 0 | 0.00 | 0.00 | 0 | 0 | 0 | 0 |
| batch-none | 16KiB | 80/20 | 645.35k | 11.952 | 1,549.5 | 1.65M | 321.79k | 2,593.89 | 2,594.39 | 1 | 1 | 1,289 | 81.58k |

## Takeaways

- Replication has a visible primary write-path cost even without compression.
- Under this 16-vCPU cap, replicated SET retains about `28%` to `51%` of
  baseline SET throughput depending on value size.
- Mixed `80/20` workloads retain more throughput, about `50%` to `71%`,
  because only the write side emits replication records.
- Large writes become bandwidth-heavy. The replicated SET rows reached roughly
  `4.3` to `4.8 GiB/s` of native replication stream output at 4KiB and 16KiB.
- The 64B SET row hit the configured queue high-water limit and reported
  backpressure. This makes tiny-value max-write streams a good target for
  queue sizing, batching policy, and per-shard exporter tuning.
- Because the replica is local to the same pinned CPU budget in this benchmark,
  these numbers represent end-to-end local replication cost, not a remote
  replica deployment with separate CPU capacity.
