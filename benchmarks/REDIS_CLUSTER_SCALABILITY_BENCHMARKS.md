# Redis Cluster Scalability Benchmarks

This document records shardcache direct-routing runs against Redis Cluster. The
goal is to compare Redis Cluster slot routing with shardcache's direct shard
routing under the same hardware budget, client settings, fixture policy, and
resolved command plan.

## Reproduce The Value-Size Sweep

Run one target at a time with Redis Cluster and shardcache SCNP direct routing:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis-cluster,shardcache-scnp-direct \
  --suite redis-getset-size-small,redis-getset-size-1k,redis-getset-size-4k,redis-getset-size-16k,redis-getset-size-64k,redis-getset-size-256k \
  --vcpus 1,2,4,8,16 \
  --key-shards vcpus \
  --pipeline-depth 256 \
  --clients 256 \
  --warmup 2 \
  --duration 10 \
  --memory-budget-mib 2048
```

For publishable Linux runs, pin server cores and client cores apart. The suite
does this automatically when the machine has enough logical CPUs: server
containers use the first `N` Linux CPUs for the selected vCPU count, and Rust
client workers use the remaining CPUs. Override with `BENCH_CLIENT_CPUSET` or
`CLIENT_CPUSET_CPUS` when a host needs a custom layout.

The size suites are intentionally isolated. Do not use one mixed tiny-to-large
suite for final claims, because a single cached command plan can make tiny rows
inherit the batch and memory behavior of the largest payload.

## Adam 16-vCPU Value-Size Sweep

Run bundle:
`benchmarks/reference/adam-getset-size-isolated-pinned-20260602T221920Z/`

Remote source:
`/home/dtietjen/shard-kv-bench-redis-cluster.TnRIzc/benchmarks/results/adam-getset-size-isolated-pinned-20260602T221920Z`

Run settings:

| Setting | Value |
| --- | --- |
| Host | Adam, Ubuntu 24.04, 32 logical CPUs |
| Targets | `redis-cluster`, `shardcache-scnp-direct` |
| Server vCPU | 16 |
| Server CPU set | Linux CPUs `0-15` |
| Client CPU set | Linux CPUs `16-31` |
| Clients | 256 |
| Pipeline depth | 256 |
| Key shards | 16 |
| Warmup | 2 seconds |
| Timed duration | 10 seconds |
| Memory budget | 2048 MiB |
| shardcache protocol | SCNP direct shard routing |
| Redis protocol | RESP, direct Redis Cluster node routing |
| Git SHA | `1d68c75c408456f2aaba1ee8404e550a47c69b49` |

Results:

| Size | Command | Redis Cluster ops/sec | Redis Cluster p99 ms | shardcache SCNP ops/sec | shardcache SCNP p99 ms | Throughput ratio | p99 ratio |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | GET | 9513826.1 | 6.468 | 17356968.4 | 3.654 | 1.82x | 1.77x |
| small | SET | 9513826.1 | 6.468 | 17356968.4 | 3.656 | 1.82x | 1.77x |
| 1 KiB | GET | 3283024.9 | 15.876 | 4185756.6 | 16.720 | 1.27x | 0.95x |
| 1 KiB | SET | 3283024.9 | 15.991 | 4185756.6 | 16.720 | 1.27x | 0.96x |
| 4 KiB | GET | 1010362.7 | 58.262 | 1508316.6 | 39.944 | 1.49x | 1.46x |
| 4 KiB | SET | 1010362.7 | 58.786 | 1508316.6 | 39.944 | 1.49x | 1.47x |
| 16 KiB | GET | 201067.2 | 255.721 | 363960.8 | 156.238 | 1.81x | 1.64x |
| 16 KiB | SET | 201067.2 | 257.688 | 363960.8 | 156.238 | 1.81x | 1.65x |
| 64 KiB | GET | 45580.3 | 881.328 | 90017.6 | 592.445 | 1.97x | 1.49x |
| 64 KiB | SET | 45580.3 | 881.328 | 90017.6 | 592.970 | 1.97x | 1.49x |
| 256 KiB | GET | 11318.1 | 861.405 | 18476.0 | 665.846 | 1.63x | 1.29x |
| 256 KiB | SET | 11318.1 | 861.405 | 18476.0 | 676.332 | 1.63x | 1.27x |

`p99 ratio` is Redis Cluster p99 divided by shardcache SCNP p99. Values above
`1.00x` mean shardcache had lower p99 latency.

## Takeaways

- shardcache SCNP direct routing had higher throughput for every tested value
  size, from the small string case through 256 KiB payloads.
- shardcache had lower p99 latency for every row except the 1 KiB GET/SET rows,
  where Redis Cluster p99 was about 4-5% lower while shardcache still delivered
  1.27x higher throughput.
- The largest throughput gap in this run was 64 KiB payloads, where shardcache
  reached 1.97x Redis Cluster throughput and about 1.49x lower p99.
- At 256 KiB, both targets are dominated by payload movement, but shardcache
  still held a 1.63x throughput advantage and lower p99.

## Saved Artifacts

The checked-in bundle includes:

- `metadata.json`: run settings and benchmark metadata.
- `resolved-plan.json`: shared suite selection and plan identifiers.
- `redis-cluster.csv`: Redis Cluster result rows.
- `shardcache-scnp-direct.csv`: shardcache SCNP direct result rows.
- `summary.json` and `report.md`: generated comparison summary.
- `plans/`: per-target resolved plans used for each isolated size suite.
- `tmp/`: generated Compose overrides and per-leg CSVs.
