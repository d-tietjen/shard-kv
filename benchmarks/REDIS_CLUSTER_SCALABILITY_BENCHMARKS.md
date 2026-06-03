# Redis Cluster Scalability Benchmarks

This document records shardcache direct-routing runs against Redis Cluster. The
goal is to compare Redis Cluster slot routing with shardcache's direct shard
routing under the same hardware budget, client settings, fixture policy, and
resolved command plan.

## Reproduce The Value-Size Sweep

Run one target at a time with Redis Cluster, shardcache's native direct-shard
client path, and shardcache RESP compatibility:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis-cluster,shardcache-scnp-direct,shardcache-resp \
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

## Server 16-vCPU Client And RESP Value-Size Sweep

Run bundle:
`benchmarks/reference/server-getset-size-rediscluster-scnp-resp-pinned-20260602T002805Z/`

Remote source:
`/home/dtietjen/shard-kv-bench-redis-cluster.TnRIzc/benchmarks/results/server-getset-size-rediscluster-scnp-resp-pinned-20260602T002805Z`

Run settings:

| Setting | Value |
| --- | --- |
| Host | Benchmark server, Ubuntu 24.04, 32 logical CPUs |
| Targets | `redis-cluster`, `shardcache-scnp-direct`, `shardcache-resp` |
| Server vCPU | 16 |
| Server CPU set | Linux CPUs `0-15` |
| Client CPU set | Linux CPUs `16-31` |
| Clients | 256 |
| Pipeline depth | 256 |
| Key shards | 16 |
| Warmup | 2 seconds |
| Timed duration | 10 seconds |
| Memory budget | 2048 MiB |
| shardcache native path | `shardcache_client_rs` direct-shard SCNP client |
| shardcache compatibility path | RESP |
| Redis protocol | RESP, direct Redis Cluster node routing |
| Git SHA | `a22d10a0b1a5ab7f161fa89e5e3b3cc1b1f24d59` |

Raw results:

| Size | Command | Redis Cluster RESP ops/sec | Redis Cluster p99 ms | shardcache native client ops/sec | shardcache native client p99 ms | shardcache RESP ops/sec | shardcache RESP p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| small | GET | 9,596,423.7 | 6.177 | 17,504,611.9 | 3.537 | 15,979,452.1 | 5.489 |
| small | SET | 9,596,423.7 | 6.177 | 17,504,611.9 | 3.537 | 15,979,452.1 | 5.489 |
| 1 KiB | GET | 3,383,814.5 | 15.016 | 4,371,801.1 | 14.402 | 4,238,185.9 | 23.118 |
| 1 KiB | SET | 3,383,814.5 | 15.131 | 4,371,801.1 | 14.410 | 4,238,185.9 | 23.118 |
| 4 KiB | GET | 971,646.5 | 52.724 | 1,572,488.9 | 34.505 | 1,533,043.6 | 58.622 |
| 4 KiB | SET | 971,646.5 | 53.150 | 1,572,488.9 | 34.505 | 1,533,043.6 | 58.655 |
| 16 KiB | GET | 208,926.4 | 226.099 | 388,713.2 | 140.247 | 365,301.6 | 208.536 |
| 16 KiB | SET | 208,926.4 | 228.065 | 388,713.2 | 140.509 | 365,301.6 | 208.536 |
| 64 KiB | GET | 46,050.2 | 842.531 | 89,446.1 | 595.067 | 80,645.0 | 687.342 |
| 64 KiB | SET | 46,050.2 | 842.531 | 89,446.1 | 595.591 | 80,645.0 | 688.390 |
| 256 KiB | GET | 11,460.7 | 812.122 | 19,681.9 | 640.680 | 17,074.8 | 741.868 |
| 256 KiB | SET | 11,460.7 | 812.122 | 19,681.9 | 649.069 | 17,074.8 | 752.353 |

Shardcache's native direct-shard client had higher ops/sec and lower p99
latency than Redis Cluster for every tested value size. Shardcache RESP also
had higher ops/sec than Redis Cluster for every tested value size; its p99 was
lower for the small, 16 KiB, 64 KiB, and 256 KiB rows, and higher for the 1 KiB
and 4 KiB rows.

## Takeaways

- The native shardcache client path was the fastest target for every row. It
  uses `shardcache_client_rs` with direct SCNP shard routing.
- Shardcache RESP was also faster than Redis Cluster on ops/sec for every row,
  which keeps the Redis-compatible path in the comparison instead of only
  reporting the native protocol.
- RESP compatibility had a p99 tradeoff at 1 KiB and 4 KiB, where Redis Cluster
  had lower p99 despite lower throughput. At small, 16 KiB, 64 KiB, and 256 KiB
  values, shardcache RESP had both higher throughput and lower p99.
- At 64 KiB and 256 KiB, all targets are dominated by payload movement and
  queueing. Those rows are useful as throughput/bandwidth saturation evidence,
  not as low-latency request/response claims.

## Saved Artifacts

The checked-in bundle includes:

- `metadata.json`: run settings and benchmark metadata.
- `resolved-plan.json`: shared suite selection and plan identifiers.
- `redis-cluster.csv`: Redis Cluster result rows.
- `shardcache-scnp-direct.csv`: shardcache native direct-shard client rows.
- `shardcache-resp.csv`: shardcache RESP compatibility rows.
- `summary.json` and `report.md`: generated comparison summary.
- `plans/`: per-target resolved plans used for each isolated size suite.
- `tmp/`: generated Compose overrides and per-leg CSVs.
