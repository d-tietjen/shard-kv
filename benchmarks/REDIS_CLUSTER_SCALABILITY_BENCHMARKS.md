# Redis Cluster Scalability Benchmarks

This document records Redis Cluster scaling runs against shardcache's routed
and shared-port server paths. The goal is to compare Redis Cluster slot
routing with shardcache direct shard routing, shardcache shared-port SCNP
without client direct routing, and shardcache RESP compatibility under the same
hardware budget, client settings, fixture policy, and resolved command plan.

## Reproduce The Value-Size Sweep

Run one target at a time with Redis Cluster, shardcache's native direct-shard
client path, shardcache shared-port SCNP without client direct routing, and
shardcache RESP compatibility:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis-cluster,shardcache-scnp-direct,shardcache-scnp,shardcache-resp \
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

The `shardcache-scnp` target uses the same multi-vCPU shard count as
`shardcache-scnp-direct`, but clients connect through the shared server port
instead of per-shard direct ports. That row measures the cost of server-side
routing when shardcache scales across vCPUs without client direct routing.

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

This original saved bundle was captured before the shared-port SCNP target was
added to the cluster value-size recipe. The shared-port SCNP companion run below
adds the missing non-direct-routing shardcache rows.

Raw results:

| Size | Command | Redis Cluster RESP ops/sec | Redis Cluster p99 ms | shardcache direct SCNP ops/sec | shardcache direct SCNP p99 ms | shardcache shared SCNP ops/sec | shardcache shared SCNP p99 ms | shardcache RESP ops/sec | shardcache RESP p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small | GET | 9,596,423.7 | 6.177 | 17,504,611.9 | 3.537 | 12,807,763.7 | 4.936 | 15,979,452.1 | 5.489 |
| small | SET | 9,596,423.7 | 6.177 | 17,504,611.9 | 3.537 | 12,807,763.7 | 4.936 | 15,979,452.1 | 5.489 |
| 1 KiB | GET | 3,383,814.5 | 15.016 | 4,371,801.1 | 14.402 | 3,716,600.1 | 25.919 | 4,238,185.9 | 23.118 |
| 1 KiB | SET | 3,383,814.5 | 15.131 | 4,371,801.1 | 14.410 | 3,716,600.1 | 25.919 | 4,238,185.9 | 23.118 |
| 4 KiB | GET | 971,646.5 | 52.724 | 1,572,488.9 | 34.505 | 1,527,329.5 | 58.917 | 1,533,043.6 | 58.622 |
| 4 KiB | SET | 971,646.5 | 53.150 | 1,572,488.9 | 34.505 | 1,527,329.5 | 58.917 | 1,533,043.6 | 58.655 |
| 16 KiB | GET | 208,926.4 | 226.099 | 388,713.2 | 140.247 | 360,015.5 | 217.186 | 365,301.6 | 208.536 |
| 16 KiB | SET | 208,926.4 | 228.065 | 388,713.2 | 140.509 | 360,015.5 | 217.186 | 365,301.6 | 208.536 |
| 64 KiB | GET | 46,050.2 | 842.531 | 89,446.1 | 595.067 | 77,939.9 | 702.546 | 80,645.0 | 687.342 |
| 64 KiB | SET | 46,050.2 | 842.531 | 89,446.1 | 595.591 | 77,939.9 | 703.594 | 80,645.0 | 688.390 |
| 256 KiB | GET | 11,460.7 | 812.122 | 19,681.9 | 640.680 | 16,792.7 | 757.072 | 17,074.8 | 741.868 |
| 256 KiB | SET | 11,460.7 | 812.122 | 19,681.9 | 649.069 | 16,792.7 | 768.082 | 17,074.8 | 752.353 |

Shardcache's native direct-shard client had higher ops/sec and lower p99
latency than Redis Cluster for every tested value size. Shardcache shared-port
SCNP also had higher ops/sec than Redis Cluster for every tested value size;
its p99 was lower for the small, 16 KiB, 64 KiB, and 256 KiB rows, and higher
for the 1 KiB and 4 KiB rows. Shardcache RESP had higher ops/sec than Redis
Cluster for every tested value size; its p99 was lower for the small, 16 KiB,
64 KiB, and 256 KiB rows, and higher for the 1 KiB and 4 KiB rows.

## Shared-Port SCNP Companion Sweep

Run bundle:
`benchmarks/reference/server-getset-size-shardcache-scnp-shared-pinned-20260603T043950Z/`

Run settings:

| Setting | Value |
| --- | --- |
| Host | Benchmark server, Ubuntu 24.04, 32 logical CPUs |
| Target | `shardcache-scnp` |
| Server vCPU matrix | `1,2,4,8,16` |
| Server CPU set | First `N` Linux CPUs for each vCPU step |
| Client CPU set | Remaining Linux CPUs |
| Clients | 256 |
| Pipeline depth | 256 |
| Key shards | Matches vCPU count |
| Warmup | 2 seconds |
| Timed duration | 10 seconds |
| Memory budget | 2048 MiB |
| shardcache shared path | SCNP through one shared server port |
| Git SHA | `8e1795a6f492509635d2fa52acd4f6f6c81a52f5` |

Shared-port SCNP scaling, GET rows:

| Size | 1 vCPU ops/sec | 1 vCPU p99 ms | 2 vCPU ops/sec | 2 vCPU p99 ms | 4 vCPU ops/sec | 4 vCPU p99 ms | 8 vCPU ops/sec | 8 vCPU p99 ms | 16 vCPU ops/sec | 16 vCPU p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 1,648,557.4 | 23.101 | 2,842,927.6 | 13.558 | 4,628,330.3 | 8.684 | 7,549,058.8 | 5.833 | 12,807,763.7 | 4.936 |
| 1 KiB | 969,648.7 | 39.387 | 1,100,317.1 | 33.620 | 1,891,880.2 | 20.775 | 2,975,705.1 | 15.417 | 3,716,600.1 | 25.919 |
| 4 KiB | 393,715.7 | 114.229 | 538,018.3 | 69.206 | 864,746.9 | 46.137 | 1,139,952.6 | 39.715 | 1,527,329.5 | 58.917 |
| 16 KiB | 102,420.4 | 615.514 | 162,701.4 | 212.861 | 255,033.9 | 169.214 | 308,708.6 | 144.048 | 360,015.5 | 217.186 |
| 64 KiB | 22,758.4 | 1000.342 | 39,052.5 | 1000.342 | 61,531.5 | 807.928 | 77,038.2 | 656.933 | 77,939.9 | 702.546 |
| 256 KiB | 6,215.5 | 1000.342 | 10,853.1 | 1000.342 | 15,072.2 | 795.345 | 16,705.9 | 692.584 | 16,792.7 | 757.072 |

## Takeaways

- The native shardcache client path was the fastest target for every row. It
  uses `shardcache_client_rs` with direct SCNP shard routing.
- Shardcache shared-port SCNP was faster than Redis Cluster on ops/sec for
  every row without requiring direct client routing. It had lower p99 than
  Redis Cluster for small, 16 KiB, 64 KiB, and 256 KiB values, but higher p99
  at 1 KiB and 4 KiB.
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

The three-target 16-vCPU bundle includes:

- `metadata.json`: run settings and benchmark metadata.
- `resolved-plan.json`: shared suite selection and plan identifiers.
- `redis-cluster.csv`: Redis Cluster result rows.
- `shardcache-scnp-direct.csv`: shardcache native direct-shard client rows.
- `shardcache-resp.csv`: shardcache RESP compatibility rows.
- `summary.json` and `report.md`: generated comparison summary.

The shared-port SCNP companion bundle includes:

- `metadata.json`: run settings and benchmark metadata.
- `resolved-plan.json`: shared suite selection and plan identifiers.
- `shardcache-scnp.csv`: shardcache shared-port SCNP result rows for the
  `1,2,4,8,16` vCPU matrix.
- `summary.json` and `report.md`: generated single-target summary.

The full per-leg command plan files and generated compatibility JSON are omitted
from the checked-in reference bundle to keep the repository small. They are
regenerated by `run-benchmark-suite.sh`; the canonical compatibility table lives
in `docs/REDIS_COMPATIBILITY.md`.
