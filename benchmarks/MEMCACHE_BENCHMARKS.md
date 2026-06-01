# Memcached Comparison Benchmarks

This suite compares shardcache server modes against Memcached on the same
cache-shaped workload: `GET`, `SET`, and mixed read/write traffic over a fixed
keyspace. It is intentionally separate from `redis_command_matrix` because
Memcached does not implement Redis commands; the fair comparison is a shared
key/value workload with the same value sizes, key count, clients, pipeline
depths, and vCPU limits.

## Quick Start

Run a short strict request/response smoke test:

```bash
./benchmarks/scripts/run-memcache-comparison.sh \
  --targets memcached,shardcache-resp,shardcache-scnp-direct \
  --vcpus 1 \
  --clients 1 \
  --pipeline-depth 1 \
  --value-size 64 \
  --mix 80-20 \
  --warmup 1 \
  --duration 3
```

Run the standard matrix:

```bash
./benchmarks/scripts/run-memcache-comparison.sh \
  --targets all \
  --vcpus 1,2,4,8,16 \
  --clients 1,16 \
  --pipeline-depth 1,16 \
  --value-size 64,512,4096 \
  --mix 100-0,80-20,0-100 \
  --warmup 2 \
  --duration 10
```

The output bundle is written to:

```text
benchmarks/results/memcache-comparison-<timestamp>/
```

Important files:

| File | Description |
| --- | --- |
| `metadata.json` | Run configuration, git SHA, target list, vCPU matrix, and Docker settings. |
| `cache-comparison.csv` | One row per target/workload/hardware shape. |
| `report.md` | Human-readable run summary and CSV location. |

## Targets

The script starts one Docker service at a time from
`benchmarks/docker/compose.yml`.

| Target | Protocol | Local port | Notes |
| --- | --- | ---: | --- |
| `memcached` | Memcached text protocol | 11211 | Uses `memcached:1.6-alpine`; persistence is not applicable. |
| `shardcache-resp` | RESP | 6383 | Local shardcache Docker build with persistence disabled. |
| `shardcache-scnp` | SCNP shared port | 6383 | Same server, native protocol on the shared listener. |
| `shardcache-scnp-direct` | SCNP direct shard ports | 6501-6516 | Routes keys directly to their owning shard. |
| `all` | Expands to every target above | | |

## Hardware Controls

Every target gets the same vCPU limit for comparable rows. On Linux, the script
pins each container to the first N CPUs by default. Docker Desktop applies CPU
quota best-effort.

Common controls:

```bash
SERVER_MEMORY_LIMIT=4g \
MEMCACHED_MEMORY_MB=4096 \
./benchmarks/scripts/run-memcache-comparison.sh --vcpus 1,2,4,8,16
```

| Variable | Meaning |
| --- | --- |
| `SERVER_MEMORY_LIMIT` | Optional Docker memory limit applied to every target. |
| `CPUSET_CPUS` | Explicit Docker cpuset, overriding the first-N-CPUs default. |
| `MEMCACHED_MEMORY_MB` | Memcached item memory in MiB. Default: `1024`. |
| `MEMCACHED_PORT` | Host port for Memcached. Default: `11211`. |
| `MEMCACHED_MAX_ITEM_SIZE` | Memcached `-I` max item size. Default: `64m`. |
| `SHARDCACHE_FEATURES` | Docker build features for shardcache. Default: `redis-server`. |
| `SHARDCACHE_PORT` | Host port for shardcache shared RESP/SCNP listener. Default: `6383`. |
| `SHARD_COUNT` | Override shardcache shard count. Defaults to current vCPU count. |

## CSV Columns

The CSV is produced by the shared `saturation` driver. Key columns include:

| Column | Meaning |
| --- | --- |
| `backend` | `memcached`, `fc-server-resp`, `fc-server-scnp`, or `fc-server-scnp-direct`. |
| `value_size` | Bytes per value. |
| `mix` | Read/write mix, such as `100-0`, `80-20`, or `0-100`. |
| `vcpu_budget` | Server vCPU allocation. |
| `clients` | Concurrent client connections. |
| `pipeline_depth` | In-flight operations per connection. |
| `ops_per_sec` | Throughput. Higher is better. |
| `p50_ns`, `p99_ns`, `p999_ns` | Latency percentiles. Lower is better. |
| `errors` | Operation errors. Investigate non-zero rows before using them in claims. |

Compare only rows with the same `value_size`, `mix`, `vcpu_budget`, `clients`,
`pipeline_depth`, `key_pattern`, and `key_distribution`.

## Notes

- This benchmark uses the Memcached text protocol because it is universally
  available in the official image and supports request pipelining.
- Memcached TTL is supported by the backend for TTL saturation runs, but the
  default comparison matrix uses non-expiring keys to match the primary cache
  throughput path.
- The shardcache direct-SCNP target is useful for measuring routed key/value
  traffic without the shared listener's fanout/routing overhead.
