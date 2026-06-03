# Docker Server Benchmarks

This guide explains how to reproduce shardcache's Redis-compatible server
benchmarks locally. The Docker suite runs Redis, Redis Cluster, Redis Stack,
Valkey, Dragonfly, shardcache over RESP, and shardcache over SCNP as isolated
server targets, then saves portable CSV artifacts that can be compared later.
The same Compose file also includes Memcached for cache-shaped GET/SET
comparisons.

The important fairness rule is simple: every target uses the same benchmark
config, same resolved command plan, same client settings, same vCPU allocation,
same pipeline depth, and same fixture policy. Targets run one at a time so they
do not compete with each other for CPU, memory, or network resources.

## Requirements

Install these tools before running the suite:

| Requirement | Notes |
| --- | --- |
| Docker with Compose v2 | Used to run Redis, Redis Stack, Valkey, Dragonfly, Memcached, and shardcache server containers. |
| Rust toolchain | Used to build the benchmark binaries and the local shardcache image. The repo pins the supported Rust version in `Cargo.toml`. |
| Bash | The scripts work with macOS Bash 3.2 and modern Linux Bash. |
| Enough disk space | The first run pulls database images and builds the shardcache Docker image. |

Linux is the recommended environment for publishable numbers because Docker CPU
pinning is more exact. Docker Desktop works for local comparisons, but CPU
limits are best effort there.

Before starting a long run:

```bash
docker info
cargo test -p shardcache-benchmarks
```

If `docker info` fails, start Docker first. The runner checks this and exits
with a short error before building or running benchmarks.

## Quick Start

Run the standard non-destructive Redis core command suite against all server
targets across the supported vCPU matrix:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis,redis-cluster,valkey,dragonfly,shardcache-resp,shardcache-scnp \
  --suite redis-core \
  --vcpus 1,2,4,8,16 \
  --key-shards vcpus
```

The output directory is printed at the end. By default it is created under:

```text
benchmarks/results/server-suite-<timestamp>/
```

Open the generated report:

```bash
less benchmarks/results/server-suite-*/report.md
```

The raw target CSVs are also saved in that directory:

```text
redis.csv
redis-cluster.csv
valkey.csv
dragonfly.csv
shardcache-resp.csv
shardcache-scnp.csv
```

## Common Recipes

Fast smoke test against Redis only:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis \
  --suite redis-core \
  --vcpus 1 \
  --duration 1 \
  --warmup 0 \
  --memory-budget-mib 32
```

Worst-case comparison shape, matching a strict request/response 1-vCPU run:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis,redis-cluster,valkey,dragonfly,shardcache-resp,shardcache-scnp \
  --suite redis-core \
  --vcpus 1 \
  --key-shards 1 \
  --pipeline-depth 1 \
  --clients 1 \
  --warmup 2 \
  --duration 10
```

Standard strict request/response matrix:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis,redis-cluster,valkey,dragonfly,shardcache-resp,shardcache-scnp \
  --suite redis-core \
  --vcpus 1,2,4,8,16 \
  --key-shards vcpus \
  --pipeline-depth 1 \
  --clients 1 \
  --warmup 2 \
  --duration 10
```

Throughput-oriented run with pipelining:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis,redis-cluster,valkey,dragonfly,shardcache-resp,shardcache-scnp \
  --suite redis-core \
  --vcpus 1,2,4,8,16 \
  --key-shards vcpus \
  --pipeline-depth 16 \
  --clients 16 \
  --warmup 2 \
  --duration 10 \
  --memory-budget-mib 512
```

Redis Cluster versus shardcache direct routing:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis-cluster,shardcache-scnp-direct,shardcache-scnp,shardcache-resp \
  --suite redis-core \
  --vcpus 1,2,4,8,16 \
  --key-shards vcpus \
  --pipeline-depth 1,16 \
  --clients 16 \
  --warmup 2 \
  --duration 10 \
  --memory-budget-mib 512
```

`redis-cluster` starts a Redis Cluster with primary nodes mapped to
`127.0.0.1:7000-7015`. The benchmark harness uses Redis hash tags per logical
key lane and connects each worker directly to the node that owns that lane's
slot range, so the comparison is direct routing against direct routing rather
than Redis redirect handling. Redis Cluster requires at least three primaries,
so the 1 and 2 vCPU runs start extra empty primaries but assign slots across
only the active logical key lanes.

Variable value-size cluster-scaling sweep:

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

This records Redis Cluster RESP, shardcache's native direct-shard SCNP client,
shardcache shared-port SCNP without client direct routing, and shardcache's
RESP compatibility path. Use `shardcache-scnp` to measure shardcache across
multiple vCPUs when the server routes through one shared port; use
`shardcache-scnp-direct` to measure client-routed shard ownership. The
value-size suites are split by payload size on purpose. Use these isolated
suites for final claims so small payload rows do not inherit the memory and
batch behavior of larger precomposed command plans. A saved server 16-vCPU
direct/RESP result bundle plus a shared-port SCNP companion bundle live in
[`REDIS_CLUSTER_SCALABILITY_BENCHMARKS.md`](REDIS_CLUSTER_SCALABILITY_BENCHMARKS.md).

Full command coverage matrix:

```bash
SHARDCACHE_FEATURES=redis-server,redis-modules-all \
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets all \
  --suite all \
  --vcpus 1,2,4,8,16 \
  --pipeline-depth 1,16 \
  --warmup 2 \
  --duration 10 \
  --memory-budget-mib 512
```

Run module command cases only:

```bash
SHARDCACHE_FEATURES=redis-server,redis-modules-all \
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets shardcache-resp,shardcache-scnp,redis-stack,valkey,dragonfly \
  --suite redis-modules \
  --vcpus 1,2,4,8,16 \
  --pipeline-depth 1
```

The module suite is useful for compatibility coverage across targets. Use
`redis-stack` when you want Redis with the common module set loaded
(RedisBloom, RediSearch, RedisJSON, and RedisTimeSeries). Some servers do not
implement all module command families; unsupported commands are recorded as
RESP errors in the CSV instead of being silently skipped.

The `all` target shorthand includes both `redis` and `redis-stack`. That keeps
plain Redis core rows separate from Redis Stack module rows while still making
`--suite all --targets all` a complete coverage run.

For full shardcache module coverage, build the shardcache target with
`SHARDCACHE_FEATURES=redis-server,redis-modules-all`. Without that feature set,
module families hidden behind compile-time flags are expected to report
unsupported-command rows.

Run Memcached cache workload comparisons:

```bash
./benchmarks/scripts/run-memcache-comparison.sh \
  --targets all \
  --vcpus 1,2,4,8,16 \
  --clients 1,16 \
  --pipeline-depth 1,16
```

This uses the same Docker isolation pattern, but drives Memcached and
shardcache with comparable GET/SET/mixed workloads instead of Redis command
cases. See [`MEMCACHE_BENCHMARKS.md`](MEMCACHE_BENCHMARKS.md).

Run the same command suites through shardcache's embedded Redis API:

```bash
./benchmarks/scripts/run-redis-embedded-command-suite.sh \
  --suite all \
  --vcpus 1,2,4,8,16 \
  --pipeline-depth 1 \
  --memory-budget-mib 512
```

The embedded runner writes `shardcache-embedded.csv` with the same CSV schema,
so it can be merged with Docker server results when you want RESP, SCNP, and
direct embedded command rows in one report.

Write results to an explicit directory:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets shardcache-resp,shardcache-scnp,redis \
  --suite redis-v6-v7 \
  --vcpus 1,2,4,8,16 \
  --out-dir /tmp/shardcache-redis-v6-v7
```

## Comparing Saved CSVs

You can compare CSVs from the same run, different runs, or different machines.
This is useful when each database was benchmarked in isolation on the same
hardware.

```bash
./benchmarks/scripts/compare-benchmark-csvs.sh \
  benchmarks/results/server-suite-20260531T120000Z/shardcache-resp.csv \
  benchmarks/results/server-suite-20260531T120000Z/shardcache-scnp.csv \
  benchmarks/results/server-suite-20260531T120000Z/redis.csv \
  benchmarks/results/server-suite-20260531T120000Z/valkey.csv \
  benchmarks/results/server-suite-20260531T120000Z/dragonfly.csv
```

Choose a baseline target for ratio columns:

```bash
BASELINE=redis \
./benchmarks/scripts/compare-benchmark-csvs.sh \
  benchmarks/results/run-a/shardcache-resp.csv \
  benchmarks/results/run-a/shardcache-scnp.csv \
  benchmarks/results/run-a/redis.csv
```

The comparison command writes:

```text
benchmarks/results/compare-<timestamp>/report.md
benchmarks/results/compare-<timestamp>/summary.json
```

## Shared Config

The default user-facing config is `benchmarks/bench.toml`:

```toml
suites = "redis-core"
targets = "redis,redis-cluster,valkey,dragonfly,shardcache-resp,shardcache-scnp"
vcpus = "1,2,4,8,16"
clients = 1
pipeline_depths = "1"
warmup = 1
duration = 5
key_shards = "vcpus"
fixture_scope = "per-client"
memory_budget_mib = 256
command_budget = 0
out_dir = "benchmarks/results"
```

CLI flags override config values for one run. For example, this uses the
defaults from `benchmarks/bench.toml` except for `targets`, `suite`, and
`vcpus`:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets shardcache-resp,shardcache-scnp,redis \
  --suite redis-core \
  --vcpus 1,2,4,8,16
```

Use a custom config file:

```bash
./benchmarks/scripts/run-benchmark-suite.sh --config /path/to/bench.toml
```

## Config Reference

| Setting | Meaning |
| --- | --- |
| `suites` | Comma-separated suites to run. Use `all` for every suite. |
| `targets` | Comma-separated server targets. Use `all` for Redis, Redis Cluster, Redis Stack, Valkey, Dragonfly, `shardcache-resp`, and shared-port `shardcache-scnp`. Use `redis-stack` for Redis module benchmarks. The shorthand `shardcache` expands to both shardcache protocol targets. Use `redis-cluster` and `shardcache-scnp-direct` for routed direct-shard experiments. |
| `vcpus` | Comma-separated vCPU counts. Each target is run once per value. |
| `clients` | Concurrent benchmark client connections per target. |
| `pipeline_depths` | Comma-separated in-flight command counts per client. |
| `warmup` | Warmup seconds before measurements are recorded. |
| `duration` | Measurement seconds per target/suite/vCPU/pipeline combination. |
| `key_shards` | Logical key lanes used to spread generated fixtures. |
| `fixture_scope` | `per-client` or `shared-keyspace`. Keyspace suites use `shared-keyspace` by default. |
| `memory_budget_mib` | Total client-side memory limit for precomposed command bytes. |
| `command_budget` | Optional total precomposed command count. `0` means memory-bounded only. |
| `out_dir` | Output directory or parent results directory. |

## Suites

Suite manifests live under `benchmarks/suites/`.

| Suite | Purpose |
| --- | --- |
| `redis-core` | Non-destructive Redis-compatible core commands. |
| `redis-v6-v7` | Redis 6/7 extension commands, including functions and hash-field TTL. |
| `redis-v8` | Redis 8 command coverage, including hash helpers and vector-set commands. |
| `redis-v8-vector` | Focused Redis 8 vector-set subset for vector-only sweeps. |
| `redis-keyspace` | Keyspace-wide commands such as scan-style workloads. |
| `redis-destructive` | Explicit destructive commands such as flush workloads. |
| `redis-modules` | Redis module command coverage by module prefix. |
| `all` | Expands to every complete suite above, including Redis 8 and module suites. Focused subsets such as `redis-v8-vector` stay opt-in. |

Each suite resolves to command filters for `redis_command_matrix`. The resolved
plan is written to the output bundle so the exact selected cases are visible.

## Server Targets

The Docker services are defined in `benchmarks/docker/compose.yml`.

| Target | Image or build | Local port | Persistence |
| --- | --- | ---: | --- |
| `redis` | `${REDIS_IMAGE:-redis:7.4-alpine}` | 6379 | Disabled with `--save "" --appendonly no`. |
| `redis-cluster` | `${REDIS_IMAGE:-redis:7.4-alpine}` | 7000-7015 | Starts a Redis Cluster inside one isolated container. The runner creates at least 3 Redis primaries and assigns slots across the active logical key lanes, so 1/2 vCPU runs keep the same command plan while satisfying Redis Cluster's minimum node count. |
| `redis-stack` | `${REDIS_STACK_IMAGE:-redis/redis-stack-server:latest}` | 6379 | Redis Stack modules loaded by the image; AOF disabled through `REDIS_ARGS`, snapshots disabled by the runner after startup. |
| `valkey` | `valkey/valkey:8.0-alpine` | 6381 | Disabled with `--save "" --appendonly no`. |
| `dragonfly` | `docker.dragonflydb.io/dragonflydb/dragonfly:v1.27.0` | 6382 | Snapshots disabled. |
| `memcached` | `${MEMCACHED_IMAGE:-memcached:1.6-alpine}` | 11211 | Used by `run-memcache-comparison.sh`; memory defaults to `${MEMCACHED_MEMORY_MB:-1024}` MiB; override the host port with `MEMCACHED_PORT`. |
| `shardcache-resp` | Local repo Docker build | 6383 | Disabled with `--disable-persistence`; benchmarked over RESP. |
| `shardcache-scnp` | Local repo Docker build | 6383 | Disabled with `--disable-persistence`; benchmarked over SCNP on the shared server port so full command suites, including non-key commands, are covered. |
| `shardcache-scnp-direct` | Local repo Docker build | 6501-6516 | Disabled with `--disable-persistence`; benchmarked over SCNP direct shard ports for routed command subsets. |

The runner starts exactly one target service at a time. It stops the service
before moving to the next target. `redis` and `redis-stack` intentionally share
the same local benchmark port because they are isolated alternatives, not
concurrent services. `redis-cluster` uses ports `7000-7015` and is still
treated as one target for CPU and memory limiting.

`shardcache-resp` and `shardcache-scnp` use the same shardcache Docker image,
same vCPU limit, same shard count, same command cases, and same logical
`resolved_plan_id`. They differ only in the wire protocol used by the benchmark
client. The optional `shardcache-scnp-direct` target uses per-shard direct
ports and should be limited to routed command subsets.

## Hardware Controls

The runner applies the same hardware settings to every target in a run.

| Control | How to set it |
| --- | --- |
| vCPU count | `--vcpus 1,2,4,8,16` or `vcpus = "1,2,4,8,16"` |
| explicit CPU set | `CPUSET_CPUS=2-3 ./benchmarks/scripts/run-benchmark-suite.sh ...` |
| server memory limit | `SERVER_MEMORY_LIMIT=4g ./benchmarks/scripts/run-benchmark-suite.sh ...` |
| shardcache shard count | `SHARD_COUNT=4 ./benchmarks/scripts/run-benchmark-suite.sh ...` |

On Linux, the default vCPU behavior pins containers to the first N CPUs. On
Docker Desktop, CPU pinning may be unavailable; Docker CPU quota is used as a
fallback and the metadata records the run settings.

The runner creates a small Compose override for each isolated target run so CPU
and memory limits are present when the container process starts. This matters
for servers that size worker threads from cgroup limits at startup.

For publishable numbers, prefer:

- A quiet machine with other heavy workloads stopped.
- Linux CPU pinning.
- The full standard matrix: `1,2,4,8,16` vCPU.
- Multiple repeated runs.
- Separate reports for `pipeline_depth=1` latency-sensitive results and
  pipelined throughput results.
- Separate reports for keyspace-wide and destructive suites.

## Precomposed Commands

`redis_command_matrix` resolves the selected suite into a command plan before
the timed window. For each worker, it pre-encodes a reusable command pool and
cycles through that pool during measurement. This keeps command construction
CPU out of the benchmark hot path.

The pool is bounded by:

| Setting | Meaning |
| --- | --- |
| `memory_budget_mib` | Total encoded command-pool memory across all clients. |
| `command_budget` | Optional total encoded command count across all clients. |

If the memory budget is too small for one full pass through the selected
commands, the runner fails before measurement and prints the minimum required
bytes for that worker.

Every output bundle includes `resolved-plan.json` plus per-suite plan files
under `plans/`. All target CSV rows from a comparable run share the same
`resolved_plan_id`.

## Output Bundle

Each run writes a self-contained artifact bundle:

| File | Description |
| --- | --- |
| `metadata.json` | Host, git, Docker, suite, target, and hardware metadata. |
| `resolved-plan.json` | Index of resolved command plans used by the run. |
| `plans/*.json` | Concrete command plan metadata for each suite/vCPU/pipeline combination. |
| `<target>.csv` | Portable target results. |
| `report.md` | Human-readable summary and comparisons. |
| `summary.json` | Machine-readable report summary. |
| `redis-compatibility.json` | Compatibility manifest generated from benchmark coverage. |

Important CSV columns:

| Column | Meaning |
| --- | --- |
| `run_id` | Unique run identifier. |
| `resolved_plan_id` | Shared plan identifier for comparable rows. |
| `suite`, `scenario`, `category` | Benchmark grouping labels. |
| `target` | Server target name. |
| `command`, `case` | Redis command and benchmark case. |
| `clients`, `pipeline_depth`, `vcpus` | Runtime shape. |
| `ops_per_sec` | Throughput for this case. Higher is better. |
| `avg_us`, `p50_us`, `p95_us`, `p99_us`, `p999_us` | Latency in microseconds. Lower is better. |
| `errors`, `expected_errors` | RESP errors. Unexpected errors should be investigated before using a row as a performance claim. |
| `memory_budget_mib`, `command_budget`, `command_bytes` | Precomposed command-pool settings and resolved size. |

## Reading Results

Use `report.md` for a quick summary and `*.csv` for detailed analysis.

When comparing targets:

- Compare rows with the same `suite`, `category`, `case`, `clients`,
  `pipeline_depth`, `vcpus`, and `resolved_plan_id`.
- Prefer `ops_per_sec` for throughput claims.
- Prefer `p99_us` for tail-latency claims.
- Treat rows with unexpected `errors > 0` as compatibility findings first, not
  performance wins or losses.
- Do not compare module unsupported-error rows as throughput claims.

### Server Prime Sweep Reference

The 2026-06-01 server prime sweep used this runner on the benchmark server:

```bash
SHARDCACHE_FEATURES=redis-server,redis-modules-all \
RUN_ID=server-prime-new-commands-20260601T012301Z \
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis-stack,valkey,dragonfly,shardcache-resp,shardcache-scnp \
  --suite redis-v6-v7,redis-modules \
  --vcpus 1,2,4,8,16 \
  --pipeline-depth 1 \
  --clients 1 \
  --warmup 2 \
  --duration 10 \
  --memory-budget-mib 512 \
  --out-dir benchmarks/results
```

The run completed 50 isolated target/suite/vCPU legs with no runner-level
errors and wrote `benchmarks/results/server-prime-new-commands-20260601T012301Z/`.
Shardcache RESP and shardcache SCNP completed every Redis v6/v7 and module row
with 0 unexpected errors. Redis 8 vector rows were run separately with the
`redis-v8-vector` suite; see
[`REDIS_V8_VECTOR_BENCHMARKS.md`](REDIS_V8_VECTOR_BENCHMARKS.md). Redis module
performance rows should use `redis-stack`; Valkey and Dragonfly module rows
remain compatibility/error coverage unless those deployments are replaced with
module-enabled images.

## Troubleshooting

Docker is not reachable:

```text
Docker daemon is not reachable. Start Docker, then rerun the benchmark suite.
```

Start Docker Desktop or the Docker daemon, then retry.

Port already in use:

```bash
docker compose -f benchmarks/docker/compose.yml --profile servers down --remove-orphans
lsof -iTCP:6379 -sTCP:LISTEN
```

Memory budget too small:

```text
memory budget is too small for one command pass
```

Raise `--memory-budget-mib` or run fewer suites/cases.

Shardcache image build is slow:

The first shardcache target run builds a local Docker image from the repo. Later
runs reuse Docker layers unless source files changed.

Dragonfly thread count:

The runner sets Dragonfly's `--proactor_threads` to the current `--vcpus` value
for that isolated run.

Unsupported module commands:

Module cases intentionally exercise broad command coverage. Unsupported module
commands are recorded as errors. Redis Stack provides the common Redis module
families, but module families outside that image can still report unsupported
rows. Use unsupported rows for compatibility coverage, not throughput claims.

Redis Stack startup fails:

The runner starts `redis/redis-stack-server` with `REDIS_ARGS` and then runs
`CONFIG SET save ""` inside the container before benchmarking. This keeps the
Redis Stack module image intact while disabling background snapshots for fair
ephemeral runs.
