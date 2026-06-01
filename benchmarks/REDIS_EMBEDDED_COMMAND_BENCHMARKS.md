# Embedded Redis Command Benchmarks

The embedded Redis command suite runs every Redis-compatible command case
directly against shardcache's in-process Redis API. It uses the same suite
manifests as the Docker server runner, so `redis-core`, `redis-v6-v7`,
`redis-keyspace`, `redis-destructive`, and `redis-modules` resolve to the same
command cases.

## Quick Start

Run the default Redis core suite across the standard CPU matrix:

```bash
./benchmarks/scripts/run-redis-embedded-command-suite.sh \
  --suite redis-core \
  --vcpus 1,2,4,8,16
```

Run every Redis and module command case:

```bash
./benchmarks/scripts/run-redis-embedded-command-suite.sh \
  --suite all \
  --vcpus 1,2,4,8,16 \
  --memory-budget-mib 512
```

The runner builds `redis_embedded_command_matrix` with the
`redis-embedded-commands` benchmark feature. That enables Redis functions and
all feature-gated module command families in the benchmark binary.

## What It Measures

The benchmark bypasses TCP, RESP parsing, SCNP framing, and socket scheduling.
Each operation is executed through `shardmap::redis_embedded::EmbeddedRedis`,
using prepared embedded command objects created before the timed window.

This is the right mode for answering:

- how fast the Redis command implementation is in embedded mode
- whether every supported Redis command is reachable through a first-party
  embedded API
- how embedded command behavior compares with the same command cases over
  shardcache RESP or SCNP

It is not a network benchmark. Use
`./benchmarks/scripts/run-benchmark-suite.sh` for Redis, Valkey, Dragonfly, and
shardcache server comparisons.

## Shared Inputs

The embedded command runner reads the same defaults from `benchmarks/bench.toml`
as the Docker server runner:

- suites
- vCPU matrix
- clients
- pipeline depths
- warmup and duration
- key lanes
- fixture scope
- command memory budget
- command count budget
- output directory

Suite manifests live in `benchmarks/suites/`. For example,
`redis-v6-v7.toml` and `redis-modules.toml` are shared by server and embedded
command runs.

## Output Bundle

Each run writes a portable result bundle:

- `metadata.json`
- `resolved-plan.json`
- `plans/*.json`
- `shardcache-embedded.csv`
- `summary.json`
- `report.md`
- `redis-compatibility.json`

CSV rows use the same schema as `redis_command_matrix`, including
`suite`, `category`, `target`, `command`, `case`, `vcpus`, `pipeline_depth`,
`ops_per_sec`, average latency, p50/p95/p99/p999 latency, error counts,
memory budget, command budget, and resolved plan id.

## Comparing With Server Runs

Run the server suite:

```bash
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis,redis-stack,valkey,dragonfly,shardcache-resp,shardcache-scnp \
  --suite redis-core \
  --vcpus 1,2,4,8,16
```

When comparing embedded module commands, include `redis-stack` and run the
`redis-modules` suite so the Redis reference has modules loaded.

Run the embedded command suite with the same suite and CPU matrix:

```bash
./benchmarks/scripts/run-redis-embedded-command-suite.sh \
  --suite redis-core \
  --vcpus 1,2,4,8,16
```

Then merge the saved CSVs without rerunning benchmarks:

```bash
OUT_DIR=benchmarks/results/combined-redis-command-report \
./benchmarks/scripts/compare-benchmark-csvs.sh \
  benchmarks/results/server-suite-*/redis.csv \
  benchmarks/results/server-suite-*/shardcache-resp.csv \
  benchmarks/results/server-suite-*/shardcache-scnp.csv \
  benchmarks/results/redis-embedded-command-suite-*/shardcache-embedded.csv
```

For exact reproducibility, compare rows with the same `resolved_plan_id`,
suite, vCPU count, client count, pipeline depth, and memory budget.

## Tuning

Use `--memory-budget-mib` to bound prepared command memory. The driver prepares
at least one full pass of selected cases per client and repeats that pass until
the memory or command-count budget is reached.

Use `--command-budget` when you want a fixed prepared operation count instead
of purely memory-bounded preparation.

The runner builds the embedded command binary with
`--features redis-embedded-commands`, which enables Redis functions and
`redis-modules-all` for benchmark coverage. Normal library users can still opt
into only the module feature families they need.

Use `--store-shards` to pin the embedded store shard count. By default it
matches each vCPU entry, so `--vcpus 1,2,4,8,16` runs stores with
`1,2,4,8,16` shards unless `SHARD_COUNT` or `--store-shards` overrides it.

Use `--key-shards` to control generated fixture key lanes. For most embedded
command runs, leave it at `1`; use higher powers of two when validating
cross-shard behavior.
