# Embedded Cache Benchmarks

This guide explains how to compare shardcache's embedded cache paths against
other in-process Rust cache baselines such as DashMap, Moka, LRU, and a
`RwLock<HashMap>` baseline.

Use this suite when you want to measure library-style in-process cache
performance without TCP, RESP, Docker, or external database server overhead. For
Redis-compatible server benchmarking, use `DOCKER_BENCHMARKS.md` instead.
For Redis command-by-command benchmarks in embedded mode, use
`REDIS_EMBEDDED_COMMAND_BENCHMARKS.md`.

## What Is Compared

The embedded runner uses the existing `saturation` benchmark driver. For each
suite it generates the same workload for every backend, then records throughput,
logical payload bandwidth, CPU use, and p50/p99/p99.9 latency.

The default standard matrix is:

```text
vCPU:        1,2,4,8,16
clients:     match-vcpus
value sizes: 64,512,4096,65536,1048576
mixes:       100-0,0-100,80-20
```

`clients = "match-vcpus"` means a 4-vCPU row uses 4 client threads, an 8-vCPU
row uses 8 client threads, and so on.

## Quick Start

Run the default embedded core suite:

```bash
./benchmarks/scripts/run-embedded-benchmark-suite.sh \
  --suite embedded-core \
  --vcpus 1,2,4,8,16
```

Run all embedded suites:

```bash
./benchmarks/scripts/run-embedded-benchmark-suite.sh \
  --suite all \
  --vcpus 1,2,4,8,16 \
  --duration 10 \
  --warmup 2
```

Fast smoke test:

```bash
./benchmarks/scripts/run-embedded-benchmark-suite.sh \
  --suite embedded-core \
  --backends fc-embed,dashmap \
  --vcpus 1 \
  --clients 1 \
  --value-sizes 64 \
  --mixes 80-20 \
  --duration 1 \
  --warmup 0 \
  --out-dir /tmp/shardcache-embedded-smoke
```

## Suites

Suite manifests live under `benchmarks/embedded-suites/`.

| Suite | Purpose |
| --- | --- |
| `embedded-core` | Main in-process cache comparison: shardcache embedded paths, DashMap, Moka, LRU, and `RwLock<HashMap>`. |
| `embedded-ttl` | TTL-capable embedded backends with active TTL writes. |
| `embedded-lru` | Capacity-bounded comparisons with LRU-style pressure. |
| `embedded-copy` | Copy-out read path comparison for backends that materialize GET values. |
| `all` | Expands to every suite above. |

The default `embedded-core` backend list is:

```text
fc-embed,
fc-shared,
dashmap,
dashmap-worker-shards,
dashmap-ref,
moka,
rwlock-hashmap,
lru
```

Override the backend list for one run:

```bash
./benchmarks/scripts/run-embedded-benchmark-suite.sh \
  --suite embedded-core \
  --backends fc-embed,fc-shared,dashmap,moka,rwlock-hashmap \
  --vcpus 1,2,4,8,16
```

## Shared Config

The default embedded config is `benchmarks/embedded.toml`:

```toml
suites = "embedded-core"
vcpus = "1,2,4,8,16"
clients = "match-vcpus"
value_sizes = "64,512,4096,65536,1048576"
mixes = "100-0,0-100,80-20"
warmup = 2
duration = 10
key_memory_cap_bytes = 4000000000
large_value_key_floor = 64
latency_sample_rate = 1
key_pattern = "point"
key_distribution = "uniform"
out_dir = "benchmarks/results"
```

CLI flags override config values for one run. Use a custom config with:

```bash
./benchmarks/scripts/run-embedded-benchmark-suite.sh --config /path/to/embedded.toml
```

## Config Reference

| Setting | Meaning |
| --- | --- |
| `suites` | Comma-separated embedded suites to run. Use `all` for every embedded suite. |
| `vcpus` | Comma-separated vCPU budgets. Each suite is run once per value. |
| `clients` | Client threads per row, or `match-vcpus`. |
| `value_sizes` | Comma-separated value sizes in bytes. |
| `mixes` | Operation mixes: `100-0`, `0-100`, `80-20`, or any `<get_pct>-<set_pct>`. |
| `warmup` | Warmup seconds before measurements are recorded. |
| `duration` | Measurement seconds per backend/value/mix/vCPU row. |
| `key_memory_cap_bytes` | Fixture cardinality budget. Larger values create fewer keys. |
| `large_value_key_floor` | Minimum key count for large-value runs. |
| `latency_sample_rate` | Record one latency sample every N measured operations; `0` disables latency sampling. |
| `key_pattern` | `point` or `session`. |
| `key_distribution` | `uniform`, `zipf[:theta]`, or `hot:<keys>[:pct]`. |
| `out_dir` | Output directory or parent results directory. |

## Hardware Controls

On Linux, the runner pins the benchmark process to the first N CPUs for an
N-vCPU row. Set `CPUSET_CPUS=2-5` to choose an explicit CPU set. On macOS or
when `taskset` is unavailable, the benchmark still runs but CPU pinning is not
enforced.

For publishable embedded numbers:

- Run on a quiet machine.
- Prefer Linux CPU pinning.
- Use the full `1,2,4,8,16` vCPU matrix.
- Keep `clients=match-vcpus` unless testing client scaling explicitly.
- Separate TTL, LRU/capacity, copy-out, and ordinary in-memory comparisons.
- Repeat important rows and compare medians.

## Output Bundle

Each run writes:

| File | Description |
| --- | --- |
| `metadata.json` | Host, git, suite, workload, and hardware metadata. |
| `report.md` | Small markdown rollup of mean ops/sec by suite/backend/value/mix/vCPU/client. |
| `<suite>.csv` | Raw saturation rows for that embedded suite. |

Important CSV columns:

| Column | Meaning |
| --- | --- |
| `backend` | Backend id, such as `fc-embed`, `dashmap`, or `moka`. |
| `build_variant` | `safe`, `unsafe`, or `competitor`. |
| `ttl_mode` | `none` or `active`. |
| `routing_mode` | Embedded/direct/shared classification. |
| `eviction_policy` | `none` or `lru`. |
| `read_mode` | `ref` or `copy`. |
| `value_size`, `mix`, `vcpu_budget`, `clients` | Workload shape. |
| `ops_per_sec` | Throughput. Higher is better. |
| `logical_payload_gb_per_sec` | Logical value bytes moved per second. |
| `vcpu_consumed` | Process CPU consumed during the row. |
| `p50_ns`, `p99_ns`, `p999_ns` | Latency percentiles in nanoseconds. |
| `read_p99_ns`, `write_p99_ns` | Per-operation tail latency. |
| `errors` | Backend operation errors. Investigate any nonzero value before using a row. |

## Reading Results

Use `report.md` for a quick glance and the suite CSVs for analysis.

For fair comparisons, compare rows with the same:

- suite
- backend build variant
- value size
- mix
- vCPU budget
- client count
- TTL mode
- eviction policy
- key pattern and distribution
- read mode

`fc-embed` is the owner-local embedded path. `fc-shared` is the shared embedded
handle path. DashMap, Moka, LRU, and `rwlock-hashmap` are competitor or baseline
in-process maps.

## Troubleshooting

Backend skipped:

Some suites intentionally exclude unsupported backends. If a backend is present
but does not support a requested mode, such as TTL or pipelining, the saturation
driver prints a skip message and continues.

Runs take too long:

Reduce the matrix:

```bash
./benchmarks/scripts/run-embedded-benchmark-suite.sh \
  --suite embedded-core \
  --vcpus 1,4,16 \
  --value-sizes 64,4096 \
  --mixes 80-20 \
  --duration 3
```

Latency sampling overhead:

Use `--latency-sample-rate 0` for raw throughput sweeps, or a larger sample rate
such as `--latency-sample-rate 100` when p99 precision is less important.
