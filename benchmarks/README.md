# fast-cache benchmarks

Head-to-head benchmarks for fast-cache against embedded Rust caches and
networked databases. Not part of the published crate.

## What is measured

Two modes, parallel and independent:

| Mode | Driver | Question |
| --- | --- | --- |
| `saturation` | Closed-loop, push as hard as possible | Peak ops/sec, logical payload GB/s, CPU and p99 at peak |
| `curve` | Open-loop, target rate sweep | How CPU and p99 scale with load up to saturation |
| `redis_command_matrix` | RESP command script, per command | Head-to-head command throughput for fast-cache vs Redis/Valkey |

Both drivers share the same backend list, the same workload axes, and
the same CSV schema. Python harnesses for `fc-py` and `fc-lmcache`
emit rows in the same schema.

## Redis Command Matrix

`redis_command_matrix` runs a deterministic RESP command script and reports
per-command throughput and average request latency. It is intentionally not a
Criterion benchmark: it talks to real TCP servers so the same command cases can
run head-to-head against fast-cache, Redis, and Valkey.

```bash
./benchmarks/scripts/run-redis-command-matrix.sh
```

The default script starts Redis and Valkey from
`benchmarks/docker/compose.yml`, starts `fast-cache-server` with the
`redis-server` feature, and writes
`benchmarks/results/redis-command-matrix.csv`. Use `CASES=hash,zset` or
`CASES=HSET,ZRANGE` for focused runs. `CASES=extended` runs the full repeatable
matrix, including larger values, 4K-key keyspace walks, and 1K-element
hash/list/set/zset objects. For concurrency scaling, prefer
`CASES=extended-no-keyspace` and run the expensive keyspace-wide cases
separately with `CASES=profile:keyspace`; otherwise `KEYS`/global `SCAN` cases
can dominate the mixed loop and hide point-command scaling. Set
`FIXTURE_SCOPE=shared-keyspace` for concurrent keyspace runs so increasing
`CLIENTS` does not also multiply the seeded keyspace size. `SKIP_CASES` accepts
the same command, family, case, and profile filters as `CASES`. `CLIENTS`,
`WARMUP`, and `DURATION` scale the run. Set `KEY_SHARDS` to split per-client
fixtures across logical key lanes, normally matching fast-cache's `SHARD_COUNT`
for parallel shard fanout runs. Set `PIPELINE_DEPTH` to keep multiple adjacent
case operations in flight on each socket while preserving the global case order;
this is useful for separating strict request/response latency from socket-fed
throughput. Set `FAIL_ON_ERROR=1` when the matrix should fail on any RESP error
reply instead of recording the error count in the output.
For fast-cache direct shard ports, use `host:base_port+shards` in `TARGETS`
and set `KEY_SHARDS` to the same shard count. When the script starts
fast-cache, also set `SERVER_DIRECT_SHARD_PORTS=1` and optionally
`FAST_CACHE_DIRECT_SHARD_BASE_PORT`; for example
`SERVER_DIRECT_SHARD_PORTS=1 FAST_CACHE_DIRECT_SHARD_BASE_PORT=6384
TARGETS=fast-cache-sharded=fcnp:127.0.0.1:6384+4 KEY_SHARDS=4` routes each
worker to the shard-owned FCNP port for its generated key lane.

Destructive keyspace-wide commands such as `FLUSHDB` and `FLUSHALL` are in an
explicit profile so they are present in the perf matrix without corrupting the
ordinary mixed command loop:

```bash
CASES=profile:destructive ./benchmarks/scripts/run-redis-command-matrix.sh
```

Set `DOCKER_SERVICES="redis valkey dragonfly"` and include
`dragonfly=127.0.0.1:6382` in `TARGETS` when running Dragonfly in the same
matrix.

For proof and release work, prefer the bundle wrapper:

```bash
CASES=extended-no-keyspace \
CLIENTS=16 \
KEY_SHARDS=4 \
FIXTURE_SCOPE=shared-keyspace \
WARMUP=2 \
DURATION=10 \
./benchmarks/scripts/run-redis-command-benchmark-bundle.sh
```

It writes an ignored artifact directory under `benchmarks/results/` containing
run metadata, the raw CSV, a Markdown report, a JSON summary, and the Redis
compatibility manifest captured at the same git SHA. Use
`docs/PROOF_GATES.md` for the exact artifact contract.

### Current Adam Command Snapshot

The latest no-keyspace Redis-command matrix was run on `adam` on 2026-05-24
with 16 clients, 16 key shards, `SHARD_COUNT=16`, shared keyspace fixtures,
and direct shard ports enabled at `127.0.0.1:6384+16`. The fast-cache rows use
the pass-2 optimized artifacts below; Redis/Valkey/Dragonfly rows are saved
reference rows from the same Adam setup so fast-cache can be rerun without
rerunning external services. Transaction cases are skipped for direct shard
ports because transactions are connection-scoped and intentionally unsupported
on shard-owned listeners.

Depth 1, strict request/response:

| Target | Cases | Sum ops/sec | Mean avg us | Errors |
| --- | ---: | ---: | ---: | ---: |
| fast-cache FCNP direct | 209 | 460,886 | 34.6 | 0 |
| fast-cache FCNP shared | 209 | 460,459 | 34.6 | 0 |
| fast-cache RESP | 209 | 381,477 | 41.8 | 0 |
| Redis | 209 | 29,251 | 546.5 | 2,818 |
| Valkey | 209 | 31,198 | 512.3 | 3,004 |
| Dragonfly | 209 | 6,506 | 2,459.9 | 3,133 |

Depth 16, ordered pipelining:

| Target | Cases | Sum ops/sec | Mean avg us | Errors |
| --- | ---: | ---: | ---: | ---: |
| fast-cache FCNP direct | 209 | 1,324,917 | 88.7 | 0 |
| fast-cache FCNP shared | 209 | 1,328,698 | 88.4 | 0 |
| fast-cache RESP | 209 | 841,790 | 116.9 | 0 |
| Redis | 209 | 37,483 | 5,216.9 | 3,604 |
| Valkey | 209 | 46,885 | 4,184.5 | 4,492 |
| Dragonfly | 209 | 17,500 | 12,175.6 | 8,396 |

Zero-error common-case summaries are often more useful for implementation
comparisons. Excluding commands that produced reference-service errors, the
depth-1 Redis/Valkey common subset has 207 cases: FCNP direct `456,474`
ops/sec at `34.6 us`, FCNP shared `456,051` at `34.6 us`, RESP `377,825` at
`41.9 us`, Redis `28,969` at `541.9 us`, and Valkey `30,898` at `507.7 us`.
The ordered depth-16 common subset has 207 cases: FCNP direct `1,312,237`
ops/sec at `88.5 us`, FCNP shared `1,315,982` at `88.2 us`, RESP `833,734`
at `117.1 us`, Redis `37,123` at `5,150.4 us`, and Valkey `46,436` at
`4,125.1 us`.

Scripting command spot-check, rerun on `adam` after adding `EVAL`, `EVALSHA`,
and `SCRIPT` support:

| Mode | Target | Cases | Ops/sec | Mean avg us | Errors |
| --- | --- | ---: | ---: | ---: | ---: |
| C1/P1 | fast-cache RESP | 3 | 14,727 | 22.5 | 0 |
| C1/P1 | Redis | 3 | 6,312 | 52.7 | 0 |
| C1/P1 | Valkey | 3 | 6,058 | 54.9 | 0 |
| C1/P1 | Dragonfly | 3 | 5,725 | 58.1 | 0 |
| C16/P16 | fast-cache RESP | 3 | 184,328 | 76.7 | 0 |
| C16/P16 | Redis | 3 | 42,200 | 363.1 | 0 |
| C16/P16 | Valkey | 3 | 41,645 | 368.0 | 0 |
| C16/P16 | Dragonfly | 3 | 85,832 | 169.0 | 0 |

For fast-cache optimization loops, save the Redis/Valkey/Dragonfly side once
and reuse those CSVs while only rerunning fast-cache. First capture the external
reference services:

```bash
OUT_DIR=benchmarks/results/redis-command-reference-$(date -u +%Y%m%dT%H%M%SZ) \
START_FAST_CACHE=0 \
TARGETS=redis=127.0.0.1:6379,valkey=127.0.0.1:6381,dragonfly=127.0.0.1:6382 \
DOCKER_SERVICES="redis valkey dragonfly" \
CASES=extended-no-keyspace \
CLIENTS=16 \
FIXTURE_SCOPE=shared-keyspace \
WARMUP=2 \
DURATION=10 \
BASELINE=redis \
./benchmarks/scripts/run-redis-command-benchmark-bundle.sh
```

Then rerun fast-cache only and merge the saved reference CSV into the report:

```bash
OUT_DIR=benchmarks/results/redis-command-fast-cache-$(date -u +%Y%m%dT%H%M%SZ) \
TARGETS=fast-cache=127.0.0.1:6383 \
DOCKER=0 \
REFERENCE_CSVS=benchmarks/results/redis-command-reference-YYYYMMDDTHHMMSSZ/redis-command-matrix.csv \
CASES=extended-no-keyspace \
CLIENTS=16 \
FIXTURE_SCOPE=shared-keyspace \
WARMUP=2 \
DURATION=10 \
BASELINE=redis \
./benchmarks/scripts/run-redis-command-benchmark-bundle.sh
```

Reference CSVs should use the same `CASES`, `CLIENTS`, `FIXTURE_SCOPE`,
`WARMUP`, `DURATION`, host, and CPU pinning knobs as the fast-cache run. The
report still shows the full target rollup, and its common-case comparison table
only compares command cases present in both the current run and the saved
references. `REFERENCE_CSVS` accepts a comma-separated list when Redis/Valkey
and Dragonfly are stored in separate files. Set `BASELINE=redis` for the usual
fast-cache-over-Redis speedup table, or `BASELINE=fast-cache` when you want the
external services normalized against the current fast-cache run.

The tracked Redis compatibility manifest is generated from the same command
registry:

```bash
cargo run -p fast-cache-benchmarks --bin redis_command_manifest -- \
  --output docs/REDIS_COMPATIBILITY.md
```

## Native CPU Builds

For local ceiling runs and perf profiles, build benchmark binaries with the
repo-level Cargo aliases:

```bash
cargo native-bench --bin saturation
cargo native-bench --bin curve
```

The aliases add `-C target-cpu=native`, which lets LLVM use host-specific CPU
instructions. Use these for same-machine benchmark investigations only; keep
ordinary release builds for portable artifacts and cross-host comparisons.

## Replication Cost

`replication_cost` measures the primary write-path overhead of native
replication and compares immediate one-record sends with batched sends:

```bash
cargo run --release -p fast-cache-benchmarks --bin replication_cost -- \
  --value-sizes 64,512,4096,16384 \
  --mixes set,80-20 \
  --value-pattern semi-random \
  --clients 16 --shards 16 --duration 10
```

The output reports ops/sec regression inputs alongside emitted mutations,
batch count, raw MiB/s, wire MiB/s, compression ratio, and replica apply rate.
`queue hi` is the highest observed shard export lane depth, not a single global
queue depth.
`semi-random` is the default value pattern and builds a bounded pool of
deterministic, mostly-random values so zstd results are not dominated by the
same repeated payload in every SET. Use `--value-pattern repeat` only as a
compression best-case control. The pool size is bounded by
`--value-pool-count` and `--value-pool-max-bytes`. The default mode list follows
the production default and benchmarks uncompressed replication. Add
`--modes baseline,batch-none,batch-zstd` when you specifically want to measure
the compression tradeoff.

The current pinned Linux matrix comparing primary throughput with and
without native replication is published in
[fast-cache Native Replication Cost](FAST_CACHE_REPLICATION_COST.md).

## Embedded Release Matrix

The crates.io release embedded matrix is orchestrated by
`scripts/run-embedded-release-matrix.sh`. Run it on Linux. The script
builds separate native safe and unsafe benchmark binaries, records
`build_variant`, `ttl_mode`, `routing_mode`, `read_mode`, eviction policy,
entry capacity, and memory capacity in the `saturation` CSV, and defaults to
the LRU gate:

```bash
PHASE=lru ./scripts/run-embedded-release-matrix.sh
```

Review the LRU CSV before running the full matrix:

```bash
PHASE=full ./scripts/run-embedded-release-matrix.sh
```

`PHASE=all` intentionally stops after LRU unless `CONTINUE_AFTER_LRU=1` is set.
Use environment variables such as `REPEATS`, `DURATION`, `VALUE_SIZES`, and
`LRU_CPU_CLIENT_PAIRS` for smoke runs. The release run should keep the defaults:
three repeats, `10s` duration, no latency sampling for throughput rows, and
`TTL_MS=60000` for active-TTL rows.

`replication_tcp_cost` exercises the native FCRP TCP transport specifically.
Build with `--features monoio`, run once normally, then run again with
`FAST_CACHE_REPLICATION_USE_MONOIO=1`:

```bash
cargo run --release -p fast-cache-benchmarks --features monoio \
  --bin replication_tcp_cost -- \
  --value-sizes 64,4096 --mixes set,80-20 \
  --modes immediate-none,batch-none \
  --clients 16 --shards 16 --duration 10
```

`wal_tcp_export_cost` measures the production persistence runtime with disk WAL
append plus live TCP export. Build with `--features monoio`, run once normally,
then run again with `FAST_CACHE_WAL_TCP_USE_MONOIO=1`:

```bash
cargo run --release -p fast-cache-benchmarks --features monoio \
  --bin wal_tcp_export_cost -- \
  --value-sizes 64,4096 \
  --clients 16 --shards 16 --duration 10
```

## Memory Write Cost

`memory_write_cost` isolates value materialization and memory write strategies
from cache lookup and eviction policy work. It is useful when investigating
large-value SET throughput on Linux:

```bash
CPUSET=0 VALUE_SIZES=4096,16384,65536,1048576 \
  ./scripts/run-memory-write-bench.sh
```

The benchmark compares reusable slice copies, fresh `Bytes` allocation,
`Vec -> Bytes`, pooled `Bytes::try_into_mut` reuse, aligned destination copies,
and x86 non-temporal SSE2/AVX2 stores when available. Current server results show
that manual non-temporal stores are slower than normal cached copies for these
cache workloads, while reusable buffers remove most of the fresh-allocation
cost. The current server note is published in
[fast-cache Memory Write Cost](FAST_CACHE_MEMORY_WRITE_COST.md). Use the CSV
artifact from this bench before changing the storage value write path.

## Backends

| id | What it is |
| --- | --- |
| `fc-embed` | `fast_cache::storage::LocalEmbeddedStore` in-process Rust, one local store per worker |
| `fc-embed-unsafe` | Same, with `--features unsafe` on the bench crate |
| `fc-server-resp` | `fast-cache-server` over RESP/TCP |
| `fc-server-fcnp` | `fast-cache-server` over native binary (FCNP v2) |
| `fc-server-fcnp-direct` | `fast-cache-server` over FCNP with client-side routing to shard-owned ports |
| `fc-py` | `fast_cache.Store` from Python (separate harness) |
| `fc-lmcache` | LMCache with fast_cache storage plugin (separate harness) |
| `dashmap` | `dashmap::DashMap` |
| `moka` | `moka::sync::Cache` |
| `lru` | `parking_lot::Mutex<lru::LruCache>` |
| `rwlock-hashmap` | `parking_lot::RwLock<HashMap>` |
| `redis` | Redis OSS 7.x via Docker |
| `valkey` | Valkey 8.x via Docker |
| `dragonfly` | Dragonfly via Docker |

## TCP Database Comparison Snapshot

The repository TCP database comparison should show both a same-core view and a
scaled-out view for Redis compatibility, plus the current large matrix against
Redis, Valkey, and Dragonfly. The current Linux baseline uses:

- Host: Linux.
- Workload: uniform key distribution, `100k` keys.
- Driver: `saturation`, `64` clients, `10s` duration, `2s` warmup.
- Pipeline depth: `1`, which is strict request/response with no pipelining.
- Latency sampling: `--latency-sample-rate 0` for raw throughput.

Cells are `ops/sec @ measured server vCPU`.

For the fuller TCP report, including the standalone pipeline sweep and the
2026-05-18 Redis/Valkey/Dragonfly matrix, see
[fast-cache vs Redis, Valkey, and Dragonfly over TCP](FAST_CACHE_VS_REDIS_TCP.md).

For an embedded Rust cache comparison, see
[fast-cache vs Moka Embedded](FAST_CACHE_VS_MOKA_EMBEDDED.md).

### Same-Core Head-To-Head

This view pins fast-cache to one CPU with `taskset -c 0` and starts it with
`--shard-count 1`. Redis is single-threaded and reports about one measured
server vCPU in the same harness.

| Value | Mix | FCNP direct | RESP | Redis |
| ---: | --- | ---: | ---: | ---: |
| 64B | GET | 105,970 @ 1.000 | 98,817 @ 0.999 | 93,631 @ 0.999 |
| 64B | SET | 107,447 @ 0.999 | 97,776 @ 0.999 | 90,881 @ 0.999 |
| 64B | 80/20 | 106,072 @ 1.000 | 99,438 @ 1.000 | 94,015 @ 0.999 |
| 512B | GET | 102,794 @ 0.999 | 95,820 @ 1.000 | 89,154 @ 0.999 |
| 512B | SET | 104,608 @ 0.999 | 95,446 @ 0.999 | 88,918 @ 1.000 |
| 512B | 80/20 | 104,270 @ 1.000 | 96,492 @ 0.999 | 88,737 @ 1.000 |
| 4KiB | GET | 85,572 @ 0.999 | 79,082 @ 0.999 | 75,676 @ 1.000 |
| 4KiB | SET | 93,055 @ 0.999 | 83,551 @ 0.999 | 81,613 @ 0.999 |
| 4KiB | 80/20 | 87,277 @ 1.000 | 81,001 @ 1.000 | 77,005 @ 0.999 |
| 16KiB | GET | 72,560 @ 1.000 | 61,492 @ 1.000 | 58,923 @ 0.999 |
| 16KiB | SET | 81,094 @ 0.999 | 75,104 @ 0.999 | 68,474 @ 1.000 |
| 16KiB | 80/20 | 75,311 @ 0.999 | 64,126 @ 0.999 | 60,532 @ 1.000 |

### 16-vCPU Saturation

This view pins fast-cache to CPUs `0-15` with `taskset -c 0-15` and starts it
with `--shard-count 16`. Redis remains the single-threaded baseline, so its
measured server vCPU remains near one.

| Value | Mix | FCNP direct | RESP | Redis |
| ---: | --- | ---: | ---: | ---: |
| 64B | GET | 895,979 @ 12.852 | 883,799 @ 13.015 | 91,193 @ 0.999 |
| 64B | SET | 892,359 @ 12.678 | 868,260 @ 12.980 | 90,049 @ 0.999 |
| 64B | 80/20 | 896,322 @ 12.595 | 870,934 @ 12.842 | 90,735 @ 0.998 |
| 512B | GET | 850,044 @ 12.628 | 813,107 @ 12.005 | 90,104 @ 0.999 |
| 512B | SET | 718,312 @ 10.519 | 701,216 @ 10.833 | 90,126 @ 0.999 |
| 512B | 80/20 | 886,041 @ 12.747 | 856,073 @ 12.941 | 89,039 @ 0.999 |
| 4KiB | GET | 734,864 @ 13.297 | 701,610 @ 13.155 | 75,055 @ 1.000 |
| 4KiB | SET | 749,694 @ 12.605 | 687,039 @ 12.483 | 79,248 @ 0.999 |
| 4KiB | 80/20 | 737,592 @ 12.831 | 709,545 @ 13.371 | 76,970 @ 0.999 |
| 16KiB | GET | 585,573 @ 13.850 | 484,220 @ 14.531 | 60,112 @ 1.000 |
| 16KiB | SET | 524,326 @ 12.491 | 487,239 @ 12.823 | 68,854 @ 1.000 |
| 16KiB | 80/20 | 586,591 @ 13.427 | 496,836 @ 13.815 | 60,774 @ 0.999 |

Use these numbers as a sanity-check baseline, not as a universal claim. The
same-core view shows per-core efficiency. The 16-vCPU view shows the intended
scaled-out server shape. In the 16-vCPU runs, fast-cache does not consume the
full CPU cap, so the observed ceiling is not purely server CPU-bound. When
investigating regressions, profile I/O polling, request framing, socket
scheduling, and benchmark client pressure before assuming the shard engine is
the limiting factor.

### Pipelining Dimension

The `saturation` driver also supports a network-only pipelining axis:
`--pipeline-depth <N>`. Depth `1` is the default strict request/response loop.
Depths above `1` queue that many requests on each connection, flush once, and
then read the ordered responses. RESP backends and FCNP backends implement this
path. Embedded backends and TTL workloads are skipped for pipelined runs.

Example:

```bash
cargo run --release -p fast-cache-benchmarks --bin saturation -- \
  --backends fc-server-fcnp-direct \
  --addr 127.0.0.1:6501 \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 16 --clients 64 --pipeline-depth 16 \
  --key-count 100000 --duration 10 --latency-sample-rate 0
```

Repeat the same shape with each backend's address, for example RESP on
`127.0.0.1:6383` and Redis on `127.0.0.1:6379`. When comparing Redis and
fast-cache, run at least depths `1`, `4`, `16`, and `64` for the ordinary
pipeline curve. For ceiling claims, also test deeper queues such as `128`,
`500`, and `2000`. Pipelining changes the meaning of latency samples because
a request can wait behind earlier requests in the same connection queue; use
`--latency-sample-rate 0` for raw throughput sweeps, then run a separate
latency-focused pass once the useful depth range is known.

### Client-Pressure Dimension

For one-core TCP claims, also run a client-count sweep. This catches cases
where one client under-drives the server, while too many clients add scheduler
and socket pressure without increasing throughput.

Start and pin the server separately first. The focused script keeps value size,
mix, server core count, and pipeline depth fixed, then varies `--clients`:

```bash
BACKENDS=fc-server-fcnp-direct \
ADDR=127.0.0.1:6501 \
SERVER_PID="$(pgrep -f fast-cache-server | head -1)" \
VCPU_BUDGET=1 \
CLIENT_COUNTS="1 4 16 64 256" \
PIPELINE_DEPTHS="1 64 128 256" \
./benchmarks/scripts/run-client-sweep.sh
```

Repeat the same sweep for `fc-server-resp` and `redis`, changing `BACKENDS`,
`ADDR`, and `SERVER_PID` as needed. Use pipeline depth `1` for strict
request/response and sweep deeper queues for pipelined capacity. The best
single-core point is usually a combination of client count and pipeline depth.

## Embedded Ownership Model

`fc-embed` does not model the same ownership pattern as `dashmap`.
`DashMap` is a shared, reference-counted cache: every benchmark worker
can hold a clone of the same shared map handle and issue requests for
any key. That is the right model for `DashMap`, but it is not the
fast-cache embedded fast path.

fast-cache assumes thread-local sharding. A worker owns its assigned
cache slabs through `LocalEmbeddedStore`; requests must be routed to
the worker that owns the key's shard. The benchmark therefore warms an
`EmbeddedStore`, splits it into local stores, gives each worker one
local store, and restricts that worker's key stream to keys it owns.
Running `fc-embed` through an `Arc<EmbeddedStore>` shared by every
worker measures lock/copy overhead in the compatibility surface, not
the intended embedded architecture.

## Axes

| Axis | Default | Range | Notes |
| --- | --- | --- | --- |
| `--value-size` | 512 | 64, 512, 4096, 65536, 1048576, up to 16 MiB | KV payload size |
| `--mix` | `80-20` | `get`, `set`, `100-0`, `0-100`, `80-20`, `<get_pct>-<set_pct>` | Read/write ratio |
| `--vcpu-budget` | 4 | 1, 2, 4, 8, 16 | Server-side shard / worker count |
| `--clients` (saturation) | 16 | 1, 4, 16, 64, 128, 256 | Concurrent worker threads or connections |
| `CLIENT_COUNTS` (script) | `1 4 16 64 256` | space-separated list | Client-count sweep values for `run-client-sweep.sh` |
| `--pipeline-depth` (saturation) | 1 | 1, 4, 16, 64, 128, 500, 2000 | Requests queued per network connection before flush/read |
| `--submitters` (curve) | 16 | 1, 4, 16, 64, 128 | Driver size, not a workload axis |
| `--target-rates` (curve) | `100K,250K,500K,1M,2M,4M,8M` | comma list, K/M suffixes | X axis of the curve |
| `--key-count` | 100000 | scale down for large values | Hot-set cardinality |
| `--duration` | 10 | seconds | Measurement window |
| `--warmup` | 2 | seconds | Drops first-N-seconds of histogram samples |
| `--latency-sample-rate` | 1 | 0, 1, N | Record one latency sample every N measured ops; 0 disables per-op timing for raw throughput/profiling |

## Quick start

Embedded backends only, headline workload:

```bash
cd benchmarks
./scripts/run-headline.sh
```

Include the networked competitors via Docker (also turns on FCNP):

```bash
DOCKER=1 FCNP=1 ./scripts/run-headline.sh
```

Pin fast-cache-server cores on Linux:

```bash
SERVER_CPUSET=0-3 DOCKER=1 ./scripts/run-headline.sh
```

## How to run each backend

### fc-embed (default fast-cache library, safe build)

```bash
cargo run --release -p fast-cache-benchmarks --bin saturation -- \
  --backends fc-embed \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 4 --clients 16 --key-count 100000 \
  --duration 10
```

`fc-embed` defaults to `--read-mode ref`, matching the embedded API's
zero-copy `get_ref` path. In that mode GET rows measure lookup plus borrowed
value access; the reported GB/s is a logical payload rate computed as
successful operations multiplied by value size. It is not physical data
throughput. Use `--read-mode copy` to force materialized GET reads into the
benchmark scratch buffer when comparing copy bandwidth against backends that
always copy values out.

```bash
cargo run --release -p fast-cache-benchmarks --bin saturation -- \
  --backends fc-embed \
  --value-size 1048576 --mix get \
  --vcpu-budget 4 --clients 16 --key-count 1024 \
  --read-mode copy --duration 10
```

Other reference-read baselines are exposed as explicit backend ids, such as
`fc-shared-ref`, `fc-shared-prepared-ref`, and `dashmap-ref`.

Shared-handle fast-cache backends default to `4 * --vcpu-budget` lock stripes.
That is the recommended comparison shape for DashMap-style shared access. Use
the `fc-shared-worker-stripes` family when you specifically need the older
one-stripe-per-worker baseline.

Prepared-key shared baselines are exposed as `fc-shared-prepared` and
`fc-shared-prepared-ref`; they precompute route/hash metadata for repeated
point-key workloads. `fc-shared-copy-unlocked` is an explicit A/B backend that
clones stored `Bytes` under the read lock and copies after unlocking.
Build the benchmark crate with `--features no-ttl` to remove shared-store TTL
checks for TTL-free point-key comparisons.

### fc-embed-unsafe (reviewed unsafe hot paths)

Build the bench crate with `--features unsafe`. The same `fc-embed`
backend id now reports as `fc-embed-unsafe`.

```bash
cargo run --release -p fast-cache-benchmarks --features unsafe \
  --bin saturation -- \
  --backends fc-embed \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 4 --clients 16 --key-count 100000 \
  --duration 10
```

To collect both rows in one CSV, run the bench twice (default and
`--features unsafe`) writing to the same `--csv` file.

### fc-server-resp (fast-cache-server, RESP wire)

Start the server (optionally pinned):

```bash
cargo run --release -p fast-cache --features server --bin fast-cache-server -- \
  --server-mode direct --disable-persistence \
  --bind-addr 127.0.0.1:6383 --shard-count 4 &
# or pinned: taskset -c 0-3 ./target/release/fast-cache-server --server-mode direct --disable-persistence ...
```

The default server build is tokio/non-monoio and works on every supported
platform. On Linux, add `--features server,monoio` and run with
`FAST_CACHE_USE_MONOIO=1` to switch the TCP workers to monoio. Monoio always
uses `bytes-handoff` for connection read buffering through the monoio read
adapter. With `SERVER_DIRECT_SHARD_PORTS=1`, monoio also uses shard-owned
listener ports so each worker owns direct accept, parsing, command execution,
and storage on its pinned thread; the fanout port remains available for
non-routed clients. Direct shard ports start at `FCNP_DIRECT_BASE_PORT` or the
fanout port + 1. The helper
`scripts/run-tcp-client-sweep-local.sh` follows the same split:
`SERVER_RUNTIME=tokio` is the default, while `SERVER_RUNTIME=monoio` builds the
monoio feature and sets the runtime environment variables. Set
`SERVER_DIRECT_SHARD_PORTS=1` with either runtime to benchmark client-routed
shard ports in safe mode. Add `SERVER_UNSAFE=1` to the same run to enable the
owned-shard lock-bypass hot path where supported.

Monoio runtime experiments can be passed through the same helper. Use
`FAST_CACHE_MONOIO_DRIVER=legacy|io_uring` to compare monoio drivers,
`FAST_CACHE_MONOIO_SAFE_WRITER=inline|split|writev` to compare safe writer
paths, and `FAST_CACHE_TCP_BUFFER_BYTES=<bytes>` for socket buffer sweeps. The
`writev` safe writer keeps command execution unchanged, but lets GET responses
reuse the queued response path so larger FCNP/RESP values can be written as
header plus stored `Bytes` payload instead of always materializing a contiguous
response buffer.

The non-command streaming paths are opt-in separately on Linux. Use
`FAST_CACHE_WAL_TCP_USE_MONOIO=1` to run the TCP WAL exporter on monoio, and
`FAST_CACHE_REPLICATION_USE_MONOIO=1` to run the native FCRP replication
transport on monoio. These switches are intentionally independent of
`FAST_CACHE_USE_MONOIO=1` so direct server runtime benchmarks, WAL export
benchmarks, and replication-cost benchmarks can be isolated.

Then run the bench:

```bash
cargo run --release -p fast-cache-benchmarks --bin saturation -- \
  --backends fc-server-resp \
  --addr 127.0.0.1:6383 \
  --server-pid "$(pgrep -f fast-cache-server | head -1)" \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 4 --clients 16 --key-count 100000 \
  --duration 10
```

`--server-pid` enables process-CPU sampling for the server process via
`/proc/<pid>/stat` (Linux). On macOS this falls back to in-process
`getrusage`, which only captures the bench process.

### fc-server-fcnp (fast-cache-server, native binary wire)

Same listener as `fc-server-resp`; the server auto-detects FCNP from
the first request byte. Just point the bench at the same address:

```bash
cargo run --release -p fast-cache-benchmarks --bin saturation -- \
  --backends fc-server-fcnp \
  --addr 127.0.0.1:6383 \
  --server-pid "$(pgrep -f fast-cache-server | head -1)" \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 4 --clients 16 --key-count 100000 \
  --duration 10
```

### Redis

```bash
docker compose -f docker/compose.yml up -d redis

cargo run --release -p fast-cache-benchmarks --bin saturation -- \
  --backends redis \
  --addr 127.0.0.1:6379 \
  --server-pid "$(docker inspect --format '{{.State.Pid}}' bench-redis)" \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 1 --clients 16 --key-count 100000 \
  --duration 10
```

Redis is single-threaded; `--vcpu-budget` is informational for this row.

### Valkey

```bash
docker compose -f docker/compose.yml up -d valkey

cargo run --release -p fast-cache-benchmarks --bin saturation -- \
  --backends valkey \
  --addr 127.0.0.1:6381 \
  --server-pid "$(docker inspect --format '{{.State.Pid}}' bench-valkey)" \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 1 --clients 16 --key-count 100000 \
  --duration 10
```

### Dragonfly

```bash
docker compose -f docker/compose.yml up -d dragonfly

cargo run --release -p fast-cache-benchmarks --bin saturation -- \
  --backends dragonfly \
  --addr 127.0.0.1:6382 \
  --server-pid "$(docker inspect --format '{{.State.Pid}}' bench-dragonfly)" \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 4 --clients 16 --key-count 100000 \
  --duration 10
```

### Embedded competitors (dashmap, moka, lru, rwlock-hashmap)

```bash
cargo run --release -p fast-cache-benchmarks --bin saturation -- \
  --backends dashmap,moka,lru,rwlock-hashmap \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 4 --clients 16 --key-count 100000 \
  --duration 10
```

### fc-py (Python harness, `fast_cache.Store`)

Build the PyO3 wheel into the active environment, then run the
Python harness:

```bash
maturin develop --release -m crates/fast-cache-py/Cargo.toml --features extension-module

python benchmarks/python/fc_py_bench.py \
  --value-size 512 --mix 80-20 \
  --vcpu-budget 4 --clients 4 --key-count 100000 \
  --warmup 2 --duration 10 \
  --csv benchmarks/results/python.csv
```

`./scripts/run-python.sh` builds the wheel if needed and runs the harness.

### fc-lmcache (LMCache plugin via `FastCacheStorageBackend`)

```bash
pip install lmcache
pip install ./integrations/lmcache_storage_backend
maturin develop --release -m crates/fast-cache-py/Cargo.toml --features extension-module

python benchmarks/python/fc_lmcache_bench.py \
  --value-size 4096 --mix 80-20 \
  --vcpu-budget 4 --clients 4 --key-count 4096 \
  --client-architecture shared \
  --latency-sample-rate 0 \
  --warmup 2 --duration 10 \
  --csv benchmarks/results/python.csv
```

Pass `--with-local-cpu` to additionally bench LMCache's built-in
`LocalCPUBackend` on the same workload through the same plugin
interface:

```bash
python benchmarks/python/fc_lmcache_bench.py --with-local-cpu \
  --value-size 4096 --mix 80-20 \
  --vcpu-budget 4 --clients 4 --key-count 4096 \
  --client-architecture shared \
  --latency-sample-rate 0 \
  --duration 10
```

`LMCACHE=1 ./scripts/run-python.sh` runs both Python harnesses.

Use `--latency-sample-rate 0` for logical payload GB/s saturation runs where latency
percentiles are secondary to raw bytes moved per second.

Use `--op-batch-size N` to issue same-operation batches from each benchmark
worker. This is the knob that exercises pipelined FCNP/TCP batches for LMCache
bandwidth ceiling tests.

The benchmark defaults to `--client-architecture shared` because it uses
arbitrary multi-client keys. `local_embedded` is for shard-owned caller
routing, such as the vLLM direct connector path. Use
`--connection embedded` for the in-process LMCache path, or
`--connection tcp --fcnp-addr 127.0.0.1:6500` to drive the LMCache plugin
through a fast-cache FCNP/TCP server. `--client-architecture` remains available
for low-level benchmark shapes such as `shared`, `local_embedded`,
`fcnp_tcp`, and `fcnp_tcp_python`.

LMCache's wire-level types (`CacheEngineKey`, `BytesBufferMemoryObj`)
vary between releases; the harness probes the common constructor
shapes and will surface a clear error if a newer LMCache changes the
signature.

## Comparing two backends

```bash
BACKENDS=fc-embed,dashmap ./scripts/run-compare.sh
BACKENDS=fc-server-resp,redis ADDR=127.0.0.1:6379 ./scripts/run-compare.sh
BACKENDS=fc-server-fcnp,fc-server-resp ADDR=127.0.0.1:6383 ./scripts/run-compare.sh
BACKENDS=fc-server-fcnp,dragonfly ADDR=127.0.0.1:6382 ./scripts/run-compare.sh
```

Workload knobs are environment variables: `VALUE_SIZE`, `MIX`,
`VCPU_BUDGET`, `CLIENTS`, `PIPELINE_DEPTH`, `KEY_COUNT`, `DURATION`,
`WARMUP`.

## Full matrices

```bash
./scripts/run-saturation.sh         # value_size x mix x vcpu_budget
PIPELINE_DEPTHS="1 4 16 64" ./scripts/run-saturation.sh
BACKENDS=redis ADDR=127.0.0.1:6379 ./scripts/run-client-sweep.sh
./scripts/run-curve.sh              # CPU-vs-load curve at headline workload
LMCACHE=1 ./scripts/run-python.sh   # fc-py + fc-lmcache
```

Both write to `results/<mode>_<timestamp>.csv`. The `results/` directory
is gitignored; commit only curated summaries.

## Server CPU pinning

For honest CPU-budget comparisons, set `SERVER_CPUSET` (Linux) to pin
`fast-cache-server` to a cpuset via `taskset`. The helper scripts pick
it up automatically:

```bash
SERVER_CPUSET=0-3 DOCKER=1 ./scripts/run-headline.sh
SERVER_CPUSET=0-3 DOCKER=1 ./scripts/run-curve.sh
SERVER_CPUSET=0-3 DOCKER=1 ./scripts/run-saturation.sh
```

The Docker'd backends are pinned by the orchestrator; pin them via
`docker run --cpuset-cpus=0-3` or via compose `cpuset:` if you need a
matching budget. Adjust `docker/compose.yml` if you want this baked in.

## Reproducibility checklist

Every committed result table should include:

- date
- machine shape (cores, RAM, OS, kernel)
- pinned versions for Redis / Valkey / Dragonfly (see `docker/compose.yml`)
- the exact `run-*.sh` command and flags
- whether `SERVER_CPUSET` was set, and to what
- correctness gate result (if any)

## Limits

- Localhost loopback bandwidth caps large-value networked comparisons before the cache does. Measure your loopback bandwidth (`iperf3 -c 127.0.0.1`) and report it alongside.
- Redis and Valkey have a 512 MiB hard cap on string values, and performance falls off long before that.
- CPU sampling for external processes works on Linux only. macOS falls back to bench-process `getrusage`, which does not include the server's CPU consumption.
- The Python latency accumulator uses reservoir sampling rather than an HDR histogram, so its tail estimates are close to the Rust drivers but not identical at extreme rates.
