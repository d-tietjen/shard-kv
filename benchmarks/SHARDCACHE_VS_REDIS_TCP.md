# shardcache vs Redis, Valkey, and Dragonfly over TCP

Published: 2026-05-14
Updated: 2026-05-18

This note collects the Linux benchmark runs that compare shardcache over
TCP against Redis-compatible TCP databases. It intentionally excludes
embedded-only results so the comparison stays focused on networked deployment
modes.

## Scope

Backends:

| Backend | Meaning |
| --- | --- |
| SCNP direct | shardcache native TCP protocol with client-side routing to shard-owned ports |
| RESP | shardcache Redis-compatible TCP protocol |
| Redis | Redis OSS TCP baseline |
| Valkey | Valkey TCP baseline |
| Dragonfly | Dragonfly TCP baseline |

Test host:

| Field | Value |
| --- | --- |
| Host | Linux |
| OS | Ubuntu 24.04 LTS family |
| Key distribution | Uniform |
| Key count | 100k |
| Benchmark driver | `saturation` |
| Clients | 64 |
| Duration | 10s measured, 2s warmup |
| Latency sampling | Disabled with `--latency-sample-rate 0` for throughput sweeps |

Cells are `ops/sec @ measured server vCPU`. The measured vCPU value is the
actual server CPU consumed during the run, not just the configured CPU cap.

## Reading The Tables

The same-core view pins shardcache to one CPU and starts it with
`--shard-count 1`. Redis is single-threaded, so this is the per-core comparison.

The 16-vCPU view pins shardcache to CPUs `0-15` and starts it with
`--shard-count 16`. Redis remains the single-threaded TCP baseline. This shows
the shardcache scale-out shape against a drop-in Redis deployment.

Pipeline depth `1` means strict request/response. Higher depths queue N
requests per connection, flush once, and read ordered responses.

## Shardcache Large Network Database Matrix, 2026-05-18

Artifact:
`benchmarks/results/network_db_server_20260518_030526/network_db_matrix.csv`.

This run used the current `main` checkout at commit
`741649ad0d7779947e1857ff8a8fd91236b67064` on Linux. Server processes
were pinned to CPUs `0-15`; the benchmark client process was pinned to CPUs
`16-31`. The sweep covered value sizes `64B`, `512B`, `4KiB`, `64KiB`, and
`1MiB`; mixes `GET`, `SET`, and `80/20`; client counts `16`, `64`, and `256`;
and pipeline depths `1`, `16`, and `64`.

Cells below are `ops/sec / logical GB/s @ measured server vCPU (clients,
pipeline)`. The table chooses each backend's best completed row for that
value/mix. Latency sampling was disabled, so latency columns are intentionally
zero in this artifact.

Dragonfly completed most of the matrix, but after the `1MiB` write-heavy rows
it stopped accepting connections. The final `1MiB 80/20` Dragonfly rows are
therefore marked `n/a`; one preceding `1MiB SET` row reported benchmark errors.

### Best 80/20 Throughput By Value Size

| Value | SCNP direct | shardcache RESP | Redis | Valkey | Dragonfly |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64B | 31.18M / 2.00 @ 14.6 (64c, p64) | 19.76M / 1.26 @ 16.0 (64c, p64) | 1.19M / 0.08 @ 1.0 (16c, p64) | 1.21M / 0.08 @ 1.0 (16c, p64) | 4.10M / 0.26 @ 15.9 (64c, p64) |
| 512B | 12.20M / 6.25 @ 9.1 (16c, p64) | 10.52M / 5.39 @ 16.0 (64c, p64) | 858k / 0.44 @ 1.0 (16c, p64) | 855k / 0.44 @ 1.0 (16c, p64) | 1.08M / 0.55 @ 14.1 (64c, p64) |
| 4KiB | 1.95M / 8.00 @ 9.8 (16c, p16) | 1.88M / 7.70 @ 10.4 (16c, p16) | 368k / 1.51 @ 1.0 (16c, p16) | 335k / 1.37 @ 1.0 (16c, p16) | 833k / 3.41 @ 14.5 (64c, p64) |
| 64KiB | 141k / 9.26 @ 10.3 (16c, p1) | 141k / 9.24 @ 11.4 (16c, p1) | 37k / 2.41 @ 1.0 (16c, p16) | 36k / 2.36 @ 1.0 (16c, p16) | 98k / 6.41 @ 10.7 (16c, p1) |
| 1MiB | 5k / 5.59 @ 13.4 (16c, p1) | 5k / 4.79 @ 13.4 (16c, p1) | 3k / 2.89 @ 1.0 (16c, p1) | 3k / 2.81 @ 1.0 (16c, p16) | n/a |

### 64B Best Throughput By Mix

| Mix | SCNP direct | shardcache RESP | Redis | Valkey | Dragonfly |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 32.11M / 2.06 @ 14.6 (64c, p64) | 23.28M / 1.49 @ 16.0 (64c, p64) | 1.22M / 0.08 @ 1.0 (16c, p64) | 1.23M / 0.08 @ 1.0 (16c, p64) | 3.93M / 0.25 @ 15.9 (64c, p64) |
| SET | 32.07M / 2.05 @ 15.3 (64c, p64) | 12.56M / 0.80 @ 15.7 (64c, p64) | 1.03M / 0.07 @ 1.0 (16c, p64) | 997k / 0.06 @ 1.0 (16c, p64) | 4.31M / 0.28 @ 16.0 (256c, p64) |
| 80/20 | 31.18M / 2.00 @ 14.6 (64c, p64) | 19.76M / 1.26 @ 16.0 (64c, p64) | 1.19M / 0.08 @ 1.0 (16c, p64) | 1.21M / 0.08 @ 1.0 (16c, p64) | 4.10M / 0.26 @ 15.9 (64c, p64) |

### Large Value Best Bandwidth

| Value | Mix | SCNP direct | shardcache RESP | Redis | Valkey | Dragonfly |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 64KiB | GET | 136k / 8.90 @ 10.1 (16c, p1) | 146k / 9.59 @ 11.0 (16c, p1) | 35k / 2.28 @ 1.0 (16c, p16) | 33k / 2.14 @ 1.0 (16c, p16) | 89k / 5.80 @ 14.8 (64c, p16) |
| 64KiB | SET | 158k / 10.33 @ 14.9 (16c, p64) | 167k / 10.98 @ 14.9 (16c, p16) | 41k / 2.67 @ 1.0 (16c, p16) | 40k / 2.65 @ 1.0 (16c, p16) | 88k / 5.74 @ 11.0 (16c, p1) |
| 64KiB | 80/20 | 141k / 9.26 @ 10.3 (16c, p1) | 141k / 9.24 @ 11.4 (16c, p1) | 37k / 2.41 @ 1.0 (16c, p16) | 36k / 2.36 @ 1.0 (16c, p16) | 98k / 6.41 @ 10.7 (16c, p1) |
| 1MiB | GET | 6k / 6.60 @ 14.2 (16c, p1) | 6k / 6.04 @ 14.7 (16c, p1) | 2k / 2.43 @ 1.0 (64c, p1) | 2k / 2.51 @ 1.0 (64c, p1) | 5k / 5.70 @ 10.4 (16c, p1) |
| 1MiB | SET | 7k / 7.31 @ 14.2 (16c, p16) | 7k / 7.01 @ 14.4 (16c, p16) | 5k / 5.55 @ 1.0 (16c, p1) | 4k / 4.64 @ 1.0 (16c, p1) | 5k / 5.29 @ 10.2 (16c, p1) |
| 1MiB | 80/20 | 5k / 5.59 @ 13.4 (16c, p1) | 5k / 4.79 @ 13.4 (16c, p1) | 3k / 2.89 @ 1.0 (16c, p1) | 3k / 2.81 @ 1.0 (16c, p16) | n/a |

### Strict Request/Response Sanity Check

Workload: `64B`, `80/20`, pipeline depth `1`. This is not the ceiling row for
the networked systems; it shows the closed-loop request/response shape before
pipelining.

| Backend | Best strict row |
| --- | ---: |
| SCNP direct | 894k / 0.06 @ 13.9 (256c, p1) |
| shardcache RESP | 888k / 0.06 @ 14.1 (256c, p1) |
| Redis | 91k / 0.01 @ 1.0 (16c, p1) |
| Valkey | 89k / 0.01 @ 1.0 (256c, p1) |
| Dragonfly | 757k / 0.05 @ 16.0 (256c, p1) |

For the `64B 80/20` best-throughput row, SCNP direct is `26.2x` Redis,
shardcache RESP is `16.6x` Redis, Valkey is effectively tied with Redis, and
Dragonfly is `3.45x` Redis on this host.

The sections below preserve earlier Redis-only fixed-shape runs. Use the
large matrix above for the current Redis/Valkey/Dragonfly head-to-head.

## Headline Non-Pipelined 80/20

Value size: `64B`. Mix: `80/20`. Pipeline depth: `1`.

| Server CPU view | SCNP direct | RESP | Redis |
| --- | ---: | ---: | ---: |
| 1 vCPU | 106,072 @ 1.000 | 99,438 @ 1.000 | 94,015 @ 0.999 |
| 16-vCPU cap | 896,322 @ 12.595 | 870,934 @ 12.842 | 90,735 @ 0.998 |

At pipeline depth `1`, same-core shardcache is modestly ahead of Redis. With
16 server cores available, shardcache is roughly 10x Redis for this workload
because requests can be routed across the shard-owned workers.

## Same-Core Non-Pipelined Matrix

Shardcache: `taskset -c 0`, `--shard-count 1`. Redis: single-threaded baseline.
Pipeline depth: `1`.

| Value | Mix | SCNP direct | RESP | Redis |
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

## 16-vCPU Non-Pipelined Matrix

Shardcache: `taskset -c 0-15`, `--shard-count 16`. Redis: single-threaded
baseline. Pipeline depth: `1`.

| Value | Mix | SCNP direct | RESP | Redis |
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

## Pipeline Sweep

Value size: `64B`. Mix: `80/20`. Clients: `64`. Key count: `100k`.

### Same-Core Pipeline Sweep

Shardcache: `taskset -c 0`, `--shard-count 1`.

| Pipeline depth | SCNP direct | RESP | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 109,927 @ 0.999 | 98,069 @ 0.999 | 89,035 @ 0.999 |
| 4 | 395,374 @ 1.000 | 358,517 @ 0.999 | 301,799 @ 0.999 |
| 16 | 1,277,655 @ 1.000 | 1,063,099 @ 0.999 | 717,818 @ 0.999 |
| 64 | 3,255,162 @ 1.000 | 2,281,784 @ 0.999 | 1,204,399 @ 0.999 |

## Fixed-Load CPU Efficiency

Artifact:
`benchmarks/results/cpu_efficiency_server_20260518_103819/cpu_efficiency_curve.csv`.

This run asks a different question from the ceiling sweeps: how much server CPU
is needed to hold a fixed request rate? It used Linux with the server
pinned to CPU `0` and benchmark clients pinned to CPUs `16-31`. Workload:
`64B`, `80/20`, `100k` keys, `64` clients, pipeline depth `64`, `10s`
measured, `2s` warmup, and latency sampling every `1024` measured operations.

Cells are achieved throughput at measured server vCPU.

| Target | SCNP direct | shardcache RESP | Redis |
| ---: | ---: | ---: | ---: |
| 100K ops/s | 99,962 @ 0.052 | 99,978 @ 0.060 | 99,976 @ 0.098 |
| 1M ops/s | 999,731 @ 0.439 | 999,736 @ 0.531 | 999,734 @ 0.909 |
| 2M ops/s | 1,999,581 @ 0.843 | 1,999,570 @ 0.975 | 1,233,072 @ 1.001 |

At the 1M fixed-load point, SCNP direct uses about half the server CPU Redis
uses for the same delivered request rate. At the 2M target, SCNP direct still
has headroom on one server CPU, shardcache RESP is near the edge of one CPU,
and Redis saturates at about `1.23M` ops/sec.

### 16-vCPU Pipeline Sweep

Shardcache: `taskset -c 0-15`, `--shard-count 16`.

| Pipeline depth | SCNP direct | RESP | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 892,222 @ 12.544 | 904,005 @ 13.394 | 91,842 @ 0.999 |
| 4 | 3,389,930 @ 12.400 | 3,146,744 @ 12.906 | 303,558 @ 0.999 |
| 16 | 12,039,668 @ 12.790 | 8,930,613 @ 13.126 | 725,161 @ 1.000 |
| 64 | 33,854,047 @ 12.496 | 16,582,102 @ 13.947 | 1,156,757 @ 0.999 |

At depth `64`, SCNP direct reaches 33.85M ops/sec under the 16-vCPU cap while
Redis reaches 1.16M ops/sec on its single server thread. This is the clearest
current TCP result for the direct shard-routed path.

## Deep Pipeline Follow-Up

The `p64` sweep is not the single-core SCNP ceiling. The direct-shard client
continues to improve with deeper queues, so this pass includes a standalone
deeper-pipeline check for the same-core SCNP direct path.

Shardcache: SCNP direct, `taskset -c 0`, `--shard-count 1`, `64B`, `64`
clients, `100k` keys.

| Pipeline depth | Mix | SCNP direct |
| ---: | --- | ---: |
| 500 | GET | 5,954,779 @ 1.000 |
| 2000 | GET | 5,950,575 @ 1.000 |
| 500 | SET | 3,826,818 @ 1.000 |
| 2000 | SET | 4,236,904 @ 1.000 |
| 500 | 80/20 | 4,982,887 @ 1.000 |
| 2000 | 80/20 | 5,519,830 @ 0.999 |

The current RESP deep-pipeline rerun at `p500` hit connection errors, so do not
cite that run as a current RESP ceiling. The moderate-depth RESP rows above are
valid. Rerun RESP and Redis at the same deeper depths before making a
standalone deep-pipeline head-to-head claim.

## Single-Core Client Sweep

The client-concurrency sweep runs `1`, `16`, `64`, and `256` clients against
the same single-core server launch shape.

This answers a different question than the pipeline sweep. Pipeline depth
shows how much work each connection can keep in flight. Client count shows
whether the server is under-driven by too few connections or slowed down by too
many contending clients.

Workload: `64B`, `80/20`, `100k` keys, `10s` measured, `2s` warmup,
`--latency-sample-rate 0`. All rows completed with zero benchmark errors.
Cells are `ops/sec @ measured server vCPU`.

### Client Sweep, Pipeline Depth 1

Strict request/response.

| Clients | SCNP direct | RESP | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 47,125 @ 0.508 | 46,706 @ 0.513 | 21,352 @ 0.322 |
| 16 | 113,773 @ 1.488 | 114,282 @ 1.476 | 78,626 @ 0.983 |
| 64 | 104,259 @ 1.547 | 104,676 @ 1.514 | 82,689 @ 0.979 |
| 256 | 104,229 @ 1.550 | 102,222 @ 1.566 | 82,250 @ 0.982 |

### Client Sweep, Pipeline Depth 64

Moderate pipelining.

| Clients | SCNP direct | RESP | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 1,612,956 @ 0.498 | 1,234,963 @ 0.470 | 551,403 @ 0.557 |
| 16 | 3,295,339 @ 1.255 | 2,398,927 @ 1.025 | 998,247 @ 0.998 |
| 64 | 3,214,048 @ 1.434 | 2,392,891 @ 1.042 | 1,016,886 @ 0.993 |
| 256 | 3,176,727 @ 1.559 | 2,280,879 @ 0.987 | 941,500 @ 0.994 |

The knee is around `16` clients for shardcache in this run. More clients do
not improve throughput at depth `64`, and at `256` clients throughput starts
to slip. Redis also peaks around `16` to `64` clients for the pipelined row.

The server was launched with `taskset -c 0`, `--server-mode direct`, and
`--disable-persistence` for shardcache. Some shardcache rows report more than
`1.0` measured server vCPU because the harness samples process CPU time for
the server process; use a hard cgroup cpuset if a strict accounting cap is
required for a publication-grade single-core claim.

The benchmark harness includes a focused script for this axis:

```bash
BACKENDS=fc-server-scnp-direct ADDR=127.0.0.1:6501 \
  VCPU_BUDGET=1 CLIENT_COUNTS="1 16 64 256" PIPELINE_DEPTH=64 \
  ./benchmarks/scripts/run-client-sweep.sh
```

Run the same sweep for `fc-server-resp` and `redis`, changing `BACKENDS` and
`ADDR` for each server. Publish this as a separate table from the fixed
`64`-client rows above so readers can see both the chosen headline point and
the client-pressure curve that led to it.

## Takeaways

- Non-pipelined same-core TCP is close: shardcache is ahead, but by a modest
  margin.
- Non-pipelined 16-vCPU shardcache is roughly 10x Redis on the 64B 80/20 row.
- Pipelining is the critical TCP dimension. At depth `64`, SCNP direct reaches
  33.85M ops/sec under the 16-vCPU cap, while Redis reaches 1.16M ops/sec.
- At a fixed 1M ops/sec same-core pipelined load, SCNP direct uses 0.439 vCPU
  versus Redis at 0.909 vCPU. At a 2M target, SCNP direct still completes the
  row at 0.843 vCPU while Redis saturates at 1.23M ops/sec.
- Deep pipeline SCNP direct reaches 5.52M ops/sec on the same-core 80/20 row.
- In the single-core client sweep, the useful client-count knee is around `16`
  clients for the pipelined shardcache rows; `64` and `256` clients do not add
  throughput.
- RESP and Redis deep-pipeline ceilings beyond `p64` should be rerun cleanly
  before publishing a deeper head-to-head table.

## Reproduction Shape

Shardcache same-core:

```bash
taskset -c 0 ./target/release/shardcache \
  --server-mode direct --disable-persistence \
  --bind-addr 127.0.0.1:6383 \
  --shard-count 1
```

Shardcache 16-vCPU:

```bash
taskset -c 0-15 ./target/release/shardcache \
  --server-mode direct --disable-persistence \
  --bind-addr 127.0.0.1:6383 \
  --shard-count 16
```

SCNP direct shard ports:

```bash
SHARDCACHE_USE_MONOIO=1 SHARDCACHE_DIRECT_SHARD_PORTS=1 \
  taskset -c 0 ./target/release/shardcache \
  --server-mode direct --disable-persistence \
  --bind-addr 127.0.0.1:6500 \
  --shard-count 1
```

The fanout port remains `6500`; the default first direct shard port is `6501`
unless `SHARDCACHE_DIRECT_SHARD_BASE_PORT` is set.

Pipeline run:

```bash
cargo run --release -p shardcache-benchmarks --bin saturation -- \
  --backends fc-server-scnp-direct \
  --addr 127.0.0.1:6501 \
  --value-size 64 \
  --mix 80-20 \
  --clients 64 \
  --pipeline-depth 64 \
  --key-count 100000 \
  --duration 10 \
  --warmup 2 \
  --latency-sample-rate 0
```

Run the same workload for `fc-server-resp` and `redis`, changing the backend
and address to match each server.

Fixed-load CPU curve:

```bash
cargo run --release -p shardcache-benchmarks --bin curve -- \
  --backends fc-server-scnp-direct \
  --addr 127.0.0.1:6501 \
  --server-pid <server-pid> \
  --vcpu-budget 1 \
  --submitters 64 \
  --pipeline-depth 64 \
  --value-size 64 \
  --mix 80-20 \
  --key-count 100000 \
  --target-rates 100K,1M,2M \
  --duration 10 \
  --warmup 2 \
  --latency-sample-rate 1024
```
