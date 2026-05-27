# shardcache LMCache Backend vs Redis TCP: Bandwidth Saturation

Standalone benchmark run on Linux on May 14, 2026.

This report is framed around GB/s, not operation rate. For LMCache-sized KV
blocks, the important question is how much payload bandwidth the integration can
move before the hardware, Python plugin layer, or Redis server path becomes the
bottleneck.

The surfaces are different: LMCache is measured through the shardcache LMCache
storage plugin API, either in-process or over shardcache's native SCNP/TCP
protocol. Redis uses RESP/TCP over loopback. Treat this as a practical
integration comparison, not a pure engine microbenchmark.

## Current Coverage

This is the curated LMCache head-to-head artifact currently in the repository.
It covers shardcache as an LMCache storage plugin against Redis TCP for
`64 KiB`, `256 KiB`, and `1 MiB` payloads. It does not yet include:

- LMCache's built-in `LocalCPUBackend` as a published baseline. The harness can
  attempt this with `--with-local-cpu`, but the original run could not construct
  that backend on LMCache `0.4.4`.
- A `5 MiB` LMCache-specific saturation run. The local Apple M5 Max rerun below
  still uses the published `64 KiB`, `256 KiB`, and `1 MiB` sizes.
- CUDA or GPU-direct transfer claims. Those belong to the runtime connector
  proof gates, not this storage-plugin benchmark.

## Setup

| Setting | Value |
| --- | --- |
| Host | Ubuntu 24.04.4 LTS |
| shardcache path | `shardcache_lmcache_backend.ShardCacheStorageBackend` |
| LMCache | `lmcache 0.4.4`, Python non-CUDA backend fallback |
| LMCache embedded architecture | `client_architecture=shared` |
| LMCache TCP architecture | `client_architecture=scnp_tcp`, generic SCNP listener at `127.0.0.1:6500` |
| Redis | `redis:7-alpine`, Docker, `--cpuset-cpus=0-15` |
| Redis protocol | RESP/TCP on `127.0.0.1:6379` |
| Clients | LMCache embedded and SCNP/TCP swept `16`, `32`, `64`; Redis used `16` |
| Redis pipeline depth | swept `1`, `16`, `64` |
| Key count | `1024`, uniform |
| Warmup / measure | `2s` warmup, `6s` measured |
| Value sizes | `64 KiB`, `256 KiB`, `1 MiB` |
| Latency timing | disabled with `--latency-sample-rate 0` |

CPU reporting differs by harness. Embedded LMCache rows report CPU used by the
Python benchmark process. LMCache SCNP/TCP rows report Python client CPU and a
separate sampled shardcache CPU. Redis rows report CPU used by the Redis
server process only; Redis client CPU is not included.

LMCache's built-in `LocalCPUBackend` was not included because LMCache 0.4.4
requires allocator or metadata state for that backend constructor.

## Peak Rows

| Backend | Workload | GB/s | Shape | vCPU |
| --- | --- | ---: | --- | ---: |
| LMCache embedded plugin | 1 MiB GET | 40.107 | 16 clients | 4.723 client |
| LMCache embedded plugin | 1 MiB SET | 19.601 | 16 clients | 10.039 client |
| LMCache embedded plugin | 1 MiB 80/20 | 20.603 | 16 clients | 9.196 client |
| LMCache SCNP/TCP plugin | 1 MiB GET | 6.944 | 16 clients | 2.578 client, 2.189 server |
| LMCache SCNP/TCP plugin | 1 MiB SET | 6.112 | 16 clients | 9.332 client, 3.938 server |
| LMCache SCNP/TCP plugin | 1 MiB 80/20 | 6.067 | 16 clients | 3.033 client, 2.187 server |
| Redis TCP | 1 MiB SET | 4.206 | 16 clients, pipeline 1 | 0.998 |
| Redis TCP | 1 MiB 80/20 | 2.685 | 16 clients, pipeline 1 | 1.001 |
| Redis TCP | 256 KiB SET | 3.621 | 16 clients, pipeline 1 | 0.998 |

## Local Apple M5 Max Rerun

Local rerun on May 26, 2026 on an Apple M5 Max with 18 CPU cores and 128 GiB
memory. The shardcache shard budget was set to `16` because embedded shard
counts must be powers of two. LMCache `0.4.5` was installed from source with
`NO_CUDA_EXT=1`, using its Python non-CUDA fallback. Redis ran locally through
Homebrew on `127.0.0.1:6390`.

The Redis vCPU column in the raw CSVs is client-process CPU on macOS, not Redis
server CPU, because the Linux external PID sampler is unavailable locally. Use
this table for payload bandwidth comparisons, not CPU-normalized claims. The
SCNP/TCP SET rows in the table below were captured before the Rust-side LMCache
PUT helpers were added to the SCNP Python store. Keep them as regression
evidence, not as the current optimized SET result.

Raw CSVs:

- [`lmcache-embedded.csv`](reference/lmcache-m5-local-20260526/lmcache-embedded.csv)
- [`lmcache-scnp-tcp.csv`](reference/lmcache-m5-local-20260526/lmcache-scnp-tcp.csv)
- [`redis-tcp.csv`](reference/lmcache-m5-local-20260526/redis-tcp.csv)

| Value | Mix | Embedded GB/s | Embedded shape | SCNP/TCP GB/s | SCNP shape | Redis GB/s | Redis shape | Embedded vs Redis | SCNP/TCP vs Redis |
| --- | --- | ---: | --- | ---: | --- | ---: | --- | ---: | ---: |
| 64 KiB | GET | 9.397 | C64 | 4.336 | C16 | 6.621 | P64 | 1.42x | 0.65x |
| 64 KiB | SET | 10.159 | C64 | 0.291 | C16 | 4.338 | P16 | 2.34x | 0.07x |
| 64 KiB | 80/20 | 9.287 | C64 | 1.043 | C16 | 5.526 | P64 | 1.68x | 0.19x |
| 256 KiB | GET | 32.418 | C64 | 9.826 | C16 | 7.423 | P64 | 4.37x | 1.32x |
| 256 KiB | SET | 31.482 | C64 | 0.311 | C32 | 9.722 | P16 | 3.24x | 0.03x |
| 256 KiB | 80/20 | 29.235 | C64 | 1.396 | C64 | 8.657 | P16 | 3.38x | 0.16x |
| 1 MiB | GET | 77.130 | C64 | 14.970 | C16 | 9.389 | P1 | 8.21x | 1.59x |
| 1 MiB | SET | 104.971 | C64 | 0.253 | C16 | 11.282 | P16 | 9.30x | 0.02x |
| 1 MiB | 80/20 | 90.147 | C32 | 0.401 | C16 | 9.467 | P1 | 9.52x | 0.04x |

## Local SCNP/TCP SET Path Optimization Probe

After the local Apple M5 Max rerun, `shardcache.ScnpStore` was updated to expose
the LMCache prepared PUT helpers, pipeline multi-item `batch_set` calls, and
stream encoded byte payloads directly into the SCNP writer. This keeps LMCache
PUT record encoding in Rust instead of falling back to Python
`_encode_memory_obj(...)` plus one generic `batch_set` loop.

Raw CSVs:

- [`scnp-set-batch1-after.csv`](reference/lmcache-m5-set-path-20260526/scnp-set-batch1-after.csv)
- [`scnp-set-batch16-after.csv`](reference/lmcache-m5-set-path-20260526/scnp-set-batch16-after.csv)

| Value | Mix | Clients | Op batch | SCNP/TCP GB/s | Previous local SCNP/TCP GB/s | Redis local GB/s | SCNP/TCP vs Redis |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 MiB | SET | 16 | 1 | 12.724 | 0.253 | 11.282 | 1.13x |
| 1 MiB | SET | 16 | 16 | 8.823 | 0.253 | 11.282 | 0.78x |

For 1 MiB values, `op-batch-size=1` remained the best local shape. Larger
batches reduce Python-side scheduling overhead, but they also bunch very large
writes onto each worker connection, so the socket copy and server receive path
become the limiter sooner.

## Local SCNP/TCP Op-Rate Probe

The same machine also ran a small-value `64 B` probe to separate raw SCNP/TCP
capacity from LMCache Python plugin overhead. Raw SCNP was measured with the
Rust saturation harness against the fanout port and shard-owned direct ports.
LMCache was measured through `ShardCacheStorageBackend` over `shardcache.ScnpStore`.

Raw CSVs:

- [`scnp-raw-64b.csv`](reference/lmcache-m5-oplimit-20260526/scnp-raw-64b.csv)
- [`lmcache-scnp-64b.csv`](reference/lmcache-m5-oplimit-20260526/lmcache-scnp-64b.csv)

| Path | Mix | Clients | Pipeline / op batch | Ops/sec |
| --- | --- | ---: | ---: | ---: |
| Raw SCNP fanout | GET | 16 | 1 | 192,140 |
| Raw SCNP fanout | GET | 16 | 64 | 8,849,835 |
| Raw SCNP fanout | SET | 16 | 64 | 6,467,487 |
| Raw SCNP fanout | SET | 256 | 64 | 8,523,650 |
| Raw SCNP shard-direct | GET | 16 | 64 | 8,810,884 |
| Raw SCNP shard-direct | SET | 64 | 64 | 8,386,538 |
| LMCache SCNP | GET | 16 | 1 | 54,600 |
| LMCache SCNP | GET | 16 | 16 | 168,060 |
| LMCache SCNP | SET | 16 | 1 | 43,964 |
| LMCache SCNP | SET | 16 | 64 | 230,008 |

The raw TCP path is therefore not the current op-rate ceiling; with request
pipelining it is already in the `6.5M-8.9M ops/sec` range for tiny values on
this local run. The LMCache path is bounded by Python key/object work,
MemoryObj reconstruction, and the plugin call shape before it reaches the raw
SCNP socket limit.

## Best By Value Size

Each row selects the best embedded LMCache client count, best SCNP/TCP LMCache
client count, and best Redis pipeline depth for that value size and mix.

| Value | Mix | Embedded GB/s | SCNP/TCP GB/s | Redis GB/s | SCNP/TCP vs Redis | Embedded vs SCNP/TCP |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 64 KiB | GET | 4.894 | 1.194 | 2.224 | 0.54x | 4.10x |
| 64 KiB | SET | 3.743 | 1.228 | 2.201 | 0.56x | 3.05x |
| 64 KiB | 80/20 | 3.936 | 1.218 | 2.351 | 0.52x | 3.23x |
| 256 KiB | GET | 11.291 | 4.388 | 2.549 | 1.72x | 2.57x |
| 256 KiB | SET | 9.372 | 3.811 | 3.621 | 1.05x | 2.46x |
| 256 KiB | 80/20 | 10.639 | 3.983 | 2.608 | 1.53x | 2.67x |
| 1 MiB | GET | 40.107 | 6.944 | 2.642 | 2.63x | 5.78x |
| 1 MiB | SET | 19.601 | 6.112 | 4.206 | 1.45x | 3.21x |
| 1 MiB | 80/20 | 20.603 | 6.067 | 2.685 | 2.26x | 3.40x |

## LMCache Sweep

| Value | Mix | Clients | GB/s | vCPU |
| --- | --- | ---: | ---: | ---: |
| 64 KiB | GET | 16 | 4.686 | 1.375 |
| 64 KiB | GET | 32 | 4.894 | 1.398 |
| 64 KiB | GET | 64 | 4.855 | 1.417 |
| 64 KiB | SET | 16 | 3.743 | 1.329 |
| 64 KiB | SET | 32 | 3.587 | 1.348 |
| 64 KiB | SET | 64 | 3.654 | 1.356 |
| 64 KiB | 80/20 | 16 | 3.936 | 1.346 |
| 64 KiB | 80/20 | 32 | 3.885 | 1.370 |
| 64 KiB | 80/20 | 64 | 3.711 | 1.375 |
| 256 KiB | GET | 16 | 11.291 | 1.619 |
| 256 KiB | GET | 32 | 10.579 | 1.579 |
| 256 KiB | GET | 64 | 10.359 | 1.614 |
| 256 KiB | SET | 16 | 9.372 | 1.691 |
| 256 KiB | SET | 32 | 8.658 | 1.657 |
| 256 KiB | SET | 64 | 8.617 | 1.656 |
| 256 KiB | 80/20 | 16 | 10.639 | 1.712 |
| 256 KiB | 80/20 | 32 | 9.648 | 1.685 |
| 256 KiB | 80/20 | 64 | 9.541 | 1.721 |
| 1 MiB | GET | 16 | 40.107 | 4.723 |
| 1 MiB | GET | 32 | 26.557 | 5.967 |
| 1 MiB | GET | 64 | 16.166 | 8.933 |
| 1 MiB | SET | 16 | 19.601 | 10.039 |
| 1 MiB | SET | 32 | 19.550 | 6.294 |
| 1 MiB | SET | 64 | 19.256 | 6.410 |
| 1 MiB | 80/20 | 16 | 20.603 | 9.196 |
| 1 MiB | 80/20 | 32 | 17.586 | 7.820 |
| 1 MiB | 80/20 | 64 | 15.464 | 8.185 |

## Redis Sweep

| Value | Mix | Pipeline | GB/s | Redis vCPU |
| --- | --- | ---: | ---: | ---: |
| 64 KiB | GET | 1 | 1.885 | 1.000 |
| 64 KiB | GET | 16 | 2.224 | 0.999 |
| 64 KiB | GET | 64 | 1.629 | 0.999 |
| 64 KiB | SET | 1 | 1.947 | 0.996 |
| 64 KiB | SET | 16 | 2.201 | 0.996 |
| 64 KiB | SET | 64 | 1.854 | 0.998 |
| 64 KiB | 80/20 | 1 | 1.786 | 0.993 |
| 64 KiB | 80/20 | 16 | 2.351 | 1.001 |
| 64 KiB | 80/20 | 64 | 2.265 | 1.001 |
| 256 KiB | GET | 1 | 2.549 | 1.000 |
| 256 KiB | GET | 16 | 1.732 | 1.002 |
| 256 KiB | GET | 64 | 1.317 | 1.001 |
| 256 KiB | SET | 1 | 3.621 | 0.998 |
| 256 KiB | SET | 16 | 3.018 | 0.999 |
| 256 KiB | SET | 64 | 2.668 | 0.997 |
| 256 KiB | 80/20 | 1 | 2.608 | 1.001 |
| 256 KiB | 80/20 | 16 | 2.577 | 1.000 |
| 256 KiB | 80/20 | 64 | 2.485 | 1.000 |
| 1 MiB | GET | 1 | 2.642 | 1.002 |
| 1 MiB | GET | 16 | 1.890 | 1.001 |
| 1 MiB | GET | 64 | 1.121 | 1.002 |
| 1 MiB | SET | 1 | 4.206 | 0.998 |
| 1 MiB | SET | 16 | 3.198 | 0.997 |
| 1 MiB | SET | 64 | 3.034 | 0.996 |
| 1 MiB | 80/20 | 1 | 2.685 | 1.001 |
| 1 MiB | 80/20 | 16 | 2.682 | 1.002 |
| 1 MiB | 80/20 | 64 | 2.552 | 1.004 |

## LMCache SCNP/TCP Sweep

| Value | Mix | Clients | GB/s | Client vCPU | Server vCPU |
| --- | --- | ---: | ---: | ---: | ---: |
| 64 KiB | GET | 16 | 1.194 | 1.961 | 0.627 |
| 64 KiB | GET | 32 | 1.167 | 1.981 | 0.622 |
| 64 KiB | GET | 64 | 1.144 | 1.973 | 0.606 |
| 64 KiB | SET | 16 | 1.228 | 1.988 | 0.480 |
| 64 KiB | SET | 32 | 1.172 | 1.998 | 0.490 |
| 64 KiB | SET | 64 | 1.154 | 1.996 | 0.509 |
| 64 KiB | 80/20 | 16 | 1.218 | 1.954 | 0.603 |
| 64 KiB | 80/20 | 32 | 1.136 | 1.968 | 0.596 |
| 64 KiB | 80/20 | 64 | 1.125 | 1.969 | 0.590 |
| 256 KiB | GET | 16 | 4.388 | 2.143 | 1.405 |
| 256 KiB | GET | 32 | 4.067 | 2.223 | 1.344 |
| 256 KiB | GET | 64 | 3.932 | 2.328 | 1.376 |
| 256 KiB | SET | 16 | 3.296 | 2.282 | 0.838 |
| 256 KiB | SET | 32 | 3.170 | 2.259 | 0.844 |
| 256 KiB | SET | 64 | 2.826 | 2.156 | 0.855 |
| 256 KiB | 80/20 | 16 | 3.983 | 2.166 | 1.271 |
| 256 KiB | 80/20 | 32 | 3.794 | 2.219 | 1.255 |
| 256 KiB | 80/20 | 64 | 3.599 | 2.318 | 1.284 |
| 1 MiB | GET | 16 | 5.809 | 4.955 | 4.393 |
| 1 MiB | GET | 32 | 5.045 | 6.204 | 5.213 |
| 1 MiB | GET | 64 | 4.659 | 6.793 | 5.394 |
| 1 MiB | SET | 16 | 4.724 | 3.176 | 1.393 |
| 1 MiB | SET | 32 | 3.506 | 2.939 | 1.267 |
| 1 MiB | SET | 64 | 3.261 | 2.952 | 1.179 |
| 1 MiB | 80/20 | 16 | 5.753 | 4.222 | 3.388 |
| 1 MiB | 80/20 | 32 | 4.695 | 4.584 | 3.433 |
| 1 MiB | 80/20 | 64 | 4.401 | 4.666 | 3.153 |

## SCNP/TCP Copy-Cut Rerun

After the initial SCNP/TCP sweep, the Python adapter was updated to use
scatter/gather writes for SET and preallocated receive buffers for GET. The
targeted rerun below uses the same server server shape with 16 clients.

| Value | Mix | GB/s | Client vCPU | Server vCPU | Previous GB/s |
| --- | --- | ---: | ---: | ---: | ---: |
| 256 KiB | GET | 4.026 | 1.790 | 1.086 | 4.388 |
| 256 KiB | SET | 3.811 | 2.256 | 0.840 | 3.296 |
| 256 KiB | 80/20 | 3.919 | 1.864 | 1.045 | 3.983 |
| 1 MiB | GET | 6.944 | 2.578 | 2.189 | 5.809 |
| 1 MiB | SET | 5.929 | 8.784 | 3.458 | 4.724 |
| 1 MiB | 80/20 | 5.453 | 3.036 | 2.003 | 5.753 |

## SCNP/TCP Hash-Frame And Pipeline Rerun

The SCNP/TCP adapter now sends `FAST_FLAG_KEY_HASH` frames, using the same Rust
XXH3 key hash exported by the `shardcache` Python extension. This lets the
server stay on the command-owned SCNP path instead of decoding generic frames.
The same run also swept `--op-batch-size`; batch size `1` remained the best
aggregate shape for 1 MiB values.

| Value | Mix | Clients | Op batch | GB/s | Client vCPU | Server vCPU |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 MiB | GET | 1 | 1 | 2.870 | 0.471 | 0.520 |
| 1 MiB | SET | 1 | 4 | 3.058 | 0.950 | 0.411 |
| 1 MiB | 80/20 | 1 | 1 | 2.878 | 0.558 | 0.483 |
| 1 MiB | GET | 16 | 1 | 6.606 | 2.522 | 2.090 |
| 1 MiB | SET | 16 | 1 | 6.112 | 9.332 | 3.938 |
| 1 MiB | 80/20 | 16 | 1 | 5.983 | 3.015 | 2.142 |
| 1 MiB | GET | 16 | 4 | 4.539 | 2.405 | 1.714 |
| 1 MiB | SET | 16 | 4 | 5.238 | 9.956 | 3.993 |
| 1 MiB | 80/20 | 16 | 4 | 4.665 | 3.007 | 1.894 |

## Local Raw SCNP GB/s Ceiling Probe

Local Apple M5 Max raw SCNP runs were added to separate the SCNP transport
ceiling from LMCache plugin/Python object overhead. The server was started in
direct mode with 16 shards, a fanout listener at `127.0.0.1:6500`, and direct
shard ports at `127.0.0.1:6501-6516`. The `fc-server-scnp-direct` backend
routes workers to the direct shard ports and filters each worker to keys owned
by that shard.

Raw CSVs are stored under
`benchmarks/reference/lmcache-m5-gbps-limit-20260526/`.

| Route | Clients | Pipeline | Mix | GB/s | Server vCPU |
| --- | ---: | ---: | --- | ---: | ---: |
| Fanout | 32 | 1 | GET | 13.322 | 1.606 |
| Fanout | 32 | 1 | SET | 14.094 | 2.121 |
| Fanout | 64 | 1 | GET | 11.560 | 1.361 |
| Fanout | 64 | 1 | SET | 12.176 | 1.674 |
| Direct shard | 32 | 1 | GET | 13.146 | 1.598 |
| Direct shard | 32 | 1 | SET | 13.972 | 2.158 |
| Direct shard | 64 | 1 | GET | 11.042 | 1.319 |
| Direct shard | 64 | 1 | SET | 12.039 | 1.668 |

The broader no-server-CPU sweep observed a best 1 MiB fanout row of
`15.623 GB/s` for SET at 32 clients and a best direct-shard row of
`15.431 GB/s` for SET at 32 clients. Direct shard routing is therefore working,
but it did not raise the local large-value loopback ceiling. Pipeline depths
above `1` generally reduced 1 MiB throughput, which points to byte movement
through loopback/TCP and user-space buffers rather than fanout dispatch as the
main limit. Values above 4 MiB need the server request handoff cap raised for
the run, for example `SHARDCACHE_HANDOFF_BUFFER_BYTES=16777216` or
`SCNP_HANDOFF_BUFFER_BYTES=16777216`, because the default cap remains 4 MiB.

## SCNP/TCP Linux Perf Profile

Linux `perf` was run on server against the 1 MiB, 16-client, no-batch SCNP/TCP
path. Kernel perf restrictions were temporarily lowered for symbolized reports
and then restored. Reports are stored under
`benchmarks/results/lmcache-scnp-tcp-perf-kernel-20260514223146/`.

| Side | Mix | Main hot spots |
| --- | --- | --- |
| Client | GET | `rep_movs_alternative` under `tcp_recvmsg_locked` / `recv` at 18.69%; Python eval at 5.15% |
| Client | SET | `clear_page_rep` under `tcp_sendmsg_locked` / `sendmsg` at 21.72%; `rep_movs_alternative` under TCP send copy at 10.44% |
| Server | GET | `clear_page_rep` under `tcp_sendmsg_locked` / io_uring send at 25.43%; TCP send copy at 8.14% |
| Server | SET | `rep_movs_alternative` under `tcp_recvmsg_locked` / io_uring recv at 16.16%; libc AVX copy loop around the received frame/store path dominated the remaining user-space samples |

## Takeaways

- The meaningful LMCache ceiling appears at large values: 1 MiB GET reached
  40.107 GB/s, while 1 MiB SET and 80/20 landed around 20 GB/s.
- For 1 MiB payloads, 16 clients was the best LMCache shape. Adding clients
  reduced GET bandwidth sharply, which points to memory movement and plugin
  contention rather than insufficient client parallelism.
- Redis stayed pinned to roughly one server vCPU. Pipelining helped at 64 KiB,
  but for 256 KiB and 1 MiB payloads, pipeline depth 1 was the best Redis shape.
- The first SCNP/TCP LMCache adapter is viable for remote sharing and beats
  Redis for 256 KiB GET, 256 KiB SET, 256 KiB 80/20, and all 1 MiB mixes. It
  trails Redis at 64 KiB because the adapter is one request per Python socket
  round trip.
- Cutting Python socket copies moved the TCP ceiling, most clearly at 1 MiB:
  GET improved from 5.809 to 6.944 GB/s and SET improved from 4.724 to
  5.929 GB/s. Sending prehashed SCNP frames lifted the best SET row again to
  6.112 GB/s.
- Same-operation SCNP/TCP batches did not raise the 1 MiB aggregate ceiling;
  batch size `1` was still best for 16 clients. The bottleneck is therefore not
  simply request/response latency.
- Raw SCNP direct-shard routing is implemented and benchmarked. On the local M5
  Max loopback run it matched fanout for large 1 MiB payloads but did not exceed
  it, so direct shard routing is more likely to help small-operation routing and
  client-side shard affinity than to unlock more single-host TCP GB/s.
- `perf` shows the current TCP ceiling is dominated by kernel TCP page-frag
  allocation and copy paths (`clear_page_rep`, `rep_movs_alternative`) plus
  Python/user-space reconstruction. The next large TCP step likely needs a
  Rust/PyO3 SCNP client and/or Linux zero-copy send experiments, not more
  Python-level batching.
- SCNP/TCP is still roughly 3.2x to 5.8x behind embedded LMCache for 1 MiB
  payloads depending on mix. The next TCP optimization targets are direct shard
  routing from the Python adapter, a Rust/PyO3 SCNP client to reduce Python
  socket/object overhead, and Linux zero-copy send experiments for large values.
- The next embedded LMCache optimization target remains the Python/plugin/shared
  store data movement path for 1 MiB blocks, especially GET reconstruction and
  SET serialization.
