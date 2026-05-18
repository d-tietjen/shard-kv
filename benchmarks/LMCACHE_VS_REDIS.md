# Fast-Cache LMCache Backend vs Redis TCP: Bandwidth Saturation

Standalone benchmark run on Linux on May 14, 2026.

This report is framed around GB/s, not operation rate. For LMCache-sized KV
blocks, the important question is how much payload bandwidth the integration can
move before the hardware, Python plugin layer, or Redis server path becomes the
bottleneck.

The surfaces are different: LMCache is measured through the fast-cache LMCache
storage plugin API, either in-process or over fast-cache's native FCNP/TCP
protocol. Redis uses RESP/TCP over loopback. Treat this as a practical
integration comparison, not a pure engine microbenchmark.

## Setup

| Setting | Value |
| --- | --- |
| Host | Ubuntu 24.04.4 LTS |
| fast-cache path | `fast_cache_lmcache_backend.FastCacheStorageBackend` |
| LMCache | `lmcache 0.4.4`, Python non-CUDA backend fallback |
| LMCache embedded architecture | `client_architecture=shared` |
| LMCache TCP architecture | `client_architecture=fcnp_tcp`, generic FCNP listener at `127.0.0.1:6500` |
| Redis | `redis:7-alpine`, Docker, `--cpuset-cpus=0-15` |
| Redis protocol | RESP/TCP on `127.0.0.1:6379` |
| Clients | LMCache embedded and FCNP/TCP swept `16`, `32`, `64`; Redis used `16` |
| Redis pipeline depth | swept `1`, `16`, `64` |
| Key count | `1024`, uniform |
| Warmup / measure | `2s` warmup, `6s` measured |
| Value sizes | `64 KiB`, `256 KiB`, `1 MiB` |
| Latency timing | disabled with `--latency-sample-rate 0` |

CPU reporting differs by harness. Embedded LMCache rows report CPU used by the
Python benchmark process. LMCache FCNP/TCP rows report Python client CPU and a
separate sampled fast-cache-server CPU. Redis rows report CPU used by the Redis
server process only; Redis client CPU is not included.

LMCache's built-in `LocalCPUBackend` was not included because LMCache 0.4.4
requires allocator or metadata state for that backend constructor.

## Peak Rows

| Backend | Workload | GB/s | Shape | vCPU |
| --- | --- | ---: | --- | ---: |
| LMCache embedded plugin | 1 MiB GET | 40.107 | 16 clients | 4.723 client |
| LMCache embedded plugin | 1 MiB SET | 19.601 | 16 clients | 10.039 client |
| LMCache embedded plugin | 1 MiB 80/20 | 20.603 | 16 clients | 9.196 client |
| LMCache FCNP/TCP plugin | 1 MiB GET | 6.944 | 16 clients | 2.578 client, 2.189 server |
| LMCache FCNP/TCP plugin | 1 MiB SET | 6.112 | 16 clients | 9.332 client, 3.938 server |
| LMCache FCNP/TCP plugin | 1 MiB 80/20 | 6.067 | 16 clients | 3.033 client, 2.187 server |
| Redis TCP | 1 MiB SET | 4.206 | 16 clients, pipeline 1 | 0.998 |
| Redis TCP | 1 MiB 80/20 | 2.685 | 16 clients, pipeline 1 | 1.001 |
| Redis TCP | 256 KiB SET | 3.621 | 16 clients, pipeline 1 | 0.998 |

## Best By Value Size

Each row selects the best embedded LMCache client count, best FCNP/TCP LMCache
client count, and best Redis pipeline depth for that value size and mix.

| Value | Mix | Embedded GB/s | FCNP/TCP GB/s | Redis GB/s | FCNP/TCP vs Redis | Embedded vs FCNP/TCP |
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

## LMCache FCNP/TCP Sweep

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

## FCNP/TCP Copy-Cut Rerun

After the initial FCNP/TCP sweep, the Python adapter was updated to use
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

## FCNP/TCP Hash-Frame And Pipeline Rerun

The FCNP/TCP adapter now sends `FAST_FLAG_KEY_HASH` frames, using the same Rust
XXH3 key hash exported by the `fast_cache` Python extension. This lets the
server stay on the command-owned FCNP path instead of decoding generic frames.
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

## FCNP/TCP Linux Perf Profile

Linux `perf` was run on server against the 1 MiB, 16-client, no-batch FCNP/TCP
path. Kernel perf restrictions were temporarily lowered for symbolized reports
and then restored. Reports are stored under
`benchmarks/results/lmcache-fcnp-tcp-perf-kernel-20260514223146/`.

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
- The first FCNP/TCP LMCache adapter is viable for remote sharing and beats
  Redis for 256 KiB GET, 256 KiB SET, 256 KiB 80/20, and all 1 MiB mixes. It
  trails Redis at 64 KiB because the adapter is one request per Python socket
  round trip.
- Cutting Python socket copies moved the TCP ceiling, most clearly at 1 MiB:
  GET improved from 5.809 to 6.944 GB/s and SET improved from 4.724 to
  5.929 GB/s. Sending prehashed FCNP frames lifted the best SET row again to
  6.112 GB/s.
- Same-operation FCNP/TCP batches did not raise the 1 MiB aggregate ceiling;
  batch size `1` was still best for 16 clients. The bottleneck is therefore not
  simply request/response latency.
- `perf` shows the current TCP ceiling is dominated by kernel TCP page-frag
  allocation and copy paths (`clear_page_rep`, `rep_movs_alternative`) plus
  Python/user-space reconstruction. The next large TCP step likely needs a
  Rust/PyO3 FCNP client and/or Linux zero-copy send experiments, not more
  Python-level batching.
- FCNP/TCP is still roughly 3.2x to 5.8x behind embedded LMCache for 1 MiB
  payloads depending on mix. The next TCP optimization targets are direct shard
  routing from the Python adapter, a Rust/PyO3 FCNP client to reduce Python
  socket/object overhead, and Linux zero-copy send experiments for large values.
- The next embedded LMCache optimization target remains the Python/plugin/shared
  store data movement path for 1 MiB blocks, especially GET reconstruction and
  SET serialization.
