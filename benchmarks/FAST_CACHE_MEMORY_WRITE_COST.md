# fast-cache Memory Bandwidth Ceiling

This benchmark isolates memory movement from cache lookup, eviction, protocol,
and routing work. Use it to set the local hardware ceiling before interpreting
large-value cache throughput.

The goal is to show how much of the machine's bandwidth fast-cache can expose.
Absolute GB/s is hardware-specific; the portable claim is the percentage of the
local ceiling reached by each cache path.

Use the ceiling mode that matches the cache path:

| Cache path | Ceiling mode | Meaning |
| --- | --- | --- |
| Borrowed or zero-copy GET | `read-scan` | Touch every byte without writing. |
| Apple Silicon read probe | `read-scan-neon` | Touch every byte with NEON vector loads. |
| SET or overwrite-heavy write path | `write-fill` | Overwrite value buffers without reading. |
| Materialized GET or copy-on-read path | copy modes | Copy payload bytes from source to destination. |

For copy/materialization rows, reported payload GB/s performs both one read and
one write for every payload byte. Compare the payload rate to the lower of the
pure read and pure write ceilings, and compare physical fabric traffic as
roughly `2x` payload bytes.

## Local Hardware Ceiling Example

Exploratory run on an Apple M5 Max, built with `cargo native-bench`, using the
native `memory_write_cost` binary and no competing benchmark jobs:

| Mode | Value size | Threads | GB/s |
| --- | ---: | ---: | ---: |
| `read-scan` | 512KiB | 6 | 298.09 |
| `read-scan-neon` | 4MiB | 6 | 296.69 |
| `read-scan` | 5MiB | 6 | 295.10 |
| `write-fill` | 512KiB | 6 | 259.10 |
| `write-fill` | 5MiB | 4 | 250.93 |

These rows mean a read-only KV path should be judged against roughly
`300 GB/s` on this host, while a write-only or copy-materialization payload path
should be judged against roughly `250-260 GB/s`. On larger memory systems, the
ceiling should move with the memory subsystem, NUMA topology, CPU pinning, and
the number of active memory controllers.

Reproduce a ceiling run:

```bash
cargo native-bench --bin memory_write_cost

target/release/memory_write_cost \
  --modes read-scan,read-scan-neon,write-fill \
  --value-sizes 524288,1048576,2097152,3145728,4194304,5242880 \
  --threads 1 \
  --pool-len 512 \
  --warmup-seconds 2 \
  --duration-seconds 8
```

Sweep `--threads` sequentially, not in parallel benchmark jobs, when finding the
machine maximum. On macOS, add `--macos-qos` to request
`QOS_CLASS_USER_INTERACTIVE` for benchmark worker threads.

## Server Run

| Field | Value |
| --- | --- |
| Host | Linux |
| CPU | AMD Ryzen 9 3950X 16-Core Processor |
| Command | `CPUSET=0 VALUE_SIZES=4096,16384,65536,1048576 ./benchmarks/scripts/run-memory-write-bench.sh` |
| Warmup | `1s` |
| Duration | `5s` per row |
| Pool length | `8` |
| Threads | `1` |
| CSV artifact | `/tmp/fast-cache-memory-write/benchmarks/results/memory_write_cost_20260518_000025/memory_write_cost.csv` |

## Server Materialization Results

| Size | Mode | GB/s | Ops/sec |
| ---: | --- | ---: | ---: |
| 4KiB | aligned-copy | 75.14 | 18.34M |
| 4KiB | slice-copy | 65.66 | 16.03M |
| 4KiB | bytes-reuse | 61.37 | 14.98M |
| 4KiB | bytes-copy | 49.39 | 12.06M |
| 4KiB | vec-bytes | 49.15 | 12.00M |
| 4KiB | nt-avx2 | 16.18 | 3.95M |
| 4KiB | nt-sse2 | 16.14 | 3.94M |
| 16KiB | bytes-reuse | 54.93 | 3.35M |
| 16KiB | slice-copy | 54.50 | 3.33M |
| 16KiB | vec-bytes | 51.58 | 3.15M |
| 16KiB | bytes-copy | 51.55 | 3.15M |
| 16KiB | aligned-copy | 51.70 | 3.16M |
| 16KiB | nt-avx2 | 22.27 | 1.36M |
| 16KiB | nt-sse2 | 22.23 | 1.36M |
| 64KiB | slice-copy | 50.78 | 774.9k |
| 64KiB | bytes-reuse | 50.17 | 765.5k |
| 64KiB | aligned-copy | 47.89 | 730.7k |
| 64KiB | bytes-copy | 46.74 | 713.2k |
| 64KiB | vec-bytes | 46.38 | 707.8k |
| 64KiB | nt-avx2 | 24.62 | 375.7k |
| 64KiB | nt-sse2 | 24.61 | 375.5k |
| 1MiB | bytes-reuse | 43.02 | 41.0k |
| 1MiB | aligned-copy | 41.17 | 39.3k |
| 1MiB | slice-copy | 40.91 | 39.0k |
| 1MiB | bytes-copy | 39.67 | 37.8k |
| 1MiB | vec-bytes | 38.65 | 36.9k |
| 1MiB | nt-sse2 | 25.39 | 24.2k |
| 1MiB | nt-avx2 | 25.28 | 24.1k |

## Linux Non-Temporal Threshold Control

This control forces glibc's non-temporal threshold to `64KiB`:

```bash
GLIBC_TUNABLES=glibc.cpu.x86_non_temporal_threshold=65536 \
  CPUSET=0 VALUE_SIZES=65536,1048576 \
  MODES=slice-copy,bytes-copy,vec-bytes,bytes-reuse,aligned-copy \
  ./benchmarks/scripts/run-memory-write-bench.sh
```

It reduced 1MiB reusable-copy throughput from roughly `43 GB/s` to roughly
`25 GB/s`, so forcing non-temporal behavior is not a good default for server.

## Takeaways

- Hardware ceilings should be recorded alongside large-value fast-cache runs.
  The fast-cache product claim is hardware-scaled bandwidth exposure, not one
  universal GB/s number.
- Manual non-temporal SSE2/AVX2 stores are slower than cached copies for this
  cache write path on server.
- The useful optimization is avoiding fresh allocation and reusing destination
  buffers when ownership is unique.
- Alignment helps at 4KiB but does not beat reusable `Bytes` at 64KiB and 1MiB.
- The remaining large-value SET ceiling is mostly memory bandwidth plus cache
  bookkeeping, not a missing SIMD copy primitive.
