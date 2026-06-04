# Embedded Typed And Codec Benchmarks

This document records the 16-vCPU max-out run for the new embedded typed API
and the `codec` feature. It compares:

- `fc-typed`: native `ShardMap<K, V>` object storage.
- `fc-codec`: typed facades over the shared byte engine without a namespace.
- `fc-codec-ns`: one typed namespace over the shared byte engine.
- `fc-codec-multi-ns`: four typed namespaces over the same shared byte engine.
- `dashmap`: the embedded Rust map baseline.
- `fc-embed`: the owner-local shardcache embedded baseline from the original
  head-to-head suite.

The pure GET rows use reference access. They measure how fast each backend can
find and hand out a borrowed value/reference; they do not copy the configured
payload size on every operation. SET rows write the configured payload and are
therefore expected to converge toward memory-copy throughput at larger sizes.
For pure SET rows, backend suffixes such as `-ref` only describe the read mode
configured for that backend; the write path is the same.

## Reproduce The Run

```bash
./benchmarks/scripts/run-embedded-benchmark-suite.sh \
  --suite embedded-core \
  --backends fc-shared,fc-typed-ref,fc-typed,fc-codec-ref,fc-codec-ns-ref,fc-codec-multi-ns-ref,fc-codec,fc-codec-ns,fc-codec-multi-ns,dashmap-ref,dashmap \
  --vcpus 16 \
  --clients match-vcpus \
  --value-sizes 64,1024,4096,16384,65536,262144 \
  --mixes 100-0,0-100 \
  --warmup 2 \
  --duration 10 \
  --latency-sample-rate 1 \
  --key-memory-cap-bytes 2147483648 \
  --large-value-key-floor 64
```

The companion owner-local baseline used the same command with
`--backends fc-embed`.

## Run Settings

| Setting | Value |
| --- | --- |
| Host | Benchmark server, Ubuntu 24.04, 32 logical CPUs |
| Suite | `embedded-core` |
| vCPU | 16 |
| CPU set | Linux CPUs `0-15` |
| Clients | `match-vcpus`, 16 clients |
| Pipeline depth | 1 |
| Value sizes | 64 B, 1 KiB, 4 KiB, 16 KiB, 64 KiB, 256 KiB |
| Mixes | `100-0`, `0-100` |
| Warmup | 2 seconds |
| Timed duration | 10 seconds |
| Read mode | `ref` |
| Key distribution | uniform |

Saved local bundles:

- `benchmarks/results/server-embedded-typed-codec-dashmap-pinned-20260603T203115Z/`
- `benchmarks/results/server-embedded-fc-embed-baseline-pinned-20260603T205945Z/`

## Head-To-Head Summary

| Workload | Best new shardmap backend | Ops/sec | p99 us | Best DashMap | Ops/sec | p99 us | Owner-local `fc-embed` ops/sec | Owner-local p99 us |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| GET ref 64 B | `fc-codec-ref` | 48,016,443 | 0.640 | `dashmap-ref` | 44,192,909 | 0.670 | 133,251,354 | 0.170 |
| GET ref 1 KiB | `fc-codec-ref` | 46,702,990 | 0.650 | `dashmap-ref` | 42,695,026 | 0.690 | 124,479,728 | 0.180 |
| GET ref 4 KiB | `fc-codec-ref` | 45,562,169 | 0.670 | `dashmap-ref` | 41,726,327 | 0.700 | 121,016,207 | 0.210 |
| GET ref 16 KiB | `fc-codec-ref` | 40,710,300 | 0.760 | `dashmap-ref` | 38,086,917 | 0.790 | 118,467,204 | 0.240 |
| GET ref 64 KiB | `fc-codec-ref` | 64,623,274 | 0.490 | `dashmap-ref` | 54,656,433 | 0.550 | 155,255,217 | 0.090 |
| GET ref 256 KiB | `fc-codec-ref` | 83,096,812 | 0.300 | `dashmap-ref` | 61,933,716 | 0.480 | 171,377,646 | 0.070 |
| SET 64 B | `fc-typed-ref` | 25,629,129 | 1.7 | `dashmap-ref` | 30,332,477 | 1.5 | 84,960,012 | 0.260 |
| SET 1 KiB | `fc-typed-ref` | 14,904,244 | 2.4 | `dashmap-ref` | 16,279,997 | 2.1 | 18,926,335 | 1.9 |
| SET 4 KiB | `fc-typed` | 3,772,367 | 15.2 | `dashmap-ref` | 3,768,080 | 15.7 | 4,026,662 | 7.1 |
| SET 16 KiB | `fc-typed` | 1,033,069 | 36.6 | `dashmap` | 1,038,575 | 36.6 | 1,072,303 | 37.3 |
| SET 64 KiB | `fc-codec-ns` | 277,492 | 98.3 | `dashmap` | 279,640 | 96.8 | 281,124 | 94.1 |
| SET 256 KiB | `fc-typed-ref` | 70,455 | 292.4 | `dashmap` | 70,583 | 290.8 | 70,901 | 289.3 |

## Takeaways

- The new codec reference path beat DashMap reference access for every tested
  pure GET row, with lower p99 latency in every row.
- The native typed API fixed the large-value reference-access gap in the older
  shared byte-engine path. For example, 64 KiB GET reference access was
  40,678,970 ops/sec for `fc-typed-ref` versus 622,421 ops/sec for
  `fc-shared`.
- Small writes are still where DashMap is strongest: DashMap was 18.4% faster
  than the best new shardmap row at 64 B SET and 9.2% faster at 1 KiB SET.
- Large writes converge because the benchmark becomes payload-copy bound.
  At 16 KiB, 64 KiB, and 256 KiB SET, DashMap's throughput edge over the best
  new shardmap row was 0.5%, 0.8%, and 0.2% respectively.
- The owner-local `fc-embed` path remains the highest-throughput embedded
  shardcache mode. The typed and codec APIs are the user-facing shared/facade
  surfaces, while `fc-embed` is the specialized owner-local baseline.
