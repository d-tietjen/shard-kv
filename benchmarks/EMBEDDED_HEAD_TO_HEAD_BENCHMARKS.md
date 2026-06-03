# Embedded Head-To-Head Benchmarks

This document records focused in-process cache comparisons for shardcache's
embedded paths. Unlike the Docker server suite, this run has no TCP, RESP, SCNP,
Docker, or external database server overhead. It compares the embedded
owner-local path against shared-handle shardcache and common Rust cache
baselines with the same generated workload.

This is an embedded reference-access benchmark. The pure GET rows use
`read_mode=ref`, so they measure how quickly each backend can find and hand out
a reference to the stored value. They do not copy the stored value out of memory
on every GET. That matters most for the larger value sizes: the 64 KiB and 256
KiB GET rows are pointer/reference lookup rows, not claims that the benchmark is
copying those bytes per operation.

## Reproduce The Value-Size Pass

```bash
RUN_ID=adam-embedded-getset-size-pinned-$(date -u +%Y%m%dT%H%M%SZ) \
./benchmarks/scripts/run-embedded-benchmark-suite.sh \
  --suite embedded-core \
  --backends fc-embed,fc-shared,dashmap,dashmap-worker-shards,dashmap-ref,moka,rwlock-hashmap,lru \
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

The `100-0` rows are pure GET/read rows. The `0-100` rows are pure SET/write
rows. This pass uses `read_mode=ref`, so GET rows measure embedded lookup and
reference access; they are not copy-out payload bandwidth claims. Use the
`embedded-copy` suite when comparing materialized read/copy-out behavior.

## Adam 16-vCPU Embedded Value-Size Pass

Run bundle:
`benchmarks/reference/adam-embedded-getset-size-pinned-20260603T004901Z/`

Remote source:
`/home/dtietjen/shard-kv-bench-redis-cluster.TnRIzc/benchmarks/results/adam-embedded-getset-size-pinned-20260603T004901Z`

Run settings:

| Setting | Value |
| --- | --- |
| Host | Adam, Ubuntu 24.04, 32 logical CPUs |
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
| Key memory cap | 2147483648 bytes |
| Git SHA | `4651dedf84ba9fd69eabd92c1867b77e891f2e85` |

## Pure GET Reference Results

These GET rows are pointer/reference lookups. They show the embedded
read/lookup ceiling for resident values of each configured size, but they do
not copy the configured value size out of memory on each operation.

| Size | fc-embed ops/sec | fc-embed p99 us | fc-shared ops/sec | fc-shared p99 us | Best competitor | Best competitor ops/sec | Best competitor p99 us |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 64 B | 134,596,497 | 0.150 | 46,508,233 | 0.650 | `dashmap-ref` | 44,657,103 | 0.650 |
| 1 KiB | 126,260,206 | 0.160 | 24,803,982 | 1.120 | `dashmap-ref` | 43,010,524 | 0.670 |
| 4 KiB | 123,223,830 | 0.170 | 8,846,622 | 2.851 | `dashmap-ref` | 42,421,808 | 0.680 |
| 16 KiB | 119,972,127 | 0.230 | 2,466,473 | 9.455 | `dashmap-ref` | 38,743,850 | 0.760 |
| 64 KiB | 155,460,791 | 0.090 | 638,279 | 33.695 | `dashmap-ref` | 57,241,333 | 0.510 |
| 256 KiB | 171,552,823 | 0.070 | 162,442 | 132.351 | `dashmap-ref` | 62,376,668 | 0.480 |

## Pure SET Results

These SET rows write the configured value payload, so large values are expected
to converge toward memory-copy behavior.

| Size | fc-embed ops/sec | fc-embed p99 us | fc-shared ops/sec | fc-shared p99 us | Best competitor | Best competitor ops/sec | Best competitor p99 us |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 64 B | 86,364,003 | 0.230 | 5,059,052 | 25.439 | `dashmap-ref` | 30,688,876 | 1.440 |
| 1 KiB | 20,360,886 | 1.700 | 4,608,805 | 26.271 | `dashmap` | 16,630,074 | 2.061 |
| 4 KiB | 4,171,710 | 6.443 | 3,683,325 | 22.655 | `dashmap-ref` | 3,831,373 | 15.511 |
| 16 KiB | 1,095,700 | 37.279 | 1,052,861 | 56.863 | `dashmap-worker-shards` | 1,061,075 | 34.911 |
| 64 KiB | 282,754 | 93.631 | 276,543 | 216.831 | `dashmap` | 280,769 | 95.551 |
| 256 KiB | 71,833 | 280.319 | 70,071 | 749.055 | `dashmap-worker-shards` | 70,974 | 289.279 |

## Takeaways

- `fc-embed` was the highest-throughput backend in every pure GET reference
  lookup row and every pure SET row in this 16-vCPU pass.
- For pure GET/reference-read rows, `fc-embed` was also the lowest-p99 backend
  across all tested value sizes. These rows are a pointer/reference-access
  benchmark, not a copy-out benchmark. The strongest competitor was
  `dashmap-ref`.
- For pure SET/write rows, large values converge because the benchmark becomes
  dominated by copying the value payload. `fc-embed` stayed slightly ahead on
  ops/sec at every tested size, but the gap narrowed substantially at 16 KiB and
  larger.
- The only table row where the best competitor had lower p99 than `fc-embed`
  was 16 KiB pure SET: `dashmap-worker-shards` reported 34.911 us p99 versus
  `fc-embed` at 37.279 us, while `fc-embed` still had higher ops/sec.
- `fc-shared` represents the shared embedded handle path, not the owner-local
  embedded path. It is useful as a topology comparison, but it is not the
  fastest embedded mode.

## Saved Artifacts

The checked-in bundle includes:

- `metadata.json`: run settings and benchmark metadata.
- `report.md`: generated suite report.
- `embedded-core.csv`: raw embedded benchmark rows for all backends, value
  sizes, and GET/SET mixes.
