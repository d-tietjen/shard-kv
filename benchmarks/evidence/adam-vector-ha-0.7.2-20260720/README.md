# Adam Vector HA 0.7.2

Matched release-candidate A/B benchmark for the vector read-replica path. The
source tree represented by this evidence contains the benchmark harness and HA
implementation. Raw command output for all six runs is stored beside this
file.

## Workload

- One ShardMap server vCPU and one client vCPU, no pipelining.
- One server shard with a 128 MiB memory budget.
- 1,024 vectors, 16 FP32 dimensions, and a pool of 64 queries.
- `VSIM COUNT 10 WITHSCORES WITHATTRIBS EF 64` over typed SCNP fanout.
- Two-second warmup and ten-second measured interval.
- Three sequential runs per mode; the table reports medians.
- Baseline and HA used the same final release binary.

The HA mode constructed `ReplicatedEmbeddedStore`, served its shared
`EmbeddedStore`, and kept the FCRP listener, vector observer, bounded
coalescing queue, and background exporter machinery active. No follower was
connected during this read-only VSIM phase, so the result isolates the primary
read-path cost of enabling HA.

## Results

| Mode | Run ops/sec | Median ops/sec | Median p99 us | Throughput retained |
| --- | --- | ---: | ---: | ---: |
| Baseline | 14,903 / 14,583 / 14,791 | 14,791 | 81.98 | 100.0% |
| Vector HA enabled | 14,521 / 14,452 / 14,662 | 14,521 | 83.01 | 98.2% |

Vector HA reduced median throughput by 1.8% and increased median p99 by 1.2%
in this single-core network test. Every request returned ten matches and no
run reported an error.

## Environment

- Host: `adam`
- CPU: AMD Ryzen 9 3950X, 16 physical cores and 32 threads, one NUMA node
- Server pinned to CPU 0; client pinned to CPU 1
- Release `embedded_server_harness` and `scnp_vector_client_cost` binaries
- Adam was not otherwise idle, so unrelated host load can add variance

## Reproduction

Baseline server:

```bash
taskset -c 0 target/release/embedded_server_harness \
  --bind-addr 127.0.0.1:26488 --shard-count 1 \
  --max-memory-bytes 134217728
```

HA-enabled server:

```bash
taskset -c 0 target/release/embedded_server_harness \
  --bind-addr 127.0.0.1:26487 --shard-count 1 \
  --max-memory-bytes 134217728 \
  --replication-bind-addr 127.0.0.1:27487
```

Client command, run three times against each SCNP address:

```bash
taskset -c 1 target/release/scnp_vector_client_cost \
  --addr 127.0.0.1:26488 --transport fanout --shard-count 1 \
  --workers 1 --entries 1024 --dims 16 --query-pool 64 \
  --count 10 --ef-search 64 --warmup-seconds 2 --duration-seconds 10
```

Vector replication correctness is covered separately by live FCRP apply,
snapshot bootstrap, TTL/delete, bounded coalescing, and type-transition tests.
The supported HA model is one writable vector primary with externally fenced
promotion after replica catch-up. Active-active vector writes are not supported
in 0.7.2.
