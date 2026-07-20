# Adam SCNP Vector Client 0.7.2

Release benchmark for the typed Object RAG SCNP client at commit
`c91cb1af50754629fb8070f27bb9d813a46fccdc` (the short commit is `c91cb1a`).
Raw CSVs for every run are stored beside this file.

## Workload

- One client and one server vCPU, no pipelining.
- 1,024 vectors per set, 16 FP32 dimensions, and a pool of 64 queries.
- `VSIM COUNT 10 WITHSCORES WITHATTRIBS EF 64`.
- Two-second warmup and ten-second measured interval.
- Three isolated runs per mode; the table reports medians.
- Redis and the ShardMap server were pinned to CPU 0. The network client was
  pinned to CPU 1. Embedded ShardMap ran entirely on CPU 0.
- Every run used a 128 MiB memory budget and reported zero unexpected errors.

## Results

| Mode | Median ops/sec | Median p99 us | Versus Redis |
| --- | ---: | ---: | ---: |
| Redis 8.0 RESP | 4,636 | 342.01 | 1.00x |
| ShardMap RESP | 13,790 | 88.51 | 2.98x |
| ShardMap SCNP fanout, protocol driver | 14,436 | 79.55 | 3.11x |
| ShardMap SCNP direct, protocol driver | 18,140 | 64.86 | 3.91x |
| Typed SCNP fanout | 14,707 | 81.02 | 3.17x |
| Typed SCNP direct | 18,761 | 64.51 | 4.05x |
| Embedded ShardMap | 83,607 | 16.98 | 18.03x |

The protocol driver consumes and validates SCNP frames but does not construct
typed matches. The typed driver performs the production FP32 encoding and
native response decoding and constructs owned `VSimMatch` values. Its fanout
and direct medians are within run variance of the protocol-only paths, so the
0.7.2 typed API does not introduce a measurable throughput regression here.

## Environment

- Host: `adam`
- CPU: AMD Ryzen 9 3950X, 16 physical cores and 32 threads, one NUMA node
- Redis image: `redis:8.0-alpine`
- Redis image digest:
  `redis@sha256:5f61955be8ab2ccee9372b84ae4d4da2e2b156f87281e3f218544055e7ee04d4`
- ShardMap server: release `embedded_server_harness`, one shard, fanout plus
  direct-shard listener
- Adam was not an otherwise idle machine. CPU pinning isolated the benchmark
  processes from each other, but unrelated system load can still add variance.

## Reproduction

The Redis-compatible rows used the shared command matrix with only
`VSIM typed object rag` selected. For example:

```bash
taskset -c 1 target/release/redis_command_matrix \
  --targets redis-8.0=127.0.0.1:26379 \
  --cases "VSIM typed object rag" \
  --clients 1 --key-shards 1 --pipeline-depth 1 \
  --warmup 2 --duration 10 --memory-budget-mib 128 \
  --fail-on-error
```

The typed rows used:

```bash
taskset -c 1 target/release/scnp_vector_client_cost \
  --addr 127.0.0.1:26484 --transport direct-shard --shard-count 1 \
  --workers 1 --entries 1024 --dimensions 16 --query-pool 64 \
  --count 10 --ef-search 64 --warmup 2 --duration 10
```

Embedded ShardMap used `redis_embedded_command_matrix` with one store shard,
the same case filter and durations, and the whole process pinned to CPU 0.
