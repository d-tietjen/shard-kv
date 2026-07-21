# Redis 8 Vector Command Benchmarks

This note covers the Redis 8 vector-set command suite registered as
`redis-v8-vector`. The suite exercises `VADD`, `VCARD`, `VDIM`, `VEMB`,
`VGETATTR`, `VINFO`, `VLINKS`, `VRANDMEMBER`, `VRANGE`, `VREM`, `VSETATTR`,
and `VSIM` through the shared command-matrix runner.

The complete Redis 8 suite is registered as `redis-v8` and also includes the
Redis 8 hash helpers `HGETDEL`, `HGETEX`, and `HSETEX`. Use `redis-v8` for full
coverage runs and `redis-v8-vector` for focused vector-only sweeps.

The `VSIM typed object rag` case is the release comparison for the 0.7.2
client. It searches a 1,024-entry, 16-dimensional FP32 vector set with
`COUNT 10 WITHSCORES WITHATTRIBS EF 64`. The command-matrix driver sends the
same binary vector and options to Redis, shardcache RESP, shared-port SCNP, and
direct-shard SCNP. This isolates server and transport cost while retaining the
response shape required by Object RAG.

## Server Summary

The current server vector sweeps used the standardized server benchmark runner
with 1 client, pipeline depth 1, precomposed command plans, and zero unexpected
command errors. Raw bundles remain ignored under `benchmarks/results`; use
these bundle IDs to locate local artifacts when rerunning.

| Run | Shape | Redis/reference sum ops/sec | shardcache RESP | shardcache SCNP | shardcache SCNP direct | Mean p99 notes |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `server-vector-final-clean-20260601T170007Z` | `1,2,4,8,16` vCPU matrix, 16 vector cases per vCPU | 40,360.0 | 74,641.6 (1.85x) | 74,087.0 (1.84x) | n/a | Redis 144.8 us; RESP 76.0 us; SCNP 79.2 us |
| `server-vector-v1-final-20260601T172915Z` | 1 vCPU shared-port RESP/SCNP, 16 vector cases | 8,000.7 | 15,019.5 (1.88x) | 13,785.3 (1.72x) | n/a | Redis 140.5 us; RESP 78.6 us; SCNP 85.4 us |
| `server-vector-v1-direct-20260601T173345Z` | 1 vCPU direct-shard SCNP, 16 vector cases | 7,400.2 | n/a | n/a | 15,620.5 (2.11x) | Redis 166.6 us; direct SCNP 71.1 us |

The sum ops/sec values are summed across registered vector command cases, not a
single-operation saturation result. Use the per-command CSV rows for individual
command claims.

The 0.7.2 typed Object RAG release run is preserved in
[`evidence/adam-scnp-vector-0.7.2-20260719`](evidence/adam-scnp-vector-0.7.2-20260719/README.md).
For the exact `VSIM COUNT 10 WITHSCORES WITHATTRIBS EF 64` case, its three-run
medians were 4,636 ops/sec for Redis 8.0, 14,707 for typed SCNP fanout, 18,761
for typed direct-shard SCNP, and 83,607 for embedded ShardMap. All measured
runs completed without unexpected command errors.

## Reproduce

Run the server comparison with the shared Redis 8 vector suite:

```bash
RUN_ID=server-vector-rerun-$(date -u +%Y%m%dT%H%M%SZ) \
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis,shardcache-resp,shardcache-scnp \
  --suite redis-v8-vector \
  --vcpus 1,2,4,8,16 \
  --pipeline-depth 1 \
  --clients 1 \
  --warmup 1 \
  --duration 5 \
  --memory-budget-mib 128 \
  --out-dir benchmarks/results
```

For shard-aware direct SCNP rows, use the direct target against the same suite:

```bash
RUN_ID=server-vector-direct-rerun-$(date -u +%Y%m%dT%H%M%SZ) \
./benchmarks/scripts/run-benchmark-suite.sh \
  --targets redis,shardcache-scnp-direct \
  --suite redis-v8-vector \
  --vcpus 1 \
  --pipeline-depth 1 \
  --clients 1 \
  --warmup 1 \
  --duration 6 \
  --memory-budget-mib 128 \
  --out-dir benchmarks/results
```

Compare rows only when `suite`, `category`, `case`, `clients`,
`pipeline_depth`, `vcpus`, and `resolved_plan_id` match. Direct SCNP rows are
best used for routed command subsets; shared-port RESP/SCNP rows are the full
protocol comparison.

## Typed Rust Client

The typed client driver performs setup with `VADD`, then repeatedly calls the
typed `VSIM` API and validates every native response. Run fanout and direct
transport separately so their connection and routing costs remain visible:

```bash
cargo build --release -p shardcache-benchmarks \
  --features typed-vector-client --bin scnp_vector_client_cost

target/release/scnp_vector_client_cost \
  --addr 127.0.0.1:6380 --mode fanout --workers 1 \
  --entries 1024 --dimensions 16 --count 10 --ef 64 \
  --warmup 2 --duration 10

target/release/scnp_vector_client_cost \
  --addr 127.0.0.1:6381 --mode direct --workers 1 \
  --entries 1024 --dimensions 16 --count 10 --ef 64 \
  --warmup 2 --duration 10
```

For direct mode, `--addr` is shard 0's direct listener. Vector commands are
intentionally pinned there by both the client and server. Set
`SHARDCACHE_AUTH_TOKEN` when the server requires authentication; the driver
uses the production typed auth and request-deadline path.
