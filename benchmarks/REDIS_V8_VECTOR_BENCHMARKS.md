# Redis 8 Vector Command Benchmarks

This note covers the Redis 8 vector-set command suite registered as
`redis-v8-vector`. The suite exercises `VADD`, `VCARD`, `VDIM`, `VEMB`,
`VGETATTR`, `VINFO`, `VLINKS`, `VRANDMEMBER`, `VRANGE`, `VREM`, `VSETATTR`,
and `VSIM` through the shared command-matrix runner.

The complete Redis 8 suite is registered as `redis-v8` and also includes the
Redis 8 hash helpers `HGETDEL`, `HGETEX`, and `HSETEX`. Use `redis-v8` for full
coverage runs and `redis-v8-vector` for focused vector-only sweeps.

## Adam Summary

The current Adam vector sweeps used the standardized server benchmark runner
with 1 client, pipeline depth 1, precomposed command plans, and zero unexpected
command errors. Raw bundles remain ignored under `benchmarks/results`; use
these bundle IDs to locate local artifacts when rerunning.

| Run | Shape | Redis/reference sum ops/sec | shardcache RESP | shardcache SCNP | shardcache SCNP direct | Mean p99 notes |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `adam-vector-final-clean-20260601T170007Z` | `1,2,4,8,16` vCPU matrix, 16 vector cases per vCPU | 40,360.0 | 74,641.6 (1.85x) | 74,087.0 (1.84x) | n/a | Redis 144.8 us; RESP 76.0 us; SCNP 79.2 us |
| `adam-vector-v1-final-20260601T172915Z` | 1 vCPU shared-port RESP/SCNP, 16 vector cases | 8,000.7 | 15,019.5 (1.88x) | 13,785.3 (1.72x) | n/a | Redis 140.5 us; RESP 78.6 us; SCNP 85.4 us |
| `adam-vector-v1-direct-20260601T173345Z` | 1 vCPU direct-shard SCNP, 16 vector cases | 7,400.2 | n/a | n/a | 15,620.5 (2.11x) | Redis 166.6 us; direct SCNP 71.1 us |

The sum ops/sec values are summed across registered vector command cases, not a
single-operation saturation result. Use the per-command CSV rows for individual
command claims.

## Reproduce

Run the server comparison with the shared Redis 8 vector suite:

```bash
RUN_ID=adam-vector-rerun-$(date -u +%Y%m%dT%H%M%SZ) \
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
RUN_ID=adam-vector-direct-rerun-$(date -u +%Y%m%dT%H%M%SZ) \
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
