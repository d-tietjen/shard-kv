# Redis Module Command Benchmarks

Head-to-head command-matrix run for the feature-gated Redis module command surface. The run exercises every module command case registered in the shardcache benchmark harness against shardcache and a Redis Stack baseline.

## Initial Audit Scope

- Date: 2026-05-31.
- Host: `adam`, Ubuntu 24.04.4 LTS.
- Git SHA: `9db56cb9fe575daa363478074d582708a6d10948`.
- shardcache features: `redis-server,redis-modules-all`.
- Baseline: `redis/redis-stack-server:latest` on `127.0.0.1:6391`.
- Shape: 1 server vCPU, 1 client, 1 key shard, pipeline depth 1, 1s warmup, 2s measurement.
- Artifact: `benchmarks/results/adam-module-command-matrix-1vcpu-p1-20260531T214753Z/report.md`.

This is a command-coverage and strict request/response comparison. Redis Stack does not ship every third-party or retired module represented by the feature-gated shardcache module surface, so rows where Redis Stack reports command errors are recorded as baseline-error coverage rows rather than performance claims.

## Standardized Docker Server Sweep

The 2026-06-01 Adam prime sweep exercised the same 227 module command cases
through the standardized Docker runner against Redis, Valkey, Dragonfly,
shardcache RESP, and shardcache SCNP. This was the PR validation shape for the
feature-gated module command surface: 1 client, pipeline depth 1, 2s warmup,
10s measurement, `1,2,4,8,16` vCPU, 512 MiB command-precomposition budget, and
`SHARDCACHE_FEATURES=redis-server,redis-modules-all`.

- Artifact: `benchmarks/results/adam-prime-new-commands-20260601T012301Z/report.md`.
- Command bundle: `redis-modules` plus the Redis v6/v7 extension suite in the
  same isolated run.
- Runner validation: 50 target/suite/vCPU legs started, no runner-level
  `Error:` lines, all target CSVs shared the resolved plan IDs, and Adam had no
  lingering `bench-*` containers or benchmark ports after cleanup.

Plain `redis:7.4-alpine`, `valkey/valkey:8.0-alpine`, and Dragonfly do not load
the module implementations in this server matrix. Their module rows are
therefore compatibility/error coverage, not throughput claims. Redis Stack
remains the Redis-family module reference for comparable implemented-module
rows; the standardized sweep proves that shardcache's feature-gated module
surface runs end to end through both RESP and SCNP across the vCPU matrix.

| vCPU | shardcache RESP ops/sec | RESP avg us | RESP p99 us | shardcache SCNP ops/sec | SCNP avg us | SCNP p99 us | Unexpected errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 15,392.4 | 64.7 | 83.5 | 15,377.2 | 64.8 | 82.4 | 0 |
| 2 | 14,937.0 | 66.7 | 86.7 | 17,235.7 | 57.8 | 68.1 | 0 |
| 4 | 14,235.6 | 70.0 | 87.4 | 15,867.5 | 62.8 | 77.3 | 0 |
| 8 | 12,579.3 | 79.2 | 100.8 | 16,939.2 | 58.8 | 73.4 | 0 |
| 16 | 15,010.6 | 66.4 | 84.0 | 16,188.7 | 61.5 | 75.9 | 0 |

Across all five vCPU tiers, shardcache RESP completed 1,135 module rows with
72,154.9 summed ops/sec, 69.4 us mean average latency, 88.5 us mean p99, and 0
unexpected errors. shardcache SCNP completed the same 1,135 rows with 81,608.3
summed ops/sec, 61.1 us mean average latency, 75.4 us mean p99, and 0
unexpected errors. The expected-error replies come from intentional error-path
cases such as reserve-on-existing, and are tracked separately from unexpected
errors in the CSV.

| Target | Module rows | Rows with unexpected errors | Unexpected error replies | Notes |
| --- | ---: | ---: | ---: | --- |
| Redis | 1,135 | 1,055 | 896,886 | Module families absent in the plain Redis image. |
| Valkey | 1,135 | 1,055 | 888,831 | Module families absent in the plain Valkey image. |
| Dragonfly | 1,135 | 890 | 612,279 | Some Redis-compatible module names are accepted as stubs, but most module families are absent. |

## Initial Summary

| Target | Cases | Sum ops/sec | Mean avg us | Unexpected errors | Expected-error replies |
| --- | ---: | ---: | ---: | ---: | ---: |
| shardcache | 227 | 25127.5 | 39.7 | 0 | 221 |
| Redis Stack | 227 | 14580.0 | 68.5 | 15039 | 1666 |

| Subset | Cases | shardcache ops/sec | Redis Stack ops/sec | sc/redis | shardcache mean avg us | Redis Stack mean avg us | Redis faster cases |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Clean non-error common subset | 96 | 10623.0 | 6165.5 | 1.72x | 44.7 | 77.9 | 0 |
| Zero-unexpected-error common subset | 109 | 12061.5 | 6999.5 | 1.72x | 42.8 | 78.1 | 0 |

Initial status counts: `ok` 96, `expected-error` 13, `redis-stack error` 118, `shardcache error` 0. Redis Stack was faster on 0 commands in both the clean non-error subset and the zero-unexpected-error subset.

## Slower-Row Optimization Follow-Up

The initial full matrix showed no Redis Stack throughput wins, but three clean rows had higher shardcache average latency: `FT._LIST`, `TS.MRANGE`, and `TS.MREVRANGE`. After optimizing those paths, the same 227-command module matrix was rerun on Adam with the same 1 vCPU, 1 client, 1 key shard, pipeline-depth-1, 1s warmup, and 2s measurement shape. The optimized clean subset has 0 Redis Stack throughput wins and 0 Redis Stack lower-latency rows.

- Artifact: `benchmarks/results/adam-module-command-matrix-optimized5-modules-1vcpu-p1-20260531T221741Z/report.md`.

| Optimized scope | Cases | shardcache ops/sec | Redis Stack ops/sec | sc/redis | shardcache mean avg us | Redis Stack mean avg us | Redis faster cases |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All module cases | 227 | 27953.5 | 14615.5 | 1.91x | 35.7 | 68.4 | n/a |
| Clean non-error common subset | 96 | 11817.5 | 6171.5 | 1.91x | 35.7 | 77.2 | 0 |
| Zero-unexpected-error common subset | 109 | 13418.5 | 7007.5 | 1.91x | 34.8 | 77.0 | 0 |

| Command | shardcache ops/sec before | shardcache ops/sec after | Redis Stack ops/sec after | shardcache avg us before | shardcache avg us after | Redis Stack avg us after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `FT._LIST` | 110.5 | 123.0 | 64.5 | 144.7 | 30.2 | 59.4 |
| `TS.MRANGE` | 110.5 | 123.0 | 64.0 | 677.8 | 328.6 | 343.1 |
| `TS.MREVRANGE` | 110.5 | 123.0 | 64.0 | 692.0 | 330.0 | 332.9 |

## P99 Tail-Latency Follow-Up

The command matrix now records per-command p50/p95/p99/p999 latency in the raw CSV and reports mean p99 in Markdown/JSON. A final 1-vCPU, pipeline-depth-1 Adam run used that harness after adding filter-aware RedisTimeSeries multi-range matching, a single-shard multi-range response path, and pre-encoded time-series sample rows. In the clean Redis Stack-comparable subset, Redis Stack has 0 throughput wins, 0 average-latency wins, and 0 p99-latency wins.

- Artifact: `benchmarks/results/adam-module-command-matrix-p99-opt3-modules-1vcpu-p1-20260531T231229Z/report.md`.

| Final optimized scope | Cases | shardcache ops/sec | Redis Stack ops/sec | sc/redis | shardcache mean avg us | Redis Stack mean avg us | shardcache mean p99 us | Redis Stack mean p99 us | Redis faster cases | Redis lower p99 cases |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All module cases | 227 | 29791.5 | 15147.0 | 1.97x | 33.1 | 65.2 | 41.3 | 89.4 | n/a | n/a |
| Clean non-error common subset | 96 | 12597.5 | 6405.5 | 1.97x | 32.8 | 74.2 | 40.8 | 103.8 | 0 | 0 |
| Zero-unexpected-error common subset | 109 | 14302.5 | 7272.0 | 1.97x | 32.1 | 74.3 | 39.7 | 105.4 | 0 | 0 |

| Command | shardcache ops/sec | Redis Stack ops/sec | shardcache avg us | Redis Stack avg us | shardcache p99 us | Redis Stack p99 us |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `TS.MRANGE` | 131.0 | 66.5 | 295.6 | 344.4 | 415.0 | 443.4 |
| `TS.MREVRANGE` | 131.0 | 66.5 | 296.5 | 332.5 | 421.4 | 423.2 |

## Initial Module Prefix Rollup

The `sc/redis` prefix ratio is only shown for prefixes where Redis Stack had no unexpected errors; mixed unsupported/error prefixes are shown as `n/a` to avoid treating error-reply throughput as a performance result.

| Prefix | Cases | Redis Stack no-error cases | shardcache ops/sec | Redis Stack ops/sec | sc/redis | shardcache mean avg us | Redis Stack mean avg us | shardcache errors | Redis Stack errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `AI` | 22 | 0 | 2442.0 | 1419.0 | n/a | 40.0 | 60.5 | 0 | 2838 |
| `BF` | 10 | 9 | 1110.0 | 645.0 | n/a | 29.3 | 58.2 | 0 | 129 |
| `CF` | 12 | 9 | 1332.0 | 774.0 | n/a | 29.2 | 58.1 | 0 | 265 |
| `CL` | 1 | 0 | 111.0 | 64.5 | n/a | 29.3 | 58.2 | 0 | 129 |
| `CMS` | 6 | 5 | 666.0 | 387.0 | n/a | 29.4 | 57.6 | 0 | 129 |
| `FT` | 27 | 24 | 2983.5 | 1728.0 | n/a | 33.3 | 66.7 | 0 | 384 |
| `GRAPH` | 8 | 0 | 888.0 | 516.0 | n/a | 43.1 | 57.5 | 0 | 1032 |
| `JS` | 3 | 0 | 333.0 | 193.5 | n/a | 29.5 | 57.0 | 0 | 387 |
| `JSON` | 24 | 24 | 2657.5 | 1548.0 | 1.72x | 29.6 | 64.2 | 0 | 0 |
| `NR` | 6 | 0 | 663.0 | 385.5 | n/a | 29.4 | 57.8 | 0 | 771 |
| `R` | 24 | 0 | 2652.0 | 1536.0 | n/a | 29.2 | 57.9 | 0 | 3072 |
| `R64` | 23 | 0 | 2541.5 | 1472.0 | n/a | 28.7 | 57.2 | 0 | 2944 |
| `REDE` | 3 | 0 | 331.5 | 192.0 | n/a | 28.9 | 56.6 | 0 | 384 |
| `RG` | 15 | 0 | 1665.0 | 967.5 | n/a | 67.1 | 56.7 | 0 | 1935 |
| `SG` | 3 | 0 | 331.5 | 192.0 | n/a | 28.5 | 98.2 | 0 | 384 |
| `SNOWFLAKE` | 2 | 0 | 221.0 | 128.0 | n/a | 28.6 | 84.9 | 0 | 256 |
| `TDIGEST` | 14 | 14 | 1547.0 | 896.0 | 1.73x | 28.9 | 99.7 | 0 | 0 |
| `TOPK` | 7 | 7 | 773.5 | 448.0 | 1.73x | 31.0 | 72.8 | 0 | 0 |
| `TS` | 17 | 17 | 1878.5 | 1088.0 | 1.73x | 108.9 | 125.0 | 0 | 0 |

## Initial Full Command Results

`ok` means both targets returned non-error replies. `expected-error` means the benchmark intentionally exercises an error-reply path, such as reserve-on-existing, and the error was classified as expected. `redis-stack error` means Redis Stack returned unexpected errors for that command in this baseline image. Ratios are omitted for Redis Stack error rows because error-reply throughput is not a useful performance comparison.

| Command | Status | shardcache ops/sec | Redis Stack ops/sec | sc/redis | shardcache avg us | Redis Stack avg us | Errors / expected |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `AI.CONFIG` | redis-stack error | 111.0 | 64.5 | n/a | 31.3 | 63.4 | sc 0/0; redis 129/0 |
| `AI.DAGEXECUTE` | redis-stack error | 111.0 | 64.5 | n/a | 28.3 | 63.2 | sc 0/0; redis 129/0 |
| `AI.DAGRUN` | redis-stack error | 111.0 | 64.5 | n/a | 28.2 | 59.9 | sc 0/0; redis 129/0 |
| `AI.DAGRUN_RO` | redis-stack error | 111.0 | 64.5 | n/a | 27.9 | 59.9 | sc 0/0; redis 129/0 |
| `AI.INFO` | redis-stack error | 111.0 | 64.5 | n/a | 30.4 | 58.9 | sc 0/0; redis 129/0 |
| `AI.MODELDEL` | redis-stack error | 111.0 | 64.5 | n/a | 28.7 | 59.3 | sc 0/0; redis 129/0 |
| `AI.MODELEXECUTE` | redis-stack error | 111.0 | 64.5 | n/a | 28.3 | 60.4 | sc 0/0; redis 129/0 |
| `AI.MODELGET` | redis-stack error | 111.0 | 64.5 | n/a | 29.0 | 69.4 | sc 0/0; redis 129/0 |
| `AI.MODELRUN` | redis-stack error | 111.0 | 64.5 | n/a | 28.0 | 58.6 | sc 0/0; redis 129/0 |
| `AI.MODELSET` | redis-stack error | 111.0 | 64.5 | n/a | 28.1 | 60.7 | sc 0/0; redis 129/0 |
| `AI.MODELSTORE` | redis-stack error | 111.0 | 64.5 | n/a | 27.9 | 62.7 | sc 0/0; redis 129/0 |
| `AI.SCRIPTDEL` | redis-stack error | 111.0 | 64.5 | n/a | 28.0 | 58.2 | sc 0/0; redis 129/0 |
| `AI.SCRIPTEXECUTE` | redis-stack error | 111.0 | 64.5 | n/a | 28.0 | 60.0 | sc 0/0; redis 129/0 |
| `AI.SCRIPTGET` | redis-stack error | 111.0 | 64.5 | n/a | 29.2 | 58.8 | sc 0/0; redis 129/0 |
| `AI.SCRIPTRUN` | redis-stack error | 111.0 | 64.5 | n/a | 28.3 | 59.1 | sc 0/0; redis 129/0 |
| `AI.SCRIPTSET` | redis-stack error | 111.0 | 64.5 | n/a | 28.0 | 60.6 | sc 0/0; redis 129/0 |
| `AI.SCRIPTSTORE` | redis-stack error | 111.0 | 64.5 | n/a | 28.3 | 58.0 | sc 0/0; redis 129/0 |
| `AI.TENSORDEL` | redis-stack error | 111.0 | 64.5 | n/a | 27.7 | 57.7 | sc 0/0; redis 129/0 |
| `AI.TENSORGET` | redis-stack error | 111.0 | 64.5 | n/a | 29.4 | 57.2 | sc 0/0; redis 129/0 |
| `AI.TENSORSET` | redis-stack error | 111.0 | 64.5 | n/a | 28.3 | 58.2 | sc 0/0; redis 129/0 |
| `AI._MODELSCAN` | redis-stack error | 111.0 | 64.5 | n/a | 165.7 | 62.0 | sc 0/0; redis 129/0 |
| `AI._SCRIPTSCAN` | redis-stack error | 111.0 | 64.5 | n/a | 142.7 | 65.2 | sc 0/0; redis 129/0 |
| `BF.ADD` | ok | 111.0 | 64.5 | 1.72x | 30.8 | 58.8 | sc 0/0; redis 0/0 |
| `BF.CARD` | ok | 111.0 | 64.5 | 1.72x | 29.5 | 56.8 | sc 0/0; redis 0/0 |
| `BF.EXISTS` | ok | 111.0 | 64.5 | 1.72x | 29.0 | 56.7 | sc 0/0; redis 0/0 |
| `BF.INFO` | ok | 111.0 | 64.5 | 1.72x | 29.9 | 58.3 | sc 0/0; redis 0/0 |
| `BF.INSERT` | ok | 111.0 | 64.5 | 1.72x | 29.3 | 58.7 | sc 0/0; redis 0/0 |
| `BF.LOADCHUNK` | redis-stack error | 111.0 | 64.5 | n/a | 28.9 | 59.1 | sc 0/0; redis 129/0 |
| `BF.MADD` | ok | 111.0 | 64.5 | 1.72x | 28.9 | 58.9 | sc 0/0; redis 0/0 |
| `BF.MEXISTS` | ok | 111.0 | 64.5 | 1.72x | 29.1 | 57.4 | sc 0/0; redis 0/0 |
| `BF.RESERVE` | expected-error | 111.0 | 64.5 | 1.72x | 28.4 | 59.7 | sc 0/0; redis 0/129 |
| `BF.SCANDUMP` | ok | 111.0 | 64.5 | 1.72x | 29.0 | 57.9 | sc 0/0; redis 0/0 |
| `CF.ADD` | redis-stack error | 111.0 | 64.5 | n/a | 30.0 | 57.8 | sc 0/0; redis 68/0 |
| `CF.ADDNX` | ok | 111.0 | 64.5 | 1.72x | 29.5 | 56.9 | sc 0/0; redis 0/0 |
| `CF.COUNT` | ok | 111.0 | 64.5 | 1.72x | 28.1 | 56.9 | sc 0/0; redis 0/0 |
| `CF.DEL` | ok | 111.0 | 64.5 | 1.72x | 28.8 | 56.6 | sc 0/0; redis 0/0 |
| `CF.EXISTS` | ok | 111.0 | 64.5 | 1.72x | 28.7 | 58.3 | sc 0/0; redis 0/0 |
| `CF.INFO` | ok | 111.0 | 64.5 | 1.72x | 30.8 | 61.0 | sc 0/0; redis 0/0 |
| `CF.INSERT` | redis-stack error | 111.0 | 64.5 | n/a | 29.6 | 58.7 | sc 0/0; redis 68/0 |
| `CF.INSERTNX` | ok | 111.0 | 64.5 | 1.72x | 29.0 | 56.9 | sc 0/0; redis 0/0 |
| `CF.LOADCHUNK` | redis-stack error | 111.0 | 64.5 | n/a | 28.4 | 59.1 | sc 0/0; redis 129/0 |
| `CF.MEXISTS` | ok | 111.0 | 64.5 | 1.72x | 29.0 | 58.9 | sc 0/0; redis 0/0 |
| `CF.RESERVE` | expected-error | 111.0 | 64.5 | 1.72x | 29.7 | 58.2 | sc 0/0; redis 0/129 |
| `CF.SCANDUMP` | ok | 111.0 | 64.5 | 1.72x | 28.6 | 57.9 | sc 0/0; redis 0/0 |
| `CL.THROTTLE` | redis-stack error | 111.0 | 64.5 | n/a | 29.3 | 58.2 | sc 0/0; redis 129/0 |
| `CMS.INCRBY` | ok | 111.0 | 64.5 | 1.72x | 28.9 | 58.0 | sc 0/0; redis 0/0 |
| `CMS.INFO` | ok | 111.0 | 64.5 | 1.72x | 30.4 | 57.4 | sc 0/0; redis 0/0 |
| `CMS.INITBYDIM` | expected-error | 111.0 | 64.5 | 1.72x | 28.6 | 58.4 | sc 0/0; redis 0/129 |
| `CMS.INITBYPROB` | expected-error | 111.0 | 64.5 | 1.72x | 28.6 | 57.6 | sc 0/0; redis 0/129 |
| `CMS.MERGE` | redis-stack error | 111.0 | 64.5 | n/a | 29.9 | 57.9 | sc 0/0; redis 129/0 |
| `CMS.QUERY` | ok | 111.0 | 64.5 | 1.72x | 29.9 | 56.3 | sc 0/0; redis 0/0 |
| `FT.AGGREGATE` | ok | 110.5 | 64.0 | 1.73x | 31.6 | 66.6 | sc 0/0; redis 0/0 |
| `FT.ALIASADD` | expected-error | 110.5 | 64.0 | 1.73x | 29.0 | 59.5 | sc 0/0; redis 0/128 |
| `FT.ALIASDEL` | ok | 110.5 | 64.0 | 1.73x | 28.5 | 59.2 | sc 0/0; redis 0/0 |
| `FT.ALIASUPDATE` | ok | 110.5 | 64.0 | 1.73x | 28.5 | 57.2 | sc 0/0; redis 0/0 |
| `FT.ALTER` | expected-error | 110.5 | 64.0 | 1.73x | 29.2 | 101.2 | sc 0/0; redis 0/128 |
| `FT.CONFIG` | ok | 110.5 | 64.0 | 1.73x | 29.0 | 60.9 | sc 0/0; redis 0/0 |
| `FT.CREATE` | expected-error | 110.5 | 64.0 | 1.73x | 29.2 | 93.4 | sc 0/0; redis 0/128 |
| `FT.CURSOR` | redis-stack error | 110.5 | 64.0 | n/a | 28.1 | 59.6 | sc 0/0; redis 128/0 |
| `FT.DICTADD` | ok | 110.5 | 64.0 | 1.73x | 29.2 | 58.6 | sc 0/0; redis 0/0 |
| `FT.DICTDEL` | ok | 110.5 | 64.0 | 1.73x | 28.3 | 56.3 | sc 0/0; redis 0/0 |
| `FT.DICTDUMP` | ok | 110.5 | 64.0 | 1.73x | 28.7 | 59.6 | sc 0/0; redis 0/0 |
| `FT.DROPINDEX` | expected-error | 110.5 | 64.0 | 1.73x | 28.4 | 72.5 | sc 0/0; redis 0/127 |
| `FT.EXPLAIN` | ok | 110.5 | 64.0 | 1.73x | 28.5 | 61.4 | sc 0/0; redis 0/0 |
| `FT.EXPLAINCLI` | ok | 110.5 | 64.0 | 1.73x | 28.3 | 60.6 | sc 0/0; redis 0/0 |
| `FT.HYBRID` | redis-stack error | 110.5 | 64.0 | n/a | 28.5 | 59.0 | sc 0/0; redis 128/0 |
| `FT.INFO` | ok | 110.5 | 64.0 | 1.73x | 31.2 | 93.5 | sc 0/0; redis 0/0 |
| `FT.PROFILE` | ok | 110.5 | 64.0 | 1.73x | 28.9 | 92.9 | sc 0/0; redis 0/0 |
| `FT.SEARCH` | ok | 110.5 | 64.0 | 1.73x | 29.5 | 65.6 | sc 0/0; redis 0/0 |
| `FT.SPELLCHECK` | ok | 110.5 | 64.0 | 1.73x | 29.0 | 74.9 | sc 0/0; redis 0/0 |
| `FT.SUGADD` | ok | 110.5 | 64.0 | 1.73x | 28.6 | 59.3 | sc 0/0; redis 0/0 |
| `FT.SUGDEL` | ok | 110.5 | 64.0 | 1.73x | 28.2 | 57.6 | sc 0/0; redis 0/0 |
| `FT.SUGGET` | ok | 110.5 | 64.0 | 1.73x | 28.7 | 59.5 | sc 0/0; redis 0/0 |
| `FT.SUGLEN` | ok | 110.5 | 64.0 | 1.73x | 28.2 | 57.0 | sc 0/0; redis 0/0 |
| `FT.SYNDUMP` | ok | 110.5 | 64.0 | 1.73x | 30.3 | 57.8 | sc 0/0; redis 0/0 |
| `FT.SYNUPDATE` | ok | 110.5 | 64.0 | 1.73x | 28.6 | 61.1 | sc 0/0; redis 0/0 |
| `FT.TAGVALS` | redis-stack error | 110.5 | 64.0 | n/a | 28.8 | 75.7 | sc 0/0; redis 128/0 |
| `FT._LIST` | ok | 110.5 | 64.0 | 1.73x | 144.7 | 59.9 | sc 0/0; redis 0/0 |
| `GRAPH.CONFIG` | redis-stack error | 111.0 | 64.5 | n/a | 28.2 | 58.7 | sc 0/0; redis 129/0 |
| `GRAPH.DELETE` | redis-stack error | 111.0 | 64.5 | n/a | 28.0 | 56.9 | sc 0/0; redis 129/0 |
| `GRAPH.EXPLAIN` | redis-stack error | 111.0 | 64.5 | n/a | 28.6 | 57.4 | sc 0/0; redis 129/0 |
| `GRAPH.LIST` | redis-stack error | 111.0 | 64.5 | n/a | 140.7 | 57.0 | sc 0/0; redis 129/0 |
| `GRAPH.PROFILE` | redis-stack error | 111.0 | 64.5 | n/a | 31.4 | 57.6 | sc 0/0; redis 129/0 |
| `GRAPH.QUERY` | redis-stack error | 111.0 | 64.5 | n/a | 29.8 | 58.3 | sc 0/0; redis 129/0 |
| `GRAPH.RO_QUERY` | redis-stack error | 111.0 | 64.5 | n/a | 29.6 | 57.4 | sc 0/0; redis 129/0 |
| `GRAPH.SLOWLOG` | redis-stack error | 111.0 | 64.5 | n/a | 28.2 | 56.9 | sc 0/0; redis 129/0 |
| `JS.DEL` | redis-stack error | 111.0 | 64.5 | n/a | 28.3 | 56.8 | sc 0/0; redis 129/0 |
| `JS.EVAL` | redis-stack error | 111.0 | 64.5 | n/a | 30.4 | 57.3 | sc 0/0; redis 129/0 |
| `JS.GET` | redis-stack error | 111.0 | 64.5 | n/a | 29.9 | 57.0 | sc 0/0; redis 129/0 |
| `JSON.ARRAPPEND` | ok | 111.0 | 64.5 | 1.72x | 29.2 | 70.2 | sc 0/0; redis 0/0 |
| `JSON.ARRINDEX` | ok | 111.0 | 64.5 | 1.72x | 28.8 | 61.3 | sc 0/0; redis 0/0 |
| `JSON.ARRINSERT` | ok | 111.0 | 64.5 | 1.72x | 29.0 | 64.2 | sc 0/0; redis 0/0 |
| `JSON.ARRLEN` | ok | 111.0 | 64.5 | 1.72x | 28.5 | 61.4 | sc 0/0; redis 0/0 |
| `JSON.ARRPOP` | ok | 111.0 | 64.5 | 1.72x | 28.3 | 60.8 | sc 0/0; redis 0/0 |
| `JSON.ARRTRIM` | ok | 111.0 | 64.5 | 1.72x | 28.6 | 63.4 | sc 0/0; redis 0/0 |
| `JSON.CLEAR` | ok | 111.0 | 64.5 | 1.72x | 28.9 | 70.1 | sc 0/0; redis 0/0 |
| `JSON.DEBUG` | ok | 111.0 | 64.5 | 1.72x | 30.4 | 61.1 | sc 0/0; redis 0/0 |
| `JSON.DEL` | ok | 111.0 | 64.5 | 1.72x | 29.7 | 57.5 | sc 0/0; redis 0/0 |
| `JSON.FORGET` | ok | 111.0 | 64.5 | 1.72x | 28.5 | 56.2 | sc 0/0; redis 0/0 |
| `JSON.GET` | ok | 111.0 | 64.5 | 1.72x | 31.2 | 60.8 | sc 0/0; redis 0/0 |
| `JSON.MERGE` | ok | 110.5 | 64.5 | 1.71x | 30.0 | 62.2 | sc 0/0; redis 0/0 |
| `JSON.MGET` | ok | 110.5 | 64.5 | 1.71x | 32.0 | 63.6 | sc 0/0; redis 0/0 |
| `JSON.MSET` | ok | 110.5 | 64.5 | 1.71x | 31.6 | 63.7 | sc 0/0; redis 0/0 |
| `JSON.NUMINCRBY` | ok | 110.5 | 64.5 | 1.71x | 29.7 | 82.2 | sc 0/0; redis 0/0 |
| `JSON.NUMMULTBY` | ok | 110.5 | 64.5 | 1.71x | 28.8 | 63.1 | sc 0/0; redis 0/0 |
| `JSON.OBJKEYS` | ok | 110.5 | 64.5 | 1.71x | 30.9 | 62.2 | sc 0/0; redis 0/0 |
| `JSON.OBJLEN` | ok | 110.5 | 64.5 | 1.71x | 29.0 | 59.4 | sc 0/0; redis 0/0 |
| `JSON.RESP` | ok | 110.5 | 64.5 | 1.71x | 29.8 | 64.1 | sc 0/0; redis 0/0 |
| `JSON.SET` | ok | 110.5 | 64.5 | 1.71x | 31.4 | 63.8 | sc 0/0; redis 0/0 |
| `JSON.STRAPPEND` | ok | 110.5 | 64.5 | 1.71x | 29.2 | 64.4 | sc 0/0; redis 0/0 |
| `JSON.STRLEN` | ok | 110.5 | 64.5 | 1.71x | 28.6 | 60.6 | sc 0/0; redis 0/0 |
| `JSON.TOGGLE` | ok | 110.5 | 64.5 | 1.71x | 28.2 | 85.9 | sc 0/0; redis 0/0 |
| `JSON.TYPE` | ok | 110.5 | 64.5 | 1.71x | 30.1 | 59.8 | sc 0/0; redis 0/0 |
| `NR.CREATE` | redis-stack error | 110.5 | 64.5 | n/a | 28.6 | 60.0 | sc 0/0; redis 129/0 |
| `NR.DELETE` | redis-stack error | 110.5 | 64.0 | n/a | 29.0 | 56.3 | sc 0/0; redis 128/0 |
| `NR.INFO` | redis-stack error | 110.5 | 64.0 | n/a | 30.4 | 57.4 | sc 0/0; redis 128/0 |
| `NR.OBSERVE` | redis-stack error | 110.5 | 64.5 | n/a | 29.1 | 57.2 | sc 0/0; redis 129/0 |
| `NR.RUN` | redis-stack error | 110.5 | 64.5 | n/a | 30.1 | 56.8 | sc 0/0; redis 129/0 |
| `NR.TRAIN` | redis-stack error | 110.5 | 64.0 | n/a | 29.0 | 59.0 | sc 0/0; redis 128/0 |
| `R.APPENDINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.8 | 57.1 | sc 0/0; redis 128/0 |
| `R.BITCOUNT` | redis-stack error | 110.5 | 64.0 | n/a | 29.2 | 56.4 | sc 0/0; redis 128/0 |
| `R.BITOP` | redis-stack error | 110.5 | 64.0 | n/a | 29.9 | 58.1 | sc 0/0; redis 128/0 |
| `R.BITPOS` | redis-stack error | 110.5 | 64.0 | n/a | 29.4 | 56.8 | sc 0/0; redis 128/0 |
| `R.CLEAR` | redis-stack error | 110.5 | 64.0 | n/a | 28.7 | 56.2 | sc 0/0; redis 128/0 |
| `R.CLEARBITS` | redis-stack error | 110.5 | 64.0 | n/a | 28.6 | 83.2 | sc 0/0; redis 128/0 |
| `R.CONTAINS` | redis-stack error | 110.5 | 64.0 | n/a | 28.9 | 56.6 | sc 0/0; redis 128/0 |
| `R.DELETEINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.4 | 58.0 | sc 0/0; redis 128/0 |
| `R.DIFF` | redis-stack error | 110.5 | 64.0 | n/a | 30.3 | 56.8 | sc 0/0; redis 128/0 |
| `R.GETBIT` | redis-stack error | 110.5 | 64.0 | n/a | 29.1 | 56.5 | sc 0/0; redis 128/0 |
| `R.GETBITARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 30.0 | 57.9 | sc 0/0; redis 128/0 |
| `R.GETBITS` | redis-stack error | 110.5 | 64.0 | n/a | 29.4 | 56.2 | sc 0/0; redis 128/0 |
| `R.GETINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.4 | 56.4 | sc 0/0; redis 128/0 |
| `R.JACCARD` | redis-stack error | 110.5 | 64.0 | n/a | 29.7 | 57.0 | sc 0/0; redis 128/0 |
| `R.MAX` | redis-stack error | 110.5 | 64.0 | n/a | 30.0 | 56.6 | sc 0/0; redis 128/0 |
| `R.MIN` | redis-stack error | 110.5 | 64.0 | n/a | 28.2 | 56.2 | sc 0/0; redis 128/0 |
| `R.OPTIMIZE` | redis-stack error | 110.5 | 64.0 | n/a | 29.7 | 56.8 | sc 0/0; redis 128/0 |
| `R.RANGEINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 29.2 | 57.2 | sc 0/0; redis 128/0 |
| `R.SETBIT` | redis-stack error | 110.5 | 64.0 | n/a | 29.1 | 56.6 | sc 0/0; redis 128/0 |
| `R.SETBITARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.5 | 56.9 | sc 0/0; redis 128/0 |
| `R.SETFULL` | redis-stack error | 110.5 | 64.0 | n/a | 28.6 | 56.5 | sc 0/0; redis 128/0 |
| `R.SETINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.4 | 56.9 | sc 0/0; redis 128/0 |
| `R.SETRANGE` | redis-stack error | 110.5 | 64.0 | n/a | 28.8 | 56.7 | sc 0/0; redis 128/0 |
| `R.STAT` | redis-stack error | 110.5 | 64.0 | n/a | 30.6 | 56.8 | sc 0/0; redis 128/0 |
| `R64.APPENDINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.7 | 57.6 | sc 0/0; redis 128/0 |
| `R64.BITCOUNT` | redis-stack error | 110.5 | 64.0 | n/a | 28.6 | 56.7 | sc 0/0; redis 128/0 |
| `R64.BITOP` | redis-stack error | 110.5 | 64.0 | n/a | 29.0 | 57.9 | sc 0/0; redis 128/0 |
| `R64.BITPOS` | redis-stack error | 110.5 | 64.0 | n/a | 28.9 | 57.8 | sc 0/0; redis 128/0 |
| `R64.CLEAR` | redis-stack error | 110.5 | 64.0 | n/a | 29.3 | 56.5 | sc 0/0; redis 128/0 |
| `R64.CLEARBITS` | redis-stack error | 110.5 | 64.0 | n/a | 28.2 | 56.5 | sc 0/0; redis 128/0 |
| `R64.CONTAINS` | redis-stack error | 110.5 | 64.0 | n/a | 28.3 | 56.8 | sc 0/0; redis 128/0 |
| `R64.DELETEINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 27.9 | 56.8 | sc 0/0; redis 128/0 |
| `R64.DIFF` | redis-stack error | 110.5 | 64.0 | n/a | 29.6 | 57.3 | sc 0/0; redis 128/0 |
| `R64.GETBIT` | redis-stack error | 110.5 | 64.0 | n/a | 28.8 | 58.7 | sc 0/0; redis 128/0 |
| `R64.GETBITARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.9 | 56.8 | sc 0/0; redis 128/0 |
| `R64.GETBITS` | redis-stack error | 110.5 | 64.0 | n/a | 28.9 | 56.5 | sc 0/0; redis 128/0 |
| `R64.GETINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.2 | 58.5 | sc 0/0; redis 128/0 |
| `R64.JACCARD` | redis-stack error | 110.5 | 64.0 | n/a | 29.4 | 56.5 | sc 0/0; redis 128/0 |
| `R64.MAX` | redis-stack error | 110.5 | 64.0 | n/a | 29.8 | 57.0 | sc 0/0; redis 128/0 |
| `R64.MIN` | redis-stack error | 110.5 | 64.0 | n/a | 28.3 | 56.4 | sc 0/0; redis 128/0 |
| `R64.OPTIMIZE` | redis-stack error | 110.5 | 64.0 | n/a | 28.3 | 58.3 | sc 0/0; redis 128/0 |
| `R64.RANGEINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.8 | 57.2 | sc 0/0; redis 128/0 |
| `R64.SETBIT` | redis-stack error | 110.5 | 64.0 | n/a | 28.8 | 57.1 | sc 0/0; redis 128/0 |
| `R64.SETBITARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.6 | 57.0 | sc 0/0; redis 128/0 |
| `R64.SETFULL` | redis-stack error | 110.5 | 64.0 | n/a | 28.5 | 56.5 | sc 0/0; redis 128/0 |
| `R64.SETINTARRAY` | redis-stack error | 110.5 | 64.0 | n/a | 28.2 | 57.2 | sc 0/0; redis 128/0 |
| `R64.SETRANGE` | redis-stack error | 110.5 | 64.0 | n/a | 28.3 | 57.3 | sc 0/0; redis 128/0 |
| `REDE.CREATE` | redis-stack error | 110.5 | 64.0 | n/a | 28.8 | 56.4 | sc 0/0; redis 128/0 |
| `REDE.DELETE` | redis-stack error | 110.5 | 64.0 | n/a | 28.2 | 56.3 | sc 0/0; redis 128/0 |
| `REDE.GET` | redis-stack error | 110.5 | 64.0 | n/a | 29.5 | 56.9 | sc 0/0; redis 128/0 |
| `RG.ABORTEXECUTION` | redis-stack error | 111.0 | 64.5 | n/a | 28.6 | 57.1 | sc 0/0; redis 129/0 |
| `RG.CONFIGGET` | redis-stack error | 111.0 | 64.5 | n/a | 28.1 | 56.9 | sc 0/0; redis 129/0 |
| `RG.CONFIGSET` | redis-stack error | 111.0 | 64.5 | n/a | 27.8 | 57.0 | sc 0/0; redis 129/0 |
| `RG.DUMPEXECUTIONS` | redis-stack error | 111.0 | 64.5 | n/a | 142.9 | 56.5 | sc 0/0; redis 129/0 |
| `RG.DUMPREGISTRATIONS` | redis-stack error | 111.0 | 64.5 | n/a | 142.0 | 56.8 | sc 0/0; redis 129/0 |
| `RG.GETRESULTS` | redis-stack error | 111.0 | 64.5 | n/a | 30.7 | 56.4 | sc 0/0; redis 129/0 |
| `RG.GETRESULTSBLOCKING` | redis-stack error | 111.0 | 64.5 | n/a | 28.2 | 56.4 | sc 0/0; redis 129/0 |
| `RG.JDUMPSESSIONS` | redis-stack error | 111.0 | 64.5 | n/a | 141.0 | 56.4 | sc 0/0; redis 129/0 |
| `RG.JEXECUTE` | redis-stack error | 111.0 | 64.5 | n/a | 32.3 | 57.0 | sc 0/0; redis 129/0 |
| `RG.PYDUMPEXECUTIONS` | redis-stack error | 111.0 | 64.5 | n/a | 141.2 | 56.3 | sc 0/0; redis 129/0 |
| `RG.PYDUMPREQS` | redis-stack error | 111.0 | 64.5 | n/a | 142.2 | 56.7 | sc 0/0; redis 129/0 |
| `RG.PYEXECUTE` | redis-stack error | 111.0 | 64.5 | n/a | 32.8 | 57.0 | sc 0/0; redis 129/0 |
| `RG.PYSTATS` | redis-stack error | 111.0 | 64.5 | n/a | 29.6 | 56.2 | sc 0/0; redis 129/0 |
| `RG.TRIGGER` | redis-stack error | 111.0 | 64.5 | n/a | 29.3 | 56.9 | sc 0/0; redis 129/0 |
| `RG.UNREGISTER` | redis-stack error | 111.0 | 64.5 | n/a | 29.5 | 56.5 | sc 0/0; redis 129/0 |
| `SG.CREATE` | redis-stack error | 110.5 | 64.0 | n/a | 28.9 | 89.7 | sc 0/0; redis 128/0 |
| `SG.DELETE` | redis-stack error | 110.5 | 64.0 | n/a | 28.2 | 99.8 | sc 0/0; redis 128/0 |
| `SG.VALIDATE` | redis-stack error | 110.5 | 64.0 | n/a | 28.5 | 105.1 | sc 0/0; redis 128/0 |
| `SNOWFLAKE.INFO` | redis-stack error | 110.5 | 64.0 | n/a | 28.7 | 86.6 | sc 0/0; redis 128/0 |
| `SNOWFLAKE.NEXT` | redis-stack error | 110.5 | 64.0 | n/a | 28.6 | 83.3 | sc 0/0; redis 128/0 |
| `TDIGEST.ADD` | ok | 110.5 | 64.0 | 1.73x | 28.9 | 98.0 | sc 0/0; redis 0/0 |
| `TDIGEST.BYRANK` | ok | 110.5 | 64.0 | 1.73x | 29.3 | 86.1 | sc 0/0; redis 0/0 |
| `TDIGEST.BYREVRANK` | ok | 110.5 | 64.0 | 1.73x | 28.9 | 103.2 | sc 0/0; redis 0/0 |
| `TDIGEST.CDF` | ok | 110.5 | 64.0 | 1.73x | 29.0 | 104.3 | sc 0/0; redis 0/0 |
| `TDIGEST.CREATE` | expected-error | 110.5 | 64.0 | 1.73x | 28.2 | 108.6 | sc 0/0; redis 0/128 |
| `TDIGEST.INFO` | ok | 110.5 | 64.0 | 1.73x | 29.6 | 105.9 | sc 0/0; redis 0/0 |
| `TDIGEST.MAX` | ok | 110.5 | 64.0 | 1.73x | 29.0 | 94.1 | sc 0/0; redis 0/0 |
| `TDIGEST.MERGE` | ok | 110.5 | 64.0 | 1.73x | 29.0 | 92.2 | sc 0/0; redis 0/0 |
| `TDIGEST.MIN` | ok | 110.5 | 64.0 | 1.73x | 28.7 | 98.7 | sc 0/0; redis 0/0 |
| `TDIGEST.QUANTILE` | ok | 110.5 | 64.0 | 1.73x | 29.2 | 112.3 | sc 0/0; redis 0/0 |
| `TDIGEST.RANK` | ok | 110.5 | 64.0 | 1.73x | 28.9 | 93.0 | sc 0/0; redis 0/0 |
| `TDIGEST.RESET` | ok | 110.5 | 64.0 | 1.73x | 28.3 | 97.0 | sc 0/0; redis 0/0 |
| `TDIGEST.REVRANK` | ok | 110.5 | 64.0 | 1.73x | 28.9 | 95.2 | sc 0/0; redis 0/0 |
| `TDIGEST.TRIMMED_MEAN` | ok | 110.5 | 64.0 | 1.73x | 29.3 | 106.9 | sc 0/0; redis 0/0 |
| `TOPK.ADD` | ok | 110.5 | 64.0 | 1.73x | 32.8 | 84.1 | sc 0/0; redis 0/0 |
| `TOPK.COUNT` | ok | 110.5 | 64.0 | 1.73x | 30.6 | 76.6 | sc 0/0; redis 0/0 |
| `TOPK.INCRBY` | ok | 110.5 | 64.0 | 1.73x | 31.3 | 77.1 | sc 0/0; redis 0/0 |
| `TOPK.INFO` | ok | 110.5 | 64.0 | 1.73x | 31.0 | 69.7 | sc 0/0; redis 0/0 |
| `TOPK.LIST` | ok | 110.5 | 64.0 | 1.73x | 30.3 | 70.8 | sc 0/0; redis 0/0 |
| `TOPK.QUERY` | ok | 110.5 | 64.0 | 1.73x | 32.1 | 66.1 | sc 0/0; redis 0/0 |
| `TOPK.RESERVE` | expected-error | 110.5 | 64.0 | 1.73x | 29.1 | 65.1 | sc 0/221; redis 0/128 |
| `TS.ADD` | ok | 110.5 | 64.0 | 1.73x | 31.3 | 93.0 | sc 0/0; redis 0/0 |
| `TS.ALTER` | ok | 110.5 | 64.0 | 1.73x | 30.2 | 90.8 | sc 0/0; redis 0/0 |
| `TS.CREATE` | expected-error | 110.5 | 64.0 | 1.73x | 29.6 | 102.1 | sc 0/0; redis 0/128 |
| `TS.CREATERULE` | expected-error | 110.5 | 64.0 | 1.73x | 29.1 | 91.7 | sc 0/0; redis 0/128 |
| `TS.DECRBY` | ok | 110.5 | 64.0 | 1.73x | 29.2 | 89.1 | sc 0/0; redis 0/0 |
| `TS.DEL` | ok | 110.5 | 64.0 | 1.73x | 29.9 | 115.7 | sc 0/0; redis 0/0 |
| `TS.DELETERULE` | expected-error | 110.5 | 64.0 | 1.73x | 28.8 | 101.4 | sc 0/0; redis 0/127 |
| `TS.GET` | ok | 110.5 | 64.0 | 1.73x | 29.8 | 87.2 | sc 0/0; redis 0/0 |
| `TS.INCRBY` | ok | 110.5 | 64.0 | 1.73x | 29.1 | 91.1 | sc 0/0; redis 0/0 |
| `TS.INFO` | ok | 110.5 | 64.0 | 1.73x | 31.1 | 95.3 | sc 0/0; redis 0/0 |
| `TS.MADD` | ok | 110.5 | 64.0 | 1.73x | 29.8 | 91.6 | sc 0/0; redis 0/0 |
| `TS.MGET` | ok | 110.5 | 64.0 | 1.73x | 48.4 | 118.7 | sc 0/0; redis 0/0 |
| `TS.MRANGE` | ok | 110.5 | 64.0 | 1.73x | 677.8 | 324.2 | sc 0/0; redis 0/0 |
| `TS.MREVRANGE` | ok | 110.5 | 64.0 | 1.73x | 692.0 | 336.2 | sc 0/0; redis 0/0 |
| `TS.QUERYINDEX` | ok | 110.5 | 64.0 | 1.73x | 42.3 | 113.9 | sc 0/0; redis 0/0 |
| `TS.RANGE` | ok | 110.5 | 64.0 | 1.73x | 33.0 | 90.3 | sc 0/0; redis 0/0 |
| `TS.REVRANGE` | ok | 110.5 | 64.0 | 1.73x | 30.8 | 92.0 | sc 0/0; redis 0/0 |
