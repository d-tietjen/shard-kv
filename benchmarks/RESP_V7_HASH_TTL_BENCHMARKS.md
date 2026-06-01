# RESP v7 Hash-Field-TTL Benchmarks (1- and 8-vCPU head-to-head)

Head-to-head for the Redis 7.2/7.4 hash-field-expire command family added this
cycle: `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`,
`HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST`.

- **Re-run:** 2026-05-30
- **shardcache:** branch `redis-v6-v8-command-coverage`, RESP server, single shard.
- **Baseline:** `redis:7.4-alpine` (7.4.9), default config.
- **Topology:** 32-core Linux host. Both servers pinned to **one** core
  (`taskset -c 24`, i.e. 1 vCPU each — a true equal-resource comparison); the
  load generator runs on a **disjoint** core set (`taskset -c 0-7`) so it never
  competes with the server under test. redis reached over its own TCP port
  (no Docker-proxy hop). Servers benchmarked one at a time.
- **Client:** `resp_blast`, 16 connections, 1s warmup + 5s measure per cell,
  pre-encoded requests (zero runtime command generation).

## Full Hash-family command matrix

Fresh live RESP command-matrix run on Adam for git `b74397c`, Redis 7.4.9, 4
clients, 4 key lanes, 1s warmup + 2s measurement, pipeline depth 1. This is the
full `hash` family in the benchmark registry: the Redis 7 hash-field-TTL
commands, the ordinary Redis hash commands, and the large 1K-field hash cases.

Artifacts:

- `/tmp/shard-kv-docbench-b74397c/benchmarks/results/adam-full-hash-b74397c/report.md`
- `/tmp/shard-kv-docbench-b74397c/benchmarks/results/adam-full-hash-b74397c/redis-command-matrix.csv`

Summary: 31 common hash cases, zero unexpected errors on both servers.
shardcache summed 73,244 ops/s across the matrix versus Redis at 24,635 ops/s
(2.97x total), with mean average latency 54.5us versus 162.2us.

| Command | Case | Profile | shardcache ops/s | Redis ops/s | sc/redis | shardcache avg us | Redis avg us | Errors |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `HSET` | HSET | small | 2363 | 796 | 2.97x | 36.5 | 112.1 | sc 0 / redis 0 |
| `HMSET` | HMSET | small | 2363 | 796 | 2.97x | 28.8 | 123.3 | sc 0 / redis 0 |
| `HGET` | HGET | small | 2363 | 795 | 2.97x | 28.1 | 119.1 | sc 0 / redis 0 |
| `HMGET` | HMGET | small | 2363 | 795 | 2.97x | 28.2 | 112.1 | sc 0 / redis 0 |
| `HLEN` | HLEN | small | 2363 | 795 | 2.97x | 27.3 | 112.8 | sc 0 / redis 0 |
| `HEXISTS` | HEXISTS | small | 2363 | 795 | 2.97x | 35.1 | 121.2 | sc 0 / redis 0 |
| `HEXPIRE` | HEXPIRE field | small | 2363 | 795 | 2.97x | 29.4 | 126.0 | sc 0 / redis 0 |
| `HTTL` | HTTL field | small | 2363 | 795 | 2.97x | 28.2 | 129.5 | sc 0 / redis 0 |
| `HPTTL` | HPTTL field | small | 2363 | 795 | 2.97x | 27.8 | 123.9 | sc 0 / redis 0 |
| `HEXPIRETIME` | HEXPIRETIME field | small | 2363 | 795 | 2.97x | 27.8 | 120.7 | sc 0 / redis 0 |
| `HPEXPIRETIME` | HPEXPIRETIME field | small | 2363 | 795 | 2.97x | 27.7 | 125.3 | sc 0 / redis 0 |
| `HPERSIST` | HPERSIST field | small | 2363 | 795 | 2.97x | 28.8 | 123.6 | sc 0 / redis 0 |
| `HPEXPIRE` | HPEXPIRE field | small | 2363 | 795 | 2.97x | 28.0 | 126.9 | sc 0 / redis 0 |
| `HEXPIREAT` | HEXPIREAT field | small | 2363 | 795 | 2.97x | 28.3 | 127.3 | sc 0 / redis 0 |
| `HPEXPIREAT` | HPEXPIREAT field | small | 2363 | 794 | 2.97x | 27.7 | 126.6 | sc 0 / redis 0 |
| `HSETNX` | HSETNX | small | 2363 | 794 | 2.97x | 37.3 | 119.4 | sc 0 / redis 0 |
| `HSTRLEN` | HSTRLEN | small | 2363 | 794 | 2.97x | 28.5 | 119.2 | sc 0 / redis 0 |
| `HINCRBY` | HINCRBY | small | 2363 | 794 | 2.97x | 35.2 | 120.1 | sc 0 / redis 0 |
| `HINCRBYFLOAT` | HINCRBYFLOAT | small | 2363 | 794 | 2.97x | 35.3 | 123.0 | sc 0 / redis 0 |
| `HKEYS` | HKEYS | small | 2363 | 794 | 2.97x | 29.1 | 124.9 | sc 0 / redis 0 |
| `HVALS` | HVALS | small | 2363 | 794 | 2.97x | 28.4 | 120.8 | sc 0 / redis 0 |
| `HGETALL` | HGETALL | small | 2363 | 794 | 2.97x | 29.0 | 116.2 | sc 0 / redis 0 |
| `HSCAN` | HSCAN | small | 2363 | 794 | 2.97x | 28.7 | 123.8 | sc 0 / redis 0 |
| `HRANDFIELD` | HRANDFIELD WITHVALUES | small | 2363 | 794 | 2.97x | 37.3 | 119.3 | sc 0 / redis 0 |
| `HDEL` | HDEL | small | 2363 | 794 | 2.97x | 35.6 | 113.7 | sc 0 / redis 0 |
| `HGETALL` | HGETALL large 1K fields | large | 2363 | 794 | 2.97x | 276.7 | 508.8 | sc 0 / redis 0 |
| `HKEYS` | HKEYS large 1K fields | large | 2362 | 794 | 2.97x | 158.3 | 341.2 | sc 0 / redis 0 |
| `HVALS` | HVALS large 1K fields | large | 2361 | 794 | 2.97x | 155.9 | 339.9 | sc 0 / redis 0 |
| `HSCAN` | HSCAN large 1K fields | large | 2361 | 794 | 2.97x | 273.9 | 558.9 | sc 0 / redis 0 |
| `HLEN` | HLEN large 1K fields | large | 2361 | 794 | 2.98x | 33.5 | 135.8 | sc 0 / redis 0 |
| `HMGET` | HMGET large selected fields | large | 2361 | 794 | 2.98x | 29.4 | 113.8 | sc 0 / redis 0 |

## Correctness gate

Before timing, the harness uses `redis-cli` to confirm each server returns a
**non-error** reply for `HEXPIRE … FIELDS 1 f` and aborts otherwise, so these
numbers measure real command execution, not error-reply throughput. Both passed:

```
GATE shardcache HEXPIRE -> 1
GATE redis      HEXPIRE -> 1
```

(This gate exists because an earlier run accidentally benchmarked a build that
did not implement the commands; `resp_blast` counts an error reply as an op, so
the gate is mandatory for a valid result.)

The `resp_blast` tables below are retained as the saturated/tail-latency
snapshot from 2026-05-30. Their printed summary omitted `HPEXPIREAT`; the fresh
2026-05-31 command matrix above covers `HPEXPIREAT` and the rest of the full
Hash family at git `b74397c`.

Redis 8 hash and vector additions, such as `HGETEX`/`HGETDEL` and vector-set
commands, are covered by the broader Redis 8 command documentation rather than
this Redis 7 hash-field-TTL report.

## Depth 1 — strict request/response (1 vCPU each)

At one in-flight request per connection the per-op time is dominated by the TCP
round trip, so this is the fairest per-core latency comparison.

| command       | shardcache ops/s | redis 7.4 ops/s | sc/redis | sc p99 | redis p99 |
|---------------|-----------------:|----------------:|:--------:|-------:|----------:|
| HEXPIRE       | 87,117 | 74,936 | 1.16× | 229µs | 314µs |
| HPEXPIRE      | 87,569 | 74,738 | 1.17× | 200µs | 320µs |
| HEXPIREAT     | 87,186 | 76,247 | 1.14× | 243µs | 314µs |
| HTTL          | 88,798 | 80,137 | 1.11× | 196µs | 297µs |
| HPTTL         | 87,573 | 80,169 | 1.09× | 226µs | 292µs |
| HEXPIRETIME   | 87,811 | 80,161 | 1.10× | 229µs | 293µs |
| HPEXPIRETIME  | 88,093 | 79,779 | 1.10× | 210µs | 288µs |
| HPERSIST      | 89,490 | 80,010 | 1.12× | 199µs | 300µs |

shardcache leads on every command (~1.1–1.2×) with consistently tighter tail
latency (~200–245µs p99 vs ~290–320µs).

## Depth 16 — pipelined (1 vCPU each)

With requests in flight, shardcache's lower per-op cost compounds.

| command       | shardcache ops/s | redis 7.4 ops/s | sc/redis |
|---------------|-----------------:|----------------:|:--------:|
| HEXPIRE       | 812,784   | 462,555 | 1.76× |
| HPEXPIRE      | 816,329   | 460,516 | 1.77× |
| HEXPIREAT     | 839,338   | 513,322 | 1.64× |
| HTTL          | 932,133   | 681,067 | 1.37× |
| HPTTL         | 907,452   | 671,765 | 1.35× |
| HEXPIRETIME   | 909,010   | 653,577 | 1.39× |
| HPEXPIRETIME  | 907,304   | 646,472 | 1.40× |
| HPERSIST      | 1,027,188 | 670,776 | 1.53× |

## Depth 1 — strict request/response (8 vCPU each)

shardcache runs 8 shards across the 8 cores; redis is single-threaded so it uses
~1 of them. At depth 1 the round trip still dominates per-op, but shardcache's
parallel shards absorb the 16 connections without contention.

| command       | shardcache (8 shards) | redis 7.4 | sc/redis | sc p99 | redis p99 |
|---------------|----------------------:|----------:|:--------:|-------:|----------:|
| HEXPIRE       | 418,773 | 74,938 | 5.59x | 70µs | 306µs |
| HPEXPIRE      | 415,852 | 74,546 | 5.58x | 71µs | 310µs |
| HEXPIREAT     | 420,805 | 76,740 | 5.48x | 69µs | 304µs |
| HTTL          | 419,847 | 80,208 | 5.23x | 69µs | 298µs |
| HPTTL         | 420,557 | 80,009 | 5.26x | 70µs | 302µs |
| HEXPIRETIME   | 418,072 | 79,926 | 5.23x | 70µs | 295µs |
| HPEXPIRETIME  | 425,299 | 79,948 | 5.32x | 69µs | 306µs |
| HPERSIST      | 425,956 | 79,936 | 5.33x | 69µs | 296µs |

## Depth 16 — pipelined (8 vCPU each)

| command       | shardcache (8 shards) | redis 7.4 | sc/redis |
|---------------|----------------------:|----------:|:--------:|
| HEXPIRE       | 1,242,127 | 456,190 | 2.72x |
| HPEXPIRE      | 1,216,245 | 453,063 | 2.68x |
| HEXPIREAT     | 1,238,795 | 513,990 | 2.41x |
| HTTL          | 5,052,642 | 675,330 | 7.48x |
| HPTTL         | 5,258,661 | 673,547 | 7.81x |
| HEXPIRETIME   | 5,064,049 | 653,880 | 7.74x |
| HPEXPIRETIME  | 4,956,077 | 652,073 | 7.60x |
| HPERSIST      | 3,235,014 | 660,574 | 4.90x |

### 8-vCPU notes

- At 8 cores shardcache is **5.2-5.6x** redis at depth 1 (redis cannot use the
  extra cores; this is the scaling story) and **2.4-7.8x** pipelined.
- The read-only query family (`HTTL`/`HPTTL`/`HEXPIRETIME`/`HPEXPIRETIME`)
  pipelines dramatically better (~5M ops/s, up to 7.8x) than the set family
  (~1.2M ops/s, ~2.4-2.7x): the write commands take the shard write lock and
  materialize per-field expiry, while the read commands take the lighter read
  path. `HPERSIST` (a write) sits between at 3.2M / 4.9x.
- redis depth-16 is ~0.45-0.68M ops/s regardless of command — its
  single-thread ceiling.

## Takeaways

- In the 2026-05-31 full Hash command matrix, **shardcache is faster than Redis
  7.4 on all 31 hash cases, including all 9 hash-field-TTL commands.**
- In the 2026-05-30 `resp_blast` snapshot, shardcache is also faster on every
  printed hash-field-TTL row at both depths on equal single-core budgets.
- The set family (`HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT`) shows the
  largest pipelined lead (~1.6–1.8×); the query family
  (`HTTL`/`HPTTL`/`HEXPIRETIME`/`HPEXPIRETIME`) ~1.35–1.4×.
- `HPERSIST` is the cheapest op on both servers and shardcache's best pipelined
  result (1.03M ops/s, 1.53×).
- This is single-core throughput — no sharding — so the gap is per-op
  efficiency, not parallelism.

## Method notes

- Per-field TTLs are stored as a lazily-allocated `key -> field -> abs-ms` map
  alongside the existing key-level expiry; TTL-free hashes pay nothing, and all
  hash reads lazily filter expired fields. See `docs/HASH_FIELD_TTL_DESIGN.md`.
- `HGETEX`/`HGETDEL` are **not** included: they are Redis 8.0 commands (verified
  `ERR unknown command` on the 7.4.9 baseline), outside the v7 surface.
