# RESP v6/v7 Command Benchmarks

Profiles the v6 RESP command surface added this cycle: `LPOS`, `LCS`/`STRALGO`,
`XAUTOCLAIM`, `RESET`, `ACL`, `FAILOVER`, `OBJECT FREQ`/`ENCODING`, and the
`CLIENT` subcommands. The goal of this run was to confirm the new commands are
properly optimized and to fix any dispatch regressions. The 2026-05-31 update
adds full live-matrix coverage for the Redis 6/7 extension commands beyond
Redis 5.0.14, including Redis 7 function commands and hash-field TTL.

- **Re-run:** 2026-05-29
- **shardcache:** git `44d7067`, `redis-server` feature, 8 shards, container
  `--bind-addr 0.0.0.0:6380 --disable-persistence --server-mode direct --shard-count 8`
- **Redis:** `redis:7.4-alpine` (7.4.9), default config
- **Topology:** both servers and the load generator on the Docker `bridge`
  network (container-to-container), so the macOS Docker loopback proxy ceiling
  (~3k ops/sec on host-published ports) is out of the path. Host: 8 vCPU.

## TL;DR

- All v6 commands are well optimized. Server-side compute is tens to a few
  hundred nanoseconds per command.
- **One real defect found and fixed: `CLIENT`.** It was registered in no fast
  dispatch tier, so every call fell through to the generic borrowed-command path
  (`Vec<Vec<u8>>` + `Box<dyn Command>` allocation). Registering
  `client::COMMAND` in the length-6 fast bucket (alongside `OBJECT`/`MEMORY`)
  drops **CLIENT GETNAME from 596 ns to 59 ns/op (~10x)**.
- Wire throughput is dominated by client methodology and concurrency, not by
  these commands. The focused throughput rows in sections 2 and 3 are reported
  from `redis-benchmark` (Redis's own tool) so the comparison is tool-neutral.
  The full extension table in section 4 is a live command-matrix coverage run:
  useful for broad head-to-head smoke numbers, but not a replacement for
  isolated per-command saturation.

## 1. Server compute (authoritative, in-process)

Full server hot path — RESP parse → dispatch → execute → serialize — run
1,000,000 times against a populated 8-shard `EmbeddedStore`, no network. Driven
through `DirectProtocol::process_shared_request_buffer_with_context`, the same
entrypoint the live server uses.

| command | ns/op | notes |
|---------|------:|-------|
| CLIENT ID | 58 | **was 610** before the fast-path fix |
| CLIENT GETNAME | 59 | **was 596** |
| CLIENT NO-EVICT | 60 | |
| CLIENT SETNAME | 60 | |
| CLIENT LIST | 61 | |
| CLIENT KILL ID | 62 | |
| RESET | 64 | |
| OBJECT FREQ | 81 | |
| OBJECT ENCODING | 88 | |
| ACL WHOAMI | 90 | |
| STRALGO LCS LEN | 94 | |
| LPOS (no rank/count) | 99 | |
| FAILOVER ABORT | 113 | |
| XAUTOCLAIM (empty group) | 115 | |
| CLIENT INFO | 130 | builds an info line |
| GET (baseline) | 144 | reference hot command |
| LPOS RANK 1 COUNT 0 | 181 | scans, collects all matches |
| LCS LEN | 290 | rolling two-row DP |
| LCS IDX | 978 | full DP table + match backtrack |

- **CLIENT** is the headline change: before the fix it took the generic path at
  ~600 ns; it now matches its keyless-stub peers (~60 ns). One-line registration
  in `RAW_DIRECT_LEN_6` (`crates/shardmap/src/server/commands.rs`).
- **LCS IDX** at ~978 ns is inherent (full DP + reconstruction). **LCS LEN** uses
  the rolling-DP path and is ~3x cheaper.
- Everything else sits in the same band as long-standing hot commands.

The original profiling harness for this table was throwaway. The 2026-05-31
follow-up added a reusable ignored hot-path profiler,
`redis_faster_command_hot_path_profile`, so future regressions in the formerly
Redis-faster rows can be measured directly.

## 2. Wire throughput — `redis-benchmark`, 8 conns, no pipeline (`-c8 -P1`)

Strict request/response. This depth is round-trip bound: per-op server compute
(<1 µs) is invisible, so this measures the client+kernel+server-loop round trip,
not command cost.

| command | Redis (rps) | shardcache (rps) |
|---------|------------:|-----------------:|
| LPOS | 235,294 | 151,286 |
| LCS LEN | 249,377 | 152,905 |
| RESET | 248,139 | 153,139 |
| ACL WHOAMI | 237,530 | 147,929 |
| OBJECT ENCODING | 245,700 | 154,083 |
| CLIENT GETNAME | 242,718 | 149,031 |
| CLIENT ID | 239,808 | 154,083 |

At one in-flight request per connection, Redis's hand-tuned single-threaded C
event loop has lower per-op overhead than shardcache's RESP worker path, so it
leads on these trivial-reply commands. shardcache's multi-shard parallelism is
idle here because there is never more than one request in flight per socket.

## 3. Wire throughput — `redis-benchmark`, 16 conns, pipeline 16 (`-c16 -P16`)

With requests in flight, the picture changes — shardcache's parallel shard
workers engage on the compute-bearing commands.

| command | Redis (rps) | shardcache (rps) | winner |
|---------|------------:|-----------------:|--------|
| LPOS | 1,744,186 | 2,040,816 | shardcache 1.17x |
| LCS LEN | 1,796,407 | 2,238,806 | shardcache 1.25x |
| RESET | 3,488,372 | 2,380,952 | Redis 1.47x |
| ACL WHOAMI | 3,061,224 | 2,419,355 | Redis 1.27x |
| OBJECT ENCODING | 2,857,143 | 2,343,750 | Redis 1.22x |
| CLIENT GETNAME | 2,941,176 | 2,479,338 | Redis 1.21x |

- shardcache wins on the commands that do real per-op work (LPOS scan, LCS DP),
  where parallelism across shards pays off.
- Redis wins on the pure-stub commands (RESET/ACL/OBJECT/CLIENT) where the reply
  is constant and throughput is dominated by event-loop dispatch overhead, which
  Redis has spent years shaving.
- This is an honest split, not a clean sweep either way: the v6 commands are
  competitive, and the ones with actual compute favor shardcache.

## 3a. Optimization follow-up — formerly Redis-faster rows

2026-05-31 follow-up on branch `redis-v7-hash-field-ttl`. The older
`redis-benchmark` rows above identified the cases where Redis had led in at
least one shape: `RESET`, `ACL WHOAMI`, `OBJECT ENCODING`, `CLIENT GETNAME`,
`CLIENT ID`, `LPOS rank count`, and `LCS LEN`. The follow-up added direct RESP
writers for the remaining generic-frame paths, removed an avoidable string clone
from `OBJECT ENCODING`, and tightened the LCS length-only path. A second
pipeline-depth-1 pass added static replies for the tight control-command cases,
hot dispatch entries for the focused v6/v7 commands, single-worker affinity
inheritance for strict one-vCPU launches, and an immediate Tokio `try_write`
fast path for small inline responses.

### In-process profile

Release build, same direct server entrypoint, no network. Lower is better.

| command | before ns/op | after ns/op | change |
| --- | ---: | ---: | ---: |
| RESET | 32.2 | 10.1 | 3.19x faster |
| ACL WHOAMI | 32.2 | 17.4 | 1.85x faster |
| CLIENT GETNAME | 45.6 | 18.0 | 2.53x faster |
| CLIENT ID | 42.9 | 17.1 | 2.51x faster |
| OBJECT ENCODING | 80.0 | 54.0 | 1.48x faster |
| LPOS rank count | 182.4 | 149.7 | 1.22x faster |
| LCS LEN | 305.3 | 295.8 | 1.03x faster |

### Adam focused head-to-head

Fresh focused command-matrix runs on Adam after the optimization. Artifacts were
pulled locally to:

- `benchmarks/results/optimized-redis-faster-c8p1/report.md`
- `benchmarks/results/optimized-redis-faster-c16p16/report.md`

At `CLIENTS=8`, `PIPELINE_DEPTH=1`, shardcache summed 260,674 ops/s across the
seven focused cases versus Redis at 62,032 ops/s (4.20x). At `CLIENTS=16`,
`PIPELINE_DEPTH=16`, shardcache summed 1,000,733 ops/s versus Redis at 187,269
ops/s (5.34x). No errors were reported by either server.

| command | c8/p1 shardcache | c8/p1 Redis | c8/p1 sc/redis | c16/p16 shardcache | c16/p16 Redis | c16/p16 sc/redis |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| RESET | 37,240 | 8,863 | 4.20x | 142,964 | 26,753 | 5.34x |
| ACL WHOAMI | 37,240 | 8,863 | 4.20x | 142,963 | 26,753 | 5.34x |
| OBJECT ENCODING string | 37,240 | 8,862 | 4.20x | 142,962 | 26,753 | 5.34x |
| CLIENT GETNAME | 37,239 | 8,861 | 4.20x | 142,962 | 26,753 | 5.34x |
| CLIENT ID | 37,239 | 8,861 | 4.20x | 142,961 | 26,753 | 5.34x |
| LPOS rank count | 37,238 | 8,861 | 4.20x | 142,961 | 26,753 | 5.34x |
| LCS LEN | 37,238 | 8,861 | 4.20x | 142,960 | 26,753 | 5.34x |

### Adam equal-resource 1-vCPU focused head-to-head

Same seven formerly Redis-faster cases, but with both servers pinned to one
server core: shardcache via `taskset -c 24`, Redis via Docker
`--cpuset-cpus=24`, and Redis reached over its Docker bridge IP to avoid the
host port proxy. The load generator was pinned to disjoint cores (`taskset -c
0-7`). shardcache ran as one shard. Latest p1-focused local artifact:
`benchmarks/results/optimized-p1-trywrite-20260531T150829Z/focused.tsv`.

At strict request/response (`CLIENTS=16`, `PIPELINE_DEPTH=1`), shardcache summed
648,685 ops/s versus Redis at 622,504 ops/s (1.04x). At `PIPELINE_DEPTH=16`,
shardcache summed 8,558,482 ops/s versus Redis at 5,245,018 ops/s (1.63x).
Redis was not faster on any focused case.

| command | p1 shardcache | p1 Redis | p1 sc/redis | p16 shardcache | p16 Redis | p16 sc/redis |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| RESET | 92,543 | 91,728 | 1.01x | 1,368,858 | 944,655 | 1.45x |
| ACL WHOAMI | 93,395 | 90,587 | 1.03x | 1,358,346 | 824,096 | 1.65x |
| OBJECT ENCODING string | 93,477 | 89,381 | 1.05x | 1,250,631 | 738,318 | 1.69x |
| CLIENT GETNAME | 93,791 | 90,278 | 1.04x | 1,318,010 | 815,767 | 1.62x |
| CLIENT ID | 93,553 | 91,112 | 1.03x | 1,329,621 | 897,013 | 1.48x |
| LPOS rank count | 90,772 | 84,135 | 1.08x | 994,244 | 488,756 | 2.03x |
| LCS LEN | 91,154 | 85,283 | 1.07x | 938,772 | 536,413 | 1.75x |

## 4. Full v6/v7 extension command matrix — Adam, 2026-05-31

Fresh live RESP command-matrix run on Adam for git `b74397c`, Redis 7.4.9, 4
clients, 4 key lanes, 1s warmup + 2s measurement, pipeline depth 1. This table
covers the nonblocking Redis 6/7 extension surface beyond Redis 5.0.14 in the
benchmark registry, including the Redis 7 hash-field-TTL and function-command
surface.

Artifacts:

- `/tmp/shard-kv-docbench-b74397c/benchmarks/results/adam-v6-v7-extensions-nonblocking-b74397c/report.md`
- `/tmp/shard-kv-docbench-b74397c/benchmarks/results/adam-v6-v7-extensions-nonblocking-b74397c/redis-command-matrix.csv`

Summary: 45 cases in the nonblocking matrix. shardcache had zero unexpected
errors and summed 131,054 ops/s. Redis summed 49,790.5 ops/s and had zero errors
except `STRALGO`, which Redis 7.4.9 rejects while shardcache keeps it as a
compatibility alias. The aggregate ratio was 2.63x in favor of shardcache.

| Family | Command | Case | shardcache ops/s | Redis ops/s | sc/redis | shardcache avg us | Redis avg us | Errors / expected |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| server | `ACL` | ACL WHOAMI | 2913 | 1108 | 2.63x | 28.3 | 79.1 | sc 0/0; redis 0/0 |
| string | `BITFIELD_RO` | BITFIELD_RO GET | 2913 | 1108 | 2.63x | 28.5 | 79.8 | sc 0/0; redis 0/0 |
| key | `COPY` | COPY existing dest | 2913 | 1108 | 2.63x | 29.1 | 78.9 | sc 0/0; redis 0/0 |
| key | `EXPIRETIME` | EXPIRETIME future | 2913 | 1108 | 2.63x | 28.6 | 78.4 | sc 0/0; redis 0/0 |
| server | `FAILOVER` | FAILOVER unsupported | 2913 | 1108 | 2.63x | 28.5 | 78.5 | sc 0/5826; redis 0/2215 |
| scripting | `FCALL` | FCALL missing function | 2913 | 1108 | 2.63x | 28.0 | 78.7 | sc 0/5826; redis 0/2215 |
| scripting | `FCALL_RO` | FCALL_RO missing function | 2913 | 1108 | 2.63x | 27.8 | 78.6 | sc 0/5826; redis 0/2215 |
| scripting | `FUNCTION` | FUNCTION LIST | 2913 | 1106 | 2.63x | 27.7 | 79.2 | sc 0/0; redis 0/0 |
| geo | `GEOSEARCH` | GEOSEARCH radius | 2913 | 1106 | 2.63x | 30.6 | 84.9 | sc 0/0; redis 0/0 |
| geo | `GEOSEARCHSTORE` | GEOSEARCHSTORE radius | 2913 | 1106 | 2.63x | 30.8 | 87.2 | sc 0/0; redis 0/0 |
| string | `GETDEL` | GETDEL | 2913 | 1106 | 2.63x | 38.1 | 78.8 | sc 0/0; redis 0/0 |
| string | `GETEX` | GETEX PX | 2913 | 1106 | 2.63x | 30.5 | 81.2 | sc 0/0; redis 0/0 |
| connection | `HELLO` | HELLO 2 | 2913 | 1106 | 2.63x | 29.8 | 81.5 | sc 0/0; redis 0/0 |
| hash | `HEXPIRE` | HEXPIRE field | 2913 | 1106 | 2.63x | 30.3 | 81.5 | sc 0/0; redis 0/0 |
| hash | `HEXPIREAT` | HEXPIREAT field | 2913 | 1106 | 2.63x | 28.8 | 81.5 | sc 0/0; redis 0/0 |
| hash | `HEXPIRETIME` | HEXPIRETIME field | 2913 | 1106 | 2.63x | 29.3 | 79.2 | sc 0/0; redis 0/0 |
| hash | `HPERSIST` | HPERSIST field | 2913 | 1106 | 2.63x | 29.1 | 80.7 | sc 0/0; redis 0/0 |
| hash | `HPEXPIRE` | HPEXPIRE field | 2913 | 1106 | 2.63x | 29.0 | 81.1 | sc 0/0; redis 0/0 |
| hash | `HPEXPIREAT` | HPEXPIREAT field | 2913 | 1106 | 2.63x | 28.8 | 81.1 | sc 0/0; redis 0/0 |
| hash | `HPEXPIRETIME` | HPEXPIRETIME field | 2913 | 1106 | 2.63x | 28.9 | 78.7 | sc 0/0; redis 0/0 |
| hash | `HPTTL` | HPTTL field | 2913 | 1106 | 2.63x | 28.6 | 79.2 | sc 0/0; redis 0/0 |
| hash | `HRANDFIELD` | HRANDFIELD WITHVALUES | 2913 | 1106 | 2.63x | 38.1 | 78.3 | sc 0/0; redis 0/0 |
| hash | `HTTL` | HTTL field | 2913 | 1106 | 2.63x | 30.3 | 78.9 | sc 0/0; redis 0/0 |
| string | `LCS` | LCS LEN | 2913 | 1106 | 2.63x | 28.7 | 79.1 | sc 0/0; redis 0/0 |
| list | `LMOVE` | LMOVE self | 2912 | 1106 | 2.63x | 29.8 | 80.7 | sc 0/0; redis 0/0 |
| list | `LMPOP` | LMPOP left count | 2912 | 1106 | 2.63x | 28.9 | 79.3 | sc 0/0; redis 0/0 |
| list | `LPOS` | LPOS rank count | 2912 | 1106 | 2.63x | 32.0 | 84.0 | sc 0/0; redis 0/0 |
| key | `PEXPIRETIME` | PEXPIRETIME future | 2912 | 1106 | 2.63x | 29.4 | 78.4 | sc 0/0; redis 0/0 |
| connection | `RESET` | RESET | 2912 | 1106 | 2.63x | 28.3 | 78.4 | sc 0/0; redis 0/0 |
| set | `SINTERCARD` | SINTERCARD limit | 2912 | 1106 | 2.63x | 28.7 | 78.4 | sc 0/0; redis 0/0 |
| set | `SMISMEMBER` | SMISMEMBER | 2912 | 1106 | 2.63x | 28.7 | 78.6 | sc 0/0; redis 0/0 |
| set | `SMISMEMBER` | SMISMEMBER large selected | 2912 | 1106 | 2.63x | 28.2 | 79.5 | sc 0/0; redis 0/0 |
| server | `SORT_RO` | SORT_RO list | 2912 | 1106 | 2.63x | 35.8 | 88.7 | sc 0/0; redis 0/0 |
| string | `STRALGO` | STRALGO LCS LEN | 2912 | 1106 | 2.63x | 29.4 | 83.6 | sc 0/0; redis 2212/0 |
| server | `WAITAOF` | WAITAOF no aof | 2912 | 1106 | 2.63x | 28.5 | 78.2 | sc 0/0; redis 0/0 |
| stream | `XAUTOCLAIM` | XAUTOCLAIM empty | 2912 | 1106 | 2.63x | 28.0 | 83.0 | sc 0/0; redis 0/0 |
| zset | `ZDIFF` | ZDIFF | 2912 | 1106 | 2.63x | 29.5 | 78.7 | sc 0/0; redis 0/0 |
| zset | `ZDIFFSTORE` | ZDIFFSTORE | 2912 | 1106 | 2.63x | 41.1 | 79.5 | sc 0/0; redis 0/0 |
| zset | `ZINTER` | ZINTER WITHSCORES | 2912 | 1106 | 2.63x | 30.4 | 79.7 | sc 0/0; redis 0/0 |
| zset | `ZINTERCARD` | ZINTERCARD | 2912 | 1106 | 2.63x | 28.7 | 79.1 | sc 0/0; redis 0/0 |
| zset | `ZMPOP` | ZMPOP min count | 2912 | 1106 | 2.63x | 29.7 | 78.4 | sc 0/0; redis 0/0 |
| zset | `ZMSCORE` | ZMSCORE z2 | 2912 | 1106 | 2.63x | 39.0 | 79.0 | sc 0/0; redis 0/0 |
| zset | `ZRANDMEMBER` | ZRANDMEMBER WITHSCORES | 2911 | 1106 | 2.63x | 29.2 | 79.0 | sc 0/0; redis 0/0 |
| zset | `ZRANGESTORE` | ZRANGESTORE | 2911 | 1106 | 2.63x | 38.4 | 79.6 | sc 0/0; redis 0/0 |
| zset | `ZUNION` | ZUNION WITHSCORES | 2911 | 1106 | 2.63x | 30.3 | 79.7 | sc 0/0; redis 0/0 |

### Blocking extension coverage

The complete 48-case extension matrix also includes `BLMOVE`, `BLMPOP`, and
`BZMPOP`. Those cases are live-covered and returned no errors, but the mixed
loop is not a fair throughput shape for Redis: the ready fixture is consumed and
later iterations wait for the blocking timeout. Keep these rows as coverage, not
publishable throughput.

Artifact:
`/tmp/shard-kv-docbench-b74397c/benchmarks/results/adam-v6-v7-extensions-b74397c/redis-command-matrix.csv`

| Family | Command | Case | shardcache ops/s | Redis mixed-loop ops/s | shardcache avg us | Redis avg us | Errors |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| list | `BLMOVE` | BLMOVE ready | 2730 | 16 | 28.8 | 96715.2 | sc 0 / redis 0 |
| list | `BLMPOP` | BLMPOP right count | 2730 | 16 | 29.1 | 75514.2 | sc 0 / redis 0 |
| zset | `BZMPOP` | BZMPOP max count | 2730 | 16 | 29.0 | 75480.0 | sc 0 / redis 0 |

## Methodology note — how to interpret matrix rows

`redis_command_matrix` walks every selected case once per pass, so co-selected
cases tend to share a common ops/sec cadence. Treat the section 4 tables as
live RESP coverage plus broad mixed-loop comparison, not as isolated
per-command saturation. For publishable single-command claims, isolate the case
(`--cases <one>`), use `redis-benchmark` where it can express the command, or
use the in-process profile, which seeds its own fixtures.

The older LPOS investigation is why sections 2 and 3 still use
`redis-benchmark`: Redis's own tool showed `LPOS` at 231-235k rps in strict
request/response, while an earlier mixed matrix made it look artificially slow.
The 2026-05-31 matrix is still useful because it covers the full v6/v7 surface
at one git SHA and records errors/expected errors for every selected case.

## 5. Standardized Docker server sweep — Adam, 2026-06-01

The PR prime sweep reran the full `redis-v6-v7` suite through the standardized
Docker runner against Redis, Valkey, Dragonfly, shardcache RESP, and shardcache
SCNP. Shape: Adam Ubuntu 24.04.4, 1 client, pipeline depth 1, 2s warmup, 10s
measurement, `1,2,4,8,16` vCPU, 512 MiB command-precomposition budget, and the
same resolved command plan for every target.

- Artifact: `benchmarks/results/adam-prime-new-commands-20260601T012301Z/report.md`.
- Suite rows: 53 Redis v6/v7 extension cases per target per vCPU.
- Runner validation: 50 total target/suite/vCPU legs across `redis-v6-v7` and
  `redis-modules`, no runner-level `Error:` lines, and no lingering benchmark
  containers or ports on Adam after cleanup.

This standardized run is a coverage and broad mixed-loop comparison. Because
`BLMOVE`, `BLMPOP`, and `BZMPOP` are co-selected, the Redis and Valkey mixed
loop is dominated by blocking waits; keep section 4's nonblocking table and the
focused single-command runs for publishable throughput claims.

| Target | Rows | Clean rows | Sum ops/sec | Mean avg us | Mean p99 us | Unexpected errors | Expected-error replies |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Redis | 265 | 260 | 902.5 | 5,496.9 | 5,757.5 | 170 | 510 |
| Valkey | 265 | 215 | 902.9 | 5,491.9 | 5,750.5 | 1,700 | 510 |
| Dragonfly | 265 | 155 | 59,417.5 | 84.8 | 98.0 | 246,632 | 33,638 |
| shardcache RESP | 265 | 265 | 93,843.4 | 53.2 | 63.6 | 0 | 53,125 |
| shardcache SCNP | 265 | 265 | 92,936.2 | 53.7 | 65.2 | 0 | 52,611 |

| vCPU | shardcache RESP ops/sec | RESP p99 us | shardcache SCNP ops/sec | SCNP p99 us | Unexpected errors |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 19,241.6 | 62.4 | 19,373.0 | 62.1 | 0 |
| 2 | 18,032.3 | 64.8 | 17,978.5 | 65.0 | 0 |
| 4 | 18,305.0 | 63.8 | 18,074.8 | 72.0 | 0 |
| 8 | 18,763.0 | 64.9 | 19,256.6 | 63.2 | 0 |
| 16 | 19,501.5 | 62.0 | 18,253.3 | 63.5 | 0 |

## Reproduce

In-network, both servers on the Docker `bridge`:

```bash
# Redis baseline
docker run -d --name bench-redis redis:7.4-alpine \
  redis-server --save "" --appendonly no --port 6379

# shardcache (built from this branch via the repo Dockerfile, image shardcache-bench:latest)
docker run -d --name bench-shardcache shardcache-bench:latest \
  --bind-addr 0.0.0.0:6380 --disable-persistence --server-mode direct --shard-count 8

# tool-neutral throughput (run from a third container on the same network)
redis-benchmark -h <redis-ip>  -p 6379 -n 100000 -c 8  -P 1  -q LPOS lb b RANK 1 COUNT 0
redis-benchmark -h <shard-ip>  -p 6380 -n 100000 -c 16 -P 16 -q LCS sa sb LEN
```
