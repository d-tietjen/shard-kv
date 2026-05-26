# Redis Head-to-Head Benchmarks

Updated: 2026-05-26

This is the curated entry point for fast-cache head-to-head benchmarks against
Redis-compatible servers. It combines the publishable 1-vCPU and 16-vCPU TCP
server rows with the current Adam command-matrix artifacts.

Raw per-case CSVs stay in `benchmarks/results/`. This document summarizes the
rows we should cite in README, release notes, and 0.2.0 readiness work.

## Scope

Backends:

| Backend | Meaning |
| --- | --- |
| FCNP direct | fast-cache native TCP protocol with client-side routing to shard-owned ports |
| FCNP shared | fast-cache native TCP protocol through the shared listener |
| RESP | fast-cache Redis-compatible TCP protocol |
| Redis | Redis OSS TCP baseline |
| Valkey | Valkey TCP baseline |
| Dragonfly | Dragonfly TCP baseline |

Server CPU views:

| View | Meaning |
| --- | --- |
| 1 vCPU | fast-cache pinned to one server CPU with one shard. Redis remains its normal single-threaded baseline. |
| 16 vCPU | fast-cache pinned to CPUs `0-15` with 16 shards. Redis remains the single-threaded drop-in baseline. |
| C16/P16 | 16 benchmark clients with pipeline depth 16. This is a load shape, not a server CPU count. |

Cells in the TCP saturation tables are `ops/sec @ measured server vCPU`.
Command-matrix tables report `sum ops/sec` across deterministic command cases
and `mean avg us` across those cases.

## Supported Command Surface

The supported Redis command surface is generated from
`benchmarks/src/redis_command_cases.rs` and tracked in
[`docs/REDIS_COMPATIBILITY.md`](../docs/REDIS_COMPATIBILITY.md). The current
manifest has `222` supported commands, `289` live benchmark cases, and `0`
missing Redis 5.0.14 commands. The `222` commands are the complete command set
covered by the head-to-head benchmark artifacts summarized below.

| Metric | Count |
| --- | ---: |
| Supported commands | 222 |
| Redis 5.0.14 commands supported and live-benchmarked | 200 |
| Supported extensions beyond Redis 5.0.14 | 22 |
| Missing Redis 5.0.14 commands | 0 |
| Live benchmark cases | 289 |
| Expected-error benchmark cases | 9 |
| Large-profile cases | 29 |
| Destructive-profile cases | 2 |
| Keyspace-wide benchmark cases | 8 |

| Family | Commands | Count |
| --- | --- | ---: |
| connection | `AUTH`, `CLIENT`, `ECHO`, `HELLO`, `PING`, `SELECT` | 6 |
| geo | `GEOADD`, `GEODIST`, `GEOHASH`, `GEOPOS`, `GEORADIUS`, `GEORADIUSBYMEMBER`, `GEORADIUSBYMEMBER_RO`, `GEORADIUS_RO` | 8 |
| hash | `HDEL`, `HEXISTS`, `HGET`, `HGETALL`, `HINCRBY`, `HINCRBYFLOAT`, `HKEYS`, `HLEN`, `HMGET`, `HMSET`, `HRANDFIELD`, `HSCAN`, `HSET`, `HSETNX`, `HSTRLEN`, `HVALS` | 16 |
| hyperloglog | `PFADD`, `PFCOUNT`, `PFDEBUG`, `PFMERGE`, `PFSELFTEST` | 5 |
| key | `COPY`, `DEL`, `DUMP`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `EXPIRETIME`, `KEYS`, `OBJECT`, `PERSIST`, `PEXPIRE`, `PEXPIREAT`, `PEXPIRETIME`, `PTTL`, `RANDOMKEY`, `RENAME`, `RENAMENX`, `RESTORE`, `RESTORE-ASKING`, `SCAN`, `TOUCH`, `TTL`, `TYPE`, `UNLINK` | 24 |
| list | `BLMOVE`, `BLMPOP`, `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `LINDEX`, `LINSERT`, `LLEN`, `LMOVE`, `LMPOP`, `LPOP`, `LPUSH`, `LPUSHX`, `LRANGE`, `LREM`, `LSET`, `LTRIM`, `RPOP`, `RPOPLPUSH`, `RPUSH`, `RPUSHX` | 21 |
| pubsub | `PSUBSCRIBE`, `PUBLISH`, `PUBSUB`, `PUNSUBSCRIBE`, `SUBSCRIBE`, `UNSUBSCRIBE` | 6 |
| scripting | `EVAL`, `EVALSHA`, `SCRIPT` | 3 |
| server | `ASKING`, `BGREWRITEAOF`, `BGSAVE`, `CLUSTER`, `COMMAND`, `CONFIG`, `DBSIZE`, `DEBUG`, `FLUSHALL`, `FLUSHDB`, `HOST:`, `INFO`, `LASTSAVE`, `LATENCY`, `LOLWUT`, `MEMORY`, `MIGRATE`, `MODULE`, `MONITOR`, `MOVE`, `POST`, `PSYNC`, `READONLY`, `READWRITE`, `REPLCONF`, `REPLICAOF`, `ROLE`, `SAVE`, `SHUTDOWN`, `SLAVEOF`, `SLOWLOG`, `SORT`, `SWAPDB`, `SYNC`, `TIME`, `WAIT` | 36 |
| set | `SADD`, `SCARD`, `SDIFF`, `SDIFFSTORE`, `SINTER`, `SINTERSTORE`, `SISMEMBER`, `SMEMBERS`, `SMISMEMBER`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SREM`, `SSCAN`, `SUNION`, `SUNIONSTORE` | 16 |
| stream | `XACK`, `XADD`, `XCLAIM`, `XDEL`, `XGROUP`, `XINFO`, `XLEN`, `XPENDING`, `XRANGE`, `XREAD`, `XREADGROUP`, `XREVRANGE`, `XSETID`, `XTRIM` | 14 |
| string | `APPEND`, `BITCOUNT`, `BITFIELD`, `BITOP`, `BITPOS`, `DECR`, `DECRBY`, `GET`, `GETBIT`, `GETDEL`, `GETEX`, `GETRANGE`, `GETSET`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `MGET`, `MSET`, `MSETNX`, `PSETEX`, `SET`, `SETBIT`, `SETEX`, `SETNX`, `SETRANGE`, `STRLEN`, `SUBSTR` | 27 |
| transaction | `DISCARD`, `EXEC`, `MULTI`, `UNWATCH`, `WATCH` | 5 |
| zset | `BZMPOP`, `BZPOPMAX`, `BZPOPMIN`, `ZADD`, `ZCARD`, `ZCOUNT`, `ZDIFF`, `ZDIFFSTORE`, `ZINCRBY`, `ZINTER`, `ZINTERCARD`, `ZINTERSTORE`, `ZLEXCOUNT`, `ZMPOP`, `ZMSCORE`, `ZPOPMAX`, `ZPOPMIN`, `ZRANDMEMBER`, `ZRANGE`, `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZRANGESTORE`, `ZRANK`, `ZREM`, `ZREMRANGEBYLEX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYLEX`, `ZREVRANGEBYSCORE`, `ZREVRANK`, `ZSCAN`, `ZSCORE`, `ZUNION`, `ZUNIONSTORE` | 35 |

Expected-error commands are included because they are part of standalone
compatibility semantics and have live RESP benchmark coverage. Those commands
are `CLUSTER`, `HOST:`, `MIGRATE`, `MONITOR`, `MOVE`, `POST`, `PSYNC`,
`SHUTDOWN`, and `SYNC`.

## Per-Command Saved Head-to-Head Benchmarks

These tables list every supported Redis command. Command rows are aggregated
by summing `ops/sec` across that command's benchmark cases in the source
artifact, matching the rollup method used by the matrix reports. Ratios are
fast-cache over Redis for the same command and shape. `n/a` means the command
is supported, but there is no saved Redis head-to-head row for that exact
benchmark shape yet.

The 16-shard opcode artifacts predate the later Redis 5 family runs. Stream,
geo, pubsub, hyperloglog, and scripting rows in the sharded C16/P1 and C16/P16
tables use isolated Adam family reruns because the first mixed-family gap-fill
run was throttled by slow stateful/diagnostic cases and made Redis look
implausibly low. FCNP-direct shard-port rows for these families are still not
published because the direct-port attempt failed during stream setup (`XLEN`).
The pubsub family reruns are throughput probes; Redis reported one transient
ack/read error on a few subscription-control cases.

| Shape | Commands with saved Redis head-to-head rows | Commands with FCNP-direct rows | Total supported commands |
| --- | ---: | ---: | ---: |
| 1-vCPU C16/P16 | 174 | n/a | 222 |
| 16-shard/opcode C16/P1 plus isolated family reruns | 182 | 145 | 222 |
| 16-shard/opcode C16/P16 plus isolated family reruns | 181 | 145 | 222 |

### 1-vCPU C16/P16

| Family | Command | Cases | fast-cache ops/sec | Redis ops/sec | FC/Redis | Source |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| connection | `AUTH` | n/a | n/a | n/a | n/a | n/a |
| connection | `CLIENT` | n/a | n/a | n/a | n/a | n/a |
| connection | `ECHO` | n/a | n/a | n/a | n/a | n/a |
| connection | `HELLO` | n/a | n/a | n/a | n/a | n/a |
| connection | `PING` | n/a | n/a | n/a | n/a | n/a |
| connection | `SELECT` | n/a | n/a | n/a | n/a | n/a |
| string | `APPEND` | 1 | 6,084.4 | 5,669.6 | 1.07x | 1vCPU family |
| string | `BITCOUNT` | 1 | 6,084.4 | 5,669.6 | 1.07x | 1vCPU family |
| string | `BITFIELD` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `BITOP` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `BITPOS` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `DECR` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `DECRBY` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `GET` | 3 | 18,250.0 | 17,006.4 | 1.07x | 1vCPU family |
| string | `GETBIT` | 1 | 6,084.4 | 5,669.6 | 1.07x | 1vCPU family |
| string | `GETDEL` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `GETEX` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `GETRANGE` | 2 | 12,166.2 | 11,337.8 | 1.07x | 1vCPU family |
| string | `GETSET` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `INCR` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `INCRBY` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `INCRBYFLOAT` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `MGET` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `MSET` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `MSETNX` | 1 | 6,083.4 | 5,668.6 | 1.07x | 1vCPU family |
| string | `PSETEX` | 1 | 6,084.4 | 5,669.6 | 1.07x | 1vCPU family |
| string | `SET` | 6 | 36,503.2 | 34,015.2 | 1.07x | 1vCPU family |
| string | `SETBIT` | 1 | 6,084.4 | 5,669.6 | 1.07x | 1vCPU family |
| string | `SETEX` | 1 | 6,084.4 | 5,669.6 | 1.07x | 1vCPU family |
| string | `SETNX` | 1 | 6,084.4 | 5,669.6 | 1.07x | 1vCPU family |
| string | `SETRANGE` | 1 | 6,084.4 | 5,669.6 | 1.07x | 1vCPU family |
| string | `STRLEN` | 2 | 12,166.4 | 11,337.8 | 1.07x | 1vCPU family |
| string | `SUBSTR` | 1 | 6,084.4 | 5,669.6 | 1.07x | 1vCPU family |
| key | `COPY` | 1 | 16,528.0 | 13,341.8 | 1.24x | 1vCPU family |
| key | `DEL` | 1 | 16,528.0 | 13,342.0 | 1.24x | 1vCPU family |
| key | `DUMP` | 1 | 16,529.6 | 13,343.4 | 1.24x | 1vCPU family |
| key | `EXISTS` | 1 | 16,528.0 | 13,342.2 | 1.24x | 1vCPU family |
| key | `EXPIRE` | 2 | 33,060.2 | 26,687.4 | 1.24x | 1vCPU family |
| key | `EXPIREAT` | 1 | 16,530.2 | 13,343.6 | 1.24x | 1vCPU family |
| key | `EXPIRETIME` | 1 | 16,530.0 | 13,343.6 | 1.24x | 1vCPU family |
| key | `KEYS` | 2 | 4,424.4 | 1,489.6 | 2.97x | 1vCPU keyspace |
| key | `OBJECT` | 1 | 16,529.6 | 13,343.4 | 1.24x | 1vCPU family |
| key | `PERSIST` | 1 | 16,530.4 | 13,344.0 | 1.24x | 1vCPU family |
| key | `PEXPIRE` | 2 | 33,060.2 | 26,687.4 | 1.24x | 1vCPU family |
| key | `PEXPIREAT` | 1 | 16,530.2 | 13,343.6 | 1.24x | 1vCPU family |
| key | `PEXPIRETIME` | 1 | 16,530.0 | 13,343.6 | 1.24x | 1vCPU family |
| key | `PTTL` | 1 | 16,530.4 | 13,344.0 | 1.24x | 1vCPU family |
| key | `RANDOMKEY` | 1 | 2,212.2 | 745.0 | 2.97x | 1vCPU keyspace |
| key | `RENAME` | 2 | 33,056.0 | 26,684.4 | 1.24x | 1vCPU family |
| key | `RENAMENX` | 1 | 16,528.0 | 13,342.0 | 1.24x | 1vCPU family |
| key | `RESTORE` | 1 | 16,529.6 | 13,343.2 | 1.24x | 1vCPU family |
| key | `RESTORE-ASKING` | 1 | 16,529.4 | 13,343.2 | 1.24x | 1vCPU family |
| key | `SCAN` | 3 | 6,636.6 | 2,234.4 | 2.97x | 1vCPU keyspace |
| key | `TOUCH` | 1 | 16,528.0 | 13,342.2 | 1.24x | 1vCPU family |
| key | `TTL` | 1 | 16,530.4 | 13,344.0 | 1.24x | 1vCPU family |
| key | `TYPE` | 1 | 16,530.0 | 13,343.4 | 1.24x | 1vCPU family |
| key | `UNLINK` | 1 | 16,528.0 | 13,342.0 | 1.24x | 1vCPU family |
| hash | `HDEL` | 1 | 4,474.8 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HEXISTS` | 1 | 4,475.0 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HGET` | 1 | 4,475.0 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HGETALL` | 2 | 8,949.2 | 3,329.4 | 2.69x | 1vCPU family |
| hash | `HINCRBY` | 1 | 4,475.0 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HINCRBYFLOAT` | 1 | 4,474.8 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HKEYS` | 2 | 8,949.0 | 3,329.4 | 2.69x | 1vCPU family |
| hash | `HLEN` | 2 | 8,949.2 | 3,329.4 | 2.69x | 1vCPU family |
| hash | `HMGET` | 2 | 8,949.2 | 3,329.4 | 2.69x | 1vCPU family |
| hash | `HMSET` | 1 | 4,475.0 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HRANDFIELD` | 1 | 4,474.8 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HSCAN` | 2 | 8,949.0 | 3,329.4 | 2.69x | 1vCPU family |
| hash | `HSET` | 1 | 4,475.2 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HSETNX` | 1 | 4,475.0 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HSTRLEN` | 1 | 4,475.0 | 1,665.2 | 2.69x | 1vCPU family |
| hash | `HVALS` | 2 | 8,949.0 | 3,329.4 | 2.69x | 1vCPU family |
| list | `BLMOVE` | 1 | 32,135.8 | 38.4 | 836.87x | 1vCPU family |
| list | `BLMPOP` | 1 | 32,135.8 | 38.4 | 836.87x | 1vCPU family |
| list | `BLPOP` | 1 | 32,135.8 | 38.4 | 836.87x | 1vCPU family |
| list | `BRPOP` | 1 | 32,135.8 | 38.4 | 836.87x | 1vCPU family |
| list | `BRPOPLPUSH` | 1 | 32,136.0 | 38.4 | 836.88x | 1vCPU family |
| list | `LINDEX` | 2 | 18,483.2 | 14,403.2 | 1.28x | 1vCPU family |
| list | `LINSERT` | 1 | 9,242.6 | 7,202.0 | 1.28x | 1vCPU family |
| list | `LLEN` | 2 | 18,483.2 | 14,403.4 | 1.28x | 1vCPU family |
| list | `LMOVE` | 1 | 9,242.6 | 7,202.4 | 1.28x | 1vCPU family |
| list | `LMPOP` | 1 | 9,240.6 | 7,201.0 | 1.28x | 1vCPU family |
| list | `LPOP` | 1 | 9,242.4 | 7,202.0 | 1.28x | 1vCPU family |
| list | `LPUSH` | 1 | 9,242.6 | 7,202.6 | 1.28x | 1vCPU family |
| list | `LPUSHX` | 1 | 9,242.2 | 7,202.0 | 1.28x | 1vCPU family |
| list | `LRANGE` | 2 | 18,483.2 | 14,403.4 | 1.28x | 1vCPU family |
| list | `LREM` | 1 | 9,242.6 | 7,202.2 | 1.28x | 1vCPU family |
| list | `LSET` | 1 | 9,242.6 | 7,202.2 | 1.28x | 1vCPU family |
| list | `LTRIM` | 1 | 9,242.4 | 7,202.0 | 1.28x | 1vCPU family |
| list | `RPOP` | 1 | 9,242.4 | 7,202.0 | 1.28x | 1vCPU family |
| list | `RPOPLPUSH` | 1 | 9,242.6 | 7,202.4 | 1.28x | 1vCPU family |
| list | `RPUSH` | 4 | 36,966.0 | 28,806.4 | 1.28x | 1vCPU family |
| list | `RPUSHX` | 1 | 9,242.2 | 7,202.0 | 1.28x | 1vCPU family |
| set | `SADD` | 2 | 15,500.6 | 7,085.6 | 2.19x | 1vCPU family |
| set | `SCARD` | 2 | 15,498.8 | 7,083.8 | 2.19x | 1vCPU family |
| set | `SDIFF` | 1 | 7,750.2 | 3,542.6 | 2.19x | 1vCPU family |
| set | `SDIFFSTORE` | 1 | 7,750.2 | 3,542.6 | 2.19x | 1vCPU family |
| set | `SINTER` | 1 | 7,750.2 | 3,542.6 | 2.19x | 1vCPU family |
| set | `SINTERSTORE` | 1 | 7,750.2 | 3,542.6 | 2.19x | 1vCPU family |
| set | `SISMEMBER` | 1 | 7,750.2 | 3,542.8 | 2.19x | 1vCPU family |
| set | `SMEMBERS` | 2 | 15,498.8 | 7,083.6 | 2.19x | 1vCPU family |
| set | `SMISMEMBER` | 2 | 15,498.6 | 7,083.8 | 2.19x | 1vCPU family |
| set | `SMOVE` | 1 | 7,750.2 | 3,542.6 | 2.19x | 1vCPU family |
| set | `SPOP` | 1 | 7,748.6 | 3,541.2 | 2.19x | 1vCPU family |
| set | `SRANDMEMBER` | 2 | 15,498.6 | 7,083.4 | 2.19x | 1vCPU family |
| set | `SREM` | 1 | 7,750.2 | 3,542.6 | 2.19x | 1vCPU family |
| set | `SSCAN` | 2 | 15,498.6 | 7,083.4 | 2.19x | 1vCPU family |
| set | `SUNION` | 1 | 7,750.2 | 3,542.6 | 2.19x | 1vCPU family |
| set | `SUNIONSTORE` | 1 | 7,750.2 | 3,542.6 | 2.19x | 1vCPU family |
| zset | `BZMPOP` | 1 | 32,135.4 | 38.4 | 836.86x | 1vCPU family |
| zset | `BZPOPMAX` | 1 | 32,135.4 | 38.4 | 836.86x | 1vCPU family |
| zset | `BZPOPMIN` | 1 | 32,135.8 | 38.4 | 836.87x | 1vCPU family |
| zset | `ZADD` | 10 | 32,988.6 | 13,746.8 | 2.40x | 1vCPU family |
| zset | `ZCARD` | 1 | 3,299.4 | 1,375.4 | 2.40x | 1vCPU family |
| zset | `ZCOUNT` | 2 | 6,596.8 | 2,748.2 | 2.40x | 1vCPU family |
| zset | `ZDIFF` | 1 | 3,298.2 | 1,374.0 | 2.40x | 1vCPU family |
| zset | `ZDIFFSTORE` | 1 | 3,298.2 | 1,374.0 | 2.40x | 1vCPU family |
| zset | `ZINCRBY` | 1 | 3,299.4 | 1,375.4 | 2.40x | 1vCPU family |
| zset | `ZINTER` | 1 | 3,298.2 | 1,374.0 | 2.40x | 1vCPU family |
| zset | `ZINTERCARD` | 1 | 3,298.2 | 1,374.0 | 2.40x | 1vCPU family |
| zset | `ZINTERSTORE` | 1 | 3,298.4 | 1,374.0 | 2.40x | 1vCPU family |
| zset | `ZLEXCOUNT` | 1 | 3,298.8 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZMPOP` | 1 | 3,298.0 | 1,373.8 | 2.40x | 1vCPU family |
| zset | `ZMSCORE` | 1 | 3,299.0 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZPOPMAX` | 1 | 3,299.4 | 1,375.4 | 2.40x | 1vCPU family |
| zset | `ZPOPMIN` | 1 | 3,299.4 | 1,375.4 | 2.40x | 1vCPU family |
| zset | `ZRANDMEMBER` | 1 | 3,298.2 | 1,374.0 | 2.40x | 1vCPU family |
| zset | `ZRANGE` | 9 | 29,686.2 | 12,368.0 | 2.40x | 1vCPU family |
| zset | `ZRANGEBYLEX` | 1 | 3,298.8 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZRANGEBYSCORE` | 1 | 3,299.0 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZRANGESTORE` | 1 | 3,298.8 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZRANK` | 2 | 6,596.8 | 2,748.2 | 2.40x | 1vCPU family |
| zset | `ZREM` | 1 | 3,299.4 | 1,375.4 | 2.40x | 1vCPU family |
| zset | `ZREMRANGEBYLEX` | 1 | 3,298.8 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZREMRANGEBYRANK` | 1 | 3,299.0 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZREMRANGEBYSCORE` | 1 | 3,299.0 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZREVRANGE` | 1 | 3,299.4 | 1,375.4 | 2.40x | 1vCPU family |
| zset | `ZREVRANGEBYLEX` | 1 | 3,298.8 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZREVRANGEBYSCORE` | 1 | 3,299.0 | 1,374.8 | 2.40x | 1vCPU family |
| zset | `ZREVRANK` | 2 | 6,596.8 | 2,748.2 | 2.40x | 1vCPU family |
| zset | `ZSCAN` | 2 | 6,596.0 | 2,747.6 | 2.40x | 1vCPU family |
| zset | `ZSCORE` | 3 | 9,895.8 | 4,123.0 | 2.40x | 1vCPU family |
| zset | `ZUNION` | 1 | 3,298.2 | 1,374.0 | 2.40x | 1vCPU family |
| zset | `ZUNIONSTORE` | 1 | 3,298.4 | 1,374.0 | 2.40x | 1vCPU family |
| stream | `XACK` | 1 | 22,530.0 | 19,028.0 | 1.18x | 1vCPU family |
| stream | `XADD` | 1 | 22,530.8 | 19,028.8 | 1.18x | 1vCPU family |
| stream | `XCLAIM` | 1 | 22,530.0 | 19,028.0 | 1.18x | 1vCPU family |
| stream | `XDEL` | 1 | 22,530.4 | 19,028.6 | 1.18x | 1vCPU family |
| stream | `XGROUP` | 1 | 22,530.4 | 19,028.4 | 1.18x | 1vCPU family |
| stream | `XINFO` | 1 | 22,529.8 | 19,027.6 | 1.18x | 1vCPU family |
| stream | `XLEN` | 1 | 22,530.6 | 19,028.8 | 1.18x | 1vCPU family |
| stream | `XPENDING` | 1 | 22,530.2 | 19,028.2 | 1.18x | 1vCPU family |
| stream | `XRANGE` | 1 | 22,530.6 | 19,028.8 | 1.18x | 1vCPU family |
| stream | `XREAD` | 1 | 22,530.4 | 19,028.4 | 1.18x | 1vCPU family |
| stream | `XREADGROUP` | 1 | 22,530.4 | 19,028.4 | 1.18x | 1vCPU family |
| stream | `XREVRANGE` | 1 | 22,530.4 | 19,028.6 | 1.18x | 1vCPU family |
| stream | `XSETID` | 1 | 22,530.4 | 19,028.6 | 1.18x | 1vCPU family |
| stream | `XTRIM` | 1 | 22,530.4 | 19,028.6 | 1.18x | 1vCPU family |
| geo | `GEOADD` | 1 | 40,386.4 | 26,148.8 | 1.54x | 1vCPU family |
| geo | `GEODIST` | 1 | 40,386.4 | 26,148.8 | 1.54x | 1vCPU family |
| geo | `GEOHASH` | 1 | 40,386.4 | 26,148.8 | 1.54x | 1vCPU family |
| geo | `GEOPOS` | 1 | 40,386.4 | 26,148.6 | 1.54x | 1vCPU family |
| geo | `GEORADIUS` | 1 | 40,386.4 | 26,148.6 | 1.54x | 1vCPU family |
| geo | `GEORADIUSBYMEMBER` | 1 | 40,386.4 | 26,148.4 | 1.54x | 1vCPU family |
| geo | `GEORADIUSBYMEMBER_RO` | 1 | 40,386.4 | 26,148.4 | 1.54x | 1vCPU family |
| geo | `GEORADIUS_RO` | 1 | 40,386.4 | 26,148.4 | 1.54x | 1vCPU family |
| pubsub | `PSUBSCRIBE` | 1 | 846.0 | 5.6 | 151.07x | 1vCPU data |
| pubsub | `PUBLISH` | 1 | 846.0 | 5.6 | 151.07x | 1vCPU data |
| pubsub | `PUBSUB` | n/a | n/a | n/a | n/a | n/a |
| pubsub | `PUNSUBSCRIBE` | 1 | 846.0 | 5.6 | 151.07x | 1vCPU data |
| pubsub | `SUBSCRIBE` | 1 | 846.0 | 5.6 | 151.07x | 1vCPU data |
| pubsub | `UNSUBSCRIBE` | 1 | 846.0 | 5.6 | 151.07x | 1vCPU data |
| hyperloglog | `PFADD` | 1 | 57,966.0 | 38,288.6 | 1.51x | 1vCPU family |
| hyperloglog | `PFCOUNT` | 1 | 57,965.8 | 38,288.6 | 1.51x | 1vCPU family |
| hyperloglog | `PFDEBUG` | 1 | 845.8 | 4.7 | 179.96x | 1vCPU data |
| hyperloglog | `PFMERGE` | 1 | 57,965.6 | 38,288.6 | 1.51x | 1vCPU family |
| hyperloglog | `PFSELFTEST` | 1 | 845.8 | 4.7 | 179.96x | 1vCPU data |
| scripting | `EVAL` | 1 | 59,134.8 | 41,691.4 | 1.42x | 1vCPU scripting |
| scripting | `EVALSHA` | 1 | 59,134.4 | 41,691.4 | 1.42x | 1vCPU scripting |
| scripting | `SCRIPT` | 1 | 59,134.4 | 41,691.0 | 1.42x | 1vCPU scripting |
| transaction | `DISCARD` | n/a | n/a | n/a | n/a | n/a |
| transaction | `EXEC` | n/a | n/a | n/a | n/a | n/a |
| transaction | `MULTI` | n/a | n/a | n/a | n/a | n/a |
| transaction | `UNWATCH` | n/a | n/a | n/a | n/a | n/a |
| transaction | `WATCH` | n/a | n/a | n/a | n/a | n/a |
| server | `ASKING` | n/a | n/a | n/a | n/a | n/a |
| server | `BGREWRITEAOF` | n/a | n/a | n/a | n/a | n/a |
| server | `BGSAVE` | n/a | n/a | n/a | n/a | n/a |
| server | `CLUSTER` | n/a | n/a | n/a | n/a | n/a |
| server | `COMMAND` | n/a | n/a | n/a | n/a | n/a |
| server | `CONFIG` | n/a | n/a | n/a | n/a | n/a |
| server | `DBSIZE` | n/a | n/a | n/a | n/a | n/a |
| server | `DEBUG` | n/a | n/a | n/a | n/a | n/a |
| server | `FLUSHALL` | n/a | n/a | n/a | n/a | n/a |
| server | `FLUSHDB` | n/a | n/a | n/a | n/a | n/a |
| server | `HOST:` | n/a | n/a | n/a | n/a | n/a |
| server | `INFO` | n/a | n/a | n/a | n/a | n/a |
| server | `LASTSAVE` | n/a | n/a | n/a | n/a | n/a |
| server | `LATENCY` | n/a | n/a | n/a | n/a | n/a |
| server | `LOLWUT` | n/a | n/a | n/a | n/a | n/a |
| server | `MEMORY` | n/a | n/a | n/a | n/a | n/a |
| server | `MIGRATE` | n/a | n/a | n/a | n/a | n/a |
| server | `MODULE` | n/a | n/a | n/a | n/a | n/a |
| server | `MONITOR` | n/a | n/a | n/a | n/a | n/a |
| server | `MOVE` | n/a | n/a | n/a | n/a | n/a |
| server | `POST` | n/a | n/a | n/a | n/a | n/a |
| server | `PSYNC` | n/a | n/a | n/a | n/a | n/a |
| server | `READONLY` | n/a | n/a | n/a | n/a | n/a |
| server | `READWRITE` | n/a | n/a | n/a | n/a | n/a |
| server | `REPLCONF` | n/a | n/a | n/a | n/a | n/a |
| server | `REPLICAOF` | n/a | n/a | n/a | n/a | n/a |
| server | `ROLE` | n/a | n/a | n/a | n/a | n/a |
| server | `SAVE` | n/a | n/a | n/a | n/a | n/a |
| server | `SHUTDOWN` | n/a | n/a | n/a | n/a | n/a |
| server | `SLAVEOF` | n/a | n/a | n/a | n/a | n/a |
| server | `SLOWLOG` | n/a | n/a | n/a | n/a | n/a |
| server | `SORT` | n/a | n/a | n/a | n/a | n/a |
| server | `SWAPDB` | n/a | n/a | n/a | n/a | n/a |
| server | `SYNC` | n/a | n/a | n/a | n/a | n/a |
| server | `TIME` | n/a | n/a | n/a | n/a | n/a |
| server | `WAIT` | n/a | n/a | n/a | n/a | n/a |

### 16-Shard C16/P1

| Family | Command | Cases | FCNP direct ops/sec | RESP ops/sec | Redis ops/sec | FCNP/Redis | RESP/Redis | Source |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| connection | `AUTH` | 1 | 2,206.0 | 1,826.0 | 141.0 | 15.65x | 12.95x | opcode C16/P1 |
| connection | `CLIENT` | 5 | 11,030.0 | 9,130.0 | 704.0 | 15.67x | 12.97x | opcode C16/P1 |
| connection | `ECHO` | 1 | 2,206.0 | 1,826.0 | 140.9 | 15.66x | 12.96x | opcode C16/P1 |
| connection | `HELLO` | 1 | 2,206.0 | 1,826.0 | 140.8 | 15.67x | 12.97x | opcode C16/P1 |
| connection | `PING` | 1 | 2,206.0 | 1,826.0 | 141.0 | 15.65x | 12.95x | opcode C16/P1 |
| connection | `SELECT` | 1 | 2,206.0 | 1,826.0 | 140.8 | 15.67x | 12.97x | opcode C16/P1 |
| string | `APPEND` | 1 | 2,205.4 | 1,825.6 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `BITCOUNT` | 1 | 2,205.3 | 1,825.5 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `BITFIELD` | 1 | 2,205.3 | 1,825.5 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `BITOP` | 1 | 2,205.3 | 1,825.5 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `BITPOS` | 1 | 2,205.3 | 1,825.5 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `DECR` | 1 | 2,205.3 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| string | `DECRBY` | 1 | 2,205.3 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| string | `GET` | 3 | 6,615.4 | 5,475.3 | 419.4 | 15.77x | 13.06x | opcode C16/P1 |
| string | `GETBIT` | 1 | 2,205.3 | 1,825.6 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `GETDEL` | 1 | 2,205.3 | 1,825.3 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `GETEX` | 1 | 2,205.3 | 1,825.4 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `GETRANGE` | 2 | 4,410.0 | 3,650.4 | 279.7 | 15.77x | 13.05x | opcode C16/P1 |
| string | `GETSET` | 1 | 2,205.3 | 1,825.5 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `INCR` | 1 | 2,205.3 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| string | `INCRBY` | 1 | 2,205.3 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| string | `INCRBYFLOAT` | 1 | 2,205.3 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| string | `MGET` | 1 | 2,205.2 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| string | `MSET` | 1 | 2,205.3 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| string | `MSETNX` | 1 | 2,205.2 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| string | `PSETEX` | 1 | 2,205.8 | 1,825.7 | 140.3 | 15.72x | 13.01x | opcode C16/P1 |
| string | `SET` | 6 | 13,232.4 | 10,952.4 | 840.3 | 15.75x | 13.03x | opcode C16/P1 |
| string | `SETBIT` | 1 | 2,205.3 | 1,825.5 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `SETEX` | 1 | 2,205.8 | 1,825.7 | 140.4 | 15.71x | 13.00x | opcode C16/P1 |
| string | `SETNX` | 1 | 2,205.6 | 1,825.7 | 140.3 | 15.72x | 13.01x | opcode C16/P1 |
| string | `SETRANGE` | 1 | 2,205.3 | 1,825.6 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| string | `STRLEN` | 2 | 4,410.0 | 3,650.4 | 279.7 | 15.77x | 13.05x | opcode C16/P1 |
| string | `SUBSTR` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `COPY` | 1 | 2,205.4 | 1,825.6 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `DEL` | 1 | 2,205.4 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `DUMP` | 1 | 2,205.6 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `EXISTS` | 1 | 2,205.5 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `EXPIRE` | 2 | 4,411.2 | 3,651.4 | 280.5 | 15.73x | 13.02x | opcode C16/P1 |
| key | `EXPIREAT` | 1 | 2,205.6 | 1,825.7 | 140.3 | 15.72x | 13.01x | opcode C16/P1 |
| key | `EXPIRETIME` | 1 | 2,205.6 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `KEYS` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `OBJECT` | 1 | 2,205.6 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `PERSIST` | 1 | 2,205.6 | 1,825.7 | 140.3 | 15.72x | 13.01x | opcode C16/P1 |
| key | `PEXPIRE` | 2 | 4,411.2 | 3,651.4 | 280.5 | 15.73x | 13.02x | opcode C16/P1 |
| key | `PEXPIREAT` | 1 | 2,205.6 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `PEXPIRETIME` | 1 | 2,205.6 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `PTTL` | 1 | 2,205.6 | 1,825.7 | 140.3 | 15.72x | 13.01x | opcode C16/P1 |
| key | `RANDOMKEY` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `RENAME` | 2 | 4,410.9 | 3,651.4 | 280.4 | 15.73x | 13.02x | opcode C16/P1 |
| key | `RENAMENX` | 1 | 2,205.4 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `RESTORE` | 1 | 2,205.5 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `RESTORE-ASKING` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `SCAN` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `TOUCH` | 1 | 2,205.5 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `TTL` | 1 | 2,205.6 | 1,825.7 | 140.3 | 15.72x | 13.01x | opcode C16/P1 |
| key | `TYPE` | 1 | 2,205.6 | 1,825.7 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| key | `UNLINK` | 1 | 2,205.4 | 1,825.6 | 140.2 | 15.73x | 13.02x | opcode C16/P1 |
| hash | `HDEL` | 1 | 2,205.1 | 1,825.2 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HEXISTS` | 1 | 2,205.2 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HGET` | 1 | 2,205.2 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HGETALL` | 2 | 4,409.8 | 3,650.0 | 279.6 | 15.77x | 13.05x | opcode C16/P1 |
| hash | `HINCRBY` | 1 | 2,205.1 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HINCRBYFLOAT` | 1 | 2,205.1 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HKEYS` | 2 | 4,409.8 | 3,650.0 | 279.6 | 15.77x | 13.05x | opcode C16/P1 |
| hash | `HLEN` | 2 | 4,409.9 | 3,650.0 | 279.5 | 15.78x | 13.06x | opcode C16/P1 |
| hash | `HMGET` | 2 | 4,409.9 | 3,649.9 | 279.5 | 15.78x | 13.06x | opcode C16/P1 |
| hash | `HMSET` | 1 | 2,205.2 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HRANDFIELD` | 1 | 2,205.1 | 1,825.2 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HSCAN` | 2 | 4,409.8 | 3,649.9 | 279.6 | 15.77x | 13.05x | opcode C16/P1 |
| hash | `HSET` | 1 | 2,205.2 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HSETNX` | 1 | 2,205.1 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HSTRLEN` | 1 | 2,205.1 | 1,825.3 | 140.1 | 15.74x | 13.03x | opcode C16/P1 |
| hash | `HVALS` | 2 | 4,409.8 | 3,649.9 | 279.6 | 15.77x | 13.05x | opcode C16/P1 |
| list | `BLMOVE` | 1 | 2,205.0 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `BLMPOP` | 1 | 2,205.0 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `BLPOP` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `BRPOP` | 1 | 2,205.0 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `BRPOPLPUSH` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| list | `LINDEX` | 2 | 4,409.8 | 3,649.8 | 279.4 | 15.78x | 13.06x | opcode C16/P1 |
| list | `LINSERT` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `LLEN` | 2 | 4,409.8 | 3,649.8 | 279.4 | 15.78x | 13.06x | opcode C16/P1 |
| list | `LMOVE` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `LMPOP` | 1 | 2,205.0 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `LPOP` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `LPUSH` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `LPUSHX` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `LRANGE` | 2 | 4,409.8 | 3,649.8 | 279.4 | 15.78x | 13.06x | opcode C16/P1 |
| list | `LREM` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `LSET` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `LTRIM` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `RPOP` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `RPOPLPUSH` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `RPUSH` | 4 | 8,820.2 | 7,300.8 | 560.0 | 15.75x | 13.04x | opcode C16/P1 |
| list | `RPUSHX` | 1 | 2,205.1 | 1,825.2 | 140.0 | 15.75x | 13.04x | opcode C16/P1 |
| set | `SADD` | 2 | 4,410.0 | 3,650.3 | 279.6 | 15.77x | 13.06x | opcode C16/P1 |
| set | `SCARD` | 2 | 4,409.6 | 3,649.7 | 279.2 | 15.79x | 13.07x | opcode C16/P1 |
| set | `SDIFF` | 1 | 2,205.0 | 1,825.1 | 139.8 | 15.77x | 13.06x | opcode C16/P1 |
| set | `SDIFFSTORE` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| set | `SINTER` | 1 | 2,205.0 | 1,825.1 | 139.8 | 15.77x | 13.06x | opcode C16/P1 |
| set | `SINTERSTORE` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| set | `SISMEMBER` | 1 | 2,205.0 | 1,825.1 | 139.8 | 15.77x | 13.06x | opcode C16/P1 |
| set | `SMEMBERS` | 2 | 4,409.6 | 3,649.7 | 279.2 | 15.79x | 13.07x | opcode C16/P1 |
| set | `SMISMEMBER` | 2 | 4,409.6 | 3,649.7 | 279.2 | 15.79x | 13.07x | opcode C16/P1 |
| set | `SMOVE` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| set | `SPOP` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| set | `SRANDMEMBER` | 2 | 4,409.6 | 3,649.7 | 279.1 | 15.80x | 13.08x | opcode C16/P1 |
| set | `SREM` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| set | `SSCAN` | 2 | 4,409.6 | 3,649.7 | 279.1 | 15.80x | 13.08x | opcode C16/P1 |
| set | `SUNION` | 1 | 2,205.0 | 1,825.1 | 139.8 | 15.77x | 13.06x | opcode C16/P1 |
| set | `SUNIONSTORE` | 1 | 2,205.0 | 1,825.1 | 139.8 | 15.77x | 13.06x | opcode C16/P1 |
| zset | `BZMPOP` | 1 | 2,204.9 | 1,824.8 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `BZPOPMAX` | 1 | 2,204.9 | 1,824.8 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `BZPOPMIN` | 1 | 2,204.9 | 1,824.8 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `ZADD` | 10 | 22,049.8 | 18,249.8 | 1,396.4 | 15.79x | 13.07x | opcode C16/P1 |
| zset | `ZCARD` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZCOUNT` | 2 | 4,409.5 | 3,649.7 | 279.1 | 15.80x | 13.08x | opcode C16/P1 |
| zset | `ZDIFF` | 1 | 2,205.0 | 1,824.9 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `ZDIFFSTORE` | 1 | 2,205.0 | 1,824.9 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `ZINCRBY` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZINTER` | 1 | 2,205.0 | 1,824.9 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `ZINTERCARD` | 1 | 2,205.0 | 1,824.9 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `ZINTERSTORE` | 1 | 2,205.0 | 1,824.9 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `ZLEXCOUNT` | 1 | 2,205.0 | 1,825.0 | 139.6 | 15.80x | 13.07x | opcode C16/P1 |
| zset | `ZMPOP` | 1 | 2,204.9 | 1,824.8 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `ZMSCORE` | 1 | 2,205.0 | 1,825.0 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZPOPMAX` | 1 | 2,205.0 | 1,825.0 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZPOPMIN` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZRANDMEMBER` | 1 | 2,204.9 | 1,824.9 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `ZRANGE` | 9 | 19,844.0 | 16,424.2 | 1,256.0 | 15.80x | 13.08x | opcode C16/P1 |
| zset | `ZRANGEBYLEX` | 1 | 2,205.0 | 1,825.0 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZRANGEBYSCORE` | 1 | 2,205.0 | 1,825.0 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZRANGESTORE` | 1 | 2,205.0 | 1,825.0 | 139.6 | 15.80x | 13.07x | opcode C16/P1 |
| zset | `ZRANK` | 2 | 4,409.5 | 3,649.6 | 279.1 | 15.80x | 13.08x | opcode C16/P1 |
| zset | `ZREM` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZREMRANGEBYLEX` | 1 | 2,205.0 | 1,825.0 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZREMRANGEBYRANK` | 1 | 2,205.0 | 1,825.0 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZREMRANGEBYSCORE` | 1 | 2,205.0 | 1,825.0 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZREVRANGE` | 1 | 2,205.0 | 1,825.1 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZREVRANGEBYLEX` | 1 | 2,205.0 | 1,825.0 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZREVRANGEBYSCORE` | 1 | 2,205.0 | 1,825.0 | 139.7 | 15.78x | 13.06x | opcode C16/P1 |
| zset | `ZREVRANK` | 2 | 4,409.5 | 3,649.6 | 279.1 | 15.80x | 13.08x | opcode C16/P1 |
| zset | `ZSCAN` | 2 | 4,409.5 | 3,649.5 | 279.1 | 15.80x | 13.08x | opcode C16/P1 |
| zset | `ZSCORE` | 3 | 6,614.5 | 5,474.6 | 418.8 | 15.79x | 13.07x | opcode C16/P1 |
| zset | `ZUNION` | 1 | 2,205.0 | 1,824.9 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| zset | `ZUNIONSTORE` | 1 | 2,205.0 | 1,825.0 | 139.5 | 15.81x | 13.08x | opcode C16/P1 |
| stream | `XACK` | 1 | n/a | 35,397.6 | 4,714.0 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XADD` | 1 | n/a | 35,400.4 | 4,716.2 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XCLAIM` | 1 | n/a | 35,397.8 | 4,714.0 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XDEL` | 1 | n/a | 35,399.2 | 4,715.6 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XGROUP` | 1 | n/a | 35,398.0 | 4,714.2 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XINFO` | 1 | n/a | 35,397.4 | 4,713.2 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XLEN` | 1 | n/a | 35,399.8 | 4,716.2 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XPENDING` | 1 | n/a | 35,397.8 | 4,714.0 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XRANGE` | 1 | n/a | 35,399.6 | 4,715.8 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XREAD` | 1 | n/a | 35,398.4 | 4,714.6 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XREADGROUP` | 1 | n/a | 35,398.0 | 4,714.2 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XREVRANGE` | 1 | n/a | 35,399.2 | 4,715.6 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XSETID` | 1 | n/a | 35,398.4 | 4,715.0 | n/a | 7.51x | sharded RESP stream C16/P1 |
| stream | `XTRIM` | 1 | n/a | 35,398.6 | 4,715.2 | n/a | 7.51x | sharded RESP stream C16/P1 |
| geo | `GEOADD` | 1 | n/a | 62,481.8 | 8,292.8 | n/a | 7.53x | sharded RESP geo C16/P1 |
| geo | `GEODIST` | 1 | n/a | 62,481.2 | 8,292.8 | n/a | 7.53x | sharded RESP geo C16/P1 |
| geo | `GEOHASH` | 1 | n/a | 62,481.0 | 8,291.8 | n/a | 7.54x | sharded RESP geo C16/P1 |
| geo | `GEOPOS` | 1 | n/a | 62,480.4 | 8,291.4 | n/a | 7.54x | sharded RESP geo C16/P1 |
| geo | `GEORADIUS` | 1 | n/a | 62,480.0 | 8,291.4 | n/a | 7.54x | sharded RESP geo C16/P1 |
| geo | `GEORADIUSBYMEMBER` | 1 | n/a | 62,479.8 | 8,291.0 | n/a | 7.54x | sharded RESP geo C16/P1 |
| geo | `GEORADIUSBYMEMBER_RO` | 1 | n/a | 62,479.0 | 8,289.8 | n/a | 7.54x | sharded RESP geo C16/P1 |
| geo | `GEORADIUS_RO` | 1 | n/a | 62,479.0 | 8,290.4 | n/a | 7.54x | sharded RESP geo C16/P1 |
| pubsub | `PSUBSCRIBE` | 1 | n/a | 87,780.0 | 164,844.0 | n/a | 0.53x | sharded RESP pubsub C16/P1 |
| pubsub | `PUBLISH` | 1 | n/a | 87,781.8 | 164,845.8 | n/a | 0.53x | sharded RESP pubsub C16/P1 |
| pubsub | `PUBSUB` | 1 | n/a | 87,781.4 | 164,845.6 | n/a | 0.53x | sharded RESP pubsub C16/P1 |
| pubsub | `PUNSUBSCRIBE` | 1 | n/a | 87,779.0 | 164,844.0 | n/a | 0.53x | sharded RESP pubsub C16/P1 |
| pubsub | `SUBSCRIBE` | 1 | n/a | 87,781.2 | 164,845.0 | n/a | 0.53x | sharded RESP pubsub C16/P1 |
| pubsub | `UNSUBSCRIBE` | 1 | n/a | 87,780.8 | 164,844.4 | n/a | 0.53x | sharded RESP pubsub C16/P1 |
| hyperloglog | `PFADD` | 1 | n/a | 132,404.4 | 16,604.0 | n/a | 7.97x | sharded RESP hll C16/P1 |
| hyperloglog | `PFCOUNT` | 1 | n/a | 132,404.0 | 16,602.6 | n/a | 7.97x | sharded RESP hll C16/P1 |
| hyperloglog | `PFDEBUG` | 1 | n/a | 132,402.8 | 16,601.6 | n/a | 7.98x | sharded RESP hll C16/P1 |
| hyperloglog | `PFMERGE` | 1 | n/a | 132,402.4 | 16,601.4 | n/a | 7.98x | sharded RESP hll C16/P1 |
| hyperloglog | `PFSELFTEST` | 1 | n/a | 544,572.6 | 6.4 | n/a | 85089.47x | sharded RESP hll C16/P1 |
| scripting | `EVAL` | 1 | n/a | 179,055.6 | 22,919.6 | n/a | 7.81x | sharded RESP scripting C16/P1 |
| scripting | `EVALSHA` | 1 | n/a | 179,054.4 | 22,918.4 | n/a | 7.81x | sharded RESP scripting C16/P1 |
| scripting | `SCRIPT` | 1 | n/a | 179,053.2 | 22,917.8 | n/a | 7.81x | sharded RESP scripting C16/P1 |
| transaction | `DISCARD` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| transaction | `EXEC` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| transaction | `MULTI` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| transaction | `UNWATCH` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| transaction | `WATCH` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `ASKING` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `BGREWRITEAOF` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `BGSAVE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `CLUSTER` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `COMMAND` | 8 | 17,647.7 | 14,606.6 | 1,124.6 | 15.69x | 12.99x | opcode C16/P1 |
| server | `CONFIG` | 1 | 2,205.9 | 1,825.7 | 140.4 | 15.71x | 13.00x | opcode C16/P1 |
| server | `DBSIZE` | 1 | 2,206.0 | 1,826.0 | 140.7 | 15.68x | 12.98x | opcode C16/P1 |
| server | `DEBUG` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `FLUSHALL` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `FLUSHDB` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `HOST:` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `INFO` | 1 | 2,206.0 | 1,826.0 | 140.7 | 15.68x | 12.98x | opcode C16/P1 |
| server | `LASTSAVE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `LATENCY` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `LOLWUT` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `MEMORY` | 1 | 2,206.0 | 1,826.0 | 140.7 | 15.68x | 12.98x | opcode C16/P1 |
| server | `MIGRATE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `MODULE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `MONITOR` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `MOVE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `POST` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `PSYNC` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `READONLY` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `READWRITE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `REPLCONF` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `REPLICAOF` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `ROLE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SAVE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SHUTDOWN` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SLAVEOF` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SLOWLOG` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SORT` | 1 | n/a | 8,408.8 | 17,017.8 | n/a | 0.49x | Redis 5 SORT C16/P1 |
| server | `SWAPDB` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SYNC` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `TIME` | 1 | 2,206.0 | 1,826.0 | 140.7 | 15.68x | 12.98x | opcode C16/P1 |
| server | `WAIT` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |

### 16-Shard C16/P16

| Family | Command | Cases | FCNP direct ops/sec | RESP ops/sec | Redis ops/sec | FCNP/Redis | RESP/Redis | Source |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| connection | `AUTH` | 1 | 6,340.0 | 4,028.2 | 180.2 | 35.18x | 22.35x | opcode C16/P16 |
| connection | `CLIENT` | 5 | 31,700.0 | 20,141.0 | 901.0 | 35.18x | 22.35x | opcode C16/P16 |
| connection | `ECHO` | 1 | 6,340.0 | 4,028.2 | 180.2 | 35.18x | 22.35x | opcode C16/P16 |
| connection | `HELLO` | 1 | 6,340.0 | 4,028.2 | 180.2 | 35.18x | 22.35x | opcode C16/P16 |
| connection | `PING` | 1 | 6,340.0 | 4,028.2 | 180.2 | 35.18x | 22.35x | opcode C16/P16 |
| connection | `SELECT` | 1 | 6,340.0 | 4,028.2 | 180.2 | 35.18x | 22.35x | opcode C16/P16 |
| string | `APPEND` | 1 | 6,339.6 | 4,028.0 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `BITCOUNT` | 1 | 6,339.6 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `BITFIELD` | 1 | 6,339.6 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `BITOP` | 1 | 6,339.6 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `BITPOS` | 1 | 6,339.6 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `DECR` | 1 | 6,339.5 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `DECRBY` | 1 | 6,339.5 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `GET` | 3 | 19,017.8 | 12,083.0 | 538.0 | 35.35x | 22.46x | opcode C16/P16 |
| string | `GETBIT` | 1 | 6,339.6 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `GETDEL` | 1 | 6,339.5 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `GETEX` | 1 | 6,339.5 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `GETRANGE` | 2 | 12,678.5 | 8,055.3 | 358.4 | 35.38x | 22.48x | opcode C16/P16 |
| string | `GETSET` | 1 | 6,339.6 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `INCR` | 1 | 6,339.5 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `INCRBY` | 1 | 6,339.5 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `INCRBYFLOAT` | 1 | 6,339.5 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `MGET` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `MSET` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `MSETNX` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `PSETEX` | 1 | 6,339.8 | 4,028.1 | 179.7 | 35.28x | 22.42x | opcode C16/P16 |
| string | `SET` | 6 | 38,037.2 | 24,167.4 | 1,077.2 | 35.31x | 22.44x | opcode C16/P16 |
| string | `SETBIT` | 1 | 6,339.6 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `SETEX` | 1 | 6,339.8 | 4,028.1 | 179.7 | 35.28x | 22.42x | opcode C16/P16 |
| string | `SETNX` | 1 | 6,339.8 | 4,028.1 | 179.7 | 35.28x | 22.42x | opcode C16/P16 |
| string | `SETRANGE` | 1 | 6,339.6 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| string | `STRLEN` | 2 | 12,678.5 | 8,055.3 | 358.5 | 35.37x | 22.47x | opcode C16/P16 |
| string | `SUBSTR` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `COPY` | 1 | 6,339.6 | 4,028.0 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| key | `DEL` | 1 | 6,339.6 | 4,028.0 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| key | `DUMP` | 1 | 6,339.7 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `EXISTS` | 1 | 6,339.7 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `EXPIRE` | 2 | 12,679.5 | 8,056.2 | 359.0 | 35.32x | 22.44x | opcode C16/P16 |
| key | `EXPIREAT` | 1 | 6,339.8 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `EXPIRETIME` | 1 | 6,339.7 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `KEYS` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `OBJECT` | 1 | 6,339.7 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `PERSIST` | 1 | 6,339.8 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `PEXPIRE` | 2 | 12,679.5 | 8,056.2 | 359.0 | 35.32x | 22.44x | opcode C16/P16 |
| key | `PEXPIREAT` | 1 | 6,339.8 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `PEXPIRETIME` | 1 | 6,339.7 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `PTTL` | 1 | 6,339.8 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `RANDOMKEY` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `RENAME` | 2 | 12,679.4 | 8,056.0 | 358.8 | 35.34x | 22.45x | opcode C16/P16 |
| key | `RENAMENX` | 1 | 6,339.6 | 4,028.0 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| key | `RESTORE` | 1 | 6,339.7 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `RESTORE-ASKING` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `SCAN` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| key | `TOUCH` | 1 | 6,339.7 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `TTL` | 1 | 6,339.8 | 4,028.1 | 179.7 | 35.28x | 22.42x | opcode C16/P16 |
| key | `TYPE` | 1 | 6,339.7 | 4,028.1 | 179.5 | 35.32x | 22.44x | opcode C16/P16 |
| key | `UNLINK` | 1 | 6,339.6 | 4,028.0 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HDEL` | 1 | 6,339.3 | 4,027.8 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HEXISTS` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HGET` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HGETALL` | 2 | 12,678.3 | 8,055.3 | 358.4 | 35.37x | 22.48x | opcode C16/P16 |
| hash | `HINCRBY` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HINCRBYFLOAT` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HKEYS` | 2 | 12,678.3 | 8,055.3 | 358.4 | 35.37x | 22.48x | opcode C16/P16 |
| hash | `HLEN` | 2 | 12,678.1 | 8,054.8 | 358.0 | 35.41x | 22.50x | opcode C16/P16 |
| hash | `HMGET` | 2 | 12,678.1 | 8,054.8 | 358.0 | 35.41x | 22.50x | opcode C16/P16 |
| hash | `HMSET` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HRANDFIELD` | 1 | 6,339.3 | 4,027.8 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HSCAN` | 2 | 12,678.2 | 8,055.2 | 358.4 | 35.37x | 22.48x | opcode C16/P16 |
| hash | `HSET` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HSETNX` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HSTRLEN` | 1 | 6,339.4 | 4,027.9 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| hash | `HVALS` | 2 | 12,678.3 | 8,055.3 | 358.4 | 35.37x | 22.48x | opcode C16/P16 |
| list | `BLMOVE` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `BLMPOP` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `BLPOP` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `BRPOP` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `BRPOPLPUSH` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| list | `LINDEX` | 2 | 12,677.8 | 8,054.6 | 357.9 | 35.42x | 22.51x | opcode C16/P16 |
| list | `LINSERT` | 1 | 6,339.2 | 4,027.7 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `LLEN` | 2 | 12,677.8 | 8,054.6 | 357.9 | 35.42x | 22.51x | opcode C16/P16 |
| list | `LMOVE` | 1 | 6,339.2 | 4,027.7 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| list | `LMPOP` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `LPOP` | 1 | 6,339.2 | 4,027.7 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `LPUSH` | 1 | 6,339.3 | 4,027.7 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| list | `LPUSHX` | 1 | 6,339.2 | 4,027.7 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `LRANGE` | 2 | 12,678.0 | 8,054.6 | 358.0 | 35.41x | 22.50x | opcode C16/P16 |
| list | `LREM` | 1 | 6,339.2 | 4,027.7 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `LSET` | 1 | 6,339.2 | 4,027.7 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `LTRIM` | 1 | 6,339.2 | 4,027.7 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `RPOP` | 1 | 6,339.2 | 4,027.7 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| list | `RPOPLPUSH` | 1 | 6,339.3 | 4,027.7 | 179.4 | 35.34x | 22.45x | opcode C16/P16 |
| list | `RPUSH` | 4 | 25,356.9 | 16,110.6 | 717.3 | 35.35x | 22.46x | opcode C16/P16 |
| list | `RPUSHX` | 1 | 6,339.2 | 4,027.7 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SADD` | 2 | 12,678.4 | 8,055.2 | 358.6 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SCARD` | 2 | 12,677.8 | 8,054.5 | 357.9 | 35.42x | 22.50x | opcode C16/P16 |
| set | `SDIFF` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SDIFFSTORE` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SINTER` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SINTERSTORE` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SISMEMBER` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SMEMBERS` | 2 | 12,677.8 | 8,054.5 | 357.9 | 35.42x | 22.50x | opcode C16/P16 |
| set | `SMISMEMBER` | 2 | 12,677.8 | 8,054.5 | 357.9 | 35.42x | 22.50x | opcode C16/P16 |
| set | `SMOVE` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SPOP` | 1 | 6,339.2 | 4,027.6 | 179.2 | 35.38x | 22.48x | opcode C16/P16 |
| set | `SRANDMEMBER` | 2 | 12,677.8 | 8,054.5 | 357.8 | 35.43x | 22.51x | opcode C16/P16 |
| set | `SREM` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SSCAN` | 2 | 12,677.8 | 8,054.5 | 357.8 | 35.43x | 22.51x | opcode C16/P16 |
| set | `SUNION` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| set | `SUNIONSTORE` | 1 | 6,339.2 | 4,027.6 | 179.3 | 35.36x | 22.46x | opcode C16/P16 |
| zset | `BZMPOP` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `BZPOPMAX` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `BZPOPMIN` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZADD` | 10 | 63,390.7 | 40,275.1 | 1,792.0 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZCARD` | 1 | 6,339.2 | 4,027.5 | 179.2 | 35.38x | 22.47x | opcode C16/P16 |
| zset | `ZCOUNT` | 2 | 12,677.7 | 8,054.4 | 357.8 | 35.43x | 22.51x | opcode C16/P16 |
| zset | `ZDIFF` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZDIFFSTORE` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZINCRBY` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZINTER` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZINTERCARD` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZINTERSTORE` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZLEXCOUNT` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZMPOP` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZMSCORE` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZPOPMAX` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZPOPMIN` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZRANDMEMBER` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZRANGE` | 9 | 57,050.5 | 36,246.3 | 1,611.6 | 35.40x | 22.49x | opcode C16/P16 |
| zset | `ZRANGEBYLEX` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZRANGEBYSCORE` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZRANGESTORE` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZRANK` | 2 | 12,677.7 | 8,054.4 | 357.8 | 35.43x | 22.51x | opcode C16/P16 |
| zset | `ZREM` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZREMRANGEBYLEX` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZREMRANGEBYRANK` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZREMRANGEBYSCORE` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZREVRANGE` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZREVRANGEBYLEX` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZREVRANGEBYSCORE` | 1 | 6,339.1 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZREVRANK` | 2 | 12,677.7 | 8,054.4 | 357.8 | 35.43x | 22.51x | opcode C16/P16 |
| zset | `ZSCAN` | 2 | 12,677.6 | 8,054.2 | 357.8 | 35.43x | 22.51x | opcode C16/P16 |
| zset | `ZSCORE` | 3 | 19,016.8 | 12,081.9 | 537.0 | 35.41x | 22.50x | opcode C16/P16 |
| zset | `ZUNION` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| zset | `ZUNIONSTORE` | 1 | 6,339.0 | 4,027.5 | 179.2 | 35.37x | 22.47x | opcode C16/P16 |
| stream | `XACK` | 1 | n/a | 100,693.2 | 18,886.2 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XADD` | 1 | n/a | 100,695.6 | 18,886.6 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XCLAIM` | 1 | n/a | 100,693.4 | 18,886.2 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XDEL` | 1 | n/a | 100,694.6 | 18,886.6 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XGROUP` | 1 | n/a | 100,693.8 | 18,886.4 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XINFO` | 1 | n/a | 100,693.0 | 18,886.2 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XLEN` | 1 | n/a | 100,695.2 | 18,886.6 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XPENDING` | 1 | n/a | 100,693.6 | 18,886.4 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XRANGE` | 1 | n/a | 100,694.8 | 18,886.6 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XREAD` | 1 | n/a | 100,694.2 | 18,886.4 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XREADGROUP` | 1 | n/a | 100,693.8 | 18,886.4 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XREVRANGE` | 1 | n/a | 100,694.6 | 18,886.6 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XSETID` | 1 | n/a | 100,694.4 | 18,886.4 | n/a | 5.33x | sharded RESP stream C16/P16 |
| stream | `XTRIM` | 1 | n/a | 100,694.4 | 18,886.6 | n/a | 5.33x | sharded RESP stream C16/P16 |
| geo | `GEOADD` | 1 | n/a | 156,198.8 | 25,922.8 | n/a | 6.03x | sharded RESP geo C16/P16 |
| geo | `GEODIST` | 1 | n/a | 156,198.8 | 25,922.8 | n/a | 6.03x | sharded RESP geo C16/P16 |
| geo | `GEOHASH` | 1 | n/a | 156,198.2 | 25,922.8 | n/a | 6.03x | sharded RESP geo C16/P16 |
| geo | `GEOPOS` | 1 | n/a | 156,198.0 | 25,922.8 | n/a | 6.03x | sharded RESP geo C16/P16 |
| geo | `GEORADIUS` | 1 | n/a | 156,197.4 | 25,922.8 | n/a | 6.03x | sharded RESP geo C16/P16 |
| geo | `GEORADIUSBYMEMBER` | 1 | n/a | 156,197.2 | 25,922.8 | n/a | 6.03x | sharded RESP geo C16/P16 |
| geo | `GEORADIUSBYMEMBER_RO` | 1 | n/a | 156,196.4 | 25,922.6 | n/a | 6.03x | sharded RESP geo C16/P16 |
| geo | `GEORADIUS_RO` | 1 | n/a | 156,196.8 | 25,922.8 | n/a | 6.03x | sharded RESP geo C16/P16 |
| pubsub | `PSUBSCRIBE` | 1 | n/a | 203,638.4 | 111,300.0 | n/a | 1.83x | sharded RESP pubsub C16/P16 |
| pubsub | `PUBLISH` | 1 | n/a | 203,639.8 | 111,301.0 | n/a | 1.83x | sharded RESP pubsub C16/P16 |
| pubsub | `PUBSUB` | 1 | n/a | 203,639.6 | 111,300.6 | n/a | 1.83x | sharded RESP pubsub C16/P16 |
| pubsub | `PUNSUBSCRIBE` | 1 | n/a | 203,638.2 | 111,299.8 | n/a | 1.83x | sharded RESP pubsub C16/P16 |
| pubsub | `SUBSCRIBE` | 1 | n/a | 203,639.0 | 111,300.4 | n/a | 1.83x | sharded RESP pubsub C16/P16 |
| pubsub | `UNSUBSCRIBE` | 1 | n/a | 203,638.8 | 111,300.2 | n/a | 1.83x | sharded RESP pubsub C16/P16 |
| hyperloglog | `PFADD` | 1 | n/a | 249,234.0 | 35,331.4 | n/a | 7.05x | sharded RESP hll C16/P16 |
| hyperloglog | `PFCOUNT` | 1 | n/a | 249,233.4 | 35,331.4 | n/a | 7.05x | sharded RESP hll C16/P16 |
| hyperloglog | `PFDEBUG` | 1 | n/a | 249,233.2 | 35,331.4 | n/a | 7.05x | sharded RESP hll C16/P16 |
| hyperloglog | `PFMERGE` | 1 | n/a | 249,232.6 | 35,331.4 | n/a | 7.05x | sharded RESP hll C16/P16 |
| hyperloglog | `PFSELFTEST` | 1 | n/a | 542,405.6 | 5.8 | n/a | 93518.21x | sharded RESP hll C16/P16 |
| scripting | `EVAL` | 1 | n/a | 286,300.6 | 42,346.2 | n/a | 6.76x | sharded RESP scripting C16/P16 |
| scripting | `EVALSHA` | 1 | n/a | 286,300.4 | 42,346.2 | n/a | 6.76x | sharded RESP scripting C16/P16 |
| scripting | `SCRIPT` | 1 | n/a | 286,300.0 | 42,346.2 | n/a | 6.76x | sharded RESP scripting C16/P16 |
| transaction | `DISCARD` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| transaction | `EXEC` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| transaction | `MULTI` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| transaction | `UNWATCH` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| transaction | `WATCH` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `ASKING` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `BGREWRITEAOF` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `BGSAVE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `CLUSTER` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `COMMAND` | 8 | 50,718.6 | 32,225.1 | 1,438.6 | 35.26x | 22.40x | opcode C16/P16 |
| server | `CONFIG` | 1 | 6,339.8 | 4,028.1 | 179.7 | 35.28x | 22.42x | opcode C16/P16 |
| server | `DBSIZE` | 1 | 6,340.0 | 4,028.2 | 180.2 | 35.18x | 22.35x | opcode C16/P16 |
| server | `DEBUG` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `FLUSHALL` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `FLUSHDB` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `HOST:` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `INFO` | 1 | 6,339.9 | 4,028.2 | 180.2 | 35.18x | 22.35x | opcode C16/P16 |
| server | `LASTSAVE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `LATENCY` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `LOLWUT` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `MEMORY` | 1 | 6,339.9 | 4,028.2 | 180.2 | 35.18x | 22.35x | opcode C16/P16 |
| server | `MIGRATE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `MODULE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `MONITOR` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `MOVE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `POST` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `PSYNC` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `READONLY` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `READWRITE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `REPLCONF` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `REPLICAOF` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `ROLE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SAVE` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SHUTDOWN` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SLAVEOF` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SLOWLOG` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SORT` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SWAPDB` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `SYNC` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| server | `TIME` | 1 | 6,339.9 | 4,028.2 | 180.2 | 35.18x | 22.35x | opcode C16/P16 |
| server | `WAIT` | n/a | n/a | n/a | n/a | n/a | n/a | n/a |

Commands with no saved Redis head-to-head row in the per-command tables
above: `DISCARD`, `EXEC`, `MULTI`, `UNWATCH`, `WATCH`, `ASKING`, `BGREWRITEAOF`, `BGSAVE`, `CLUSTER`, `DEBUG`, `FLUSHALL`, `FLUSHDB`, `HOST:`, `LASTSAVE`, `LATENCY`, `LOLWUT`, `MIGRATE`, `MODULE`, `MONITOR`, `MOVE`, `POST`, `PSYNC`, `READONLY`, `READWRITE`, `REPLCONF`, `REPLICAOF`, `ROLE`, `SAVE`, `SHUTDOWN`, `SLAVEOF`, `SLOWLOG`, `SWAPDB`, `SYNC`, `WAIT`.


## Artifact Index

| Artifact | What it covers |
| --- | --- |
| [`FAST_CACHE_VS_REDIS_TCP.md`](FAST_CACHE_VS_REDIS_TCP.md) | Published TCP saturation, 1-vCPU and 16-vCPU Redis rows, fixed-load CPU, and Redis/Valkey/Dragonfly large-value matrix |
| [`benchmarks/results/redis-command-opcode-optimized-pass2-depth1-20260524T1555Z/report.md`](results/redis-command-opcode-optimized-pass2-depth1-20260524T1555Z/report.md) | 16-shard fast-cache command matrix, depth 1, optimized opcode pass |
| [`benchmarks/results/redis-command-opcode-optimized-pass2-depth16-20260524T1600Z/report.md`](results/redis-command-opcode-optimized-pass2-depth16-20260524T1600Z/report.md) | 16-shard fast-cache command matrix, ordered depth 16, optimized opcode pass |
| [`benchmarks/results/redis-command-opcode-optimized-direct-depth1-20260524T152034Z/report.md`](results/redis-command-opcode-optimized-direct-depth1-20260524T152034Z/report.md) | 16-shard command matrix, depth 1, saved Redis/Valkey/Dragonfly reference rows |
| [`benchmarks/results/redis-command-opcode-optimized-direct-depth16-ordered-20260524T152654Z/report.md`](results/redis-command-opcode-optimized-direct-depth16-ordered-20260524T152654Z/report.md) | 16-shard command matrix, ordered depth 16, saved Redis/Valkey/Dragonfly reference rows |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p1-stream.csv` | Adam isolated 16-shard RESP stream rows at C16/P1 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p16-stream.csv` | Adam isolated 16-shard RESP stream rows at C16/P16 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p1-geo.csv` | Adam isolated 16-shard RESP geo rows at C16/P1 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p16-geo.csv` | Adam isolated 16-shard RESP geo rows at C16/P16 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p1-pubsub-family.csv` | Adam isolated 16-shard RESP pubsub rows at C16/P1 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p16-pubsub-family.csv` | Adam isolated 16-shard RESP pubsub rows at C16/P16 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p1-hll-main.csv` | Adam isolated 16-shard RESP hyperloglog rows at C16/P1 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p16-hll-main.csv` | Adam isolated 16-shard RESP hyperloglog rows at C16/P16 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p1-pfselftest.csv` | Adam isolated 16-shard RESP PFSELFTEST row at C16/P1 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p16-pfselftest.csv` | Adam isolated 16-shard RESP PFSELFTEST row at C16/P16 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p1-scripting.csv` | Adam isolated 16-shard RESP scripting rows at C16/P1 |
| `benchmarks/results/sharded-gapfill-20260525/resp-c16p16-scripting.csv` | Adam isolated 16-shard RESP scripting rows at C16/P16 |
| [`benchmarks/results/adam-redis-family-1vcpu-c16p16-20260525T042537Z/report.md`](results/adam-redis-family-1vcpu-c16p16-20260525T042537Z/report.md) | 1-vCPU Redis family command matrix, C16/P16 |
| [`benchmarks/results/adam-redis-data-1vcpu-c16p16-20260525T042128Z/report.md`](results/adam-redis-data-1vcpu-c16p16-20260525T042128Z/report.md) | 1-vCPU Redis data-command matrix, C16/P16 |
| [`benchmarks/results/adam-redis-keyspace-1vcpu-c16p16-20260525T042907Z/report.md`](results/adam-redis-keyspace-1vcpu-c16p16-20260525T042907Z/report.md) | 1-vCPU Redis keyspace matrix, C16/P16 |
| [`benchmarks/results/adam-monoio-transport-20260525T220653Z/`](results/adam-monoio-transport-20260525T220653Z/) | Adam Tokio vs monoio RESP transport hot-mix comparison, 1-vCPU and 16-vCPU server views |
| [`benchmarks/results/adam-monoio-profile-20260525T222318Z/`](results/adam-monoio-profile-20260525T222318Z/) | Adam `perf record` comparison for 16-vCPU C16/P16 Tokio vs monoio hot RESP transport |
| [`benchmarks/results/adam-monoio-writev-control-20260525T222741Z/`](results/adam-monoio-writev-control-20260525T222741Z/) | Adam monoio `FAST_CACHE_MONOIO_SAFE_WRITER=writev` control for 16-vCPU C16/P16 hot RESP transport |
| [`benchmarks/results/adam-monoio-legacy-control-20260525T223157Z/`](results/adam-monoio-legacy-control-20260525T223157Z/) | Adam monoio `FAST_CACHE_MONOIO_DRIVER=legacy` control for 16-vCPU C16/P16 hot RESP transport |
| [`benchmarks/results/adam-monoio-legacy-transport-20260525T223244Z/`](results/adam-monoio-legacy-transport-20260525T223244Z/) | Adam monoio legacy driver controls for the remaining hot RESP transport rows |
| [`benchmarks/results/adam-monoio-auto-driver-20260525T223959Z/`](results/adam-monoio-auto-driver-20260525T223959Z/) | Adam monoio `FAST_CACHE_MONOIO_DRIVER=auto` proof rows showing 16-worker legacy and 1-worker io_uring selection |
| [`benchmarks/results/adam-monoio-driver-sweep-20260525T224908Z/`](results/adam-monoio-driver-sweep-20260525T224908Z/) | Adam monoio driver sweep for 2, 4, and 8 workers on the C16/P16 hot RESP mix |
| [`benchmarks/results/adam-monoio-driver-w4-repeat-20260525T225104Z/`](results/adam-monoio-driver-w4-repeat-20260525T225104Z/) | Longer Adam repeat for the 4-worker monoio driver crossover check |
| [`benchmarks/results/adam-single-shard-input-profile-20260525/`](results/adam-single-shard-input-profile-20260525/) | Adam one-worker monoio C16/P16 hot input profile before the direct RESP no-parts fast path |
| [`benchmarks/results/adam-single-shard-input-fastpath-20260525/`](results/adam-single-shard-input-fastpath-20260525/) | Adam one-worker monoio C16/P16 hot input profile after the direct RESP no-parts fast path |
| [`benchmarks/results/redis5-new-commands-local-20260524Tbenchmark/report-head-to-head-common-c1.md`](results/redis5-new-commands-local-20260524Tbenchmark/report-head-to-head-common-c1.md) | Redis 5 common-case source retained for 1-client historical comparison |
| [`benchmarks/results/redis5-new-commands-local-20260524Tbenchmark/report-head-to-head-common-c16-optimized.md`](results/redis5-new-commands-local-20260524Tbenchmark/report-head-to-head-common-c16-optimized.md) | Redis 5 common-case source retained for the SORT C16/P1 backfill |

## Headline TCP Rows

Workload: `64B`, `80/20`, pipeline depth `1`.

| Server CPU view | FCNP direct | RESP | Redis |
| --- | ---: | ---: | ---: |
| 1 vCPU | 106,072 @ 1.000 | 99,438 @ 1.000 | 94,015 @ 0.999 |
| 16 vCPU | 896,322 @ 12.595 | 870,934 @ 12.842 | 90,735 @ 0.998 |

At strict request/response depth, same-core fast-cache RESP is `1.06x` Redis
and FCNP direct is `1.13x` Redis. With 16 server CPUs available, fast-cache RESP
is `9.60x` Redis and FCNP direct is `9.88x` Redis for the same workload.

## Monoio RESP Transport Spot Check

Artifact:
[`benchmarks/results/adam-monoio-transport-20260525T220653Z/`](results/adam-monoio-transport-20260525T220653Z/).

These rows isolate the server socket runtime on Adam using the hot RESP command
mix (`PING`, `ECHO`, `GET`, `SET`, `PUBSUB`). Fast-cache was built once with
`redis-server,monoio`; the Tokio rows use that binary with
`FAST_CACHE_USE_MONOIO` unset, and the monoio rows use the original io_uring
monoio path (`FAST_CACHE_USE_MONOIO=1`, io_uring driver, inline safe writer).
Redis uses `redis:7-alpine`. Each row sums 12 command cases and reports the
mean of per-case `avg us`.

| Server CPU view | Load | Tokio RESP ops/sec | io_uring monoio ops/sec | Redis ops/sec | io_uring/Tokio | io_uring/Redis | Mean avg us (Tokio / io_uring / Redis) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 vCPU | C16/P1 | 83,472.2 | 88,748.4 | 72,846.1 | 1.06x | 1.22x | 191.6 / 180.2 / 219.5 |
| 1 vCPU | C16/P16 | 208,397.9 | 210,113.4 | 173,921.1 | 1.01x | 1.21x | 870.9 / 501.6 / 995.2 |
| 16 vCPU | C16/P1 | 456,135.9 | 411,154.0 | 72,711.3 | 0.90x | 5.65x | 35.0 / 38.8 / 219.9 |
| 16 vCPU | C16/P16 | 1,118,280.7 | 977,626.5 | 176,239.4 | 0.87x | 5.55x | 106.5 / 116.3 / 984.3 |

Takeaway: the original io_uring monoio path helps the single-worker,
socket-bound RESP shape, but does not beat the Tokio transport in the 16-worker
shared-listener shape. After profiling, server monoio uses an adaptive `auto`
driver default: io_uring for one worker, legacy poll for multi-worker socket
runs. Benchmark `FAST_CACHE_MONOIO_SAFE_WRITER=writev` before using monoio for
large-value RESP claims.

Follow-up profile artifact:
[`benchmarks/results/adam-monoio-profile-20260525T222318Z/`](results/adam-monoio-profile-20260525T222318Z/).
This ran the 16-vCPU C16/P16 hot RESP mix under `perf record -e cycles -F 997 -g --call-graph fp`.
Profiler overhead changed the ordering slightly (`monoio` 1,002,484 ops/sec,
Tokio 995,773 ops/sec), so use the stack shape rather than the profiled
throughput as the key result. Monoio spent most sampled cycles below
`io_uring_enter`: `__x64_sys_io_uring_enter` 82.1% children,
`io_submit_sqes` 66.3%, `io_send` 47.7%, and `io_recv` 15.7%. Tokio spent its
transport cycles in the normal task/syscall path: `tokio::runtime::task::raw::poll`
87.7% children, `poll_read_priv` 25.3%, `poll_write_priv` 25.0%, and
`poll_write_vectored_priv` 20.0%. The flat hot symbols were kernel TCP costs in
both cases: `clear_page_rep`, `rep_movs_alternative`, and `nft_do_chain`.

The `FAST_CACHE_MONOIO_SAFE_WRITER=writev` control artifact:
[`benchmarks/results/adam-monoio-writev-control-20260525T222741Z/`](results/adam-monoio-writev-control-20260525T222741Z/).
It produced 959,772 ops/sec for the same 16-vCPU C16/P16 hot RESP mix, below
the io_uring monoio row, so writev is not currently a small-response win.

The `FAST_CACHE_MONOIO_DRIVER=legacy` control artifacts:
[`benchmarks/results/adam-monoio-legacy-control-20260525T223157Z/`](results/adam-monoio-legacy-control-20260525T223157Z/)
and
[`benchmarks/results/adam-monoio-legacy-transport-20260525T223244Z/`](results/adam-monoio-legacy-transport-20260525T223244Z/).
Legacy is worse for the one-worker io_uring-friendly shape, but better once the
server fans out across 16 monoio workers:

| Server CPU view | Load | io_uring monoio ops/sec | legacy monoio ops/sec | legacy/io_uring | Mean avg us legacy |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 vCPU | C16/P1 | 88,748.4 | 80,318.7 | 0.91x | 199.1 |
| 1 vCPU | C16/P16 | 210,113.4 | 192,170.5 | 0.91x | 943.8 |
| 16 vCPU | C16/P1 | 411,154.0 | 435,550.3 | 1.06x | 36.6 |
| 16 vCPU | C16/P16 | 977,626.5 | 1,052,395.4 | 1.08x | 120.6 |

Server monoio now defaults `FAST_CACHE_MONOIO_DRIVER=auto`: io_uring for one
worker and legacy for multi-worker socket runs. Explicit
`FAST_CACHE_MONOIO_DRIVER=legacy|io_uring` still overrides the auto choice.
The auto proof artifact:
[`benchmarks/results/adam-monoio-auto-driver-20260525T223959Z/`](results/adam-monoio-auto-driver-20260525T223959Z/).
It logged `Legacy driver (16 workers)` for the 16-vCPU C16/P16 row and
`IoUring driver (1 workers)` for the 1-vCPU C16/P16 row. The 16-worker auto
row produced 1,119,198 ops/sec, closing the earlier default monoio gap against
the 1,118,281 ops/sec Tokio row for this hot mix.

Additional driver sweep artifacts:
[`benchmarks/results/adam-monoio-driver-sweep-20260525T224908Z/`](results/adam-monoio-driver-sweep-20260525T224908Z/)
and
[`benchmarks/results/adam-monoio-driver-w4-repeat-20260525T225104Z/`](results/adam-monoio-driver-w4-repeat-20260525T225104Z/).
The C16/P16 hot RESP sweep shows io_uring is not an 8-shard win; legacy was
737,181 ops/sec vs io_uring at 678,564 ops/sec. A longer 4-worker repeat also
favored legacy at 477,492 ops/sec vs io_uring at 394,699 ops/sec, so the
current auto rule remains: io_uring for one worker, legacy for multi-worker
server socket runs.

## Single-Worker RESP Input Spot Check

Artifacts:
[`benchmarks/results/adam-single-shard-input-profile-20260525/`](results/adam-single-shard-input-profile-20260525/)
and
[`benchmarks/results/adam-single-shard-input-fastpath-20260525/`](results/adam-single-shard-input-fastpath-20260525/).

These rows isolate the one-worker monoio/io_uring input path under C16/P16 on
the hot RESP case set (`PING`, `GET`, `SET`, `PUBLISH`). The optimized path
keeps the full Redis-compatible dispatch path, but skips temporary
`BorrowedCommandParts` construction for direct RESP fanout commands when no
transaction is active and no direct-shard route check is required.

| Variant | Per-case ops/sec | Mean first-row avg us | Profile shape |
| --- | ---: | ---: | --- |
| Before direct RESP no-parts path | 16,805 | 489.7 | dominated by `io_uring_enter` TCP send/recv |
| After direct RESP no-parts path | 18,182 | 455.5 | still dominated by `io_uring_enter` TCP send/recv |

This is an 8.2% lift in the measured one-worker input profile. The profile
still shows kernel TCP send/recv as the dominant cost, so larger single-worker
wins should focus on socket batching and response write behavior rather than
switching to the legacy local `DirectConnection` path, which does not carry the
current Redis compatibility and RESP2/RESP3 transaction surface.

## 1-vCPU TCP Matrix

Fast-cache is pinned to one CPU and started with `--shard-count 1`. Redis is
the single-threaded baseline. Pipeline depth is `1`.

| Value | Mix | FCNP direct | RESP | Redis |
| ---: | --- | ---: | ---: | ---: |
| 64B | GET | 105,970 @ 1.000 | 98,817 @ 0.999 | 93,631 @ 0.999 |
| 64B | SET | 107,447 @ 0.999 | 97,776 @ 0.999 | 90,881 @ 0.999 |
| 64B | 80/20 | 106,072 @ 1.000 | 99,438 @ 1.000 | 94,015 @ 0.999 |
| 512B | GET | 102,794 @ 0.999 | 95,820 @ 1.000 | 89,154 @ 0.999 |
| 512B | SET | 104,608 @ 0.999 | 95,446 @ 0.999 | 88,918 @ 1.000 |
| 512B | 80/20 | 104,270 @ 1.000 | 96,492 @ 0.999 | 88,737 @ 1.000 |
| 4KiB | GET | 85,572 @ 0.999 | 79,082 @ 0.999 | 75,676 @ 1.000 |
| 4KiB | SET | 93,055 @ 0.999 | 83,551 @ 0.999 | 81,613 @ 0.999 |
| 4KiB | 80/20 | 87,277 @ 1.000 | 81,001 @ 1.000 | 77,005 @ 0.999 |
| 16KiB | GET | 72,560 @ 1.000 | 61,492 @ 1.000 | 58,923 @ 0.999 |
| 16KiB | SET | 81,094 @ 0.999 | 75,104 @ 0.999 | 68,474 @ 1.000 |
| 16KiB | 80/20 | 75,311 @ 0.999 | 64,126 @ 0.999 | 60,532 @ 1.000 |

## 16-vCPU TCP Matrix

Fast-cache is pinned to CPUs `0-15` and started with `--shard-count 16`. Redis
remains the single-threaded baseline. Pipeline depth is `1`.

| Value | Mix | FCNP direct | RESP | Redis |
| ---: | --- | ---: | ---: | ---: |
| 64B | GET | 895,979 @ 12.852 | 883,799 @ 13.015 | 91,193 @ 0.999 |
| 64B | SET | 892,359 @ 12.678 | 868,260 @ 12.980 | 90,049 @ 0.999 |
| 64B | 80/20 | 896,322 @ 12.595 | 870,934 @ 12.842 | 90,735 @ 0.998 |
| 512B | GET | 850,044 @ 12.628 | 813,107 @ 12.005 | 90,104 @ 0.999 |
| 512B | SET | 718,312 @ 10.519 | 701,216 @ 10.833 | 90,126 @ 0.999 |
| 512B | 80/20 | 886,041 @ 12.747 | 856,073 @ 12.941 | 89,039 @ 0.999 |
| 4KiB | GET | 734,864 @ 13.297 | 701,610 @ 13.155 | 75,055 @ 1.000 |
| 4KiB | SET | 749,694 @ 12.605 | 687,039 @ 12.483 | 79,248 @ 0.999 |
| 4KiB | 80/20 | 737,592 @ 12.831 | 709,545 @ 13.371 | 76,970 @ 0.999 |
| 16KiB | GET | 585,573 @ 13.850 | 484,220 @ 14.531 | 60,112 @ 1.000 |
| 16KiB | SET | 524,326 @ 12.491 | 487,239 @ 12.823 | 68,854 @ 1.000 |
| 16KiB | 80/20 | 586,591 @ 13.427 | 496,836 @ 13.815 | 60,774 @ 0.999 |

## Pipeline Sweep

Workload: `64B`, `80/20`, `64` clients, `100k` keys.

### 1 vCPU

| Pipeline depth | FCNP direct | RESP | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 109,927 @ 0.999 | 98,069 @ 0.999 | 89,035 @ 0.999 |
| 4 | 395,374 @ 1.000 | 358,517 @ 0.999 | 301,799 @ 0.999 |
| 16 | 1,277,655 @ 1.000 | 1,063,099 @ 0.999 | 717,818 @ 0.999 |
| 64 | 3,255,162 @ 1.000 | 2,281,784 @ 0.999 | 1,204,399 @ 0.999 |

### 16 vCPU

| Pipeline depth | FCNP direct | RESP | Redis |
| ---: | ---: | ---: | ---: |
| 1 | 892,222 @ 12.544 | 904,005 @ 13.394 | 91,842 @ 0.999 |
| 4 | 3,389,930 @ 12.400 | 3,146,744 @ 12.906 | 303,558 @ 0.999 |
| 16 | 12,039,668 @ 12.790 | 8,930,613 @ 13.126 | 725,161 @ 1.000 |
| 64 | 33,854,047 @ 12.496 | 16,582,102 @ 13.947 | 1,156,757 @ 0.999 |

## Fixed-Load CPU Efficiency

Artifact:
`benchmarks/results/cpu_efficiency_server_20260518_103819/cpu_efficiency_curve.csv`.

Workload: `64B`, `80/20`, `64` clients, pipeline depth `64`, server pinned to
CPU `0`.

| Target | FCNP direct | RESP | Redis |
| ---: | ---: | ---: | ---: |
| 100K ops/s | 99,962 @ 0.052 | 99,978 @ 0.060 | 99,976 @ 0.098 |
| 1M ops/s | 999,731 @ 0.439 | 999,736 @ 0.531 | 999,734 @ 0.909 |
| 2M ops/s | 1,999,581 @ 0.843 | 1,999,570 @ 0.975 | 1,233,072 @ 1.001 |

At the 1M fixed-load point, FCNP direct uses about half the server CPU Redis
uses for the same delivered request rate. At the 2M target, Redis is saturated
at about `1.23M` ops/sec while fast-cache FCNP and RESP still deliver about
`2.00M` ops/sec within one measured server vCPU.

## 16-vCPU Redis-Compatible Large Matrix

Artifact:
`benchmarks/results/network_db_server_20260518_030526/network_db_matrix.csv`.

This sweep compares fast-cache, Redis, Valkey, and Dragonfly over TCP. Server
processes were pinned to CPUs `0-15`; benchmark clients were pinned to CPUs
`16-31`. Cells are `ops/sec / logical GB/s @ measured server vCPU (clients,
pipeline)`.

### Best 80/20 Throughput By Value Size

| Value | FCNP direct | RESP | Redis | Valkey | Dragonfly |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64B | 31.18M / 2.00 @ 14.6 (64c, p64) | 19.76M / 1.26 @ 16.0 (64c, p64) | 1.19M / 0.08 @ 1.0 (16c, p64) | 1.21M / 0.08 @ 1.0 (16c, p64) | 4.10M / 0.26 @ 15.9 (64c, p64) |
| 512B | 12.20M / 6.25 @ 9.1 (16c, p64) | 10.52M / 5.39 @ 16.0 (64c, p64) | 858k / 0.44 @ 1.0 (16c, p64) | 855k / 0.44 @ 1.0 (16c, p64) | 1.08M / 0.55 @ 14.1 (64c, p64) |
| 4KiB | 1.95M / 8.00 @ 9.8 (16c, p16) | 1.88M / 7.70 @ 10.4 (16c, p16) | 368k / 1.51 @ 1.0 (16c, p16) | 335k / 1.37 @ 1.0 (16c, p16) | 833k / 3.41 @ 14.5 (64c, p64) |
| 64KiB | 141k / 9.26 @ 10.3 (16c, p1) | 141k / 9.24 @ 11.4 (16c, p1) | 37k / 2.41 @ 1.0 (16c, p16) | 36k / 2.36 @ 1.0 (16c, p16) | 98k / 6.41 @ 10.7 (16c, p1) |
| 1MiB | 5k / 5.59 @ 13.4 (16c, p1) | 5k / 4.79 @ 13.4 (16c, p1) | 3k / 2.89 @ 1.0 (16c, p1) | 3k / 2.81 @ 1.0 (16c, p16) | n/a |

### 64B Best Throughput By Mix

| Mix | FCNP direct | RESP | Redis | Valkey | Dragonfly |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 32.11M / 2.06 @ 14.6 (64c, p64) | 23.28M / 1.49 @ 16.0 (64c, p64) | 1.22M / 0.08 @ 1.0 (16c, p64) | 1.23M / 0.08 @ 1.0 (16c, p64) | 3.93M / 0.25 @ 15.9 (64c, p64) |
| SET | 32.07M / 2.05 @ 15.3 (64c, p64) | 12.56M / 0.80 @ 15.7 (64c, p64) | 1.03M / 0.07 @ 1.0 (16c, p64) | 997k / 0.06 @ 1.0 (16c, p64) | 4.31M / 0.28 @ 16.0 (256c, p64) |
| 80/20 | 31.18M / 2.00 @ 14.6 (64c, p64) | 19.76M / 1.26 @ 16.0 (64c, p64) | 1.19M / 0.08 @ 1.0 (16c, p64) | 1.21M / 0.08 @ 1.0 (16c, p64) | 4.10M / 0.26 @ 15.9 (64c, p64) |

## 16-Shard Command Matrix

These rows were run on Adam with 16 clients, 16 key shards, `SHARD_COUNT=16`,
shared keyspace fixtures, and direct shard ports enabled at
`127.0.0.1:6384+16`. Transaction cases are skipped on direct shard ports
because transactions are connection-scoped.

Depth 1, strict request/response:

| Target | Cases | Sum ops/sec | Mean avg us | Errors |
| --- | ---: | ---: | ---: | ---: |
| fast-cache FCNP direct | 209 | 460,886 | 34.6 | 0 |
| fast-cache FCNP shared | 209 | 460,459 | 34.6 | 0 |
| fast-cache RESP | 209 | 381,477 | 41.8 | 0 |
| Redis | 209 | 29,251 | 546.5 | 2,818 |
| Valkey | 209 | 31,198 | 512.3 | 3,004 |
| Dragonfly | 209 | 6,506 | 2,459.9 | 3,133 |

Depth 16, ordered pipelining:

| Target | Cases | Sum ops/sec | Mean avg us | Errors |
| --- | ---: | ---: | ---: | ---: |
| fast-cache FCNP direct | 209 | 1,324,917 | 88.7 | 0 |
| fast-cache FCNP shared | 209 | 1,328,698 | 88.4 | 0 |
| fast-cache RESP | 209 | 841,790 | 116.9 | 0 |
| Redis | 209 | 37,483 | 5,216.9 | 3,604 |
| Valkey | 209 | 46,885 | 4,184.5 | 4,492 |
| Dragonfly | 209 | 17,500 | 12,175.6 | 8,396 |

Zero-error common subsets are the cleaner implementation comparison for these
runs:

| Shape | Common cases | FCNP direct | FCNP shared | RESP | Redis | Valkey |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Depth 1 | 207 | 456,474 @ 34.6 us | 456,051 @ 34.6 us | 377,825 @ 41.9 us | 28,969 @ 541.9 us | 30,898 @ 507.7 us |
| Depth 16 | 207 | 1,312,237 @ 88.5 us | 1,315,982 @ 88.2 us | 833,734 @ 117.1 us | 37,123 @ 5,150.4 us | 46,436 @ 4,125.1 us |

## 1-vCPU Command Matrices

These Redis-only command proof runs use Adam with a 1-vCPU fast-cache server
shape and C16/P16 load. They are useful for compatibility and command-family
regression tracking.

| Matrix | Cases | Clients/Pipeline | Duration | fast-cache sum ops/sec | Redis sum ops/sec | Ratio vs Redis | fast-cache mean avg us | Redis mean avg us | Errors |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Family matrix | 217 | C16/P16 | 5s | 2,514,174.8 | 1,590,932.8 | 1.58x | 851.6 | 8,739.9 | 0 / 0 |
| Data-command matrix | 224 | C16/P16 | 10s | 189,399.3 | 1,009.6 | 187.60x | 1,191.5 | 246,239.0 | 0 / 0 |
| Keyspace matrix | 6 | C16/P16 | 5s | 13,273.2 | 4,469.0 | 2.97x | 6,435.6 | 16,814.6 | 0 / 0 |

The data-command matrix is intentionally broad and includes slow stateful
families such as streams, geo, and hyperloglog. Treat it as a proof artifact,
not as the network saturation headline.

## Scripting Spot Check

Scripting support was spot-checked after adding `EVAL`, `EVALSHA`, and `SCRIPT`
support.

| Mode | Target | Cases | Ops/sec | Mean avg us | Errors |
| --- | --- | ---: | ---: | ---: | ---: |
| C1/P1 | fast-cache RESP | 3 | 14,727 | 22.5 | 0 |
| C1/P1 | Redis | 3 | 6,312 | 52.7 | 0 |
| C1/P1 | Valkey | 3 | 6,058 | 54.9 | 0 |
| C1/P1 | Dragonfly | 3 | 5,725 | 58.1 | 0 |
| C16/P16 | fast-cache RESP | 3 | 184,328 | 76.7 | 0 |
| C16/P16 | Redis | 3 | 42,200 | 363.1 | 0 |
| C16/P16 | Valkey | 3 | 41,645 | 368.0 | 0 |
| C16/P16 | Dragonfly | 3 | 85,832 | 169.0 | 0 |

## Reading Notes

- Use the TCP saturation rows for public performance claims about 1-vCPU and
  16-vCPU Redis-compatible deployment shapes.
- Use the command matrices for proofing command behavior and finding
  command-family hotspots.
- Use isolated family artifacts for stream, geo, pubsub, hyperloglog, and
  scripting. Mixed-family command matrices are proof runs and can understate
  fast commands when they share a timed loop with slow stateful or diagnostic
  cases.
- Use zero-error common subsets when comparing implementation speed across
  fast-cache, Redis, Valkey, and Dragonfly.
- External reference CSVs should be saved and reused for optimization loops so
  new fast-cache runs can be compared against stable Redis, Valkey, and
  Dragonfly baselines.
