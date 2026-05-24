# Redis Compatibility Manifest

Generated from `benchmarks/src/redis_command_cases.rs`. Keep this file fresh with:

```bash
cargo run -p fast-cache-benchmarks --bin redis_command_manifest -- --output docs/REDIS_COMPATIBILITY.md
```

## Summary

| Metric | Count |
| --- | ---: |
| Supported commands | 155 |
| Missing commands | 0 |
| Live benchmark cases | 222 |
| Large-profile cases | 29 |
| Destructive-profile cases | 2 |
| Keyspace-wide benchmark cases | 7 |

`supported` means there is a Redis/Valkey-compatible implementation and at least one live RESP benchmark case. Destructive keyspace-wide cases live in the explicit `profile:destructive` matrix so they do not poison ordinary mixed runs. `missing` means it is outside the 0.2.0 compatibility surface.

## Commands

| Family | Command | Status | Cases | Profiles | Keyspace Wide | Notes |
| --- | --- | --- | ---: | --- | --- | --- |
| string | `APPEND` | supported | 1 | small | no | Benchmark cases: APPEND |
| connection | `AUTH` | supported | 1 | small | no | Benchmark cases: AUTH |
| string | `BITCOUNT` | supported | 1 | small | no | Benchmark cases: BITCOUNT |
| string | `BITFIELD` | supported | 1 | small | no | Benchmark cases: BITFIELD GET SET |
| string | `BITOP` | supported | 1 | small | no | Benchmark cases: BITOP OR |
| string | `BITPOS` | supported | 1 | small | no | Benchmark cases: BITPOS |
| list | `BLMOVE` | supported | 1 | small | no | Benchmark cases: BLMOVE ready |
| list | `BLMPOP` | supported | 1 | small | no | Benchmark cases: BLMPOP right count |
| list | `BLPOP` | supported | 1 | small | no | Benchmark cases: BLPOP ready |
| list | `BRPOP` | supported | 1 | small | no | Benchmark cases: BRPOP ready |
| zset | `BZMPOP` | supported | 1 | small | no | Benchmark cases: BZMPOP max count |
| zset | `BZPOPMAX` | supported | 1 | small | no | Benchmark cases: BZPOPMAX ready |
| zset | `BZPOPMIN` | supported | 1 | small | no | Benchmark cases: BZPOPMIN ready |
| connection | `CLIENT` | supported | 5 | small | no | Benchmark cases: CLIENT GETNAME, CLIENT ID, CLIENT KILL ID 0, CLIENT LIST, CLIENT SETNAME |
| server | `COMMAND` | supported | 8 | small | no | Benchmark cases: COMMAND, COMMAND COUNT, COMMAND DOCS, COMMAND GETKEYS MGET, COMMAND GETKEYSANDFLAGS MGET, COMMAND HELP, COMMAND INFO GET, COMMAND LIST |
| server | `CONFIG` | supported | 1 | small | no | Benchmark cases: CONFIG GET all |
| key | `COPY` | supported | 1 | small | no | Benchmark cases: COPY existing dest |
| server | `DBSIZE` | supported | 1 | small | no | Benchmark cases: DBSIZE empty |
| string | `DECR` | supported | 1 | small | no | Benchmark cases: DECR |
| string | `DECRBY` | supported | 1 | small | no | Benchmark cases: DECRBY |
| key | `DEL` | supported | 1 | small | no | Benchmark cases: DEL missing |
| transaction | `DISCARD` | supported | 1 | small | no | Benchmark cases: DISCARD queued SET |
| key | `DUMP` | supported | 1 | small | no | Benchmark cases: DUMP string |
| connection | `ECHO` | supported | 1 | small | no | Benchmark cases: ECHO |
| transaction | `EXEC` | supported | 1 | small | no | Benchmark cases: MULTI EXEC SET GET |
| key | `EXISTS` | supported | 1 | small | no | Benchmark cases: EXISTS mixed |
| key | `EXPIRE` | supported | 2 | small | no | Benchmark cases: EXPIRE NX, EXPIRE future |
| key | `EXPIREAT` | supported | 1 | small | no | Benchmark cases: EXPIREAT future |
| key | `EXPIRETIME` | supported | 1 | small | no | Benchmark cases: EXPIRETIME future |
| server | `FLUSHALL` | supported | 1 | destructive | yes | Destructive perf matrix case; run separately with `CASES=profile:destructive`. Benchmark cases: FLUSHALL one key |
| server | `FLUSHDB` | supported | 1 | destructive | yes | Destructive perf matrix case; run separately with `CASES=profile:destructive`. Benchmark cases: FLUSHDB one key |
| string | `GET` | supported | 3 | large, small | no | Benchmark cases: GET large 4KiB value, GET large 64KiB value, GET string |
| string | `GETBIT` | supported | 1 | small | no | Benchmark cases: GETBIT |
| string | `GETDEL` | supported | 1 | small | no | Benchmark cases: GETDEL |
| string | `GETEX` | supported | 1 | small | no | Benchmark cases: GETEX PX |
| string | `GETRANGE` | supported | 2 | large, small | no | Benchmark cases: GETRANGE, GETRANGE large 64KiB full |
| string | `GETSET` | supported | 1 | small | no | Benchmark cases: GETSET |
| hash | `HDEL` | supported | 1 | small | no | Benchmark cases: HDEL |
| connection | `HELLO` | supported | 1 | small | no | Benchmark cases: HELLO 2 |
| hash | `HEXISTS` | supported | 1 | small | no | Benchmark cases: HEXISTS |
| hash | `HGET` | supported | 1 | small | no | Benchmark cases: HGET |
| hash | `HGETALL` | supported | 2 | large, small | no | Benchmark cases: HGETALL, HGETALL large 1K fields |
| hash | `HINCRBY` | supported | 1 | small | no | Benchmark cases: HINCRBY |
| hash | `HINCRBYFLOAT` | supported | 1 | small | no | Benchmark cases: HINCRBYFLOAT |
| hash | `HKEYS` | supported | 2 | large, small | no | Benchmark cases: HKEYS, HKEYS large 1K fields |
| hash | `HLEN` | supported | 2 | large, small | no | Benchmark cases: HLEN, HLEN large 1K fields |
| hash | `HMGET` | supported | 2 | large, small | no | Benchmark cases: HMGET, HMGET large selected fields |
| hash | `HMSET` | supported | 1 | small | no | Benchmark cases: HMSET |
| hash | `HRANDFIELD` | supported | 1 | small | no | Benchmark cases: HRANDFIELD WITHVALUES |
| hash | `HSCAN` | supported | 2 | large, small | no | Benchmark cases: HSCAN, HSCAN large 1K fields |
| hash | `HSET` | supported | 1 | small | no | Benchmark cases: HSET |
| hash | `HSETNX` | supported | 1 | small | no | Benchmark cases: HSETNX |
| hash | `HSTRLEN` | supported | 1 | small | no | Benchmark cases: HSTRLEN |
| hash | `HVALS` | supported | 2 | large, small | no | Benchmark cases: HVALS, HVALS large 1K fields |
| string | `INCR` | supported | 1 | small | no | Benchmark cases: INCR |
| string | `INCRBY` | supported | 1 | small | no | Benchmark cases: INCRBY |
| string | `INCRBYFLOAT` | supported | 1 | small | no | Benchmark cases: INCRBYFLOAT |
| server | `INFO` | supported | 1 | small | no | Benchmark cases: INFO |
| key | `KEYS` | supported | 2 | large, small | yes | Benchmark cases: KEYS all, KEYS large keyspace |
| list | `LINDEX` | supported | 2 | large, small | no | Benchmark cases: LINDEX, LINDEX large middle |
| list | `LINSERT` | supported | 1 | small | no | Benchmark cases: LINSERT |
| list | `LLEN` | supported | 2 | large, small | no | Benchmark cases: LLEN, LLEN large 1K list |
| list | `LMOVE` | supported | 1 | small | no | Benchmark cases: LMOVE self |
| list | `LMPOP` | supported | 1 | small | no | Benchmark cases: LMPOP left count |
| list | `LPOP` | supported | 1 | small | no | Benchmark cases: LPOP |
| list | `LPUSH` | supported | 1 | small | no | Benchmark cases: LPUSH |
| list | `LPUSHX` | supported | 1 | small | no | Benchmark cases: LPUSHX missing |
| list | `LRANGE` | supported | 2 | large, small | no | Benchmark cases: LRANGE, LRANGE large 1K full |
| list | `LREM` | supported | 1 | small | no | Benchmark cases: LREM |
| list | `LSET` | supported | 1 | small | no | Benchmark cases: LSET |
| list | `LTRIM` | supported | 1 | small | no | Benchmark cases: LTRIM |
| server | `MEMORY` | supported | 1 | small | no | Benchmark cases: MEMORY USAGE string |
| string | `MGET` | supported | 1 | small | no | Benchmark cases: MGET order |
| string | `MSET` | supported | 1 | small | no | Benchmark cases: MSET |
| string | `MSETNX` | supported | 1 | small | no | Benchmark cases: MSETNX existing |
| transaction | `MULTI` | supported | 1 | small | no | Benchmark cases: MULTI DISCARD |
| key | `OBJECT` | supported | 1 | small | no | Benchmark cases: OBJECT ENCODING string |
| key | `PERSIST` | supported | 1 | small | no | Benchmark cases: PERSIST |
| key | `PEXPIRE` | supported | 2 | small | no | Benchmark cases: PEXPIRE XX, PEXPIRE future |
| key | `PEXPIREAT` | supported | 1 | small | no | Benchmark cases: PEXPIREAT future |
| key | `PEXPIRETIME` | supported | 1 | small | no | Benchmark cases: PEXPIRETIME future |
| connection | `PING` | supported | 1 | small | no | Benchmark cases: PING |
| string | `PSETEX` | supported | 1 | small | no | Benchmark cases: PSETEX |
| key | `PTTL` | supported | 1 | small | no | Benchmark cases: PTTL positive |
| key | `RANDOMKEY` | supported | 1 | small | no | Benchmark cases: RANDOMKEY nonempty |
| key | `RENAME` | supported | 2 | small | no | Benchmark cases: RENAME a to b, RENAME b to a |
| key | `RENAMENX` | supported | 1 | small | no | Benchmark cases: RENAMENX existing dest |
| key | `RESTORE` | supported | 1 | small | no | Benchmark cases: RESTORE string replace |
| list | `RPOP` | supported | 1 | small | no | Benchmark cases: RPOP |
| list | `RPOPLPUSH` | supported | 1 | small | no | Benchmark cases: RPOPLPUSH self |
| list | `RPUSH` | supported | 4 | small | no | Benchmark cases: RPUSH, RPUSH bl seed, RPUSH bm seed, RPUSH br seed |
| list | `RPUSHX` | supported | 1 | small | no | Benchmark cases: RPUSHX missing |
| set | `SADD` | supported | 2 | small | no | Benchmark cases: SADD set-a, SADD set-b |
| key | `SCAN` | supported | 3 | large, small | yes | Benchmark cases: SCAN all, SCAN large keyspace, SCAN type string |
| set | `SCARD` | supported | 2 | large, small | no | Benchmark cases: SCARD, SCARD large 1K set |
| set | `SDIFF` | supported | 1 | small | no | Benchmark cases: SDIFF |
| set | `SDIFFSTORE` | supported | 1 | small | no | Benchmark cases: SDIFFSTORE |
| connection | `SELECT` | supported | 1 | small | no | Benchmark cases: SELECT |
| string | `SET` | supported | 6 | large, small | no | Benchmark cases: SET EX, SET NX miss, SET XX hit, SET large 4KiB value, SET large 64KiB value, SET string |
| string | `SETBIT` | supported | 1 | small | no | Benchmark cases: SETBIT |
| string | `SETEX` | supported | 1 | small | no | Benchmark cases: SETEX |
| string | `SETNX` | supported | 1 | small | no | Benchmark cases: SETNX existing |
| string | `SETRANGE` | supported | 1 | small | no | Benchmark cases: SETRANGE |
| set | `SINTER` | supported | 1 | small | no | Benchmark cases: SINTER |
| set | `SINTERSTORE` | supported | 1 | small | no | Benchmark cases: SINTERSTORE |
| set | `SISMEMBER` | supported | 1 | small | no | Benchmark cases: SISMEMBER |
| set | `SMEMBERS` | supported | 2 | large, small | no | Benchmark cases: SMEMBERS, SMEMBERS large 1K set |
| set | `SMISMEMBER` | supported | 2 | large, small | no | Benchmark cases: SMISMEMBER, SMISMEMBER large selected |
| set | `SMOVE` | supported | 1 | small | no | Benchmark cases: SMOVE |
| set | `SPOP` | supported | 1 | small | no | Benchmark cases: SPOP |
| set | `SRANDMEMBER` | supported | 2 | large, small | no | Benchmark cases: SRANDMEMBER, SRANDMEMBER large 32 |
| set | `SREM` | supported | 1 | small | no | Benchmark cases: SREM |
| set | `SSCAN` | supported | 2 | large, small | no | Benchmark cases: SSCAN, SSCAN large 1K set |
| string | `STRLEN` | supported | 2 | large, small | no | Benchmark cases: STRLEN, STRLEN large 64KiB value |
| set | `SUNION` | supported | 1 | small | no | Benchmark cases: SUNION |
| set | `SUNIONSTORE` | supported | 1 | small | no | Benchmark cases: SUNIONSTORE |
| server | `TIME` | supported | 1 | small | no | Benchmark cases: TIME |
| key | `TOUCH` | supported | 1 | small | no | Benchmark cases: TOUCH mixed |
| key | `TTL` | supported | 1 | small | no | Benchmark cases: TTL positive |
| key | `TYPE` | supported | 1 | small | no | Benchmark cases: TYPE string |
| key | `UNLINK` | supported | 1 | small | no | Benchmark cases: UNLINK missing |
| transaction | `UNWATCH` | supported | 1 | small | no | Benchmark cases: UNWATCH simple |
| transaction | `WATCH` | supported | 1 | small | no | Benchmark cases: WATCH simple |
| zset | `ZADD` | supported | 10 | small | no | Benchmark cases: ZADD, ZADD CH, ZADD NX, ZADD XX GT, ZADD bzmax, ZADD bzmin, ZADD z2 seed, ZADD zlex seed, ZADD zu1, ZADD zu2 |
| zset | `ZCARD` | supported | 1 | small | no | Benchmark cases: ZCARD |
| zset | `ZCOUNT` | supported | 2 | large, small | no | Benchmark cases: ZCOUNT, ZCOUNT large 1K all |
| zset | `ZDIFF` | supported | 1 | small | no | Benchmark cases: ZDIFF |
| zset | `ZDIFFSTORE` | supported | 1 | small | no | Benchmark cases: ZDIFFSTORE |
| zset | `ZINCRBY` | supported | 1 | small | no | Benchmark cases: ZINCRBY |
| zset | `ZINTER` | supported | 1 | small | no | Benchmark cases: ZINTER WITHSCORES |
| zset | `ZINTERCARD` | supported | 1 | small | no | Benchmark cases: ZINTERCARD |
| zset | `ZINTERSTORE` | supported | 1 | small | no | Benchmark cases: ZINTERSTORE weighted |
| zset | `ZLEXCOUNT` | supported | 1 | small | no | Benchmark cases: ZLEXCOUNT |
| zset | `ZMPOP` | supported | 1 | small | no | Benchmark cases: ZMPOP min count |
| zset | `ZMSCORE` | supported | 1 | small | no | Benchmark cases: ZMSCORE z2 |
| zset | `ZPOPMAX` | supported | 1 | small | no | Benchmark cases: ZPOPMAX |
| zset | `ZPOPMIN` | supported | 1 | small | no | Benchmark cases: ZPOPMIN |
| zset | `ZRANDMEMBER` | supported | 1 | small | no | Benchmark cases: ZRANDMEMBER WITHSCORES |
| zset | `ZRANGE` | supported | 9 | large, small | no | Benchmark cases: ZRANGE, ZRANGE BYSCORE REV LIMIT, ZRANGE WITHSCORES, ZRANGE large 100 WITHSCORES, ZRANGE large 1K full, ZRANGE zd, ZRANGE zi, ZRANGE zout, ZRANGE zstored |
| zset | `ZRANGEBYLEX` | supported | 1 | small | no | Benchmark cases: ZRANGEBYLEX |
| zset | `ZRANGEBYSCORE` | supported | 1 | small | no | Benchmark cases: ZRANGEBYSCORE exclusive |
| zset | `ZRANGESTORE` | supported | 1 | small | no | Benchmark cases: ZRANGESTORE |
| zset | `ZRANK` | supported | 2 | large, small | no | Benchmark cases: ZRANK, ZRANK large middle |
| zset | `ZREM` | supported | 1 | small | no | Benchmark cases: ZREM missing |
| zset | `ZREMRANGEBYLEX` | supported | 1 | small | no | Benchmark cases: ZREMRANGEBYLEX no-op |
| zset | `ZREMRANGEBYRANK` | supported | 1 | small | no | Benchmark cases: ZREMRANGEBYRANK no-op |
| zset | `ZREMRANGEBYSCORE` | supported | 1 | small | no | Benchmark cases: ZREMRANGEBYSCORE no-op |
| zset | `ZREVRANGE` | supported | 1 | small | no | Benchmark cases: ZREVRANGE |
| zset | `ZREVRANGEBYLEX` | supported | 1 | small | no | Benchmark cases: ZREVRANGEBYLEX |
| zset | `ZREVRANGEBYSCORE` | supported | 1 | small | no | Benchmark cases: ZREVRANGEBYSCORE limit |
| zset | `ZREVRANK` | supported | 2 | large, small | no | Benchmark cases: ZREVRANK, ZREVRANK large middle |
| zset | `ZSCAN` | supported | 2 | large, small | no | Benchmark cases: ZSCAN, ZSCAN large 1K zset |
| zset | `ZSCORE` | supported | 3 | large, small | no | Benchmark cases: ZSCORE, ZSCORE large middle, ZSCORE z2 |
| zset | `ZUNION` | supported | 1 | small | no | Benchmark cases: ZUNION WITHSCORES |
| zset | `ZUNIONSTORE` | supported | 1 | small | no | Benchmark cases: ZUNIONSTORE weighted |
