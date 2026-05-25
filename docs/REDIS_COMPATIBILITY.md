# Redis Compatibility Manifest

Generated from `benchmarks/src/redis_command_cases.rs`. Keep this file fresh with:

```bash
cargo run -p fast-cache-benchmarks --bin redis_command_manifest -- --output docs/REDIS_COMPATIBILITY.md
```

## Summary

| Metric | Count |
| --- | ---: |
| Supported commands | 222 |
| Missing commands | 0 |
| Live benchmark cases | 289 |
| Expected-error benchmark cases | 9 |
| Large-profile cases | 29 |
| Destructive-profile cases | 2 |
| Keyspace-wide benchmark cases | 8 |

`supported` means there is a Redis/Valkey-compatible implementation and at least one live RESP benchmark case. Expected-error cases are commands whose Redis-compatible behavior in fast-cache's standalone mode is an error reply, such as disabled cluster, replication, monitor, shutdown, or security-warning commands. Destructive keyspace-wide cases live in the explicit `profile:destructive` matrix so they do not poison ordinary mixed runs. `missing` means it is outside the 0.2.0 compatibility surface.

## Redis 5.0.14 Baseline

Official baseline: Redis 5.0.14 `redisCommandTable` from <https://github.com/redis/redis/blob/5.0.14/src/server.c>.

| Metric | Count |
| --- | ---: |
| Redis 5.0.14 command table entries | 200 |
| Redis 5.0.14 commands supported and live-benchmarked | 200 |
| Redis 5.0.14 commands explicitly excluded from 0.2.0 | 0 |
| Redis 5.0.14 commands missing | 0 |
| Supported extensions beyond Redis 5.0.14 | 22 |

No Redis 5.0.14 commands are excluded from the compatibility target. Redis 5.0.14 commands that are not supported yet are tracked as missing compatibility work.

Missing Redis 5.0.14 commands: none.

Supported extensions beyond Redis 5.0.14: `BLMOVE`, `BLMPOP`, `BZMPOP`, `COPY`, `EXPIRETIME`, `GETDEL`, `GETEX`, `HELLO`, `HRANDFIELD`, `LMOVE`, `LMPOP`, `PEXPIRETIME`, `SMISMEMBER`, `ZDIFF`, `ZDIFFSTORE`, `ZINTER`, `ZINTERCARD`, `ZMPOP`, `ZMSCORE`, `ZRANDMEMBER`, `ZRANGESTORE`, `ZUNION`.

Explicit Redis 5.0.14 exclusions: none.

## Semantic Compatibility Notes

- The manifest tracks live RESP command acceptance and benchmark coverage, not a promise that every edge case, exact error string, or background subsystem is byte-for-byte identical to Redis.
- Expected-error commands are part of the compatibility surface in standalone mode. They intentionally return Redis-style errors for disabled cluster, replication, monitor, shutdown, module loading, migration, cross-DB movement, and security-warning paths.
- Pub/Sub coverage currently validates publish-without-subscribers, subscription acknowledgements, unsubscribe acknowledgements, and empty introspection. Persistent subscriber fanout is not part of the 0.2.0 compatibility semantics.
- Stream coverage includes basic append, length, range, reverse range, delete, trim, set-id, read, and minimal group/readgroup paths. Pending-entry-list, claim, ack, and detailed group/consumer introspection behavior is intentionally lightweight.
- Scripting uses a constrained evaluator for return values, KEYS/ARGV, tonumber, and redis.call/pcall over supported commands. It is not a general Lua VM.
- HyperLogLog commands return compatible cardinalities for the covered operations, but fast-cache stores exact sets in its own representation rather than Redis' binary HLL encoding.
- Blocking list and sorted-set commands are live-tested on ready or short-timeout paths. Long-lived blocking wakeups across clients need separate proofing before being described as full Redis parity.
- FCNP one-byte opcodes cover the hot command set. Commands outside that compact opcode table use the RESP/FCNP command-name fallback path so the server can still route and execute them.

## Commands

| Family | Command | Status | Cases | Profiles | Keyspace Wide | Expected Error | Notes |
| --- | --- | --- | ---: | --- | --- | --- | --- |
| string | `APPEND` | supported | 1 | small | no | no | Benchmark cases: APPEND |
| server | `ASKING` | supported | 1 | small | no | no | Benchmark cases: ASKING |
| connection | `AUTH` | supported | 1 | small | no | no | Benchmark cases: AUTH |
| server | `BGREWRITEAOF` | supported | 1 | small | no | no | Benchmark cases: BGREWRITEAOF |
| server | `BGSAVE` | supported | 1 | small | no | no | Benchmark cases: BGSAVE |
| string | `BITCOUNT` | supported | 1 | small | no | no | Benchmark cases: BITCOUNT |
| string | `BITFIELD` | supported | 1 | small | no | no | Benchmark cases: BITFIELD GET SET |
| string | `BITOP` | supported | 1 | small | no | no | Benchmark cases: BITOP OR |
| string | `BITPOS` | supported | 1 | small | no | no | Benchmark cases: BITPOS |
| list | `BLMOVE` | supported | 1 | small | no | no | Benchmark cases: BLMOVE ready |
| list | `BLMPOP` | supported | 1 | small | no | no | Benchmark cases: BLMPOP right count |
| list | `BLPOP` | supported | 1 | small | no | no | Benchmark cases: BLPOP ready |
| list | `BRPOP` | supported | 1 | small | no | no | Benchmark cases: BRPOP ready |
| list | `BRPOPLPUSH` | supported | 1 | small | no | no | Benchmark cases: BRPOPLPUSH ready |
| zset | `BZMPOP` | supported | 1 | small | no | no | Benchmark cases: BZMPOP max count |
| zset | `BZPOPMAX` | supported | 1 | small | no | no | Benchmark cases: BZPOPMAX ready |
| zset | `BZPOPMIN` | supported | 1 | small | no | no | Benchmark cases: BZPOPMIN ready |
| connection | `CLIENT` | supported | 5 | small | no | no | Benchmark cases: CLIENT GETNAME, CLIENT ID, CLIENT KILL ID 0, CLIENT LIST, CLIENT SETNAME |
| server | `CLUSTER` | supported | 1 | small | no | yes | Expected RESP error reply in standalone compatibility mode. Benchmark cases: CLUSTER INFO |
| server | `COMMAND` | supported | 8 | small | no | no | Benchmark cases: COMMAND, COMMAND COUNT, COMMAND DOCS, COMMAND GETKEYS MGET, COMMAND GETKEYSANDFLAGS MGET, COMMAND HELP, COMMAND INFO GET, COMMAND LIST |
| server | `CONFIG` | supported | 1 | small | no | no | Benchmark cases: CONFIG GET all |
| key | `COPY` | supported | 1 | small | no | no | Benchmark cases: COPY existing dest |
| server | `DBSIZE` | supported | 1 | small | no | no | Benchmark cases: DBSIZE empty |
| server | `DEBUG` | supported | 1 | small | no | no | Benchmark cases: DEBUG HELP |
| string | `DECR` | supported | 1 | small | no | no | Benchmark cases: DECR |
| string | `DECRBY` | supported | 1 | small | no | no | Benchmark cases: DECRBY |
| key | `DEL` | supported | 1 | small | no | no | Benchmark cases: DEL missing |
| transaction | `DISCARD` | supported | 1 | small | no | no | Benchmark cases: DISCARD queued SET |
| key | `DUMP` | supported | 1 | small | no | no | Benchmark cases: DUMP string |
| connection | `ECHO` | supported | 1 | small | no | no | Benchmark cases: ECHO |
| scripting | `EVAL` | supported | 1 | small | no | no | Constrained scripting evaluator: return values, KEYS/ARGV, tonumber, and redis.call/pcall over supported commands. Benchmark cases: EVAL return bulk |
| scripting | `EVALSHA` | supported | 1 | small | no | no | Constrained scripting evaluator: return values, KEYS/ARGV, tonumber, and redis.call/pcall over supported commands. Benchmark cases: EVALSHA return bulk |
| transaction | `EXEC` | supported | 1 | small | no | no | Benchmark cases: MULTI EXEC SET GET |
| key | `EXISTS` | supported | 1 | small | no | no | Benchmark cases: EXISTS mixed |
| key | `EXPIRE` | supported | 2 | small | no | no | Benchmark cases: EXPIRE NX, EXPIRE future |
| key | `EXPIREAT` | supported | 1 | small | no | no | Benchmark cases: EXPIREAT future |
| key | `EXPIRETIME` | supported | 1 | small | no | no | Benchmark cases: EXPIRETIME future |
| server | `FLUSHALL` | supported | 1 | destructive | yes | no | Destructive perf matrix case; run separately with `CASES=profile:destructive`. Benchmark cases: FLUSHALL one key |
| server | `FLUSHDB` | supported | 1 | destructive | yes | no | Destructive perf matrix case; run separately with `CASES=profile:destructive`. Benchmark cases: FLUSHDB one key |
| geo | `GEOADD` | supported | 1 | small | no | no | Benchmark cases: GEOADD |
| geo | `GEODIST` | supported | 1 | small | no | no | Benchmark cases: GEODIST |
| geo | `GEOHASH` | supported | 1 | small | no | no | Benchmark cases: GEOHASH |
| geo | `GEOPOS` | supported | 1 | small | no | no | Benchmark cases: GEOPOS |
| geo | `GEORADIUS` | supported | 1 | small | no | no | Benchmark cases: GEORADIUS |
| geo | `GEORADIUSBYMEMBER` | supported | 1 | small | no | no | Benchmark cases: GEORADIUSBYMEMBER |
| geo | `GEORADIUSBYMEMBER_RO` | supported | 1 | small | no | no | Benchmark cases: GEORADIUSBYMEMBER_RO |
| geo | `GEORADIUS_RO` | supported | 1 | small | no | no | Benchmark cases: GEORADIUS_RO |
| string | `GET` | supported | 3 | large, small | no | no | Benchmark cases: GET large 4KiB value, GET large 64KiB value, GET string |
| string | `GETBIT` | supported | 1 | small | no | no | Benchmark cases: GETBIT |
| string | `GETDEL` | supported | 1 | small | no | no | Benchmark cases: GETDEL |
| string | `GETEX` | supported | 1 | small | no | no | Benchmark cases: GETEX PX |
| string | `GETRANGE` | supported | 2 | large, small | no | no | Benchmark cases: GETRANGE, GETRANGE large 64KiB full |
| string | `GETSET` | supported | 1 | small | no | no | Benchmark cases: GETSET |
| hash | `HDEL` | supported | 1 | small | no | no | Benchmark cases: HDEL |
| connection | `HELLO` | supported | 1 | small | no | no | Benchmark cases: HELLO 2 |
| hash | `HEXISTS` | supported | 1 | small | no | no | Benchmark cases: HEXISTS |
| hash | `HGET` | supported | 1 | small | no | no | Benchmark cases: HGET |
| hash | `HGETALL` | supported | 2 | large, small | no | no | Benchmark cases: HGETALL, HGETALL large 1K fields |
| hash | `HINCRBY` | supported | 1 | small | no | no | Benchmark cases: HINCRBY |
| hash | `HINCRBYFLOAT` | supported | 1 | small | no | no | Benchmark cases: HINCRBYFLOAT |
| hash | `HKEYS` | supported | 2 | large, small | no | no | Benchmark cases: HKEYS, HKEYS large 1K fields |
| hash | `HLEN` | supported | 2 | large, small | no | no | Benchmark cases: HLEN, HLEN large 1K fields |
| hash | `HMGET` | supported | 2 | large, small | no | no | Benchmark cases: HMGET, HMGET large selected fields |
| hash | `HMSET` | supported | 1 | small | no | no | Benchmark cases: HMSET |
| server | `HOST:` | supported | 1 | small | no | yes | Expected RESP error reply in standalone compatibility mode. Benchmark cases: HOST attack warning |
| hash | `HRANDFIELD` | supported | 1 | small | no | no | Benchmark cases: HRANDFIELD WITHVALUES |
| hash | `HSCAN` | supported | 2 | large, small | no | no | Benchmark cases: HSCAN, HSCAN large 1K fields |
| hash | `HSET` | supported | 1 | small | no | no | Benchmark cases: HSET |
| hash | `HSETNX` | supported | 1 | small | no | no | Benchmark cases: HSETNX |
| hash | `HSTRLEN` | supported | 1 | small | no | no | Benchmark cases: HSTRLEN |
| hash | `HVALS` | supported | 2 | large, small | no | no | Benchmark cases: HVALS, HVALS large 1K fields |
| string | `INCR` | supported | 1 | small | no | no | Benchmark cases: INCR |
| string | `INCRBY` | supported | 1 | small | no | no | Benchmark cases: INCRBY |
| string | `INCRBYFLOAT` | supported | 1 | small | no | no | Benchmark cases: INCRBYFLOAT |
| server | `INFO` | supported | 1 | small | no | no | Benchmark cases: INFO |
| key | `KEYS` | supported | 2 | large, small | yes | no | Benchmark cases: KEYS all, KEYS large keyspace |
| server | `LASTSAVE` | supported | 1 | small | no | no | Benchmark cases: LASTSAVE |
| server | `LATENCY` | supported | 1 | small | no | no | Benchmark cases: LATENCY LATEST |
| list | `LINDEX` | supported | 2 | large, small | no | no | Benchmark cases: LINDEX, LINDEX large middle |
| list | `LINSERT` | supported | 1 | small | no | no | Benchmark cases: LINSERT |
| list | `LLEN` | supported | 2 | large, small | no | no | Benchmark cases: LLEN, LLEN large 1K list |
| list | `LMOVE` | supported | 1 | small | no | no | Benchmark cases: LMOVE self |
| list | `LMPOP` | supported | 1 | small | no | no | Benchmark cases: LMPOP left count |
| server | `LOLWUT` | supported | 1 | small | no | no | Benchmark cases: LOLWUT |
| list | `LPOP` | supported | 1 | small | no | no | Benchmark cases: LPOP |
| list | `LPUSH` | supported | 1 | small | no | no | Benchmark cases: LPUSH |
| list | `LPUSHX` | supported | 1 | small | no | no | Benchmark cases: LPUSHX missing |
| list | `LRANGE` | supported | 2 | large, small | no | no | Benchmark cases: LRANGE, LRANGE large 1K full |
| list | `LREM` | supported | 1 | small | no | no | Benchmark cases: LREM |
| list | `LSET` | supported | 1 | small | no | no | Benchmark cases: LSET |
| list | `LTRIM` | supported | 1 | small | no | no | Benchmark cases: LTRIM |
| server | `MEMORY` | supported | 1 | small | no | no | Benchmark cases: MEMORY USAGE string |
| string | `MGET` | supported | 1 | small | no | no | Benchmark cases: MGET order |
| server | `MIGRATE` | supported | 1 | small | no | yes | Expected RESP error reply in standalone compatibility mode. Benchmark cases: MIGRATE unsupported |
| server | `MODULE` | supported | 1 | small | no | no | Benchmark cases: MODULE LIST |
| server | `MONITOR` | supported | 1 | small | no | yes | Expected RESP error reply in standalone compatibility mode. Benchmark cases: MONITOR disabled |
| server | `MOVE` | supported | 1 | small | no | yes | Expected RESP error reply in standalone compatibility mode. Benchmark cases: MOVE same db |
| string | `MSET` | supported | 1 | small | no | no | Benchmark cases: MSET |
| string | `MSETNX` | supported | 1 | small | no | no | Benchmark cases: MSETNX existing |
| transaction | `MULTI` | supported | 1 | small | no | no | Benchmark cases: MULTI DISCARD |
| key | `OBJECT` | supported | 1 | small | no | no | Benchmark cases: OBJECT ENCODING string |
| key | `PERSIST` | supported | 1 | small | no | no | Benchmark cases: PERSIST |
| key | `PEXPIRE` | supported | 2 | small | no | no | Benchmark cases: PEXPIRE XX, PEXPIRE future |
| key | `PEXPIREAT` | supported | 1 | small | no | no | Benchmark cases: PEXPIREAT future |
| key | `PEXPIRETIME` | supported | 1 | small | no | no | Benchmark cases: PEXPIRETIME future |
| hyperloglog | `PFADD` | supported | 1 | small | no | no | Benchmark cases: PFADD |
| hyperloglog | `PFCOUNT` | supported | 1 | small | no | no | Benchmark cases: PFCOUNT |
| hyperloglog | `PFDEBUG` | supported | 1 | small | no | no | Benchmark cases: PFDEBUG ENCODING |
| hyperloglog | `PFMERGE` | supported | 1 | small | no | no | Benchmark cases: PFMERGE |
| hyperloglog | `PFSELFTEST` | supported | 1 | small | no | no | Benchmark cases: PFSELFTEST |
| connection | `PING` | supported | 1 | small | no | no | Benchmark cases: PING |
| server | `POST` | supported | 1 | small | no | yes | Expected RESP error reply in standalone compatibility mode. Benchmark cases: POST attack warning |
| string | `PSETEX` | supported | 1 | small | no | no | Benchmark cases: PSETEX |
| pubsub | `PSUBSCRIBE` | supported | 1 | small | no | no | Benchmark cases: PSUBSCRIBE ack |
| server | `PSYNC` | supported | 1 | small | no | yes | Expected RESP error reply in standalone compatibility mode. Benchmark cases: PSYNC unsupported |
| key | `PTTL` | supported | 1 | small | no | no | Benchmark cases: PTTL positive |
| pubsub | `PUBLISH` | supported | 1 | small | no | no | Benchmark cases: PUBLISH no subscribers |
| pubsub | `PUBSUB` | supported | 1 | small | no | no | Benchmark cases: PUBSUB NUMPAT |
| pubsub | `PUNSUBSCRIBE` | supported | 1 | small | no | no | Benchmark cases: PUNSUBSCRIBE ack |
| key | `RANDOMKEY` | supported | 1 | small | yes | no | Benchmark cases: RANDOMKEY nonempty |
| server | `READONLY` | supported | 1 | small | no | no | Benchmark cases: READONLY |
| server | `READWRITE` | supported | 1 | small | no | no | Benchmark cases: READWRITE |
| key | `RENAME` | supported | 2 | small | no | no | Benchmark cases: RENAME a to b, RENAME b to a |
| key | `RENAMENX` | supported | 1 | small | no | no | Benchmark cases: RENAMENX existing dest |
| server | `REPLCONF` | supported | 1 | small | no | no | Benchmark cases: REPLCONF ACK |
| server | `REPLICAOF` | supported | 1 | small | no | no | Benchmark cases: REPLICAOF NO ONE |
| key | `RESTORE` | supported | 1 | small | no | no | Benchmark cases: RESTORE string replace |
| key | `RESTORE-ASKING` | supported | 1 | small | no | no | Benchmark cases: RESTORE-ASKING string replace |
| server | `ROLE` | supported | 1 | small | no | no | Benchmark cases: ROLE |
| list | `RPOP` | supported | 1 | small | no | no | Benchmark cases: RPOP |
| list | `RPOPLPUSH` | supported | 1 | small | no | no | Benchmark cases: RPOPLPUSH self |
| list | `RPUSH` | supported | 4 | small | no | no | Benchmark cases: RPUSH, RPUSH bl seed, RPUSH bm seed, RPUSH br seed |
| list | `RPUSHX` | supported | 1 | small | no | no | Benchmark cases: RPUSHX missing |
| set | `SADD` | supported | 2 | small | no | no | Benchmark cases: SADD set-a, SADD set-b |
| server | `SAVE` | supported | 1 | small | no | no | Benchmark cases: SAVE |
| key | `SCAN` | supported | 3 | large, small | yes | no | Benchmark cases: SCAN all, SCAN large keyspace, SCAN type string |
| set | `SCARD` | supported | 2 | large, small | no | no | Benchmark cases: SCARD, SCARD large 1K set |
| scripting | `SCRIPT` | supported | 1 | small | no | no | Constrained scripting evaluator: return values, KEYS/ARGV, tonumber, and redis.call/pcall over supported commands. Benchmark cases: SCRIPT LOAD |
| set | `SDIFF` | supported | 1 | small | no | no | Benchmark cases: SDIFF |
| set | `SDIFFSTORE` | supported | 1 | small | no | no | Benchmark cases: SDIFFSTORE |
| connection | `SELECT` | supported | 1 | small | no | no | Benchmark cases: SELECT |
| string | `SET` | supported | 6 | large, small | no | no | Benchmark cases: SET EX, SET NX miss, SET XX hit, SET large 4KiB value, SET large 64KiB value, SET string |
| string | `SETBIT` | supported | 1 | small | no | no | Benchmark cases: SETBIT |
| string | `SETEX` | supported | 1 | small | no | no | Benchmark cases: SETEX |
| string | `SETNX` | supported | 1 | small | no | no | Benchmark cases: SETNX existing |
| string | `SETRANGE` | supported | 1 | small | no | no | Benchmark cases: SETRANGE |
| server | `SHUTDOWN` | supported | 1 | small | no | yes | Expected RESP error reply in standalone compatibility mode. Benchmark cases: SHUTDOWN disabled |
| set | `SINTER` | supported | 1 | small | no | no | Benchmark cases: SINTER |
| set | `SINTERSTORE` | supported | 1 | small | no | no | Benchmark cases: SINTERSTORE |
| set | `SISMEMBER` | supported | 1 | small | no | no | Benchmark cases: SISMEMBER |
| server | `SLAVEOF` | supported | 1 | small | no | no | Benchmark cases: SLAVEOF NO ONE |
| server | `SLOWLOG` | supported | 1 | small | no | no | Benchmark cases: SLOWLOG LEN |
| set | `SMEMBERS` | supported | 2 | large, small | no | no | Benchmark cases: SMEMBERS, SMEMBERS large 1K set |
| set | `SMISMEMBER` | supported | 2 | large, small | no | no | Benchmark cases: SMISMEMBER, SMISMEMBER large selected |
| set | `SMOVE` | supported | 1 | small | no | no | Benchmark cases: SMOVE |
| server | `SORT` | supported | 1 | small | no | no | Benchmark cases: SORT missing |
| set | `SPOP` | supported | 1 | small | no | no | Benchmark cases: SPOP |
| set | `SRANDMEMBER` | supported | 2 | large, small | no | no | Benchmark cases: SRANDMEMBER, SRANDMEMBER large 32 |
| set | `SREM` | supported | 1 | small | no | no | Benchmark cases: SREM |
| set | `SSCAN` | supported | 2 | large, small | no | no | Benchmark cases: SSCAN, SSCAN large 1K set |
| string | `STRLEN` | supported | 2 | large, small | no | no | Benchmark cases: STRLEN, STRLEN large 64KiB value |
| pubsub | `SUBSCRIBE` | supported | 1 | small | no | no | Benchmark cases: SUBSCRIBE ack |
| string | `SUBSTR` | supported | 1 | small | no | no | Benchmark cases: SUBSTR |
| set | `SUNION` | supported | 1 | small | no | no | Benchmark cases: SUNION |
| set | `SUNIONSTORE` | supported | 1 | small | no | no | Benchmark cases: SUNIONSTORE |
| server | `SWAPDB` | supported | 1 | small | no | no | Benchmark cases: SWAPDB 0 0 |
| server | `SYNC` | supported | 1 | small | no | yes | Expected RESP error reply in standalone compatibility mode. Benchmark cases: SYNC unsupported |
| server | `TIME` | supported | 1 | small | no | no | Benchmark cases: TIME |
| key | `TOUCH` | supported | 1 | small | no | no | Benchmark cases: TOUCH mixed |
| key | `TTL` | supported | 1 | small | no | no | Benchmark cases: TTL positive |
| key | `TYPE` | supported | 1 | small | no | no | Benchmark cases: TYPE string |
| key | `UNLINK` | supported | 1 | small | no | no | Benchmark cases: UNLINK missing |
| pubsub | `UNSUBSCRIBE` | supported | 1 | small | no | no | Benchmark cases: UNSUBSCRIBE ack |
| transaction | `UNWATCH` | supported | 1 | small | no | no | Benchmark cases: UNWATCH simple |
| server | `WAIT` | supported | 1 | small | no | no | Benchmark cases: WAIT |
| transaction | `WATCH` | supported | 1 | small | no | no | Benchmark cases: WATCH simple |
| stream | `XACK` | supported | 1 | small | no | no | Benchmark cases: XACK empty |
| stream | `XADD` | supported | 1 | small | no | no | Benchmark cases: XADD |
| stream | `XCLAIM` | supported | 1 | small | no | no | Benchmark cases: XCLAIM empty |
| stream | `XDEL` | supported | 1 | small | no | no | Benchmark cases: XDEL |
| stream | `XGROUP` | supported | 1 | small | no | no | Benchmark cases: XGROUP CREATECONSUMER |
| stream | `XINFO` | supported | 1 | small | no | no | Benchmark cases: XINFO STREAM |
| stream | `XLEN` | supported | 1 | small | no | no | Benchmark cases: XLEN |
| stream | `XPENDING` | supported | 1 | small | no | no | Benchmark cases: XPENDING summary |
| stream | `XRANGE` | supported | 1 | small | no | no | Benchmark cases: XRANGE |
| stream | `XREAD` | supported | 1 | small | no | no | Benchmark cases: XREAD |
| stream | `XREADGROUP` | supported | 1 | small | no | no | Benchmark cases: XREADGROUP |
| stream | `XREVRANGE` | supported | 1 | small | no | no | Benchmark cases: XREVRANGE |
| stream | `XSETID` | supported | 1 | small | no | no | Benchmark cases: XSETID |
| stream | `XTRIM` | supported | 1 | small | no | no | Benchmark cases: XTRIM |
| zset | `ZADD` | supported | 10 | small | no | no | Benchmark cases: ZADD, ZADD CH, ZADD NX, ZADD XX GT, ZADD bzmax, ZADD bzmin, ZADD z2 seed, ZADD zlex seed, ZADD zu1, ZADD zu2 |
| zset | `ZCARD` | supported | 1 | small | no | no | Benchmark cases: ZCARD |
| zset | `ZCOUNT` | supported | 2 | large, small | no | no | Benchmark cases: ZCOUNT, ZCOUNT large 1K all |
| zset | `ZDIFF` | supported | 1 | small | no | no | Benchmark cases: ZDIFF |
| zset | `ZDIFFSTORE` | supported | 1 | small | no | no | Benchmark cases: ZDIFFSTORE |
| zset | `ZINCRBY` | supported | 1 | small | no | no | Benchmark cases: ZINCRBY |
| zset | `ZINTER` | supported | 1 | small | no | no | Benchmark cases: ZINTER WITHSCORES |
| zset | `ZINTERCARD` | supported | 1 | small | no | no | Benchmark cases: ZINTERCARD |
| zset | `ZINTERSTORE` | supported | 1 | small | no | no | Benchmark cases: ZINTERSTORE weighted |
| zset | `ZLEXCOUNT` | supported | 1 | small | no | no | Benchmark cases: ZLEXCOUNT |
| zset | `ZMPOP` | supported | 1 | small | no | no | Benchmark cases: ZMPOP min count |
| zset | `ZMSCORE` | supported | 1 | small | no | no | Benchmark cases: ZMSCORE z2 |
| zset | `ZPOPMAX` | supported | 1 | small | no | no | Benchmark cases: ZPOPMAX |
| zset | `ZPOPMIN` | supported | 1 | small | no | no | Benchmark cases: ZPOPMIN |
| zset | `ZRANDMEMBER` | supported | 1 | small | no | no | Benchmark cases: ZRANDMEMBER WITHSCORES |
| zset | `ZRANGE` | supported | 9 | large, small | no | no | Benchmark cases: ZRANGE, ZRANGE BYSCORE REV LIMIT, ZRANGE WITHSCORES, ZRANGE large 100 WITHSCORES, ZRANGE large 1K full, ZRANGE zd, ZRANGE zi, ZRANGE zout, ZRANGE zstored |
| zset | `ZRANGEBYLEX` | supported | 1 | small | no | no | Benchmark cases: ZRANGEBYLEX |
| zset | `ZRANGEBYSCORE` | supported | 1 | small | no | no | Benchmark cases: ZRANGEBYSCORE exclusive |
| zset | `ZRANGESTORE` | supported | 1 | small | no | no | Benchmark cases: ZRANGESTORE |
| zset | `ZRANK` | supported | 2 | large, small | no | no | Benchmark cases: ZRANK, ZRANK large middle |
| zset | `ZREM` | supported | 1 | small | no | no | Benchmark cases: ZREM missing |
| zset | `ZREMRANGEBYLEX` | supported | 1 | small | no | no | Benchmark cases: ZREMRANGEBYLEX no-op |
| zset | `ZREMRANGEBYRANK` | supported | 1 | small | no | no | Benchmark cases: ZREMRANGEBYRANK no-op |
| zset | `ZREMRANGEBYSCORE` | supported | 1 | small | no | no | Benchmark cases: ZREMRANGEBYSCORE no-op |
| zset | `ZREVRANGE` | supported | 1 | small | no | no | Benchmark cases: ZREVRANGE |
| zset | `ZREVRANGEBYLEX` | supported | 1 | small | no | no | Benchmark cases: ZREVRANGEBYLEX |
| zset | `ZREVRANGEBYSCORE` | supported | 1 | small | no | no | Benchmark cases: ZREVRANGEBYSCORE limit |
| zset | `ZREVRANK` | supported | 2 | large, small | no | no | Benchmark cases: ZREVRANK, ZREVRANK large middle |
| zset | `ZSCAN` | supported | 2 | large, small | no | no | Benchmark cases: ZSCAN, ZSCAN large 1K zset |
| zset | `ZSCORE` | supported | 3 | large, small | no | no | Benchmark cases: ZSCORE, ZSCORE large middle, ZSCORE z2 |
| zset | `ZUNION` | supported | 1 | small | no | no | Benchmark cases: ZUNION WITHSCORES |
| zset | `ZUNIONSTORE` | supported | 1 | small | no | no | Benchmark cases: ZUNIONSTORE weighted |
