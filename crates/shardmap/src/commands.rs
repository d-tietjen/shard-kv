pub(crate) mod parsing;

#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/formal/bounds.rs"]
pub(crate) mod formal_bounds;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/formal/range.rs"]
pub(crate) mod formal_range;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/formal/rank.rs"]
pub(crate) mod formal_rank;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/formal/transactions.rs"]
pub(crate) mod formal_transactions;

#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/array.rs"]
pub mod array;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/blocking.rs"]
pub(crate) mod blocking;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/redis.rs"]
pub(crate) mod redis;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/vector_set.rs"]
pub mod vector_set;

// Key commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/copy.rs"]
pub mod copy;
#[path = "commands/key/del.rs"]
pub mod del;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/dump.rs"]
pub mod dump;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/dump_restore.rs"]
pub(crate) mod dump_restore;
#[path = "commands/key/exists.rs"]
pub mod exists;
#[path = "commands/key/expire.rs"]
pub mod expire;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/expireat.rs"]
pub mod expireat;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/expiretime.rs"]
pub mod expiretime;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/keys.rs"]
pub mod keys;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/object.rs"]
pub mod object;
#[path = "commands/key/persist.rs"]
pub mod persist;
#[path = "commands/key/pexpire.rs"]
pub mod pexpire;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/pexpireat.rs"]
pub mod pexpireat;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/pexpiretime.rs"]
pub mod pexpiretime;
#[path = "commands/key/pttl.rs"]
pub mod pttl;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/randomkey.rs"]
pub mod randomkey;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/rename.rs"]
pub mod rename;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/renamenx.rs"]
pub mod renamenx;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/restore.rs"]
pub mod restore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/scan.rs"]
pub mod scan;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/touch.rs"]
pub mod touch;
#[path = "commands/key/ttl.rs"]
pub mod ttl;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/type_cmd.rs"]
pub mod type_cmd;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/key/unlink.rs"]
pub mod unlink;

// String commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/append.rs"]
pub mod append;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/bitcount.rs"]
pub mod bitcount;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/bitfield.rs"]
pub mod bitfield;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/bitfield_ro.rs"]
pub mod bitfield_ro;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/bitop.rs"]
pub mod bitop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/bitpos.rs"]
pub mod bitpos;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/decr.rs"]
pub mod decr;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/decrby.rs"]
pub mod decrby;
#[path = "commands/string/get.rs"]
pub mod get;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/getbit.rs"]
pub mod getbit;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/getdel.rs"]
pub mod getdel;
#[path = "commands/string/getex.rs"]
pub mod getex;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/getrange.rs"]
pub mod getrange;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/getset.rs"]
pub mod getset;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/incr.rs"]
pub mod incr;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/incrby.rs"]
pub mod incrby;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/incrbyfloat.rs"]
pub mod incrbyfloat;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/lcs.rs"]
pub mod lcs;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/mget.rs"]
pub mod mget;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/mset.rs"]
pub mod mset;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/msetnx.rs"]
pub mod msetnx;
#[path = "commands/string/psetex.rs"]
pub mod psetex;
#[cfg(feature = "redis-modules")]
#[path = "../../shardcache-redis/src/commands/redis_modules.rs"]
pub mod redis_modules;
#[cfg(feature = "server")]
#[path = "commands/semantic.rs"]
pub(crate) mod semantic;
#[path = "commands/string/set.rs"]
pub mod set;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/setbit.rs"]
pub mod setbit;
#[path = "commands/string/setex.rs"]
pub mod setex;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/setnx.rs"]
pub mod setnx;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/setrange.rs"]
pub mod setrange;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/stralgo.rs"]
pub mod stralgo;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/bit_shared.rs"]
pub(crate) mod string_bits;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/shared.rs"]
pub(crate) mod string_shared;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/v8.rs"]
pub mod string_v8;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/string/strlen.rs"]
pub mod strlen;

// Connection commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/connection/auth.rs"]
pub mod auth;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/connection/echo.rs"]
pub mod echo;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/connection/hello.rs"]
pub mod hello;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/connection/ping.rs"]
pub mod ping;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/connection/quit.rs"]
pub mod quit;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/connection/reset.rs"]
pub mod reset;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/connection/select.rs"]
pub mod select;

// Server commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/acl.rs"]
pub mod acl;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/admin.rs"]
pub mod admin;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/client.rs"]
pub mod client;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/command.rs"]
pub mod command;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/config.rs"]
pub mod config;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/dbsize.rs"]
pub mod dbsize;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/flush.rs"]
pub mod flush;
#[cfg(feature = "redis-functions")]
#[path = "../../shardcache-redis/src/commands/server/function_cmd.rs"]
pub mod function_cmd;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/hotkeys.rs"]
pub mod hotkeys;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/info.rs"]
pub mod info;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/memory.rs"]
pub mod memory;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/time.rs"]
pub mod time;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/server/waitaof.rs"]
pub mod waitaof;

// Pub/Sub commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/pubsub.rs"]
pub mod pubsub;

// Scripting commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/scripting.rs"]
pub mod scripting;

// HyperLogLog commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hll.rs"]
pub mod hll;

// Geo commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/geo.rs"]
pub mod geo;

// Stream commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/stream.rs"]
pub mod stream;

// Hash commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hdel.rs"]
pub mod hdel;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hexists.rs"]
pub mod hexists;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hexpire.rs"]
pub mod hexpire;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hexpireat.rs"]
pub mod hexpireat;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hexpiretime.rs"]
pub mod hexpiretime;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hget.rs"]
pub mod hget;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hgetall.rs"]
pub mod hgetall;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hgetdel.rs"]
pub mod hgetdel;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hgetex.rs"]
pub mod hgetex;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hincrby.rs"]
pub mod hincrby;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hincrbyfloat.rs"]
pub mod hincrbyfloat;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hkeys.rs"]
pub mod hkeys;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hlen.rs"]
pub mod hlen;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hmget.rs"]
pub mod hmget;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hmset.rs"]
pub mod hmset;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hpersist.rs"]
pub mod hpersist;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hpexpire.rs"]
pub mod hpexpire;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hpexpireat.rs"]
pub mod hpexpireat;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hpexpiretime.rs"]
pub mod hpexpiretime;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hpttl.rs"]
pub mod hpttl;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hrandfield.rs"]
pub mod hrandfield;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hscan.rs"]
pub mod hscan;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hset.rs"]
pub mod hset;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hsetex.rs"]
pub mod hsetex;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hsetnx.rs"]
pub mod hsetnx;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hstrlen.rs"]
pub mod hstrlen;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/httl.rs"]
pub mod httl;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/hash/hvals.rs"]
pub mod hvals;

// List commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/blmove.rs"]
pub mod blmove;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/blmpop.rs"]
pub mod blmpop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/blpop.rs"]
pub mod blpop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/brpop.rs"]
pub mod brpop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/brpoplpush.rs"]
pub mod brpoplpush;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lindex.rs"]
pub mod lindex;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/linsert.rs"]
pub mod linsert;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/shared.rs"]
pub(crate) mod list_shared;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/llen.rs"]
pub mod llen;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lmove.rs"]
pub mod lmove;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lmpop.rs"]
pub mod lmpop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lpop.rs"]
pub mod lpop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lpos.rs"]
pub mod lpos;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lpush.rs"]
pub mod lpush;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lpushx.rs"]
pub mod lpushx;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lrange.rs"]
pub mod lrange;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lrem.rs"]
pub mod lrem;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/lset.rs"]
pub mod lset;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/ltrim.rs"]
pub mod ltrim;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/rpop.rs"]
pub mod rpop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/rpoplpush.rs"]
pub mod rpoplpush;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/rpush.rs"]
pub mod rpush;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/list/rpushx.rs"]
pub mod rpushx;

// Set collection commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sadd.rs"]
pub mod sadd;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/scard.rs"]
pub mod scard;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sdiff.rs"]
pub mod sdiff;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sdiffstore.rs"]
pub mod sdiffstore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/shared.rs"]
pub(crate) mod set_shared;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sinter.rs"]
pub mod sinter;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sintercard.rs"]
pub mod sintercard;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sinterstore.rs"]
pub mod sinterstore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sismember.rs"]
pub mod sismember;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/smembers.rs"]
pub mod smembers;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/smismember.rs"]
pub mod smismember;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/smove.rs"]
pub mod smove;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/spop.rs"]
pub mod spop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/srandmember.rs"]
pub mod srandmember;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/srem.rs"]
pub mod srem;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sscan.rs"]
pub mod sscan;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sunion.rs"]
pub mod sunion;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/sets/sunionstore.rs"]
pub mod sunionstore;

// Sorted set commands.
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/bzmpop.rs"]
pub mod bzmpop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/bzpopmax.rs"]
pub mod bzpopmax;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/bzpopmin.rs"]
pub mod bzpopmin;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zadd.rs"]
pub mod zadd;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zcard.rs"]
pub mod zcard;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zcount.rs"]
pub mod zcount;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zdiff.rs"]
pub mod zdiff;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zdiffstore.rs"]
pub mod zdiffstore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zincrby.rs"]
pub mod zincrby;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zinter.rs"]
pub mod zinter;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zintercard.rs"]
pub mod zintercard;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zinterstore.rs"]
pub mod zinterstore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zlexcount.rs"]
pub mod zlexcount;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zmpop.rs"]
pub mod zmpop;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zmscore.rs"]
pub mod zmscore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zpopmax.rs"]
pub mod zpopmax;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zpopmin.rs"]
pub mod zpopmin;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrandmember.rs"]
pub mod zrandmember;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrange.rs"]
pub mod zrange;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrangebylex.rs"]
pub mod zrangebylex;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrangebyscore.rs"]
pub mod zrangebyscore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrangestore.rs"]
pub mod zrangestore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrank.rs"]
pub mod zrank;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrem.rs"]
pub mod zrem;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zremrangebylex.rs"]
pub mod zremrangebylex;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zremrangebyrank.rs"]
pub mod zremrangebyrank;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zremrangebyscore.rs"]
pub mod zremrangebyscore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrevrange.rs"]
pub mod zrevrange;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrevrangebylex.rs"]
pub mod zrevrangebylex;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrevrangebyscore.rs"]
pub mod zrevrangebyscore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zrevrank.rs"]
pub mod zrevrank;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zscan.rs"]
pub mod zscan;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zscore.rs"]
pub mod zscore;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/shared.rs"]
pub(crate) mod zset_shared;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zunion.rs"]
pub mod zunion;
#[cfg(feature = "redis")]
#[path = "../../shardcache-redis/src/commands/zset/zunionstore.rs"]
pub mod zunionstore;

use bytes::Bytes as SharedBytes;

use crate::protocol::{CommandSpanFrame, FastCommand, FastRequest, FastResponse, Frame};
use crate::storage::{
    EngineCommandContext, EngineFastFuture, EngineFrameFuture, EngineRespSpanFuture,
};
use crate::{Result, ShardCacheError};

pub(crate) trait CommandSpec {
    const NAME: &'static str;
    const MUTATES_VALUE: bool;

    #[inline(always)]
    fn matches(name: &[u8]) -> bool {
        name.eq_ignore_ascii_case(Self::NAME.as_bytes())
    }
}

pub(crate) trait OwnedCommandParse: CommandSpec {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<crate::storage::Command>;
}

pub(crate) type OwnedCommandBox = Box<dyn OwnedCommandObject>;

/// Command-owned data that was parsed from an owned RESP frame.
///
/// Implement this for the concrete owned command payload. The blanket
/// `OwnedCommandObject` impl supplies the command name and mutation metadata
/// from the command spec so each command file does not repeat that boilerplate.
pub(crate) trait OwnedCommandData: std::fmt::Debug + Send + Sync {
    type Spec: CommandSpec;

    fn route_key(&self) -> Option<&[u8]>;
    fn to_borrowed_command(&self) -> BorrowedCommandBox<'_>;
}

/// Parsed owned command data owned by a concrete command module.
pub(crate) trait OwnedCommandObject: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn mutates_value(&self) -> bool;
    fn route_key(&self) -> Option<&[u8]>;
    fn to_borrowed_command(&self) -> BorrowedCommandBox<'_>;
}

impl<T> OwnedCommandObject for T
where
    T: OwnedCommandData,
{
    fn name(&self) -> &'static str {
        <T::Spec as CommandSpec>::NAME
    }

    fn mutates_value(&self) -> bool {
        <T::Spec as CommandSpec>::MUTATES_VALUE
    }

    fn route_key(&self) -> Option<&[u8]> {
        <T as OwnedCommandData>::route_key(self)
    }

    fn to_borrowed_command(&self) -> BorrowedCommandBox<'_> {
        <T as OwnedCommandData>::to_borrowed_command(self)
    }
}

pub(crate) type BorrowedCommandBox<'a> = Box<dyn BorrowedCommandObject<'a> + 'a>;

/// Command-owned data that borrows directly from a decoded request buffer.
///
/// The blanket `BorrowedCommandObject` impl keeps name/mutation metadata
/// centralized while allowing each command file to own execution details.
pub(crate) trait BorrowedCommandData<'a>: std::fmt::Debug + Send + Sync {
    type Spec: CommandSpec;

    fn route_key(&self) -> Option<&'a [u8]>;
    fn supports_spanned_resp(&self) -> bool {
        false
    }
    fn to_owned_command(&self) -> crate::storage::Command;
    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b;

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, now_ms: u64) -> Frame;

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: crate::server::commands::BorrowedCommandContext<'_, '_, '_>);

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: crate::server::commands::DirectCommandContext) -> Frame;
}

/// Parsed borrowed command data owned by a concrete command module.
pub(crate) trait BorrowedCommandObject<'a>: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn mutates_value(&self) -> bool;
    fn route_key(&self) -> Option<&'a [u8]>;
    fn supports_spanned_resp(&self) -> bool;
    fn to_owned_command(&self) -> crate::storage::Command;
    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b;

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, now_ms: u64) -> Frame;

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: crate::server::commands::BorrowedCommandContext<'_, '_, '_>);

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: crate::server::commands::DirectCommandContext) -> Frame;
}

impl<'a, T> BorrowedCommandObject<'a> for T
where
    T: BorrowedCommandData<'a>,
{
    fn name(&self) -> &'static str {
        <T::Spec as CommandSpec>::NAME
    }

    fn mutates_value(&self) -> bool {
        <T::Spec as CommandSpec>::MUTATES_VALUE
    }

    fn route_key(&self) -> Option<&'a [u8]> {
        <T as BorrowedCommandData<'a>>::route_key(self)
    }

    fn supports_spanned_resp(&self) -> bool {
        <T as BorrowedCommandData<'a>>::supports_spanned_resp(self)
    }

    fn to_owned_command(&self) -> crate::storage::Command {
        <T as BorrowedCommandData<'a>>::to_owned_command(self)
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        <T as BorrowedCommandData<'a>>::execute_engine(self, ctx)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, now_ms: u64) -> Frame {
        <T as BorrowedCommandData<'a>>::execute_borrowed_frame(self, store, now_ms)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: crate::server::commands::BorrowedCommandContext<'_, '_, '_>) {
        <T as BorrowedCommandData<'a>>::execute_borrowed(self, ctx);
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: crate::server::commands::DirectCommandContext) -> Frame {
        <T as BorrowedCommandData<'a>>::execute_direct_borrowed(self, ctx)
    }
}

pub(crate) trait BorrowedCommandParse<'a>: CommandSpec {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<BorrowedCommandBox<'a>>;
}

/// Object-safe metadata shared by command implementations.
pub(crate) trait CommandMetadata: Sync {
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
    fn mutates_value(&self) -> bool;
    fn matches(&self, name: &[u8]) -> bool;
}

impl<T> CommandMetadata for T
where
    T: CommandSpec + Sync,
{
    fn name(&self) -> &'static str {
        T::NAME
    }

    fn mutates_value(&self) -> bool {
        T::MUTATES_VALUE
    }

    #[inline(always)]
    fn matches(&self, name: &[u8]) -> bool {
        <T as CommandSpec>::matches(name)
    }
}

/// Object-safe parser entry owned by a command object.
pub(crate) trait CommandDefinition: CommandMetadata {
    fn parse_owned(&self, parts: &[Vec<u8>]) -> Result<crate::storage::Command>;
    fn parse_borrowed<'a>(&self, parts: &[&'a [u8]]) -> Result<BorrowedCommandBox<'a>>;
}

impl<T> CommandDefinition for T
where
    T: CommandSpec + OwnedCommandParse + Sync,
    for<'a> T: BorrowedCommandParse<'a>,
{
    fn parse_owned(&self, parts: &[Vec<u8>]) -> Result<crate::storage::Command> {
        <T as OwnedCommandParse>::parse_owned(parts)
    }

    fn parse_borrowed<'a>(&self, parts: &[&'a [u8]]) -> Result<BorrowedCommandBox<'a>> {
        <T as BorrowedCommandParse<'a>>::parse_borrowed(parts)
    }
}

pub(crate) trait DecodedFastCommand: CommandMetadata {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool;
}

pub(crate) trait EngineCommandDispatch: DecodedFastCommand {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a>;
}

pub(crate) trait EngineRespSpanCommandDispatch: CommandMetadata {
    fn execute_engine_resp_spanned<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        frame: CommandSpanFrame,
        owner: SharedBytes,
        out: &'a mut Vec<u8>,
    ) -> EngineRespSpanFuture<'a>;
}

pub(crate) static CATALOG: &[&dyn CommandDefinition] = &[
    #[cfg(feature = "redis")]
    &array::ARCOUNT_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARDEL_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARDELRANGE_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARGET_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARGETRANGE_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARGREP_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARINFO_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARINSERT_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARLASTITEMS_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARLEN_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARMGET_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARMSET_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARNEXT_COMMAND,
    #[cfg(feature = "redis")]
    &array::AROP_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARRING_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARSCAN_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARSEEK_COMMAND,
    #[cfg(feature = "redis")]
    &array::ARSET_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VADD_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VCARD_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VDIM_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VEMB_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VGETATTR_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VINFO_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VISMEMBER_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VLINKS_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VRANDMEMBER_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VRANGE_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VREM_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VSETATTR_COMMAND,
    #[cfg(feature = "redis")]
    &vector_set::VSIM_COMMAND,
    &get::COMMAND,
    &set::COMMAND,
    &del::COMMAND,
    &exists::COMMAND,
    &ttl::COMMAND,
    &pttl::COMMAND,
    &expire::COMMAND,
    &pexpire::COMMAND,
    #[cfg(feature = "redis")]
    &expireat::COMMAND,
    #[cfg(feature = "redis")]
    &pexpireat::COMMAND,
    #[cfg(feature = "redis")]
    &expiretime::COMMAND,
    #[cfg(feature = "redis")]
    &pexpiretime::COMMAND,
    &persist::COMMAND,
    &getex::COMMAND,
    &setex::COMMAND,
    &psetex::COMMAND,
    #[cfg(feature = "redis")]
    &ping::COMMAND,
    #[cfg(feature = "redis")]
    &auth::COMMAND,
    #[cfg(feature = "redis")]
    &hello::COMMAND,
    #[cfg(feature = "redis")]
    &select::COMMAND,
    #[cfg(feature = "redis")]
    &quit::COMMAND,
    #[cfg(feature = "redis")]
    &echo::COMMAND,
    #[cfg(feature = "redis")]
    &admin::ASKING_COMMAND,
    #[cfg(feature = "redis")]
    &admin::BGREWRITEAOF_COMMAND,
    #[cfg(feature = "redis")]
    &admin::BGSAVE_COMMAND,
    #[cfg(feature = "redis")]
    &admin::CLUSTER_COMMAND,
    #[cfg(feature = "redis")]
    &admin::DEBUG_COMMAND,
    #[cfg(feature = "redis")]
    &admin::FAILOVER_COMMAND,
    #[cfg(feature = "redis")]
    &acl::COMMAND,
    #[cfg(feature = "redis")]
    &waitaof::COMMAND,
    #[cfg(feature = "redis-functions")]
    &function_cmd::FUNCTION_COMMAND,
    #[cfg(feature = "redis-functions")]
    &function_cmd::FCALL_COMMAND,
    #[cfg(feature = "redis-functions")]
    &function_cmd::FCALL_RO_COMMAND,
    #[cfg(feature = "redis")]
    &reset::COMMAND,
    #[cfg(feature = "redis")]
    &lpos::COMMAND,
    #[cfg(feature = "redis")]
    &lcs::COMMAND,
    #[cfg(feature = "redis")]
    &stralgo::COMMAND,
    #[cfg(feature = "redis")]
    &stream::XAUTOCLAIM_COMMAND,
    #[cfg(feature = "redis")]
    &admin::HOST_WARNING_COMMAND,
    #[cfg(feature = "redis")]
    &admin::LASTSAVE_COMMAND,
    #[cfg(feature = "redis")]
    &admin::LATENCY_COMMAND,
    #[cfg(feature = "redis")]
    &admin::LOLWUT_COMMAND,
    #[cfg(feature = "redis")]
    &admin::MIGRATE_COMMAND,
    #[cfg(feature = "redis-modules")]
    &admin::MODULE_COMMAND,
    #[cfg(feature = "redis")]
    &admin::MONITOR_COMMAND,
    #[cfg(feature = "redis")]
    &admin::MOVE_COMMAND,
    #[cfg(feature = "redis")]
    &admin::POST_WARNING_COMMAND,
    #[cfg(feature = "redis")]
    &admin::PSYNC_COMMAND,
    #[cfg(feature = "redis")]
    &admin::READONLY_COMMAND,
    #[cfg(feature = "redis")]
    &admin::READWRITE_COMMAND,
    #[cfg(feature = "redis")]
    &admin::REPLCONF_COMMAND,
    #[cfg(feature = "redis")]
    &admin::REPLICAOF_COMMAND,
    #[cfg(feature = "redis")]
    &admin::ROLE_COMMAND,
    #[cfg(feature = "redis")]
    &admin::SAVE_COMMAND,
    #[cfg(feature = "redis")]
    &admin::SHUTDOWN_COMMAND,
    #[cfg(feature = "redis")]
    &admin::SLOWLOG_COMMAND,
    #[cfg(feature = "redis")]
    &admin::SORT_COMMAND,
    #[cfg(feature = "redis")]
    &admin::SORT_RO_COMMAND,
    #[cfg(feature = "redis")]
    &admin::SWAPDB_COMMAND,
    #[cfg(feature = "redis")]
    &admin::SYNC_COMMAND,
    #[cfg(feature = "redis")]
    &admin::WAIT_COMMAND,
    #[cfg(feature = "redis")]
    &command::COMMAND,
    #[cfg(feature = "redis")]
    &scripting::EVAL_COMMAND,
    #[cfg(feature = "redis")]
    &scripting::EVAL_RO_COMMAND,
    #[cfg(feature = "redis")]
    &scripting::EVALSHA_COMMAND,
    #[cfg(feature = "redis")]
    &scripting::EVALSHA_RO_COMMAND,
    #[cfg(feature = "redis")]
    &scripting::SCRIPT_COMMAND,
    #[cfg(feature = "redis")]
    &config::COMMAND,
    #[cfg(feature = "redis")]
    &client::COMMAND,
    #[cfg(feature = "redis")]
    &dbsize::COMMAND,
    #[cfg(feature = "redis")]
    &flush::FLUSHDB_COMMAND,
    #[cfg(feature = "redis")]
    &flush::FLUSHALL_COMMAND,
    #[cfg(feature = "redis")]
    &time::COMMAND,
    #[cfg(feature = "redis")]
    &info::COMMAND,
    #[cfg(feature = "redis")]
    &hotkeys::COMMAND,
    #[cfg(feature = "redis")]
    &memory::COMMAND,
    #[cfg(feature = "redis")]
    &pubsub::PUBLISH_COMMAND,
    #[cfg(feature = "redis")]
    &pubsub::SPUBLISH_COMMAND,
    #[cfg(feature = "redis")]
    &pubsub::PUBSUB_COMMAND,
    #[cfg(feature = "redis")]
    &pubsub::SUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &pubsub::UNSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &pubsub::PSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &pubsub::PUNSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &pubsub::SSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &pubsub::SUNSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &hll::PFADD_COMMAND,
    #[cfg(feature = "redis")]
    &hll::PFCOUNT_COMMAND,
    #[cfg(feature = "redis")]
    &hll::PFMERGE_COMMAND,
    #[cfg(feature = "redis")]
    &hll::PFDEBUG_COMMAND,
    #[cfg(feature = "redis")]
    &hll::PFSELFTEST_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEOADD_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEODIST_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEOHASH_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEOPOS_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEORADIUS_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEORADIUSBYMEMBER_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEORADIUS_RO_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEORADIUSBYMEMBER_RO_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEOSEARCH_COMMAND,
    #[cfg(feature = "redis")]
    &geo::GEOSEARCHSTORE_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XACK_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XADD_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XCLAIM_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XDEL_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XGROUP_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XINFO_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XLEN_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XPENDING_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XRANGE_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XREAD_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XREADGROUP_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XREVRANGE_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XSETID_COMMAND,
    #[cfg(feature = "redis")]
    &stream::XTRIM_COMMAND,
    #[cfg(feature = "redis")]
    &object::COMMAND,
    #[cfg(feature = "redis")]
    &keys::COMMAND,
    #[cfg(feature = "redis")]
    &scan::COMMAND,
    #[cfg(feature = "redis")]
    &type_cmd::COMMAND,
    #[cfg(feature = "redis")]
    &touch::COMMAND,
    #[cfg(feature = "redis")]
    &randomkey::COMMAND,
    #[cfg(feature = "redis")]
    &copy::COMMAND,
    #[cfg(feature = "redis")]
    &dump::COMMAND,
    #[cfg(feature = "redis")]
    &restore::COMMAND,
    #[cfg(feature = "redis")]
    &rename::COMMAND,
    #[cfg(feature = "redis")]
    &renamenx::COMMAND,
    #[cfg(feature = "redis")]
    &unlink::COMMAND,
    #[cfg(feature = "redis")]
    &append::COMMAND,
    #[cfg(feature = "redis")]
    &strlen::COMMAND,
    #[cfg(feature = "redis")]
    &getrange::COMMAND,
    #[cfg(feature = "redis")]
    &setrange::COMMAND,
    #[cfg(feature = "redis")]
    &getbit::COMMAND,
    #[cfg(feature = "redis")]
    &setbit::COMMAND,
    #[cfg(feature = "redis")]
    &bitcount::COMMAND,
    #[cfg(feature = "redis")]
    &bitpos::COMMAND,
    #[cfg(feature = "redis")]
    &bitop::COMMAND,
    #[cfg(feature = "redis")]
    &bitfield::COMMAND,
    #[cfg(feature = "redis")]
    &bitfield_ro::COMMAND,
    #[cfg(feature = "redis")]
    &getset::COMMAND,
    #[cfg(feature = "redis")]
    &getdel::COMMAND,
    #[cfg(feature = "redis")]
    &string_v8::DELEX_COMMAND,
    #[cfg(feature = "redis")]
    &string_v8::DIGEST_COMMAND,
    #[cfg(feature = "redis")]
    &incr::COMMAND,
    #[cfg(feature = "redis")]
    &string_v8::INCREX_COMMAND,
    #[cfg(feature = "redis")]
    &incrby::COMMAND,
    #[cfg(feature = "redis")]
    &decr::COMMAND,
    #[cfg(feature = "redis")]
    &decrby::COMMAND,
    #[cfg(feature = "redis")]
    &incrbyfloat::COMMAND,
    #[cfg(feature = "redis")]
    &mset::COMMAND,
    #[cfg(feature = "redis")]
    &string_v8::MSETEX_COMMAND,
    #[cfg(feature = "redis")]
    &mget::COMMAND,
    #[cfg(feature = "redis")]
    &msetnx::COMMAND,
    #[cfg(feature = "redis")]
    &setnx::COMMAND,
    #[cfg(feature = "redis")]
    &hset::COMMAND,
    #[cfg(feature = "redis")]
    &hget::COMMAND,
    #[cfg(feature = "redis")]
    &hgetdel::COMMAND,
    #[cfg(feature = "redis")]
    &hgetex::COMMAND,
    #[cfg(feature = "redis")]
    &hmset::COMMAND,
    #[cfg(feature = "redis")]
    &hmget::COMMAND,
    #[cfg(feature = "redis")]
    &hlen::COMMAND,
    #[cfg(feature = "redis")]
    &hexists::COMMAND,
    #[cfg(feature = "redis")]
    &hexpire::COMMAND,
    #[cfg(feature = "redis")]
    &hpexpire::COMMAND,
    #[cfg(feature = "redis")]
    &hexpireat::COMMAND,
    #[cfg(feature = "redis")]
    &hpexpireat::COMMAND,
    #[cfg(feature = "redis")]
    &httl::COMMAND,
    #[cfg(feature = "redis")]
    &hpttl::COMMAND,
    #[cfg(feature = "redis")]
    &hexpiretime::COMMAND,
    #[cfg(feature = "redis")]
    &hpexpiretime::COMMAND,
    #[cfg(feature = "redis")]
    &hpersist::COMMAND,
    #[cfg(feature = "redis")]
    &hsetex::COMMAND,
    #[cfg(feature = "redis")]
    &hsetnx::COMMAND,
    #[cfg(feature = "redis")]
    &hstrlen::COMMAND,
    #[cfg(feature = "redis")]
    &hincrby::COMMAND,
    #[cfg(feature = "redis")]
    &hincrbyfloat::COMMAND,
    #[cfg(feature = "redis")]
    &hkeys::COMMAND,
    #[cfg(feature = "redis")]
    &hvals::COMMAND,
    #[cfg(feature = "redis")]
    &hgetall::COMMAND,
    #[cfg(feature = "redis")]
    &hscan::COMMAND,
    #[cfg(feature = "redis")]
    &hrandfield::COMMAND,
    #[cfg(feature = "redis")]
    &hdel::COMMAND,
    #[cfg(feature = "redis")]
    &lpush::COMMAND,
    #[cfg(feature = "redis")]
    &rpush::COMMAND,
    #[cfg(feature = "redis")]
    &lrange::COMMAND,
    #[cfg(feature = "redis")]
    &llen::COMMAND,
    #[cfg(feature = "redis")]
    &lindex::COMMAND,
    #[cfg(feature = "redis")]
    &lset::COMMAND,
    #[cfg(feature = "redis")]
    &lrem::COMMAND,
    #[cfg(feature = "redis")]
    &linsert::COMMAND,
    #[cfg(feature = "redis")]
    &ltrim::COMMAND,
    #[cfg(feature = "redis")]
    &lpop::COMMAND,
    #[cfg(feature = "redis")]
    &rpop::COMMAND,
    #[cfg(feature = "redis")]
    &rpoplpush::COMMAND,
    #[cfg(feature = "redis")]
    &lmove::COMMAND,
    #[cfg(feature = "redis")]
    &lpushx::COMMAND,
    #[cfg(feature = "redis")]
    &rpushx::COMMAND,
    #[cfg(feature = "redis")]
    &blpop::COMMAND,
    #[cfg(feature = "redis")]
    &brpop::COMMAND,
    #[cfg(feature = "redis")]
    &brpoplpush::COMMAND,
    #[cfg(feature = "redis")]
    &blmove::COMMAND,
    #[cfg(feature = "redis")]
    &lmpop::COMMAND,
    #[cfg(feature = "redis")]
    &blmpop::COMMAND,
    #[cfg(feature = "redis")]
    &sadd::COMMAND,
    #[cfg(feature = "redis")]
    &sismember::COMMAND,
    #[cfg(feature = "redis")]
    &smismember::COMMAND,
    #[cfg(feature = "redis")]
    &scard::COMMAND,
    #[cfg(feature = "redis")]
    &smembers::COMMAND,
    #[cfg(feature = "redis")]
    &sunion::COMMAND,
    #[cfg(feature = "redis")]
    &sinter::COMMAND,
    #[cfg(feature = "redis")]
    &sdiff::COMMAND,
    #[cfg(feature = "redis")]
    &sunionstore::COMMAND,
    #[cfg(feature = "redis")]
    &sinterstore::COMMAND,
    #[cfg(feature = "redis")]
    &sintercard::COMMAND,
    #[cfg(feature = "redis")]
    &sdiffstore::COMMAND,
    #[cfg(feature = "redis")]
    &smove::COMMAND,
    #[cfg(feature = "redis")]
    &srem::COMMAND,
    #[cfg(feature = "redis")]
    &sscan::COMMAND,
    #[cfg(feature = "redis")]
    &srandmember::COMMAND,
    #[cfg(feature = "redis")]
    &spop::COMMAND,
    #[cfg(feature = "redis")]
    &zadd::COMMAND,
    #[cfg(feature = "redis")]
    &zscore::COMMAND,
    #[cfg(feature = "redis")]
    &zmscore::COMMAND,
    #[cfg(feature = "redis")]
    &zcard::COMMAND,
    #[cfg(feature = "redis")]
    &zunion::COMMAND,
    #[cfg(feature = "redis")]
    &zinter::COMMAND,
    #[cfg(feature = "redis")]
    &zdiff::COMMAND,
    #[cfg(feature = "redis")]
    &zintercard::COMMAND,
    #[cfg(feature = "redis")]
    &zrandmember::COMMAND,
    #[cfg(feature = "redis")]
    &zrange::COMMAND,
    #[cfg(feature = "redis")]
    &zincrby::COMMAND,
    #[cfg(feature = "redis")]
    &zcount::COMMAND,
    #[cfg(feature = "redis")]
    &zrank::COMMAND,
    #[cfg(feature = "redis")]
    &zrevrank::COMMAND,
    #[cfg(feature = "redis")]
    &zpopmin::COMMAND,
    #[cfg(feature = "redis")]
    &zpopmax::COMMAND,
    #[cfg(feature = "redis")]
    &zmpop::COMMAND,
    #[cfg(feature = "redis")]
    &zrem::COMMAND,
    #[cfg(feature = "redis")]
    &zremrangebyrank::COMMAND,
    #[cfg(feature = "redis")]
    &zremrangebyscore::COMMAND,
    #[cfg(feature = "redis")]
    &zremrangebylex::COMMAND,
    #[cfg(feature = "redis")]
    &zrangebyscore::COMMAND,
    #[cfg(feature = "redis")]
    &zrevrange::COMMAND,
    #[cfg(feature = "redis")]
    &zrevrangebyscore::COMMAND,
    #[cfg(feature = "redis")]
    &zscan::COMMAND,
    #[cfg(feature = "redis")]
    &zrangebylex::COMMAND,
    #[cfg(feature = "redis")]
    &zrevrangebylex::COMMAND,
    #[cfg(feature = "redis")]
    &zlexcount::COMMAND,
    #[cfg(feature = "redis")]
    &zrangestore::COMMAND,
    #[cfg(feature = "redis")]
    &zunionstore::COMMAND,
    #[cfg(feature = "redis")]
    &zinterstore::COMMAND,
    #[cfg(feature = "redis")]
    &zdiffstore::COMMAND,
    #[cfg(feature = "redis")]
    &bzpopmin::COMMAND,
    #[cfg(feature = "redis")]
    &bzpopmax::COMMAND,
    #[cfg(feature = "redis")]
    &bzmpop::COMMAND,
];

pub(crate) struct CommandCatalog;

impl CommandCatalog {
    pub(crate) fn find(name: &[u8]) -> Option<&'static dyn CommandDefinition> {
        CATALOG
            .iter()
            .copied()
            .find(|command| command.matches(name))
            .or({
                #[cfg(feature = "redis-modules")]
                {
                    redis_modules::find_command_definition(name)
                }
                #[cfg(not(feature = "redis-modules"))]
                {
                    None
                }
            })
    }

    pub(crate) fn parse_owned(parts: &[Vec<u8>]) -> Result<crate::storage::Command> {
        let command = Self::find_required(parts.first().map(Vec::as_slice))?;
        command.parse_owned(parts)
    }

    pub(crate) fn parse_borrowed<'a>(parts: &[&'a [u8]]) -> Result<BorrowedCommandBox<'a>> {
        let command = Self::find_required(parts.first().copied())?;
        command.parse_borrowed(parts)
    }

    fn find_required(name: Option<&[u8]>) -> Result<&'static dyn CommandDefinition> {
        match name {
            Some(name) => Self::find(name).ok_or_else(|| {
                ShardCacheError::Command(format!(
                    "unsupported command: {}",
                    String::from_utf8_lossy(name)
                ))
            }),
            None => Err(ShardCacheError::Command("empty command".into())),
        }
    }
}

pub(crate) struct EngineCommandCatalog;

impl EngineCommandCatalog {
    fn find_fast(command: &FastCommand<'_>) -> Option<&'static dyn EngineCommandDispatch> {
        [
            &get::COMMAND as &dyn EngineCommandDispatch,
            &set::COMMAND,
            &del::COMMAND,
            &exists::COMMAND,
            &ttl::COMMAND,
            &expire::COMMAND,
            &getex::COMMAND,
            &setex::COMMAND,
        ]
        .into_iter()
        .find(|candidate| candidate.matches_decoded_fast(command))
    }

    fn find_resp_span(name: &[u8]) -> Option<&'static dyn EngineRespSpanCommandDispatch> {
        [&set::COMMAND as &dyn EngineRespSpanCommandDispatch]
            .into_iter()
            .find(|candidate| candidate.matches(name))
    }

    pub(crate) async fn execute_fast<'a>(
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> Option<Result<FastResponse>> {
        let handler = Self::find_fast(&request.command)?;
        Some(handler.execute_engine_fast(ctx, request).await)
    }

    pub(crate) async fn execute_resp_spanned<'a>(
        ctx: EngineCommandContext<'a>,
        frame: CommandSpanFrame,
        owner: SharedBytes,
        out: &'a mut Vec<u8>,
    ) -> Option<Result<()>> {
        let name_span = frame.parts.first()?;
        let name = &owner[name_span.start..name_span.end];
        let handler = Self::find_resp_span(name)?;
        Some(
            handler
                .execute_engine_resp_spanned(ctx, frame, owner, out)
                .await,
        )
    }
}
