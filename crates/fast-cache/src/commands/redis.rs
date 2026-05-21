//! Shared command adapter and RESP-compatible Redis/Valkey command logic.

use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use bytes::BytesMut;
use smallvec::SmallVec;

use crate::commands::CommandSpec;
use crate::protocol::{FastCommand, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{
    BorrowedCommandContext, DirectCommandContext, FastCommandContext, FastDirectCommand,
    RawCommandContext, RawDirectCommand,
};
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{
    Command, DEFAULT_SCAN_COUNT, EmbeddedStore, EngineCommandContext, EngineFrameFuture,
    RedisKeyScanType, RedisObjectError, RedisObjectReadOutcome, RedisObjectResult,
    RedisObjectValue, RedisObjectZSetRangeItem,
};
use crate::{FastCacheError, Result};

pub(crate) type BorrowedArgs<'a> = SmallVec<[&'a [u8]; 16]>;

#[derive(Clone)]
pub(crate) struct OwnedRedisCommand<C>
where
    C: RedisCompatCommand,
{
    args: Vec<Vec<u8>>,
    _marker: PhantomData<C>,
}

impl<C> std::fmt::Debug for OwnedRedisCommand<C>
where
    C: RedisCompatCommand,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(C::NAME).field("args", &self.args).finish()
    }
}

impl<C> OwnedRedisCommand<C>
where
    C: RedisCompatCommand,
{
    fn new(args: Vec<Vec<u8>>) -> Self {
        Self {
            args,
            _marker: PhantomData,
        }
    }
}

#[derive(Clone)]
pub(crate) struct BorrowedRedisCommand<'a, C>
where
    C: RedisCompatCommand,
{
    args: BorrowedArgs<'a>,
    _marker: PhantomData<C>,
}

impl<C> std::fmt::Debug for BorrowedRedisCommand<'_, C>
where
    C: RedisCompatCommand,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(C::NAME).field("args", &self.args).finish()
    }
}

impl<'a, C> BorrowedRedisCommand<'a, C>
where
    C: RedisCompatCommand,
{
    fn new(args: BorrowedArgs<'a>) -> Self {
        Self {
            args,
            _marker: PhantomData,
        }
    }
}

pub(crate) trait RedisCompatCommand: CommandSpec + Send + Sync + 'static {
    #[inline(always)]
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        dispatch(Self::NAME, store, args)
    }

    #[cfg(feature = "server")]
    #[inline(always)]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_frame(out, &Self::execute(store, args));
    }

    #[inline(always)]
    fn matches_fast(_command: &FastCommand<'_>) -> bool {
        false
    }

    #[cfg(feature = "server")]
    #[inline(always)]
    fn execute_fast(_store: &EmbeddedStore, _command: FastCommand<'_>) -> Option<Frame> {
        None
    }
}

impl<C> crate::commands::OwnedCommandData for OwnedRedisCommand<C>
where
    C: RedisCompatCommand,
{
    type Spec = C;

    fn route_key(&self) -> Option<&[u8]> {
        route_key_for_name(C::NAME, self.args.first().map(Vec::as_slice))
    }

    fn to_borrowed_command(&self) -> crate::commands::BorrowedCommandBox<'_> {
        let args = self
            .args
            .iter()
            .map(Vec::as_slice)
            .collect::<SmallVec<[_; 16]>>();
        Box::new(BorrowedRedisCommand::<C>::new(args))
    }
}

impl<'a, C> crate::commands::BorrowedCommandData<'a> for BorrowedRedisCommand<'a, C>
where
    C: RedisCompatCommand,
{
    type Spec = C;

    fn route_key(&self) -> Option<&'a [u8]> {
        route_key_for_name(C::NAME, self.args.first().copied())
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedRedisCommand::<C>::new(
            self.args.iter().map(|arg| arg.to_vec()).collect(),
        )))
    }

    fn execute_engine<'b>(&'b self, _ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(async move {
            Err(FastCacheError::Command(format!(
                "{} requires embedded Redis compatibility storage",
                C::NAME
            )))
        })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &EmbeddedStore, _now_ms: u64) -> Frame {
        C::execute(store, &self.args)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        C::write_resp(ctx.store, &self.args, ctx.out);
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, _ctx: DirectCommandContext) -> Frame {
        Frame::Error(format!(
            "ERR {} requires embedded Redis compatibility storage",
            C::NAME
        ))
    }
}

impl<C> crate::commands::OwnedCommandParse for C
where
    C: RedisCompatCommand,
{
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        Ok(Command::new(Box::new(OwnedRedisCommand::<C>::new(
            parts.iter().skip(1).cloned().collect(),
        ))))
    }
}

impl<'a, C> crate::commands::BorrowedCommandParse<'a> for C
where
    C: RedisCompatCommand,
{
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<crate::commands::BorrowedCommandBox<'a>> {
        Ok(Box::new(BorrowedRedisCommand::<C>::new(
            parts.iter().skip(1).copied().collect(),
        )))
    }
}

impl<C> crate::commands::DecodedFastCommand for C
where
    C: RedisCompatCommand,
{
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        C::matches_fast(command)
    }
}

#[cfg(feature = "server")]
impl<C> RawDirectCommand for C
where
    C: RedisCompatCommand,
{
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_>) {
        C::write_resp(ctx.store, &ctx.args, ctx.out);
    }
}

#[cfg(feature = "server")]
impl<C> FastDirectCommand for C
where
    C: RedisCompatCommand,
{
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match C::execute_fast(ctx.store, command) {
            Some(frame) => write_fast_frame(ctx.out, &frame),
            None => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}

macro_rules! define_compat_command {
    ($module:ident, $type:ident, $name:literal, $mutates:expr) => {
        pub(crate) mod $module {
            #[derive(Debug, Clone, Copy)]
            pub(crate) struct $type;

            pub(crate) static COMMAND: $type = $type;

            impl crate::commands::CommandSpec for $type {
                const NAME: &'static str = $name;
                const MUTATES_VALUE: bool = $mutates;
            }

            impl crate::commands::redis_compat::RedisCompatCommand for $type {}
        }
    };
}

define_compat_command!(ping, Ping, "PING", false);
define_compat_command!(auth, Auth, "AUTH", false);
define_compat_command!(hello, Hello, "HELLO", false);
define_compat_command!(select, Select, "SELECT", false);
define_compat_command!(quit, Quit, "QUIT", false);
define_compat_command!(echo, Echo, "ECHO", false);
define_compat_command!(command, CommandInfo, "COMMAND", false);
define_compat_command!(config, Config, "CONFIG", false);
define_compat_command!(client, Client, "CLIENT", false);
define_compat_command!(dbsize, DbSize, "DBSIZE", false);
define_compat_command!(time, Time, "TIME", false);
define_compat_command!(info, Info, "INFO", false);
define_compat_command!(keys, Keys, "KEYS", false);
pub(crate) mod scan {
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct Scan;

    pub(crate) static COMMAND: Scan = Scan;

    impl crate::commands::CommandSpec for Scan {
        const NAME: &'static str = "SCAN";
        const MUTATES_VALUE: bool = false;
    }

    impl crate::commands::redis_compat::RedisCompatCommand for Scan {
        #[cfg(feature = "server")]
        #[inline(always)]
        fn write_resp(
            store: &crate::storage::EmbeddedStore,
            args: &[&[u8]],
            out: &mut bytes::BytesMut,
        ) {
            super::write_scan_resp(store, args, out);
        }
    }
}
define_compat_command!(redis_type, Type, "TYPE", false);
define_compat_command!(append, Append, "APPEND", true);
define_compat_command!(strlen, StrLen, "STRLEN", false);
define_compat_command!(getrange, GetRange, "GETRANGE", false);
define_compat_command!(setrange, SetRange, "SETRANGE", true);
define_compat_command!(getset, GetSet, "GETSET", true);
define_compat_command!(getdel, GetDel, "GETDEL", true);
define_compat_command!(incr, Incr, "INCR", true);
define_compat_command!(incrby, IncrBy, "INCRBY", true);
define_compat_command!(decr, Decr, "DECR", true);
define_compat_command!(decrby, DecrBy, "DECRBY", true);
define_compat_command!(incrbyfloat, IncrByFloat, "INCRBYFLOAT", true);
define_compat_command!(mset, MSet, "MSET", true);
define_compat_command!(mget, MGet, "MGET", false);
define_compat_command!(msetnx, MSetNx, "MSETNX", true);
define_compat_command!(hset, HSet, "HSET", true);
define_compat_command!(hget, HGet, "HGET", false);
define_compat_command!(hmget, HMGet, "HMGET", false);
define_compat_command!(hlen, HLen, "HLEN", false);
define_compat_command!(hexists, HExists, "HEXISTS", false);
define_compat_command!(hsetnx, HSetNx, "HSETNX", true);
define_compat_command!(hincrby, HIncrBy, "HINCRBY", true);
define_compat_command!(hincrbyfloat, HIncrByFloat, "HINCRBYFLOAT", true);
define_compat_command!(hkeys, HKeys, "HKEYS", false);
define_compat_command!(hvals, HVals, "HVALS", false);
define_compat_command!(hgetall, HGetAll, "HGETALL", false);
define_compat_command!(hscan, HScan, "HSCAN", false);
define_compat_command!(hrandfield, HRandField, "HRANDFIELD", false);
define_compat_command!(hdel, HDel, "HDEL", true);
define_compat_command!(lpush, LPush, "LPUSH", true);
define_compat_command!(rpush, RPush, "RPUSH", true);
define_compat_command!(lrange, LRange, "LRANGE", false);
define_compat_command!(llen, LLen, "LLEN", false);
define_compat_command!(lindex, LIndex, "LINDEX", false);
define_compat_command!(lset, LSet, "LSET", true);
define_compat_command!(lrem, LRem, "LREM", true);
define_compat_command!(linsert, LInsert, "LINSERT", true);
define_compat_command!(ltrim, LTrim, "LTRIM", true);
define_compat_command!(lpop, LPop, "LPOP", true);
define_compat_command!(rpop, RPop, "RPOP", true);
define_compat_command!(lpushx, LPushX, "LPUSHX", true);
define_compat_command!(rpushx, RPushX, "RPUSHX", true);
define_compat_command!(blpop, BLPop, "BLPOP", true);
define_compat_command!(brpop, BRPop, "BRPOP", true);
define_compat_command!(blmove, BLMove, "BLMOVE", true);
define_compat_command!(sadd, SAdd, "SADD", true);
define_compat_command!(sismember, SIsMember, "SISMEMBER", false);
define_compat_command!(smismember, SMIsMember, "SMISMEMBER", false);
define_compat_command!(scard, SCard, "SCARD", false);
define_compat_command!(smembers, SMembers, "SMEMBERS", false);
define_compat_command!(sunion, SUnion, "SUNION", false);
define_compat_command!(sinter, SInter, "SINTER", false);
define_compat_command!(sdiff, SDiff, "SDIFF", false);
define_compat_command!(sunionstore, SUnionStore, "SUNIONSTORE", true);
define_compat_command!(sinterstore, SInterStore, "SINTERSTORE", true);
define_compat_command!(sdiffstore, SDiffStore, "SDIFFSTORE", true);
define_compat_command!(smove, SMove, "SMOVE", true);
define_compat_command!(srem, SRem, "SREM", true);
define_compat_command!(sscan, SScan, "SSCAN", false);
define_compat_command!(srandmember, SRandMember, "SRANDMEMBER", false);
define_compat_command!(spop, SPop, "SPOP", true);
define_compat_command!(zadd, ZAdd, "ZADD", true);
define_compat_command!(zscore, ZScore, "ZSCORE", false);
define_compat_command!(zmscore, ZMScore, "ZMSCORE", false);
define_compat_command!(zcard, ZCard, "ZCARD", false);
pub(crate) mod zrange {
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct ZRange;

    pub(crate) static COMMAND: ZRange = ZRange;

    impl crate::commands::CommandSpec for ZRange {
        const NAME: &'static str = "ZRANGE";
        const MUTATES_VALUE: bool = false;
    }

    impl crate::commands::redis_compat::RedisCompatCommand for ZRange {
        #[cfg(feature = "server")]
        #[inline(always)]
        fn write_resp(
            store: &crate::storage::EmbeddedStore,
            args: &[&[u8]],
            out: &mut bytes::BytesMut,
        ) {
            super::write_zrange_resp(store, args, out);
        }
    }
}
define_compat_command!(zincrby, ZIncrBy, "ZINCRBY", true);
define_compat_command!(zcount, ZCount, "ZCOUNT", false);
define_compat_command!(zrank, ZRank, "ZRANK", false);
define_compat_command!(zrevrank, ZRevRank, "ZREVRANK", false);
define_compat_command!(zpopmin, ZPopMin, "ZPOPMIN", true);
define_compat_command!(zpopmax, ZPopMax, "ZPOPMAX", true);
define_compat_command!(zrem, ZRem, "ZREM", true);
define_compat_command!(zrangebyscore, ZRangeByScore, "ZRANGEBYSCORE", false);
define_compat_command!(zscan, ZScan, "ZSCAN", false);
define_compat_command!(zrangebylex, ZRangeByLex, "ZRANGEBYLEX", false);
define_compat_command!(zrevrangebylex, ZRevRangeByLex, "ZREVRANGEBYLEX", false);
define_compat_command!(zlexcount, ZLexCount, "ZLEXCOUNT", false);
define_compat_command!(zrangestore, ZRangeStore, "ZRANGESTORE", true);
define_compat_command!(zunionstore, ZUnionStore, "ZUNIONSTORE", true);
define_compat_command!(zinterstore, ZInterStore, "ZINTERSTORE", true);
define_compat_command!(zdiffstore, ZDiffStore, "ZDIFFSTORE", true);
define_compat_command!(bzpopmin, BZPopMin, "BZPOPMIN", true);
define_compat_command!(bzpopmax, BZPopMax, "BZPOPMAX", true);

fn dispatch(name: &str, store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match name {
        "PING" => ping(args),
        "AUTH" | "SELECT" | "CLIENT" | "CONFIG" | "COMMAND" => metadata_command(name, args),
        "HELLO" => hello(args),
        "QUIT" => simple("OK"),
        "ECHO" => match args {
            [payload] => bulk((*payload).to_vec()),
            _ => wrong_arity(name),
        },
        "DBSIZE" => match args {
            [] => int(store.len() as i64),
            _ => wrong_arity(name),
        },
        "TIME" => time(args),
        "INFO" => info(store, args),
        "TYPE" => match args {
            [key] => simple(store.redis_type(key)),
            _ => wrong_arity(name),
        },
        "KEYS" => keys(store, args),
        "SCAN" => scan(store, args),
        "APPEND" => append(store, args),
        "STRLEN" => strlen(store, args),
        "GETRANGE" => getrange(store, args),
        "SETRANGE" => setrange(store, args),
        "GETSET" => getset(store, args),
        "GETDEL" => getdel(store, args),
        "INCR" => incrby(store, args, 1),
        "INCRBY" => incrby_arg(store, args),
        "DECR" => incrby(store, args, -1),
        "DECRBY" => decrby_arg(store, args),
        "INCRBYFLOAT" => incrbyfloat(store, args),
        "MSET" => mset(store, args),
        "MGET" => mget(store, args),
        "MSETNX" => msetnx(store, args),
        "HSET" => hset(store, args),
        "HGET" => object_result(name, args, 2, || store.hget(args[0], args[1])),
        "HMGET" => hmget(store, args),
        "HLEN" => object_result(name, args, 1, || store.hlen(args[0])),
        "HEXISTS" => object_result(name, args, 2, || store.hexists(args[0], args[1])),
        "HSETNX" => object_result(name, args, 3, || store.hsetnx(args[0], args[1], args[2])),
        "HINCRBY" => hincrby(store, args),
        "HINCRBYFLOAT" => hincrbyfloat(store, args),
        "HKEYS" => object_result(name, args, 1, || store.hkeys(args[0])),
        "HVALS" => object_result(name, args, 1, || store.hvals(args[0])),
        "HGETALL" => object_result(name, args, 1, || store.hgetall(args[0])),
        "HSCAN" => hscan(store, args),
        "HRANDFIELD" => hrandfield(store, args),
        "HDEL" => hdel(store, args),
        "LPUSH" => push_list(store, args, true, false),
        "RPUSH" => push_list(store, args, false, false),
        "LPUSHX" => push_list(store, args, true, true),
        "RPUSHX" => push_list(store, args, false, true),
        "LRANGE" => list_range(store, args),
        "LLEN" => object_result(name, args, 1, || store.llen(args[0])),
        "LINDEX" => lindex(store, args),
        "LSET" => lset(store, args),
        "LREM" => lrem(store, args),
        "LINSERT" => linsert(store, args),
        "LTRIM" => ltrim(store, args),
        "LPOP" => pop_list(store, args, true),
        "RPOP" => pop_list(store, args, false),
        "BLPOP" => blocking_pop(store, args, true),
        "BRPOP" => blocking_pop(store, args, false),
        "BLMOVE" => blmove(store, args),
        "SADD" => sadd(store, args),
        "SISMEMBER" => object_result(name, args, 2, || store.sismember(args[0], args[1])),
        "SMISMEMBER" => smismember(store, args),
        "SCARD" => object_result(name, args, 1, || store.scard(args[0])),
        "SMEMBERS" => object_result(name, args, 1, || store.smembers(args[0])),
        "SUNION" => set_op(store, args, SetOp::Union, None),
        "SINTER" => set_op(store, args, SetOp::Inter, None),
        "SDIFF" => set_op(store, args, SetOp::Diff, None),
        "SUNIONSTORE" => set_store(store, args, SetOp::Union),
        "SINTERSTORE" => set_store(store, args, SetOp::Inter),
        "SDIFFSTORE" => set_store(store, args, SetOp::Diff),
        "SMOVE" => smove(store, args),
        "SREM" => srem(store, args),
        "SSCAN" => sscan(store, args),
        "SRANDMEMBER" => srandmember(store, args),
        "SPOP" => spop(store, args),
        "ZADD" => zadd(store, args),
        "ZSCORE" => object_result(name, args, 2, || store.zscore(args[0], args[1])),
        "ZMSCORE" => zmscore(store, args),
        "ZCARD" => object_result(name, args, 1, || store.zcard(args[0])),
        "ZRANGE" => zrange(store, args),
        "ZRANGEBYSCORE" => zrangebyscore(store, args),
        "ZRANGEBYLEX" => zrangebylex(store, args, false),
        "ZREVRANGEBYLEX" => zrangebylex(store, args, true),
        "ZLEXCOUNT" => zlexcount(store, args),
        "ZINCRBY" => zincrby(store, args),
        "ZCOUNT" => zcount(store, args),
        "ZRANK" => zrank(store, args, false),
        "ZREVRANK" => zrank(store, args, true),
        "ZPOPMIN" => zpop(store, args, false),
        "ZPOPMAX" => zpop(store, args, true),
        "ZREM" => zrem(store, args),
        "ZSCAN" => zscan(store, args),
        "ZRANGESTORE" => zrangestore(store, args),
        "ZUNIONSTORE" => zaggregate_store(store, args, ZAggregateKind::Union),
        "ZINTERSTORE" => zaggregate_store(store, args, ZAggregateKind::Inter),
        "ZDIFFSTORE" => zaggregate_store(store, args, ZAggregateKind::Diff),
        "BZPOPMIN" => bzpop(store, args, false),
        "BZPOPMAX" => bzpop(store, args, true),
        _ => error("ERR unsupported command"),
    }
}

fn route_key_for_name<'a>(name: &str, first: Option<&'a [u8]>) -> Option<&'a [u8]> {
    match name {
        "PING" | "AUTH" | "HELLO" | "SELECT" | "QUIT" | "ECHO" | "COMMAND" | "CONFIG"
        | "CLIENT" | "DBSIZE" | "TIME" | "INFO" | "SCAN" => None,
        _ => first,
    }
}

fn ping(args: &[&[u8]]) -> Frame {
    match args {
        [] => simple("PONG"),
        [payload] => bulk((*payload).to_vec()),
        _ => wrong_arity("PING"),
    }
}

fn metadata_command(name: &str, args: &[&[u8]]) -> Frame {
    match name {
        "AUTH" => simple("OK"),
        "SELECT" => match args {
            [b"0"] => simple("OK"),
            [_] => error("ERR DB index is out of range"),
            _ => wrong_arity(name),
        },
        "COMMAND" => Frame::Array(Vec::new()),
        "CONFIG" => match args {
            [sub, ..] if eq_ignore_ascii_case(sub, b"GET") => Frame::Array(Vec::new()),
            _ => simple("OK"),
        },
        "CLIENT" => match args {
            [sub] if eq_ignore_ascii_case(sub, b"GETNAME") => Frame::Null,
            [sub, _] if eq_ignore_ascii_case(sub, b"SETNAME") => simple("OK"),
            [sub] if eq_ignore_ascii_case(sub, b"ID") => int(0),
            [sub] if eq_ignore_ascii_case(sub, b"LIST") => bulk(Vec::new()),
            [sub, ..] if eq_ignore_ascii_case(sub, b"KILL") => int(0),
            _ => simple("OK"),
        },
        _ => error("ERR unsupported command"),
    }
}

fn hello(args: &[&[u8]]) -> Frame {
    if args.len() > 1 {
        return wrong_arity("HELLO");
    }
    Frame::Array(vec![
        bulk(b"server".to_vec()),
        bulk(b"fast-cache".to_vec()),
        bulk(b"version".to_vec()),
        bulk(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
        bulk(b"proto".to_vec()),
        int(2),
        bulk(b"id".to_vec()),
        int(0),
        bulk(b"mode".to_vec()),
        bulk(b"standalone".to_vec()),
        bulk(b"role".to_vec()),
        bulk(b"master".to_vec()),
        bulk(b"modules".to_vec()),
        Frame::Array(Vec::new()),
    ])
}

fn time(args: &[&[u8]]) -> Frame {
    if !args.is_empty() {
        return wrong_arity("TIME");
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Frame::Array(vec![
        bulk(now.as_secs().to_string().into_bytes()),
        bulk(now.subsec_micros().to_string().into_bytes()),
    ])
}

fn info(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() > 1 {
        return wrong_arity("INFO");
    }
    bulk(
        format!(
            "# Server\r\nredis_version:{}\r\nfast_cache_version:{}\r\n# Keyspace\r\ndb0:keys={},expires=0,avg_ttl=0\r\n",
            env!("CARGO_PKG_VERSION"),
            env!("CARGO_PKG_VERSION"),
            store.len()
        )
        .into_bytes(),
    )
}

fn keys(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [pattern] => array_bulk(filter_key_pattern(store.key_snapshot_unsorted(), pattern)),
        _ => wrong_arity("KEYS"),
    }
}

fn scan(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match parse_key_scan_args(args, "SCAN") {
        Ok(options) => {
            let mut keys = Vec::with_capacity(options.count.min(1024));
            let result = store.scan_redis_keys_visit(
                options.cursor,
                options.count,
                options.scan_type(),
                &mut |key| {
                    if key_pattern_matches(key, options.pattern) {
                        keys.push(key.to_vec());
                        true
                    } else {
                        false
                    }
                },
            );
            scan_array_with_cursor(result.cursor, keys)
        }
        Err(frame) => frame,
    }
}

#[cfg(feature = "server")]
fn write_scan_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
    match parse_key_scan_args(args, "SCAN") {
        Ok(options) => write_scan_resp_from_options(store, options, out),
        Err(frame) => write_frame(out, &frame),
    }
}

#[cfg(feature = "server")]
fn write_scan_shard_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
    match parse_fcnp_scan_shard_args(args) {
        Ok((shard_id, options)) => {
            let mut items =
                BytesMut::with_capacity(options.count.saturating_mul(32).min(64 * 1024));
            let result = store.scan_redis_keys_in_shard_visit(
                shard_id,
                options.cursor,
                options.count,
                options.scan_type(),
                &mut |key| {
                    if key_pattern_matches(key, options.pattern) {
                        ServerWire::write_resp_blob_string(&mut items, key);
                        true
                    } else {
                        false
                    }
                },
            );
            match result {
                Some(result) => write_scan_resp_payload(out, result.cursor, result.keys, &items),
                None => write_frame(out, &error("ERR invalid shard")),
            }
        }
        Err(frame) => write_frame(out, &frame),
    }
}

#[cfg(feature = "server")]
fn write_scan_resp_from_options(
    store: &EmbeddedStore,
    options: KeyScanOptions<'_>,
    out: &mut BytesMut,
) {
    let mut items = BytesMut::with_capacity(options.count.saturating_mul(32).min(64 * 1024));
    let result = store.scan_redis_keys_visit(
        options.cursor,
        options.count,
        options.scan_type(),
        &mut |key| {
            if key_pattern_matches(key, options.pattern) {
                ServerWire::write_resp_blob_string(&mut items, key);
                true
            } else {
                false
            }
        },
    );
    write_scan_resp_payload(out, result.cursor, result.keys, &items);
}

#[cfg(feature = "server")]
fn write_scan_resp_payload(out: &mut BytesMut, cursor: u64, item_count: usize, items: &[u8]) {
    write_resp_array_header(out, 2);
    let mut cursor_buffer = itoa::Buffer::new();
    ServerWire::write_resp_blob_string(out, cursor_buffer.format(cursor).as_bytes());
    write_resp_array_header(out, item_count);
    out.extend_from_slice(items);
}

#[cfg(feature = "server")]
pub(crate) fn write_fcnp_scan_fast_response(
    store: &EmbeddedStore,
    args: &[&[u8]],
    out: &mut BytesMut,
) {
    let start = ServerWire::begin_fast_value(out);
    write_scan_resp(store, args, out);
    ServerWire::finish_fast_value(out, start);
}

#[cfg(feature = "server")]
pub(crate) fn write_fcnp_scan_shard_fast_response(
    store: &EmbeddedStore,
    args: &[&[u8]],
    out: &mut BytesMut,
) {
    let start = ServerWire::begin_fast_value(out);
    write_scan_shard_resp(store, args, out);
    ServerWire::finish_fast_value(out, start);
}

struct KeyScanOptions<'a> {
    cursor: u64,
    count: usize,
    pattern: &'a [u8],
    type_filter: Option<&'a [u8]>,
}

impl<'a> KeyScanOptions<'a> {
    fn scan_type(&self) -> RedisKeyScanType<'a> {
        match self.type_filter {
            None => RedisKeyScanType::All,
            Some(kind) if kind.eq_ignore_ascii_case(b"string") => RedisKeyScanType::String,
            Some(kind) => RedisKeyScanType::Object(kind),
        }
    }
}

fn parse_key_scan_args<'a>(
    args: &'a [&'a [u8]],
    command_name: &str,
) -> std::result::Result<KeyScanOptions<'a>, Frame> {
    let Some(cursor) = args.first() else {
        return Err(wrong_arity(command_name));
    };
    let cursor = parse_u64(cursor).map_err(|_| error("ERR invalid cursor"))?;
    parse_key_scan_options(cursor, &args[1..])
}

fn parse_fcnp_scan_shard_args<'a>(
    args: &'a [&'a [u8]],
) -> std::result::Result<(usize, KeyScanOptions<'a>), Frame> {
    match args {
        [shard_id, cursor, rest @ ..] => {
            let shard_id = parse_usize(shard_id).map_err(|_| error("ERR invalid shard"))?;
            let cursor = parse_u64(cursor).map_err(|_| error("ERR invalid cursor"))?;
            parse_key_scan_options(cursor, rest).map(|options| (shard_id, options))
        }
        _ => Err(wrong_arity("FCNP.SCANSHARD")),
    }
}

fn parse_key_scan_options<'a>(
    cursor: u64,
    args: &'a [&'a [u8]],
) -> std::result::Result<KeyScanOptions<'a>, Frame> {
    let mut pattern: &[u8] = b"*";
    let mut type_filter = None;
    let mut count = DEFAULT_SCAN_COUNT;
    let mut index = 0;
    while index < args.len() {
        match upper(args[index]).as_slice() {
            b"MATCH" if index + 1 < args.len() => {
                pattern = args[index + 1];
                index += 2;
            }
            b"COUNT" if index + 1 < args.len() => {
                count = parse_usize(args[index + 1])
                    .map_err(|_| error("ERR value is not an integer or out of range"))?;
                index += 2;
            }
            b"TYPE" if index + 1 < args.len() => {
                type_filter = Some(args[index + 1]);
                index += 2;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok(KeyScanOptions {
        cursor,
        count,
        pattern,
        type_filter,
    })
}

fn filter_key_pattern(keys: Vec<Vec<u8>>, pattern: &[u8]) -> Vec<Vec<u8>> {
    match pattern {
        b"*" => keys,
        pattern => keys
            .into_iter()
            .filter(|key| glob_matches(pattern, key))
            .collect(),
    }
}

fn key_pattern_matches(key: &[u8], pattern: &[u8]) -> bool {
    pattern == b"*" || glob_matches(pattern, key)
}

fn append(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, suffix] => match string_value(store, key) {
            Ok(mut value) => {
                value.extend_from_slice(suffix);
                let len = value.len() as i64;
                store.set((*key).to_vec(), value, None);
                int(len)
            }
            Err(frame) => frame,
        },
        _ => wrong_arity("APPEND"),
    }
}

fn strlen(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key] => match string_value(store, key) {
            Ok(value) => int(value.len() as i64),
            Err(frame) => frame,
        },
        _ => wrong_arity("STRLEN"),
    }
}

fn getrange(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, start, stop] => {
            let Ok(start) = parse_i64(start) else {
                return error("ERR value is not an integer or out of range");
            };
            let Ok(stop) = parse_i64(stop) else {
                return error("ERR value is not an integer or out of range");
            };
            match string_value(store, key) {
                Ok(value) => match normalize_range(value.len(), start, stop) {
                    Some((start, stop)) => bulk(value[start..=stop].to_vec()),
                    None => bulk(Vec::new()),
                },
                Err(frame) => frame,
            }
        }
        _ => wrong_arity("GETRANGE"),
    }
}

fn setrange(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, offset, replacement] => {
            let Ok(offset) = parse_usize(offset) else {
                return error("ERR offset is not an integer or out of range");
            };
            match string_value(store, key) {
                Ok(mut value) => {
                    let required = offset.saturating_add(replacement.len());
                    if value.len() < required {
                        value.resize(required, 0);
                    }
                    value[offset..offset + replacement.len()].copy_from_slice(replacement);
                    let len = value.len() as i64;
                    store.set((*key).to_vec(), value, None);
                    int(len)
                }
                Err(frame) => frame,
            }
        }
        _ => wrong_arity("SETRANGE"),
    }
}

fn getset(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, value] => match optional_string_value(store, key, true) {
            Ok(old) => {
                store.set((*key).to_vec(), (*value).to_vec(), None);
                old.map_or(Frame::Null, bulk)
            }
            Err(frame) => frame,
        },
        _ => wrong_arity("GETSET"),
    }
}

fn getdel(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key] => match optional_string_value(store, key, true) {
            Ok(old) => {
                if old.is_some() {
                    store.delete(key);
                }
                old.map_or(Frame::Null, bulk)
            }
            Err(frame) => frame,
        },
        _ => wrong_arity("GETDEL"),
    }
}

fn incrby_arg(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, delta] => match parse_i64(delta) {
            Ok(delta) => incrby(store, &[*key], delta),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("INCRBY"),
    }
}

fn decrby_arg(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, delta] => match parse_i64(delta) {
            Ok(delta) => incrby(store, &[*key], delta.saturating_neg()),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("DECRBY"),
    }
}

fn incrby(store: &EmbeddedStore, args: &[&[u8]], delta: i64) -> Frame {
    match args {
        [key] => match string_value(store, key) {
            Ok(value) => {
                let current = if value.is_empty() {
                    0
                } else {
                    match parse_i64(&value) {
                        Ok(value) => value,
                        Err(_) => return error("ERR value is not an integer or out of range"),
                    }
                };
                let Some(next) = current.checked_add(delta) else {
                    return error("ERR increment or decrement would overflow");
                };
                store.set((*key).to_vec(), next.to_string().into_bytes(), None);
                int(next)
            }
            Err(frame) => frame,
        },
        _ => wrong_arity("INCR"),
    }
}

fn incrbyfloat(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, delta] => {
            let Ok(delta) = parse_f64(delta) else {
                return error("ERR value is not a valid float");
            };
            match string_value(store, key) {
                Ok(value) => {
                    let current = if value.is_empty() {
                        0.0
                    } else {
                        match parse_f64(&value) {
                            Ok(value) => value,
                            Err(_) => return error("ERR value is not a valid float"),
                        }
                    };
                    let next = current + delta;
                    if !next.is_finite() {
                        return error("ERR increment would produce NaN or Infinity");
                    }
                    let bytes = format_score(next);
                    store.set((*key).to_vec(), bytes.clone(), None);
                    bulk(bytes)
                }
                Err(frame) => frame,
            }
        }
        _ => wrong_arity("INCRBYFLOAT"),
    }
}

fn mset(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return wrong_arity("MSET");
    }
    for pair in args.chunks_exact(2) {
        store.set(pair[0].to_vec(), pair[1].to_vec(), None);
    }
    simple("OK")
}

fn mget(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.is_empty() {
        return wrong_arity("MGET");
    }
    Frame::Array(
        args.iter()
            .map(|key| match optional_string_value(store, key, false) {
                Ok(Some(value)) => bulk(value),
                _ => Frame::Null,
            })
            .collect(),
    )
}

fn msetnx(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return wrong_arity("MSETNX");
    }
    if args.chunks_exact(2).any(|pair| store.exists(pair[0])) {
        return int(0);
    }
    for pair in args.chunks_exact(2) {
        store.set(pair[0].to_vec(), pair[1].to_vec(), None);
    }
    int(1)
}

fn hset(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 3 || args[1..].len() % 2 != 0 {
        return wrong_arity("HSET");
    }
    frame_from_result(
        store.hset_many(
            args[0],
            &args[1..]
                .chunks_exact(2)
                .map(|pair| (pair[0], pair[1]))
                .collect::<Vec<_>>(),
        ),
    )
}

fn hmget(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("HMGET");
    }
    frame_from_result(store.hmget(args[0], &args[1..]))
}

fn hincrby(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, field, delta] => match parse_i64(delta) {
            Ok(delta) => frame_from_result(store.hincrby(key, field, delta)),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("HINCRBY"),
    }
}

fn hincrbyfloat(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, field, delta] => match parse_f64(delta) {
            Ok(delta) => frame_from_result(store.hincrbyfloat(key, field, delta)),
            Err(_) => error("ERR value is not a valid float"),
        },
        _ => wrong_arity("HINCRBYFLOAT"),
    }
}

fn hscan(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("HSCAN");
    }
    scan_from_result(store.hgetall(args[0]))
}

fn hrandfield(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.is_empty() || args.len() > 3 {
        return wrong_arity("HRANDFIELD");
    }
    let count = match args.get(1) {
        Some(value) => match parse_i64(value) {
            Ok(value) => Some(value),
            Err(_) => return error("ERR value is not an integer or out of range"),
        },
        None => None,
    };
    let with_values = args
        .get(2)
        .is_some_and(|value| eq_ignore_ascii_case(value, b"WITHVALUES"));
    frame_from_result(store.hrandfield(args[0], count, with_values))
}

fn hdel(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("HDEL");
    }
    frame_from_result(store.hdel_many(args[0], &args[1..]))
}

fn push_list(store: &EmbeddedStore, args: &[&[u8]], front: bool, existing: bool) -> Frame {
    if args.len() < 2 {
        return wrong_arity(if front { "LPUSH" } else { "RPUSH" });
    }
    let result = match (front, existing) {
        (true, false) => store.lpush(args[0], &args[1..]),
        (false, false) => store.rpush(args[0], &args[1..]),
        (true, true) => store.lpushx(args[0], &args[1..]),
        (false, true) => store.rpushx(args[0], &args[1..]),
    };
    frame_from_result(result)
}

fn list_range(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, start, stop] => match (parse_i64(start), parse_i64(stop)) {
            (Ok(start), Ok(stop)) => frame_from_result(store.lrange(key, start, stop)),
            _ => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("LRANGE"),
    }
}

fn lindex(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, index] => match parse_i64(index) {
            Ok(index) => frame_from_result(store.lindex(key, index)),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("LINDEX"),
    }
}

fn lset(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, index, value] => match parse_i64(index) {
            Ok(index) => frame_from_result(store.lset(key, index, value)),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("LSET"),
    }
}

fn lrem(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, count, value] => match parse_i64(count) {
            Ok(count) => frame_from_result(store.lrem(key, count, value)),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("LREM"),
    }
}

fn linsert(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, where_arg, pivot, value] => {
            let before = match upper(where_arg).as_slice() {
                b"BEFORE" => true,
                b"AFTER" => false,
                _ => return error("ERR syntax error"),
            };
            frame_from_result(store.linsert(key, before, pivot, value))
        }
        _ => wrong_arity("LINSERT"),
    }
}

fn ltrim(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, start, stop] => match (parse_i64(start), parse_i64(stop)) {
            (Ok(start), Ok(stop)) => frame_from_result(store.ltrim(key, start, stop)),
            _ => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("LTRIM"),
    }
}

fn pop_list(store: &EmbeddedStore, args: &[&[u8]], front: bool) -> Frame {
    match args {
        [key] => frame_from_result(if front {
            store.lpop(key)
        } else {
            store.rpop(key)
        }),
        [key, count] => match parse_usize(count) {
            Ok(count) => frame_from_result(if front {
                store.lpop_count(key, count)
            } else {
                store.rpop_count(key, count)
            }),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity(if front { "LPOP" } else { "RPOP" }),
    }
}

fn blocking_pop(store: &EmbeddedStore, args: &[&[u8]], front: bool) -> Frame {
    if args.len() < 2 {
        return wrong_arity(if front { "BLPOP" } else { "BRPOP" });
    }
    for key in &args[..args.len() - 1] {
        let popped = if front {
            store.lpop(key)
        } else {
            store.rpop(key)
        };
        match frame_from_result(popped) {
            Frame::BlobString(value) => {
                return Frame::Array(vec![bulk((*key).to_vec()), bulk(value)]);
            }
            Frame::Error(error) => return Frame::Error(error),
            _ => {}
        }
    }
    Frame::Null
}

fn blmove(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [source, dest, source_side, dest_side, _timeout] => {
            let from_front = match upper(source_side).as_slice() {
                b"LEFT" => true,
                b"RIGHT" => false,
                _ => return error("ERR syntax error"),
            };
            let to_front = match upper(dest_side).as_slice() {
                b"LEFT" => true,
                b"RIGHT" => false,
                _ => return error("ERR syntax error"),
            };
            let popped = if from_front {
                store.lpop(source)
            } else {
                store.rpop(source)
            };
            match frame_from_result(popped) {
                Frame::BlobString(value) => {
                    let values = [value.as_slice()];
                    let result = if to_front {
                        store.lpush(dest, &values)
                    } else {
                        store.rpush(dest, &values)
                    };
                    match result {
                        RedisObjectResult::WrongType => wrongtype(),
                        _ => bulk(value),
                    }
                }
                other => other,
            }
        }
        _ => wrong_arity("BLMOVE"),
    }
}

fn sadd(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("SADD");
    }
    frame_from_result(store.sadd(args[0], &args[1..]))
}

fn smismember(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("SMISMEMBER");
    }
    frame_from_result(store.smismember(args[0], &args[1..]))
}

fn set_op(store: &EmbeddedStore, args: &[&[u8]], op: SetOp, dest: Option<&[u8]>) -> Frame {
    if args.is_empty() {
        return wrong_arity(op.name(dest.is_some()));
    }
    let result = match compute_set_op(store, args, op) {
        Ok(values) => values,
        Err(frame) => return frame,
    };
    match dest {
        Some(dest) => {
            store.set_object_value(dest, RedisObjectValue::Set(result.clone()), None);
            int(result.len() as i64)
        }
        None => array_bulk(result),
    }
}

fn set_store(store: &EmbeddedStore, args: &[&[u8]], op: SetOp) -> Frame {
    if args.len() < 2 {
        return wrong_arity(op.name(true));
    }
    set_op(store, &args[1..], op, Some(args[0]))
}

fn smove(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [source, dest, member] => match store.set_members(source) {
            Ok(members) if members.iter().any(|item| item.as_slice() == *member) => {
                match store.sadd(dest, &[*member]) {
                    RedisObjectResult::WrongType => wrongtype(),
                    _ => {
                        let _ = store.srem(source, &[*member]);
                        int(1)
                    }
                }
            }
            Ok(_) => int(0),
            Err(RedisObjectError::WrongType) => wrongtype(),
            Err(RedisObjectError::MissingKey) => int(0),
        },
        _ => wrong_arity("SMOVE"),
    }
}

fn srem(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("SREM");
    }
    frame_from_result(store.srem(args[0], &args[1..]))
}

fn sscan(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("SSCAN");
    }
    scan_from_result(store.smembers(args[0]))
}

fn srandmember(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key] => frame_from_result(store.srandmember(key, None)),
        [key, count] => match parse_i64(count) {
            Ok(count) => frame_from_result(store.srandmember(key, Some(count))),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("SRANDMEMBER"),
    }
}

fn spop(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key] => frame_from_result(store.spop(key, None)),
        [key, count] => match parse_usize(count) {
            Ok(count) => frame_from_result(store.spop(key, Some(count))),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity("SPOP"),
    }
}

fn zadd(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 3 {
        return wrong_arity("ZADD");
    }
    let key = args[0];
    let mut index = 1;
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut incr = false;
    while index < args.len() {
        match upper(args[index]).as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            b"GT" => gt = true,
            b"LT" => lt = true,
            b"CH" => ch = true,
            b"INCR" => incr = true,
            _ => break,
        }
        index += 1;
    }
    if index >= args.len() || !(args.len() - index).is_multiple_of(2) {
        return error("ERR syntax error");
    }
    let mut total = 0_i64;
    let mut last_bulk = None;
    for pair in args[index..].chunks_exact(2) {
        let Ok(score) = parse_f64(pair[0]) else {
            return error("ERR value is not a valid float");
        };
        match store.zadd_cond(key, score, pair[1], nx, xx, gt, lt, ch, incr) {
            RedisObjectResult::Integer(value) => total += value,
            RedisObjectResult::Bulk(value) => last_bulk = value,
            RedisObjectResult::WrongType => return wrongtype(),
            RedisObjectResult::Simple(message) if message.starts_with("ERR ") => {
                return error(message);
            }
            _ => {}
        }
    }
    if incr {
        last_bulk.map_or(Frame::Null, bulk)
    } else {
        int(total)
    }
}

fn zmscore(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("ZMSCORE");
    }
    frame_from_result(store.zmscore(args[0], &args[1..]))
}

fn zrange(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 3 {
        return wrong_arity("ZRANGE");
    }
    let mut by_score = false;
    let mut rev = false;
    let mut with_scores = false;
    let mut limit: Option<(usize, usize)> = None;
    let mut index = 3;
    while index < args.len() {
        match upper(args[index]).as_slice() {
            b"BYSCORE" => {
                by_score = true;
                index += 1;
            }
            b"REV" => {
                rev = true;
                index += 1;
            }
            b"WITHSCORES" => {
                with_scores = true;
                index += 1;
            }
            b"LIMIT" if index + 2 < args.len() => {
                let (Ok(offset), Ok(count)) =
                    (parse_usize(args[index + 1]), parse_usize(args[index + 2]))
                else {
                    return error("ERR value is not an integer or out of range");
                };
                limit = Some((offset, count));
                index += 3;
            }
            _ => return error("ERR syntax error"),
        }
    }
    if by_score {
        zrange_by_score_impl(store, args[0], args[1], args[2], rev, with_scores, limit)
    } else {
        let (Ok(start), Ok(stop)) = (parse_i64(args[1]), parse_i64(args[2])) else {
            return error("ERR value is not an integer or out of range");
        };
        zrange_by_rank_impl(store, args[0], start, stop, rev, with_scores)
    }
}

fn zrangebyscore(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 3 {
        return wrong_arity("ZRANGEBYSCORE");
    }
    let mut with_scores = false;
    let mut limit = None;
    let mut index = 3;
    while index < args.len() {
        match upper(args[index]).as_slice() {
            b"WITHSCORES" => {
                with_scores = true;
                index += 1;
            }
            b"LIMIT" if index + 2 < args.len() => {
                let (Ok(offset), Ok(count)) =
                    (parse_usize(args[index + 1]), parse_usize(args[index + 2]))
                else {
                    return error("ERR value is not an integer or out of range");
                };
                limit = Some((offset, count));
                index += 3;
            }
            _ => return error("ERR syntax error"),
        }
    }
    zrange_by_score_impl(store, args[0], args[1], args[2], false, with_scores, limit)
}

#[cfg(feature = "server")]
fn write_zrange_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
    if args.len() < 3 {
        write_frame(out, &wrong_arity("ZRANGE"));
        return;
    }

    let mut rev = false;
    let mut with_scores = false;
    let mut index = 3;
    while index < args.len() {
        if eq_ignore_ascii_case(args[index], b"REV") {
            rev = true;
            index += 1;
        } else if eq_ignore_ascii_case(args[index], b"WITHSCORES") {
            with_scores = true;
            index += 1;
        } else {
            write_frame(out, &zrange(store, args));
            return;
        }
    }

    let (Ok(start), Ok(stop)) = (parse_i64(args[1]), parse_i64(args[2])) else {
        write_frame(out, &error("ERR value is not an integer or out of range"));
        return;
    };

    match store.zrange_entries_visit(args[0], start, stop, rev, |item| match item {
        RedisObjectZSetRangeItem::Begin(count) => {
            let len = if with_scores {
                count.saturating_mul(2)
            } else {
                count
            };
            write_resp_array_header(out, len);
        }
        RedisObjectZSetRangeItem::Entry { member, score } => {
            ServerWire::write_resp_blob_string(out, member);
            if with_scores {
                let score = format_score(score);
                ServerWire::write_resp_blob_string(out, &score);
            }
        }
    }) {
        RedisObjectReadOutcome::Written => {}
        RedisObjectReadOutcome::Missing => write_resp_array_header(out, 0),
        RedisObjectReadOutcome::WrongType => write_frame(out, &wrongtype()),
    }
}

fn zrange_by_rank_impl(
    store: &EmbeddedStore,
    key: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
    with_scores: bool,
) -> Frame {
    let mut entries = match store.zentries(key) {
        Ok(entries) => entries,
        Err(RedisObjectError::WrongType) => return wrongtype(),
        Err(RedisObjectError::MissingKey) => Vec::new(),
    };
    if rev {
        entries.reverse();
    }
    let Some((start, stop)) = normalize_range(entries.len(), start, stop) else {
        return Frame::Array(Vec::new());
    };
    zentries_frame(entries[start..=stop].to_vec(), with_scores)
}

fn zrange_by_score_impl(
    store: &EmbeddedStore,
    key: &[u8],
    min: &[u8],
    max: &[u8],
    rev: bool,
    with_scores: bool,
    limit: Option<(usize, usize)>,
) -> Frame {
    let lower = if rev { max } else { min };
    let upper_bound = if rev { min } else { max };
    let Ok(lower) = parse_score_bound(lower) else {
        return error("ERR min or max is not a float");
    };
    let Ok(upper) = parse_score_bound(upper_bound) else {
        return error("ERR min or max is not a float");
    };
    let mut entries = match store.zentries(key) {
        Ok(entries) => entries,
        Err(RedisObjectError::WrongType) => return wrongtype(),
        Err(RedisObjectError::MissingKey) => Vec::new(),
    };
    entries.retain(|(_, score)| lower.contains(*score, true) && upper.contains(*score, false));
    if rev {
        entries.reverse();
    }
    if let Some((offset, count)) = limit {
        entries = entries.into_iter().skip(offset).take(count).collect();
    }
    zentries_frame(entries, with_scores)
}

fn zincrby(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, delta, member] => match parse_f64(delta) {
            Ok(delta) => frame_from_result(store.zincrby(key, delta, member)),
            Err(_) => error("ERR value is not a valid float"),
        },
        _ => wrong_arity("ZINCRBY"),
    }
}

fn zcount(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, min, max] => {
            let (Ok(min), Ok(max)) = (parse_score_bound(min), parse_score_bound(max)) else {
                return error("ERR min or max is not a float");
            };
            match store.zentries(key) {
                Ok(entries) => int(entries
                    .iter()
                    .filter(|(_, score)| min.contains(*score, true) && max.contains(*score, false))
                    .count() as i64),
                Err(RedisObjectError::WrongType) => wrongtype(),
                Err(RedisObjectError::MissingKey) => int(0),
            }
        }
        _ => wrong_arity("ZCOUNT"),
    }
}

fn zrank(store: &EmbeddedStore, args: &[&[u8]], rev: bool) -> Frame {
    match args {
        [key, member] => match store.zrank_value(key, member, rev) {
            Ok(Some(rank)) => int(rank as i64),
            Ok(None) | Err(RedisObjectError::MissingKey) => Frame::Null,
            Err(RedisObjectError::WrongType) => wrongtype(),
        },
        _ => wrong_arity(if rev { "ZREVRANK" } else { "ZRANK" }),
    }
}

fn zpop(store: &EmbeddedStore, args: &[&[u8]], max: bool) -> Frame {
    match args {
        [key] => frame_from_result(store.zpop(key, 1, max)),
        [key, count] => match parse_usize(count) {
            Ok(count) => frame_from_result(store.zpop(key, count, max)),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity(if max { "ZPOPMAX" } else { "ZPOPMIN" }),
    }
}

fn zrem(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("ZREM");
    }
    frame_from_result(store.zrem_many(args[0], &args[1..]))
}

fn zscan(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity("ZSCAN");
    }
    match store.zentries(args[0]) {
        Ok(entries) => scan_array(zentries_flat(entries)),
        Err(RedisObjectError::WrongType) => wrongtype(),
        Err(RedisObjectError::MissingKey) => scan_array(Vec::new()),
    }
}

fn zrangebylex(store: &EmbeddedStore, args: &[&[u8]], rev: bool) -> Frame {
    match args {
        [key, min, max] => {
            let (Ok(min), Ok(max)) = (parse_lex_bound(min), parse_lex_bound(max)) else {
                return error("ERR min or max not valid string range item");
            };
            let mut entries = match store.zentries(key) {
                Ok(entries) => entries,
                Err(RedisObjectError::WrongType) => return wrongtype(),
                Err(RedisObjectError::MissingKey) => Vec::new(),
            };
            entries.retain(|(member, _)| min.contains(member, true) && max.contains(member, false));
            if rev {
                entries.reverse();
            }
            array_bulk(entries.into_iter().map(|(member, _)| member).collect())
        }
        _ => wrong_arity(if rev { "ZREVRANGEBYLEX" } else { "ZRANGEBYLEX" }),
    }
}

fn zlexcount(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key, min, max] => {
            let (Ok(min), Ok(max)) = (parse_lex_bound(min), parse_lex_bound(max)) else {
                return error("ERR min or max not valid string range item");
            };
            match store.zentries(key) {
                Ok(entries) => int(entries
                    .iter()
                    .filter(|(member, _)| min.contains(member, true) && max.contains(member, false))
                    .count() as i64),
                Err(RedisObjectError::WrongType) => wrongtype(),
                Err(RedisObjectError::MissingKey) => int(0),
            }
        }
        _ => wrong_arity("ZLEXCOUNT"),
    }
}

fn zrangestore(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [dest, source, start, stop] => {
            let (Ok(start), Ok(stop)) = (parse_i64(start), parse_i64(stop)) else {
                return error("ERR value is not an integer or out of range");
            };
            let entries = match store.zentries(source) {
                Ok(entries) => entries,
                Err(RedisObjectError::WrongType) => return wrongtype(),
                Err(RedisObjectError::MissingKey) => Vec::new(),
            };
            let selected = normalize_range(entries.len(), start, stop)
                .map(|(start, stop)| entries[start..=stop].to_vec())
                .unwrap_or_default();
            store.set_object_value(dest, RedisObjectValue::ZSet(selected.clone()), None);
            int(selected.len() as i64)
        }
        _ => wrong_arity("ZRANGESTORE"),
    }
}

fn zaggregate_store(store: &EmbeddedStore, args: &[&[u8]], kind: ZAggregateKind) -> Frame {
    if args.len() < 3 {
        return wrong_arity(kind.name());
    }
    let Ok(numkeys) = parse_usize(args[1]) else {
        return error("ERR value is not an integer or out of range");
    };
    if args.len() < 2 + numkeys {
        return error("ERR syntax error");
    }
    let dest = args[0];
    let keys = &args[2..2 + numkeys];
    let mut weights = vec![1.0; numkeys];
    let mut aggregate = Aggregate::Sum;
    let mut index = 2 + numkeys;
    while index < args.len() {
        match upper(args[index]).as_slice() {
            b"WEIGHTS" if index + numkeys < args.len() => {
                for (weight, raw) in weights
                    .iter_mut()
                    .zip(&args[index + 1..index + 1 + numkeys])
                {
                    let Ok(parsed) = parse_f64(raw) else {
                        return error("ERR weight value is not a float");
                    };
                    *weight = parsed;
                }
                index += 1 + numkeys;
            }
            b"AGGREGATE" if index + 1 < args.len() => {
                aggregate = match upper(args[index + 1]).as_slice() {
                    b"SUM" => Aggregate::Sum,
                    b"MIN" => Aggregate::Min,
                    b"MAX" => Aggregate::Max,
                    _ => return error("ERR syntax error"),
                };
                index += 2;
            }
            _ => return error("ERR syntax error"),
        }
    }
    let entries = match compute_zaggregate(store, keys, &weights, kind, aggregate) {
        Ok(entries) => entries,
        Err(frame) => return frame,
    };
    store.set_object_value(dest, RedisObjectValue::ZSet(entries.clone()), None);
    int(entries.len() as i64)
}

fn bzpop(store: &EmbeddedStore, args: &[&[u8]], max: bool) -> Frame {
    if args.len() < 2 {
        return wrong_arity(if max { "BZPOPMAX" } else { "BZPOPMIN" });
    }
    for key in &args[..args.len() - 1] {
        let mut entries = match store.zentries(key) {
            Ok(entries) => entries,
            Err(RedisObjectError::WrongType) => return wrongtype(),
            Err(RedisObjectError::MissingKey) => Vec::new(),
        };
        if entries.is_empty() {
            continue;
        }
        if max {
            entries.reverse();
        }
        let (member, score) = entries[0].clone();
        let _ = store.zrem(key, &member);
        return Frame::Array(vec![
            bulk((*key).to_vec()),
            bulk(member),
            bulk(format_score(score)),
        ]);
    }
    Frame::Null
}

#[derive(Clone, Copy)]
enum SetOp {
    Union,
    Inter,
    Diff,
}

impl SetOp {
    fn name(self, store: bool) -> &'static str {
        match (self, store) {
            (Self::Union, false) => "SUNION",
            (Self::Inter, false) => "SINTER",
            (Self::Diff, false) => "SDIFF",
            (Self::Union, true) => "SUNIONSTORE",
            (Self::Inter, true) => "SINTERSTORE",
            (Self::Diff, true) => "SDIFFSTORE",
        }
    }
}

fn compute_set_op(
    store: &EmbeddedStore,
    keys: &[&[u8]],
    op: SetOp,
) -> std::result::Result<Vec<Vec<u8>>, Frame> {
    let mut sets = Vec::with_capacity(keys.len());
    for key in keys {
        match store.set_members(key) {
            Ok(members) => sets.push(members),
            Err(RedisObjectError::WrongType) => return Err(wrongtype()),
            Err(RedisObjectError::MissingKey) => sets.push(Vec::new()),
        }
    }
    let mut result = BTreeSet::<Vec<u8>>::new();
    match op {
        SetOp::Union => {
            for set in sets {
                result.extend(set);
            }
        }
        SetOp::Inter => {
            if let Some((first, rest)) = sets.split_first() {
                result.extend(
                    first.iter().cloned().filter(|member| {
                        rest.iter().all(|set| set.iter().any(|item| item == member))
                    }),
                );
            }
        }
        SetOp::Diff => {
            if let Some((first, rest)) = sets.split_first() {
                result.extend(
                    first
                        .iter()
                        .filter(|member| {
                            !rest
                                .iter()
                                .any(|set| set.iter().any(|item| item == *member))
                        })
                        .cloned(),
                );
            }
        }
    }
    Ok(result.into_iter().collect())
}

#[derive(Clone, Copy)]
enum ZAggregateKind {
    Union,
    Inter,
    Diff,
}

impl ZAggregateKind {
    fn name(self) -> &'static str {
        match self {
            Self::Union => "ZUNIONSTORE",
            Self::Inter => "ZINTERSTORE",
            Self::Diff => "ZDIFFSTORE",
        }
    }
}

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

fn compute_zaggregate(
    store: &EmbeddedStore,
    keys: &[&[u8]],
    weights: &[f64],
    kind: ZAggregateKind,
    aggregate: Aggregate,
) -> std::result::Result<Vec<(Vec<u8>, f64)>, Frame> {
    let mut maps = Vec::with_capacity(keys.len());
    for (key, weight) in keys.iter().zip(weights.iter().copied()) {
        let entries = match store.zentries(key) {
            Ok(entries) => entries,
            Err(RedisObjectError::WrongType) => return Err(wrongtype()),
            Err(RedisObjectError::MissingKey) => Vec::new(),
        };
        maps.push(
            entries
                .into_iter()
                .map(|(member, score)| (member, score * weight))
                .collect::<BTreeMap<_, _>>(),
        );
    }

    let mut out = BTreeMap::<Vec<u8>, f64>::new();
    match kind {
        ZAggregateKind::Union => {
            for map in maps {
                for (member, score) in map {
                    out.entry(member)
                        .and_modify(|existing| {
                            *existing = aggregate_score(*existing, score, aggregate)
                        })
                        .or_insert(score);
                }
            }
        }
        ZAggregateKind::Inter => {
            if let Some((first, rest)) = maps.split_first() {
                for (member, score) in first {
                    if rest.iter().all(|map| map.contains_key(member)) {
                        let combined = rest.iter().fold(*score, |acc, map| {
                            aggregate_score(acc, map[member], aggregate)
                        });
                        out.insert(member.clone(), combined);
                    }
                }
            }
        }
        ZAggregateKind::Diff => {
            if let Some((first, rest)) = maps.split_first() {
                for (member, score) in first {
                    if !rest.iter().any(|map| map.contains_key(member)) {
                        out.insert(member.clone(), *score);
                    }
                }
            }
        }
    }
    let mut entries = out.into_iter().collect::<Vec<_>>();
    entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
        left_score
            .total_cmp(right_score)
            .then_with(|| left_member.cmp(right_member))
    });
    Ok(entries)
}

fn aggregate_score(left: f64, right: f64, aggregate: Aggregate) -> f64 {
    match aggregate {
        Aggregate::Sum => left + right,
        Aggregate::Min => left.min(right),
        Aggregate::Max => left.max(right),
    }
}

fn object_result(
    name: &str,
    args: &[&[u8]],
    arity: usize,
    op: impl FnOnce() -> RedisObjectResult,
) -> Frame {
    if args.len() != arity {
        return wrong_arity(name);
    }
    frame_from_result(op())
}

fn string_value(store: &EmbeddedStore, key: &[u8]) -> std::result::Result<Vec<u8>, Frame> {
    match optional_string_value(store, key, true)? {
        Some(value) => Ok(value),
        None => Ok(Vec::new()),
    }
}

fn optional_string_value(
    store: &EmbeddedStore,
    key: &[u8],
    wrongtype_errors: bool,
) -> std::result::Result<Option<Vec<u8>>, Frame> {
    let mut value = None;
    match store.get_string_value_into(key, |bytes| value = Some(bytes.to_vec())) {
        crate::storage::RedisStringLookup::Hit => Ok(value),
        crate::storage::RedisStringLookup::Miss => Ok(None),
        crate::storage::RedisStringLookup::WrongType if wrongtype_errors => Err(wrongtype()),
        crate::storage::RedisStringLookup::WrongType => Ok(None),
    }
}

fn frame_from_result(result: RedisObjectResult) -> Frame {
    match result {
        RedisObjectResult::Simple("OK") => simple("OK"),
        RedisObjectResult::Simple(message) if message.starts_with("ERR ") => error(message),
        RedisObjectResult::Simple(message) => bulk(message.as_bytes().to_vec()),
        RedisObjectResult::Integer(value) => int(value),
        RedisObjectResult::IntegerArray(values) => {
            Frame::Array(values.into_iter().map(Frame::Integer).collect())
        }
        RedisObjectResult::Bulk(Some(value)) => bulk(value),
        RedisObjectResult::Bulk(None) => Frame::Null,
        RedisObjectResult::Array(values) => Frame::Array(
            values
                .into_iter()
                .map(|value| value.map_or(Frame::Null, bulk))
                .collect(),
        ),
        RedisObjectResult::WrongType => wrongtype(),
    }
}

fn scan_from_result(result: RedisObjectResult) -> Frame {
    match frame_from_result(result) {
        Frame::Array(items) => Frame::Array(vec![bulk(b"0".to_vec()), Frame::Array(items)]),
        other => other,
    }
}

fn scan_array(values: Vec<Vec<u8>>) -> Frame {
    scan_array_with_cursor(0, values)
}

fn scan_array_with_cursor(cursor: u64, values: Vec<Vec<u8>>) -> Frame {
    Frame::Array(vec![
        bulk(cursor.to_string().into_bytes()),
        array_bulk(values),
    ])
}

fn array_bulk(values: Vec<Vec<u8>>) -> Frame {
    Frame::Array(values.into_iter().map(bulk).collect())
}

fn zentries_frame(entries: Vec<(Vec<u8>, f64)>, with_scores: bool) -> Frame {
    if with_scores {
        array_bulk(zentries_flat(entries))
    } else {
        array_bulk(entries.into_iter().map(|(member, _)| member).collect())
    }
}

fn zentries_flat(entries: Vec<(Vec<u8>, f64)>) -> Vec<Vec<u8>> {
    entries
        .into_iter()
        .flat_map(|(member, score)| [member, format_score(score)])
        .collect()
}

fn normalize_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len_i64 = len as i64;
    let start = if start < 0 { len_i64 + start } else { start }.clamp(0, len_i64);
    let stop = if stop < 0 { len_i64 + stop } else { stop }.clamp(0, len_i64 - 1);
    if start > stop {
        None
    } else {
        Some((start as usize, stop as usize))
    }
}

#[derive(Clone, Copy)]
struct ScoreBound {
    value: f64,
    inclusive: bool,
    neg_inf: bool,
    pos_inf: bool,
}

impl ScoreBound {
    fn contains(self, score: f64, lower: bool) -> bool {
        if self.neg_inf {
            return !lower;
        }
        if self.pos_inf {
            return lower;
        }
        match (lower, self.inclusive) {
            (true, true) => score >= self.value,
            (true, false) => score > self.value,
            (false, true) => score <= self.value,
            (false, false) => score < self.value,
        }
    }
}

fn parse_score_bound(raw: &[u8]) -> std::result::Result<ScoreBound, ()> {
    let (inclusive, raw) = match raw.split_first() {
        Some((b'(', tail)) => (false, tail),
        _ => (true, raw),
    };
    match upper(raw).as_slice() {
        b"-INF" => Ok(ScoreBound {
            value: f64::NEG_INFINITY,
            inclusive,
            neg_inf: true,
            pos_inf: false,
        }),
        b"+INF" | b"INF" => Ok(ScoreBound {
            value: f64::INFINITY,
            inclusive,
            neg_inf: false,
            pos_inf: true,
        }),
        _ => parse_f64(raw).map(|value| ScoreBound {
            value,
            inclusive,
            neg_inf: false,
            pos_inf: false,
        }),
    }
}

#[derive(Clone, Copy)]
struct LexBound<'a> {
    value: &'a [u8],
    inclusive: bool,
    neg_inf: bool,
    pos_inf: bool,
}

impl LexBound<'_> {
    fn contains(self, member: &[u8], lower: bool) -> bool {
        if self.neg_inf {
            return !lower;
        }
        if self.pos_inf {
            return lower;
        }
        match (lower, self.inclusive) {
            (true, true) => member >= self.value,
            (true, false) => member > self.value,
            (false, true) => member <= self.value,
            (false, false) => member < self.value,
        }
    }
}

fn parse_lex_bound(raw: &[u8]) -> std::result::Result<LexBound<'_>, ()> {
    match raw {
        b"-" => Ok(LexBound {
            value: raw,
            inclusive: false,
            neg_inf: true,
            pos_inf: false,
        }),
        b"+" => Ok(LexBound {
            value: raw,
            inclusive: false,
            neg_inf: false,
            pos_inf: true,
        }),
        [b'[', tail @ ..] => Ok(LexBound {
            value: tail,
            inclusive: true,
            neg_inf: false,
            pos_inf: false,
        }),
        [b'(', tail @ ..] => Ok(LexBound {
            value: tail,
            inclusive: false,
            neg_inf: false,
            pos_inf: false,
        }),
        _ => Err(()),
    }
}

fn parse_i64(raw: &[u8]) -> std::result::Result<i64, ()> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(())
}

fn parse_usize(raw: &[u8]) -> std::result::Result<usize, ()> {
    let value = parse_i64(raw)?;
    usize::try_from(value).map_err(|_| ())
}

fn parse_u64(raw: &[u8]) -> std::result::Result<u64, ()> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(())
}

fn parse_f64(raw: &[u8]) -> std::result::Result<f64, ()> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .ok_or(())?;
    value.is_finite().then_some(value).ok_or(())
}

fn format_score(score: f64) -> Vec<u8> {
    score.to_string().into_bytes()
}

fn glob_matches(pattern: &[u8], value: &[u8]) -> bool {
    if pattern == b"*" {
        return true;
    }
    match pattern.iter().position(|byte| *byte == b'*') {
        Some(index) => {
            value.starts_with(&pattern[..index]) && value.ends_with(&pattern[index + 1..])
        }
        None => pattern == value,
    }
}

fn upper(value: &[u8]) -> Vec<u8> {
    value.iter().map(u8::to_ascii_uppercase).collect()
}

fn eq_ignore_ascii_case(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn wrong_arity(command: &str) -> Frame {
    error(&format!(
        "ERR wrong number of arguments for '{}' command",
        command.to_ascii_lowercase()
    ))
}

fn wrongtype() -> Frame {
    error(crate::storage::WRONGTYPE_MESSAGE)
}

fn error(message: &str) -> Frame {
    Frame::Error(message.into())
}

fn simple(message: &str) -> Frame {
    Frame::SimpleString(message.into())
}

fn bulk(value: Vec<u8>) -> Frame {
    Frame::BlobString(value)
}

fn int(value: i64) -> Frame {
    Frame::Integer(value)
}

#[cfg(feature = "server")]
fn write_fast_frame(out: &mut BytesMut, frame: &Frame) {
    match frame {
        Frame::SimpleString(value) => ServerWire::write_fast_value(out, value.as_bytes()),
        Frame::BlobString(value) => ServerWire::write_fast_value(out, value),
        Frame::Integer(value) => ServerWire::write_fast_integer(out, *value),
        Frame::Null => ServerWire::write_fast_null(out),
        Frame::Error(message) => ServerWire::write_fast_error(out, message),
        Frame::Array(_) | Frame::Boolean(_) => {
            let mut resp = BytesMut::new();
            write_frame(&mut resp, frame);
            ServerWire::write_fast_value(out, &resp);
        }
    }
}

#[cfg(feature = "server")]
fn write_frame(out: &mut BytesMut, frame: &Frame) {
    match frame {
        Frame::SimpleString(value) => {
            out.extend_from_slice(b"+");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::BlobString(value) => ServerWire::write_resp_blob_string(out, value),
        Frame::Integer(value) => ServerWire::write_resp_integer(out, *value),
        Frame::Array(items) => {
            write_resp_array_header(out, items.len());
            for item in items {
                write_frame(out, item);
            }
        }
        Frame::Null => out.extend_from_slice(b"$-1\r\n"),
        Frame::Boolean(value) => {
            out.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" });
        }
        Frame::Error(message) => ServerWire::write_resp_error(out, message),
    }
}

#[cfg(feature = "server")]
fn write_resp_array_header(out: &mut BytesMut, len: usize) {
    out.extend_from_slice(b"*");
    let mut len_buf = itoa::Buffer::new();
    out.extend_from_slice(len_buf.format(len).as_bytes());
    out.extend_from_slice(b"\r\n");
}
