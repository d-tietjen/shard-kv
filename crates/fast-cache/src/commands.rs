pub(crate) mod parsing;

#[cfg(feature = "redis-compat")]
#[path = "commands/compat/redis_compat.rs"]
pub(crate) mod redis_compat;

// Key commands.
#[path = "commands/key/del.rs"]
pub mod del;
#[path = "commands/key/exists.rs"]
pub mod exists;
#[path = "commands/key/expire.rs"]
pub mod expire;
#[cfg(feature = "redis-compat")]
#[path = "commands/key/keys.rs"]
pub mod keys;
#[path = "commands/key/persist.rs"]
pub mod persist;
#[path = "commands/key/pexpire.rs"]
pub mod pexpire;
#[path = "commands/key/pttl.rs"]
pub mod pttl;
#[cfg(feature = "redis-compat")]
#[path = "commands/key/scan.rs"]
pub mod scan;
#[path = "commands/key/ttl.rs"]
pub mod ttl;
#[cfg(feature = "redis-compat")]
#[path = "commands/key/type_cmd.rs"]
pub mod type_cmd;

// String commands.
#[cfg(feature = "redis-compat")]
#[path = "commands/string/append.rs"]
pub mod append;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/decr.rs"]
pub mod decr;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/decrby.rs"]
pub mod decrby;
#[path = "commands/string/get.rs"]
pub mod get;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/getdel.rs"]
pub mod getdel;
#[path = "commands/string/getex.rs"]
pub mod getex;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/getrange.rs"]
pub mod getrange;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/getset.rs"]
pub mod getset;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/incr.rs"]
pub mod incr;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/incrby.rs"]
pub mod incrby;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/incrbyfloat.rs"]
pub mod incrbyfloat;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/mget.rs"]
pub mod mget;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/mset.rs"]
pub mod mset;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/msetnx.rs"]
pub mod msetnx;
#[path = "commands/string/psetex.rs"]
pub mod psetex;
#[path = "commands/string/set.rs"]
pub mod set;
#[path = "commands/string/setex.rs"]
pub mod setex;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/setrange.rs"]
pub mod setrange;
#[cfg(feature = "redis-compat")]
#[path = "commands/string/strlen.rs"]
pub mod strlen;

// Connection commands.
#[cfg(feature = "redis-compat")]
#[path = "commands/connection/auth.rs"]
pub mod auth;
#[cfg(feature = "redis-compat")]
#[path = "commands/connection/echo.rs"]
pub mod echo;
#[cfg(feature = "redis-compat")]
#[path = "commands/connection/hello.rs"]
pub mod hello;
#[cfg(feature = "redis-compat")]
#[path = "commands/connection/ping.rs"]
pub mod ping;
#[cfg(feature = "redis-compat")]
#[path = "commands/connection/quit.rs"]
pub mod quit;
#[cfg(feature = "redis-compat")]
#[path = "commands/connection/select.rs"]
pub mod select;

// Server commands.
#[cfg(feature = "redis-compat")]
#[path = "commands/server/client.rs"]
pub mod client;
#[cfg(feature = "redis-compat")]
#[path = "commands/server/command.rs"]
pub mod command;
#[cfg(feature = "redis-compat")]
#[path = "commands/server/config.rs"]
pub mod config;
#[cfg(feature = "redis-compat")]
#[path = "commands/server/dbsize.rs"]
pub mod dbsize;
#[cfg(feature = "redis-compat")]
#[path = "commands/server/info.rs"]
pub mod info;
#[cfg(feature = "redis-compat")]
#[path = "commands/server/time.rs"]
pub mod time;

// Hash commands.
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hdel.rs"]
pub mod hdel;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hexists.rs"]
pub mod hexists;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hget.rs"]
pub mod hget;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hgetall.rs"]
pub mod hgetall;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hincrby.rs"]
pub mod hincrby;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hincrbyfloat.rs"]
pub mod hincrbyfloat;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hkeys.rs"]
pub mod hkeys;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hlen.rs"]
pub mod hlen;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hmget.rs"]
pub mod hmget;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hrandfield.rs"]
pub mod hrandfield;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hscan.rs"]
pub mod hscan;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hset.rs"]
pub mod hset;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hsetnx.rs"]
pub mod hsetnx;
#[cfg(feature = "redis-compat")]
#[path = "commands/hash/hvals.rs"]
pub mod hvals;

// List commands.
#[cfg(feature = "redis-compat")]
#[path = "commands/list/blmove.rs"]
pub mod blmove;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/blpop.rs"]
pub mod blpop;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/brpop.rs"]
pub mod brpop;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/lindex.rs"]
pub mod lindex;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/linsert.rs"]
pub mod linsert;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/llen.rs"]
pub mod llen;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/lpop.rs"]
pub mod lpop;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/lpush.rs"]
pub mod lpush;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/lpushx.rs"]
pub mod lpushx;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/lrange.rs"]
pub mod lrange;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/lrem.rs"]
pub mod lrem;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/lset.rs"]
pub mod lset;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/ltrim.rs"]
pub mod ltrim;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/rpop.rs"]
pub mod rpop;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/rpush.rs"]
pub mod rpush;
#[cfg(feature = "redis-compat")]
#[path = "commands/list/rpushx.rs"]
pub mod rpushx;

// Set collection commands.
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/sadd.rs"]
pub mod sadd;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/scard.rs"]
pub mod scard;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/sdiff.rs"]
pub mod sdiff;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/sdiffstore.rs"]
pub mod sdiffstore;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/sinter.rs"]
pub mod sinter;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/sinterstore.rs"]
pub mod sinterstore;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/sismember.rs"]
pub mod sismember;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/smembers.rs"]
pub mod smembers;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/smismember.rs"]
pub mod smismember;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/smove.rs"]
pub mod smove;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/spop.rs"]
pub mod spop;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/srandmember.rs"]
pub mod srandmember;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/srem.rs"]
pub mod srem;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/sscan.rs"]
pub mod sscan;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/sunion.rs"]
pub mod sunion;
#[cfg(feature = "redis-compat")]
#[path = "commands/sets/sunionstore.rs"]
pub mod sunionstore;

// Sorted set commands.
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/bzpopmax.rs"]
pub mod bzpopmax;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/bzpopmin.rs"]
pub mod bzpopmin;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zadd.rs"]
pub mod zadd;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zcard.rs"]
pub mod zcard;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zcount.rs"]
pub mod zcount;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zdiffstore.rs"]
pub mod zdiffstore;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zincrby.rs"]
pub mod zincrby;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zinterstore.rs"]
pub mod zinterstore;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zlexcount.rs"]
pub mod zlexcount;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zmscore.rs"]
pub mod zmscore;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zpopmax.rs"]
pub mod zpopmax;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zpopmin.rs"]
pub mod zpopmin;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zrange.rs"]
pub mod zrange;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zrangebylex.rs"]
pub mod zrangebylex;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zrangebyscore.rs"]
pub mod zrangebyscore;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zrangestore.rs"]
pub mod zrangestore;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zrank.rs"]
pub mod zrank;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zrem.rs"]
pub mod zrem;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zrevrangebylex.rs"]
pub mod zrevrangebylex;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zrevrank.rs"]
pub mod zrevrank;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zscan.rs"]
pub mod zscan;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zscore.rs"]
pub mod zscore;
#[cfg(feature = "redis-compat")]
#[path = "commands/zset/zunionstore.rs"]
pub mod zunionstore;

use bytes::Bytes as SharedBytes;

use crate::protocol::{CommandSpanFrame, FastCommand, FastRequest, FastResponse, Frame};
use crate::storage::{
    EngineCommandContext, EngineFastFuture, EngineFrameFuture, EngineRespSpanFuture,
};
use crate::{FastCacheError, Result};

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
    fn mutates_value(&self) -> bool;
    fn matches(&self, name: &[u8]) -> bool;
}

impl<T> CommandMetadata for T
where
    T: CommandSpec + Sync,
{
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
    &get::COMMAND,
    &set::COMMAND,
    &del::COMMAND,
    &exists::COMMAND,
    &ttl::COMMAND,
    &pttl::COMMAND,
    &expire::COMMAND,
    &pexpire::COMMAND,
    &persist::COMMAND,
    &getex::COMMAND,
    &setex::COMMAND,
    &psetex::COMMAND,
    #[cfg(feature = "redis-compat")]
    &ping::COMMAND,
    #[cfg(feature = "redis-compat")]
    &auth::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hello::COMMAND,
    #[cfg(feature = "redis-compat")]
    &select::COMMAND,
    #[cfg(feature = "redis-compat")]
    &quit::COMMAND,
    #[cfg(feature = "redis-compat")]
    &echo::COMMAND,
    #[cfg(feature = "redis-compat")]
    &command::COMMAND,
    #[cfg(feature = "redis-compat")]
    &config::COMMAND,
    #[cfg(feature = "redis-compat")]
    &client::COMMAND,
    #[cfg(feature = "redis-compat")]
    &dbsize::COMMAND,
    #[cfg(feature = "redis-compat")]
    &time::COMMAND,
    #[cfg(feature = "redis-compat")]
    &info::COMMAND,
    #[cfg(feature = "redis-compat")]
    &keys::COMMAND,
    #[cfg(feature = "redis-compat")]
    &scan::COMMAND,
    #[cfg(feature = "redis-compat")]
    &type_cmd::COMMAND,
    #[cfg(feature = "redis-compat")]
    &append::COMMAND,
    #[cfg(feature = "redis-compat")]
    &strlen::COMMAND,
    #[cfg(feature = "redis-compat")]
    &getrange::COMMAND,
    #[cfg(feature = "redis-compat")]
    &setrange::COMMAND,
    #[cfg(feature = "redis-compat")]
    &getset::COMMAND,
    #[cfg(feature = "redis-compat")]
    &getdel::COMMAND,
    #[cfg(feature = "redis-compat")]
    &incr::COMMAND,
    #[cfg(feature = "redis-compat")]
    &incrby::COMMAND,
    #[cfg(feature = "redis-compat")]
    &decr::COMMAND,
    #[cfg(feature = "redis-compat")]
    &decrby::COMMAND,
    #[cfg(feature = "redis-compat")]
    &incrbyfloat::COMMAND,
    #[cfg(feature = "redis-compat")]
    &mset::COMMAND,
    #[cfg(feature = "redis-compat")]
    &mget::COMMAND,
    #[cfg(feature = "redis-compat")]
    &msetnx::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hset::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hget::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hmget::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hlen::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hexists::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hsetnx::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hincrby::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hincrbyfloat::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hkeys::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hvals::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hgetall::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hscan::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hrandfield::COMMAND,
    #[cfg(feature = "redis-compat")]
    &hdel::COMMAND,
    #[cfg(feature = "redis-compat")]
    &lpush::COMMAND,
    #[cfg(feature = "redis-compat")]
    &rpush::COMMAND,
    #[cfg(feature = "redis-compat")]
    &lrange::COMMAND,
    #[cfg(feature = "redis-compat")]
    &llen::COMMAND,
    #[cfg(feature = "redis-compat")]
    &lindex::COMMAND,
    #[cfg(feature = "redis-compat")]
    &lset::COMMAND,
    #[cfg(feature = "redis-compat")]
    &lrem::COMMAND,
    #[cfg(feature = "redis-compat")]
    &linsert::COMMAND,
    #[cfg(feature = "redis-compat")]
    &ltrim::COMMAND,
    #[cfg(feature = "redis-compat")]
    &lpop::COMMAND,
    #[cfg(feature = "redis-compat")]
    &rpop::COMMAND,
    #[cfg(feature = "redis-compat")]
    &lpushx::COMMAND,
    #[cfg(feature = "redis-compat")]
    &rpushx::COMMAND,
    #[cfg(feature = "redis-compat")]
    &blpop::COMMAND,
    #[cfg(feature = "redis-compat")]
    &brpop::COMMAND,
    #[cfg(feature = "redis-compat")]
    &blmove::COMMAND,
    #[cfg(feature = "redis-compat")]
    &sadd::COMMAND,
    #[cfg(feature = "redis-compat")]
    &sismember::COMMAND,
    #[cfg(feature = "redis-compat")]
    &smismember::COMMAND,
    #[cfg(feature = "redis-compat")]
    &scard::COMMAND,
    #[cfg(feature = "redis-compat")]
    &smembers::COMMAND,
    #[cfg(feature = "redis-compat")]
    &sunion::COMMAND,
    #[cfg(feature = "redis-compat")]
    &sinter::COMMAND,
    #[cfg(feature = "redis-compat")]
    &sdiff::COMMAND,
    #[cfg(feature = "redis-compat")]
    &sunionstore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &sinterstore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &sdiffstore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &smove::COMMAND,
    #[cfg(feature = "redis-compat")]
    &srem::COMMAND,
    #[cfg(feature = "redis-compat")]
    &sscan::COMMAND,
    #[cfg(feature = "redis-compat")]
    &srandmember::COMMAND,
    #[cfg(feature = "redis-compat")]
    &spop::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zadd::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zscore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zmscore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zcard::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zrange::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zincrby::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zcount::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zrank::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zrevrank::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zpopmin::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zpopmax::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zrem::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zrangebyscore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zscan::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zrangebylex::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zrevrangebylex::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zlexcount::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zrangestore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zunionstore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zinterstore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &zdiffstore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &bzpopmin::COMMAND,
    #[cfg(feature = "redis-compat")]
    &bzpopmax::COMMAND,
];

pub(crate) struct CommandCatalog;

impl CommandCatalog {
    pub(crate) fn find(name: &[u8]) -> Option<&'static dyn CommandDefinition> {
        CATALOG
            .iter()
            .copied()
            .find(|command| command.matches(name))
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
                FastCacheError::Command(format!(
                    "unsupported command: {}",
                    String::from_utf8_lossy(name)
                ))
            }),
            None => Err(FastCacheError::Command("empty command".into())),
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
        let name = &owner[frame.parts.first()?.clone()];
        let handler = Self::find_resp_span(name)?;
        Some(
            handler
                .execute_engine_resp_spanned(ctx, frame, owner, out)
                .await,
        )
    }
}
