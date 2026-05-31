use super::frame::*;
#[cfg(feature = "redis-modules")]
use crate::commands::admin::Module;
#[cfg(feature = "redis-functions")]
use crate::commands::function_cmd::{FCall, FCallRo, Function};
use crate::commands::redis::RedisCommand;
use crate::commands::{
    acl::Acl,
    admin::{
        Asking, BgRewriteAof, BgSave, Cluster, Debug, Failover, HostWarning, LastSave, Latency,
        Lolwut, Migrate, Monitor, Move, PSync, PostWarning, ReadOnly, ReadWrite, ReplConf,
        ReplicaOf, Role, Save, Shutdown, SlowLog, Sort, SwapDb, Sync, Wait,
    },
    append::Append,
    auth::Auth,
    blmpop::BLMPop,
    blpop::BLPop,
    brpop::BRPop,
    brpoplpush::BRPopLPush,
    bzmpop::BZMPop,
    bzpopmax::BZPopMax,
    bzpopmin::BZPopMin,
    client::Client,
    command::CommandInfo,
    config::Config,
    dbsize::DbSize,
    decr::Decr,
    decrby::DecrBy,
    dump::Dump,
    echo::Echo,
    expiretime::ExpireTime,
    flush,
    geo::{
        GeoAdd, GeoDist, GeoHash, GeoPos, GeoRadius, GeoRadiusByMember, GeoRadiusByMemberRo,
        GeoRadiusRo, GeoSearch, GeoSearchStore,
    },
    getdel::GetDel,
    getrange::GetRange,
    getset::GetSet,
    hdel::HDel,
    hello::Hello,
    hexists::HExists,
    hget::HGet,
    hgetall::HGetAll,
    hincrby::HIncrBy,
    hincrbyfloat::HIncrByFloat,
    hkeys::HKeys,
    hlen::HLen,
    hll::{PFAdd, PFCount, PFDebug, PFMerge, PFSelfTest},
    hmget::HMGet,
    hrandfield::HRandField,
    hscan::HScan,
    hset::HSet,
    hsetnx::HSetNx,
    hvals::HVals,
    incr::Incr,
    incrby::IncrBy,
    incrbyfloat::IncrByFloat,
    info::Info,
    keys::Keys,
    lcs::Lcs,
    lindex::LIndex,
    linsert::LInsert,
    llen::LLen,
    lmpop::LMPop,
    lpop::LPop,
    lpos::LPos,
    lpush::LPush,
    lpushx::LPushX,
    lrange::LRange,
    lrem::LRem,
    lset::LSet,
    ltrim::LTrim,
    memory::Memory,
    mget::MGet,
    mset::MSet,
    msetnx::MSetNx,
    pexpiretime::PExpireTime,
    ping::Ping,
    pubsub::{
        PSubscribe, PUnsubscribe, PubSub, Publish, SPublish, SSubscribe, SUnsubscribe, Subscribe,
        Unsubscribe,
    },
    quit::Quit,
    reset::Reset,
    restore::Restore,
    rpop::RPop,
    rpush::RPush,
    rpushx::RPushX,
    sadd::SAdd,
    scan::Scan,
    scard::SCard,
    scripting::{Eval, EvalRo, EvalSha, EvalShaRo, Script},
    sdiff::SDiff,
    sdiffstore::SDiffStore,
    select::Select,
    setrange::SetRange,
    sinter::SInter,
    sinterstore::SInterStore,
    sismember::SIsMember,
    smembers::SMembers,
    smismember::SMIsMember,
    smove::SMove,
    spop::SPop,
    srandmember::SRandMember,
    srem::SRem,
    sscan::SScan,
    stralgo::StrAlgo,
    stream::{
        XAck, XAdd, XAutoClaim, XClaim, XDel, XGroup, XInfo, XLen, XPending, XRange, XRead,
        XReadGroup, XRevRange, XSetId, XTrim,
    },
    strlen::StrLen,
    sunion::SUnion,
    sunionstore::SUnionStore,
    time::Time,
    type_cmd::Type as TypeCommand,
    zadd::ZAdd,
    zcard::ZCard,
    zcount::ZCount,
    zdiff::ZDiff,
    zdiffstore::ZDiffStore,
    zincrby::ZIncrBy,
    zinter::ZInter,
    zintercard::ZInterCard,
    zinterstore::ZInterStore,
    zlexcount::ZLexCount,
    zmpop::ZMPop,
    zmscore::ZMScore,
    zpopmax::ZPopMax,
    zpopmin::ZPopMin,
    zrandmember::ZRandMember,
    zrange::ZRange,
    zrangebylex::ZRangeByLex,
    zrangebyscore::ZRangeByScore,
    zrangestore::ZRangeStore,
    zrank::ZRank,
    zrem::ZRem,
    zremrangebylex::ZRemRangeByLex,
    zremrangebyrank::ZRemRangeByRank,
    zremrangebyscore::ZRemRangeByScore,
    zrevrange::ZRevRange,
    zrevrangebylex::ZRevRangeByLex,
    zrevrangebyscore::ZRevRangeByScore,
    zrevrank::ZRevRank,
    zscan::ZScan,
    zscore::ZScore,
    zunion::ZUnion,
    zunionstore::ZUnionStore,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

pub(crate) fn dispatch(name: &str, store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    #[cfg(feature = "redis-modules")]
    if let Some(frame) = crate::commands::redis_modules::dispatch(name, store, args) {
        return frame;
    }

    match name {
        "PING" => Ping::execute(store, args),
        "AUTH" => Auth::execute(store, args),
        "SELECT" => Select::execute(store, args),
        "CLIENT" => Client::execute(store, args),
        "CONFIG" => Config::execute(store, args),
        "COMMAND" => CommandInfo::execute(store, args),
        "EVAL" => Eval::execute(store, args),
        "EVAL_RO" => EvalRo::execute(store, args),
        "EVALSHA" => EvalSha::execute(store, args),
        "EVALSHA_RO" => EvalShaRo::execute(store, args),
        "SCRIPT" => Script::execute(store, args),
        "HELLO" => Hello::execute(store, args),
        "QUIT" => Quit::execute(store, args),
        "ECHO" => Echo::execute(store, args),
        "ASKING" => Asking::execute(store, args),
        "BGREWRITEAOF" => BgRewriteAof::execute(store, args),
        "BGSAVE" => BgSave::execute(store, args),
        "CLUSTER" => Cluster::execute(store, args),
        "ACL" => Acl::execute(store, args),
        "FAILOVER" => Failover::execute(store, args),
        "RESET" => Reset::execute(store, args),
        "DEBUG" => Debug::execute(store, args),
        "HOST:" => HostWarning::execute(store, args),
        "LASTSAVE" => LastSave::execute(store, args),
        "LATENCY" => Latency::execute(store, args),
        "LOLWUT" => Lolwut::execute(store, args),
        "MIGRATE" => Migrate::execute(store, args),
        #[cfg(feature = "redis-modules")]
        "MODULE" => Module::execute(store, args),
        "MONITOR" => Monitor::execute(store, args),
        "MOVE" => Move::execute(store, args),
        "POST" => PostWarning::execute(store, args),
        "PSYNC" => PSync::execute(store, args),
        "READONLY" => ReadOnly::execute(store, args),
        "READWRITE" => ReadWrite::execute(store, args),
        "REPLCONF" => ReplConf::execute(store, args),
        "REPLICAOF" | "SLAVEOF" => ReplicaOf::execute(store, args),
        "ROLE" => Role::execute(store, args),
        "SAVE" => Save::execute(store, args),
        "SHUTDOWN" => Shutdown::execute(store, args),
        "SLOWLOG" => SlowLog::execute(store, args),
        "SORT" => Sort::execute(store, args),
        "SWAPDB" => SwapDb::execute(store, args),
        "SYNC" => Sync::execute(store, args),
        "WAIT" => Wait::execute(store, args),
        "DBSIZE" => DbSize::execute(store, args),
        "FLUSHDB" => flush::FlushDb::execute(store, args),
        "FLUSHALL" => flush::FlushAll::execute(store, args),
        #[cfg(feature = "redis-functions")]
        "FUNCTION" => Function::execute(store, args),
        #[cfg(feature = "redis-functions")]
        "FCALL" => FCall::execute(store, args),
        #[cfg(feature = "redis-functions")]
        "FCALL_RO" => FCallRo::execute(store, args),
        "TIME" => Time::execute(store, args),
        "INFO" => Info::execute(store, args),
        "MEMORY" => Memory::execute(store, args),
        "PUBLISH" => Publish::execute(store, args),
        "SPUBLISH" => SPublish::execute(store, args),
        "PUBSUB" => PubSub::execute(store, args),
        "SUBSCRIBE" => Subscribe::execute(store, args),
        "UNSUBSCRIBE" => Unsubscribe::execute(store, args),
        "PSUBSCRIBE" => PSubscribe::execute(store, args),
        "PUNSUBSCRIBE" => PUnsubscribe::execute(store, args),
        "SSUBSCRIBE" => SSubscribe::execute(store, args),
        "SUNSUBSCRIBE" => SUnsubscribe::execute(store, args),
        "TYPE" => TypeCommand::execute(store, args),
        "KEYS" => Keys::execute(store, args),
        "SCAN" => Scan::execute(store, args),
        "DUMP" => Dump::execute(store, args),
        "RESTORE" | "RESTORE-ASKING" => Restore::execute(store, args),
        "EXPIRETIME" => ExpireTime::execute(store, args),
        "PEXPIRETIME" => PExpireTime::execute(store, args),
        "APPEND" => Append::execute(store, args),
        "STRLEN" => StrLen::execute(store, args),
        "GETRANGE" | "SUBSTR" => GetRange::execute(store, args),
        "SETRANGE" => SetRange::execute(store, args),
        "GETSET" => GetSet::execute(store, args),
        "GETDEL" => GetDel::execute(store, args),
        "LCS" => Lcs::execute(store, args),
        "STRALGO" => StrAlgo::execute(store, args),
        "INCR" => Incr::execute(store, args),
        "INCRBY" => IncrBy::execute(store, args),
        "DECR" => Decr::execute(store, args),
        "DECRBY" => DecrBy::execute(store, args),
        "INCRBYFLOAT" => IncrByFloat::execute(store, args),
        "MSET" => MSet::execute(store, args),
        "MGET" => MGet::execute(store, args),
        "MSETNX" => MSetNx::execute(store, args),
        "PFADD" => PFAdd::execute(store, args),
        "PFCOUNT" => PFCount::execute(store, args),
        "PFMERGE" => PFMerge::execute(store, args),
        "PFDEBUG" => PFDebug::execute(store, args),
        "PFSELFTEST" => PFSelfTest::execute(store, args),
        "GEOADD" => GeoAdd::execute(store, args),
        "GEODIST" => GeoDist::execute(store, args),
        "GEOHASH" => GeoHash::execute(store, args),
        "GEOPOS" => GeoPos::execute(store, args),
        "GEORADIUS" => GeoRadius::execute(store, args),
        "GEORADIUS_RO" => GeoRadiusRo::execute(store, args),
        "GEORADIUSBYMEMBER" => GeoRadiusByMember::execute(store, args),
        "GEORADIUSBYMEMBER_RO" => GeoRadiusByMemberRo::execute(store, args),
        "GEOSEARCH" => GeoSearch::execute(store, args),
        "GEOSEARCHSTORE" => GeoSearchStore::execute(store, args),
        "HSET" => HSet::execute(store, args),
        "HGET" => HGet::execute(store, args),
        "HMGET" => HMGet::execute(store, args),
        "HLEN" => HLen::execute(store, args),
        "HEXISTS" => HExists::execute(store, args),
        "HSETNX" => HSetNx::execute(store, args),
        "HINCRBY" => HIncrBy::execute(store, args),
        "HINCRBYFLOAT" => HIncrByFloat::execute(store, args),
        "HKEYS" => HKeys::execute(store, args),
        "HVALS" => HVals::execute(store, args),
        "HGETALL" => HGetAll::execute(store, args),
        "HSCAN" => HScan::execute(store, args),
        "HRANDFIELD" => HRandField::execute(store, args),
        "HDEL" => HDel::execute(store, args),
        "LPUSH" => LPush::execute(store, args),
        "RPUSH" => RPush::execute(store, args),
        "LPUSHX" => LPushX::execute(store, args),
        "RPUSHX" => RPushX::execute(store, args),
        "LRANGE" => LRange::execute(store, args),
        "LLEN" => LLen::execute(store, args),
        "LINDEX" => LIndex::execute(store, args),
        "LSET" => LSet::execute(store, args),
        "LREM" => LRem::execute(store, args),
        "LINSERT" => LInsert::execute(store, args),
        "LTRIM" => LTrim::execute(store, args),
        "LPOP" => LPop::execute(store, args),
        "LPOS" => LPos::execute(store, args),
        "RPOP" => RPop::execute(store, args),
        "BLPOP" => BLPop::execute(store, args),
        "BRPOP" => BRPop::execute(store, args),
        "BRPOPLPUSH" => BRPopLPush::execute(store, args),
        "LMPOP" => LMPop::execute(store, args),
        "BLMPOP" => BLMPop::execute(store, args),
        "SADD" => SAdd::execute(store, args),
        "SISMEMBER" => SIsMember::execute(store, args),
        "SMISMEMBER" => SMIsMember::execute(store, args),
        "SCARD" => SCard::execute(store, args),
        "SMEMBERS" => SMembers::execute(store, args),
        "SUNION" => SUnion::execute(store, args),
        "SINTER" => SInter::execute(store, args),
        "SDIFF" => SDiff::execute(store, args),
        "SUNIONSTORE" => SUnionStore::execute(store, args),
        "SINTERSTORE" => SInterStore::execute(store, args),
        "SDIFFSTORE" => SDiffStore::execute(store, args),
        "SMOVE" => SMove::execute(store, args),
        "SREM" => SRem::execute(store, args),
        "SSCAN" => SScan::execute(store, args),
        "SRANDMEMBER" => SRandMember::execute(store, args),
        "SPOP" => SPop::execute(store, args),
        "ZADD" => ZAdd::execute(store, args),
        "ZSCORE" => ZScore::execute(store, args),
        "ZMSCORE" => ZMScore::execute(store, args),
        "ZCARD" => ZCard::execute(store, args),
        "ZUNION" => ZUnion::execute(store, args),
        "ZINTER" => ZInter::execute(store, args),
        "ZDIFF" => ZDiff::execute(store, args),
        "ZINTERCARD" => ZInterCard::execute(store, args),
        "ZRANDMEMBER" => ZRandMember::execute(store, args),
        "ZRANGE" => ZRange::execute(store, args),
        "ZRANGEBYSCORE" => ZRangeByScore::execute(store, args),
        "ZREVRANGE" => ZRevRange::execute(store, args),
        "ZREVRANGEBYSCORE" => ZRevRangeByScore::execute(store, args),
        "ZRANGEBYLEX" => ZRangeByLex::execute(store, args),
        "ZREVRANGEBYLEX" => ZRevRangeByLex::execute(store, args),
        "ZLEXCOUNT" => ZLexCount::execute(store, args),
        "ZINCRBY" => ZIncrBy::execute(store, args),
        "ZCOUNT" => ZCount::execute(store, args),
        "ZRANK" => ZRank::execute(store, args),
        "ZREVRANK" => ZRevRank::execute(store, args),
        "ZPOPMIN" => ZPopMin::execute(store, args),
        "ZPOPMAX" => ZPopMax::execute(store, args),
        "ZMPOP" => ZMPop::execute(store, args),
        "ZREM" => ZRem::execute(store, args),
        "ZREMRANGEBYRANK" => ZRemRangeByRank::execute(store, args),
        "ZREMRANGEBYSCORE" => ZRemRangeByScore::execute(store, args),
        "ZREMRANGEBYLEX" => ZRemRangeByLex::execute(store, args),
        "ZSCAN" => ZScan::execute(store, args),
        "ZRANGESTORE" => ZRangeStore::execute(store, args),
        "ZUNIONSTORE" => ZUnionStore::execute(store, args),
        "ZINTERSTORE" => ZInterStore::execute(store, args),
        "ZDIFFSTORE" => ZDiffStore::execute(store, args),
        "BZPOPMIN" => BZPopMin::execute(store, args),
        "BZPOPMAX" => BZPopMax::execute(store, args),
        "BZMPOP" => BZMPop::execute(store, args),
        "XACK" => XAck::execute(store, args),
        "XADD" => XAdd::execute(store, args),
        "XAUTOCLAIM" => XAutoClaim::execute(store, args),
        "XCLAIM" => XClaim::execute(store, args),
        "XDEL" => XDel::execute(store, args),
        "XGROUP" => XGroup::execute(store, args),
        "XINFO" => XInfo::execute(store, args),
        "XLEN" => XLen::execute(store, args),
        "XPENDING" => XPending::execute(store, args),
        "XRANGE" => XRange::execute(store, args),
        "XREAD" => XRead::execute(store, args),
        "XREADGROUP" => XReadGroup::execute(store, args),
        "XREVRANGE" => XRevRange::execute(store, args),
        "XSETID" => XSetId::execute(store, args),
        "XTRIM" => XTrim::execute(store, args),
        _ => error("ERR unsupported command"),
    }
}

pub(super) fn route_key_for_command<'a>(name: &str, args: &[&'a [u8]]) -> Option<&'a [u8]> {
    if command_has_no_route_key(name) {
        return None;
    }
    match name {
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" => {
            crate::commands::scripting::script_route_key(args)
        }
        #[cfg(feature = "redis-functions")]
        "FCALL" | "FCALL_RO" => function_route_key(args),
        _ => args.first().copied(),
    }
}

pub(super) fn route_key_for_owned_command<'a>(name: &str, args: &'a [Vec<u8>]) -> Option<&'a [u8]> {
    if command_has_no_route_key(name) {
        return None;
    }
    match name {
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" => {
            let numkeys = args
                .get(1)
                .and_then(|raw| std::str::from_utf8(raw).ok())
                .and_then(|raw| raw.parse::<usize>().ok())?;
            if numkeys == 0 {
                None
            } else {
                args.get(2).map(Vec::as_slice)
            }
        }
        #[cfg(feature = "redis-functions")]
        "FCALL" | "FCALL_RO" => {
            let numkeys = args
                .get(1)
                .and_then(|raw| std::str::from_utf8(raw).ok())
                .and_then(|raw| raw.parse::<usize>().ok())?;
            if numkeys == 0 {
                None
            } else {
                args.get(2).map(Vec::as_slice)
            }
        }
        _ => args.first().map(Vec::as_slice),
    }
}

#[cfg(feature = "redis-functions")]
fn function_route_key<'a>(args: &[&'a [u8]]) -> Option<&'a [u8]> {
    let numkeys = args
        .get(1)
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|raw| raw.parse::<usize>().ok())?;
    if numkeys == 0 {
        None
    } else {
        args.get(2).copied()
    }
}

fn command_has_no_route_key(name: &str) -> bool {
    matches!(
        name,
        "PING"
            | "AUTH"
            | "HELLO"
            | "SELECT"
            | "QUIT"
            | "ECHO"
            | "COMMAND"
            | "CONFIG"
            | "CLIENT"
            | "FUNCTION"
            | "DBSIZE"
            | "TIME"
            | "INFO"
            | "MEMORY"
            | "SCAN"
            | "RANDOMKEY"
            | "FLUSHDB"
            | "FLUSHALL"
            | "ASKING"
            | "BGREWRITEAOF"
            | "BGSAVE"
            | "CLUSTER"
            | "ACL"
            | "FAILOVER"
            | "RESET"
            | "STRALGO"
            | "DEBUG"
            | "HOST:"
            | "LASTSAVE"
            | "LATENCY"
            | "LOLWUT"
            | "MIGRATE"
            | "MODULE"
            | "MONITOR"
            | "POST"
            | "PSYNC"
            | "READONLY"
            | "READWRITE"
            | "REPLCONF"
            | "REPLICAOF"
            | "SLAVEOF"
            | "ROLE"
            | "SAVE"
            | "SHUTDOWN"
            | "SLOWLOG"
            | "SWAPDB"
            | "SYNC"
            | "WAIT"
            | "PUBLISH"
            | "SPUBLISH"
            | "PUBSUB"
            | "SUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "SSUBSCRIBE"
            | "SUNSUBSCRIBE"
            | "PFDEBUG"
            | "PFSELFTEST"
            | "XGROUP"
            | "XINFO"
            | "XREAD"
            | "XREADGROUP"
            | "SCRIPT"
    )
}
