use super::frame::*;
use super::parse::{eq_ignore_ascii_case, parse_u64};
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
        ReplicaOf, Role, Save, Shutdown, SlowLog, Sort, SortRo, SwapDb, Sync, Wait,
    },
    append::Append,
    array::{
        ArCount, ArDel, ArDelRange, ArGet, ArGetRange, ArGrep, ArInfo, ArInsert, ArLastItems,
        ArLen, ArMGet, ArMSet, ArNext, ArOp, ArRing, ArScan, ArSeek, ArSet,
    },
    auth::Auth,
    bitcount::BitCount,
    bitfield::BitField,
    bitfield_ro::BitFieldRo,
    bitop::BitOp,
    bitpos::BitPos,
    blmove::BLMove,
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
    copy::Copy,
    dbsize::DbSize,
    decr::Decr,
    decrby::DecrBy,
    dump::Dump,
    echo::Echo,
    expireat::ExpireAt,
    expiretime::ExpireTime,
    flush,
    geo::{
        GeoAdd, GeoDist, GeoHash, GeoPos, GeoRadius, GeoRadiusByMember, GeoRadiusByMemberRo,
        GeoRadiusRo, GeoSearch, GeoSearchStore,
    },
    getbit::GetBit,
    getdel::GetDel,
    getrange::GetRange,
    getset::GetSet,
    hdel::HDel,
    hello::Hello,
    hexists::HExists,
    hexpire::HExpire,
    hexpireat::HExpireAt,
    hexpiretime::HExpireTime,
    hget::HGet,
    hgetall::HGetAll,
    hgetdel::HGetDel,
    hgetex::HGetEx,
    hincrby::HIncrBy,
    hincrbyfloat::HIncrByFloat,
    hkeys::HKeys,
    hlen::HLen,
    hll::{PFAdd, PFCount, PFDebug, PFMerge, PFSelfTest},
    hmget::HMGet,
    hmset::HMSet,
    hotkeys::HotKeys,
    hpersist::HPersist,
    hpexpire::HPExpire,
    hpexpireat::HPExpireAt,
    hpexpiretime::HPExpireTime,
    hpttl::HPTtl,
    hrandfield::HRandField,
    hscan::HScan,
    hset::HSet,
    hsetex::HSetEx,
    hsetnx::HSetNx,
    hstrlen::HStrLen,
    httl::HTtl,
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
    lmove::LMove,
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
    object::Object,
    pexpireat::PExpireAt,
    pexpiretime::PExpireTime,
    ping::Ping,
    pubsub::{
        PSubscribe, PUnsubscribe, PubSub, Publish, SPublish, SSubscribe, SUnsubscribe, Subscribe,
        Unsubscribe,
    },
    quit::Quit,
    randomkey::RandomKey,
    rename::Rename,
    renamenx::RenameNx,
    reset::Reset,
    restore::Restore,
    rpop::RPop,
    rpoplpush::RPopLPush,
    rpush::RPush,
    rpushx::RPushX,
    sadd::SAdd,
    scan::Scan,
    scard::SCard,
    scripting::{Eval, EvalRo, EvalSha, EvalShaRo, Script},
    sdiff::SDiff,
    sdiffstore::SDiffStore,
    select::Select,
    setbit::SetBit,
    setnx::SetNx,
    setrange::SetRange,
    sinter::SInter,
    sintercard::SInterCard,
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
    string_v8::{Delex, Digest, IncrEx, MSetEx},
    strlen::StrLen,
    sunion::SUnion,
    sunionstore::SUnionStore,
    time::Time,
    touch::Touch,
    type_cmd::Type as TypeCommand,
    unlink::Unlink,
    vector_set::{
        VAdd, VCard, VDim, VEmb, VGetAttr, VInfo, VIsMember, VLinks, VRandMember, VRange, VRem,
        VSetAttr, VSim,
    },
    waitaof::WaitAof,
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
use crate::storage::{EmbeddedStore, now_millis};

pub(crate) fn dispatch(name: &str, store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if let Some(frame) = dispatch_native_embedded(name, store, args) {
        return frame;
    }

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
        "SORT_RO" => SortRo::execute(store, args),
        "SWAPDB" => SwapDb::execute(store, args),
        "SYNC" => Sync::execute(store, args),
        "WAIT" => Wait::execute(store, args),
        "WAITAOF" => WaitAof::execute(store, args),
        "ARCOUNT" => ArCount::execute(store, args),
        "ARDEL" => ArDel::execute(store, args),
        "ARDELRANGE" => ArDelRange::execute(store, args),
        "ARGET" => ArGet::execute(store, args),
        "ARGETRANGE" => ArGetRange::execute(store, args),
        "ARGREP" => ArGrep::execute(store, args),
        "ARINFO" => ArInfo::execute(store, args),
        "ARINSERT" => ArInsert::execute(store, args),
        "ARLASTITEMS" => ArLastItems::execute(store, args),
        "ARLEN" => ArLen::execute(store, args),
        "ARMGET" => ArMGet::execute(store, args),
        "ARMSET" => ArMSet::execute(store, args),
        "ARNEXT" => ArNext::execute(store, args),
        "AROP" => ArOp::execute(store, args),
        "ARRING" => ArRing::execute(store, args),
        "ARSCAN" => ArScan::execute(store, args),
        "ARSEEK" => ArSeek::execute(store, args),
        "ARSET" => ArSet::execute(store, args),
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
        "HOTKEYS" => HotKeys::execute(store, args),
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
        "OBJECT" => Object::execute(store, args),
        "TOUCH" => Touch::execute(store, args),
        "RANDOMKEY" => RandomKey::execute(store, args),
        "COPY" => Copy::execute(store, args),
        "RENAME" => Rename::execute(store, args),
        "RENAMENX" => RenameNx::execute(store, args),
        "UNLINK" => Unlink::execute(store, args),
        "DUMP" => Dump::execute(store, args),
        "RESTORE" | "RESTORE-ASKING" => Restore::execute(store, args),
        "EXPIREAT" => ExpireAt::execute(store, args),
        "PEXPIREAT" => PExpireAt::execute(store, args),
        "EXPIRETIME" => ExpireTime::execute(store, args),
        "PEXPIRETIME" => PExpireTime::execute(store, args),
        "APPEND" => Append::execute(store, args),
        "STRLEN" => StrLen::execute(store, args),
        "GETRANGE" | "SUBSTR" => GetRange::execute(store, args),
        "SETRANGE" => SetRange::execute(store, args),
        "GETBIT" => GetBit::execute(store, args),
        "SETBIT" => SetBit::execute(store, args),
        "BITCOUNT" => BitCount::execute(store, args),
        "BITPOS" => BitPos::execute(store, args),
        "BITOP" => BitOp::execute(store, args),
        "BITFIELD" => BitField::execute(store, args),
        "BITFIELD_RO" => BitFieldRo::execute(store, args),
        "GETSET" => GetSet::execute(store, args),
        "GETDEL" => GetDel::execute(store, args),
        "DELEX" => Delex::execute(store, args),
        "DIGEST" => Digest::execute(store, args),
        "LCS" => Lcs::execute(store, args),
        "STRALGO" => StrAlgo::execute(store, args),
        "INCR" => Incr::execute(store, args),
        "INCREX" => IncrEx::execute(store, args),
        "INCRBY" => IncrBy::execute(store, args),
        "DECR" => Decr::execute(store, args),
        "DECRBY" => DecrBy::execute(store, args),
        "INCRBYFLOAT" => IncrByFloat::execute(store, args),
        "MSET" => MSet::execute(store, args),
        "MSETEX" => MSetEx::execute(store, args),
        "MGET" => MGet::execute(store, args),
        "MSETNX" => MSetNx::execute(store, args),
        "SETNX" => SetNx::execute(store, args),
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
        "HGETDEL" => HGetDel::execute(store, args),
        "HGETEX" => HGetEx::execute(store, args),
        "HMSET" => HMSet::execute(store, args),
        "HMGET" => HMGet::execute(store, args),
        "HLEN" => HLen::execute(store, args),
        "HEXISTS" => HExists::execute(store, args),
        "HEXPIRE" => HExpire::execute(store, args),
        "HPEXPIRE" => HPExpire::execute(store, args),
        "HEXPIREAT" => HExpireAt::execute(store, args),
        "HPEXPIREAT" => HPExpireAt::execute(store, args),
        "HTTL" => HTtl::execute(store, args),
        "HPTTL" => HPTtl::execute(store, args),
        "HEXPIRETIME" => HExpireTime::execute(store, args),
        "HPEXPIRETIME" => HPExpireTime::execute(store, args),
        "HPERSIST" => HPersist::execute(store, args),
        "HSETEX" => HSetEx::execute(store, args),
        "HSETNX" => HSetNx::execute(store, args),
        "HSTRLEN" => HStrLen::execute(store, args),
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
        "RPOPLPUSH" => RPopLPush::execute(store, args),
        "LMOVE" => LMove::execute(store, args),
        "BLMOVE" => BLMove::execute(store, args),
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
        "SINTERCARD" => SInterCard::execute(store, args),
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
        "VADD" => VAdd::execute(store, args),
        "VCARD" => VCard::execute(store, args),
        "VDIM" => VDim::execute(store, args),
        "VEMB" => VEmb::execute(store, args),
        "VGETATTR" => VGetAttr::execute(store, args),
        "VINFO" => VInfo::execute(store, args),
        "VISMEMBER" => VIsMember::execute(store, args),
        "VLINKS" => VLinks::execute(store, args),
        "VRANDMEMBER" => VRandMember::execute(store, args),
        "VRANGE" => VRange::execute(store, args),
        "VREM" => VRem::execute(store, args),
        "VSETATTR" => VSetAttr::execute(store, args),
        "VSIM" => VSim::execute(store, args),
        _ => error("ERR unsupported command"),
    }
}

fn dispatch_native_embedded(name: &str, store: &EmbeddedStore, args: &[&[u8]]) -> Option<Frame> {
    match name {
        "GET" => Some(native_get(store, args)),
        "SET" => Some(native_set(store, args)),
        "SETEX" => Some(native_setex(store, args, 1_000, "SETEX")),
        "PSETEX" => Some(native_setex(store, args, 1, "PSETEX")),
        "GETEX" => Some(native_getex(store, args)),
        "DEL" => Some(native_del(store, args)),
        "EXISTS" => Some(native_exists(store, args)),
        "TTL" => Some(native_ttl(store, args, false, "TTL")),
        "PTTL" => Some(native_ttl(store, args, true, "PTTL")),
        "PERSIST" => Some(native_persist(store, args)),
        "EXPIRE" => Some(native_expire(store, args, 1_000, "EXPIRE")),
        "PEXPIRE" => Some(native_expire(store, args, 1, "PEXPIRE")),
        "MULTI" | "DISCARD" | "WATCH" | "UNWATCH" => Some(simple("OK")),
        "EXEC" => Some(Frame::Array(Vec::new())),
        _ => None,
    }
}

fn native_get(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key] => store.get(key).map_or(Frame::Null, Frame::BlobString),
        _ => wrong_arity("GET"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeSetCondition<'a> {
    Always,
    Nx,
    Xx,
    IfEq(&'a [u8]),
    IfNe(&'a [u8]),
    IfDigestEq(&'a [u8]),
    IfDigestNe(&'a [u8]),
}

#[derive(Debug, Clone, Copy)]
struct NativeSetOptions<'a> {
    ttl_ms: Option<u64>,
    condition: NativeSetCondition<'a>,
    keep_ttl: bool,
    get: bool,
}

fn native_set(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let [key, value, rest @ ..] = args else {
        return wrong_arity("SET");
    };
    let options = match parse_native_set_options(rest) {
        Ok(options) => options,
        Err(frame) => return frame,
    };
    let current = if options.get || options.condition.needs_string_value() {
        match optional_string_value(store, key, true) {
            Ok(value) => value,
            Err(frame) => return frame,
        }
    } else {
        None
    };
    let exists = current.is_some() || store.exists(key);
    if !options.condition.matches(exists, current.as_deref()) {
        return Frame::Null;
    }
    let ttl_ms = if options.keep_ttl {
        match store.pttl_millis(key) {
            ttl if ttl >= 0 => Some(ttl as u64),
            _ => None,
        }
    } else {
        options.ttl_ms
    };
    store.set(key.to_vec(), value.to_vec(), ttl_ms);
    if options.get {
        current.map_or(Frame::Null, Frame::BlobString)
    } else {
        simple("OK")
    }
}

impl NativeSetCondition<'_> {
    fn needs_string_value(self) -> bool {
        matches!(
            self,
            Self::IfEq(_) | Self::IfNe(_) | Self::IfDigestEq(_) | Self::IfDigestNe(_)
        )
    }

    fn matches(self, exists: bool, current: Option<&[u8]>) -> bool {
        match self {
            Self::Always => true,
            Self::Nx => !exists,
            Self::Xx => exists,
            Self::IfEq(expected) => current.is_some_and(|value| value == expected),
            Self::IfNe(expected) => current.is_none_or(|value| value != expected),
            Self::IfDigestEq(expected) => current
                .map(native_value_digest_hex)
                .is_some_and(|digest| digest.as_bytes().eq_ignore_ascii_case(expected)),
            Self::IfDigestNe(expected) => current
                .map(native_value_digest_hex)
                .is_none_or(|digest| !digest.as_bytes().eq_ignore_ascii_case(expected)),
        }
    }
}

fn parse_native_set_options<'a>(
    args: &'a [&'a [u8]],
) -> std::result::Result<NativeSetOptions<'a>, Frame> {
    let mut options = NativeSetOptions {
        ttl_ms: None,
        condition: NativeSetCondition::Always,
        keep_ttl: false,
        get: false,
    };
    let mut cursor = 0usize;
    while cursor < args.len() {
        let option = args[cursor];
        match option {
            option
                if eq_ignore_ascii_case(option, b"EX")
                    || eq_ignore_ascii_case(option, b"PX")
                    || eq_ignore_ascii_case(option, b"EXAT")
                    || eq_ignore_ascii_case(option, b"PXAT") =>
            {
                let Some(value) = args.get(cursor + 1) else {
                    return Err(error("ERR syntax error"));
                };
                let ttl = match parse_u64(value) {
                    Ok(ttl) => ttl,
                    Err(()) => {
                        return Err(error("ERR value is not an integer or out of range"));
                    }
                };
                options.ttl_ms = Some(match option {
                    option if eq_ignore_ascii_case(option, b"EX") => ttl.saturating_mul(1_000),
                    option if eq_ignore_ascii_case(option, b"PX") => ttl,
                    option if eq_ignore_ascii_case(option, b"EXAT") => {
                        ttl.saturating_mul(1_000).saturating_sub(now_millis())
                    }
                    _ => ttl.saturating_sub(now_millis()),
                });
                cursor += 2;
            }
            option if eq_ignore_ascii_case(option, b"NX") => {
                if !matches!(options.condition, NativeSetCondition::Always) {
                    return Err(error("ERR syntax error"));
                }
                options.condition = NativeSetCondition::Nx;
                cursor += 1;
            }
            option if eq_ignore_ascii_case(option, b"XX") => {
                if !matches!(options.condition, NativeSetCondition::Always) {
                    return Err(error("ERR syntax error"));
                }
                options.condition = NativeSetCondition::Xx;
                cursor += 1;
            }
            option
                if eq_ignore_ascii_case(option, b"IFEQ")
                    || eq_ignore_ascii_case(option, b"IFNE")
                    || eq_ignore_ascii_case(option, b"IFDEQ")
                    || eq_ignore_ascii_case(option, b"IFDNE") =>
            {
                if !matches!(options.condition, NativeSetCondition::Always) {
                    return Err(error("ERR syntax error"));
                }
                let Some(value) = args.get(cursor + 1) else {
                    return Err(error("ERR syntax error"));
                };
                options.condition = match option {
                    option if eq_ignore_ascii_case(option, b"IFEQ") => {
                        NativeSetCondition::IfEq(value)
                    }
                    option if eq_ignore_ascii_case(option, b"IFNE") => {
                        NativeSetCondition::IfNe(value)
                    }
                    option if eq_ignore_ascii_case(option, b"IFDEQ") => {
                        NativeSetCondition::IfDigestEq(value)
                    }
                    _ => NativeSetCondition::IfDigestNe(value),
                };
                cursor += 2;
            }
            option if eq_ignore_ascii_case(option, b"GET") => {
                options.get = true;
                cursor += 1;
            }
            option if eq_ignore_ascii_case(option, b"KEEPTTL") => {
                options.keep_ttl = true;
                cursor += 1;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    if options.keep_ttl && options.ttl_ms.is_some() {
        return Err(error("ERR syntax error"));
    }
    Ok(options)
}

fn native_value_digest_hex(value: &[u8]) -> String {
    format!("{:016x}", xxhash_rust::xxh3::xxh3_64(value))
}

fn native_setex(store: &EmbeddedStore, args: &[&[u8]], multiplier: u64, command: &str) -> Frame {
    let [key, ttl, value] = args else {
        return wrong_arity(command);
    };
    let ttl_ms = match parse_u64(ttl) {
        Ok(ttl) => ttl.saturating_mul(multiplier),
        Err(()) => return error("ERR value is not an integer or out of range"),
    };
    store.set(key.to_vec(), value.to_vec(), Some(ttl_ms));
    simple("OK")
}

fn native_getex(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let Some((key, expiration)) = parse_native_getex(args) else {
        return error("ERR syntax error");
    };
    let value = store.get(key);
    if value.is_some() {
        match expiration {
            NativeGetExExpiration::Keep => {}
            NativeGetExExpiration::Persist => {
                store.persist(key);
            }
            NativeGetExExpiration::ExpireIn(ttl_ms) => {
                store.expire(key, now_millis().saturating_add(ttl_ms));
            }
        }
    }
    value.map_or(Frame::Null, Frame::BlobString)
}

#[derive(Debug, Clone, Copy)]
enum NativeGetExExpiration {
    Keep,
    Persist,
    ExpireIn(u64),
}

fn parse_native_getex<'a>(args: &'a [&'a [u8]]) -> Option<(&'a [u8], NativeGetExExpiration)> {
    match args {
        [key] => Some((key, NativeGetExExpiration::Keep)),
        [key, option] if eq_ignore_ascii_case(option, b"PERSIST") => {
            Some((key, NativeGetExExpiration::Persist))
        }
        [key, option, value] if eq_ignore_ascii_case(option, b"EX") => {
            parse_u64(value).ok().map(|ttl| {
                (
                    *key,
                    NativeGetExExpiration::ExpireIn(ttl.saturating_mul(1_000)),
                )
            })
        }
        [key, option, value] if eq_ignore_ascii_case(option, b"PX") => parse_u64(value)
            .ok()
            .map(|ttl| (*key, NativeGetExExpiration::ExpireIn(ttl))),
        [key, option, value] if eq_ignore_ascii_case(option, b"EXAT") => parse_u64(value)
            .ok()
            .map(|ttl| ttl.saturating_mul(1_000).saturating_sub(now_millis()))
            .map(|ttl| (*key, NativeGetExExpiration::ExpireIn(ttl))),
        [key, option, value] if eq_ignore_ascii_case(option, b"PXAT") => parse_u64(value)
            .ok()
            .map(|ttl| ttl.saturating_sub(now_millis()))
            .map(|ttl| (*key, NativeGetExExpiration::ExpireIn(ttl))),
        _ => None,
    }
}

fn native_del(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.is_empty() {
        return wrong_arity("DEL");
    }
    int(args.iter().filter(|key| store.delete(key)).count() as i64)
}

fn native_exists(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    if args.is_empty() {
        return wrong_arity("EXISTS");
    }
    int(args.iter().filter(|key| store.exists(key)).count() as i64)
}

fn native_ttl(store: &EmbeddedStore, args: &[&[u8]], millis: bool, command: &str) -> Frame {
    match args {
        [key] if millis => int(store.pttl_millis(key)),
        [key] => int(store.ttl_seconds(key)),
        _ => wrong_arity(command),
    }
}

fn native_persist(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [key] => int(store.persist(key) as i64),
        _ => wrong_arity("PERSIST"),
    }
}

fn native_expire(store: &EmbeddedStore, args: &[&[u8]], multiplier: u64, command: &str) -> Frame {
    let [key, ttl, options @ ..] = args else {
        return wrong_arity(command);
    };
    if options.len() > 1 {
        return wrong_arity(command);
    }
    let ttl_ms = match parse_u64(ttl) {
        Ok(ttl) => ttl.saturating_mul(multiplier),
        Err(()) => return error("ERR value is not an integer or out of range"),
    };
    let condition = match crate::commands::expire::parse_expire_condition_frame(options) {
        Ok(condition) => condition,
        Err(frame) => return frame,
    };
    int(crate::commands::expire::expire_at_changed(
        store,
        key,
        crate::commands::expire::relative_expire_at_ms(ttl_ms),
        condition,
    ))
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
            | "HOTKEYS"
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
