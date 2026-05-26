use crate::{FastCacheError, Result};

/// Native fast-cache protocol, version 2.
///
/// Requests use an 8-byte raw-binary header:
/// `[magic:u8, version:u8, command:u8, flags:u8, body_len:u32le]`.
/// Optional routing fields live at the start of the body when their flag bits
/// are set, followed by command-specific binary fields. Field order is:
/// `key_hash:u64`, `route_shard:u32`, `key_tag:u64`. The Redis-command flag
/// switches the command body to compact Redis argument encoding so command
/// opcodes can coexist with older typed FCNP opcodes. RESP remains supported
/// separately by the server ingress path.
pub const FAST_REQUEST_MAGIC: u8 = 0xFA;
pub const FAST_RESPONSE_MAGIC: u8 = 0xFB;
pub const FAST_PROTOCOL_VERSION: u8 = 2;

pub const FAST_FLAG_KEY_HASH: u8 = 0x01;
pub const FAST_FLAG_ROUTE_SHARD: u8 = 0x02;
pub const FAST_FLAG_KEY_TAG: u8 = 0x04;
pub const FAST_FLAG_REDIS_COMMAND_ARGS: u8 = 0x08;

const REQUEST_HEADER_LEN: usize = 8;
const RESPONSE_HEADER_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FastCommandKind {
    Get = 1,
    Set = 2,
    SetEx = 3,
    GetEx = 4,
    Delete = 5,
    Exists = 6,
    Ttl = 7,
    Expire = 8,
    Ping = 9,
    MGet = 10,
    MSet = 11,
    Append = 80,
    BitCount = 81,
    BitField = 82,
    BitOp = 83,
    BitPos = 84,
    Decr = 85,
    DecrBy = 86,
    GetBit = 87,
    GetDel = 88,
    GetRange = 89,
    GetSet = 90,
    Incr = 91,
    IncrBy = 92,
    IncrByFloat = 93,
    MSetNx = 94,
    PSetEx = 95,
    SetBit = 96,
    SetNx = 97,
    SetRange = 98,
    StrLen = 99,
    Auth = 60,
    Hello = 61,
    Select = 62,
    Quit = 63,
    Echo = 64,
    Info = 65,
    Command = 66,
    CommandDocs = 67,
    ConfigGet = 68,
    DbSize = 69,
    Time = 70,
    ClientGetName = 71,
    ClientSetName = 72,
    ClientId = 73,
    ClientList = 74,
    ClientKill = 75,
    Config = 76,
    Client = 77,
    FlushAll = 78,
    FlushDb = 79,
    HSet = 20,
    HGet = 21,
    HDel = 22,
    HLen = 23,
    HMGet = 24,
    HExists = 140,
    HGetAll = 141,
    HIncrBy = 142,
    HIncrByFloat = 143,
    HKeys = 144,
    HMSet = 145,
    HRandField = 146,
    HScan = 147,
    HSetNx = 148,
    HStrLen = 149,
    HVals = 150,
    LPush = 30,
    RPush = 31,
    LPop = 32,
    RPop = 33,
    LLen = 34,
    LIndex = 35,
    LRange = 36,
    BLMove = 160,
    BLMPop = 161,
    BLPop = 162,
    BRPop = 163,
    LInsert = 164,
    LMove = 165,
    LMPop = 166,
    LPushX = 167,
    LRem = 168,
    LSet = 169,
    LTrim = 170,
    RPopLPush = 171,
    RPushX = 172,
    BRPopLPush = 173,
    SAdd = 40,
    SRem = 41,
    SIsMember = 42,
    SCard = 43,
    SMembers = 44,
    SDiff = 180,
    SDiffStore = 181,
    SInter = 182,
    SInterStore = 183,
    SMIsMember = 184,
    SMove = 185,
    SPop = 186,
    SRandMember = 187,
    SScan = 188,
    SUnion = 189,
    SUnionStore = 190,
    ZAdd = 50,
    ZRem = 51,
    ZScore = 52,
    ZCard = 53,
    ZRange = 54,
    Copy = 100,
    Dump = 101,
    ExpireAt = 102,
    ExpireTime = 103,
    Keys = 104,
    Memory = 105,
    Object = 106,
    Persist = 107,
    PExpire = 108,
    PExpireAt = 109,
    PExpireTime = 110,
    PTtl = 111,
    RandomKey = 112,
    Rename = 113,
    RenameNx = 114,
    Restore = 115,
    Scan = 116,
    Touch = 117,
    Type = 118,
    Unlink = 119,
    Multi = 120,
    Exec = 121,
    Discard = 122,
    Watch = 123,
    Unwatch = 124,
    RestoreAsking = 125,
    Substr = 126,
    RespCommand = 200,
    BZMPop = 201,
    BZPopMax = 202,
    BZPopMin = 203,
    ZCount = 204,
    ZDiff = 205,
    ZDiffStore = 206,
    ZIncrBy = 207,
    ZInter = 208,
    ZInterCard = 209,
    ZInterStore = 210,
    ZLexCount = 211,
    ZMPop = 212,
    ZMScore = 213,
    ZPopMax = 214,
    ZPopMin = 215,
    ZRandMember = 216,
    ZRangeByLex = 217,
    ZRangeByScore = 218,
    ZRangeStore = 219,
    ZRank = 220,
    ZRemRangeByLex = 221,
    ZRemRangeByRank = 222,
    ZRemRangeByScore = 223,
    ZRevRange = 224,
    ZRevRangeByLex = 225,
    ZRevRangeByScore = 226,
    ZRevRank = 227,
    ZScan = 228,
    ZUnion = 229,
    ZUnionStore = 230,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FastRedisCommandOpcode {
    pub name: &'static str,
    pub kind: FastCommandKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FastRedisRouteKeys<'a> {
    None,
    AllShards,
    Keys(Vec<&'a [u8]>),
}

impl<'a> FastRedisRouteKeys<'a> {
    pub fn first_key(&self) -> Option<&'a [u8]> {
        match self {
            Self::Keys(keys) => keys.first().copied(),
            Self::None | Self::AllShards => None,
        }
    }

    pub fn has_keys(&self) -> bool {
        matches!(self, Self::Keys(keys) if !keys.is_empty())
    }
}

pub const FAST_REDIS_COMMAND_OPCODES: &[FastRedisCommandOpcode] = &[
    FastRedisCommandOpcode {
        name: "APPEND",
        kind: FastCommandKind::Append,
    },
    FastRedisCommandOpcode {
        name: "AUTH",
        kind: FastCommandKind::Auth,
    },
    FastRedisCommandOpcode {
        name: "BITCOUNT",
        kind: FastCommandKind::BitCount,
    },
    FastRedisCommandOpcode {
        name: "BITFIELD",
        kind: FastCommandKind::BitField,
    },
    FastRedisCommandOpcode {
        name: "BITOP",
        kind: FastCommandKind::BitOp,
    },
    FastRedisCommandOpcode {
        name: "BITPOS",
        kind: FastCommandKind::BitPos,
    },
    FastRedisCommandOpcode {
        name: "BLMOVE",
        kind: FastCommandKind::BLMove,
    },
    FastRedisCommandOpcode {
        name: "BLMPOP",
        kind: FastCommandKind::BLMPop,
    },
    FastRedisCommandOpcode {
        name: "BLPOP",
        kind: FastCommandKind::BLPop,
    },
    FastRedisCommandOpcode {
        name: "BRPOP",
        kind: FastCommandKind::BRPop,
    },
    FastRedisCommandOpcode {
        name: "BRPOPLPUSH",
        kind: FastCommandKind::BRPopLPush,
    },
    FastRedisCommandOpcode {
        name: "BZMPOP",
        kind: FastCommandKind::BZMPop,
    },
    FastRedisCommandOpcode {
        name: "BZPOPMAX",
        kind: FastCommandKind::BZPopMax,
    },
    FastRedisCommandOpcode {
        name: "BZPOPMIN",
        kind: FastCommandKind::BZPopMin,
    },
    FastRedisCommandOpcode {
        name: "CLIENT",
        kind: FastCommandKind::Client,
    },
    FastRedisCommandOpcode {
        name: "COMMAND",
        kind: FastCommandKind::Command,
    },
    FastRedisCommandOpcode {
        name: "CONFIG",
        kind: FastCommandKind::Config,
    },
    FastRedisCommandOpcode {
        name: "COPY",
        kind: FastCommandKind::Copy,
    },
    FastRedisCommandOpcode {
        name: "DBSIZE",
        kind: FastCommandKind::DbSize,
    },
    FastRedisCommandOpcode {
        name: "DECR",
        kind: FastCommandKind::Decr,
    },
    FastRedisCommandOpcode {
        name: "DECRBY",
        kind: FastCommandKind::DecrBy,
    },
    FastRedisCommandOpcode {
        name: "DEL",
        kind: FastCommandKind::Delete,
    },
    FastRedisCommandOpcode {
        name: "DISCARD",
        kind: FastCommandKind::Discard,
    },
    FastRedisCommandOpcode {
        name: "DUMP",
        kind: FastCommandKind::Dump,
    },
    FastRedisCommandOpcode {
        name: "ECHO",
        kind: FastCommandKind::Echo,
    },
    FastRedisCommandOpcode {
        name: "EXEC",
        kind: FastCommandKind::Exec,
    },
    FastRedisCommandOpcode {
        name: "EXISTS",
        kind: FastCommandKind::Exists,
    },
    FastRedisCommandOpcode {
        name: "EXPIRE",
        kind: FastCommandKind::Expire,
    },
    FastRedisCommandOpcode {
        name: "EXPIREAT",
        kind: FastCommandKind::ExpireAt,
    },
    FastRedisCommandOpcode {
        name: "EXPIRETIME",
        kind: FastCommandKind::ExpireTime,
    },
    FastRedisCommandOpcode {
        name: "FLUSHALL",
        kind: FastCommandKind::FlushAll,
    },
    FastRedisCommandOpcode {
        name: "FLUSHDB",
        kind: FastCommandKind::FlushDb,
    },
    FastRedisCommandOpcode {
        name: "GET",
        kind: FastCommandKind::Get,
    },
    FastRedisCommandOpcode {
        name: "GETBIT",
        kind: FastCommandKind::GetBit,
    },
    FastRedisCommandOpcode {
        name: "GETDEL",
        kind: FastCommandKind::GetDel,
    },
    FastRedisCommandOpcode {
        name: "GETEX",
        kind: FastCommandKind::GetEx,
    },
    FastRedisCommandOpcode {
        name: "GETRANGE",
        kind: FastCommandKind::GetRange,
    },
    FastRedisCommandOpcode {
        name: "SUBSTR",
        kind: FastCommandKind::Substr,
    },
    FastRedisCommandOpcode {
        name: "GETSET",
        kind: FastCommandKind::GetSet,
    },
    FastRedisCommandOpcode {
        name: "HELLO",
        kind: FastCommandKind::Hello,
    },
    FastRedisCommandOpcode {
        name: "HDEL",
        kind: FastCommandKind::HDel,
    },
    FastRedisCommandOpcode {
        name: "HEXISTS",
        kind: FastCommandKind::HExists,
    },
    FastRedisCommandOpcode {
        name: "HGET",
        kind: FastCommandKind::HGet,
    },
    FastRedisCommandOpcode {
        name: "HGETALL",
        kind: FastCommandKind::HGetAll,
    },
    FastRedisCommandOpcode {
        name: "HINCRBY",
        kind: FastCommandKind::HIncrBy,
    },
    FastRedisCommandOpcode {
        name: "HINCRBYFLOAT",
        kind: FastCommandKind::HIncrByFloat,
    },
    FastRedisCommandOpcode {
        name: "HKEYS",
        kind: FastCommandKind::HKeys,
    },
    FastRedisCommandOpcode {
        name: "HLEN",
        kind: FastCommandKind::HLen,
    },
    FastRedisCommandOpcode {
        name: "HMGET",
        kind: FastCommandKind::HMGet,
    },
    FastRedisCommandOpcode {
        name: "HMSET",
        kind: FastCommandKind::HMSet,
    },
    FastRedisCommandOpcode {
        name: "HRANDFIELD",
        kind: FastCommandKind::HRandField,
    },
    FastRedisCommandOpcode {
        name: "HSCAN",
        kind: FastCommandKind::HScan,
    },
    FastRedisCommandOpcode {
        name: "HSET",
        kind: FastCommandKind::HSet,
    },
    FastRedisCommandOpcode {
        name: "HSETNX",
        kind: FastCommandKind::HSetNx,
    },
    FastRedisCommandOpcode {
        name: "HSTRLEN",
        kind: FastCommandKind::HStrLen,
    },
    FastRedisCommandOpcode {
        name: "HVALS",
        kind: FastCommandKind::HVals,
    },
    FastRedisCommandOpcode {
        name: "INFO",
        kind: FastCommandKind::Info,
    },
    FastRedisCommandOpcode {
        name: "INCR",
        kind: FastCommandKind::Incr,
    },
    FastRedisCommandOpcode {
        name: "INCRBY",
        kind: FastCommandKind::IncrBy,
    },
    FastRedisCommandOpcode {
        name: "INCRBYFLOAT",
        kind: FastCommandKind::IncrByFloat,
    },
    FastRedisCommandOpcode {
        name: "KEYS",
        kind: FastCommandKind::Keys,
    },
    FastRedisCommandOpcode {
        name: "LINDEX",
        kind: FastCommandKind::LIndex,
    },
    FastRedisCommandOpcode {
        name: "LINSERT",
        kind: FastCommandKind::LInsert,
    },
    FastRedisCommandOpcode {
        name: "LLEN",
        kind: FastCommandKind::LLen,
    },
    FastRedisCommandOpcode {
        name: "LMOVE",
        kind: FastCommandKind::LMove,
    },
    FastRedisCommandOpcode {
        name: "LMPOP",
        kind: FastCommandKind::LMPop,
    },
    FastRedisCommandOpcode {
        name: "LPOP",
        kind: FastCommandKind::LPop,
    },
    FastRedisCommandOpcode {
        name: "LPUSH",
        kind: FastCommandKind::LPush,
    },
    FastRedisCommandOpcode {
        name: "LPUSHX",
        kind: FastCommandKind::LPushX,
    },
    FastRedisCommandOpcode {
        name: "LRANGE",
        kind: FastCommandKind::LRange,
    },
    FastRedisCommandOpcode {
        name: "LREM",
        kind: FastCommandKind::LRem,
    },
    FastRedisCommandOpcode {
        name: "LSET",
        kind: FastCommandKind::LSet,
    },
    FastRedisCommandOpcode {
        name: "LTRIM",
        kind: FastCommandKind::LTrim,
    },
    FastRedisCommandOpcode {
        name: "MEMORY",
        kind: FastCommandKind::Memory,
    },
    FastRedisCommandOpcode {
        name: "MGET",
        kind: FastCommandKind::MGet,
    },
    FastRedisCommandOpcode {
        name: "MSET",
        kind: FastCommandKind::MSet,
    },
    FastRedisCommandOpcode {
        name: "MSETNX",
        kind: FastCommandKind::MSetNx,
    },
    FastRedisCommandOpcode {
        name: "MULTI",
        kind: FastCommandKind::Multi,
    },
    FastRedisCommandOpcode {
        name: "OBJECT",
        kind: FastCommandKind::Object,
    },
    FastRedisCommandOpcode {
        name: "PERSIST",
        kind: FastCommandKind::Persist,
    },
    FastRedisCommandOpcode {
        name: "PEXPIRE",
        kind: FastCommandKind::PExpire,
    },
    FastRedisCommandOpcode {
        name: "PEXPIREAT",
        kind: FastCommandKind::PExpireAt,
    },
    FastRedisCommandOpcode {
        name: "PEXPIRETIME",
        kind: FastCommandKind::PExpireTime,
    },
    FastRedisCommandOpcode {
        name: "PING",
        kind: FastCommandKind::Ping,
    },
    FastRedisCommandOpcode {
        name: "PSETEX",
        kind: FastCommandKind::PSetEx,
    },
    FastRedisCommandOpcode {
        name: "PTTL",
        kind: FastCommandKind::PTtl,
    },
    FastRedisCommandOpcode {
        name: "QUIT",
        kind: FastCommandKind::Quit,
    },
    FastRedisCommandOpcode {
        name: "RANDOMKEY",
        kind: FastCommandKind::RandomKey,
    },
    FastRedisCommandOpcode {
        name: "RENAME",
        kind: FastCommandKind::Rename,
    },
    FastRedisCommandOpcode {
        name: "RENAMENX",
        kind: FastCommandKind::RenameNx,
    },
    FastRedisCommandOpcode {
        name: "RESTORE",
        kind: FastCommandKind::Restore,
    },
    FastRedisCommandOpcode {
        name: "RESTORE-ASKING",
        kind: FastCommandKind::RestoreAsking,
    },
    FastRedisCommandOpcode {
        name: "RPOP",
        kind: FastCommandKind::RPop,
    },
    FastRedisCommandOpcode {
        name: "RPOPLPUSH",
        kind: FastCommandKind::RPopLPush,
    },
    FastRedisCommandOpcode {
        name: "RPUSH",
        kind: FastCommandKind::RPush,
    },
    FastRedisCommandOpcode {
        name: "RPUSHX",
        kind: FastCommandKind::RPushX,
    },
    FastRedisCommandOpcode {
        name: "SADD",
        kind: FastCommandKind::SAdd,
    },
    FastRedisCommandOpcode {
        name: "SCAN",
        kind: FastCommandKind::Scan,
    },
    FastRedisCommandOpcode {
        name: "SCARD",
        kind: FastCommandKind::SCard,
    },
    FastRedisCommandOpcode {
        name: "SDIFF",
        kind: FastCommandKind::SDiff,
    },
    FastRedisCommandOpcode {
        name: "SDIFFSTORE",
        kind: FastCommandKind::SDiffStore,
    },
    FastRedisCommandOpcode {
        name: "SELECT",
        kind: FastCommandKind::Select,
    },
    FastRedisCommandOpcode {
        name: "SET",
        kind: FastCommandKind::Set,
    },
    FastRedisCommandOpcode {
        name: "SETBIT",
        kind: FastCommandKind::SetBit,
    },
    FastRedisCommandOpcode {
        name: "SETEX",
        kind: FastCommandKind::SetEx,
    },
    FastRedisCommandOpcode {
        name: "SETNX",
        kind: FastCommandKind::SetNx,
    },
    FastRedisCommandOpcode {
        name: "SETRANGE",
        kind: FastCommandKind::SetRange,
    },
    FastRedisCommandOpcode {
        name: "SINTER",
        kind: FastCommandKind::SInter,
    },
    FastRedisCommandOpcode {
        name: "SINTERSTORE",
        kind: FastCommandKind::SInterStore,
    },
    FastRedisCommandOpcode {
        name: "SISMEMBER",
        kind: FastCommandKind::SIsMember,
    },
    FastRedisCommandOpcode {
        name: "SMEMBERS",
        kind: FastCommandKind::SMembers,
    },
    FastRedisCommandOpcode {
        name: "SMISMEMBER",
        kind: FastCommandKind::SMIsMember,
    },
    FastRedisCommandOpcode {
        name: "SMOVE",
        kind: FastCommandKind::SMove,
    },
    FastRedisCommandOpcode {
        name: "SPOP",
        kind: FastCommandKind::SPop,
    },
    FastRedisCommandOpcode {
        name: "SRANDMEMBER",
        kind: FastCommandKind::SRandMember,
    },
    FastRedisCommandOpcode {
        name: "SREM",
        kind: FastCommandKind::SRem,
    },
    FastRedisCommandOpcode {
        name: "SSCAN",
        kind: FastCommandKind::SScan,
    },
    FastRedisCommandOpcode {
        name: "STRLEN",
        kind: FastCommandKind::StrLen,
    },
    FastRedisCommandOpcode {
        name: "SUNION",
        kind: FastCommandKind::SUnion,
    },
    FastRedisCommandOpcode {
        name: "SUNIONSTORE",
        kind: FastCommandKind::SUnionStore,
    },
    FastRedisCommandOpcode {
        name: "TIME",
        kind: FastCommandKind::Time,
    },
    FastRedisCommandOpcode {
        name: "TOUCH",
        kind: FastCommandKind::Touch,
    },
    FastRedisCommandOpcode {
        name: "TTL",
        kind: FastCommandKind::Ttl,
    },
    FastRedisCommandOpcode {
        name: "TYPE",
        kind: FastCommandKind::Type,
    },
    FastRedisCommandOpcode {
        name: "UNLINK",
        kind: FastCommandKind::Unlink,
    },
    FastRedisCommandOpcode {
        name: "UNWATCH",
        kind: FastCommandKind::Unwatch,
    },
    FastRedisCommandOpcode {
        name: "WATCH",
        kind: FastCommandKind::Watch,
    },
    FastRedisCommandOpcode {
        name: "ZADD",
        kind: FastCommandKind::ZAdd,
    },
    FastRedisCommandOpcode {
        name: "ZCARD",
        kind: FastCommandKind::ZCard,
    },
    FastRedisCommandOpcode {
        name: "ZCOUNT",
        kind: FastCommandKind::ZCount,
    },
    FastRedisCommandOpcode {
        name: "ZDIFF",
        kind: FastCommandKind::ZDiff,
    },
    FastRedisCommandOpcode {
        name: "ZDIFFSTORE",
        kind: FastCommandKind::ZDiffStore,
    },
    FastRedisCommandOpcode {
        name: "ZINCRBY",
        kind: FastCommandKind::ZIncrBy,
    },
    FastRedisCommandOpcode {
        name: "ZINTER",
        kind: FastCommandKind::ZInter,
    },
    FastRedisCommandOpcode {
        name: "ZINTERCARD",
        kind: FastCommandKind::ZInterCard,
    },
    FastRedisCommandOpcode {
        name: "ZINTERSTORE",
        kind: FastCommandKind::ZInterStore,
    },
    FastRedisCommandOpcode {
        name: "ZLEXCOUNT",
        kind: FastCommandKind::ZLexCount,
    },
    FastRedisCommandOpcode {
        name: "ZMPOP",
        kind: FastCommandKind::ZMPop,
    },
    FastRedisCommandOpcode {
        name: "ZMSCORE",
        kind: FastCommandKind::ZMScore,
    },
    FastRedisCommandOpcode {
        name: "ZPOPMAX",
        kind: FastCommandKind::ZPopMax,
    },
    FastRedisCommandOpcode {
        name: "ZPOPMIN",
        kind: FastCommandKind::ZPopMin,
    },
    FastRedisCommandOpcode {
        name: "ZRANDMEMBER",
        kind: FastCommandKind::ZRandMember,
    },
    FastRedisCommandOpcode {
        name: "ZRANGE",
        kind: FastCommandKind::ZRange,
    },
    FastRedisCommandOpcode {
        name: "ZRANGEBYLEX",
        kind: FastCommandKind::ZRangeByLex,
    },
    FastRedisCommandOpcode {
        name: "ZRANGEBYSCORE",
        kind: FastCommandKind::ZRangeByScore,
    },
    FastRedisCommandOpcode {
        name: "ZRANGESTORE",
        kind: FastCommandKind::ZRangeStore,
    },
    FastRedisCommandOpcode {
        name: "ZRANK",
        kind: FastCommandKind::ZRank,
    },
    FastRedisCommandOpcode {
        name: "ZREM",
        kind: FastCommandKind::ZRem,
    },
    FastRedisCommandOpcode {
        name: "ZREMRANGEBYLEX",
        kind: FastCommandKind::ZRemRangeByLex,
    },
    FastRedisCommandOpcode {
        name: "ZREMRANGEBYRANK",
        kind: FastCommandKind::ZRemRangeByRank,
    },
    FastRedisCommandOpcode {
        name: "ZREMRANGEBYSCORE",
        kind: FastCommandKind::ZRemRangeByScore,
    },
    FastRedisCommandOpcode {
        name: "ZREVRANGE",
        kind: FastCommandKind::ZRevRange,
    },
    FastRedisCommandOpcode {
        name: "ZREVRANGEBYLEX",
        kind: FastCommandKind::ZRevRangeByLex,
    },
    FastRedisCommandOpcode {
        name: "ZREVRANGEBYSCORE",
        kind: FastCommandKind::ZRevRangeByScore,
    },
    FastRedisCommandOpcode {
        name: "ZREVRANK",
        kind: FastCommandKind::ZRevRank,
    },
    FastRedisCommandOpcode {
        name: "ZSCAN",
        kind: FastCommandKind::ZScan,
    },
    FastRedisCommandOpcode {
        name: "ZSCORE",
        kind: FastCommandKind::ZScore,
    },
    FastRedisCommandOpcode {
        name: "ZUNION",
        kind: FastCommandKind::ZUnion,
    },
    FastRedisCommandOpcode {
        name: "ZUNIONSTORE",
        kind: FastCommandKind::ZUnionStore,
    },
];

impl FastCommandKind {
    fn from_u8(value: u8) -> Result<Self> {
        Self::from_opcode(value).ok_or_else(|| {
            FastCacheError::Protocol(format!("unsupported fast command id: {value}"))
        })
    }

    pub fn from_opcode(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Get),
            2 => Some(Self::Set),
            3 => Some(Self::SetEx),
            4 => Some(Self::GetEx),
            5 => Some(Self::Delete),
            6 => Some(Self::Exists),
            7 => Some(Self::Ttl),
            8 => Some(Self::Expire),
            9 => Some(Self::Ping),
            10 => Some(Self::MGet),
            11 => Some(Self::MSet),
            20 => Some(Self::HSet),
            21 => Some(Self::HGet),
            22 => Some(Self::HDel),
            23 => Some(Self::HLen),
            24 => Some(Self::HMGet),
            30 => Some(Self::LPush),
            31 => Some(Self::RPush),
            32 => Some(Self::LPop),
            33 => Some(Self::RPop),
            34 => Some(Self::LLen),
            35 => Some(Self::LIndex),
            36 => Some(Self::LRange),
            40 => Some(Self::SAdd),
            41 => Some(Self::SRem),
            42 => Some(Self::SIsMember),
            43 => Some(Self::SCard),
            44 => Some(Self::SMembers),
            50 => Some(Self::ZAdd),
            51 => Some(Self::ZRem),
            52 => Some(Self::ZScore),
            53 => Some(Self::ZCard),
            54 => Some(Self::ZRange),
            60 => Some(Self::Auth),
            61 => Some(Self::Hello),
            62 => Some(Self::Select),
            63 => Some(Self::Quit),
            64 => Some(Self::Echo),
            65 => Some(Self::Info),
            66 => Some(Self::Command),
            67 => Some(Self::CommandDocs),
            68 => Some(Self::ConfigGet),
            69 => Some(Self::DbSize),
            70 => Some(Self::Time),
            71 => Some(Self::ClientGetName),
            72 => Some(Self::ClientSetName),
            73 => Some(Self::ClientId),
            74 => Some(Self::ClientList),
            75 => Some(Self::ClientKill),
            76 => Some(Self::Config),
            77 => Some(Self::Client),
            78 => Some(Self::FlushAll),
            79 => Some(Self::FlushDb),
            80 => Some(Self::Append),
            81 => Some(Self::BitCount),
            82 => Some(Self::BitField),
            83 => Some(Self::BitOp),
            84 => Some(Self::BitPos),
            85 => Some(Self::Decr),
            86 => Some(Self::DecrBy),
            87 => Some(Self::GetBit),
            88 => Some(Self::GetDel),
            89 => Some(Self::GetRange),
            90 => Some(Self::GetSet),
            91 => Some(Self::Incr),
            92 => Some(Self::IncrBy),
            93 => Some(Self::IncrByFloat),
            94 => Some(Self::MSetNx),
            95 => Some(Self::PSetEx),
            96 => Some(Self::SetBit),
            97 => Some(Self::SetNx),
            98 => Some(Self::SetRange),
            99 => Some(Self::StrLen),
            100 => Some(Self::Copy),
            101 => Some(Self::Dump),
            102 => Some(Self::ExpireAt),
            103 => Some(Self::ExpireTime),
            104 => Some(Self::Keys),
            105 => Some(Self::Memory),
            106 => Some(Self::Object),
            107 => Some(Self::Persist),
            108 => Some(Self::PExpire),
            109 => Some(Self::PExpireAt),
            110 => Some(Self::PExpireTime),
            111 => Some(Self::PTtl),
            112 => Some(Self::RandomKey),
            113 => Some(Self::Rename),
            114 => Some(Self::RenameNx),
            115 => Some(Self::Restore),
            116 => Some(Self::Scan),
            117 => Some(Self::Touch),
            118 => Some(Self::Type),
            119 => Some(Self::Unlink),
            120 => Some(Self::Multi),
            121 => Some(Self::Exec),
            122 => Some(Self::Discard),
            123 => Some(Self::Watch),
            124 => Some(Self::Unwatch),
            125 => Some(Self::RestoreAsking),
            126 => Some(Self::Substr),
            140 => Some(Self::HExists),
            141 => Some(Self::HGetAll),
            142 => Some(Self::HIncrBy),
            143 => Some(Self::HIncrByFloat),
            144 => Some(Self::HKeys),
            145 => Some(Self::HMSet),
            146 => Some(Self::HRandField),
            147 => Some(Self::HScan),
            148 => Some(Self::HSetNx),
            149 => Some(Self::HStrLen),
            150 => Some(Self::HVals),
            160 => Some(Self::BLMove),
            161 => Some(Self::BLMPop),
            162 => Some(Self::BLPop),
            163 => Some(Self::BRPop),
            164 => Some(Self::LInsert),
            165 => Some(Self::LMove),
            166 => Some(Self::LMPop),
            167 => Some(Self::LPushX),
            168 => Some(Self::LRem),
            169 => Some(Self::LSet),
            170 => Some(Self::LTrim),
            171 => Some(Self::RPopLPush),
            172 => Some(Self::RPushX),
            173 => Some(Self::BRPopLPush),
            180 => Some(Self::SDiff),
            181 => Some(Self::SDiffStore),
            182 => Some(Self::SInter),
            183 => Some(Self::SInterStore),
            184 => Some(Self::SMIsMember),
            185 => Some(Self::SMove),
            186 => Some(Self::SPop),
            187 => Some(Self::SRandMember),
            188 => Some(Self::SScan),
            189 => Some(Self::SUnion),
            190 => Some(Self::SUnionStore),
            200 => Some(Self::RespCommand),
            201 => Some(Self::BZMPop),
            202 => Some(Self::BZPopMax),
            203 => Some(Self::BZPopMin),
            204 => Some(Self::ZCount),
            205 => Some(Self::ZDiff),
            206 => Some(Self::ZDiffStore),
            207 => Some(Self::ZIncrBy),
            208 => Some(Self::ZInter),
            209 => Some(Self::ZInterCard),
            210 => Some(Self::ZInterStore),
            211 => Some(Self::ZLexCount),
            212 => Some(Self::ZMPop),
            213 => Some(Self::ZMScore),
            214 => Some(Self::ZPopMax),
            215 => Some(Self::ZPopMin),
            216 => Some(Self::ZRandMember),
            217 => Some(Self::ZRangeByLex),
            218 => Some(Self::ZRangeByScore),
            219 => Some(Self::ZRangeStore),
            220 => Some(Self::ZRank),
            221 => Some(Self::ZRemRangeByLex),
            222 => Some(Self::ZRemRangeByRank),
            223 => Some(Self::ZRemRangeByScore),
            224 => Some(Self::ZRevRange),
            225 => Some(Self::ZRevRangeByLex),
            226 => Some(Self::ZRevRangeByScore),
            227 => Some(Self::ZRevRank),
            228 => Some(Self::ZScan),
            229 => Some(Self::ZUnion),
            230 => Some(Self::ZUnionStore),
            _ => None,
        }
    }

    pub fn from_redis_name(name: &[u8]) -> Option<Self> {
        FAST_REDIS_COMMAND_OPCODES
            .iter()
            .find(|entry| name.eq_ignore_ascii_case(entry.name.as_bytes()))
            .map(|entry| entry.kind)
    }

    pub fn redis_name(self) -> Option<&'static str> {
        match self {
            Self::Append => Some("APPEND"),
            Self::Auth => Some("AUTH"),
            Self::BitCount => Some("BITCOUNT"),
            Self::BitField => Some("BITFIELD"),
            Self::BitOp => Some("BITOP"),
            Self::BitPos => Some("BITPOS"),
            Self::BLMove => Some("BLMOVE"),
            Self::BLMPop => Some("BLMPOP"),
            Self::BLPop => Some("BLPOP"),
            Self::BRPop => Some("BRPOP"),
            Self::BRPopLPush => Some("BRPOPLPUSH"),
            Self::BZMPop => Some("BZMPOP"),
            Self::BZPopMax => Some("BZPOPMAX"),
            Self::BZPopMin => Some("BZPOPMIN"),
            Self::Client => Some("CLIENT"),
            Self::Command => Some("COMMAND"),
            Self::Config => Some("CONFIG"),
            Self::Copy => Some("COPY"),
            Self::DbSize => Some("DBSIZE"),
            Self::Decr => Some("DECR"),
            Self::DecrBy => Some("DECRBY"),
            Self::Delete => Some("DEL"),
            Self::Discard => Some("DISCARD"),
            Self::Dump => Some("DUMP"),
            Self::Echo => Some("ECHO"),
            Self::Exec => Some("EXEC"),
            Self::Exists => Some("EXISTS"),
            Self::Expire => Some("EXPIRE"),
            Self::ExpireAt => Some("EXPIREAT"),
            Self::ExpireTime => Some("EXPIRETIME"),
            Self::FlushAll => Some("FLUSHALL"),
            Self::FlushDb => Some("FLUSHDB"),
            Self::Get => Some("GET"),
            Self::GetBit => Some("GETBIT"),
            Self::GetDel => Some("GETDEL"),
            Self::GetEx => Some("GETEX"),
            Self::GetRange => Some("GETRANGE"),
            Self::GetSet => Some("GETSET"),
            Self::Hello => Some("HELLO"),
            Self::HDel => Some("HDEL"),
            Self::HExists => Some("HEXISTS"),
            Self::HGet => Some("HGET"),
            Self::HGetAll => Some("HGETALL"),
            Self::HIncrBy => Some("HINCRBY"),
            Self::HIncrByFloat => Some("HINCRBYFLOAT"),
            Self::HKeys => Some("HKEYS"),
            Self::HLen => Some("HLEN"),
            Self::HMGet => Some("HMGET"),
            Self::HMSet => Some("HMSET"),
            Self::HRandField => Some("HRANDFIELD"),
            Self::HScan => Some("HSCAN"),
            Self::HSet => Some("HSET"),
            Self::HSetNx => Some("HSETNX"),
            Self::HStrLen => Some("HSTRLEN"),
            Self::HVals => Some("HVALS"),
            Self::Info => Some("INFO"),
            Self::Incr => Some("INCR"),
            Self::IncrBy => Some("INCRBY"),
            Self::IncrByFloat => Some("INCRBYFLOAT"),
            Self::Keys => Some("KEYS"),
            Self::LIndex => Some("LINDEX"),
            Self::LInsert => Some("LINSERT"),
            Self::LLen => Some("LLEN"),
            Self::LMove => Some("LMOVE"),
            Self::LMPop => Some("LMPOP"),
            Self::LPop => Some("LPOP"),
            Self::LPush => Some("LPUSH"),
            Self::LPushX => Some("LPUSHX"),
            Self::LRange => Some("LRANGE"),
            Self::LRem => Some("LREM"),
            Self::LSet => Some("LSET"),
            Self::LTrim => Some("LTRIM"),
            Self::Memory => Some("MEMORY"),
            Self::MGet => Some("MGET"),
            Self::MSet => Some("MSET"),
            Self::MSetNx => Some("MSETNX"),
            Self::Multi => Some("MULTI"),
            Self::Object => Some("OBJECT"),
            Self::Persist => Some("PERSIST"),
            Self::PExpire => Some("PEXPIRE"),
            Self::PExpireAt => Some("PEXPIREAT"),
            Self::PExpireTime => Some("PEXPIRETIME"),
            Self::Ping => Some("PING"),
            Self::PSetEx => Some("PSETEX"),
            Self::PTtl => Some("PTTL"),
            Self::Quit => Some("QUIT"),
            Self::RandomKey => Some("RANDOMKEY"),
            Self::Rename => Some("RENAME"),
            Self::RenameNx => Some("RENAMENX"),
            Self::Restore => Some("RESTORE"),
            Self::RestoreAsking => Some("RESTORE-ASKING"),
            Self::RPop => Some("RPOP"),
            Self::RPopLPush => Some("RPOPLPUSH"),
            Self::RPush => Some("RPUSH"),
            Self::RPushX => Some("RPUSHX"),
            Self::SAdd => Some("SADD"),
            Self::Scan => Some("SCAN"),
            Self::SCard => Some("SCARD"),
            Self::SDiff => Some("SDIFF"),
            Self::SDiffStore => Some("SDIFFSTORE"),
            Self::Select => Some("SELECT"),
            Self::Set => Some("SET"),
            Self::SetBit => Some("SETBIT"),
            Self::SetEx => Some("SETEX"),
            Self::SetNx => Some("SETNX"),
            Self::SetRange => Some("SETRANGE"),
            Self::SInter => Some("SINTER"),
            Self::SInterStore => Some("SINTERSTORE"),
            Self::SIsMember => Some("SISMEMBER"),
            Self::SMembers => Some("SMEMBERS"),
            Self::SMIsMember => Some("SMISMEMBER"),
            Self::SMove => Some("SMOVE"),
            Self::SPop => Some("SPOP"),
            Self::SRandMember => Some("SRANDMEMBER"),
            Self::SRem => Some("SREM"),
            Self::SScan => Some("SSCAN"),
            Self::StrLen => Some("STRLEN"),
            Self::Substr => Some("SUBSTR"),
            Self::SUnion => Some("SUNION"),
            Self::SUnionStore => Some("SUNIONSTORE"),
            Self::Time => Some("TIME"),
            Self::Touch => Some("TOUCH"),
            Self::Ttl => Some("TTL"),
            Self::Type => Some("TYPE"),
            Self::Unlink => Some("UNLINK"),
            Self::Unwatch => Some("UNWATCH"),
            Self::Watch => Some("WATCH"),
            Self::ZAdd => Some("ZADD"),
            Self::ZCard => Some("ZCARD"),
            Self::ZCount => Some("ZCOUNT"),
            Self::ZDiff => Some("ZDIFF"),
            Self::ZDiffStore => Some("ZDIFFSTORE"),
            Self::ZIncrBy => Some("ZINCRBY"),
            Self::ZInter => Some("ZINTER"),
            Self::ZInterCard => Some("ZINTERCARD"),
            Self::ZInterStore => Some("ZINTERSTORE"),
            Self::ZLexCount => Some("ZLEXCOUNT"),
            Self::ZMPop => Some("ZMPOP"),
            Self::ZMScore => Some("ZMSCORE"),
            Self::ZPopMax => Some("ZPOPMAX"),
            Self::ZPopMin => Some("ZPOPMIN"),
            Self::ZRandMember => Some("ZRANDMEMBER"),
            Self::ZRange => Some("ZRANGE"),
            Self::ZRangeByLex => Some("ZRANGEBYLEX"),
            Self::ZRangeByScore => Some("ZRANGEBYSCORE"),
            Self::ZRangeStore => Some("ZRANGESTORE"),
            Self::ZRank => Some("ZRANK"),
            Self::ZRem => Some("ZREM"),
            Self::ZRemRangeByLex => Some("ZREMRANGEBYLEX"),
            Self::ZRemRangeByRank => Some("ZREMRANGEBYRANK"),
            Self::ZRemRangeByScore => Some("ZREMRANGEBYSCORE"),
            Self::ZRevRange => Some("ZREVRANGE"),
            Self::ZRevRangeByLex => Some("ZREVRANGEBYLEX"),
            Self::ZRevRangeByScore => Some("ZREVRANGEBYSCORE"),
            Self::ZRevRank => Some("ZREVRANK"),
            Self::ZScan => Some("ZSCAN"),
            Self::ZScore => Some("ZSCORE"),
            Self::ZUnion => Some("ZUNION"),
            Self::ZUnionStore => Some("ZUNIONSTORE"),
            Self::RespCommand
            | Self::CommandDocs
            | Self::ConfigGet
            | Self::ClientGetName
            | Self::ClientSetName
            | Self::ClientId
            | Self::ClientList
            | Self::ClientKill => None,
        }
    }

    pub fn redis_route_keys<'a>(self, args: &[&'a [u8]]) -> FastRedisRouteKeys<'a> {
        match self {
            Self::Auth
            | Self::Hello
            | Self::Select
            | Self::Quit
            | Self::Ping
            | Self::Echo
            | Self::Info
            | Self::Command
            | Self::CommandDocs
            | Self::Config
            | Self::ConfigGet
            | Self::DbSize
            | Self::Time
            | Self::Client
            | Self::ClientGetName
            | Self::ClientSetName
            | Self::ClientId
            | Self::ClientList
            | Self::ClientKill
            | Self::Multi
            | Self::Exec
            | Self::Discard
            | Self::Unwatch => FastRedisRouteKeys::None,
            Self::Keys | Self::RandomKey | Self::Scan | Self::FlushAll | Self::FlushDb => {
                FastRedisRouteKeys::AllShards
            }
            Self::Delete
            | Self::Unlink
            | Self::Touch
            | Self::MGet
            | Self::SUnion
            | Self::SInter
            | Self::SDiff
            | Self::Watch => all_route_keys(args),
            Self::MSet | Self::MSetNx => every_nth_route_key(args, 0, 2),
            Self::Copy
            | Self::Rename
            | Self::RenameNx
            | Self::RPopLPush
            | Self::BRPopLPush
            | Self::LMove
            | Self::BLMove
            | Self::SMove => first_n_route_keys(args, 2),
            Self::Object | Self::Memory => first_n_route_keys(args.get(1..).unwrap_or_default(), 1),
            Self::BitOp => all_route_keys(args.get(1..).unwrap_or_default()),
            Self::LMPop | Self::ZMPop | Self::ZDiff | Self::ZInter | Self::ZUnion => {
                counted_route_keys(args, 0)
            }
            Self::BLMPop | Self::BZMPop => counted_route_keys(args, 1),
            Self::BLPop | Self::BRPop | Self::BZPopMin | Self::BZPopMax => {
                route_keys_before_last_arg(args)
            }
            Self::SUnionStore | Self::SInterStore | Self::SDiffStore => {
                first_and_tail_route_keys(args, 0, 1)
            }
            Self::ZUnionStore | Self::ZInterStore | Self::ZDiffStore => {
                zaggregate_store_route_keys(args)
            }
            Self::ZInterCard => counted_route_keys(args, 0),
            Self::ZRangeStore => first_n_route_keys(args, 2),
            _ => first_n_route_keys(args, 1),
        }
    }

    pub fn route_key_from_redis_args<'a>(self, args: &[&'a [u8]]) -> Option<&'a [u8]> {
        self.redis_route_keys(args).first_key()
    }

    fn is_generic_redis_opcode(self) -> bool {
        self.redis_name().is_some()
            && !matches!(
                self,
                Self::Get
                    | Self::Set
                    | Self::SetEx
                    | Self::GetEx
                    | Self::Delete
                    | Self::Exists
                    | Self::Ttl
                    | Self::Expire
                    | Self::Ping
                    | Self::MGet
                    | Self::MSet
                    | Self::Auth
                    | Self::Hello
                    | Self::Select
                    | Self::Quit
                    | Self::Echo
                    | Self::Info
                    | Self::Command
                    | Self::DbSize
                    | Self::Time
                    | Self::HSet
                    | Self::HGet
                    | Self::HDel
                    | Self::HLen
                    | Self::HMGet
                    | Self::LPush
                    | Self::RPush
                    | Self::LPop
                    | Self::RPop
                    | Self::LLen
                    | Self::LIndex
                    | Self::LRange
                    | Self::SAdd
                    | Self::SRem
                    | Self::SIsMember
                    | Self::SCard
                    | Self::SMembers
                    | Self::ZAdd
                    | Self::ZRem
                    | Self::ZScore
                    | Self::ZCard
                    | Self::ZRange
            )
    }
}

fn all_route_keys<'a>(args: &[&'a [u8]]) -> FastRedisRouteKeys<'a> {
    FastRedisRouteKeys::Keys(args.to_vec())
}

fn first_n_route_keys<'a>(args: &[&'a [u8]], count: usize) -> FastRedisRouteKeys<'a> {
    FastRedisRouteKeys::Keys(args.iter().take(count).copied().collect())
}

fn every_nth_route_key<'a>(args: &[&'a [u8]], start: usize, step: usize) -> FastRedisRouteKeys<'a> {
    FastRedisRouteKeys::Keys(args.iter().skip(start).step_by(step).copied().collect())
}

fn first_and_tail_route_keys<'a>(
    args: &[&'a [u8]],
    first_count: usize,
    tail_start: usize,
) -> FastRedisRouteKeys<'a> {
    let mut keys = Vec::new();
    keys.extend(args.iter().take(first_count).copied());
    keys.extend(args.iter().skip(tail_start).copied());
    FastRedisRouteKeys::Keys(keys)
}

fn route_keys_before_last_arg<'a>(args: &[&'a [u8]]) -> FastRedisRouteKeys<'a> {
    FastRedisRouteKeys::Keys(
        args.iter()
            .take(args.len().saturating_sub(1))
            .copied()
            .collect(),
    )
}

fn counted_route_keys<'a>(args: &[&'a [u8]], numkeys_index: usize) -> FastRedisRouteKeys<'a> {
    match counted_key_span(args, numkeys_index) {
        Some(keys) => FastRedisRouteKeys::Keys(keys.to_vec()),
        None => FastRedisRouteKeys::AllShards,
    }
}

fn counted_key_span<'args, 'value>(
    args: &'args [&'value [u8]],
    numkeys_index: usize,
) -> Option<&'args [&'value [u8]]> {
    let numkeys = args
        .get(numkeys_index)
        .and_then(|raw| parse_ascii_usize(raw))?;
    let key_start = numkeys_index.checked_add(1)?;
    let key_end = key_start.checked_add(numkeys)?;
    match numkeys {
        0 => None,
        _ => args.get(key_start..key_end),
    }
}

fn parse_ascii_usize(raw: &[u8]) -> Option<usize> {
    std::str::from_utf8(raw).ok()?.parse().ok()
}

fn zaggregate_store_route_keys<'a>(args: &[&'a [u8]]) -> FastRedisRouteKeys<'a> {
    if args.len() < 2 {
        return first_n_route_keys(args, 1);
    }
    let Some(numkeys) = std::str::from_utf8(args[1])
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return FastRedisRouteKeys::AllShards;
    };
    let mut keys = Vec::with_capacity(numkeys + 1);
    keys.push(args[0]);
    keys.extend(args.iter().skip(2).take(numkeys).copied());
    FastRedisRouteKeys::Keys(keys)
}

#[derive(Debug, Clone, PartialEq)]
pub enum FastCommand<'a> {
    Ping(Option<&'a [u8]>),
    Auth,
    Hello {
        proto: Option<u64>,
    },
    Select {
        db: u64,
    },
    Quit,
    Echo {
        payload: &'a [u8],
    },
    Info,
    Command,
    CommandDocs,
    ConfigGet {
        patterns: Vec<&'a [u8]>,
    },
    DbSize,
    Time,
    ClientGetName,
    ClientSetName {
        name: &'a [u8],
    },
    ClientId,
    ClientList,
    ClientKill,
    Get {
        key: &'a [u8],
    },
    Set {
        key: &'a [u8],
        value: &'a [u8],
    },
    SetEx {
        key: &'a [u8],
        value: &'a [u8],
        ttl_ms: u64,
    },
    GetEx {
        key: &'a [u8],
        ttl_ms: u64,
    },
    Delete {
        key: &'a [u8],
    },
    Exists {
        key: &'a [u8],
    },
    Ttl {
        key: &'a [u8],
    },
    Expire {
        key: &'a [u8],
        ttl_ms: u64,
    },
    MGet {
        keys: Vec<&'a [u8]>,
    },
    MSet {
        items: Vec<(&'a [u8], &'a [u8])>,
    },
    HSet {
        key: &'a [u8],
        field: &'a [u8],
        value: &'a [u8],
    },
    HGet {
        key: &'a [u8],
        field: &'a [u8],
    },
    HDel {
        key: &'a [u8],
        field: &'a [u8],
    },
    HLen {
        key: &'a [u8],
    },
    HMGet {
        key: &'a [u8],
        fields: Vec<&'a [u8]>,
    },
    LPush {
        key: &'a [u8],
        values: Vec<&'a [u8]>,
    },
    RPush {
        key: &'a [u8],
        values: Vec<&'a [u8]>,
    },
    LPop {
        key: &'a [u8],
    },
    RPop {
        key: &'a [u8],
    },
    LLen {
        key: &'a [u8],
    },
    LIndex {
        key: &'a [u8],
        index: i64,
    },
    LRange {
        key: &'a [u8],
        start: i64,
        stop: i64,
    },
    SAdd {
        key: &'a [u8],
        members: Vec<&'a [u8]>,
    },
    SRem {
        key: &'a [u8],
        members: Vec<&'a [u8]>,
    },
    SIsMember {
        key: &'a [u8],
        member: &'a [u8],
    },
    SCard {
        key: &'a [u8],
    },
    SMembers {
        key: &'a [u8],
    },
    ZAdd {
        key: &'a [u8],
        score: f64,
        member: &'a [u8],
    },
    ZRem {
        key: &'a [u8],
        member: &'a [u8],
    },
    ZScore {
        key: &'a [u8],
        member: &'a [u8],
    },
    ZCard {
        key: &'a [u8],
    },
    ZRange {
        key: &'a [u8],
        start: i64,
        stop: i64,
    },
    /// Generic FCNP wrapper for the Redis command surface.
    ///
    /// The request body is a length-prefixed list of command parts, starting
    /// with the command name. The server returns the RESP reply bytes as an
    /// FCNP value so clients can use one native ingress path for the complete
    /// compatibility surface while hot commands keep their typed opcodes.
    RespCommand {
        parts: Vec<&'a [u8]>,
    },
    /// Opcode-specific FCNP wrapper for Redis commands without a typed body.
    ///
    /// The opcode supplies the command name and the body is a compact
    /// varint-length list of arguments. This lets FCNP clients avoid
    /// constructing full RESP command frames while we incrementally add typed
    /// binary bodies to hot commands.
    RedisCommand {
        kind: FastCommandKind,
        args: Vec<&'a [u8]>,
    },
}

impl<'a> FastCommand<'a> {
    pub fn kind(&self) -> FastCommandKind {
        match self {
            Self::Get { .. } => FastCommandKind::Get,
            Self::Set { .. } => FastCommandKind::Set,
            Self::SetEx { .. } => FastCommandKind::SetEx,
            Self::GetEx { .. } => FastCommandKind::GetEx,
            Self::Delete { .. } => FastCommandKind::Delete,
            Self::Exists { .. } => FastCommandKind::Exists,
            Self::Ttl { .. } => FastCommandKind::Ttl,
            Self::Expire { .. } => FastCommandKind::Expire,
            Self::Ping(_) => FastCommandKind::Ping,
            Self::MGet { .. } => FastCommandKind::MGet,
            Self::MSet { .. } => FastCommandKind::MSet,
            Self::Auth => FastCommandKind::Auth,
            Self::Hello { .. } => FastCommandKind::Hello,
            Self::Select { .. } => FastCommandKind::Select,
            Self::Quit => FastCommandKind::Quit,
            Self::Echo { .. } => FastCommandKind::Echo,
            Self::Info => FastCommandKind::Info,
            Self::Command => FastCommandKind::Command,
            Self::CommandDocs => FastCommandKind::CommandDocs,
            Self::ConfigGet { .. } => FastCommandKind::ConfigGet,
            Self::DbSize => FastCommandKind::DbSize,
            Self::Time => FastCommandKind::Time,
            Self::ClientGetName => FastCommandKind::ClientGetName,
            Self::ClientSetName { .. } => FastCommandKind::ClientSetName,
            Self::ClientId => FastCommandKind::ClientId,
            Self::ClientList => FastCommandKind::ClientList,
            Self::ClientKill => FastCommandKind::ClientKill,
            Self::HSet { .. } => FastCommandKind::HSet,
            Self::HGet { .. } => FastCommandKind::HGet,
            Self::HDel { .. } => FastCommandKind::HDel,
            Self::HLen { .. } => FastCommandKind::HLen,
            Self::HMGet { .. } => FastCommandKind::HMGet,
            Self::LPush { .. } => FastCommandKind::LPush,
            Self::RPush { .. } => FastCommandKind::RPush,
            Self::LPop { .. } => FastCommandKind::LPop,
            Self::RPop { .. } => FastCommandKind::RPop,
            Self::LLen { .. } => FastCommandKind::LLen,
            Self::LIndex { .. } => FastCommandKind::LIndex,
            Self::LRange { .. } => FastCommandKind::LRange,
            Self::SAdd { .. } => FastCommandKind::SAdd,
            Self::SRem { .. } => FastCommandKind::SRem,
            Self::SIsMember { .. } => FastCommandKind::SIsMember,
            Self::SCard { .. } => FastCommandKind::SCard,
            Self::SMembers { .. } => FastCommandKind::SMembers,
            Self::ZAdd { .. } => FastCommandKind::ZAdd,
            Self::ZRem { .. } => FastCommandKind::ZRem,
            Self::ZScore { .. } => FastCommandKind::ZScore,
            Self::ZCard { .. } => FastCommandKind::ZCard,
            Self::ZRange { .. } => FastCommandKind::ZRange,
            Self::RespCommand { .. } => FastCommandKind::RespCommand,
            Self::RedisCommand { kind, .. } => *kind,
        }
    }

    pub fn route_keys(&self) -> FastRedisRouteKeys<'a> {
        match self {
            Self::Ping(_)
            | Self::Auth
            | Self::Hello { .. }
            | Self::Select { .. }
            | Self::Quit
            | Self::Echo { .. }
            | Self::Info
            | Self::Command
            | Self::CommandDocs
            | Self::ConfigGet { .. }
            | Self::DbSize
            | Self::Time
            | Self::ClientGetName
            | Self::ClientSetName { .. }
            | Self::ClientId
            | Self::ClientList
            | Self::ClientKill => FastRedisRouteKeys::None,
            Self::RespCommand { parts } => {
                let Some((command, args)) = parts.split_first() else {
                    return FastRedisRouteKeys::None;
                };
                FastCommandKind::from_redis_name(command)
                    .map(|kind| kind.redis_route_keys(args))
                    .unwrap_or(FastRedisRouteKeys::None)
            }
            Self::RedisCommand { kind, args } => kind.redis_route_keys(args),
            Self::Get { key }
            | Self::Set { key, .. }
            | Self::SetEx { key, .. }
            | Self::GetEx { key, .. }
            | Self::Delete { key }
            | Self::Exists { key }
            | Self::Ttl { key }
            | Self::Expire { key, .. }
            | Self::HSet { key, .. }
            | Self::HGet { key, .. }
            | Self::HDel { key, .. }
            | Self::HLen { key }
            | Self::HMGet { key, .. }
            | Self::LPush { key, .. }
            | Self::RPush { key, .. }
            | Self::LPop { key }
            | Self::RPop { key }
            | Self::LLen { key }
            | Self::LIndex { key, .. }
            | Self::LRange { key, .. }
            | Self::SAdd { key, .. }
            | Self::SRem { key, .. }
            | Self::SIsMember { key, .. }
            | Self::SCard { key }
            | Self::SMembers { key }
            | Self::ZAdd { key, .. }
            | Self::ZRem { key, .. }
            | Self::ZScore { key, .. }
            | Self::ZCard { key }
            | Self::ZRange { key, .. } => FastRedisRouteKeys::Keys(vec![*key]),
            Self::MGet { keys } => FastRedisRouteKeys::Keys(keys.clone()),
            Self::MSet { items } => {
                FastRedisRouteKeys::Keys(items.iter().map(|(key, _)| *key).collect())
            }
        }
    }

    pub fn route_key(&self) -> Option<&'a [u8]> {
        self.route_keys().first_key()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FastRequest<'a> {
    pub key_hash: Option<u64>,
    pub route_shard: Option<u32>,
    pub key_tag: Option<u64>,
    pub command: FastCommand<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FastResponse {
    Ok,
    Null,
    Error(Vec<u8>),
    Integer(i64),
    Value(Vec<u8>),
    Boolean(bool),
    Array(Vec<Option<Vec<u8>>>),
    Float(f64),
}

pub type FastRequestDecodeResult<'a> = Option<(FastRequest<'a>, usize)>;
pub type FastResponseDecodeResult = Option<(FastResponse, usize)>;

#[derive(Debug, Default, Clone, Copy)]
pub struct FastCodec;

impl FastCodec {
    pub fn is_fast_request_prefix(byte: u8) -> bool {
        byte == FAST_REQUEST_MAGIC
    }

    pub fn decode_request(buffer: &[u8]) -> Result<FastRequestDecodeResult<'_>> {
        if buffer.is_empty() {
            return Ok(None);
        }
        if buffer[0] != FAST_REQUEST_MAGIC {
            return Err(FastCacheError::Protocol(
                "invalid fast request magic byte".into(),
            ));
        }
        if buffer.len() < REQUEST_HEADER_LEN {
            return Ok(None);
        }

        let version = buffer[1];
        if version != FAST_PROTOCOL_VERSION {
            return Err(FastCacheError::Protocol(format!(
                "unsupported fast protocol version: {version}"
            )));
        }

        let kind = FastCommandKind::from_u8(buffer[2])?;
        let flags = buffer[3];
        if flags
            & !(FAST_FLAG_KEY_HASH
                | FAST_FLAG_ROUTE_SHARD
                | FAST_FLAG_KEY_TAG
                | FAST_FLAG_REDIS_COMMAND_ARGS)
            != 0
        {
            return Err(FastCacheError::Protocol(format!(
                "unsupported fast flags: {flags:#04x}"
            )));
        }

        let body_len = u32::from_le_bytes(buffer[4..8].try_into().unwrap()) as usize;
        if buffer.len() < REQUEST_HEADER_LEN + body_len {
            return Ok(None);
        }

        let body = &buffer[REQUEST_HEADER_LEN..REQUEST_HEADER_LEN + body_len];
        let mut cursor = 0usize;
        let key_hash = if flags & FAST_FLAG_KEY_HASH != 0 {
            Some(take_u64(body, &mut cursor, "fast key hash")?)
        } else {
            None
        };
        let route_shard = if flags & FAST_FLAG_ROUTE_SHARD != 0 {
            Some(take_u32(body, &mut cursor, "fast route shard")?)
        } else {
            None
        };
        let key_tag = if flags & FAST_FLAG_KEY_TAG != 0 {
            Some(take_u64(body, &mut cursor, "fast key tag")?)
        } else {
            None
        };

        if flags & FAST_FLAG_REDIS_COMMAND_ARGS != 0 {
            if kind.redis_name().is_none() {
                return Err(FastCacheError::Protocol(format!(
                    "fast command id {} is not a Redis command opcode",
                    kind as u8
                )));
            }
            let command = FastCommand::RedisCommand {
                kind,
                args: take_compact_len_prefixed_list(
                    body,
                    &mut cursor,
                    "fast Redis command arguments",
                )?,
            };
            ensure_finished(body, cursor, "fast request")?;
            return Ok(Some((
                FastRequest {
                    key_hash,
                    route_shard,
                    key_tag,
                    command,
                },
                REQUEST_HEADER_LEN + body_len,
            )));
        }

        let command = match kind {
            FastCommandKind::Ping => {
                let payload = (!body[cursor..].is_empty()).then_some(&body[cursor..]);
                cursor = body.len();
                FastCommand::Ping(payload)
            }
            FastCommandKind::Auth => FastCommand::Auth,
            FastCommandKind::Hello => {
                let proto = if body.len().saturating_sub(cursor) >= 8 {
                    Some(take_u64(body, &mut cursor, "fast HELLO proto")?)
                } else {
                    None
                };
                FastCommand::Hello { proto }
            }
            FastCommandKind::Select => FastCommand::Select {
                db: take_u64(body, &mut cursor, "fast SELECT db")?,
            },
            FastCommandKind::Quit => FastCommand::Quit,
            FastCommandKind::Echo => FastCommand::Echo {
                payload: take_len_prefixed_slice(body, &mut cursor, "fast ECHO payload")?,
            },
            FastCommandKind::Info => FastCommand::Info,
            FastCommandKind::Command => FastCommand::Command,
            FastCommandKind::CommandDocs => FastCommand::CommandDocs,
            FastCommandKind::ConfigGet => FastCommand::ConfigGet {
                patterns: take_len_prefixed_list(body, &mut cursor, "fast CONFIG GET patterns")?,
            },
            FastCommandKind::DbSize => FastCommand::DbSize,
            FastCommandKind::Time => FastCommand::Time,
            FastCommandKind::ClientGetName => FastCommand::ClientGetName,
            FastCommandKind::ClientSetName => FastCommand::ClientSetName {
                name: take_len_prefixed_slice(body, &mut cursor, "fast CLIENT SETNAME name")?,
            },
            FastCommandKind::ClientId => FastCommand::ClientId,
            FastCommandKind::ClientList => FastCommand::ClientList,
            FastCommandKind::ClientKill => FastCommand::ClientKill,
            FastCommandKind::Get => FastCommand::Get {
                key: take_len_prefixed_slice(body, &mut cursor, "fast GET key")?,
            },
            FastCommandKind::Set => {
                let (key, value) = take_key_value(body, &mut cursor, "fast SET")?;
                FastCommand::Set { key, value }
            }
            FastCommandKind::SetEx => {
                let ttl_ms = take_u64(body, &mut cursor, "fast SETEX ttl")?;
                let (key, value) = take_key_value(body, &mut cursor, "fast SETEX")?;
                FastCommand::SetEx { key, value, ttl_ms }
            }
            FastCommandKind::GetEx => {
                let ttl_ms = take_u64(body, &mut cursor, "fast GETEX ttl")?;
                FastCommand::GetEx {
                    key: take_len_prefixed_slice(body, &mut cursor, "fast GETEX key")?,
                    ttl_ms,
                }
            }
            FastCommandKind::Delete => FastCommand::Delete {
                key: take_len_prefixed_slice(body, &mut cursor, "fast DELETE key")?,
            },
            FastCommandKind::Exists => FastCommand::Exists {
                key: take_len_prefixed_slice(body, &mut cursor, "fast EXISTS key")?,
            },
            FastCommandKind::Ttl => FastCommand::Ttl {
                key: take_len_prefixed_slice(body, &mut cursor, "fast TTL key")?,
            },
            FastCommandKind::Expire => {
                let ttl_ms = take_u64(body, &mut cursor, "fast EXPIRE ttl")?;
                FastCommand::Expire {
                    key: take_len_prefixed_slice(body, &mut cursor, "fast EXPIRE key")?,
                    ttl_ms,
                }
            }
            FastCommandKind::MGet => FastCommand::MGet {
                keys: take_len_prefixed_list(body, &mut cursor, "fast MGET keys")?,
            },
            FastCommandKind::MSet => FastCommand::MSet {
                items: take_key_value_list(body, &mut cursor, "fast MSET items")?,
            },
            FastCommandKind::HSet => {
                let (key, field, value) = take_key_field_value(body, &mut cursor, "fast HSET")?;
                FastCommand::HSet { key, field, value }
            }
            FastCommandKind::HGet => {
                let (key, field) = take_key_field(body, &mut cursor, "fast HGET")?;
                FastCommand::HGet { key, field }
            }
            FastCommandKind::HDel => {
                let (key, field) = take_key_field(body, &mut cursor, "fast HDEL")?;
                FastCommand::HDel { key, field }
            }
            FastCommandKind::HLen => FastCommand::HLen {
                key: take_len_prefixed_slice(body, &mut cursor, "fast HLEN key")?,
            },
            FastCommandKind::HMGet => {
                let (key, fields) = take_key_list(body, &mut cursor, "fast HMGET")?;
                FastCommand::HMGet { key, fields }
            }
            FastCommandKind::LPush => {
                let (key, values) = take_key_list(body, &mut cursor, "fast LPUSH")?;
                FastCommand::LPush { key, values }
            }
            FastCommandKind::RPush => {
                let (key, values) = take_key_list(body, &mut cursor, "fast RPUSH")?;
                FastCommand::RPush { key, values }
            }
            FastCommandKind::LPop => FastCommand::LPop {
                key: take_len_prefixed_slice(body, &mut cursor, "fast LPOP key")?,
            },
            FastCommandKind::RPop => FastCommand::RPop {
                key: take_len_prefixed_slice(body, &mut cursor, "fast RPOP key")?,
            },
            FastCommandKind::LLen => FastCommand::LLen {
                key: take_len_prefixed_slice(body, &mut cursor, "fast LLEN key")?,
            },
            FastCommandKind::LIndex => {
                let index = take_i64(body, &mut cursor, "fast LINDEX index")?;
                FastCommand::LIndex {
                    key: take_len_prefixed_slice(body, &mut cursor, "fast LINDEX key")?,
                    index,
                }
            }
            FastCommandKind::LRange => {
                let start = take_i64(body, &mut cursor, "fast LRANGE start")?;
                let stop = take_i64(body, &mut cursor, "fast LRANGE stop")?;
                FastCommand::LRange {
                    key: take_len_prefixed_slice(body, &mut cursor, "fast LRANGE key")?,
                    start,
                    stop,
                }
            }
            FastCommandKind::SAdd => {
                let (key, members) = take_key_list(body, &mut cursor, "fast SADD")?;
                FastCommand::SAdd { key, members }
            }
            FastCommandKind::SRem => {
                let (key, members) = take_key_list(body, &mut cursor, "fast SREM")?;
                FastCommand::SRem { key, members }
            }
            FastCommandKind::SIsMember => {
                let (key, member) = take_key_field(body, &mut cursor, "fast SISMEMBER")?;
                FastCommand::SIsMember { key, member }
            }
            FastCommandKind::SCard => FastCommand::SCard {
                key: take_len_prefixed_slice(body, &mut cursor, "fast SCARD key")?,
            },
            FastCommandKind::SMembers => FastCommand::SMembers {
                key: take_len_prefixed_slice(body, &mut cursor, "fast SMEMBERS key")?,
            },
            FastCommandKind::ZAdd => {
                let score = take_f64(body, &mut cursor, "fast ZADD score")?;
                let (key, member) = take_key_field(body, &mut cursor, "fast ZADD")?;
                FastCommand::ZAdd { key, score, member }
            }
            FastCommandKind::ZRem => {
                let (key, member) = take_key_field(body, &mut cursor, "fast ZREM")?;
                FastCommand::ZRem { key, member }
            }
            FastCommandKind::ZScore => {
                let (key, member) = take_key_field(body, &mut cursor, "fast ZSCORE")?;
                FastCommand::ZScore { key, member }
            }
            FastCommandKind::ZCard => FastCommand::ZCard {
                key: take_len_prefixed_slice(body, &mut cursor, "fast ZCARD key")?,
            },
            FastCommandKind::ZRange => {
                let start = take_i64(body, &mut cursor, "fast ZRANGE start")?;
                let stop = take_i64(body, &mut cursor, "fast ZRANGE stop")?;
                FastCommand::ZRange {
                    key: take_len_prefixed_slice(body, &mut cursor, "fast ZRANGE key")?,
                    start,
                    stop,
                }
            }
            FastCommandKind::RespCommand => FastCommand::RespCommand {
                parts: take_len_prefixed_list(body, &mut cursor, "fast RESP command parts")?,
            },
            kind => {
                if !kind.is_generic_redis_opcode() {
                    return Err(FastCacheError::Protocol(format!(
                        "fast command id {} has no request decoder",
                        kind as u8
                    )));
                }
                FastCommand::RedisCommand {
                    kind,
                    args: take_compact_len_prefixed_list(
                        body,
                        &mut cursor,
                        "fast Redis command arguments",
                    )?,
                }
            }
        };
        ensure_finished(body, cursor, "fast request")?;

        Ok(Some((
            FastRequest {
                key_hash,
                route_shard,
                key_tag,
                command,
            },
            REQUEST_HEADER_LEN + body_len,
        )))
    }

    pub fn encode_request(request: &FastRequest<'_>, out: &mut Vec<u8>) {
        let mut flags = 0u8;
        if request.key_hash.is_some() {
            flags |= FAST_FLAG_KEY_HASH;
        }
        if request.route_shard.is_some() {
            flags |= FAST_FLAG_ROUTE_SHARD;
        }
        if request.key_tag.is_some() {
            flags |= FAST_FLAG_KEY_TAG;
        }
        if matches!(request.command, FastCommand::RedisCommand { .. }) {
            flags |= FAST_FLAG_REDIS_COMMAND_ARGS;
        }

        out.push(FAST_REQUEST_MAGIC);
        out.push(FAST_PROTOCOL_VERSION);
        out.push(request.command.kind() as u8);
        out.push(flags);

        let body_len_index = out.len();
        out.extend_from_slice(&0_u32.to_le_bytes());
        let body_start = out.len();

        if let Some(key_hash) = request.key_hash {
            out.extend_from_slice(&key_hash.to_le_bytes());
        }
        if let Some(route_shard) = request.route_shard {
            out.extend_from_slice(&route_shard.to_le_bytes());
        }
        if let Some(key_tag) = request.key_tag {
            out.extend_from_slice(&key_tag.to_le_bytes());
        }

        match &request.command {
            FastCommand::Ping(payload) => {
                if let Some(payload) = payload {
                    out.extend_from_slice(payload);
                }
            }
            FastCommand::Auth
            | FastCommand::Quit
            | FastCommand::Info
            | FastCommand::Command
            | FastCommand::CommandDocs
            | FastCommand::DbSize
            | FastCommand::Time
            | FastCommand::ClientGetName
            | FastCommand::ClientId
            | FastCommand::ClientList
            | FastCommand::ClientKill => {}
            FastCommand::Hello { proto } => {
                if let Some(proto) = proto {
                    out.extend_from_slice(&proto.to_le_bytes());
                }
            }
            FastCommand::Select { db } => {
                out.extend_from_slice(&db.to_le_bytes());
            }
            FastCommand::Echo { payload } => encode_len_prefixed(payload, out),
            FastCommand::ConfigGet { patterns } => encode_len_prefixed_list(patterns, out),
            FastCommand::ClientSetName { name } => encode_len_prefixed(name, out),
            FastCommand::Get { key }
            | FastCommand::Delete { key }
            | FastCommand::Exists { key }
            | FastCommand::Ttl { key } => encode_len_prefixed(key, out),
            FastCommand::Set { key, value } => encode_key_value(key, value, out),
            FastCommand::SetEx { key, value, ttl_ms } => {
                out.extend_from_slice(&ttl_ms.to_le_bytes());
                encode_key_value(key, value, out);
            }
            FastCommand::GetEx { key, ttl_ms } | FastCommand::Expire { key, ttl_ms } => {
                out.extend_from_slice(&ttl_ms.to_le_bytes());
                encode_len_prefixed(key, out);
            }
            FastCommand::MGet { keys } => encode_len_prefixed_list(keys, out),
            FastCommand::MSet { items } => encode_key_value_list(items, out),
            FastCommand::HSet { key, field, value } => {
                encode_key_field_value(key, field, value, out);
            }
            FastCommand::HGet { key, field }
            | FastCommand::HDel { key, field }
            | FastCommand::SIsMember { key, member: field }
            | FastCommand::ZRem { key, member: field }
            | FastCommand::ZScore { key, member: field } => {
                encode_key_field(key, field, out);
            }
            FastCommand::HLen { key }
            | FastCommand::LPop { key }
            | FastCommand::RPop { key }
            | FastCommand::LLen { key }
            | FastCommand::SCard { key }
            | FastCommand::SMembers { key }
            | FastCommand::ZCard { key } => encode_len_prefixed(key, out),
            FastCommand::HMGet { key, fields } => encode_key_list(key, fields, out),
            FastCommand::LPush { key, values } | FastCommand::RPush { key, values } => {
                encode_key_list(key, values, out);
            }
            FastCommand::LIndex { key, index } => {
                out.extend_from_slice(&index.to_le_bytes());
                encode_len_prefixed(key, out);
            }
            FastCommand::LRange { key, start, stop } | FastCommand::ZRange { key, start, stop } => {
                out.extend_from_slice(&start.to_le_bytes());
                out.extend_from_slice(&stop.to_le_bytes());
                encode_len_prefixed(key, out);
            }
            FastCommand::SAdd { key, members } | FastCommand::SRem { key, members } => {
                encode_key_list(key, members, out);
            }
            FastCommand::ZAdd { key, score, member } => {
                out.extend_from_slice(&score.to_le_bytes());
                encode_key_field(key, member, out);
            }
            FastCommand::RespCommand { parts } => encode_len_prefixed_list(parts, out),
            FastCommand::RedisCommand { args, .. } => encode_compact_len_prefixed_list(args, out),
        }

        let body_len = (out.len() - body_start) as u32;
        out[body_len_index..body_len_index + 4].copy_from_slice(&body_len.to_le_bytes());
    }

    pub fn encode_response(response: &FastResponse, out: &mut Vec<u8>) {
        out.push(FAST_RESPONSE_MAGIC);
        out.push(FAST_PROTOCOL_VERSION);
        match response {
            FastResponse::Ok => {
                out.push(0);
                out.push(0);
                out.extend_from_slice(&0_u32.to_le_bytes());
            }
            FastResponse::Null => {
                out.push(1);
                out.push(0);
                out.extend_from_slice(&0_u32.to_le_bytes());
            }
            FastResponse::Error(message) => {
                out.push(2);
                out.push(0);
                out.extend_from_slice(&(message.len() as u32).to_le_bytes());
                out.extend_from_slice(message);
            }
            FastResponse::Integer(value) => {
                out.push(3);
                out.push(0);
                out.extend_from_slice(&8_u32.to_le_bytes());
                out.extend_from_slice(&value.to_le_bytes());
            }
            FastResponse::Value(bytes) => {
                out.push(4);
                out.push(0);
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
            FastResponse::Boolean(value) => {
                out.push(5);
                out.push(0);
                out.extend_from_slice(&1_u32.to_le_bytes());
                out.push(*value as u8);
            }
            FastResponse::Array(values) => {
                out.push(6);
                out.push(0);
                let body_len_index = out.len();
                out.extend_from_slice(&0_u32.to_le_bytes());
                let body_start = out.len();
                encode_array_body(values.iter().map(|value| value.as_deref()), out);
                let body_len = (out.len() - body_start) as u32;
                out[body_len_index..body_len_index + 4].copy_from_slice(&body_len.to_le_bytes());
            }
            FastResponse::Float(value) => {
                out.push(7);
                out.push(0);
                out.extend_from_slice(&8_u32.to_le_bytes());
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
    }

    pub fn decode_response(buffer: &[u8]) -> Result<FastResponseDecodeResult> {
        if buffer.is_empty() {
            return Ok(None);
        }
        if buffer[0] != FAST_RESPONSE_MAGIC {
            return Err(FastCacheError::Protocol(
                "invalid fast response magic byte".into(),
            ));
        }
        if buffer.len() < RESPONSE_HEADER_LEN {
            return Ok(None);
        }

        let version = buffer[1];
        if version != FAST_PROTOCOL_VERSION {
            return Err(FastCacheError::Protocol(format!(
                "unsupported fast response protocol version: {version}"
            )));
        }

        let status = buffer[2];
        let _flags = buffer[3];
        let body_len = u32::from_le_bytes(buffer[4..8].try_into().unwrap()) as usize;
        if buffer.len() < RESPONSE_HEADER_LEN + body_len {
            return Ok(None);
        }

        let body = &buffer[RESPONSE_HEADER_LEN..RESPONSE_HEADER_LEN + body_len];
        let response = match status {
            0 => FastResponse::Ok,
            1 => FastResponse::Null,
            2 => FastResponse::Error(body.to_vec()),
            3 => {
                if body.len() != 8 {
                    return Err(FastCacheError::Protocol(
                        "fast integer response is truncated".into(),
                    ));
                }
                FastResponse::Integer(i64::from_le_bytes(body.try_into().unwrap()))
            }
            4 => FastResponse::Value(body.to_vec()),
            5 => {
                if body.len() != 1 {
                    return Err(FastCacheError::Protocol(
                        "fast boolean response is truncated".into(),
                    ));
                }
                FastResponse::Boolean(body[0] != 0)
            }
            6 => FastResponse::Array(decode_array_body(body)?),
            7 => {
                if body.len() != 8 {
                    return Err(FastCacheError::Protocol(
                        "fast float response is truncated".into(),
                    ));
                }
                FastResponse::Float(f64::from_le_bytes(body.try_into().unwrap()))
            }
            other => {
                return Err(FastCacheError::Protocol(format!(
                    "unsupported fast response status: {other}"
                )));
            }
        };

        Ok(Some((response, RESPONSE_HEADER_LEN + body_len)))
    }
}

fn take_u32(body: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
    if body.len().saturating_sub(*cursor) < 4 {
        return Err(FastCacheError::Protocol(format!("{field} is truncated")));
    }
    let value = u32::from_le_bytes(body[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

fn take_u64(body: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
    if body.len().saturating_sub(*cursor) < 8 {
        return Err(FastCacheError::Protocol(format!("{field} is truncated")));
    }
    let value = u64::from_le_bytes(body[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn take_i64(body: &[u8], cursor: &mut usize, field: &str) -> Result<i64> {
    if body.len().saturating_sub(*cursor) < 8 {
        return Err(FastCacheError::Protocol(format!("{field} is truncated")));
    }
    let value = i64::from_le_bytes(body[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn take_f64(body: &[u8], cursor: &mut usize, field: &str) -> Result<f64> {
    if body.len().saturating_sub(*cursor) < 8 {
        return Err(FastCacheError::Protocol(format!("{field} is truncated")));
    }
    let value = f64::from_le_bytes(body[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn take_exact_slice<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    len: usize,
    field: &str,
) -> Result<&'a [u8]> {
    if body.len().saturating_sub(*cursor) < len {
        return Err(FastCacheError::Protocol(format!(
            "{field} length does not match body"
        )));
    }
    let value = &body[*cursor..*cursor + len];
    *cursor += len;
    Ok(value)
}

fn take_len_prefixed_slice<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    field: &str,
) -> Result<&'a [u8]> {
    let len = take_u32(body, cursor, field)? as usize;
    take_exact_slice(body, cursor, len, field)
}

fn take_key_value<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    field: &str,
) -> Result<(&'a [u8], &'a [u8])> {
    let key_len = take_u32(body, cursor, field)? as usize;
    let value_len = take_u32(body, cursor, field)? as usize;
    let key = take_exact_slice(body, cursor, key_len, field)?;
    let value = take_exact_slice(body, cursor, value_len, field)?;
    Ok((key, value))
}

fn take_key_field<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    field: &str,
) -> Result<(&'a [u8], &'a [u8])> {
    let key_len = take_u32(body, cursor, field)? as usize;
    let field_len = take_u32(body, cursor, field)? as usize;
    let key = take_exact_slice(body, cursor, key_len, field)?;
    let member = take_exact_slice(body, cursor, field_len, field)?;
    Ok((key, member))
}

fn take_key_field_value<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    field: &str,
) -> Result<(&'a [u8], &'a [u8], &'a [u8])> {
    let key_len = take_u32(body, cursor, field)? as usize;
    let field_len = take_u32(body, cursor, field)? as usize;
    let value_len = take_u32(body, cursor, field)? as usize;
    let key = take_exact_slice(body, cursor, key_len, field)?;
    let member = take_exact_slice(body, cursor, field_len, field)?;
    let value = take_exact_slice(body, cursor, value_len, field)?;
    Ok((key, member, value))
}

fn take_len_prefixed_list<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    field: &str,
) -> Result<Vec<&'a [u8]>> {
    let count = take_u32(body, cursor, field)? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(take_len_prefixed_slice(body, cursor, field)?);
    }
    Ok(values)
}

fn take_var_u32(body: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
    let mut value = 0u32;
    let mut shift = 0u32;
    for _ in 0..5 {
        if *cursor >= body.len() {
            return Err(FastCacheError::Protocol(format!(
                "{field} varint is truncated"
            )));
        }
        let byte = body[*cursor];
        *cursor += 1;
        value |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(FastCacheError::Protocol(format!(
        "{field} varint is too large"
    )))
}

fn take_compact_len_prefixed_slice<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    field: &str,
) -> Result<&'a [u8]> {
    let len = take_var_u32(body, cursor, field)? as usize;
    take_exact_slice(body, cursor, len, field)
}

fn take_compact_len_prefixed_list<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    field: &str,
) -> Result<Vec<&'a [u8]>> {
    let count = take_var_u32(body, cursor, field)? as usize;
    if count > body.len().saturating_sub(*cursor) {
        return Err(FastCacheError::Protocol(format!(
            "{field} count exceeds remaining body"
        )));
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(take_compact_len_prefixed_slice(body, cursor, field)?);
    }
    Ok(values)
}

fn take_key_list<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    field: &str,
) -> Result<(&'a [u8], Vec<&'a [u8]>)> {
    let key = take_len_prefixed_slice(body, cursor, field)?;
    let values = take_len_prefixed_list(body, cursor, field)?;
    Ok((key, values))
}

fn take_key_value_list<'a>(
    body: &'a [u8],
    cursor: &mut usize,
    field: &str,
) -> Result<Vec<(&'a [u8], &'a [u8])>> {
    let count = take_u32(body, cursor, field)? as usize;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(take_key_value(body, cursor, field)?);
    }
    Ok(items)
}

fn ensure_finished(body: &[u8], cursor: usize, field: &str) -> Result<()> {
    if cursor != body.len() {
        return Err(FastCacheError::Protocol(format!(
            "{field} has trailing bytes"
        )));
    }
    Ok(())
}

fn encode_len_prefixed(value: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

fn encode_key_value(key: &[u8], value: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(value);
}

fn encode_key_field(key: &[u8], field: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(&(field.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(field);
}

fn encode_key_field_value(key: &[u8], field: &[u8], value: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(&(field.len() as u32).to_le_bytes());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(field);
    out.extend_from_slice(value);
}

fn encode_len_prefixed_list(values: &[&[u8]], out: &mut Vec<u8>) {
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for value in values {
        encode_len_prefixed(value, out);
    }
}

fn encode_var_u32(mut value: u32, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn encode_compact_len_prefixed(value: &[u8], out: &mut Vec<u8>) {
    encode_var_u32(value.len() as u32, out);
    out.extend_from_slice(value);
}

fn encode_compact_len_prefixed_list(values: &[&[u8]], out: &mut Vec<u8>) {
    encode_var_u32(values.len() as u32, out);
    for value in values {
        encode_compact_len_prefixed(value, out);
    }
}

fn encode_key_list(key: &[u8], values: &[&[u8]], out: &mut Vec<u8>) {
    encode_len_prefixed(key, out);
    encode_len_prefixed_list(values, out);
}

fn encode_key_value_list(items: &[(&[u8], &[u8])], out: &mut Vec<u8>) {
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for (key, value) in items {
        encode_key_value(key, value, out);
    }
}

fn encode_array_body<'a>(values: impl IntoIterator<Item = Option<&'a [u8]>>, out: &mut Vec<u8>) {
    let values = values.into_iter().collect::<Vec<_>>();
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for value in values {
        match value {
            Some(value) => {
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value);
            }
            None => out.extend_from_slice(&u32::MAX.to_le_bytes()),
        }
    }
}

fn decode_array_body(body: &[u8]) -> Result<Vec<Option<Vec<u8>>>> {
    let mut cursor = 0usize;
    let count = take_u32(body, &mut cursor, "fast array count")? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let len = take_u32(body, &mut cursor, "fast array item")?;
        if len == u32::MAX {
            values.push(None);
        } else {
            values.push(Some(
                take_exact_slice(body, &mut cursor, len as usize, "fast array item")?.to_vec(),
            ));
        }
    }
    ensure_finished(body, cursor, "fast array response")?;
    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        FAST_FLAG_REDIS_COMMAND_ARGS, FAST_PROTOCOL_VERSION, FAST_REDIS_COMMAND_OPCODES, FastCodec,
        FastCommand, FastCommandKind, FastRedisRouteKeys, FastRequest, FastResponse,
        REQUEST_HEADER_LEN,
    };

    #[test]
    fn redis_opcode_table_has_unique_names_and_values() {
        let mut names = BTreeSet::new();
        let mut opcodes = BTreeSet::new();
        for entry in FAST_REDIS_COMMAND_OPCODES {
            assert!(
                names.insert(entry.name),
                "duplicate Redis opcode name {}",
                entry.name
            );
            assert!(
                opcodes.insert(entry.kind as u8),
                "duplicate Redis opcode value {} for {}",
                entry.kind as u8,
                entry.name
            );
        }
    }

    #[test]
    fn redis_opcode_table_round_trips_names_and_values() {
        for entry in FAST_REDIS_COMMAND_OPCODES {
            assert_eq!(
                FastCommandKind::from_redis_name(entry.name.as_bytes()),
                Some(entry.kind)
            );
            assert_eq!(entry.kind.redis_name(), Some(entry.name));
            assert_eq!(
                FastCommandKind::from_u8(entry.kind as u8).unwrap(),
                entry.kind
            );
            assert_eq!(
                FastCommandKind::from_opcode(entry.kind as u8),
                Some(entry.kind)
            );
        }
    }

    #[test]
    fn redis_opcode_route_metadata_covers_multikey_shapes() {
        assert_eq!(
            FastCommandKind::MSet.redis_route_keys(&[
                b"a".as_slice(),
                b"1".as_slice(),
                b"b".as_slice(),
                b"2".as_slice()
            ]),
            FastRedisRouteKeys::Keys(vec![b"a".as_slice(), b"b".as_slice()])
        );
        assert_eq!(
            FastCommandKind::ZUnionStore.redis_route_keys(&[
                b"out".as_slice(),
                b"2".as_slice(),
                b"a".as_slice(),
                b"b".as_slice()
            ]),
            FastRedisRouteKeys::Keys(vec![b"out".as_slice(), b"a".as_slice(), b"b".as_slice()])
        );
        assert_eq!(
            FastCommandKind::BLMPop.redis_route_keys(&[
                b"0.001".as_slice(),
                b"2".as_slice(),
                b"a".as_slice(),
                b"b".as_slice(),
                b"LEFT".as_slice()
            ]),
            FastRedisRouteKeys::Keys(vec![b"a".as_slice(), b"b".as_slice()])
        );
        assert_eq!(
            FastCommandKind::BitOp.redis_route_keys(&[
                b"OR".as_slice(),
                b"out".as_slice(),
                b"a".as_slice(),
                b"b".as_slice(),
            ]),
            FastRedisRouteKeys::Keys(vec![b"out".as_slice(), b"a".as_slice(), b"b".as_slice()])
        );
        assert_eq!(
            FastCommandKind::Keys.redis_route_keys(&[b"*".as_slice()]),
            FastRedisRouteKeys::AllShards
        );
        assert_eq!(
            FastCommandKind::Ping.redis_route_keys(&[]),
            FastRedisRouteKeys::None
        );
    }

    #[test]
    fn round_trips_get_request_with_hash_and_shard() {
        let request = FastRequest {
            key_hash: Some(42),
            route_shard: Some(3),
            key_tag: Some(99),
            command: FastCommand::Get { key: b"alpha" },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);
        assert_eq!(encoded[1], FAST_PROTOCOL_VERSION);
        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
    }

    #[test]
    fn round_trips_set_request() {
        let request = FastRequest {
            key_hash: Some(7),
            route_shard: None,
            key_tag: Some(13),
            command: FastCommand::Set {
                key: b"alpha",
                value: b"beta",
            },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);
        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
    }

    #[test]
    fn round_trips_setex_request() {
        let request = FastRequest {
            key_hash: Some(7),
            route_shard: None,
            key_tag: Some(13),
            command: FastCommand::SetEx {
                key: b"alpha",
                value: b"beta",
                ttl_ms: 60_000,
            },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);
        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
    }

    #[test]
    fn round_trips_getex_request() {
        let request = FastRequest {
            key_hash: Some(11),
            route_shard: None,
            key_tag: None,
            command: FastCommand::GetEx {
                key: b"alpha",
                ttl_ms: 5_000,
            },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);
        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
    }

    #[test]
    fn round_trips_probe_requests() {
        let requests = [
            FastRequest {
                key_hash: None,
                route_shard: None,
                key_tag: None,
                command: FastCommand::Hello { proto: Some(2) },
            },
            FastRequest {
                key_hash: None,
                route_shard: None,
                key_tag: None,
                command: FastCommand::Select { db: 0 },
            },
            FastRequest {
                key_hash: None,
                route_shard: None,
                key_tag: None,
                command: FastCommand::Echo { payload: b"hello" },
            },
            FastRequest {
                key_hash: None,
                route_shard: None,
                key_tag: None,
                command: FastCommand::ConfigGet {
                    patterns: vec![b"*".as_slice()],
                },
            },
            FastRequest {
                key_hash: None,
                route_shard: None,
                key_tag: None,
                command: FastCommand::ClientSetName { name: b"bench" },
            },
            FastRequest {
                key_hash: None,
                route_shard: None,
                key_tag: None,
                command: FastCommand::DbSize,
            },
        ];

        for request in requests {
            let mut encoded = Vec::new();
            FastCodec::encode_request(&request, &mut encoded);
            let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
            assert_eq!(decoded, request);
        }
    }

    #[test]
    fn round_trips_hash_object_request() {
        let request = FastRequest {
            key_hash: Some(12),
            route_shard: Some(1),
            key_tag: None,
            command: FastCommand::HSet {
                key: b"user:1",
                field: b"name",
                value: b"ada",
            },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);
        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
    }

    #[test]
    fn round_trips_multivalue_object_request() {
        let request = FastRequest {
            key_hash: Some(13),
            route_shard: None,
            key_tag: None,
            command: FastCommand::HMGet {
                key: b"user:1",
                fields: vec![b"name".as_slice(), b"email".as_slice()],
            },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);
        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
    }

    #[test]
    fn round_trips_range_and_zadd_requests() {
        let range = FastRequest {
            key_hash: Some(14),
            route_shard: None,
            key_tag: None,
            command: FastCommand::LRange {
                key: b"items",
                start: -2,
                stop: -1,
            },
        };
        let zadd = FastRequest {
            key_hash: Some(15),
            route_shard: None,
            key_tag: None,
            command: FastCommand::ZAdd {
                key: b"scores",
                score: 42.5,
                member: b"ada",
            },
        };
        for request in [range, zadd] {
            let mut encoded = Vec::new();
            FastCodec::encode_request(&request, &mut encoded);
            let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
            assert_eq!(decoded, request);
        }
    }

    #[test]
    fn round_trips_generic_resp_command_request() {
        let request = FastRequest {
            key_hash: None,
            route_shard: None,
            key_tag: None,
            command: FastCommand::RespCommand {
                parts: vec![
                    b"HSET".as_slice(),
                    b"h".as_slice(),
                    b"f".as_slice(),
                    b"v".as_slice(),
                ],
            },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);
        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
        assert_eq!(decoded.command.route_key(), Some(b"h".as_slice()));
    }

    #[test]
    fn round_trips_opcode_redis_command_request() {
        let request = FastRequest {
            key_hash: Some(99),
            route_shard: Some(2),
            key_tag: None,
            command: FastCommand::RedisCommand {
                kind: FastCommandKind::Restore,
                args: vec![
                    b"restore-key".as_slice(),
                    b"0".as_slice(),
                    b"serialized".as_slice(),
                    b"REPLACE".as_slice(),
                ],
            },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);
        assert_eq!(encoded[2], FastCommandKind::Restore as u8);
        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
        assert_eq!(decoded.command.route_key(), Some(b"restore-key".as_slice()));
    }

    #[test]
    fn redis_command_flag_disambiguates_typed_opcodes() {
        let request = FastRequest {
            key_hash: None,
            route_shard: None,
            key_tag: None,
            command: FastCommand::RedisCommand {
                kind: FastCommandKind::Set,
                args: vec![b"key".as_slice(), b"value".as_slice(), b"NX".as_slice()],
            },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);

        assert_eq!(encoded[2], FastCommandKind::Set as u8);
        assert_ne!(encoded[3] & FAST_FLAG_REDIS_COMMAND_ARGS, 0);
        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
    }

    #[test]
    fn opcode_redis_command_uses_compact_argument_body() {
        let args = vec![
            b"restore-key".as_slice(),
            b"0".as_slice(),
            b"serialized".as_slice(),
            b"REPLACE".as_slice(),
        ];
        let request = FastRequest {
            key_hash: None,
            route_shard: None,
            key_tag: None,
            command: FastCommand::RedisCommand {
                kind: FastCommandKind::Restore,
                args: args.clone(),
            },
        };
        let mut encoded = Vec::new();
        FastCodec::encode_request(&request, &mut encoded);

        let body_len = u32::from_le_bytes(encoded[4..8].try_into().unwrap()) as usize;
        let legacy_body_len = 4 + args.iter().map(|arg| 4 + arg.len()).sum::<usize>();
        let compact_body_len = 1 + args.iter().map(|arg| 1 + arg.len()).sum::<usize>();
        assert_eq!(encoded.len(), REQUEST_HEADER_LEN + body_len);
        assert_eq!(body_len, compact_body_len);
        assert!(body_len < legacy_body_len);

        let decoded = FastCodec::decode_request(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, request);
    }

    #[test]
    fn encodes_value_response() {
        let mut encoded = Vec::new();
        FastCodec::encode_response(&FastResponse::Value(b"payload".to_vec()), &mut encoded);
        assert_eq!(encoded[0], super::FAST_RESPONSE_MAGIC);
        assert_eq!(encoded[1], FAST_PROTOCOL_VERSION);
    }

    #[test]
    fn round_trips_integer_response() {
        let mut encoded = Vec::new();
        FastCodec::encode_response(&FastResponse::Integer(42), &mut encoded);
        let decoded = FastCodec::decode_response(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, FastResponse::Integer(42));
    }

    #[test]
    fn round_trips_array_and_float_responses() {
        let array = FastResponse::Array(vec![
            Some(b"ada".to_vec()),
            None,
            Some(b"lovelace".to_vec()),
        ]);
        let float = FastResponse::Float(12.25);
        for response in [array, float] {
            let mut encoded = Vec::new();
            FastCodec::encode_response(&response, &mut encoded);
            let decoded = FastCodec::decode_response(&encoded).unwrap().unwrap().0;
            assert_eq!(decoded, response);
        }
    }
}
