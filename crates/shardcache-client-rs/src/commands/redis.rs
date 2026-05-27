use std::io::Write;

use crate::commands::ScnpCommand;
use crate::connection::ScnpConnection;
use crate::error::{Result, ShardCacheClientError};
use crate::protocol::{FAST_FLAG_REDIS_COMMAND_ARGS, ROUTED_FLAGS};
use crate::routing::ShardCacheRoute;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisResponse {
    Ok,
    Null,
    Integer(i64),
    Value(Vec<u8>),
    Array(Vec<RedisResponse>),
    Map(Vec<(RedisResponse, RedisResponse)>),
    Set(Vec<RedisResponse>),
    Push(Vec<RedisResponse>),
    Float(f64),
    Boolean(bool),
}

impl RedisResponse {
    /// Decodes a RESP2/RESP3 payload returned by the generic SCNP Redis wrapper.
    pub fn from_resp_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0usize;
        let response = parse_resp_value(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(ShardCacheClientError::Protocol(
                "RESP response has trailing bytes".into(),
            ));
        }
        Ok(response)
    }

    /// If this value contains an embedded RESP payload, decode it.
    ///
    /// Some less-common Redis commands return nested RESP through the native
    /// value status. This helper lets callers opt into decoding those payloads
    /// without making ordinary bulk-string values ambiguous.
    pub fn decode_resp_value(self) -> Result<Self> {
        match self {
            Self::Value(bytes) => Self::from_resp_bytes(&bytes),
            other => Ok(other),
        }
    }
}

macro_rules! define_redis_command_kind {
    ($($variant:ident => ($name:literal, $opcode:expr),)+) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(u8)]
        pub enum RedisCommandKind {
            $($variant = $opcode,)+
        }

        impl RedisCommandKind {
            pub fn name(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }

            pub fn opcode(self) -> u8 {
                self as u8
            }

            pub fn from_name(name: &[u8]) -> Option<Self> {
                REDIS_COMMAND_KINDS
                    .iter()
                    .copied()
                    .find(|kind| name.eq_ignore_ascii_case(kind.name().as_bytes()))
            }

            pub fn from_opcode(opcode: u8) -> Option<Self> {
                match opcode {
                    $($opcode => Some(Self::$variant),)+
                    _ => None,
                }
            }

            pub fn all() -> &'static [Self] {
                REDIS_COMMAND_KINDS
            }
        }

        pub const REDIS_COMMAND_KINDS: &[RedisCommandKind] = &[
            $(RedisCommandKind::$variant,)+
        ];
    };
}

define_redis_command_kind! {
    Append => ("APPEND", 80),
    Auth => ("AUTH", 60),
    BitCount => ("BITCOUNT", 81),
    BitField => ("BITFIELD", 82),
    BitOp => ("BITOP", 83),
    BitPos => ("BITPOS", 84),
    BLMove => ("BLMOVE", 160),
    BLMPop => ("BLMPOP", 161),
    BLPop => ("BLPOP", 162),
    BRPop => ("BRPOP", 163),
    BRPopLPush => ("BRPOPLPUSH", 173),
    BZMPop => ("BZMPOP", 201),
    BZPopMax => ("BZPOPMAX", 202),
    BZPopMin => ("BZPOPMIN", 203),
    Client => ("CLIENT", 77),
    Command => ("COMMAND", 66),
    Config => ("CONFIG", 76),
    Copy => ("COPY", 100),
    DbSize => ("DBSIZE", 69),
    Decr => ("DECR", 85),
    DecrBy => ("DECRBY", 86),
    Delete => ("DEL", 5),
    Discard => ("DISCARD", 122),
    Dump => ("DUMP", 101),
    Echo => ("ECHO", 64),
    Exec => ("EXEC", 121),
    Exists => ("EXISTS", 6),
    Expire => ("EXPIRE", 8),
    ExpireAt => ("EXPIREAT", 102),
    ExpireTime => ("EXPIRETIME", 103),
    FlushAll => ("FLUSHALL", 78),
    FlushDb => ("FLUSHDB", 79),
    Get => ("GET", 1),
    GetBit => ("GETBIT", 87),
    GetDel => ("GETDEL", 88),
    GetEx => ("GETEX", 4),
    GetRange => ("GETRANGE", 89),
    Substr => ("SUBSTR", 126),
    GetSet => ("GETSET", 90),
    Hello => ("HELLO", 61),
    HDel => ("HDEL", 22),
    HExists => ("HEXISTS", 140),
    HGet => ("HGET", 21),
    HGetAll => ("HGETALL", 141),
    HIncrBy => ("HINCRBY", 142),
    HIncrByFloat => ("HINCRBYFLOAT", 143),
    HKeys => ("HKEYS", 144),
    HLen => ("HLEN", 23),
    HMGet => ("HMGET", 24),
    HMSet => ("HMSET", 145),
    HRandField => ("HRANDFIELD", 146),
    HScan => ("HSCAN", 147),
    HSet => ("HSET", 20),
    HSetNx => ("HSETNX", 148),
    HStrLen => ("HSTRLEN", 149),
    HVals => ("HVALS", 150),
    Info => ("INFO", 65),
    Incr => ("INCR", 91),
    IncrBy => ("INCRBY", 92),
    IncrByFloat => ("INCRBYFLOAT", 93),
    Keys => ("KEYS", 104),
    LIndex => ("LINDEX", 35),
    LInsert => ("LINSERT", 164),
    LLen => ("LLEN", 34),
    LMove => ("LMOVE", 165),
    LMPop => ("LMPOP", 166),
    LPop => ("LPOP", 32),
    LPush => ("LPUSH", 30),
    LPushX => ("LPUSHX", 167),
    LRange => ("LRANGE", 36),
    LRem => ("LREM", 168),
    LSet => ("LSET", 169),
    LTrim => ("LTRIM", 170),
    Memory => ("MEMORY", 105),
    MGet => ("MGET", 10),
    MSet => ("MSET", 11),
    MSetNx => ("MSETNX", 94),
    Multi => ("MULTI", 120),
    Object => ("OBJECT", 106),
    Persist => ("PERSIST", 107),
    PExpire => ("PEXPIRE", 108),
    PExpireAt => ("PEXPIREAT", 109),
    PExpireTime => ("PEXPIRETIME", 110),
    Ping => ("PING", 9),
    PSetEx => ("PSETEX", 95),
    PTtl => ("PTTL", 111),
    Quit => ("QUIT", 63),
    RandomKey => ("RANDOMKEY", 112),
    Rename => ("RENAME", 113),
    RenameNx => ("RENAMENX", 114),
    Restore => ("RESTORE", 115),
    RestoreAsking => ("RESTORE-ASKING", 125),
    RPop => ("RPOP", 33),
    RPopLPush => ("RPOPLPUSH", 171),
    RPush => ("RPUSH", 31),
    RPushX => ("RPUSHX", 172),
    SAdd => ("SADD", 40),
    Scan => ("SCAN", 116),
    SCard => ("SCARD", 43),
    SDiff => ("SDIFF", 180),
    SDiffStore => ("SDIFFSTORE", 181),
    Select => ("SELECT", 62),
    Set => ("SET", 2),
    SetBit => ("SETBIT", 96),
    SetEx => ("SETEX", 3),
    SetNx => ("SETNX", 97),
    SetRange => ("SETRANGE", 98),
    SInter => ("SINTER", 182),
    SInterStore => ("SINTERSTORE", 183),
    SIsMember => ("SISMEMBER", 42),
    SMembers => ("SMEMBERS", 44),
    SMIsMember => ("SMISMEMBER", 184),
    SMove => ("SMOVE", 185),
    SPop => ("SPOP", 186),
    SRandMember => ("SRANDMEMBER", 187),
    SRem => ("SREM", 41),
    SScan => ("SSCAN", 188),
    StrLen => ("STRLEN", 99),
    SUnion => ("SUNION", 189),
    SUnionStore => ("SUNIONSTORE", 190),
    Time => ("TIME", 70),
    Touch => ("TOUCH", 117),
    Ttl => ("TTL", 7),
    Type => ("TYPE", 118),
    Unlink => ("UNLINK", 119),
    Unwatch => ("UNWATCH", 124),
    Watch => ("WATCH", 123),
    ZAdd => ("ZADD", 50),
    ZCard => ("ZCARD", 53),
    ZCount => ("ZCOUNT", 204),
    ZDiff => ("ZDIFF", 205),
    ZDiffStore => ("ZDIFFSTORE", 206),
    ZIncrBy => ("ZINCRBY", 207),
    ZInter => ("ZINTER", 208),
    ZInterCard => ("ZINTERCARD", 209),
    ZInterStore => ("ZINTERSTORE", 210),
    ZLexCount => ("ZLEXCOUNT", 211),
    ZMPop => ("ZMPOP", 212),
    ZMScore => ("ZMSCORE", 213),
    ZPopMax => ("ZPOPMAX", 214),
    ZPopMin => ("ZPOPMIN", 215),
    ZRandMember => ("ZRANDMEMBER", 216),
    ZRange => ("ZRANGE", 54),
    ZRangeByLex => ("ZRANGEBYLEX", 217),
    ZRangeByScore => ("ZRANGEBYSCORE", 218),
    ZRangeStore => ("ZRANGESTORE", 219),
    ZRank => ("ZRANK", 220),
    ZRem => ("ZREM", 51),
    ZRemRangeByLex => ("ZREMRANGEBYLEX", 221),
    ZRemRangeByRank => ("ZREMRANGEBYRANK", 222),
    ZRemRangeByScore => ("ZREMRANGEBYSCORE", 223),
    ZRevRange => ("ZREVRANGE", 224),
    ZRevRangeByLex => ("ZREVRANGEBYLEX", 225),
    ZRevRangeByScore => ("ZREVRANGEBYSCORE", 226),
    ZRevRank => ("ZREVRANK", 227),
    ZScan => ("ZSCAN", 228),
    ZScore => ("ZSCORE", 52),
    ZUnion => ("ZUNION", 229),
    ZUnionStore => ("ZUNIONSTORE", 230),
}

impl RedisCommandKind {
    pub(crate) fn route_keys<'a>(self, args: &[&'a [u8]]) -> RedisCommandRouteKeys<'a> {
        match self {
            Self::Auth
            | Self::Hello
            | Self::Select
            | Self::Quit
            | Self::Ping
            | Self::Echo
            | Self::Info
            | Self::Command
            | Self::Config
            | Self::DbSize
            | Self::Time
            | Self::Client
            | Self::Multi
            | Self::Exec
            | Self::Discard
            | Self::Unwatch => RedisCommandRouteKeys::None,
            Self::Keys | Self::RandomKey | Self::Scan | Self::FlushAll | Self::FlushDb => {
                RedisCommandRouteKeys::AllShards
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
}

pub(crate) enum RedisCommandRouteKeys<'a> {
    None,
    AllShards,
    Keys(Vec<&'a [u8]>),
}

pub(crate) struct RedisCommand<'args> {
    kind: RedisCommandKind,
    args: &'args [&'args [u8]],
    route: Option<ShardCacheRoute>,
}

pub(crate) struct RedisRespCommand<'args> {
    command: &'args [u8],
    args: &'args [&'args [u8]],
}

impl<'args> RedisRespCommand<'args> {
    pub(crate) fn new(command: &'args [u8], args: &'args [&'args [u8]]) -> Self {
        Self { command, args }
    }
}

impl<'args> RedisCommand<'args> {
    pub(crate) fn new(kind: RedisCommandKind, args: &'args [&'args [u8]]) -> Self {
        Self {
            kind,
            args,
            route: None,
        }
    }

    pub(crate) fn routed(
        kind: RedisCommandKind,
        route: Option<ShardCacheRoute>,
        args: &'args [&'args [u8]],
    ) -> Self {
        Self { kind, args, route }
    }
}

impl ScnpCommand for RedisCommand<'_> {
    type Output = RedisResponse;

    const NAME: &'static str = "REDIS";
    const OPCODE: u8 = 0;

    fn opcode(&self) -> u8 {
        self.kind.opcode()
    }

    fn flags(&self) -> u8 {
        FAST_FLAG_REDIS_COMMAND_ARGS | self.route.map_or(0, |_| ROUTED_FLAGS)
    }

    fn body_len(&self) -> usize {
        self.route.map_or(0, |_| 20) + compact_arg_list_len(self.args)
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        if let Some(route) = self.route {
            route.write_to(w)?;
        }
        write_compact_arg_list(w, self.args)
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        conn.read_native_redis_response(self.kind.name())
    }
}

impl ScnpCommand for RedisRespCommand<'_> {
    type Output = RedisResponse;

    const NAME: &'static str = "RESP";
    const OPCODE: u8 = 200;

    fn body_len(&self) -> usize {
        redis_resp_command_body_len(self.command, self.args)
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        write_redis_resp_command_body(w, self.command, self.args)
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        conn.read_resp_redis_response("RESP")
    }
}

pub(crate) fn write_request(
    conn: &mut ScnpConnection,
    kind: RedisCommandKind,
    route: Option<ShardCacheRoute>,
    args: &[&[u8]],
) -> Result<()> {
    conn.write_header(
        kind.opcode(),
        FAST_FLAG_REDIS_COMMAND_ARGS | route.map_or(0, |_| ROUTED_FLAGS),
        (route.map_or(0, |_| 20) + compact_arg_list_len(args)) as u32,
    )?;
    if let Some(route) = route {
        route.write_to(&mut conn.w)?;
    }
    write_compact_arg_list(&mut conn.w, args)
}

pub(crate) fn write_resp_request(
    conn: &mut ScnpConnection,
    command: &[u8],
    args: &[&[u8]],
) -> Result<()> {
    conn.write_header(200, 0, redis_resp_command_body_len(command, args) as u32)?;
    write_redis_resp_command_body(&mut conn.w, command, args)
}

fn redis_resp_command_body_len(command: &[u8], args: &[&[u8]]) -> usize {
    4 + 4
        + command.len()
        + args
            .iter()
            .map(|arg| 4usize.saturating_add(arg.len()))
            .sum::<usize>()
}

fn write_redis_resp_command_body<W: Write>(
    w: &mut W,
    command: &[u8],
    args: &[&[u8]],
) -> Result<()> {
    w.write_all(&((args.len() + 1) as u32).to_le_bytes())?;
    w.write_all(&(command.len() as u32).to_le_bytes())?;
    w.write_all(command)?;
    for arg in args {
        w.write_all(&(arg.len() as u32).to_le_bytes())?;
        w.write_all(arg)?;
    }
    Ok(())
}

fn all_route_keys<'a>(args: &[&'a [u8]]) -> RedisCommandRouteKeys<'a> {
    RedisCommandRouteKeys::Keys(args.to_vec())
}

fn first_n_route_keys<'a>(args: &[&'a [u8]], count: usize) -> RedisCommandRouteKeys<'a> {
    RedisCommandRouteKeys::Keys(args.iter().take(count).copied().collect())
}

fn every_nth_route_key<'a>(
    args: &[&'a [u8]],
    start: usize,
    step: usize,
) -> RedisCommandRouteKeys<'a> {
    RedisCommandRouteKeys::Keys(args.iter().skip(start).step_by(step).copied().collect())
}

fn first_and_tail_route_keys<'a>(
    args: &[&'a [u8]],
    first_count: usize,
    tail_start: usize,
) -> RedisCommandRouteKeys<'a> {
    let mut keys = Vec::new();
    keys.extend(args.iter().take(first_count).copied());
    keys.extend(args.iter().skip(tail_start).copied());
    RedisCommandRouteKeys::Keys(keys)
}

fn route_keys_before_last_arg<'a>(args: &[&'a [u8]]) -> RedisCommandRouteKeys<'a> {
    RedisCommandRouteKeys::Keys(
        args.iter()
            .take(args.len().saturating_sub(1))
            .copied()
            .collect(),
    )
}

fn counted_route_keys<'a>(args: &[&'a [u8]], numkeys_index: usize) -> RedisCommandRouteKeys<'a> {
    match counted_key_span(args, numkeys_index) {
        Some(keys) => RedisCommandRouteKeys::Keys(keys.to_vec()),
        None => RedisCommandRouteKeys::AllShards,
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

fn zaggregate_store_route_keys<'a>(args: &[&'a [u8]]) -> RedisCommandRouteKeys<'a> {
    if args.len() < 2 {
        return first_n_route_keys(args, 1);
    }
    let Some(numkeys) = std::str::from_utf8(args[1])
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return RedisCommandRouteKeys::AllShards;
    };
    let mut keys = Vec::with_capacity(numkeys + 1);
    keys.push(args[0]);
    keys.extend(args.iter().skip(2).take(numkeys).copied());
    RedisCommandRouteKeys::Keys(keys)
}

fn compact_arg_list_len(args: &[&[u8]]) -> usize {
    var_u32_len(args.len() as u32)
        + args
            .iter()
            .map(|arg| var_u32_len(arg.len() as u32) + arg.len())
            .sum::<usize>()
}

fn var_u32_len(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn write_compact_arg_list<W: Write>(w: &mut W, args: &[&[u8]]) -> Result<()> {
    write_var_u32(w, args.len() as u32)?;
    for arg in args {
        write_var_u32(w, arg.len() as u32)?;
        w.write_all(arg)?;
    }
    Ok(())
}

fn write_var_u32<W: Write>(w: &mut W, mut value: u32) -> Result<()> {
    while value >= 0x80 {
        w.write_all(&[((value as u8) & 0x7f) | 0x80])?;
        value >>= 7;
    }
    w.write_all(&[value as u8])?;
    Ok(())
}

fn parse_resp_value(buf: &[u8], cursor: &mut usize) -> Result<RedisResponse> {
    let Some(prefix) = buf.get(*cursor).copied() else {
        return Err(ShardCacheClientError::Protocol(
            "truncated RESP response".into(),
        ));
    };
    *cursor += 1;
    match prefix {
        b'+' => {
            let line = read_resp_line(buf, cursor, "simple string")?;
            if line.eq_ignore_ascii_case(b"OK") {
                Ok(RedisResponse::Ok)
            } else {
                Ok(RedisResponse::Value(line.to_vec()))
            }
        }
        b'-' => {
            let line = read_resp_line(buf, cursor, "error")?;
            Err(ShardCacheClientError::Protocol(
                String::from_utf8_lossy(line).into_owned(),
            ))
        }
        b':' => {
            let line = read_resp_line(buf, cursor, "integer")?;
            Ok(RedisResponse::Integer(parse_resp_i64(line, "integer")?))
        }
        b',' => {
            let line = read_resp_line(buf, cursor, "double")?;
            let value = std::str::from_utf8(line)
                .map_err(|_| ShardCacheClientError::Protocol("invalid RESP double".into()))?
                .parse::<f64>()
                .map_err(|_| ShardCacheClientError::Protocol("invalid RESP double".into()))?;
            Ok(RedisResponse::Float(value))
        }
        b'#' => {
            let line = read_resp_line(buf, cursor, "boolean")?;
            match line {
                b"t" => Ok(RedisResponse::Boolean(true)),
                b"f" => Ok(RedisResponse::Boolean(false)),
                _ => Err(ShardCacheClientError::Protocol(
                    "invalid RESP boolean".into(),
                )),
            }
        }
        b'_' => {
            expect_resp_crlf(buf, cursor, "null")?;
            Ok(RedisResponse::Null)
        }
        b'$' => parse_resp_bulk(buf, cursor, false),
        b'!' => parse_resp_bulk(buf, cursor, true),
        b'=' => parse_resp_verbatim(buf, cursor),
        b'*' => parse_resp_sequence(buf, cursor, RedisSequenceKind::Array),
        b'~' => parse_resp_sequence(buf, cursor, RedisSequenceKind::Set),
        b'>' => parse_resp_sequence(buf, cursor, RedisSequenceKind::Push),
        b'%' => parse_resp_map(buf, cursor),
        b'(' => {
            let line = read_resp_line(buf, cursor, "big number")?;
            Ok(RedisResponse::Value(line.to_vec()))
        }
        other => Err(ShardCacheClientError::Protocol(format!(
            "unsupported RESP prefix: 0x{other:02X}"
        ))),
    }
}

#[derive(Clone, Copy)]
enum RedisSequenceKind {
    Array,
    Set,
    Push,
}

fn parse_resp_sequence(
    buf: &[u8],
    cursor: &mut usize,
    kind: RedisSequenceKind,
) -> Result<RedisResponse> {
    let line = read_resp_line(buf, cursor, "array length")?;
    let len = parse_resp_i64(line, "array length")?;
    if len < 0 {
        return Ok(RedisResponse::Null);
    }
    let mut values = Vec::with_capacity(len as usize);
    for _ in 0..len {
        values.push(parse_resp_value(buf, cursor)?);
    }
    match kind {
        RedisSequenceKind::Array => Ok(RedisResponse::Array(values)),
        RedisSequenceKind::Set => Ok(RedisResponse::Set(values)),
        RedisSequenceKind::Push => Ok(RedisResponse::Push(values)),
    }
}

fn parse_resp_map(buf: &[u8], cursor: &mut usize) -> Result<RedisResponse> {
    let line = read_resp_line(buf, cursor, "map length")?;
    let len = parse_resp_i64(line, "map length")?;
    if len < 0 {
        return Ok(RedisResponse::Null);
    }
    let mut entries = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let key = parse_resp_value(buf, cursor)?;
        let value = parse_resp_value(buf, cursor)?;
        entries.push((key, value));
    }
    Ok(RedisResponse::Map(entries))
}

fn parse_resp_bulk(buf: &[u8], cursor: &mut usize, is_error: bool) -> Result<RedisResponse> {
    let line = read_resp_line(buf, cursor, "bulk length")?;
    let len = parse_resp_i64(line, "bulk length")?;
    if len < 0 {
        return Ok(RedisResponse::Null);
    }
    let len = len as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| ShardCacheClientError::Protocol("RESP bulk length overflow".into()))?;
    if end.checked_add(2).is_none_or(|end| end > buf.len()) {
        return Err(ShardCacheClientError::Protocol(
            "truncated RESP bulk string".into(),
        ));
    }
    let value = &buf[*cursor..end];
    *cursor = end;
    expect_resp_crlf(buf, cursor, "bulk string")?;
    if is_error {
        return Err(ShardCacheClientError::Protocol(
            String::from_utf8_lossy(value).into_owned(),
        ));
    }
    Ok(RedisResponse::Value(value.to_vec()))
}

fn parse_resp_verbatim(buf: &[u8], cursor: &mut usize) -> Result<RedisResponse> {
    let value = match parse_resp_bulk(buf, cursor, false)? {
        RedisResponse::Value(value) => value,
        other => return Ok(other),
    };
    let payload = value
        .get(4..)
        .filter(|_| value.get(3) == Some(&b':'))
        .unwrap_or(value.as_slice());
    Ok(RedisResponse::Value(payload.to_vec()))
}

fn read_resp_line<'a>(buf: &'a [u8], cursor: &mut usize, label: &str) -> Result<&'a [u8]> {
    let start = *cursor;
    let Some(rel_end) = buf[start..].windows(2).position(|window| window == b"\r\n") else {
        return Err(ShardCacheClientError::Protocol(format!(
            "truncated RESP {label}"
        )));
    };
    let end = start + rel_end;
    *cursor = end + 2;
    Ok(&buf[start..end])
}

fn expect_resp_crlf(buf: &[u8], cursor: &mut usize, label: &str) -> Result<()> {
    if buf.get(*cursor..(*cursor).saturating_add(2)) != Some(b"\r\n") {
        return Err(ShardCacheClientError::Protocol(format!(
            "missing RESP CRLF after {label}"
        )));
    }
    *cursor += 2;
    Ok(())
}

fn parse_resp_i64(raw: &[u8], label: &str) -> Result<i64> {
    std::str::from_utf8(raw)
        .map_err(|_| ShardCacheClientError::Protocol(format!("invalid RESP {label}")))?
        .parse::<i64>()
        .map_err(|_| ShardCacheClientError::Protocol(format!("invalid RESP {label}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_names_round_trip() {
        for kind in RedisCommandKind::all() {
            assert_eq!(
                RedisCommandKind::from_name(kind.name().as_bytes()),
                Some(*kind)
            );
            assert_eq!(
                RedisCommandKind::from_opcode(kind.opcode()),
                Some(*kind),
                "{}",
                kind.name()
            );
        }
    }

    #[test]
    fn compact_arg_list_len_matches_wire_encoding() {
        let args = [b"alpha".as_slice(), b"beta-beta".as_slice(), b"".as_slice()];
        let mut encoded = Vec::new();
        write_compact_arg_list(&mut encoded, &args).unwrap();
        assert_eq!(encoded.len(), compact_arg_list_len(&args));
        assert_eq!(encoded, b"\x03\x05alpha\x09beta-beta\x00");
    }

    #[test]
    fn parses_resp_scalars() {
        assert_eq!(
            RedisResponse::from_resp_bytes(b"+OK\r\n").unwrap(),
            RedisResponse::Ok
        );
        assert_eq!(
            RedisResponse::from_resp_bytes(b"+PONG\r\n").unwrap(),
            RedisResponse::Value(b"PONG".to_vec())
        );
        assert_eq!(
            RedisResponse::from_resp_bytes(b":42\r\n").unwrap(),
            RedisResponse::Integer(42)
        );
        assert_eq!(
            RedisResponse::from_resp_bytes(b"$5\r\nhello\r\n").unwrap(),
            RedisResponse::Value(b"hello".to_vec())
        );
        assert_eq!(
            RedisResponse::from_resp_bytes(b"$-1\r\n").unwrap(),
            RedisResponse::Null
        );
    }

    #[test]
    fn parses_resp_nested_values() {
        let response =
            RedisResponse::from_resp_bytes(b"*3\r\n$5\r\nalpha\r\n$-1\r\n:7\r\n").unwrap();
        assert_eq!(
            response,
            RedisResponse::Array(vec![
                RedisResponse::Value(b"alpha".to_vec()),
                RedisResponse::Null,
                RedisResponse::Integer(7),
            ])
        );

        let response =
            RedisResponse::from_resp_bytes(b"%1\r\n+role\r\n*2\r\n+master\r\n:1\r\n").unwrap();
        assert_eq!(
            response,
            RedisResponse::Map(vec![(
                RedisResponse::Value(b"role".to_vec()),
                RedisResponse::Array(vec![
                    RedisResponse::Value(b"master".to_vec()),
                    RedisResponse::Integer(1),
                ]),
            )])
        );
    }

    #[test]
    fn resp_error_is_protocol_error() {
        let err = RedisResponse::from_resp_bytes(b"-ERR nope\r\n").unwrap_err();
        assert!(err.to_string().contains("ERR nope"));
    }
}
