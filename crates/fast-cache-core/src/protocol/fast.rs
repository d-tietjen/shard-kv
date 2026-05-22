use crate::{FastCacheError, Result};

/// Native fast-cache protocol, version 2.
///
/// Requests use an 8-byte raw-binary header:
/// `[magic:u8, version:u8, command:u8, flags:u8, body_len:u32le]`.
/// Optional routing fields live at the start of the body when their flag bits
/// are set, followed by command-specific binary fields. Field order is:
/// `key_hash:u64`, `route_shard:u32`, `key_tag:u64`. RESP remains supported
/// separately by the server ingress path.
pub const FAST_REQUEST_MAGIC: u8 = 0xFA;
pub const FAST_RESPONSE_MAGIC: u8 = 0xFB;
pub const FAST_PROTOCOL_VERSION: u8 = 2;

pub const FAST_FLAG_KEY_HASH: u8 = 0x01;
pub const FAST_FLAG_ROUTE_SHARD: u8 = 0x02;
pub const FAST_FLAG_KEY_TAG: u8 = 0x04;

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
    HSet = 20,
    HGet = 21,
    HDel = 22,
    HLen = 23,
    HMGet = 24,
    LPush = 30,
    RPush = 31,
    LPop = 32,
    RPop = 33,
    LLen = 34,
    LIndex = 35,
    LRange = 36,
    SAdd = 40,
    SRem = 41,
    SIsMember = 42,
    SCard = 43,
    SMembers = 44,
    ZAdd = 50,
    ZRem = 51,
    ZScore = 52,
    ZCard = 53,
    ZRange = 54,
    RespCommand = 200,
}

impl FastCommandKind {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Get),
            2 => Ok(Self::Set),
            3 => Ok(Self::SetEx),
            4 => Ok(Self::GetEx),
            5 => Ok(Self::Delete),
            6 => Ok(Self::Exists),
            7 => Ok(Self::Ttl),
            8 => Ok(Self::Expire),
            9 => Ok(Self::Ping),
            10 => Ok(Self::MGet),
            11 => Ok(Self::MSet),
            60 => Ok(Self::Auth),
            61 => Ok(Self::Hello),
            62 => Ok(Self::Select),
            63 => Ok(Self::Quit),
            64 => Ok(Self::Echo),
            65 => Ok(Self::Info),
            66 => Ok(Self::Command),
            67 => Ok(Self::CommandDocs),
            68 => Ok(Self::ConfigGet),
            69 => Ok(Self::DbSize),
            70 => Ok(Self::Time),
            71 => Ok(Self::ClientGetName),
            72 => Ok(Self::ClientSetName),
            73 => Ok(Self::ClientId),
            74 => Ok(Self::ClientList),
            75 => Ok(Self::ClientKill),
            20 => Ok(Self::HSet),
            21 => Ok(Self::HGet),
            22 => Ok(Self::HDel),
            23 => Ok(Self::HLen),
            24 => Ok(Self::HMGet),
            30 => Ok(Self::LPush),
            31 => Ok(Self::RPush),
            32 => Ok(Self::LPop),
            33 => Ok(Self::RPop),
            34 => Ok(Self::LLen),
            35 => Ok(Self::LIndex),
            36 => Ok(Self::LRange),
            40 => Ok(Self::SAdd),
            41 => Ok(Self::SRem),
            42 => Ok(Self::SIsMember),
            43 => Ok(Self::SCard),
            44 => Ok(Self::SMembers),
            50 => Ok(Self::ZAdd),
            51 => Ok(Self::ZRem),
            52 => Ok(Self::ZScore),
            53 => Ok(Self::ZCard),
            54 => Ok(Self::ZRange),
            200 => Ok(Self::RespCommand),
            other => Err(FastCacheError::Protocol(format!(
                "unsupported fast command id: {other}"
            ))),
        }
    }
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
        }
    }

    pub fn route_key(&self) -> Option<&'a [u8]> {
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
            | Self::ClientKill => None,
            Self::RespCommand { parts } => parts.get(1).copied(),
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
            | Self::ZRange { key, .. } => Some(key),
            Self::MGet { keys } => keys.first().copied(),
            Self::MSet { items } => items.first().map(|(key, _)| *key),
        }
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
        if flags & !(FAST_FLAG_KEY_HASH | FAST_FLAG_ROUTE_SHARD | FAST_FLAG_KEY_TAG) != 0 {
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
    use super::{FAST_PROTOCOL_VERSION, FastCodec, FastCommand, FastRequest, FastResponse};

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
