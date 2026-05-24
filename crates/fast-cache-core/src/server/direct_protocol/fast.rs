use super::*;
use crate::protocol::FastCommandKind;
use crate::server::commands::BorrowedCommandContext;
use crate::storage::BorrowedCommand;

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn fast_command_mutates_value(command: &FastCommand<'_>) -> bool {
        match command {
            FastCommand::RespCommand { parts } => parts
                .first()
                .is_some_and(|command| RawCommandDispatcher::mutates_value(command)),
            FastCommand::RedisCommand { kind, .. } => kind
                .redis_name()
                .is_some_and(|command| RawCommandDispatcher::mutates_value(command.as_bytes())),
            other => FastCommandDispatcher::mutates_value(other),
        }
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn shared_execute_fast_into(
        store: &EmbeddedStore,
        request: FastRequest<'_>,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        started_at: Instant,
    ) {
        match request.command {
            FastCommand::RespCommand { parts } => DirectProtocol::execute_fast_resp_command(
                store,
                parts,
                out,
                single_threaded,
                started_at,
            ),
            FastCommand::RedisCommand { kind, args } => {
                DirectProtocol::execute_fast_redis_opcode_command(
                    store,
                    kind,
                    args,
                    out,
                    fast_write_queue,
                    single_threaded,
                    started_at,
                )
            }
            _ => {
                let request_for_fallback = request.clone();
                match FastCommandDispatcher::execute(store, request, out, single_threaded) {
                    true => {}
                    false => {
                        if !DirectProtocol::execute_decoded_fast_redis_command(
                            store,
                            &request_for_fallback.command,
                            out,
                            single_threaded,
                            started_at,
                        ) {
                            ServerWire::write_fast_error(out, "ERR unsupported command");
                        }
                    }
                }
            }
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    fn execute_fast_resp_command(
        store: &EmbeddedStore,
        parts: Vec<&[u8]>,
        out: &mut BytesMut,
        single_threaded: bool,
        started_at: Instant,
    ) {
        match parts.split_first() {
            Some((command, args)) => DirectProtocol::execute_fast_redis_parts(
                store,
                command,
                args,
                out,
                single_threaded,
                started_at,
            ),
            None => {
                ServerWire::write_fast_error(out, "ERR empty RESP command");
            }
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    fn execute_fast_redis_opcode_command(
        store: &EmbeddedStore,
        kind: FastCommandKind,
        args: Vec<&[u8]>,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        started_at: Instant,
    ) {
        let mut direct_args = RespDirectArgs::new();
        direct_args.extend(args.iter().copied());
        if let Some(command) = DirectProtocol::parse_redis_opcode_direct_command(kind, direct_args)
        {
            DirectProtocol::shared_execute_fast_direct_cmd_into(
                store,
                command,
                out,
                fast_write_queue,
                single_threaded,
                started_at,
            );
            return;
        }

        match kind.redis_name() {
            Some(command) => DirectProtocol::execute_fast_redis_borrowed_parts(
                store,
                command.as_bytes(),
                &args,
                out,
                single_threaded,
                started_at,
            ),
            None => ServerWire::write_fast_error(out, "ERR unsupported command"),
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    fn execute_fast_redis_borrowed_parts(
        store: &EmbeddedStore,
        command: &[u8],
        args: &[&[u8]],
        out: &mut BytesMut,
        single_threaded: bool,
        _started_at: Instant,
    ) {
        let mut borrowed_parts = Vec::with_capacity(args.len() + 1);
        borrowed_parts.push(command);
        borrowed_parts.extend(args.iter().copied());
        match BorrowedCommand::from_parts(&borrowed_parts) {
            Ok(command) => {
                let start = ServerWire::begin_fast_value(out);
                command.execute_borrowed(BorrowedCommandContext {
                    store,
                    out,
                    fast_write_queue: None,
                    single_threaded,
                });
                ServerWire::finish_fast_value(out, start);
            }
            Err(error) => {
                ServerWire::write_fast_error(out, &format!("ERR {error}"));
            }
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    fn execute_fast_redis_parts(
        store: &EmbeddedStore,
        command: &[u8],
        args: &[&[u8]],
        out: &mut BytesMut,
        single_threaded: bool,
        started_at: Instant,
    ) {
        #[cfg(feature = "redis-compat")]
        if command.eq_ignore_ascii_case(b"FCNP.SCAN") {
            crate::commands::redis::write_fcnp_scan_fast_response(store, args, out);
            return;
        }
        #[cfg(feature = "redis-compat")]
        if command.eq_ignore_ascii_case(b"FCNP.SCANSHARD")
            || command.eq_ignore_ascii_case(b"FCNP.SCAN.SHARD")
        {
            crate::commands::redis::write_fcnp_scan_shard_fast_response(store, args, out);
            return;
        }
        let mut direct_args = RespDirectArgs::new();
        direct_args.extend(args.iter().copied());
        match DirectProtocol::parse_resp_direct_command(command, direct_args) {
            Some(command) => {
                let start = ServerWire::begin_fast_value(out);
                DirectProtocol::shared_execute_resp_direct_cmd_into(
                    store,
                    command,
                    out,
                    None,
                    single_threaded,
                    started_at,
                );
                ServerWire::finish_fast_value(out, start);
            }
            None => {
                DirectProtocol::execute_fast_redis_borrowed_parts(
                    store,
                    command,
                    args,
                    out,
                    single_threaded,
                    started_at,
                );
            }
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn decoded_fast_redis_command(
        command: &FastCommand<'_>,
    ) -> Option<DecodedFastRedisCommand> {
        DecodedFastRedisCommand::from_fast(command)
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    fn execute_decoded_fast_redis_command(
        store: &EmbeddedStore,
        command: &FastCommand<'_>,
        out: &mut BytesMut,
        single_threaded: bool,
        started_at: Instant,
    ) -> bool {
        let Some(decoded) = DirectProtocol::decoded_fast_redis_command(command) else {
            return false;
        };
        let args = decoded.args.iter().map(Vec::as_slice).collect::<Vec<_>>();
        DirectProtocol::execute_fast_redis_parts(
            store,
            decoded.command,
            &args,
            out,
            single_threaded,
            started_at,
        );
        true
    }
}

#[cfg(feature = "embedded")]
pub(in crate::server) struct DecodedFastRedisCommand {
    pub(in crate::server) command: &'static [u8],
    pub(in crate::server) args: Vec<Vec<u8>>,
}

#[cfg(feature = "embedded")]
impl DecodedFastRedisCommand {
    fn from_fast(command: &FastCommand<'_>) -> Option<Self> {
        let mut args = Vec::new();
        let name: &'static [u8] = match command {
            FastCommand::Ping(payload) => {
                if let Some(payload) = payload {
                    args.push(payload.to_vec());
                }
                b"PING".as_slice()
            }
            FastCommand::Auth => b"AUTH",
            FastCommand::Hello { proto } => {
                if let Some(proto) = proto {
                    args.push(proto.to_string().into_bytes());
                }
                b"HELLO"
            }
            FastCommand::Select { db } => {
                args.push(db.to_string().into_bytes());
                b"SELECT"
            }
            FastCommand::Quit => b"QUIT",
            FastCommand::Echo { payload } => {
                args.push(payload.to_vec());
                b"ECHO"
            }
            FastCommand::Info => b"INFO",
            FastCommand::Command => b"COMMAND",
            FastCommand::CommandDocs => {
                args.push(b"DOCS".to_vec());
                b"COMMAND"
            }
            FastCommand::ConfigGet { patterns } => {
                args.push(b"GET".to_vec());
                args.extend(patterns.iter().map(|pattern| pattern.to_vec()));
                b"CONFIG"
            }
            FastCommand::DbSize => b"DBSIZE",
            FastCommand::Time => b"TIME",
            FastCommand::ClientGetName => {
                args.push(b"GETNAME".to_vec());
                b"CLIENT"
            }
            FastCommand::ClientSetName { name } => {
                args.push(b"SETNAME".to_vec());
                args.push(name.to_vec());
                b"CLIENT"
            }
            FastCommand::ClientId => {
                args.push(b"ID".to_vec());
                b"CLIENT"
            }
            FastCommand::ClientList => {
                args.push(b"LIST".to_vec());
                b"CLIENT"
            }
            FastCommand::ClientKill => {
                args.push(b"KILL".to_vec());
                b"CLIENT"
            }
            FastCommand::Get { key } => {
                args.push(key.to_vec());
                b"GET"
            }
            FastCommand::Set { key, value } => {
                args.push(key.to_vec());
                args.push(value.to_vec());
                b"SET"
            }
            FastCommand::SetEx { key, value, ttl_ms } => {
                args.push(key.to_vec());
                args.push(ttl_ms.to_string().into_bytes());
                args.push(value.to_vec());
                b"PSETEX"
            }
            FastCommand::GetEx { key, ttl_ms } => {
                args.push(key.to_vec());
                args.push(b"PX".to_vec());
                args.push(ttl_ms.to_string().into_bytes());
                b"GETEX"
            }
            FastCommand::Delete { key } => {
                args.push(key.to_vec());
                b"DEL"
            }
            FastCommand::Exists { key } => {
                args.push(key.to_vec());
                b"EXISTS"
            }
            FastCommand::Ttl { key } => {
                args.push(key.to_vec());
                b"TTL"
            }
            FastCommand::Expire { key, ttl_ms } => {
                args.push(key.to_vec());
                args.push(ttl_ms.to_string().into_bytes());
                b"PEXPIRE"
            }
            FastCommand::MGet { keys } => {
                args.extend(keys.iter().map(|key| key.to_vec()));
                b"MGET"
            }
            FastCommand::MSet { items } => {
                for (key, value) in items {
                    args.push(key.to_vec());
                    args.push(value.to_vec());
                }
                b"MSET"
            }
            FastCommand::HSet { key, field, value } => {
                args.push(key.to_vec());
                args.push(field.to_vec());
                args.push(value.to_vec());
                b"HSET"
            }
            FastCommand::HGet { key, field } => {
                args.push(key.to_vec());
                args.push(field.to_vec());
                b"HGET"
            }
            FastCommand::HDel { key, field } => {
                args.push(key.to_vec());
                args.push(field.to_vec());
                b"HDEL"
            }
            FastCommand::HLen { key } => {
                args.push(key.to_vec());
                b"HLEN"
            }
            FastCommand::HMGet { key, fields } => {
                args.push(key.to_vec());
                args.extend(fields.iter().map(|field| field.to_vec()));
                b"HMGET"
            }
            FastCommand::LPush { key, values } => {
                args.push(key.to_vec());
                args.extend(values.iter().map(|value| value.to_vec()));
                b"LPUSH"
            }
            FastCommand::RPush { key, values } => {
                args.push(key.to_vec());
                args.extend(values.iter().map(|value| value.to_vec()));
                b"RPUSH"
            }
            FastCommand::LPop { key } => {
                args.push(key.to_vec());
                b"LPOP"
            }
            FastCommand::RPop { key } => {
                args.push(key.to_vec());
                b"RPOP"
            }
            FastCommand::LLen { key } => {
                args.push(key.to_vec());
                b"LLEN"
            }
            FastCommand::LIndex { key, index } => {
                args.push(key.to_vec());
                args.push(index.to_string().into_bytes());
                b"LINDEX"
            }
            FastCommand::LRange { key, start, stop } => {
                args.push(key.to_vec());
                args.push(start.to_string().into_bytes());
                args.push(stop.to_string().into_bytes());
                b"LRANGE"
            }
            FastCommand::SAdd { key, members } => {
                args.push(key.to_vec());
                args.extend(members.iter().map(|member| member.to_vec()));
                b"SADD"
            }
            FastCommand::SRem { key, members } => {
                args.push(key.to_vec());
                args.extend(members.iter().map(|member| member.to_vec()));
                b"SREM"
            }
            FastCommand::SIsMember { key, member } => {
                args.push(key.to_vec());
                args.push(member.to_vec());
                b"SISMEMBER"
            }
            FastCommand::SCard { key } => {
                args.push(key.to_vec());
                b"SCARD"
            }
            FastCommand::SMembers { key } => {
                args.push(key.to_vec());
                b"SMEMBERS"
            }
            FastCommand::ZAdd { key, score, member } => {
                args.push(key.to_vec());
                args.push(score.to_string().into_bytes());
                args.push(member.to_vec());
                b"ZADD"
            }
            FastCommand::ZRem { key, member } => {
                args.push(key.to_vec());
                args.push(member.to_vec());
                b"ZREM"
            }
            FastCommand::ZScore { key, member } => {
                args.push(key.to_vec());
                args.push(member.to_vec());
                b"ZSCORE"
            }
            FastCommand::ZCard { key } => {
                args.push(key.to_vec());
                b"ZCARD"
            }
            FastCommand::ZRange { key, start, stop } => {
                args.push(key.to_vec());
                args.push(start.to_string().into_bytes());
                args.push(stop.to_string().into_bytes());
                b"ZRANGE"
            }
            FastCommand::RespCommand { .. } | FastCommand::RedisCommand { .. } => return None,
        };
        Some(Self {
            command: name,
            args,
        })
    }
}
