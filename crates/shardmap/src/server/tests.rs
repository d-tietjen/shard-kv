#[cfg(feature = "embedded")]
use super::commands::{RAW_DIRECT_CATALOG, RawCommandDispatcher, find_primary_raw_command};
use super::direct_protocol::*;
use super::transactions::{TransactionCoordinator, TransactionState};
use super::wire::*;
use super::*;
use crate::config::TransactionMode;
use crate::protocol::FastCommandKind;
#[cfg(feature = "redis")]
use crate::storage::RedisObjectResult;
use crate::storage::{hash_key, hash_key_tag, shift_for, stripe_index};
#[cfg(feature = "redis")]
use std::collections::BTreeSet;

struct RespTestHarness;

impl RespTestHarness {
    fn exec_resp(store: &EmbeddedStore, parts: &[&[u8]]) -> Vec<u8> {
        let mut args = RespDirectArgs::new();
        args.extend(parts[1..].iter().copied());
        let command =
            DirectProtocol::parse_resp_direct_command(parts[0], args).unwrap_or_else(|| {
                panic!(
                    "command should parse: {}",
                    String::from_utf8_lossy(parts[0])
                )
            });
        let mut out = BytesMut::new();
        DirectProtocol::shared_execute_resp_direct_cmd_into(
            store,
            command,
            &mut out,
            None,
            false,
            RespProtocolVersion::Resp2,
            Instant::now(),
        );
        out.to_vec()
    }

    fn exec_resp_sequence(
        store: &EmbeddedStore,
        commands: &[&[&[u8]]],
        transaction_mode: TransactionMode,
    ) -> Vec<Frame> {
        decode_resp_stream(&Self::exec_resp_sequence_raw_on_owned_shard(
            store,
            commands,
            transaction_mode,
            None,
        ))
    }

    fn exec_resp_sequence_on_owned_shard(
        store: &EmbeddedStore,
        commands: &[&[&[u8]]],
        transaction_mode: TransactionMode,
        owned_shard_id: Option<usize>,
    ) -> Vec<Frame> {
        decode_resp_stream(&Self::exec_resp_sequence_raw_on_owned_shard(
            store,
            commands,
            transaction_mode,
            owned_shard_id,
        ))
    }

    fn exec_resp_sequence_raw(
        store: &EmbeddedStore,
        commands: &[&[&[u8]]],
        transaction_mode: TransactionMode,
    ) -> Vec<u8> {
        Self::exec_resp_sequence_raw_on_owned_shard(store, commands, transaction_mode, None)
    }

    fn exec_resp_sequence_raw_on_owned_shard(
        store: &EmbeddedStore,
        commands: &[&[&[u8]]],
        transaction_mode: TransactionMode,
        owned_shard_id: Option<usize>,
    ) -> Vec<u8> {
        let mut input = Vec::new();
        for command in commands {
            encode_resp_command(command, &mut input);
        }

        let coordinator =
            TransactionCoordinator::new(store.shard_count(), transaction_mode).map(Arc::new);
        let mut transaction_state = TransactionState::default();
        let mut resp_protocol = RespProtocolVersion::default();
        let mut out = BytesMut::new();
        let consumed = DirectProtocol::process_shared_request_buffer_with_context(
            &input,
            store,
            &mut out,
            None,
            SharedRequestBufferContext {
                single_threaded: false,
                owned_shard_id,
                started_at: Instant::now(),
                transaction_coordinator: coordinator.as_deref(),
                transaction_state: &mut transaction_state,
                resp_protocol: &mut resp_protocol,
            },
        )
        .expect("request buffer should process");
        assert_eq!(consumed, input.len());
        out.to_vec()
    }

    fn exec_scnp_resp_sequence(
        store: &EmbeddedStore,
        commands: &[&[&[u8]]],
        transaction_mode: TransactionMode,
    ) -> Vec<FastResponse> {
        let mut input = Vec::new();
        for command in commands {
            encode_scnp_resp_command(command, &mut input);
        }
        Self::process_scnp_input(store, &input, transaction_mode)
    }

    fn process_scnp_input(
        store: &EmbeddedStore,
        input: &[u8],
        transaction_mode: TransactionMode,
    ) -> Vec<FastResponse> {
        let coordinator =
            TransactionCoordinator::new(store.shard_count(), transaction_mode).map(Arc::new);
        let mut transaction_state = TransactionState::default();
        let mut resp_protocol = RespProtocolVersion::default();
        let mut out = BytesMut::new();
        let consumed = DirectProtocol::process_shared_request_buffer_with_context(
            input,
            store,
            &mut out,
            None,
            SharedRequestBufferContext {
                single_threaded: false,
                owned_shard_id: None,
                started_at: Instant::now(),
                transaction_coordinator: coordinator.as_deref(),
                transaction_state: &mut transaction_state,
                resp_protocol: &mut resp_protocol,
            },
        )
        .expect("request buffer should process");
        assert_eq!(consumed, input.len());
        decode_fast_stream(&out)
    }

    fn exec_resp_integer(store: &EmbeddedStore, parts: &[&[u8]]) -> i64 {
        let raw = Self::exec_resp(store, parts);
        let raw = std::str::from_utf8(&raw).expect("integer response is utf8");
        raw.strip_prefix(':')
            .and_then(|value| value.strip_suffix("\r\n"))
            .expect("integer response format")
            .parse()
            .expect("integer response value")
    }

    fn exec_scnp_resp(store: &EmbeddedStore, parts: Vec<&[u8]>) -> FastResponse {
        let mut out = BytesMut::new();
        DirectProtocol::shared_execute_fast_into(
            store,
            FastRequest {
                key_hash: None,
                route_shard: None,
                key_tag: None,
                command: FastCommand::RespCommand { parts },
            },
            &mut out,
            None,
            false,
            Instant::now(),
        );
        FastCodec::decode_response(&out).unwrap().unwrap().0
    }
}

#[test]
fn resp_direct_parser_accepts_five_digit_bulk_lengths() {
    let value = vec![b'x'; 65_536];
    let mut input = Vec::new();
    input.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$65536\r\n");
    input.extend_from_slice(&value);
    input.extend_from_slice(b"\r\n");

    let (consumed, command, args) = DirectProtocol::try_resp_command_parts(&input)
        .expect("five-digit RESP bulk length should stay on direct parser path");

    assert_eq!(consumed, input.len());
    assert_eq!(command, b"SET");
    assert_eq!(args[0], b"key");
    assert_eq!(args[1], value.as_slice());
    assert!(DirectProtocol::parse_resp_direct_command(command, args).is_some());
}

#[cfg(feature = "embedded")]
#[test]
fn raw_command_dispatcher_covers_catalog_primary_names_and_aliases() {
    for command in RAW_DIRECT_CATALOG {
        let resolved = find_primary_raw_command(command.name().as_bytes())
            .unwrap_or_else(|| panic!("raw command should dispatch: {}", command.name()));
        assert_eq!(resolved.name(), command.name());
    }

    #[cfg(feature = "redis")]
    for (alias, primary) in [
        (b"RESTORE-ASKING".as_slice(), "RESTORE"),
        (b"SLAVEOF".as_slice(), "REPLICAOF"),
        (b"SUBSTR".as_slice(), "GETRANGE"),
    ] {
        let resolved = RawCommandDispatcher::find(alias).unwrap_or_else(|| {
            panic!(
                "raw command alias should dispatch: {}",
                String::from_utf8_lossy(alias)
            )
        });
        assert_eq!(resolved.name(), primary);
    }
}

fn encode_resp_command(parts: &[&[u8]], out: &mut Vec<u8>) {
    out.extend_from_slice(b"*");
    out.extend_from_slice(parts.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for part in parts {
        out.extend_from_slice(b"$");
        out.extend_from_slice(part.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
}

fn encode_scnp_resp_command(parts: &[&[u8]], out: &mut Vec<u8>) {
    FastCodec::encode_request(
        &FastRequest {
            key_hash: None,
            route_shard: None,
            key_tag: None,
            command: FastCommand::RespCommand {
                parts: parts.to_vec(),
            },
        },
        out,
    );
}

fn exec_scnp_redis_opcode_on_owned_shard(
    store: &EmbeddedStore,
    owned_shard_id: usize,
    kind: FastCommandKind,
    args: Vec<&[u8]>,
) -> FastResponse {
    let route = args.first().map(|key| store.route_key(key));
    let mut frame = Vec::new();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: route.as_ref().map(|route| route.key_hash),
            route_shard: route.as_ref().map(|route| route.shard_id as u32),
            key_tag: args.first().map(|key| hash_key_tag(key)),
            command: FastCommand::RedisCommand { kind, args },
        },
        &mut frame,
    );
    let mut out = BytesMut::new();
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        store,
        &mut out,
        None,
        false,
        Some(owned_shard_id),
        Instant::now(),
    )
    .expect("SCNP Redis opcode should process");
    assert_eq!(consumed, frame.len());
    FastCodec::decode_response(&out).unwrap().unwrap().0
}

fn decode_resp_stream(mut raw: &[u8]) -> Vec<Frame> {
    let mut frames = Vec::new();
    while !raw.is_empty() {
        let (frame, consumed) = RespCodec::decode(raw)
            .expect("RESP decode should succeed")
            .expect("RESP frame should be complete");
        frames.push(frame);
        raw = &raw[consumed..];
    }
    frames
}

fn decode_fast_stream(mut raw: &[u8]) -> Vec<FastResponse> {
    let mut responses = Vec::new();
    while !raw.is_empty() {
        let (response, consumed) = FastCodec::decode_response(raw)
            .expect("SCNP response decode should succeed")
            .expect("SCNP response should be complete");
        responses.push(response);
        raw = &raw[consumed..];
    }
    responses
}

fn key_for_shard(store: &EmbeddedStore, shard_id: usize) -> Vec<u8> {
    for index in 0..10_000 {
        let key = format!("txn-key-{shard_id}-{index}").into_bytes();
        if store.route_key(&key).shard_id == shard_id {
            return key;
        }
    }
    panic!("unable to find key for shard {shard_id}");
}

#[test]
#[cfg(feature = "redis")]
fn transaction_command_shards_use_shared_redis_key_specs() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let key_a = key_for_shard(&store, 1);
    let key_b = key_for_shard(&store, 3);
    let shards = |parts: &[&[u8]]| {
        super::transactions::command_shards(&store, parts)
            .into_iter()
            .collect::<BTreeSet<_>>()
    };

    assert_eq!(
        shards(&[
            b"XREAD",
            b"COUNT",
            b"1",
            b"STREAMS",
            key_a.as_slice(),
            key_b.as_slice(),
            b"0-0",
            b"0-0",
        ]),
        BTreeSet::from([1, 3])
    );
    assert_eq!(
        shards(&[b"PFMERGE", key_a.as_slice(), key_b.as_slice()]),
        BTreeSet::from([1, 3])
    );
    assert_eq!(
        shards(&[
            b"EVAL",
            b"return 1",
            b"2",
            key_a.as_slice(),
            key_b.as_slice(),
        ]),
        BTreeSet::from([1, 3])
    );
    assert_eq!(
        shards(&[
            b"SORT",
            key_a.as_slice(),
            b"BY",
            b"nosort",
            b"STORE",
            key_b.as_slice(),
        ]),
        BTreeSet::from([1, 3])
    );
    assert_eq!(shards(&[b"SCRIPT", b"LOAD", b"return 1"]), BTreeSet::new());
}

#[cfg(feature = "redis")]
fn bulk(value: &[u8]) -> Frame {
    Frame::BlobString(value.to_vec())
}

#[cfg(feature = "redis")]
const RESP2_PROTOCOL_COMMANDS: &[&str] = &["DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH"];

#[cfg(feature = "redis")]
const RESP2_ALIAS_COMMANDS: &[&str] = &["RESTORE-ASKING", "SLAVEOF", "SUBSTR"];

#[cfg(feature = "redis")]
fn resp2_part(part: impl AsRef<[u8]>) -> Vec<u8> {
    part.as_ref().to_vec()
}

#[cfg(feature = "redis")]
fn resp2_smoke_commands(name: &str) -> Option<Vec<Vec<Vec<u8>>>> {
    macro_rules! commands {
        ($([$($part:expr),+ $(,)?]),+ $(,)?) => {
            Some(vec![$(vec![$(resp2_part($part)),+]),+])
        };
    }

    match name {
        "APPEND" => commands!([b"APPEND", b"string", b"value"]),
        "ASKING" => commands!([b"ASKING"]),
        "AUTH" => commands!([b"AUTH", b"password"]),
        "BGREWRITEAOF" => commands!([b"BGREWRITEAOF"]),
        "BGSAVE" => commands!([b"BGSAVE"]),
        "BITCOUNT" => commands!([b"SETBIT", b"bits", b"7", b"1"], [b"BITCOUNT", b"bits"]),
        "BITFIELD" => commands!([
            b"BITFIELD",
            b"bf",
            b"SET",
            b"u8",
            b"0",
            b"1",
            b"GET",
            b"u8",
            b"0"
        ]),
        "BITOP" => commands!(
            [b"SET", b"bit-a", b"\x0f"],
            [b"SET", b"bit-b", b"\xf0"],
            [b"BITOP", b"OR", b"bit-out", b"bit-a", b"bit-b"]
        ),
        "BITPOS" => commands!([b"SETBIT", b"bits", b"7", b"1"], [b"BITPOS", b"bits", b"1"]),
        "BLMPOP" => commands!(
            [b"RPUSH", b"blmpop-list", b"a", b"b"],
            [
                b"BLMPOP",
                b"0.001",
                b"1",
                b"blmpop-list",
                b"LEFT",
                b"COUNT",
                b"1"
            ]
        ),
        "BLMOVE" => commands!(
            [b"RPUSH", b"blmove-src", b"a"],
            [
                b"BLMOVE",
                b"blmove-src",
                b"blmove-dst",
                b"LEFT",
                b"RIGHT",
                b"0"
            ]
        ),
        "BLPOP" => commands!(
            [b"RPUSH", b"blpop-list", b"a"],
            [b"BLPOP", b"blpop-list", b"0"]
        ),
        "BRPOP" => commands!(
            [b"RPUSH", b"brpop-list", b"a"],
            [b"BRPOP", b"brpop-list", b"0"]
        ),
        "BRPOPLPUSH" => commands!(
            [b"RPUSH", b"brpoplpush-src", b"a"],
            [b"BRPOPLPUSH", b"brpoplpush-src", b"brpoplpush-dst", b"0"]
        ),
        "BZMPOP" => commands!(
            [b"ZADD", b"bzmpop-z", b"1", b"a"],
            [
                b"BZMPOP",
                b"0.001",
                b"1",
                b"bzmpop-z",
                b"MIN",
                b"COUNT",
                b"1"
            ]
        ),
        "BZPOPMAX" => commands!(
            [b"ZADD", b"bzpopmax-z", b"1", b"a"],
            [b"BZPOPMAX", b"bzpopmax-z", b"0"]
        ),
        "BZPOPMIN" => commands!(
            [b"ZADD", b"bzpopmin-z", b"1", b"a"],
            [b"BZPOPMIN", b"bzpopmin-z", b"0"]
        ),
        "CLIENT" => commands!([b"CLIENT", b"ID"]),
        "CLUSTER" => commands!([b"CLUSTER", b"INFO"]),
        "COMMAND" => commands!(
            [b"COMMAND", b"COUNT"],
            [b"COMMAND", b"INFO", b"MULTI"],
            [b"COMMAND", b"GETKEYS", b"WATCH", b"watched"]
        ),
        "CONFIG" => commands!([b"CONFIG", b"GET", b"*"]),
        "COPY" => commands!(
            [b"SET", b"copy-src", b"value"],
            [b"COPY", b"copy-src", b"copy-dst"]
        ),
        "DBSIZE" => commands!([b"DBSIZE"]),
        "DEBUG" => commands!([b"DEBUG", b"HELP"]),
        "DECR" => commands!([b"DECR", b"counter"]),
        "DECRBY" => commands!([b"DECRBY", b"counter", b"2"]),
        "DEL" => commands!([b"SET", b"delete-me", b"value"], [b"DEL", b"delete-me"]),
        "DISCARD" => commands!([b"MULTI"], [b"DISCARD"]),
        "DUMP" => commands!([b"SET", b"dump-key", b"value"], [b"DUMP", b"dump-key"]),
        "ECHO" => commands!([b"ECHO", b"hello"]),
        "EVAL" => commands!([b"EVAL", b"return 1", b"0"]),
        "EVALSHA" => commands!(
            [b"SCRIPT", b"LOAD", b"return 1"],
            [
                b"EVALSHA",
                b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db",
                b"0"
            ]
        ),
        "EXEC" => commands!([b"MULTI"], [b"SET", b"tx-key", b"value"], [b"EXEC"]),
        "EXISTS" => commands!(
            [b"SET", b"exists-key", b"value"],
            [b"EXISTS", b"exists-key"]
        ),
        "EXPIRE" => commands!(
            [b"SET", b"expire-key", b"value"],
            [b"EXPIRE", b"expire-key", b"60"]
        ),
        "EXPIREAT" => commands!(
            [b"SET", b"expireat-key", b"value"],
            [b"EXPIREAT", b"expireat-key", b"4102444800"]
        ),
        "EXPIRETIME" => commands!(
            [b"SET", b"expiretime-key", b"value"],
            [b"EXPIRE", b"expiretime-key", b"60"],
            [b"EXPIRETIME", b"expiretime-key"]
        ),
        "FLUSHALL" => commands!([b"SET", b"flushall-key", b"value"], [b"FLUSHALL"]),
        "FLUSHDB" => commands!([b"SET", b"flushdb-key", b"value"], [b"FLUSHDB"]),
        "GEOADD" => commands!([b"GEOADD", b"geo", b"-73.9857", b"40.7484", b"empire"]),
        "GEODIST" => commands!(
            [
                b"GEOADD",
                b"geo",
                b"-73.9857",
                b"40.7484",
                b"empire",
                b"-73.9897",
                b"40.7411",
                b"flatiron"
            ],
            [b"GEODIST", b"geo", b"empire", b"flatiron", b"km"]
        ),
        "GEOHASH" => commands!(
            [b"GEOADD", b"geo", b"-73.9857", b"40.7484", b"empire"],
            [b"GEOHASH", b"geo", b"empire"]
        ),
        "GEOPOS" => commands!(
            [b"GEOADD", b"geo", b"-73.9857", b"40.7484", b"empire"],
            [b"GEOPOS", b"geo", b"empire"]
        ),
        "GEORADIUS" => commands!(
            [b"GEOADD", b"geo", b"-73.9857", b"40.7484", b"empire"],
            [b"GEORADIUS", b"geo", b"-73.9857", b"40.7484", b"2", b"km"]
        ),
        "GEORADIUSBYMEMBER" => commands!(
            [b"GEOADD", b"geo", b"-73.9857", b"40.7484", b"empire"],
            [b"GEORADIUSBYMEMBER", b"geo", b"empire", b"2", b"km"]
        ),
        "GEORADIUSBYMEMBER_RO" => commands!(
            [b"GEOADD", b"geo", b"-73.9857", b"40.7484", b"empire"],
            [b"GEORADIUSBYMEMBER_RO", b"geo", b"empire", b"2", b"km"]
        ),
        "GEORADIUS_RO" => commands!(
            [b"GEOADD", b"geo", b"-73.9857", b"40.7484", b"empire"],
            [
                b"GEORADIUS_RO",
                b"geo",
                b"-73.9857",
                b"40.7484",
                b"2",
                b"km"
            ]
        ),
        "GET" => commands!([b"SET", b"string", b"value"], [b"GET", b"string"]),
        "GETBIT" => commands!([b"SETBIT", b"bits", b"7", b"1"], [b"GETBIT", b"bits", b"7"]),
        "GETDEL" => commands!(
            [b"SET", b"getdel-key", b"value"],
            [b"GETDEL", b"getdel-key"]
        ),
        "GETEX" => commands!([b"SET", b"getex-key", b"value"], [b"GETEX", b"getex-key"]),
        "GETRANGE" => commands!(
            [b"SET", b"range-key", b"value"],
            [b"GETRANGE", b"range-key", b"0", b"2"]
        ),
        "GETSET" => commands!(
            [b"SET", b"getset-key", b"old"],
            [b"GETSET", b"getset-key", b"new"]
        ),
        "HDEL" => commands!(
            [b"HSET", b"hash", b"field", b"value"],
            [b"HDEL", b"hash", b"field"]
        ),
        "HEXISTS" => commands!(
            [b"HSET", b"hash", b"field", b"value"],
            [b"HEXISTS", b"hash", b"field"]
        ),
        "HGET" => commands!(
            [b"HSET", b"hash", b"field", b"value"],
            [b"HGET", b"hash", b"field"]
        ),
        "HGETALL" => commands!(
            [b"HSET", b"hash", b"field", b"value"],
            [b"HGETALL", b"hash"]
        ),
        "HINCRBY" => commands!([b"HINCRBY", b"hash", b"field", b"2"]),
        "HINCRBYFLOAT" => commands!([b"HINCRBYFLOAT", b"hash", b"field", b"1.5"]),
        "HKEYS" => commands!([b"HSET", b"hash", b"field", b"value"], [b"HKEYS", b"hash"]),
        "HLEN" => commands!([b"HSET", b"hash", b"field", b"value"], [b"HLEN", b"hash"]),
        "HMGET" => commands!(
            [b"HMSET", b"hash", b"f1", b"v1", b"f2", b"v2"],
            [b"HMGET", b"hash", b"f1", b"f2"]
        ),
        "HMSET" => commands!([b"HMSET", b"hash", b"f1", b"v1", b"f2", b"v2"]),
        "HOST:" => commands!([b"HOST:"]),
        "HRANDFIELD" => commands!(
            [b"HSET", b"hash", b"field", b"value"],
            [b"HRANDFIELD", b"hash"]
        ),
        "HSCAN" => commands!(
            [b"HSET", b"hash", b"field", b"value"],
            [b"HSCAN", b"hash", b"0"]
        ),
        "HSET" => commands!([b"HSET", b"hash", b"field", b"value"]),
        "HSETNX" => commands!([b"HSETNX", b"hash", b"field", b"value"]),
        "HSTRLEN" => commands!(
            [b"HSET", b"hash", b"field", b"value"],
            [b"HSTRLEN", b"hash", b"field"]
        ),
        "HVALS" => commands!([b"HSET", b"hash", b"field", b"value"], [b"HVALS", b"hash"]),
        "HELLO" => commands!([b"HELLO", b"2"]),
        "INFO" => commands!([b"INFO"]),
        "INCR" => commands!([b"INCR", b"counter"]),
        "INCRBY" => commands!([b"INCRBY", b"counter", b"2"]),
        "INCRBYFLOAT" => commands!([b"INCRBYFLOAT", b"counter", b"1.5"]),
        "KEYS" => commands!([b"SET", b"keys-one", b"value"], [b"KEYS", b"*"]),
        "LASTSAVE" => commands!([b"LASTSAVE"]),
        "LATENCY" => commands!([b"LATENCY", b"LATEST"]),
        "LINDEX" => commands!([b"RPUSH", b"list", b"a", b"b"], [b"LINDEX", b"list", b"0"]),
        "LINSERT" => commands!(
            [b"RPUSH", b"list", b"a", b"c"],
            [b"LINSERT", b"list", b"BEFORE", b"c", b"b"]
        ),
        "LLEN" => commands!([b"RPUSH", b"list", b"a", b"b"], [b"LLEN", b"list"]),
        "LMOVE" => commands!(
            [b"RPUSH", b"lmove-src", b"a", b"b"],
            [b"LMOVE", b"lmove-src", b"lmove-dst", b"RIGHT", b"LEFT"]
        ),
        "LMPOP" => commands!(
            [b"RPUSH", b"lmpop-list", b"a", b"b"],
            [b"LMPOP", b"1", b"lmpop-list", b"LEFT", b"COUNT", b"1"]
        ),
        "LOLWUT" => commands!([b"LOLWUT"]),
        "LPOP" => commands!([b"RPUSH", b"list", b"a", b"b"], [b"LPOP", b"list"]),
        "LPUSH" => commands!([b"LPUSH", b"list", b"a"]),
        "LPUSHX" => commands!([b"RPUSH", b"list", b"a"], [b"LPUSHX", b"list", b"b"]),
        "LRANGE" => commands!(
            [b"RPUSH", b"list", b"a", b"b"],
            [b"LRANGE", b"list", b"0", b"-1"]
        ),
        "LREM" => commands!(
            [b"RPUSH", b"list", b"a", b"a", b"b"],
            [b"LREM", b"list", b"0", b"a"]
        ),
        "LSET" => commands!([b"RPUSH", b"list", b"a"], [b"LSET", b"list", b"0", b"x"]),
        "LTRIM" => commands!(
            [b"RPUSH", b"list", b"a", b"b", b"c"],
            [b"LTRIM", b"list", b"0", b"1"]
        ),
        "MEMORY" => commands!(
            [b"SET", b"memory-key", b"value"],
            [b"MEMORY", b"USAGE", b"memory-key"]
        ),
        "MGET" => commands!(
            [b"MSET", b"mget-a", b"1", b"mget-b", b"2"],
            [b"MGET", b"mget-a", b"mget-b"]
        ),
        "MIGRATE" => commands!([b"MIGRATE", b"127.0.0.1", b"6379", b"key", b"0", b"1000"]),
        "MODULE" => commands!([b"MODULE", b"LIST"]),
        "MONITOR" => commands!([b"MONITOR"]),
        "MOVE" => commands!([b"MOVE", b"move-key", b"1"]),
        "MSET" => commands!([b"MSET", b"mset-a", b"1", b"mset-b", b"2"]),
        "MSETNX" => commands!([b"MSETNX", b"msetnx-a", b"1", b"msetnx-b", b"2"]),
        "MULTI" => commands!([b"MULTI"], [b"DISCARD"]),
        "OBJECT" => commands!(
            [b"SET", b"object-key", b"value"],
            [b"OBJECT", b"ENCODING", b"object-key"]
        ),
        "PERSIST" => commands!(
            [b"SET", b"persist-key", b"value"],
            [b"EXPIRE", b"persist-key", b"60"],
            [b"PERSIST", b"persist-key"]
        ),
        "PEXPIRE" => commands!(
            [b"SET", b"pexpire-key", b"value"],
            [b"PEXPIRE", b"pexpire-key", b"60000"]
        ),
        "PEXPIREAT" => commands!(
            [b"SET", b"pexpireat-key", b"value"],
            [b"PEXPIREAT", b"pexpireat-key", b"4102444800000"]
        ),
        "PEXPIRETIME" => commands!(
            [b"SET", b"pexpiretime-key", b"value"],
            [b"PEXPIRE", b"pexpiretime-key", b"60000"],
            [b"PEXPIRETIME", b"pexpiretime-key"]
        ),
        "PFADD" => commands!([b"PFADD", b"hll", b"a", b"b"]),
        "PFCOUNT" => commands!([b"PFADD", b"hll", b"a", b"b"], [b"PFCOUNT", b"hll"]),
        "PFDEBUG" => commands!([b"PFADD", b"hll", b"a"], [b"PFDEBUG", b"ENCODING", b"hll"]),
        "PFMERGE" => commands!(
            [b"PFADD", b"hll", b"a"],
            [b"PFMERGE", b"hll-merged", b"hll"]
        ),
        "PFSELFTEST" => commands!([b"PFSELFTEST"]),
        "PING" => commands!([b"PING"]),
        "POST" => commands!([b"POST"]),
        "PSETEX" => commands!([b"PSETEX", b"psetex-key", b"60000", b"value"]),
        "PSUBSCRIBE" => commands!([b"PSUBSCRIBE", b"news.*"]),
        "PSYNC" => commands!([b"PSYNC", b"?", b"-1"]),
        "PTTL" => commands!([b"SET", b"pttl-key", b"value"], [b"PTTL", b"pttl-key"]),
        "PUBLISH" => commands!([b"PUBLISH", b"channel", b"message"]),
        "PUBSUB" => commands!([b"PUBSUB", b"NUMPAT"]),
        "PUNSUBSCRIBE" => commands!([b"PUNSUBSCRIBE", b"news.*"]),
        "QUIT" => commands!([b"QUIT"]),
        "RANDOMKEY" => commands!([b"SET", b"random-key", b"value"], [b"RANDOMKEY"]),
        "READONLY" => commands!([b"READONLY"]),
        "READWRITE" => commands!([b"READWRITE"]),
        "RENAME" => commands!(
            [b"SET", b"rename-src", b"value"],
            [b"RENAME", b"rename-src", b"rename-dst"]
        ),
        "RENAMENX" => commands!(
            [b"SET", b"renamenx-src", b"value"],
            [b"RENAMENX", b"renamenx-src", b"renamenx-dst"]
        ),
        "REPLCONF" => commands!([b"REPLCONF", b"ACK", b"0"]),
        "REPLICAOF" => commands!([b"REPLICAOF", b"NO", b"ONE"]),
        "RESTORE" => Some(vec![vec![
            resp2_part(b"RESTORE"),
            resp2_part(b"restore-key"),
            resp2_part(b"0"),
            crate::commands::dump_restore::encode_string_dump_value(b"value"),
        ]]),
        "RESTORE-ASKING" => Some(vec![vec![
            resp2_part(b"RESTORE-ASKING"),
            resp2_part(b"restore-asking-key"),
            resp2_part(b"0"),
            crate::commands::dump_restore::encode_string_dump_value(b"value"),
        ]]),
        "ROLE" => commands!([b"ROLE"]),
        "RPOP" => commands!([b"RPUSH", b"list", b"a", b"b"], [b"RPOP", b"list"]),
        "RPOPLPUSH" => commands!(
            [b"RPUSH", b"rpoplpush-src", b"a"],
            [b"RPOPLPUSH", b"rpoplpush-src", b"rpoplpush-dst"]
        ),
        "RPUSH" => commands!([b"RPUSH", b"list", b"a"]),
        "RPUSHX" => commands!([b"RPUSH", b"list", b"a"], [b"RPUSHX", b"list", b"b"]),
        "SADD" => commands!([b"SADD", b"set", b"a"]),
        "SAVE" => commands!([b"SAVE"]),
        "SCAN" => commands!([b"SCAN", b"0"]),
        "SCARD" => commands!([b"SADD", b"set", b"a"], [b"SCARD", b"set"]),
        "SCRIPT" => commands!([b"SCRIPT", b"LOAD", b"return 1"]),
        "SDIFF" => commands!(
            [b"SADD", b"set-a", b"a", b"b"],
            [b"SADD", b"set-b", b"b"],
            [b"SDIFF", b"set-a", b"set-b"]
        ),
        "SDIFFSTORE" => commands!(
            [b"SADD", b"set-a", b"a", b"b"],
            [b"SADD", b"set-b", b"b"],
            [b"SDIFFSTORE", b"set-out", b"set-a", b"set-b"]
        ),
        "SELECT" => commands!([b"SELECT", b"0"]),
        "SET" => commands!([b"SET", b"string", b"value"]),
        "SETBIT" => commands!([b"SETBIT", b"bits", b"7", b"1"]),
        "SETEX" => commands!([b"SETEX", b"setex-key", b"60", b"value"]),
        "SETNX" => commands!([b"SETNX", b"setnx-key", b"value"]),
        "SETRANGE" => commands!(
            [b"SET", b"range-key", b"value"],
            [b"SETRANGE", b"range-key", b"1", b"aa"]
        ),
        "SHUTDOWN" => commands!([b"SHUTDOWN"]),
        "SINTER" => commands!(
            [b"SADD", b"set-a", b"a"],
            [b"SADD", b"set-b", b"a"],
            [b"SINTER", b"set-a", b"set-b"]
        ),
        "SINTERSTORE" => commands!(
            [b"SADD", b"set-a", b"a"],
            [b"SADD", b"set-b", b"a"],
            [b"SINTERSTORE", b"set-out", b"set-a", b"set-b"]
        ),
        "SISMEMBER" => commands!([b"SADD", b"set", b"a"], [b"SISMEMBER", b"set", b"a"]),
        "SLAVEOF" => commands!([b"SLAVEOF", b"NO", b"ONE"]),
        "SLOWLOG" => commands!([b"SLOWLOG", b"LEN"]),
        "SMEMBERS" => commands!([b"SADD", b"set", b"a"], [b"SMEMBERS", b"set"]),
        "SMISMEMBER" => commands!([b"SADD", b"set", b"a"], [b"SMISMEMBER", b"set", b"a", b"b"]),
        "SMOVE" => commands!(
            [b"SADD", b"set-src", b"a"],
            [b"SMOVE", b"set-src", b"set-dst", b"a"]
        ),
        "SORT" => commands!(
            [b"RPUSH", b"sort-list", b"3", b"1", b"2"],
            [b"SORT", b"sort-list"]
        ),
        "SPOP" => commands!([b"SADD", b"set", b"a"], [b"SPOP", b"set"]),
        "SRANDMEMBER" => commands!([b"SADD", b"set", b"a"], [b"SRANDMEMBER", b"set"]),
        "SREM" => commands!([b"SADD", b"set", b"a"], [b"SREM", b"set", b"a"]),
        "SSCAN" => commands!([b"SADD", b"set", b"a"], [b"SSCAN", b"set", b"0"]),
        "STRLEN" => commands!(
            [b"SET", b"strlen-key", b"value"],
            [b"STRLEN", b"strlen-key"]
        ),
        "SUBSCRIBE" => commands!([b"SUBSCRIBE", b"channel"]),
        "SUBSTR" => commands!(
            [b"SET", b"substr-key", b"value"],
            [b"SUBSTR", b"substr-key", b"0", b"2"]
        ),
        "SUNION" => commands!(
            [b"SADD", b"set-a", b"a"],
            [b"SADD", b"set-b", b"b"],
            [b"SUNION", b"set-a", b"set-b"]
        ),
        "SUNIONSTORE" => commands!(
            [b"SADD", b"set-a", b"a"],
            [b"SADD", b"set-b", b"b"],
            [b"SUNIONSTORE", b"set-out", b"set-a", b"set-b"]
        ),
        "SWAPDB" => commands!([b"SWAPDB", b"0", b"0"]),
        "SYNC" => commands!([b"SYNC"]),
        "TIME" => commands!([b"TIME"]),
        "TOUCH" => commands!([b"SET", b"touch-key", b"value"], [b"TOUCH", b"touch-key"]),
        "TTL" => commands!([b"SET", b"ttl-key", b"value"], [b"TTL", b"ttl-key"]),
        "TYPE" => commands!([b"SET", b"type-key", b"value"], [b"TYPE", b"type-key"]),
        "UNLINK" => commands!(
            [b"SET", b"unlink-key", b"value"],
            [b"UNLINK", b"unlink-key"]
        ),
        "UNSUBSCRIBE" => commands!([b"UNSUBSCRIBE", b"channel"]),
        "UNWATCH" => commands!([b"WATCH", b"watched"], [b"UNWATCH"]),
        "WAIT" => commands!([b"WAIT", b"1", b"1"]),
        "WATCH" => commands!([b"WATCH", b"watched"]),
        "XACK" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XGROUP", b"CREATE", b"stream", b"group", b"0-0"],
            [b"XACK", b"stream", b"group", b"1-0"]
        ),
        "XADD" => commands!([b"XADD", b"stream", b"1-0", b"f", b"v"]),
        "XCLAIM" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XGROUP", b"CREATE", b"stream", b"group", b"0-0"],
            [b"XCLAIM", b"stream", b"group", b"consumer", b"0", b"1-0"]
        ),
        "XDEL" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XDEL", b"stream", b"1-0"]
        ),
        "XGROUP" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XGROUP", b"CREATE", b"stream", b"group", b"0-0"]
        ),
        "XINFO" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XINFO", b"STREAM", b"stream"]
        ),
        "XLEN" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XLEN", b"stream"]
        ),
        "XPENDING" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XGROUP", b"CREATE", b"stream", b"group", b"0-0"],
            [b"XPENDING", b"stream", b"group"]
        ),
        "XRANGE" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XRANGE", b"stream", b"-", b"+"]
        ),
        "XREAD" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XREAD", b"COUNT", b"1", b"STREAMS", b"stream", b"0-0"]
        ),
        "XREADGROUP" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XGROUP", b"CREATE", b"stream", b"group", b"0-0"],
            [
                b"XREADGROUP",
                b"GROUP",
                b"group",
                b"consumer",
                b"COUNT",
                b"1",
                b"STREAMS",
                b"stream",
                b">"
            ]
        ),
        "XREVRANGE" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XREVRANGE", b"stream", b"+", b"-"]
        ),
        "XSETID" => commands!([b"XSETID", b"stream", b"5-0"]),
        "XTRIM" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XTRIM", b"stream", b"MAXLEN", b"1"]
        ),
        "ZADD" => commands!([b"ZADD", b"zset", b"1", b"a"]),
        "ZCARD" => commands!([b"ZADD", b"zset", b"1", b"a"], [b"ZCARD", b"zset"]),
        "ZCOUNT" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZCOUNT", b"zset", b"-inf", b"+inf"]
        ),
        "ZDIFF" => commands!(
            [b"ZADD", b"zset-a", b"1", b"a", b"2", b"b"],
            [b"ZADD", b"zset-b", b"2", b"b"],
            [b"ZDIFF", b"2", b"zset-a", b"zset-b"]
        ),
        "ZDIFFSTORE" => commands!(
            [b"ZADD", b"zset-a", b"1", b"a", b"2", b"b"],
            [b"ZADD", b"zset-b", b"2", b"b"],
            [b"ZDIFFSTORE", b"zset-out", b"2", b"zset-a", b"zset-b"]
        ),
        "ZINCRBY" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZINCRBY", b"zset", b"1", b"a"]
        ),
        "ZINTER" => commands!(
            [b"ZADD", b"zset-a", b"1", b"a"],
            [b"ZADD", b"zset-b", b"2", b"a"],
            [b"ZINTER", b"2", b"zset-a", b"zset-b"]
        ),
        "ZINTERCARD" => commands!(
            [b"ZADD", b"zset-a", b"1", b"a"],
            [b"ZADD", b"zset-b", b"2", b"a"],
            [b"ZINTERCARD", b"2", b"zset-a", b"zset-b"]
        ),
        "ZINTERSTORE" => commands!(
            [b"ZADD", b"zset-a", b"1", b"a"],
            [b"ZADD", b"zset-b", b"2", b"a"],
            [b"ZINTERSTORE", b"zset-out", b"2", b"zset-a", b"zset-b"]
        ),
        "ZLEXCOUNT" => commands!(
            [b"ZADD", b"zset", b"0", b"a", b"0", b"b"],
            [b"ZLEXCOUNT", b"zset", b"-", b"+"]
        ),
        "ZMPOP" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZMPOP", b"1", b"zset", b"MIN", b"COUNT", b"1"]
        ),
        "ZMSCORE" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZMSCORE", b"zset", b"a", b"b"]
        ),
        "ZPOPMAX" => commands!([b"ZADD", b"zset", b"1", b"a"], [b"ZPOPMAX", b"zset"]),
        "ZPOPMIN" => commands!([b"ZADD", b"zset", b"1", b"a"], [b"ZPOPMIN", b"zset"]),
        "ZRANDMEMBER" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZRANDMEMBER", b"zset", b"1"]
        ),
        "ZRANGE" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZRANGE", b"zset", b"0", b"-1"]
        ),
        "ZRANGEBYLEX" => commands!(
            [b"ZADD", b"zset", b"0", b"a", b"0", b"b"],
            [b"ZRANGEBYLEX", b"zset", b"-", b"+"]
        ),
        "ZRANGEBYSCORE" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZRANGEBYSCORE", b"zset", b"-inf", b"+inf"]
        ),
        "ZRANGESTORE" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZRANGESTORE", b"zset-out", b"zset", b"0", b"-1"]
        ),
        "ZRANK" => commands!([b"ZADD", b"zset", b"1", b"a"], [b"ZRANK", b"zset", b"a"]),
        "ZREM" => commands!([b"ZADD", b"zset", b"1", b"a"], [b"ZREM", b"zset", b"a"]),
        "ZREMRANGEBYLEX" => commands!(
            [b"ZADD", b"zset", b"0", b"a", b"0", b"b"],
            [b"ZREMRANGEBYLEX", b"zset", b"-", b"+"]
        ),
        "ZREMRANGEBYRANK" => commands!(
            [b"ZADD", b"zset", b"1", b"a", b"2", b"b"],
            [b"ZREMRANGEBYRANK", b"zset", b"0", b"0"]
        ),
        "ZREMRANGEBYSCORE" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZREMRANGEBYSCORE", b"zset", b"-inf", b"+inf"]
        ),
        "ZREVRANGE" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZREVRANGE", b"zset", b"0", b"-1"]
        ),
        "ZREVRANGEBYLEX" => commands!(
            [b"ZADD", b"zset", b"0", b"a", b"0", b"b"],
            [b"ZREVRANGEBYLEX", b"zset", b"+", b"-"]
        ),
        "ZREVRANGEBYSCORE" => commands!(
            [b"ZADD", b"zset", b"1", b"a"],
            [b"ZREVRANGEBYSCORE", b"zset", b"+inf", b"-inf"]
        ),
        "ZREVRANK" => commands!([b"ZADD", b"zset", b"1", b"a"], [b"ZREVRANK", b"zset", b"a"]),
        "ZSCAN" => commands!([b"ZADD", b"zset", b"1", b"a"], [b"ZSCAN", b"zset", b"0"]),
        "ZSCORE" => commands!([b"ZADD", b"zset", b"1", b"a"], [b"ZSCORE", b"zset", b"a"]),
        "ZUNION" => commands!(
            [b"ZADD", b"zset-a", b"1", b"a"],
            [b"ZADD", b"zset-b", b"2", b"b"],
            [b"ZUNION", b"2", b"zset-a", b"zset-b"]
        ),
        "ZUNIONSTORE" => commands!(
            [b"ZADD", b"zset-a", b"1", b"a"],
            [b"ZADD", b"zset-b", b"2", b"b"],
            [b"ZUNIONSTORE", b"zset-out", b"2", b"zset-a", b"zset-b"]
        ),
        _ => None,
    }
}

#[cfg(feature = "redis")]
fn assert_resp2_smoke_case(name: &str, commands: Vec<Vec<Vec<u8>>>) {
    let store = EmbeddedStore::new(4);
    let borrowed_commands = commands
        .iter()
        .map(|command| command.iter().map(Vec::as_slice).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let command_refs = borrowed_commands
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    let raw =
        RespTestHarness::exec_resp_sequence_raw(&store, &command_refs, TransactionMode::ShardLocal);
    let frames = decode_resp_stream(&raw);
    assert_eq!(
        frames.len(),
        commands.len(),
        "RESP2 smoke case {name} returned unexpected frame count: {}",
        String::from_utf8_lossy(&raw)
    );
    for (command, frame) in commands.iter().zip(frames.iter()) {
        if let Frame::Error(message) = frame {
            assert_ne!(
                message,
                "ERR unsupported command",
                "RESP2 smoke case {name} hit unsupported dispatch for {}",
                resp2_command_label(command)
            );
            assert!(
                !message.contains("wrong number of arguments"),
                "RESP2 smoke case {name} used invalid arity for {}: {message}",
                resp2_command_label(command)
            );
        }
    }
}

#[cfg(feature = "redis")]
fn resp2_command_label(command: &[Vec<u8>]) -> String {
    command
        .iter()
        .map(|part| String::from_utf8_lossy(part))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(feature = "redis")]
fn decode_scan_response(raw: &[u8]) -> (u64, Vec<Vec<u8>>) {
    let (frame, consumed) = RespCodec::decode(raw).unwrap().expect("scan response");
    assert_eq!(consumed, raw.len());
    let Frame::Array(items) = frame else {
        panic!("scan response should be an array");
    };
    let [cursor, values]: [Frame; 2] = items.try_into().expect("scan response shape");
    let Frame::BlobString(cursor) = cursor else {
        panic!("scan cursor should be a bulk string");
    };
    let cursor = std::str::from_utf8(&cursor)
        .expect("cursor utf8")
        .parse::<u64>()
        .expect("cursor value");
    let Frame::Array(values) = values else {
        panic!("scan values should be an array");
    };
    let values = values
        .into_iter()
        .map(|frame| match frame {
            Frame::BlobString(value) => value,
            other => panic!("scan value should be bulk string, got {other:?}"),
        })
        .collect();
    (cursor, values)
}

#[cfg(feature = "redis")]
fn decode_bulk_array(raw: &[u8]) -> Vec<Vec<u8>> {
    let (frame, consumed) = RespCodec::decode(raw).unwrap().expect("array response");
    assert_eq!(consumed, raw.len());
    let Frame::Array(items) = frame else {
        panic!("response should be an array");
    };
    items
        .into_iter()
        .map(|frame| match frame {
            Frame::BlobString(value) => value,
            other => panic!("array item should be bulk string, got {other:?}"),
        })
        .collect()
}

#[cfg(feature = "redis")]
fn decode_optional_bulk(raw: &[u8]) -> Option<Vec<u8>> {
    let (frame, consumed) = RespCodec::decode(raw).unwrap().expect("bulk response");
    assert_eq!(consumed, raw.len());
    match frame {
        Frame::BlobString(value) => Some(value),
        Frame::Null => None,
        other => panic!("response should be bulk string or null, got {other:?}"),
    }
}

#[cfg(feature = "redis")]
fn assert_resp_error_contains(store: &EmbeddedStore, parts: &[&[u8]], expected: &str) {
    let raw = RespTestHarness::exec_resp(store, parts);
    let (frame, consumed) = RespCodec::decode(&raw).unwrap().expect("error response");
    assert_eq!(consumed, raw.len());
    let Frame::Error(message) = frame else {
        panic!("expected RESP error for {parts:?}, got {frame:?}");
    };
    assert!(
        message.contains(expected),
        "expected error containing {expected:?}, got {message:?}"
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_hello_two_returns_resp2_array() {
    let store = EmbeddedStore::new(4);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[&[b"HELLO", b"2"]],
        TransactionMode::ShardLocal,
    );

    match frames.as_slice() {
        [Frame::Array(items)] => {
            assert_eq!(items.len(), 14);
            assert_eq!(items.get(4), Some(&bulk(b"proto")));
            assert_eq!(items.get(5), Some(&Frame::Integer(2)));
        }
        other => panic!("unexpected HELLO 2 response: {other:?}"),
    }
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_hello_three_returns_resp3_map() {
    let store = EmbeddedStore::new(4);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[&[b"HELLO", b"3"]],
        TransactionMode::ShardLocal,
    );

    match frames.as_slice() {
        [Frame::Map(items)] => {
            assert_eq!(items.len(), 7);
            assert_eq!(resp_map_value(items, b"proto"), Some(&Frame::Integer(3)));
        }
        other => panic!("unexpected HELLO 3 response: {other:?}"),
    }
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_hello_negotiation_persists_for_connection() {
    let store = EmbeddedStore::new(4);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[&[b"HELLO", b"3"], &[b"HELLO"]],
        TransactionMode::ShardLocal,
    );

    match frames.as_slice() {
        [Frame::Map(first), Frame::Map(second)] => {
            assert_eq!(resp_map_value(first, b"proto"), Some(&Frame::Integer(3)));
            assert_eq!(resp_map_value(second, b"proto"), Some(&Frame::Integer(3)));
        }
        other => panic!("unexpected pipelined HELLO response: {other:?}"),
    }
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_hello_accepts_auth_and_setname_options() {
    let store = EmbeddedStore::new(4);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[
            &[b"HELLO", b"AUTH", b"default", b"secret"],
            &[b"HELLO", b"3", b"SETNAME", b"client"],
        ],
        TransactionMode::ShardLocal,
    );

    match frames.as_slice() {
        [Frame::Array(items), Frame::Map(map)] => {
            assert_eq!(items.get(4), Some(&bulk(b"proto")));
            assert_eq!(items.get(5), Some(&Frame::Integer(2)));
            assert_eq!(resp_map_value(map, b"proto"), Some(&Frame::Integer(3)));
        }
        other => panic!("unexpected HELLO option responses: {other:?}"),
    }
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_hello_rejects_incomplete_options() {
    let store = EmbeddedStore::new(4);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[
            &[b"HELLO", b"3", b"AUTH", b"default"],
            &[b"HELLO", b"SETNAME"],
        ],
        TransactionMode::ShardLocal,
    );

    match frames.as_slice() {
        [Frame::Error(first), Frame::Error(second)] => {
            assert!(first.contains("syntax"));
            assert!(second.contains("syntax"));
        }
        other => panic!("unexpected HELLO syntax responses: {other:?}"),
    }
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp3_negotiation_writes_native_nulls() {
    let store = EmbeddedStore::new(4);
    let raw = RespTestHarness::exec_resp_sequence_raw(
        &store,
        &[
            &[b"HELLO", b"3"],
            &[b"GET", b"missing"],
            &[b"MGET", b"a", b"b"],
        ],
        TransactionMode::ShardLocal,
    );

    let (_, hello_len) = RespCodec::decode(&raw).unwrap().unwrap();
    assert!(
        raw[hello_len..].starts_with(b"_\r\n*2\r\n_\r\n_\r\n"),
        "expected RESP3 nulls after HELLO 3, got {:?}",
        String::from_utf8_lossy(&raw[hello_len..])
    );
}

#[cfg(feature = "redis")]
fn resp_map_value<'a>(items: &'a [(Frame, Frame)], key: &[u8]) -> Option<&'a Frame> {
    items.iter().find_map(|(item_key, value)| match item_key {
        Frame::BlobString(bytes) if bytes == key => Some(value),
        _ => None,
    })
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_dump_restore_round_trips_strings_and_objects() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"DUMP", b"missing"]),
        b"$-1\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"dump-s", b"value"]),
        b"+OK\r\n".to_vec()
    );
    let string_dump =
        decode_optional_bulk(&RespTestHarness::exec_resp(&store, &[b"DUMP", b"dump-s"]))
            .expect("dump-s should exist");
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"RESTORE",
                b"restore-s",
                b"0",
                string_dump.as_slice(),
                b"REPLACE",
            ],
        ),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"restore-s"]),
        b"$5\r\nvalue\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"RESTORE-ASKING",
                b"restore-asking-s",
                b"0",
                string_dump.as_slice(),
                b"REPLACE",
            ],
        ),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"restore-asking-s"]),
        b"$5\r\nvalue\r\n".to_vec()
    );
    assert_resp_error_contains(
        &store,
        &[b"RESTORE", b"restore-s", b"0", string_dump.as_slice()],
        "BUSYKEY",
    );
    let mut bad_dump = string_dump.clone();
    bad_dump[1] ^= 0xff;
    assert_resp_error_contains(
        &store,
        &[
            b"RESTORE",
            b"bad-restore",
            b"0",
            bad_dump.as_slice(),
            b"REPLACE",
        ],
        "DUMP payload",
    );

    let past = crate::storage::now_millis().saturating_sub(1).to_string();
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"RESTORE",
                b"restore-expired",
                past.as_bytes(),
                string_dump.as_slice(),
                b"REPLACE",
                b"ABSTTL",
            ],
        ),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXISTS", b"restore-expired"]),
        0
    );

    assert_eq!(
        store.hset(b"dump-h", b"a", b"1"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.hset(b"dump-h", b"b", b"2"),
        RedisObjectResult::Integer(1)
    );
    let hash_dump =
        decode_optional_bulk(&RespTestHarness::exec_resp(&store, &[b"DUMP", b"dump-h"]))
            .expect("dump-h should exist");
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[b"RESTORE", b"restore-h", b"0", hash_dump.as_slice()],
        ),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"HGET", b"restore-h", b"b"]),
        b"$1\r\n2\r\n".to_vec()
    );

    assert_eq!(
        store.rpush(b"dump-l", &[b"a".as_slice(), b"b".as_slice()]),
        RedisObjectResult::Integer(2)
    );
    let list_dump =
        decode_optional_bulk(&RespTestHarness::exec_resp(&store, &[b"DUMP", b"dump-l"]))
            .expect("dump-l should exist");
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[b"RESTORE", b"restore-l", b"0", list_dump.as_slice()],
        ),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"LRANGE", b"restore-l", b"0", b"-1"],
        )),
        vec![b"a".to_vec(), b"b".to_vec()]
    );

    assert_eq!(
        store.sadd(b"dump-set", &[b"b".as_slice(), b"a".as_slice()]),
        RedisObjectResult::Integer(2)
    );
    let set_dump =
        decode_optional_bulk(&RespTestHarness::exec_resp(&store, &[b"DUMP", b"dump-set"]))
            .expect("dump-set should exist");
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[b"RESTORE", b"restore-set", b"0", set_dump.as_slice()],
        ),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        BTreeSet::from_iter(decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"SMEMBERS", b"restore-set"],
        ))),
        BTreeSet::from([b"a".to_vec(), b"b".to_vec()])
    );

    assert_eq!(
        store.zadd(b"dump-z", 1.0, b"a"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"dump-z", 2.5, b"b"),
        RedisObjectResult::Integer(1)
    );
    let zset_dump =
        decode_optional_bulk(&RespTestHarness::exec_resp(&store, &[b"DUMP", b"dump-z"]))
            .expect("dump-z should exist");
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[b"RESTORE", b"restore-z", b"0", zset_dump.as_slice()],
        ),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZRANGE", b"restore-z", b"0", b"-1", b"WITHSCORES"],
        )),
        vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"2.5".to_vec()]
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_restore_validates_idle_time_and_freq_options() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"restore-options-src", b"value"]),
        b"+OK\r\n".to_vec()
    );
    let payload = decode_optional_bulk(&RespTestHarness::exec_resp(
        &store,
        &[b"DUMP", b"restore-options-src"],
    ))
    .expect("restore-options-src should exist");

    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"RESTORE",
                b"restore-idle",
                b"0",
                payload.as_slice(),
                b"REPLACE",
                b"IDLETIME",
                b"0",
            ],
        ),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"RESTORE",
                b"restore-freq",
                b"0",
                payload.as_slice(),
                b"REPLACE",
                b"FREQ",
                b"255",
            ],
        ),
        b"+OK\r\n".to_vec()
    );

    assert_resp_error_contains(
        &store,
        &[
            b"RESTORE",
            b"restore-bad-idle",
            b"0",
            payload.as_slice(),
            b"IDLETIME",
            b"-1",
        ],
        "integer",
    );
    assert_resp_error_contains(
        &store,
        &[
            b"RESTORE",
            b"restore-bad-freq",
            b"0",
            payload.as_slice(),
            b"FREQ",
            b"256",
        ],
        "integer",
    );
    assert_resp_error_contains(
        &store,
        &[
            b"RESTORE",
            b"restore-conflict",
            b"0",
            payload.as_slice(),
            b"IDLETIME",
            b"0",
            b"FREQ",
            b"1",
        ],
        "syntax",
    );
    assert_resp_error_contains(
        &store,
        &[
            b"RESTORE",
            b"restore-missing-idle",
            b"0",
            payload.as_slice(),
            b"IDLETIME",
        ],
        "syntax",
    );
}

#[test]
fn resp_integer_writer_covers_fast_and_fallback_paths() {
    let cases = [
        (-2, b":-2\r\n".as_slice()),
        (-1, b":-1\r\n".as_slice()),
        (0, b":0\r\n".as_slice()),
        (9, b":9\r\n".as_slice()),
        (10, b":10\r\n".as_slice()),
        (99, b":99\r\n".as_slice()),
        (100, b":100\r\n".as_slice()),
        (999, b":999\r\n".as_slice()),
        (1_000, b":1000\r\n".as_slice()),
        (9_999, b":9999\r\n".as_slice()),
        (10_000, b":10000\r\n".as_slice()),
        (-10, b":-10\r\n".as_slice()),
    ];
    let mut out = BytesMut::new();
    for (value, expected) in cases {
        out.clear();
        ServerWire::write_resp_integer(&mut out, value);
        assert_eq!(&out[..], expected);
    }
}

#[test]
fn raw_resp_get_set_del_commands_round_trip() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"k", b"v", b"NX"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"k", b"v2", b"NX"]),
        b"$-1\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"k"]),
        b"$1\r\nv\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"DEL", b"k", b"missing"]),
        b":1\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"k"]),
        b"$-1\r\n".to_vec()
    );
}

#[test]
fn raw_resp_cache_lifecycle_commands_round_trip() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SETEX", b"k", b"60", b"v"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GETEX", b"k", b"PX", b"60000"]),
        b"$1\r\nv\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXISTS", b"k"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"TTL", b"k"]),
        60
    );
    assert!(RespTestHarness::exec_resp_integer(&store, &[b"PTTL", b"k"]) > 0);
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PERSIST", b"k"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"TTL", b"k"]),
        -1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXPIRE", b"k", b"60"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PEXPIRE", b"k", b"60000"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"PSETEX", b"px", b"60000", b"v2"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"px"]),
        b"$2\r\nv2\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"DEL", b"k"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXISTS", b"k"]),
        0
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_redis_backfill_commands_round_trip() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"rename-src", b"v"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"RENAME", b"rename-src", b"rename-dst"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"rename-dst"]),
        b"$1\r\nv\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"renamenx-src", b"v"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"renamenx-dst", b"existing"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(
            &store,
            &[b"RENAMENX", b"renamenx-src", b"renamenx-dst"]
        ),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"UNLINK", b"rename-dst", b"missing"]),
        1
    );

    assert_eq!(
        store.hset(b"h", b"field", b"value"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"HSTRLEN", b"h", b"field"]),
        5
    );

    assert_eq!(
        store.rpush(b"l", &[b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]),
        RedisObjectResult::Integer(3)
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"RPOPLPUSH", b"l", b"l"]),
        b"$1\r\nc\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"BRPOPLPUSH", b"l", b"l2", b"0"]),
        b"$1\r\nb\r\n".to_vec()
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"LRANGE", b"l", b"0", b"-1"]
        )),
        vec![b"c".to_vec(), b"a".to_vec()]
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"LRANGE", b"l2", b"0", b"-1"]
        )),
        vec![b"b".to_vec()]
    );

    assert_eq!(store.zadd(b"z", 1.0, b"a"), RedisObjectResult::Integer(1));
    assert_eq!(store.zadd(b"z", 2.0, b"b"), RedisObjectResult::Integer(1));
    assert_eq!(store.zadd(b"z", 3.0, b"c"), RedisObjectResult::Integer(1));
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZREVRANGE", b"z", b"0", b"-1"],
        )),
        vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZREVRANGEBYSCORE", b"z", b"3", b"(1"],
        )),
        vec![b"c".to_vec(), b"b".to_vec()]
    );

    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZREMRANGEBYRANK", b"z", b"1", b"1"]),
        1
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZRANGE", b"z", b"0", b"-1"]
        )),
        vec![b"a".to_vec(), b"c".to_vec()]
    );

    assert_eq!(
        store.zadd(b"zscore", 1.0, b"a"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"zscore", 2.0, b"b"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"zscore", 3.0, b"c"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZREMRANGEBYSCORE", b"zscore", b"(1", b"3"]),
        2
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZRANGE", b"zscore", b"0", b"-1"],
        )),
        vec![b"a".to_vec()]
    );

    assert_eq!(
        store.zadd(b"zlex2", 0.0, b"a"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"zlex2", 0.0, b"b"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"zlex2", 0.0, b"c"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZREMRANGEBYLEX", b"zlex2", b"[b", b"[c"]),
        2
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZRANGE", b"zlex2", b"0", b"-1"],
        )),
        vec![b"a".to_vec()]
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_missing_compat_batch_round_trip() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"SETNX", b"setnx", b"v"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"SETNX", b"setnx", b"v2"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"setnx"]),
        b"$1\r\nv\r\n".to_vec()
    );

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"expireat", b"v"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXPIREAT", b"expireat", b"9999999999"]),
        1
    );
    assert!(RespTestHarness::exec_resp_integer(&store, &[b"TTL", b"expireat"]) > 0);
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PEXPIREAT", b"expireat", b"1"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXISTS", b"expireat"]),
        0
    );

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"RANDOMKEY"]),
        b"$5\r\nsetnx\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"TOUCH", b"setnx", b"missing"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"OBJECT", b"ENCODING", b"setnx"]),
        b"$3\r\nraw\r\n".to_vec()
    );

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"COPY", b"setnx", b"copy-dst"]),
        b":1\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"copy-dst"]),
        b"$1\r\nv\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"COPY", b"setnx", b"copy-dst"]),
        b":0\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"COPY", b"setnx", b"copy-dst", b"REPLACE"]),
        b":1\r\n".to_vec()
    );

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"HMSET", b"hm", b"f1", b"v1", b"f2", b"v2"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"HGET", b"hm", b"f2"]),
        b"$2\r\nv2\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"OBJECT", b"ENCODING", b"hm"]),
        b"$9\r\nhashtable\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"COPY", b"hm", b"hm-copy"]),
        b":1\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"HGET", b"hm-copy", b"f1"]),
        b"$2\r\nv1\r\n".to_vec()
    );

    assert_eq!(
        store.rpush(b"lm", &[b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]),
        RedisObjectResult::Integer(3)
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"LMOVE", b"lm", b"lm-dst", b"RIGHT", b"LEFT"]),
        b"$1\r\nc\r\n".to_vec()
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"LRANGE", b"lm-dst", b"0", b"-1"]
        )),
        vec![b"c".to_vec()]
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_expanded_redis_surface_round_trip() {
    let store = EmbeddedStore::new(4);
    let bulk = |value: &[u8]| Frame::BlobString(value.to_vec());

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"exp-nx", b"v"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXPIRE", b"exp-nx", b"60", b"NX"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXPIRE", b"exp-nx", b"60", b"NX"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXPIRE", b"exp-nx", b"120", b"GT"]),
        1
    );
    assert!(RespTestHarness::exec_resp_integer(&store, &[b"EXPIRETIME", b"exp-nx"]) > 0);
    assert!(RespTestHarness::exec_resp_integer(&store, &[b"PEXPIRETIME", b"exp-nx"]) > 0);

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"mem", b"value"]),
        b"+OK\r\n".to_vec()
    );
    assert!(RespTestHarness::exec_resp_integer(&store, &[b"MEMORY", b"USAGE", b"mem"]) >= 8);

    assert!(RespTestHarness::exec_resp_integer(&store, &[b"COMMAND", b"COUNT"]) > 0);
    let command_list =
        decode_bulk_array(&RespTestHarness::exec_resp(&store, &[b"COMMAND", b"LIST"]));
    assert!(command_list.iter().any(|name| name == b"GET"));
    assert!(command_list.iter().any(|name| name == b"FLUSHDB"));
    let command_info = decode_resp_stream(&RespTestHarness::exec_resp(
        &store,
        &[b"COMMAND", b"INFO", b"GET"],
    ));
    assert!(matches!(
        &command_info[..],
        [Frame::Array(items)] if matches!(items.first(), Some(Frame::Array(_)))
    ));
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"COMMAND", b"GETKEYS", b"MGET", b"ca", b"cb"]
        )),
        vec![b"ca".to_vec(), b"cb".to_vec()]
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[
                b"COMMAND", b"GETKEYS", b"LMPOP", b"2", b"la", b"lb", b"LEFT"
            ]
        )),
        vec![Frame::Array(vec![bulk(b"la"), bulk(b"lb")])]
    );
    let keys_and_flags = decode_resp_stream(&RespTestHarness::exec_resp(
        &store,
        &[b"COMMAND", b"GETKEYSANDFLAGS", b"MEMORY", b"USAGE", b"mem"],
    ));
    assert!(matches!(
        &keys_and_flags[..],
        [Frame::Array(items)]
            if matches!(
                items.first(),
                Some(Frame::Array(pair))
                    if matches!(pair.first(), Some(Frame::BlobString(key)) if key == b"mem")
            )
    ));
    assert!(
        !decode_bulk_array(&RespTestHarness::exec_resp(&store, &[b"COMMAND", b"HELP"])).is_empty()
    );

    assert_eq!(
        store.rpush(
            b"lmpop",
            &[b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
        ),
        RedisObjectResult::Integer(3)
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"LMPOP", b"1", b"lmpop", b"LEFT", b"COUNT", b"2"]
        )),
        vec![Frame::Array(vec![
            bulk(b"lmpop"),
            Frame::Array(vec![bulk(b"a"), bulk(b"b")])
        ])]
    );

    assert_eq!(
        store.rpush(
            b"blmpop",
            &[b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
        ),
        RedisObjectResult::Integer(3)
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[
                b"BLMPOP", b"0.001", b"1", b"blmpop", b"RIGHT", b"COUNT", b"2"
            ]
        )),
        vec![Frame::Array(vec![
            bulk(b"blmpop"),
            Frame::Array(vec![bulk(b"c"), bulk(b"b")])
        ])]
    );

    assert_eq!(store.zadd(b"zu1", 1.0, b"a"), RedisObjectResult::Integer(1));
    assert_eq!(store.zadd(b"zu1", 2.0, b"b"), RedisObjectResult::Integer(1));
    assert_eq!(store.zadd(b"zu2", 3.0, b"a"), RedisObjectResult::Integer(1));
    assert_eq!(store.zadd(b"zu2", 4.0, b"c"), RedisObjectResult::Integer(1));
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZUNION", b"2", b"zu1", b"zu2", b"WITHSCORES"]
        )),
        vec![
            b"b".to_vec(),
            b"2".to_vec(),
            b"a".to_vec(),
            b"4".to_vec(),
            b"c".to_vec(),
            b"4".to_vec(),
        ]
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZINTER", b"2", b"zu1", b"zu2", b"WITHSCORES"]
        )),
        vec![b"a".to_vec(), b"4".to_vec()]
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZDIFF", b"2", b"zu1", b"zu2"]
        )),
        vec![b"b".to_vec()]
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(
            &store,
            &[b"ZINTERCARD", b"2", b"zu1", b"zu2", b"LIMIT", b"10"]
        ),
        1
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZRANDMEMBER", b"zu1", b"2", b"WITHSCORES"]
        )),
        vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"2".to_vec()]
    );

    assert_eq!(
        store.zadd(b"zmpop", 1.0, b"a"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"zmpop", 2.0, b"b"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"zmpop", 3.0, b"c"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"ZMPOP", b"1", b"zmpop", b"MIN", b"COUNT", b"2"]
        )),
        vec![Frame::Array(vec![
            bulk(b"zmpop"),
            Frame::Array(vec![
                Frame::Array(vec![bulk(b"a"), bulk(b"1")]),
                Frame::Array(vec![bulk(b"b"), bulk(b"2")]),
            ])
        ])]
    );

    assert_eq!(
        store.zadd(b"bzmpop", 1.0, b"a"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"bzmpop", 2.0, b"b"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"bzmpop", 3.0, b"c"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"BZMPOP", b"0.001", b"1", b"bzmpop", b"MAX", b"COUNT", b"2"]
        )),
        vec![Frame::Array(vec![
            bulk(b"bzmpop"),
            Frame::Array(vec![
                Frame::Array(vec![bulk(b"c"), bulk(b"3")]),
                Frame::Array(vec![bulk(b"b"), bulk(b"2")]),
            ])
        ])]
    );

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"ASKING"]),
        b"+OK\r\n".to_vec()
    );
    assert!(RespTestHarness::exec_resp_integer(&store, &[b"LASTSAVE"]) > 0);
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(&store, &[b"ROLE"])),
        vec![Frame::Array(vec![
            bulk(b"master"),
            Frame::Integer(0),
            Frame::Array(Vec::new())
        ])]
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(&store, &[b"MODULE", b"LIST"])),
        vec![Frame::Array(Vec::new())]
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"SLOWLOG", b"LEN"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"WAIT", b"1", b"1"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"REPLICAOF", b"NO", b"ONE"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SLAVEOF", b"NO", b"ONE"]),
        b"+OK\r\n".to_vec()
    );
    assert_resp_error_contains(&store, &[b"CLUSTER", b"INFO"], "cluster support disabled");

    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PUBLISH", b"chan", b"msg"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PUBSUB", b"NUMPAT"]),
        0
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"SUBSCRIBE", b"chan"]
        )),
        vec![Frame::Array(vec![
            bulk(b"subscribe"),
            bulk(b"chan"),
            Frame::Integer(1)
        ])]
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"UNSUBSCRIBE", b"chan"]
        )),
        vec![Frame::Array(vec![
            bulk(b"unsubscribe"),
            bulk(b"chan"),
            Frame::Integer(0)
        ])]
    );

    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PFADD", b"hll", b"a", b"b"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PFADD", b"hll", b"a"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PFCOUNT", b"hll"]),
        2
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"PFMERGE", b"hll-merged", b"hll"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PFCOUNT", b"hll-merged"]),
        2
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"PFSELFTEST"]),
        b"+OK\r\n".to_vec()
    );

    assert_eq!(
        store.rpush(
            b"sort-list",
            &[b"3".as_slice(), b"1".as_slice(), b"2".as_slice()]
        ),
        RedisObjectResult::Integer(3)
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"SORT", b"sort-list"]
        )),
        vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(
            &store,
            &[b"SORT", b"sort-list", b"DESC", b"STORE", b"sort-out"]
        ),
        3
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"LRANGE", b"sort-out", b"0", b"-1"]
        )),
        vec![b"3".to_vec(), b"2".to_vec(), b"1".to_vec()]
    );

    assert_eq!(
        RespTestHarness::exec_resp_integer(
            &store,
            &[
                b"GEOADD",
                b"geo",
                b"-73.9857",
                b"40.7484",
                b"empire",
                b"-73.9897",
                b"40.7411",
                b"flatiron"
            ]
        ),
        2
    );
    assert_eq!(store.zcard(b"geo"), RedisObjectResult::Integer(2));
    assert!(
        !decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"GEOPOS", b"geo", b"empire"]
        ))
        .is_empty()
    );
    assert!(
        !decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"GEOHASH", b"geo", b"empire"]
        ))
        .is_empty()
    );
    assert!(
        !decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"GEORADIUS", b"geo", b"-73.9857", b"40.7484", b"2", b"km"]
        ))
        .is_empty()
    );
    assert!(
        !decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"GEORADIUSBYMEMBER", b"geo", b"empire", b"2", b"km"]
        ))
        .is_empty()
    );
    assert_ne!(
        RespTestHarness::exec_resp(&store, &[b"GEODIST", b"geo", b"empire", b"flatiron", b"km"]),
        b"$-1\r\n".to_vec()
    );

    let xadd = RespTestHarness::exec_resp(&store, &[b"XADD", b"stream", b"1-0", b"f", b"v"]);
    assert_eq!(xadd, b"$3\r\n1-0\r\n".to_vec());
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"XLEN", b"stream"]),
        1
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"XRANGE", b"stream", b"-", b"+"]
        ))
        .len(),
        1
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"XREAD", b"COUNT", b"1", b"STREAMS", b"stream", b"0-0"]
        ))
        .len(),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"XGROUP", b"CREATE", b"stream", b"g", b"0-0"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"XACK", b"stream", b"g", b"1-0"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"XCLAIM", b"stream", b"g", b"c", b"0", b"1-0"]),
        b"*0\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"XDEL", b"stream", b"1-0"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"XSETID", b"stream", b"5-0"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"XTRIM", b"stream", b"MAXLEN", b"1"]),
        0
    );
    assert!(
        !decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"XINFO", b"STREAM", b"stream"]
        ))
        .is_empty()
    );

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"flush-me", b"v"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"FLUSHDB"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXISTS", b"flush-me"]),
        0
    );

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"flush-all", b"v"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"FLUSHALL", b"SYNC"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"EXISTS", b"flush-all"]),
        0
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp2_supported_command_surface_has_smoke_coverage() {
    let mut expected = crate::commands::CATALOG
        .iter()
        .map(|command| command.name())
        .collect::<BTreeSet<_>>();
    expected.extend(RESP2_PROTOCOL_COMMANDS.iter().copied());
    expected.extend(RESP2_ALIAS_COMMANDS.iter().copied());

    let command_list = decode_bulk_array(&RespTestHarness::exec_resp(
        &EmbeddedStore::new(4),
        &[b"COMMAND", b"LIST"],
    ))
    .into_iter()
    .map(|name| String::from_utf8(name).expect("COMMAND LIST name should be utf8"))
    .collect::<BTreeSet<_>>();
    let expected_command_list = expected
        .iter()
        .map(|name| (*name).to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        command_list, expected_command_list,
        "COMMAND LIST must expose every supported RESP2 command"
    );

    let missing = expected
        .iter()
        .copied()
        .filter(|name| resp2_smoke_commands(name).is_none())
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "missing RESP2 smoke coverage for: {}",
        missing.join(", ")
    );

    for alias in RESP2_ALIAS_COMMANDS {
        assert!(
            crate::commands::CommandCatalog::find(alias.as_bytes()).is_some(),
            "RESP2 alias {alias} should be accepted by the command catalog"
        );
    }

    for name in expected {
        let commands = resp2_smoke_commands(name).expect("coverage was checked above");
        assert_resp2_smoke_case(name, commands);
    }
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_redis_backfill_error_paths_round_trip() {
    let store = EmbeddedStore::new(4);

    assert_resp_error_contains(&store, &[b"RENAME", b"missing", b"dest"], "no such key");
    assert_resp_error_contains(&store, &[b"RENAMENX", b"missing", b"dest"], "no such key");
    assert_resp_error_contains(&store, &[b"UNLINK"], "wrong number");

    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"HSTRLEN", b"missing-h", b"field"]),
        0
    );
    store.set(b"wrong".to_vec(), b"value".to_vec(), None);
    assert_resp_error_contains(&store, &[b"HSTRLEN", b"wrong", b"field"], "WRONGTYPE");

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"RPOPLPUSH", b"missing-list", b"wrong"]),
        b"$-1\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"BRPOPLPUSH", b"missing-list", b"wrong", b"0"]),
        b"$-1\r\n".to_vec()
    );
    assert_eq!(
        store.rpush(b"rpl-src", &[b"a".as_slice(), b"b".as_slice()]),
        RedisObjectResult::Integer(2)
    );
    assert_resp_error_contains(&store, &[b"RPOPLPUSH", b"rpl-src", b"wrong"], "WRONGTYPE");
    assert_resp_error_contains(
        &store,
        &[b"BRPOPLPUSH", b"rpl-src", b"wrong", b"0"],
        "WRONGTYPE",
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"LRANGE", b"rpl-src", b"0", b"-1"]
        )),
        vec![b"a".to_vec(), b"b".to_vec()]
    );

    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZREVRANGE", b"missing-z", b"0", b"-1"],
        )),
        Vec::<Vec<u8>>::new()
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZREVRANGEBYSCORE", b"missing-z", b"+inf", b"-inf"],
        )),
        Vec::<Vec<u8>>::new()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZREMRANGEBYRANK", b"missing-z", b"0", b"1"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(
            &store,
            &[b"ZREMRANGEBYSCORE", b"missing-z", b"-inf", b"+inf"]
        ),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZREMRANGEBYLEX", b"missing-z", b"-", b"+"]),
        0
    );

    assert_resp_error_contains(&store, &[b"ZREVRANGE", b"wrong", b"0", b"-1"], "WRONGTYPE");
    assert_resp_error_contains(
        &store,
        &[b"ZREVRANGEBYSCORE", b"wrong", b"+inf", b"-inf"],
        "WRONGTYPE",
    );
    assert_resp_error_contains(
        &store,
        &[b"ZREMRANGEBYRANK", b"wrong", b"0", b"1"],
        "WRONGTYPE",
    );
    assert_resp_error_contains(
        &store,
        &[b"ZREMRANGEBYSCORE", b"wrong", b"-inf", b"+inf"],
        "WRONGTYPE",
    );
    assert_resp_error_contains(
        &store,
        &[b"ZREMRANGEBYLEX", b"wrong", b"-", b"+"],
        "WRONGTYPE",
    );

    assert_resp_error_contains(&store, &[b"ZREVRANGE", b"z", b"0", b"not-int"], "integer");
    assert_resp_error_contains(
        &store,
        &[b"ZREVRANGEBYSCORE", b"z", b"not-float", b"0"],
        "float",
    );
    assert_resp_error_contains(
        &store,
        &[b"ZREMRANGEBYSCORE", b"z", b"not-float", b"0"],
        "float",
    );
    assert_resp_error_contains(
        &store,
        &[b"ZREMRANGEBYLEX", b"z", b"bad-bound", b"+"],
        "not valid",
    );
    assert_resp_error_contains(&store, &[b"XRANGE", b"stream", b"-"], "wrong number");
    assert_resp_error_contains(
        &store,
        &[b"XRANGE", b"stream", b"-", b"+", b"BAD"],
        "syntax",
    );
    assert_resp_error_contains(
        &store,
        &[b"XRANGE", b"stream", b"-", b"+", b"COUNT", b"bad"],
        "integer",
    );
    assert_resp_error_contains(&store, &[b"XREAD", b"COUNT"], "syntax");
    assert_resp_error_contains(&store, &[b"XREAD", b"BLOCK"], "syntax");
    assert_resp_error_contains(
        &store,
        &[b"XREAD", b"COUNT", b"bad", b"STREAMS", b"stream", b"0-0"],
        "integer",
    );
    assert_resp_error_contains(&store, &[b"XREAD", b"STREAMS", b"stream"], "Unbalanced");
    assert_resp_error_contains(&store, &[b"XREADGROUP", b"GROUP"], "wrong number");
    assert_resp_error_contains(&store, &[b"XREADGROUP", b"GROUP", b"g", b"c"], "syntax");
    assert_resp_error_contains(&store, &[b"ZMPOP"], "wrong number");
    assert_resp_error_contains(&store, &[b"ZMPOP", b"0", b"z", b"MIN"], "numkeys");
    assert_resp_error_contains(&store, &[b"ZMPOP", b"1", b"z", b"MIDDLE"], "syntax");
    assert_resp_error_contains(&store, &[b"ZMPOP", b"1", b"z", b"MIN", b"COUNT"], "syntax");
    assert_resp_error_contains(
        &store,
        &[b"ZMPOP", b"1", b"z", b"MIN", b"COUNT", b"0"],
        "count",
    );
    assert_resp_error_contains(&store, &[b"BZMPOP", b"bad", b"1", b"z", b"MIN"], "timeout");
    assert_resp_error_contains(&store, &[b"BZMPOP", b"-1", b"1", b"z", b"MIN"], "negative");
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_missing_compat_batch_error_paths_round_trip() {
    let store = EmbeddedStore::new(4);

    assert_resp_error_contains(&store, &[b"SETNX", b"only-key"], "wrong number");
    assert_resp_error_contains(&store, &[b"EXPIREAT", b"k", b"not-int"], "integer");
    assert_resp_error_contains(&store, &[b"PEXPIREAT", b"k", b"not-int"], "integer");
    assert_resp_error_contains(&store, &[b"TOUCH"], "wrong number");
    assert_resp_error_contains(&store, &[b"RANDOMKEY", b"extra"], "wrong number");
    assert_resp_error_contains(&store, &[b"COPY", b"src"], "wrong number");
    assert_resp_error_contains(&store, &[b"COPY", b"src", b"dst", b"BAD"], "syntax");
    assert_resp_error_contains(&store, &[b"COPY", b"src", b"dst", b"DB", b"1"], "DB index");
    assert_resp_error_contains(&store, &[b"OBJECT", b"REFCOUNT", b"k"], "syntax");
    assert_resp_error_contains(&store, &[b"HMSET", b"h", b"f"], "wrong number");
    assert_resp_error_contains(
        &store,
        &[b"LMOVE", b"src", b"dst", b"BAD", b"LEFT"],
        "syntax",
    );

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"RANDOMKEY"]),
        b"$-1\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"OBJECT", b"ENCODING", b"missing"]),
        b"$-1\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"LMOVE", b"missing", b"dst", b"RIGHT", b"LEFT"]),
        b"$-1\r\n".to_vec()
    );

    store.set(b"wrong-dst".to_vec(), b"value".to_vec(), None);
    assert_eq!(
        store.rpush(b"lm-src", &[b"a".as_slice(), b"b".as_slice()]),
        RedisObjectResult::Integer(2)
    );
    assert_resp_error_contains(
        &store,
        &[b"LMOVE", b"lm-src", b"wrong-dst", b"RIGHT", b"LEFT"],
        "WRONGTYPE",
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"LRANGE", b"lm-src", b"0", b"-1"]
        )),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
    assert_resp_error_contains(
        &store,
        &[b"BLMOVE", b"lm-src", b"wrong-dst", b"RIGHT", b"LEFT", b"0"],
        "WRONGTYPE",
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"LRANGE", b"lm-src", b"0", b"-1"]
        )),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_bit_commands_round_trip() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"GETBIT", b"bits", b"7"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"SETBIT", b"bits", b"7", b"1"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"GETBIT", b"bits", b"7"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"SETBIT", b"bits", b"0", b"1"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"BITCOUNT", b"bits"]),
        2
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"BITCOUNT", b"bits", b"0", b"7", b"BIT"]),
        2
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"BITPOS", b"bits", b"1"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"BITPOS", b"bits", b"0"]),
        1
    );

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"bit-a", b"\x0f"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SET", b"bit-b", b"\xf0"]),
        b"+OK\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(
            &store,
            &[b"BITOP", b"OR", b"bit-out", b"bit-a", b"bit-b"]
        ),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"bit-out"]),
        b"$1\r\n\xff\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"BITOP", b"NOT", b"bit-not", b"bit-a"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"bit-not"]),
        b"$1\r\n\xf0\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"BITFIELD",
                b"bitfield",
                b"SET",
                b"u8",
                b"0",
                b"255",
                b"GET",
                b"u8",
                b"0",
                b"INCRBY",
                b"u8",
                b"0",
                b"1",
            ],
        ),
        b"*3\r\n:0\r\n:255\r\n:0\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"BITFIELD",
                b"bitfield-sat",
                b"OVERFLOW",
                b"SAT",
                b"INCRBY",
                b"i4",
                b"0",
                b"8",
            ],
        ),
        b"*1\r\n:7\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"BITFIELD",
                b"bitfield-fail",
                b"SET",
                b"u2",
                b"0",
                b"3",
                b"OVERFLOW",
                b"FAIL",
                b"INCRBY",
                b"u2",
                b"0",
                b"1",
                b"GET",
                b"u2",
                b"0",
            ],
        ),
        b"*3\r\n:0\r\n$-1\r\n:3\r\n".to_vec()
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_bit_commands_error_paths_round_trip() {
    let store = EmbeddedStore::new(4);

    assert_resp_error_contains(&store, &[b"GETBIT", b"k"], "wrong number");
    assert_resp_error_contains(&store, &[b"GETBIT", b"k", b"-1"], "bit offset");
    assert_resp_error_contains(&store, &[b"SETBIT", b"k", b"0", b"2"], "bit");
    assert_resp_error_contains(&store, &[b"BITCOUNT", b"k", b"0"], "wrong number");
    assert_resp_error_contains(&store, &[b"BITCOUNT", b"k", b"0", b"1", b"BAD"], "syntax");
    assert_resp_error_contains(&store, &[b"BITPOS", b"k", b"2"], "bit");
    assert_resp_error_contains(&store, &[b"BITOP", b"BAD", b"dst", b"k"], "syntax");
    assert_resp_error_contains(
        &store,
        &[b"BITOP", b"NOT", b"dst", b"a", b"b"],
        "wrong number",
    );
    assert_resp_error_contains(
        &store,
        &[b"BITFIELD", b"k", b"GET", b"bad", b"0"],
        "bitfield",
    );
    assert_resp_error_contains(
        &store,
        &[b"BITFIELD", b"k", b"GET", b"u8", b"-1"],
        "bit offset",
    );

    assert_eq!(
        store.hset(b"hbits", b"f", b"v"),
        RedisObjectResult::Integer(1)
    );
    assert_resp_error_contains(&store, &[b"GETBIT", b"hbits", b"0"], "WRONGTYPE");
    assert_resp_error_contains(&store, &[b"BITOP", b"OR", b"dst", b"hbits"], "WRONGTYPE");
    assert_resp_error_contains(
        &store,
        &[b"BITFIELD", b"hbits", b"GET", b"u8", b"0"],
        "WRONGTYPE",
    );
}

#[cfg(feature = "redis")]
#[test]
fn raw_resp_transactions_queue_and_exec_in_order() {
    let store = EmbeddedStore::new(4);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[
            &[b"MULTI"],
            &[b"SET", b"txn", b"1"],
            &[b"GET", b"txn"],
            &[b"EXEC"],
        ],
        TransactionMode::ShardLocal,
    );

    assert_eq!(frames[0], Frame::SimpleString("OK".into()));
    assert_eq!(frames[1], Frame::SimpleString("QUEUED".into()));
    assert_eq!(frames[2], Frame::SimpleString("QUEUED".into()));
    assert_eq!(
        frames[3],
        Frame::Array(vec![
            Frame::SimpleString("OK".into()),
            Frame::BlobString(b"1".to_vec())
        ])
    );
}

#[cfg(feature = "redis")]
#[test]
fn raw_resp_transactions_discard_queued_commands() {
    let store = EmbeddedStore::new(4);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[&[b"MULTI"], &[b"SET", b"txn-discard", b"1"], &[b"DISCARD"]],
        TransactionMode::ShardLocal,
    );

    assert_eq!(frames[0], Frame::SimpleString("OK".into()));
    assert_eq!(frames[1], Frame::SimpleString("QUEUED".into()));
    assert_eq!(frames[2], Frame::SimpleString("OK".into()));
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"txn-discard"]),
        b"$-1\r\n"
    );
}

#[cfg(feature = "redis")]
#[test]
fn raw_resp_watch_unwatch_and_exec_conflicts() {
    let store = EmbeddedStore::new(4);
    let coordinator = TransactionCoordinator::new(store.shard_count(), TransactionMode::ShardLocal)
        .expect("coordinator");
    let mut transaction_state = TransactionState::default();
    let mut out = BytesMut::new();

    assert!(transaction_state.handle_resp_command(
        Some(&coordinator),
        &store,
        &[b"WATCH", b"watched"],
        &mut out,
        RespProtocolVersion::Resp2,
    ));
    store.set(b"watched".to_vec(), b"outside".to_vec(), None);
    assert!(transaction_state.handle_resp_command(
        Some(&coordinator),
        &store,
        &[b"MULTI"],
        &mut out,
        RespProtocolVersion::Resp2,
    ));
    assert!(transaction_state.handle_resp_command(
        Some(&coordinator),
        &store,
        &[b"SET", b"watched", b"inside"],
        &mut out,
        RespProtocolVersion::Resp2,
    ));
    assert!(transaction_state.handle_resp_command(
        Some(&coordinator),
        &store,
        &[b"EXEC"],
        &mut out,
        RespProtocolVersion::Resp2,
    ));

    assert_eq!(
        decode_resp_stream(&out),
        vec![
            Frame::SimpleString("OK".into()),
            Frame::SimpleString("OK".into()),
            Frame::SimpleString("QUEUED".into()),
            Frame::Null,
        ]
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"watched"]),
        b"$7\r\noutside\r\n".to_vec()
    );

    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[&[b"WATCH", b"watched"], &[b"UNWATCH"]],
        TransactionMode::ShardLocal,
    );
    assert_eq!(
        frames,
        vec![
            Frame::SimpleString("OK".into()),
            Frame::SimpleString("OK".into())
        ]
    );
}

#[cfg(feature = "redis")]
#[test]
fn raw_resp_scripting_commands_round_trip() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"EVAL", b"return 'ok'", b"0"]),
        b"$2\r\nok\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"EVAL",
                b"return {KEYS[1], ARGV[1], tonumber(ARGV[2])}",
                b"1",
                b"script-key",
                b"arg-value",
                b"42",
            ],
        ),
        b"*3\r\n$10\r\nscript-key\r\n$9\r\narg-value\r\n:42\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"EVAL",
                b"redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
                b"1",
                b"script-store",
                b"value",
            ],
        ),
        b"$5\r\nvalue\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"script-store"]),
        b"$5\r\nvalue\r\n".to_vec()
    );

    let load = RespTestHarness::exec_resp(&store, &[b"SCRIPT", b"LOAD", b"return 'ok'"]);
    assert_eq!(
        load,
        b"$40\r\n34f6a80fdc91746367dd8b572351df66b92c67ed\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"SCRIPT",
                b"EXISTS",
                b"34f6a80fdc91746367dd8b572351df66b92c67ed",
                b"ffffffffffffffffffffffffffffffffffffffff",
            ],
        ),
        b"*2\r\n:1\r\n:0\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"EVALSHA",
                b"34f6a80fdc91746367dd8b572351df66b92c67ed",
                b"0",
            ],
        ),
        b"$2\r\nok\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SCRIPT", b"KILL"]),
        b"-NOTBUSY No scripts in execution right now.\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SCRIPT", b"FLUSH"]),
        b"+OK\r\n".to_vec()
    );

    let missing = RespTestHarness::exec_resp(
        &store,
        &[
            b"EVALSHA",
            b"34f6a80fdc91746367dd8b572351df66b92c67ed",
            b"0",
        ],
    );
    let frames = decode_resp_stream(&missing);
    assert!(matches!(&frames[0], Frame::Error(message) if message.contains("NOSCRIPT")));
}

#[cfg(feature = "redis")]
#[test]
fn raw_resp_transactions_unknown_command_aborts_exec() {
    let store = EmbeddedStore::new(4);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[
            &[b"MULTI"],
            &[b"NO_SUCH_COMMAND", b"txn-abort"],
            &[b"SET", b"txn-abort", b"1"],
            &[b"EXEC"],
        ],
        TransactionMode::ShardLocal,
    );

    assert_eq!(frames[0], Frame::SimpleString("OK".into()));
    assert!(matches!(&frames[1], Frame::Error(message) if message.contains("unsupported command")));
    assert_eq!(frames[2], Frame::SimpleString("QUEUED".into()));
    assert!(matches!(&frames[3], Frame::Error(message) if message.contains("EXECABORT")));
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"txn-abort"]),
        b"$-1\r\n"
    );
}

#[cfg(feature = "redis")]
#[test]
fn raw_resp_transactions_can_be_disabled() {
    let store = EmbeddedStore::new(4);
    let frames =
        RespTestHarness::exec_resp_sequence(&store, &[&[b"MULTI"]], TransactionMode::Disabled);

    assert!(
        matches!(&frames[0], Frame::Error(message) if message.contains("transactions are disabled"))
    );
}

#[cfg(feature = "redis")]
#[test]
fn raw_resp_shard_local_transactions_reject_cross_shard_exec() {
    let store = EmbeddedStore::new(4);
    let key_a = key_for_shard(&store, 0);
    let key_b = key_for_shard(&store, 1);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[
            &[b"MULTI"],
            &[b"SET", key_a.as_slice(), b"a"],
            &[b"SET", key_b.as_slice(), b"b"],
            &[b"EXEC"],
        ],
        TransactionMode::ShardLocal,
    );

    assert_eq!(frames[0], Frame::SimpleString("OK".into()));
    assert_eq!(frames[1], Frame::SimpleString("QUEUED".into()));
    assert_eq!(frames[2], Frame::SimpleString("QUEUED".into()));
    assert_eq!(
        frames[3],
        Frame::Error("CROSSSLOT Keys in request don't hash to the same shard".into())
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", key_a.as_slice()]),
        b"$-1\r\n"
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", key_b.as_slice()]),
        b"$-1\r\n"
    );
}

#[cfg(feature = "redis")]
#[test]
fn raw_resp_coordinated_transactions_allow_cross_shard_exec() {
    let store = EmbeddedStore::new(4);
    let key_a = key_for_shard(&store, 0);
    let key_b = key_for_shard(&store, 1);
    let frames = RespTestHarness::exec_resp_sequence(
        &store,
        &[
            &[b"MULTI"],
            &[b"SET", key_a.as_slice(), b"a"],
            &[b"SET", key_b.as_slice(), b"b"],
            &[b"EXEC"],
        ],
        TransactionMode::CoordinatedCrossShard,
    );

    assert_eq!(frames[0], Frame::SimpleString("OK".into()));
    assert_eq!(frames[1], Frame::SimpleString("QUEUED".into()));
    assert_eq!(frames[2], Frame::SimpleString("QUEUED".into()));
    assert_eq!(
        frames[3],
        Frame::Array(vec![
            Frame::SimpleString("OK".into()),
            Frame::SimpleString("OK".into())
        ])
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", key_a.as_slice()]),
        b"$1\r\na\r\n"
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", key_b.as_slice()]),
        b"$1\r\nb\r\n"
    );
}

#[cfg(feature = "redis")]
#[test]
fn scnp_resp_transactions_queue_and_exec_in_order() {
    let store = EmbeddedStore::new(4);
    let responses = RespTestHarness::exec_scnp_resp_sequence(
        &store,
        &[
            &[b"MULTI"],
            &[b"SET", b"scnp-txn", b"1"],
            &[b"GET", b"scnp-txn"],
            &[b"EXEC"],
        ],
        TransactionMode::ShardLocal,
    );

    assert_eq!(responses[0], FastResponse::Value(b"+OK\r\n".to_vec()));
    assert_eq!(responses[1], FastResponse::Value(b"+QUEUED\r\n".to_vec()));
    assert_eq!(responses[2], FastResponse::Value(b"+QUEUED\r\n".to_vec()));
    assert_eq!(
        responses[3],
        FastResponse::Value(b"*2\r\n+OK\r\n$1\r\n1\r\n".to_vec())
    );
}

#[cfg(feature = "redis")]
#[test]
fn scnp_typed_command_inside_transaction_aborts_exec() {
    let store = EmbeddedStore::new(4);
    let mut input = Vec::new();
    encode_scnp_resp_command(&[b"MULTI"], &mut input);
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(hash_key(b"typed-scnp-txn")),
            route_shard: None,
            key_tag: None,
            command: FastCommand::Set {
                key: b"typed-scnp-txn",
                value: b"1",
            },
        },
        &mut input,
    );
    encode_scnp_resp_command(&[b"EXEC"], &mut input);

    let responses =
        RespTestHarness::process_scnp_input(&store, &input, TransactionMode::ShardLocal);
    assert_eq!(responses[0], FastResponse::Value(b"+OK\r\n".to_vec()));
    assert!(
        matches!(&responses[1], FastResponse::Error(message) if message.windows("typed SCNP".len()).any(|window| window == b"typed SCNP"))
    );
    assert!(
        matches!(&responses[2], FastResponse::Value(message) if message.starts_with(b"-EXECABORT"))
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"typed-scnp-txn"]),
        b"$-1\r\n"
    );
}

#[test]
fn scnp_resp_command_wraps_redis_reply_bytes() {
    let store = EmbeddedStore::new(4);
    let response = RespTestHarness::exec_scnp_resp(
        &store,
        vec![b"SET".as_slice(), b"k".as_slice(), b"v".as_slice()],
    );
    assert_eq!(response, FastResponse::Value(b"+OK\r\n".to_vec()));

    let response =
        RespTestHarness::exec_scnp_resp(&store, vec![b"GET".as_slice(), b"k".as_slice()]);
    assert_eq!(response, FastResponse::Value(b"$1\r\nv\r\n".to_vec()));
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_scan_walks_with_cursor_and_count() {
    let store = EmbeddedStore::new(4);
    for key in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
        store.set(key.to_vec(), b"v".to_vec(), None);
    }

    let mut cursor = 0;
    let mut seen = BTreeSet::new();
    for _ in 0..16 {
        let cursor_text = cursor.to_string();
        let raw =
            RespTestHarness::exec_resp(&store, &[b"SCAN", cursor_text.as_bytes(), b"COUNT", b"1"]);
        let (next_cursor, keys) = decode_scan_response(&raw);
        assert!(keys.len() <= 1);
        seen.extend(keys);
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    assert_eq!(
        seen,
        BTreeSet::from([b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_scan_match_still_bounds_examined_keys() {
    let store = EmbeddedStore::new(4);
    for index in 0..16 {
        store.set(format!("key:{index:02}").into_bytes(), b"v".to_vec(), None);
    }

    let raw = RespTestHarness::exec_resp(
        &store,
        &[b"SCAN", b"0", b"MATCH", b"absent:*", b"COUNT", b"2"],
    );
    let (cursor, keys) = decode_scan_response(&raw);

    assert_ne!(cursor, 0);
    assert!(keys.is_empty());
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_scan_type_string_excludes_object_keys() {
    let store = EmbeddedStore::new(4);
    store.set(b"s".to_vec(), b"v".to_vec(), None);
    assert_eq!(store.hset(b"h", b"f", b"v"), RedisObjectResult::Integer(1));

    let raw = RespTestHarness::exec_resp(
        &store,
        &[b"SCAN", b"0", b"TYPE", b"string", b"COUNT", b"100"],
    );
    let (cursor, keys) = decode_scan_response(&raw);

    assert_eq!(cursor, 0);
    assert_eq!(BTreeSet::from_iter(keys), BTreeSet::from([b"s".to_vec()]));
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_scan_type_object_walks_objects_with_cursor() {
    let store = EmbeddedStore::new(4);
    assert_eq!(
        store.hset(b"h:one", b"f", b"1"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.hset(b"h:two", b"f", b"2"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.sadd(b"s", &[b"m".as_slice()]),
        RedisObjectResult::Integer(1)
    );

    let mut cursor = 0;
    let mut seen = BTreeSet::new();
    for _ in 0..16 {
        let cursor_text = cursor.to_string();
        let raw = RespTestHarness::exec_resp(
            &store,
            &[
                b"SCAN",
                cursor_text.as_bytes(),
                b"TYPE",
                b"hash",
                b"COUNT",
                b"1",
            ],
        );
        let (next_cursor, keys) = decode_scan_response(&raw);
        assert!(keys.len() <= 1);
        seen.extend(keys);
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    assert_eq!(seen, BTreeSet::from([b"h:one".to_vec(), b"h:two".to_vec()]));
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_keys_streams_string_and_object_keys() {
    let store = EmbeddedStore::new(4);
    store.set(b"str:one".to_vec(), b"v".to_vec(), None);
    store.set(b"other".to_vec(), b"v".to_vec(), None);
    assert_eq!(
        store.hset(b"hash:one", b"field", b"value"),
        RedisObjectResult::Integer(1)
    );

    let all = decode_bulk_array(&RespTestHarness::exec_resp(&store, &[b"KEYS", b"*"]));
    assert_eq!(
        BTreeSet::from_iter(all),
        BTreeSet::from([b"hash:one".to_vec(), b"other".to_vec(), b"str:one".to_vec()])
    );

    let filtered = decode_bulk_array(&RespTestHarness::exec_resp(&store, &[b"KEYS", b"*one"]));
    assert_eq!(
        BTreeSet::from_iter(filtered),
        BTreeSet::from([b"hash:one".to_vec(), b"str:one".to_vec()])
    );
}

#[test]
#[cfg(feature = "redis")]
fn raw_resp_object_streaming_commands_round_trip() {
    let store = EmbeddedStore::new(4);

    store.set(b"blob".to_vec(), vec![b'x'; 64], None);
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"STRLEN", b"blob"]),
        64
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"SUBSTR", b"blob", b"1", b"3"]),
        b"$3\r\nxxx\r\n".to_vec()
    );

    assert_eq!(store.hset(b"h", b"a", b"1"), RedisObjectResult::Integer(1));
    assert_eq!(store.hset(b"h", b"b", b"2"), RedisObjectResult::Integer(1));
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"STRLEN", b"blob"]),
        64
    );
    let mut full_blob = b"$64\r\n".to_vec();
    full_blob.extend_from_slice(&[b'x'; 64]);
    full_blob.extend_from_slice(b"\r\n");
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GETRANGE", b"blob", b"0", b"-1"]),
        full_blob
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"blob"]),
        full_blob
    );
    assert_resp_error_contains(&store, &[b"GET", b"h"], "WRONGTYPE");
    assert_resp_error_contains(&store, &[b"STRLEN", b"h"], "WRONGTYPE");
    assert_resp_error_contains(&store, &[b"GETRANGE", b"h", b"0", b"-1"], "WRONGTYPE");

    let hkeys = decode_bulk_array(&RespTestHarness::exec_resp(&store, &[b"HKEYS", b"h"]));
    assert_eq!(
        BTreeSet::from_iter(hkeys),
        BTreeSet::from([b"a".to_vec(), b"b".to_vec()])
    );
    let hvals = decode_bulk_array(&RespTestHarness::exec_resp(&store, &[b"HVALS", b"h"]));
    assert_eq!(
        BTreeSet::from_iter(hvals),
        BTreeSet::from([b"1".to_vec(), b"2".to_vec()])
    );
    let hgetall = decode_bulk_array(&RespTestHarness::exec_resp(&store, &[b"HGETALL", b"h"]));
    assert_eq!(
        hgetall
            .chunks_exact(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec())
        ])
    );

    let (cursor, hscan) =
        decode_scan_response(&RespTestHarness::exec_resp(&store, &[b"HSCAN", b"h", b"0"]));
    assert_eq!(cursor, 0);
    assert_eq!(
        hscan
            .chunks_exact(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec())
        ])
    );

    assert_eq!(
        store.sadd(b"s", &[b"m1".as_slice(), b"m2".as_slice()]),
        RedisObjectResult::Integer(2)
    );
    let smembers = decode_bulk_array(&RespTestHarness::exec_resp(&store, &[b"SMEMBERS", b"s"]));
    assert_eq!(
        BTreeSet::from_iter(smembers),
        BTreeSet::from([b"m1".to_vec(), b"m2".to_vec()])
    );
    let srandmember = decode_bulk_array(&RespTestHarness::exec_resp(
        &store,
        &[b"SRANDMEMBER", b"s", b"2"],
    ));
    assert_eq!(
        BTreeSet::from_iter(srandmember),
        BTreeSet::from([b"m1".to_vec(), b"m2".to_vec()])
    );

    let (cursor, sscan) =
        decode_scan_response(&RespTestHarness::exec_resp(&store, &[b"SSCAN", b"s", b"0"]));
    assert_eq!(cursor, 0);
    assert_eq!(
        BTreeSet::from_iter(sscan),
        BTreeSet::from([b"m1".to_vec(), b"m2".to_vec()])
    );

    assert_eq!(store.zadd(b"z", 1.0, b"a"), RedisObjectResult::Integer(1));
    assert_eq!(store.zadd(b"z", 2.0, b"b"), RedisObjectResult::Integer(1));
    assert_eq!(store.zadd(b"z", 3.0, b"c"), RedisObjectResult::Integer(1));
    assert_eq!(
        store.zadd(b"z", 25.0 / 10.0, b"d"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"ZSCORE", b"z", b"b"]),
        b"$1\r\n2\r\n".to_vec()
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"ZSCORE", b"z", b"d"]),
        b"$3\r\n2.5\r\n".to_vec()
    );
    let zrange_with_scores = decode_bulk_array(&RespTestHarness::exec_resp(
        &store,
        &[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"],
    ));
    assert_eq!(
        zrange_with_scores,
        vec![
            b"a".to_vec(),
            b"1".to_vec(),
            b"b".to_vec(),
            b"2".to_vec(),
            b"d".to_vec(),
            b"2.5".to_vec(),
            b"c".to_vec(),
            b"3".to_vec(),
        ]
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[
                b"ZRANGE", b"z", b"+inf", b"-inf", b"BYSCORE", b"REV", b"LIMIT", b"0", b"3",
            ],
        )),
        vec![b"c".to_vec(), b"d".to_vec(), b"b".to_vec()]
    );
    let (cursor, zscan) = decode_scan_response(&RespTestHarness::exec_resp(
        &store,
        &[b"ZSCAN", b"z", b"0", b"MATCH", b"*", b"COUNT", b"1000"],
    ));
    assert_eq!(cursor, 0);
    assert_eq!(
        zscan
            .chunks_exact(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"2.5".to_vec()),
        ])
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZCOUNT", b"z", b"1", b"2"]),
        2
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZCOUNT", b"z", b"(1", b"2"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZCOUNT", b"z", b"-inf", b"+inf"]),
        4
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZRANK", b"z", b"b"]),
        1
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZREVRANK", b"z", b"b"]),
        2
    );

    assert_eq!(
        store.zadd(b"zlex", 0.0, b"a"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"zlex", 0.0, b"b"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(b"zlex", 0.0, b"c"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZRANGEBYLEX", b"zlex", b"-", b"+"],
        )),
        vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
    );
    assert_eq!(
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"ZREVRANGEBYLEX", b"zlex", b"[c", b"[a"],
        )),
        vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"ZLEXCOUNT", b"zlex", b"(a", b"[c"]),
        2
    );
}

#[test]
#[cfg(feature = "redis")]
fn scnp_resp_scan_and_shard_scan_return_resp_bytes() {
    let store = EmbeddedStore::new(4);
    store.set(b"k".to_vec(), b"v".to_vec(), None);

    let response = RespTestHarness::exec_scnp_resp(
        &store,
        vec![
            b"SCAN".as_slice(),
            b"0".as_slice(),
            b"COUNT".as_slice(),
            b"10".as_slice(),
        ],
    );
    let FastResponse::Value(raw) = response else {
        panic!("SCNP SCAN should return RESP bytes");
    };
    let (_cursor, keys) = decode_scan_response(&raw);
    assert_eq!(BTreeSet::from_iter(keys), BTreeSet::from([b"k".to_vec()]));

    let route = store.route_key(b"k");
    let shard_id = route.shard_id.to_string();
    let response = RespTestHarness::exec_scnp_resp(
        &store,
        vec![
            b"SCNP.SCANSHARD".as_slice(),
            shard_id.as_bytes(),
            b"0".as_slice(),
            b"COUNT".as_slice(),
            b"10".as_slice(),
        ],
    );
    let FastResponse::Value(raw) = response else {
        panic!("SCNP.SCANSHARD should return RESP bytes");
    };
    let (_cursor, keys) = decode_scan_response(&raw);
    assert_eq!(BTreeSet::from_iter(keys), BTreeSet::from([b"k".to_vec()]));

    let mut frame = Vec::new();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: None,
            route_shard: None,
            key_tag: None,
            command: FastCommand::RespCommand {
                parts: vec![
                    b"SCNP.SCANSHARD".as_slice(),
                    shard_id.as_bytes(),
                    b"0".as_slice(),
                    b"COUNT".as_slice(),
                    b"10".as_slice(),
                ],
            },
        },
        &mut frame,
    );
    let mut out = BytesMut::new();
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(route.shard_id),
        Instant::now(),
    )
    .expect("direct shard scan should process");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    let FastResponse::Value(raw) = response else {
        panic!("direct shard SCNP.SCANSHARD should return RESP bytes");
    };
    let (_cursor, keys) = decode_scan_response(&raw);
    assert_eq!(BTreeSet::from_iter(keys), BTreeSet::from([b"k".to_vec()]));
}

#[test]
fn scnp_owned_shard_fast_path_handles_tagged_get_set_del() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let key = b"owned-shard-key-1".as_slice();
    let value = b"value-1".as_slice();
    let route_hash = hash_key(key);
    let route_shard = stripe_index(route_hash, shift_for(store.shard_count()));
    let key_tag = hash_key_tag(key);

    let mut frame = Vec::new();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route_hash),
            route_shard: Some(route_shard as u32),
            key_tag: Some(key_tag),
            command: FastCommand::Set { key, value },
        },
        &mut frame,
    );
    let mut out = BytesMut::new();
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(route_shard),
        Instant::now(),
    )
    .expect("SET should process");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    assert_eq!(response, FastResponse::Ok);

    frame.clear();
    out.clear();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route_hash),
            route_shard: Some(route_shard as u32),
            key_tag: Some(key_tag),
            command: FastCommand::Get { key },
        },
        &mut frame,
    );
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(route_shard),
        Instant::now(),
    )
    .expect("GET should process");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    assert_eq!(response, FastResponse::Value(value.to_vec()));

    frame.clear();
    out.clear();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route_hash),
            route_shard: Some(route_shard as u32),
            key_tag: Some(key_tag),
            command: FastCommand::Exists { key },
        },
        &mut frame,
    );
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(route_shard),
        Instant::now(),
    )
    .expect("EXISTS should process");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    assert_eq!(response, FastResponse::Integer(1));

    frame.clear();
    out.clear();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route_hash),
            route_shard: Some(route_shard as u32),
            key_tag: Some(key_tag),
            command: FastCommand::Ttl { key },
        },
        &mut frame,
    );
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(route_shard),
        Instant::now(),
    )
    .expect("TTL should process");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    assert_eq!(response, FastResponse::Integer(-1));

    frame.clear();
    out.clear();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route_hash),
            route_shard: Some(route_shard as u32),
            key_tag: Some(key_tag),
            command: FastCommand::Expire {
                key,
                ttl_ms: 60_000,
            },
        },
        &mut frame,
    );
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(route_shard),
        Instant::now(),
    )
    .expect("EXPIRE should process");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    assert_eq!(response, FastResponse::Integer(1));

    frame.clear();
    out.clear();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route_hash),
            route_shard: Some(route_shard as u32),
            key_tag: Some(key_tag),
            command: FastCommand::GetEx {
                key,
                ttl_ms: 60_000,
            },
        },
        &mut frame,
    );
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(route_shard),
        Instant::now(),
    )
    .expect("GETEX should process");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    assert_eq!(response, FastResponse::Value(value.to_vec()));

    frame.clear();
    out.clear();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route_hash),
            route_shard: Some(route_shard as u32),
            key_tag: Some(key_tag),
            command: FastCommand::SetEx {
                key,
                value: b"value-2",
                ttl_ms: 60_000,
            },
        },
        &mut frame,
    );
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(route_shard),
        Instant::now(),
    )
    .expect("SETEX should process");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    assert_eq!(response, FastResponse::Ok);

    frame.clear();
    out.clear();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route_hash),
            route_shard: Some(route_shard as u32),
            key_tag: Some(key_tag),
            command: FastCommand::Delete { key },
        },
        &mut frame,
    );
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(route_shard),
        Instant::now(),
    )
    .expect("DEL should process");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    assert_eq!(response, FastResponse::Integer(1));
    assert!(store.get(key).is_none());
}

#[test]
#[cfg(feature = "redis")]
fn scnp_owned_shard_port_accepts_typed_object_opcodes() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let owned_shard = 2;
    let key = key_for_shard(&store, owned_shard);
    let route = store.route_key(&key);

    let mut frame = Vec::new();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route.key_hash),
            route_shard: Some(route.shard_id as u32),
            key_tag: Some(hash_key_tag(&key)),
            command: FastCommand::HSet {
                key: key.as_slice(),
                field: b"field",
                value: b"value",
            },
        },
        &mut frame,
    );
    let mut out = BytesMut::new();
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(owned_shard),
        Instant::now(),
    )
    .expect("typed HSET should process on owned shard");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    let FastResponse::Value(raw) = response else {
        panic!("typed HSET should return RESP bytes, got {response:?}");
    };
    assert_eq!(decode_resp_stream(&raw), vec![Frame::Integer(1)]);

    frame.clear();
    out.clear();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route.key_hash),
            route_shard: Some(route.shard_id as u32),
            key_tag: Some(hash_key_tag(&key)),
            command: FastCommand::HGet {
                key: key.as_slice(),
                field: b"field",
            },
        },
        &mut frame,
    );
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(owned_shard),
        Instant::now(),
    )
    .expect("typed HGET should process on owned shard");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    let FastResponse::Value(raw) = response else {
        panic!("typed HGET should return RESP bytes, got {response:?}");
    };
    assert_eq!(
        decode_resp_stream(&raw),
        vec![Frame::BlobString(b"value".to_vec())]
    );
}

#[test]
fn resp_owned_shard_port_accepts_routed_redis_commands() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let owned_shard = 2;
    let key = key_for_shard(&store, owned_shard);
    let responses = RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[
            &[b"SET", key.as_slice(), b"value"],
            &[b"GET", key.as_slice()],
        ],
        TransactionMode::Disabled,
        Some(owned_shard),
    );

    assert_eq!(
        responses,
        vec![
            Frame::SimpleString("OK".to_string()),
            Frame::BlobString(b"value".to_vec())
        ]
    );
}

#[test]
#[cfg(feature = "redis")]
fn resp_owned_shard_port_handles_dump_restore_fast_path() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let owned_shard = 2;
    let key = key_for_shard(&store, owned_shard);
    let responses = RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[
            &[b"SET", key.as_slice(), b"value"],
            &[b"DUMP", key.as_slice()],
        ],
        TransactionMode::Disabled,
        Some(owned_shard),
    );

    assert_eq!(responses[0], Frame::SimpleString("OK".to_string()));
    let Frame::BlobString(payload) = responses[1].clone() else {
        panic!("DUMP should return a payload");
    };

    let responses = RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[&[
            b"RESTORE",
            key.as_slice(),
            b"0",
            payload.as_slice(),
            b"REPLACE",
        ]],
        TransactionMode::Disabled,
        Some(owned_shard),
    );
    assert_eq!(responses, vec![Frame::SimpleString("OK".to_string())]);

    let responses = RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[&[b"GET", key.as_slice()]],
        TransactionMode::Disabled,
        Some(owned_shard),
    );
    assert_eq!(responses, vec![Frame::BlobString(b"value".to_vec())]);
}

#[test]
#[cfg(feature = "redis")]
fn scnp_owned_shard_port_accepts_opcode_redis_commands() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let owned_shard = 2;
    let key = key_for_shard(&store, owned_shard);
    RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[&[b"SET", key.as_slice(), b"value"]],
        TransactionMode::Disabled,
        Some(owned_shard),
    );

    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::Dump,
        vec![key.as_slice()],
    );
    let FastResponse::Value(raw) = response else {
        panic!("DUMP opcode should return RESP bytes");
    };
    let payload = decode_optional_bulk(&raw).expect("DUMP payload");

    RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[&[b"SET", key.as_slice(), b"changed"]],
        TransactionMode::Disabled,
        Some(owned_shard),
    );
    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::Restore,
        vec![
            key.as_slice(),
            b"0".as_slice(),
            payload.as_slice(),
            b"REPLACE".as_slice(),
        ],
    );
    let FastResponse::Value(raw) = response else {
        panic!("RESTORE opcode should return RESP bytes");
    };
    assert_eq!(
        decode_resp_stream(&raw),
        vec![Frame::SimpleString("OK".to_string())]
    );

    let responses = RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[&[b"GET", key.as_slice()]],
        TransactionMode::Disabled,
        Some(owned_shard),
    );
    assert_eq!(responses, vec![Frame::BlobString(b"value".to_vec())]);
}

#[test]
#[cfg(feature = "redis")]
fn scnp_redis_opcode_hot_arrays_use_fast_array_responses() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let owned_shard = 2;
    let key = key_for_shard(&store, owned_shard);
    let same_shard_key = (0..10_000)
        .map(|index| format!("array-key-{owned_shard}-{index}").into_bytes())
        .find(|candidate| candidate != &key && store.route_key(candidate).shard_id == owned_shard)
        .expect("same-shard key");

    RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[
            &[b"SET", key.as_slice(), b"value"],
            &[b"SET", same_shard_key.as_slice(), b"other"],
        ],
        TransactionMode::Disabled,
        Some(owned_shard),
    );
    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::MGet,
        vec![key.as_slice(), same_shard_key.as_slice()],
    );
    assert_eq!(
        response,
        FastResponse::Array(vec![Some(b"value".to_vec()), Some(b"other".to_vec())])
    );

    let list_key = (0..10_000)
        .map(|index| format!("array-list-{owned_shard}-{index}").into_bytes())
        .find(|candidate| store.route_key(candidate).shard_id == owned_shard)
        .expect("same-shard list key");
    assert_eq!(
        store.rpush(&list_key, &[b"a".as_slice(), b"b".as_slice()]),
        RedisObjectResult::Integer(2)
    );
    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::LRange,
        vec![list_key.as_slice(), b"0".as_slice(), b"-1".as_slice()],
    );
    assert_eq!(
        response,
        FastResponse::Array(vec![Some(b"a".to_vec()), Some(b"b".to_vec())])
    );

    let hash_key = (0..10_000)
        .map(|index| format!("array-hash-{owned_shard}-{index}").into_bytes())
        .find(|candidate| store.route_key(candidate).shard_id == owned_shard)
        .expect("same-shard hash key");
    RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[&[b"HSET", hash_key.as_slice(), b"f1", b"v1", b"f2", b"v2"]],
        TransactionMode::Disabled,
        Some(owned_shard),
    );
    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::HGetAll,
        vec![hash_key.as_slice()],
    );
    let FastResponse::Array(values) = response else {
        panic!("HGETALL should use SCNP array status");
    };
    assert_eq!(
        values
            .chunks_exact(2)
            .map(|pair| (pair[0].clone().unwrap(), pair[1].clone().unwrap()))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (b"f1".to_vec(), b"v1".to_vec()),
            (b"f2".to_vec(), b"v2".to_vec())
        ])
    );

    let zset_key = (0..10_000)
        .map(|index| format!("array-zset-{owned_shard}-{index}").into_bytes())
        .find(|candidate| store.route_key(candidate).shard_id == owned_shard)
        .expect("same-shard zset key");
    assert_eq!(
        store.zadd(&zset_key, 1.0, b"one"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.zadd(&zset_key, 2.0, b"two"),
        RedisObjectResult::Integer(1)
    );
    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::ZRange,
        vec![
            zset_key.as_slice(),
            b"0".as_slice(),
            b"-1".as_slice(),
            b"WITHSCORES".as_slice(),
        ],
    );
    assert_eq!(
        response,
        FastResponse::Array(vec![
            Some(b"one".to_vec()),
            Some(b"1".to_vec()),
            Some(b"two".to_vec()),
            Some(b"2".to_vec())
        ])
    );

    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::ZRangeByScore,
        vec![
            zset_key.as_slice(),
            b"-inf".as_slice(),
            b"+inf".as_slice(),
            b"WITHSCORES".as_slice(),
        ],
    );
    assert_eq!(
        response,
        FastResponse::Array(vec![
            Some(b"one".to_vec()),
            Some(b"1".to_vec()),
            Some(b"two".to_vec()),
            Some(b"2".to_vec())
        ])
    );

    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::ZRevRangeByScore,
        vec![
            zset_key.as_slice(),
            b"+inf".as_slice(),
            b"-inf".as_slice(),
            b"WITHSCORES".as_slice(),
        ],
    );
    assert_eq!(
        response,
        FastResponse::Array(vec![
            Some(b"two".to_vec()),
            Some(b"2".to_vec()),
            Some(b"one".to_vec()),
            Some(b"1".to_vec())
        ])
    );
}

#[test]
#[cfg(feature = "redis")]
fn scnp_redis_opcode_command_uses_cached_fast_responses() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let owned_shard = 0;

    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::Command,
        vec![b"COUNT".as_slice()],
    );
    let FastResponse::Integer(count) = response else {
        panic!("COMMAND COUNT should use SCNP integer status");
    };
    assert!(count > 0);

    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::Command,
        vec![b"LIST".as_slice()],
    );
    let FastResponse::Array(commands) = response else {
        panic!("COMMAND LIST should use SCNP array status");
    };
    assert!(commands.iter().any(|command| {
        command
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case(b"GET"))
    }));

    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::Command,
        vec![],
    );
    let FastResponse::Value(raw) = response else {
        panic!("COMMAND should keep RESP bytes for nested command metadata");
    };
    assert!(matches!(
        decode_resp_stream(&raw).as_slice(),
        [Frame::Array(_)]
    ));
}

#[test]
#[cfg(feature = "redis")]
fn scnp_owned_shard_port_rejects_misrouted_opcode_redis_commands() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let owned_shard = 0;
    let wrong_shard = 1;
    let key = key_for_shard(&store, wrong_shard);
    let response = exec_scnp_redis_opcode_on_owned_shard(
        &store,
        owned_shard,
        FastCommandKind::Dump,
        vec![key.as_slice()],
    );

    assert_eq!(
        response,
        FastResponse::Error(b"ERR SCNP route shard mismatch".to_vec())
    );
}

#[test]
fn resp_owned_shard_port_rejects_misrouted_redis_commands() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let owned_shard = 0;
    let wrong_shard = 1;
    let key = key_for_shard(&store, wrong_shard);
    let responses = RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[&[b"GET", key.as_slice()]],
        TransactionMode::Disabled,
        Some(owned_shard),
    );

    assert_eq!(
        responses,
        vec![Frame::Error("ERR direct shard route mismatch".to_string())]
    );
}

#[test]
fn resp_owned_shard_port_rejects_all_shard_redis_commands() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let responses = RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[&[b"KEYS", b"*"]],
        TransactionMode::Disabled,
        Some(0),
    );

    assert_eq!(
        responses,
        vec![Frame::Error("ERR direct shard route mismatch".to_string())]
    );
}

#[test]
fn resp_owned_shard_port_rejects_transactions() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let responses = RespTestHarness::exec_resp_sequence_on_owned_shard(
        &store,
        &[&[b"MULTI"]],
        TransactionMode::ShardLocal,
        Some(0),
    );

    assert_eq!(
        responses,
        vec![Frame::Error(
            "ERR transactions are not supported on direct shard RESP ports".to_string()
        )]
    );
}

#[test]
fn scnp_owned_shard_rejects_mismatched_route() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let key = b"owned-shard-key-2".as_slice();
    let value = b"value-2".as_slice();
    let route_hash = hash_key(key);
    let route_shard = stripe_index(route_hash, shift_for(store.shard_count()));
    let wrong_shard = (route_shard + 1) % store.shard_count();

    let mut frame = Vec::new();
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(route_hash),
            route_shard: Some(wrong_shard as u32),
            key_tag: Some(hash_key_tag(key)),
            command: FastCommand::Set { key, value },
        },
        &mut frame,
    );
    let mut out = BytesMut::new();
    let consumed = DirectProtocol::process_shared_request_buffer(
        &frame,
        &store,
        &mut out,
        None,
        false,
        Some(wrong_shard),
        Instant::now(),
    )
    .expect("misrouted SET should be handled");
    assert_eq!(consumed, frame.len());
    let response = FastCodec::decode_response(&out).unwrap().unwrap().0;
    assert_eq!(
        response,
        FastResponse::Error(b"ERR SCNP route shard mismatch".to_vec())
    );
    assert!(store.get(key).is_none());
}
