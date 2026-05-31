#[cfg(feature = "redis")]
use super::commands::RawCommandDispatcher;
#[cfg(feature = "embedded")]
use super::commands::{RAW_DIRECT_CATALOG, find_primary_raw_command};
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

#[cfg(feature = "redis-modules-all")]
#[path = "tests/redis_module_semantics.rs"]
mod redis_module_semantics;

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
        let resolved = if command.name().contains('*') {
            RawCommandDispatcher::find(command.name().as_bytes())
        } else {
            find_primary_raw_command(command.name().as_bytes())
        }
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

#[cfg(feature = "redis")]
#[test]
#[ignore = "manual hot-path profiler"]
fn redis_faster_command_hot_path_profile() {
    let store = EmbeddedStore::new(4);
    RespTestHarness::exec_resp_sequence(
        &store,
        &[
            &[b"SET", b"object-key", b"value"],
            &[b"RPUSH", b"lpos-list", b"a", b"b", b"c", b"b", b"b"],
            &[b"SET", b"lcs-a", b"hello_world_foobar"],
            &[b"SET", b"lcs-b", b"hello_there_foobaz"],
        ],
        TransactionMode::Disabled,
    );

    let cases: &[(&str, &[&[u8]], usize)] = &[
        ("RESET", &[b"RESET"], 1_000_000),
        ("ACL WHOAMI", &[b"ACL", b"WHOAMI"], 1_000_000),
        ("CLIENT GETNAME", &[b"CLIENT", b"GETNAME"], 1_000_000),
        ("CLIENT ID", &[b"CLIENT", b"ID"], 1_000_000),
        (
            "OBJECT ENCODING",
            &[b"OBJECT", b"ENCODING", b"object-key"],
            1_000_000,
        ),
        (
            "LPOS COUNT",
            &[b"LPOS", b"lpos-list", b"b", b"RANK", b"1", b"COUNT", b"0"],
            500_000,
        ),
        ("LCS LEN", &[b"LCS", b"lcs-a", b"lcs-b", b"LEN"], 500_000),
    ];

    for (label, parts, iterations) in cases {
        let mut input = Vec::new();
        for _ in 0..*iterations {
            encode_resp_command(parts, &mut input);
        }

        let mut out = BytesMut::with_capacity(128 * *iterations);
        let started = Instant::now();
        let consumed = DirectProtocol::process_shared_request_buffer(
            &input,
            &store,
            &mut out,
            None,
            false,
            None,
            Instant::now(),
        )
        .expect("profile input should process");
        let elapsed = started.elapsed();
        assert_eq!(consumed, input.len());
        std::hint::black_box(out.len());
        println!(
            "profile {label}: {:.1} ns/op ({} iters, {} bytes out)",
            elapsed.as_nanos() as f64 / *iterations as f64,
            iterations,
            out.len()
        );
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

fn f32_bytes(values: &[f32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

#[test]
fn semantic_resp_commands_return_cached_match() {
    let store = EmbeddedStore::new(1);
    let stored = f32_bytes(&[1.0, 0.0]);
    let query = f32_bytes(&[0.99, 0.01]);

    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[b"SEMANTIC.SET", b"semantic:cat", b"meow", stored.as_slice()],
        ),
        b"+OK\r\n"
    );

    let response = decode_resp_stream(&RespTestHarness::exec_resp(
        &store,
        &[b"SEMANTIC.SEARCH", query.as_slice(), b"0.75"],
    ));
    match response.as_slice() {
        [Frame::Array(items)] => {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0], Frame::BlobString(b"semantic:cat".to_vec()));
            assert_eq!(items[1], Frame::BlobString(b"meow".to_vec()));
            assert!(matches!(items[2], Frame::BlobString(_)));
            assert_eq!(items[3], Frame::Null);
        }
        other => panic!("unexpected semantic search response: {other:?}"),
    }
}

#[test]
fn semantic_resp_commands_return_governance_metadata() {
    let store = EmbeddedStore::new(1);
    let stored = f32_bytes(&[0.0, 1.0]);

    assert_eq!(
        RespTestHarness::exec_resp(
            &store,
            &[
                b"SEMANTIC.SET",
                b"semantic:dog",
                b"woof",
                stored.as_slice(),
                b"tenant=acme",
            ],
        ),
        b"+OK\r\n"
    );

    let response = decode_resp_stream(&RespTestHarness::exec_resp(
        &store,
        &[b"SEMANTIC.SEARCH", stored.as_slice(), b"0.75"],
    ));
    match response.as_slice() {
        [Frame::Array(items)] => {
            assert_eq!(items[0], Frame::BlobString(b"semantic:dog".to_vec()));
            assert_eq!(items[1], Frame::BlobString(b"woof".to_vec()));
            assert_eq!(items[3], Frame::BlobString(b"tenant=acme".to_vec()));
        }
        other => panic!("unexpected governed semantic search response: {other:?}"),
    }
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
        "ACL" => commands!([b"ACL", b"WHOAMI"]),
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
        "BITFIELD_RO" => commands!(
            [b"BITFIELD", b"bf-ro", b"SET", b"u8", b"0", b"1"],
            [b"BITFIELD_RO", b"bf-ro", b"GET", b"u8", b"0"]
        ),
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
        "EVAL_RO" => commands!([b"EVAL_RO", b"return 1", b"0"]),
        "EVALSHA" => commands!(
            [b"SCRIPT", b"LOAD", b"return 1"],
            [
                b"EVALSHA",
                b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db",
                b"0"
            ]
        ),
        "EVALSHA_RO" => commands!(
            [b"SCRIPT", b"LOAD", b"return 1"],
            [
                b"EVALSHA_RO",
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
        "FAILOVER" => commands!([b"FAILOVER"]),
        "FLUSHALL" => commands!([b"SET", b"flushall-key", b"value"], [b"FLUSHALL"]),
        "FLUSHDB" => commands!([b"SET", b"flushdb-key", b"value"], [b"FLUSHDB"]),
        "FCALL" => commands!([b"FCALL", b"missing", b"0"]),
        "FCALL_RO" => commands!([b"FCALL_RO", b"missing", b"0"]),
        "FUNCTION" => commands!([b"FUNCTION", b"LIST"]),
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
        "GEOSEARCH" => commands!(
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
            [
                b"GEOSEARCH",
                b"geo",
                b"FROMMEMBER",
                b"empire",
                b"BYRADIUS",
                b"2",
                b"km"
            ]
        ),
        "GEOSEARCHSTORE" => commands!(
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
            [
                b"GEOSEARCHSTORE",
                b"geo-out",
                b"geo",
                b"FROMMEMBER",
                b"empire",
                b"BYRADIUS",
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
        "HEXPIRE" => commands!(
            [b"HSET", b"hx", b"f", b"v"],
            [b"HEXPIRE", b"hx", b"100", b"FIELDS", b"1", b"f"]
        ),
        "HTTL" => commands!(
            [b"HSET", b"hx", b"f", b"v"],
            [b"HTTL", b"hx", b"FIELDS", b"1", b"f"]
        ),
        "HPTTL" => commands!(
            [b"HSET", b"hx", b"f", b"v"],
            [b"HPTTL", b"hx", b"FIELDS", b"1", b"f"]
        ),
        "HEXPIRETIME" => commands!(
            [b"HSET", b"hx", b"f", b"v"],
            [b"HEXPIRETIME", b"hx", b"FIELDS", b"1", b"f"]
        ),
        "HPEXPIRETIME" => commands!(
            [b"HSET", b"hx", b"f", b"v"],
            [b"HPEXPIRETIME", b"hx", b"FIELDS", b"1", b"f"]
        ),
        "HPERSIST" => commands!(
            [b"HSET", b"hx", b"f", b"v"],
            [b"HPERSIST", b"hx", b"FIELDS", b"1", b"f"]
        ),
        "HPEXPIRE" => commands!(
            [b"HSET", b"hx", b"f", b"v"],
            [b"HPEXPIRE", b"hx", b"100000", b"FIELDS", b"1", b"f"]
        ),
        "HEXPIREAT" => commands!(
            [b"HSET", b"hx", b"f", b"v"],
            [b"HEXPIREAT", b"hx", b"99999999999", b"FIELDS", b"1", b"f"]
        ),
        "HPEXPIREAT" => commands!(
            [b"HSET", b"hx", b"f", b"v"],
            [
                b"HPEXPIREAT",
                b"hx",
                b"99999999999000",
                b"FIELDS",
                b"1",
                b"f"
            ]
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
        "LCS" => commands!(
            [b"SET", b"lcs-a", b"ohmytext"],
            [b"SET", b"lcs-b", b"mynewtext"],
            [b"LCS", b"lcs-a", b"lcs-b", b"LEN"]
        ),
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
        "LPOS" => commands!(
            [b"RPUSH", b"lpos-list", b"a", b"b", b"c", b"b"],
            [b"LPOS", b"lpos-list", b"b"]
        ),
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
        "RESET" => commands!([b"RESET"]),
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
        "SINTERCARD" => commands!(
            [b"SADD", b"set-a", b"a"],
            [b"SADD", b"set-b", b"a"],
            [b"SINTERCARD", b"2", b"set-a", b"set-b"]
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
        "SORT_RO" => commands!(
            [b"RPUSH", b"sortro-list", b"3", b"1", b"2"],
            [b"SORT_RO", b"sortro-list"]
        ),
        "SPOP" => commands!([b"SADD", b"set", b"a"], [b"SPOP", b"set"]),
        "SRANDMEMBER" => commands!([b"SADD", b"set", b"a"], [b"SRANDMEMBER", b"set"]),
        "SREM" => commands!([b"SADD", b"set", b"a"], [b"SREM", b"set", b"a"]),
        "SSCAN" => commands!([b"SADD", b"set", b"a"], [b"SSCAN", b"set", b"0"]),
        "STRALGO" => commands!(
            [b"SET", b"stralgo-a", b"ohmytext"],
            [b"SET", b"stralgo-b", b"mynewtext"],
            [b"STRALGO", b"LCS", b"stralgo-a", b"stralgo-b", b"LEN"]
        ),
        "STRLEN" => commands!(
            [b"SET", b"strlen-key", b"value"],
            [b"STRLEN", b"strlen-key"]
        ),
        "SPUBLISH" => commands!([b"SPUBLISH", b"shard-channel", b"message"]),
        "SUBSCRIBE" => commands!([b"SUBSCRIBE", b"channel"]),
        "SSUBSCRIBE" => commands!([b"SSUBSCRIBE", b"shard-channel"]),
        "SUBSTR" => commands!(
            [b"SET", b"substr-key", b"value"],
            [b"SUBSTR", b"substr-key", b"0", b"2"]
        ),
        "SUNSUBSCRIBE" => commands!([b"SUNSUBSCRIBE", b"shard-channel"]),
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
        "WAITAOF" => commands!([b"WAITAOF", b"0", b"0", b"0"]),
        "WATCH" => commands!([b"WATCH", b"watched"]),
        "XACK" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XGROUP", b"CREATE", b"stream", b"group", b"0-0"],
            [b"XACK", b"stream", b"group", b"1-0"]
        ),
        "XADD" => commands!([b"XADD", b"stream", b"1-0", b"f", b"v"]),
        "XAUTOCLAIM" => commands!(
            [b"XADD", b"stream", b"1-0", b"f", b"v"],
            [b"XGROUP", b"CREATE", b"stream", b"group", b"0-0"],
            [
                b"XAUTOCLAIM",
                b"stream",
                b"group",
                b"consumer",
                b"0",
                b"0-0"
            ]
        ),
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
        _ => {
            #[cfg(feature = "redis-modules")]
            {
                resp2_redis_module_smoke_commands(name)
            }
            #[cfg(not(feature = "redis-modules"))]
            {
                None
            }
        }
    }
}

#[cfg(feature = "redis-modules")]
fn resp2_redis_module_smoke_commands(name: &str) -> Option<Vec<Vec<Vec<u8>>>> {
    macro_rules! module_commands {
        ($([$($part:expr),+ $(,)?]),+ $(,)?) => {
            vec![$(vec![$(resp2_part($part)),+]),+]
        };
    }

    crate::commands::redis_modules::module_family_for_command(name.as_bytes())?;
    let commands = match name {
        "AI._MODELSCAN" => module_commands!(
            [
                b"AI.MODELSET",
                b"model",
                b"TF",
                b"CPU",
                b"BLOB",
                b"model-bytes"
            ],
            [b"AI._MODELSCAN"]
        ),
        "AI._SCRIPTSCAN" => module_commands!(
            [b"AI.SCRIPTSET", b"script", b"CPU", b"SOURCE", b"return 1"],
            [b"AI._SCRIPTSCAN"]
        ),
        "AI.CONFIG" => module_commands!([b"AI.CONFIG", b"GET", b"BACKENDS"]),
        "AI.DAGEXECUTE" => module_commands!([b"AI.DAGEXECUTE"]),
        "AI.DAGRUN" => module_commands!([b"AI.DAGRUN"]),
        "AI.DAGRUN_RO" => module_commands!([b"AI.DAGRUN_RO"]),
        "AI.INFO" => module_commands!(
            [b"AI.TENSORSET", b"tensor", b"FLOAT", b"1", b"VALUES", b"1"],
            [b"AI.INFO", b"tensor"]
        ),
        "AI.MODELDEL" => module_commands!(
            [
                b"AI.MODELSET",
                b"model",
                b"TF",
                b"CPU",
                b"BLOB",
                b"model-bytes"
            ],
            [b"AI.MODELDEL", b"model"]
        ),
        "AI.MODELEXECUTE" => module_commands!([
            b"AI.MODELEXECUTE",
            b"model",
            b"INPUTS",
            b"tensor",
            b"OUTPUTS",
            b"out"
        ]),
        "AI.MODELGET" => module_commands!(
            [
                b"AI.MODELSET",
                b"model",
                b"TF",
                b"CPU",
                b"BLOB",
                b"model-bytes"
            ],
            [b"AI.MODELGET", b"model"]
        ),
        "AI.MODELRUN" => module_commands!([
            b"AI.MODELRUN",
            b"model",
            b"INPUTS",
            b"tensor",
            b"OUTPUTS",
            b"out"
        ]),
        "AI.MODELSET" => {
            module_commands!([
                b"AI.MODELSET",
                b"model",
                b"TF",
                b"CPU",
                b"BLOB",
                b"model-bytes"
            ])
        }
        "AI.MODELSTORE" => {
            module_commands!([
                b"AI.MODELSTORE",
                b"model",
                b"TF",
                b"CPU",
                b"BLOB",
                b"model-bytes"
            ])
        }
        "AI.SCRIPTDEL" => module_commands!(
            [b"AI.SCRIPTSET", b"script", b"CPU", b"SOURCE", b"return 1"],
            [b"AI.SCRIPTDEL", b"script"]
        ),
        "AI.SCRIPTEXECUTE" => module_commands!([
            b"AI.SCRIPTEXECUTE",
            b"script",
            b"bar",
            b"INPUTS",
            b"tensor",
            b"OUTPUTS",
            b"out"
        ]),
        "AI.SCRIPTGET" => module_commands!(
            [b"AI.SCRIPTSET", b"script", b"CPU", b"SOURCE", b"return 1"],
            [b"AI.SCRIPTGET", b"script"]
        ),
        "AI.SCRIPTRUN" => module_commands!([
            b"AI.SCRIPTRUN",
            b"script",
            b"bar",
            b"INPUTS",
            b"tensor",
            b"OUTPUTS",
            b"out"
        ]),
        "AI.SCRIPTSET" => {
            module_commands!([b"AI.SCRIPTSET", b"script", b"CPU", b"SOURCE", b"return 1"])
        }
        "AI.SCRIPTSTORE" => {
            module_commands!([b"AI.SCRIPTSTORE", b"script", b"CPU", b"SOURCE", b"return 1"])
        }
        "AI.TENSORDEL" => module_commands!(
            [b"AI.TENSORSET", b"tensor", b"FLOAT", b"1", b"VALUES", b"1"],
            [b"AI.TENSORDEL", b"tensor"]
        ),
        "AI.TENSORGET" => module_commands!(
            [b"AI.TENSORSET", b"tensor", b"FLOAT", b"1", b"VALUES", b"1"],
            [b"AI.TENSORGET", b"tensor"]
        ),
        "AI.TENSORSET" => {
            module_commands!([b"AI.TENSORSET", b"tensor", b"FLOAT", b"1", b"VALUES", b"1"])
        }
        "BF.ADD" => module_commands!([b"BF.ADD", b"bf", b"item"]),
        "BF.CARD" => module_commands!([b"BF.ADD", b"bf", b"item"], [b"BF.CARD", b"bf"]),
        "BF.EXISTS" => {
            module_commands!([b"BF.ADD", b"bf", b"item"], [b"BF.EXISTS", b"bf", b"item"])
        }
        "BF.INFO" => module_commands!([b"BF.ADD", b"bf", b"item"], [b"BF.INFO", b"bf"]),
        "BF.INSERT" => module_commands!([b"BF.INSERT", b"bf", b"ITEMS", b"a", b"b"]),
        "BF.LOADCHUNK" => module_commands!([b"BF.LOADCHUNK", b"bf", b"1", b"chunk"]),
        "BF.MADD" => module_commands!([b"BF.MADD", b"bf", b"a", b"b"]),
        "BF.MEXISTS" => module_commands!(
            [b"BF.MADD", b"bf", b"a", b"b"],
            [b"BF.MEXISTS", b"bf", b"a", b"c"]
        ),
        "BF.RESERVE" => module_commands!([b"BF.RESERVE", b"bf", b"0.01", b"100"]),
        "BF.SCANDUMP" => {
            module_commands!([b"BF.ADD", b"bf", b"item"], [b"BF.SCANDUMP", b"bf", b"0"])
        }
        "CF.ADD" => module_commands!([b"CF.ADD", b"cf", b"item"]),
        "CF.ADDNX" => module_commands!([b"CF.ADDNX", b"cf", b"item"]),
        "CF.COUNT" => module_commands!([b"CF.ADD", b"cf", b"item"], [b"CF.COUNT", b"cf", b"item"]),
        "CF.DEL" => module_commands!([b"CF.ADD", b"cf", b"item"], [b"CF.DEL", b"cf", b"item"]),
        "CF.EXISTS" => {
            module_commands!([b"CF.ADD", b"cf", b"item"], [b"CF.EXISTS", b"cf", b"item"])
        }
        "CF.INFO" => module_commands!([b"CF.ADD", b"cf", b"item"], [b"CF.INFO", b"cf"]),
        "CF.INSERT" => module_commands!([b"CF.INSERT", b"cf", b"ITEMS", b"a", b"b"]),
        "CF.INSERTNX" => module_commands!([b"CF.INSERTNX", b"cf", b"ITEMS", b"a", b"b"]),
        "CF.LOADCHUNK" => module_commands!([b"CF.LOADCHUNK", b"cf", b"1", b"chunk"]),
        "CF.MEXISTS" => {
            module_commands!([b"CF.ADD", b"cf", b"a"], [b"CF.MEXISTS", b"cf", b"a", b"b"])
        }
        "CF.RESERVE" => module_commands!([b"CF.RESERVE", b"cf", b"100"]),
        "CF.SCANDUMP" => {
            module_commands!([b"CF.ADD", b"cf", b"item"], [b"CF.SCANDUMP", b"cf", b"0"])
        }
        "CL.THROTTLE" => module_commands!([b"CL.THROTTLE", b"cell", b"1", b"2", b"3", b"1"]),
        "CMS.INCRBY" => module_commands!(
            [b"CMS.INITBYDIM", b"cms", b"10", b"5"],
            [b"CMS.INCRBY", b"cms", b"a", b"2"]
        ),
        "CMS.INFO" => module_commands!(
            [b"CMS.INITBYDIM", b"cms", b"10", b"5"],
            [b"CMS.INFO", b"cms"]
        ),
        "CMS.INITBYDIM" => module_commands!([b"CMS.INITBYDIM", b"cms", b"10", b"5"]),
        "CMS.INITBYPROB" => module_commands!([b"CMS.INITBYPROB", b"cms", b"0.01", b"0.01"]),
        "CMS.MERGE" => module_commands!(
            [b"CMS.INITBYDIM", b"cms-src", b"10", b"5"],
            [b"CMS.INCRBY", b"cms-src", b"a", b"2"],
            [b"CMS.MERGE", b"cms-out", b"1", b"cms-src"]
        ),
        "CMS.QUERY" => module_commands!(
            [b"CMS.INITBYDIM", b"cms", b"10", b"5"],
            [b"CMS.QUERY", b"cms", b"a"]
        ),
        "FT._LIST" => module_commands!([b"FT._LIST"]),
        "FT.AGGREGATE" => module_commands!([b"FT.AGGREGATE", b"idx", b"*"]),
        "FT.ALIASADD" => module_commands!([b"FT.ALIASADD", b"alias", b"idx"]),
        "FT.ALIASDEL" => module_commands!(
            [b"FT.ALIASADD", b"alias", b"idx"],
            [b"FT.ALIASDEL", b"alias"]
        ),
        "FT.ALIASUPDATE" => module_commands!([b"FT.ALIASUPDATE", b"alias", b"idx"]),
        "FT.ALTER" => module_commands!([b"FT.ALTER", b"idx", b"SCHEMA", b"body", b"TEXT"]),
        "FT.CONFIG" => module_commands!([b"FT.CONFIG", b"GET", b"TIMEOUT"]),
        "FT.CREATE" => module_commands!([b"FT.CREATE", b"idx", b"SCHEMA", b"title", b"TEXT"]),
        "FT.CURSOR" => module_commands!([b"FT.CURSOR", b"READ", b"idx", b"0"]),
        "FT.DICTADD" => module_commands!([b"FT.DICTADD", b"dict", b"term"]),
        "FT.DICTDEL" => module_commands!(
            [b"FT.DICTADD", b"dict", b"term"],
            [b"FT.DICTDEL", b"dict", b"term"]
        ),
        "FT.DICTDUMP" => {
            module_commands!([b"FT.DICTADD", b"dict", b"term"], [b"FT.DICTDUMP", b"dict"])
        }
        "FT.DROPINDEX" => module_commands!(
            [b"FT.CREATE", b"idx", b"SCHEMA", b"title", b"TEXT"],
            [b"FT.DROPINDEX", b"idx"]
        ),
        "FT.EXPLAIN" => module_commands!([b"FT.EXPLAIN", b"idx", b"*"]),
        "FT.EXPLAINCLI" => module_commands!([b"FT.EXPLAINCLI", b"idx", b"*"]),
        "FT.HYBRID" => module_commands!([b"FT.HYBRID", b"idx", b"SEARCH", b"*"]),
        "FT.INFO" => module_commands!(
            [b"FT.CREATE", b"idx", b"SCHEMA", b"title", b"TEXT"],
            [b"FT.INFO", b"idx"]
        ),
        "FT.PROFILE" => module_commands!([b"FT.PROFILE", b"idx", b"SEARCH", b"QUERY", b"*"]),
        "FT.SEARCH" => module_commands!([b"FT.SEARCH", b"idx", b"*"]),
        "FT.SPELLCHECK" => module_commands!([b"FT.SPELLCHECK", b"idx", b"term"]),
        "FT.SUGADD" => module_commands!([b"FT.SUGADD", b"sug", b"hello", b"1"]),
        "FT.SUGDEL" => module_commands!(
            [b"FT.SUGADD", b"sug", b"hello", b"1"],
            [b"FT.SUGDEL", b"sug", b"hello"]
        ),
        "FT.SUGGET" => module_commands!(
            [b"FT.SUGADD", b"sug", b"hello", b"1"],
            [b"FT.SUGGET", b"sug", b"he"]
        ),
        "FT.SUGLEN" => module_commands!(
            [b"FT.SUGADD", b"sug", b"hello", b"1"],
            [b"FT.SUGLEN", b"sug"]
        ),
        "FT.SYNDUMP" => module_commands!(
            [b"FT.SYNUPDATE", b"idx", b"group", b"term"],
            [b"FT.SYNDUMP", b"idx"]
        ),
        "FT.SYNUPDATE" => module_commands!([b"FT.SYNUPDATE", b"idx", b"group", b"term"]),
        "FT.TAGVALS" => module_commands!([b"FT.TAGVALS", b"idx", b"tag"]),
        "GRAPH.CONFIG" => module_commands!([b"GRAPH.CONFIG", b"GET", b"timeout"]),
        "GRAPH.DELETE" => module_commands!(
            [b"GRAPH.QUERY", b"graph", b"RETURN 1"],
            [b"GRAPH.DELETE", b"graph"]
        ),
        "GRAPH.EXPLAIN" => module_commands!([b"GRAPH.EXPLAIN", b"graph", b"RETURN 1"]),
        "GRAPH.LIST" => module_commands!([b"GRAPH.LIST"]),
        "GRAPH.PROFILE" => module_commands!([b"GRAPH.PROFILE", b"graph", b"RETURN 1"]),
        "GRAPH.QUERY" => module_commands!([b"GRAPH.QUERY", b"graph", b"RETURN 1"]),
        "GRAPH.RO_QUERY" => module_commands!([b"GRAPH.RO_QUERY", b"graph", b"RETURN 1"]),
        "GRAPH.SLOWLOG" => module_commands!([b"GRAPH.SLOWLOG", b"graph"]),
        "JS.DEL" => module_commands!([b"JS.EVAL", b"1 + 1"], [b"JS.DEL", b"script"]),
        "JS.EVAL" => module_commands!([b"JS.EVAL", b"1 + 1"]),
        "JS.GET" => module_commands!([b"JS.GET", b"script"]),
        "JSON.ARRAPPEND" => module_commands!(
            [b"JSON.SET", b"arr", b"$", b"[1]"],
            [b"JSON.ARRAPPEND", b"arr", b"$", b"2"]
        ),
        "JSON.ARRINDEX" => module_commands!(
            [b"JSON.SET", b"arr", b"$", b"[1,2]"],
            [b"JSON.ARRINDEX", b"arr", b"$", b"2"]
        ),
        "JSON.ARRINSERT" => module_commands!(
            [b"JSON.SET", b"arr", b"$", b"[1,3]"],
            [b"JSON.ARRINSERT", b"arr", b"$", b"1", b"2"]
        ),
        "JSON.ARRLEN" => module_commands!(
            [b"JSON.SET", b"arr", b"$", b"[1,2]"],
            [b"JSON.ARRLEN", b"arr", b"$"]
        ),
        "JSON.ARRPOP" => module_commands!(
            [b"JSON.SET", b"arr", b"$", b"[1,2]"],
            [b"JSON.ARRPOP", b"arr", b"$"]
        ),
        "JSON.ARRTRIM" => module_commands!(
            [b"JSON.SET", b"arr", b"$", b"[1,2,3]"],
            [b"JSON.ARRTRIM", b"arr", b"$", b"0", b"1"]
        ),
        "JSON.CLEAR" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.CLEAR", b"doc", b"$"]
        ),
        "JSON.DEBUG" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.DEBUG", b"MEMORY", b"doc", b"$"]
        ),
        "JSON.DEL" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.DEL", b"doc", b"$"]
        ),
        "JSON.FORGET" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.FORGET", b"doc", b"$"]
        ),
        "JSON.GET" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.GET", b"doc", b"$"]
        ),
        "JSON.MERGE" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.MERGE", b"doc", b"$", br#"{"b":2}"#]
        ),
        "JSON.MGET" => module_commands!(
            [b"JSON.SET", b"doc1", b"$", br#"{"a":1}"#],
            [b"JSON.SET", b"doc2", b"$", br#"{"a":2}"#],
            [b"JSON.MGET", b"doc1", b"doc2", b"$"]
        ),
        "JSON.MSET" => module_commands!([
            b"JSON.MSET",
            b"doc1",
            b"$",
            br#"{"a":1}"#,
            b"doc2",
            b"$",
            br#"{"a":2}"#
        ]),
        "JSON.NUMINCRBY" => module_commands!(
            [b"JSON.SET", b"num", b"$", b"2"],
            [b"JSON.NUMINCRBY", b"num", b"$", b"3"]
        ),
        "JSON.NUMMULTBY" => module_commands!(
            [b"JSON.SET", b"num", b"$", b"2"],
            [b"JSON.NUMMULTBY", b"num", b"$", b"3"]
        ),
        "JSON.OBJKEYS" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.OBJKEYS", b"doc", b"$"]
        ),
        "JSON.OBJLEN" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.OBJLEN", b"doc", b"$"]
        ),
        "JSON.RESP" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.RESP", b"doc", b"$"]
        ),
        "JSON.SET" => module_commands!([b"JSON.SET", b"doc", b"$", br#"{"a":1}"#]),
        "JSON.STRAPPEND" => module_commands!(
            [b"JSON.SET", b"str", b"$", br#""a""#],
            [b"JSON.STRAPPEND", b"str", b"$", br#""b""#]
        ),
        "JSON.STRLEN" => module_commands!(
            [b"JSON.SET", b"str", b"$", br#""a""#],
            [b"JSON.STRLEN", b"str", b"$"]
        ),
        "JSON.TOGGLE" => module_commands!(
            [b"JSON.SET", b"bool", b"$", b"true"],
            [b"JSON.TOGGLE", b"bool", b"$"]
        ),
        "JSON.TYPE" => module_commands!(
            [b"JSON.SET", b"doc", b"$", br#"{"a":1}"#],
            [b"JSON.TYPE", b"doc", b"$"]
        ),
        "NR.CREATE" => module_commands!([b"NR.CREATE", b"net"]),
        "NR.DELETE" => module_commands!([b"NR.CREATE", b"net"], [b"NR.DELETE", b"net"]),
        "NR.INFO" => module_commands!([b"NR.CREATE", b"net"], [b"NR.INFO", b"net"]),
        "NR.OBSERVE" => module_commands!([b"NR.CREATE", b"net"], [b"NR.OBSERVE", b"net", b"1"]),
        "NR.RUN" => module_commands!([b"NR.RUN", b"net", b"1"]),
        "NR.TRAIN" => module_commands!([b"NR.CREATE", b"net"], [b"NR.TRAIN", b"net"]),
        "R.APPENDINTARRAY" => module_commands!([b"R.APPENDINTARRAY", b"rb", b"1", b"2"]),
        "R.BITCOUNT" => module_commands!([b"R.SETBIT", b"rb", b"7", b"1"], [b"R.BITCOUNT", b"rb"]),
        "R.BITOP" => module_commands!(
            [b"R.SETBIT", b"ra", b"1", b"1"],
            [b"R.SETBIT", b"rb", b"2", b"1"],
            [b"R.BITOP", b"OR", b"rout", b"ra", b"rb"]
        ),
        "R.BITPOS" => {
            module_commands!([b"R.SETBIT", b"rb", b"7", b"1"], [b"R.BITPOS", b"rb", b"1"])
        }
        "R.CLEAR" => module_commands!([b"R.SETBIT", b"rb", b"7", b"1"], [b"R.CLEAR", b"rb"]),
        "R.CLEARBITS" => {
            module_commands!(
                [b"R.SETBIT", b"rb", b"7", b"1"],
                [b"R.CLEARBITS", b"rb", b"7"]
            )
        }
        "R.CONTAINS" => module_commands!(
            [b"R.SETBIT", b"rb", b"7", b"1"],
            [b"R.CONTAINS", b"rb", b"7"]
        ),
        "R.DELETEINTARRAY" => module_commands!(
            [b"R.SETINTARRAY", b"rb", b"7"],
            [b"R.DELETEINTARRAY", b"rb", b"7"]
        ),
        "R.DIFF" => module_commands!(
            [b"R.SETBIT", b"ra", b"1", b"1"],
            [b"R.SETBIT", b"rb", b"2", b"1"],
            [b"R.DIFF", b"ra", b"rb"]
        ),
        "R.GETBIT" => {
            module_commands!([b"R.SETBIT", b"rb", b"7", b"1"], [b"R.GETBIT", b"rb", b"7"])
        }
        "R.GETBITARRAY" => {
            module_commands!([b"R.SETBIT", b"rb", b"7", b"1"], [b"R.GETBITARRAY", b"rb"])
        }
        "R.GETBITS" => module_commands!(
            [b"R.SETBIT", b"rb", b"7", b"1"],
            [b"R.GETBITS", b"rb", b"7"]
        ),
        "R.GETINTARRAY" => {
            module_commands!([b"R.SETBIT", b"rb", b"7", b"1"], [b"R.GETINTARRAY", b"rb"])
        }
        "R.JACCARD" => module_commands!(
            [b"R.SETBIT", b"ra", b"1", b"1"],
            [b"R.SETBIT", b"rb", b"1", b"1"],
            [b"R.JACCARD", b"ra", b"rb"]
        ),
        "R.MAX" => module_commands!([b"R.SETBIT", b"rb", b"7", b"1"], [b"R.MAX", b"rb"]),
        "R.MIN" => module_commands!([b"R.SETBIT", b"rb", b"7", b"1"], [b"R.MIN", b"rb"]),
        "R.OPTIMIZE" => module_commands!([b"R.OPTIMIZE", b"rb"]),
        "R.RANGEINTARRAY" => module_commands!(
            [b"R.SETINTARRAY", b"rb", b"1", b"3", b"5"],
            [b"R.RANGEINTARRAY", b"rb", b"1", b"4"]
        ),
        "R.SETBIT" => module_commands!([b"R.SETBIT", b"rb", b"7", b"1"]),
        "R.SETBITARRAY" => module_commands!([b"R.SETBITARRAY", b"rb", b"1", b"2"]),
        "R.SETFULL" => module_commands!([b"R.SETFULL", b"rb"]),
        "R.SETINTARRAY" => module_commands!([b"R.SETINTARRAY", b"rb", b"1", b"2"]),
        "R.SETRANGE" => module_commands!([b"R.SETRANGE", b"rb", b"1", b"3"]),
        "R.STAT" => module_commands!([b"R.SETBIT", b"rb", b"7", b"1"], [b"R.STAT", b"rb"]),
        "R64.APPENDINTARRAY" => module_commands!([b"R64.APPENDINTARRAY", b"rb64", b"1", b"2"]),
        "R64.BITCOUNT" => {
            module_commands!(
                [b"R64.SETBIT", b"rb64", b"7", b"1"],
                [b"R64.BITCOUNT", b"rb64"]
            )
        }
        "R64.BITOP" => module_commands!(
            [b"R64.SETBIT", b"ra64", b"1", b"1"],
            [b"R64.SETBIT", b"rb64", b"2", b"1"],
            [b"R64.BITOP", b"OR", b"rout64", b"ra64", b"rb64"]
        ),
        "R64.BITPOS" => {
            module_commands!(
                [b"R64.SETBIT", b"rb64", b"7", b"1"],
                [b"R64.BITPOS", b"rb64", b"1"]
            )
        }
        "R64.CLEAR" => {
            module_commands!(
                [b"R64.SETBIT", b"rb64", b"7", b"1"],
                [b"R64.CLEAR", b"rb64"]
            )
        }
        "R64.CLEARBITS" => {
            module_commands!(
                [b"R64.SETBIT", b"rb64", b"7", b"1"],
                [b"R64.CLEARBITS", b"rb64", b"7"]
            )
        }
        "R64.CONTAINS" => {
            module_commands!(
                [b"R64.SETBIT", b"rb64", b"7", b"1"],
                [b"R64.CONTAINS", b"rb64", b"7"]
            )
        }
        "R64.DELETEINTARRAY" => module_commands!(
            [b"R64.SETINTARRAY", b"rb64", b"7"],
            [b"R64.DELETEINTARRAY", b"rb64", b"7"]
        ),
        "R64.DIFF" => module_commands!(
            [b"R64.SETBIT", b"ra64", b"1", b"1"],
            [b"R64.SETBIT", b"rb64", b"2", b"1"],
            [b"R64.DIFF", b"ra64", b"rb64"]
        ),
        "R64.GETBIT" => {
            module_commands!(
                [b"R64.SETBIT", b"rb64", b"7", b"1"],
                [b"R64.GETBIT", b"rb64", b"7"]
            )
        }
        "R64.GETBITARRAY" => {
            module_commands!(
                [b"R64.SETBIT", b"rb64", b"7", b"1"],
                [b"R64.GETBITARRAY", b"rb64"]
            )
        }
        "R64.GETBITS" => {
            module_commands!(
                [b"R64.SETBIT", b"rb64", b"7", b"1"],
                [b"R64.GETBITS", b"rb64", b"7"]
            )
        }
        "R64.GETINTARRAY" => {
            module_commands!(
                [b"R64.SETBIT", b"rb64", b"7", b"1"],
                [b"R64.GETINTARRAY", b"rb64"]
            )
        }
        "R64.JACCARD" => module_commands!(
            [b"R64.SETBIT", b"ra64", b"1", b"1"],
            [b"R64.SETBIT", b"rb64", b"1", b"1"],
            [b"R64.JACCARD", b"ra64", b"rb64"]
        ),
        "R64.MAX" => {
            module_commands!([b"R64.SETBIT", b"rb64", b"7", b"1"], [b"R64.MAX", b"rb64"])
        }
        "R64.MIN" => {
            module_commands!([b"R64.SETBIT", b"rb64", b"7", b"1"], [b"R64.MIN", b"rb64"])
        }
        "R64.OPTIMIZE" => module_commands!([b"R64.OPTIMIZE", b"rb64"]),
        "R64.RANGEINTARRAY" => module_commands!(
            [b"R64.SETINTARRAY", b"rb64", b"1", b"3", b"5"],
            [b"R64.RANGEINTARRAY", b"rb64", b"1", b"4"]
        ),
        "R64.SETBIT" => module_commands!([b"R64.SETBIT", b"rb64", b"7", b"1"]),
        "R64.SETBITARRAY" => module_commands!([b"R64.SETBITARRAY", b"rb64", b"1", b"2"]),
        "R64.SETFULL" => module_commands!([b"R64.SETFULL", b"rb64"]),
        "R64.SETINTARRAY" => module_commands!([b"R64.SETINTARRAY", b"rb64", b"1", b"2"]),
        "R64.SETRANGE" => module_commands!([b"R64.SETRANGE", b"rb64", b"1", b"3"]),
        "REDE.CREATE" => module_commands!([b"REDE.CREATE", b"rede"]),
        "REDE.DELETE" => module_commands!([b"REDE.CREATE", b"rede"], [b"REDE.DELETE", b"rede"]),
        "REDE.GET" => module_commands!([b"REDE.CREATE", b"rede"], [b"REDE.GET", b"rede"]),
        "RG.ABORTEXECUTION" => module_commands!([b"RG.ABORTEXECUTION", b"exec-1"]),
        "RG.CONFIGGET" => module_commands!([b"RG.CONFIGGET", b"MaxExecutions"]),
        "RG.CONFIGSET" => module_commands!([b"RG.CONFIGSET", b"MaxExecutions", b"100"]),
        "RG.DUMPEXECUTIONS" => module_commands!([b"RG.DUMPEXECUTIONS"]),
        "RG.DUMPREGISTRATIONS" => module_commands!([b"RG.DUMPREGISTRATIONS"]),
        "RG.GETRESULTS" => module_commands!([b"RG.GETRESULTS", b"exec-1"]),
        "RG.GETRESULTSBLOCKING" => module_commands!([b"RG.GETRESULTSBLOCKING", b"exec-1"]),
        "RG.JDUMPSESSIONS" => module_commands!([b"RG.JDUMPSESSIONS"]),
        "RG.JEXECUTE" => {
            module_commands!([b"RG.JEXECUTE", b"redis.registerFunction(function(){})"])
        }
        "RG.PYDUMPEXECUTIONS" => module_commands!([b"RG.PYDUMPEXECUTIONS"]),
        "RG.PYDUMPREQS" => module_commands!([b"RG.PYDUMPREQS"]),
        "RG.PYEXECUTE" => module_commands!([b"RG.PYEXECUTE", b"GB().run()"]),
        "RG.PYSTATS" => module_commands!([b"RG.PYSTATS"]),
        "RG.TRIGGER" => module_commands!([b"RG.TRIGGER", b"trigger", b"arg"]),
        "RG.UNREGISTER" => module_commands!([b"RG.UNREGISTER", b"registration"]),
        "SG.CREATE" => module_commands!([b"SG.CREATE", b"gate"]),
        "SG.DELETE" => module_commands!([b"SG.CREATE", b"gate"], [b"SG.DELETE", b"gate"]),
        "SG.VALIDATE" => module_commands!([b"SG.CREATE", b"gate"], [b"SG.VALIDATE", b"gate"]),
        "SNOWFLAKE.INFO" => module_commands!([b"SNOWFLAKE.INFO"]),
        "SNOWFLAKE.NEXT" => module_commands!([b"SNOWFLAKE.NEXT"]),
        "TDIGEST.ADD" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"]
        ),
        "TDIGEST.BYRANK" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"],
            [b"TDIGEST.BYRANK", b"td", b"0"]
        ),
        "TDIGEST.BYREVRANK" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"],
            [b"TDIGEST.BYREVRANK", b"td", b"0"]
        ),
        "TDIGEST.CDF" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"],
            [b"TDIGEST.CDF", b"td", b"1"]
        ),
        "TDIGEST.CREATE" => module_commands!([b"TDIGEST.CREATE", b"td"]),
        "TDIGEST.INFO" => module_commands!([b"TDIGEST.CREATE", b"td"], [b"TDIGEST.INFO", b"td"]),
        "TDIGEST.MAX" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"],
            [b"TDIGEST.MAX", b"td"]
        ),
        "TDIGEST.MERGE" => module_commands!(
            [b"TDIGEST.CREATE", b"td-src"],
            [b"TDIGEST.ADD", b"td-src", b"1"],
            [b"TDIGEST.MERGE", b"td-out", b"1", b"td-src"]
        ),
        "TDIGEST.MIN" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"],
            [b"TDIGEST.MIN", b"td"]
        ),
        "TDIGEST.QUANTILE" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"],
            [b"TDIGEST.QUANTILE", b"td", b"0.5"]
        ),
        "TDIGEST.RANK" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"],
            [b"TDIGEST.RANK", b"td", b"1"]
        ),
        "TDIGEST.RESET" => module_commands!([b"TDIGEST.CREATE", b"td"], [b"TDIGEST.RESET", b"td"]),
        "TDIGEST.REVRANK" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"],
            [b"TDIGEST.REVRANK", b"td", b"1"]
        ),
        "TDIGEST.TRIMMED_MEAN" => module_commands!(
            [b"TDIGEST.CREATE", b"td"],
            [b"TDIGEST.ADD", b"td", b"1", b"2"],
            [b"TDIGEST.TRIMMED_MEAN", b"td", b"0", b"1"]
        ),
        "TOPK.ADD" => module_commands!(
            [b"TOPK.RESERVE", b"topk", b"3"],
            [b"TOPK.ADD", b"topk", b"a"]
        ),
        "TOPK.COUNT" => module_commands!(
            [b"TOPK.RESERVE", b"topk", b"3"],
            [b"TOPK.COUNT", b"topk", b"a"]
        ),
        "TOPK.INCRBY" => module_commands!(
            [b"TOPK.RESERVE", b"topk", b"3"],
            [b"TOPK.INCRBY", b"topk", b"a", b"2"]
        ),
        "TOPK.INFO" => module_commands!([b"TOPK.RESERVE", b"topk", b"3"], [b"TOPK.INFO", b"topk"]),
        "TOPK.LIST" => module_commands!([b"TOPK.RESERVE", b"topk", b"3"], [b"TOPK.LIST", b"topk"]),
        "TOPK.QUERY" => module_commands!(
            [b"TOPK.RESERVE", b"topk", b"3"],
            [b"TOPK.QUERY", b"topk", b"a"]
        ),
        "TOPK.RESERVE" => module_commands!([b"TOPK.RESERVE", b"topk", b"3"]),
        "TS.ADD" => module_commands!([b"TS.ADD", b"ts", b"1", b"1"]),
        "TS.ALTER" => module_commands!(
            [b"TS.CREATE", b"ts"],
            [b"TS.ALTER", b"ts", b"LABELS", b"sensor", b"1"]
        ),
        "TS.CREATE" => module_commands!([b"TS.CREATE", b"ts"]),
        "TS.CREATERULE" => module_commands!([
            b"TS.CREATERULE",
            b"ts",
            b"ts-out",
            b"AGGREGATION",
            b"avg",
            b"60"
        ]),
        "TS.DECRBY" => module_commands!([b"TS.DECRBY", b"ts", b"1"]),
        "TS.DEL" => module_commands!(
            [b"TS.ADD", b"ts", b"1", b"1"],
            [b"TS.DEL", b"ts", b"0", b"2"]
        ),
        "TS.DELETERULE" => module_commands!([b"TS.DELETERULE", b"ts", b"ts-out"]),
        "TS.GET" => module_commands!([b"TS.ADD", b"ts", b"1", b"1"], [b"TS.GET", b"ts"]),
        "TS.INCRBY" => module_commands!([b"TS.INCRBY", b"ts", b"1"]),
        "TS.INFO" => module_commands!([b"TS.ADD", b"ts", b"1", b"1"], [b"TS.INFO", b"ts"]),
        "TS.MADD" => module_commands!([b"TS.MADD", b"ts1", b"1", b"1", b"ts2", b"1", b"2"]),
        "TS.MGET" => module_commands!(
            [b"TS.ADD", b"ts", b"1", b"1"],
            [b"TS.MGET", b"FILTER", b"sensor=1"]
        ),
        "TS.MRANGE" => module_commands!(
            [b"TS.ADD", b"ts", b"1", b"1"],
            [b"TS.MRANGE", b"-", b"+", b"FILTER", b"sensor=1"]
        ),
        "TS.MREVRANGE" => module_commands!(
            [b"TS.ADD", b"ts", b"1", b"1"],
            [b"TS.MREVRANGE", b"-", b"+", b"FILTER", b"sensor=1"]
        ),
        "TS.QUERYINDEX" => module_commands!([b"TS.QUERYINDEX", b"sensor=1"]),
        "TS.RANGE" => module_commands!(
            [b"TS.ADD", b"ts", b"1", b"1"],
            [b"TS.RANGE", b"ts", b"-", b"+"]
        ),
        "TS.REVRANGE" => module_commands!(
            [b"TS.ADD", b"ts", b"1", b"1"],
            [b"TS.REVRANGE", b"ts", b"-", b"+"]
        ),
        _ => return None,
    };
    Some(commands)
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
            &[b"CLIENT", b"GETNAME"],
            &[b"OBJECT", b"ENCODING", b"missing"],
            &[b"GET", b"missing"],
            &[b"MGET", b"a", b"b"],
        ],
        TransactionMode::ShardLocal,
    );

    let (_, hello_len) = RespCodec::decode(&raw).unwrap().unwrap();
    assert!(
        raw[hello_len..].starts_with(b"_\r\n_\r\n_\r\n*2\r\n_\r\n_\r\n"),
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
    assert_module_list_frame_matches_features(
        decode_resp_stream(&RespTestHarness::exec_resp(&store, &[b"MODULE", b"LIST"]))
            .into_iter()
            .next()
            .expect("MODULE LIST response"),
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
        RespTestHarness::exec_resp_integer(&store, &[b"SPUBLISH", b"shard-chan", b"msg"]),
        0
    );
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"PUBSUB", b"NUMPAT"]),
        0
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"PUBSUB", b"SHARDCHANNELS"]
        )),
        vec![Frame::Array(Vec::new())]
    );
    assert_eq!(
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"PUBSUB", b"SHARDNUMSUB", b"shard-chan"]
        )),
        vec![Frame::Array(vec![bulk(b"shard-chan"), Frame::Integer(0)])]
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
            &[b"SSUBSCRIBE", b"shard-chan"]
        )),
        vec![Frame::Array(vec![
            bulk(b"ssubscribe"),
            bulk(b"shard-chan"),
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
        decode_resp_stream(&RespTestHarness::exec_resp(
            &store,
            &[b"SUNSUBSCRIBE", b"shard-chan"]
        )),
        vec![Frame::Array(vec![
            bulk(b"sunsubscribe"),
            bulk(b"shard-chan"),
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
    #[cfg(feature = "redis-modules")]
    expected.extend(
        crate::commands::redis_modules::command_info_metadata()
            .into_iter()
            .map(|command| command.name),
    );

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
    assert_resp_error_contains(&store, &[b"OBJECT", b"REFCOUNT", b"k"], "no such key");
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
        RespTestHarness::exec_resp(&store, &[b"EVAL_RO", b"return 'ok'", b"0"]),
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
        RespTestHarness::exec_resp(
            &store,
            &[
                b"EVALSHA_RO",
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

#[cfg(feature = "redis")]
#[test]
fn lpos_and_lcs_match_redis_documented_examples() {
    let store = EmbeddedStore::new(4);
    let run = |parts: &[&[u8]]| -> Frame {
        RespTestHarness::exec_resp_sequence(&store, &[parts], TransactionMode::ShardLocal)
            .pop()
            .expect("exactly one frame per command")
    };

    // List from the Redis LPOS documentation:
    // index:  0 1 2 3 4 5 6 7 8 9 10
    // value:  a b c d 1 2 3 4 3 3 3
    run(&[
        b"RPUSH", b"mylist", b"a", b"b", b"c", b"d", b"1", b"2", b"3", b"4", b"3", b"3", b"3",
    ]);

    assert_eq!(run(&[b"LPOS", b"mylist", b"3"]), Frame::Integer(6));
    assert_eq!(run(&[b"LPOS", b"mylist", b"c"]), Frame::Integer(2));
    assert_eq!(
        run(&[b"LPOS", b"mylist", b"3", b"RANK", b"-1"]),
        Frame::Integer(10)
    );
    assert_eq!(
        run(&[b"LPOS", b"mylist", b"3", b"RANK", b"2"]),
        Frame::Integer(8)
    );
    assert_eq!(
        run(&[b"LPOS", b"mylist", b"3", b"COUNT", b"0"]),
        Frame::Array(vec![
            Frame::Integer(6),
            Frame::Integer(8),
            Frame::Integer(9),
            Frame::Integer(10),
        ])
    );
    assert_eq!(
        run(&[b"LPOS", b"mylist", b"3", b"COUNT", b"2"]),
        Frame::Array(vec![Frame::Integer(6), Frame::Integer(8)])
    );
    assert_eq!(
        run(&[b"LPOS", b"mylist", b"3", b"RANK", b"-1", b"COUNT", b"2"]),
        Frame::Array(vec![Frame::Integer(10), Frame::Integer(9)])
    );
    assert_eq!(
        run(&[b"LPOS", b"mylist", b"3", b"RANK", b"-1", b"COUNT", b"0"]),
        Frame::Array(vec![
            Frame::Integer(10),
            Frame::Integer(9),
            Frame::Integer(8),
            Frame::Integer(6),
        ])
    );
    // MAXLEN forward: only the first 7 elements (indices 0..=6) are compared.
    assert_eq!(
        run(&[b"LPOS", b"mylist", b"3", b"COUNT", b"0", b"MAXLEN", b"7"]),
        Frame::Array(vec![Frame::Integer(6)])
    );
    // MAXLEN backward: only the last 4 elements (indices 7..=10) are compared.
    assert_eq!(
        run(&[
            b"LPOS", b"mylist", b"3", b"RANK", b"-1", b"COUNT", b"0", b"MAXLEN", b"4",
        ]),
        Frame::Array(vec![
            Frame::Integer(10),
            Frame::Integer(9),
            Frame::Integer(8),
        ])
    );
    // Missing element and missing key.
    assert_eq!(run(&[b"LPOS", b"mylist", b"x"]), Frame::Null);
    assert_eq!(
        run(&[b"LPOS", b"mylist", b"x", b"COUNT", b"0"]),
        Frame::Array(Vec::new())
    );
    assert_eq!(run(&[b"LPOS", b"nokey", b"x"]), Frame::Null);
    assert_eq!(
        run(&[b"LPOS", b"nokey", b"x", b"COUNT", b"0"]),
        Frame::Array(Vec::new())
    );

    // LCS example from the Redis documentation.
    run(&[b"SET", b"key1", b"ohmytext"]);
    run(&[b"SET", b"key2", b"mynewtext"]);
    assert_eq!(
        run(&[b"LCS", b"key1", b"key2"]),
        Frame::BlobString(b"mytext".to_vec())
    );
    assert_eq!(run(&[b"LCS", b"key1", b"key2", b"LEN"]), Frame::Integer(6));
    assert_eq!(
        run(&[b"STRALGO", b"LCS", b"key1", b"key2", b"LEN"]),
        Frame::Integer(6)
    );
    // LCS over absent keys behaves as empty strings.
    assert_eq!(
        run(&[b"LCS", b"nope1", b"nope2", b"LEN"]),
        Frame::Integer(0)
    );
    assert_eq!(
        run(&[b"LCS", b"nope1", b"nope2"]),
        Frame::BlobString(Vec::new())
    );
}

#[cfg(feature = "redis")]
#[test]
fn waitaof_returns_zero_acks_without_persistence() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw = RespTestHarness::exec_resp(&store, parts);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };
    assert_eq!(
        run(&[b"WAITAOF", b"0", b"0", b"0"]),
        Frame::Array(vec![Frame::Integer(0), Frame::Integer(0)])
    );
    assert_eq!(
        run(&[b"WAITAOF", b"1", b"0", b"100"]),
        Frame::Error(
            "ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.".into()
        )
    );
    assert_eq!(
        run(&[b"WAITAOF", b"2", b"0", b"0"]),
        Frame::Error("ERR WAITAOF numlocal value should be 0 or 1.".into())
    );
    assert_eq!(
        run(&[b"WAITAOF", b"x", b"0", b"0"]),
        Frame::Error("ERR value is not an integer or out of range".into())
    );
    assert!(matches!(run(&[b"WAITAOF", b"0", b"0"]), Frame::Error(_)));
}

#[cfg(feature = "redis")]
#[test]
fn sintercard_counts_intersection_with_limit() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw = RespTestHarness::exec_resp(&store, parts);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };
    let _ = RespTestHarness::exec_resp_sequence_raw(
        &store,
        &[
            &[b"SADD", b"sc1", b"a", b"b", b"c", b"d"],
            &[b"SADD", b"sc2", b"b", b"c", b"d", b"e"],
            &[b"SADD", b"sc3", b"c", b"d", b"f"],
        ],
        TransactionMode::Disabled,
    );
    assert_eq!(
        run(&[b"SINTERCARD", b"2", b"sc1", b"sc2"]),
        Frame::Integer(3)
    );
    assert_eq!(
        run(&[b"SINTERCARD", b"3", b"sc1", b"sc2", b"sc3"]),
        Frame::Integer(2)
    );
    assert_eq!(
        run(&[b"SINTERCARD", b"2", b"sc1", b"sc2", b"LIMIT", b"2"]),
        Frame::Integer(2)
    );
    assert_eq!(
        run(&[b"SINTERCARD", b"2", b"sc1", b"sc2", b"LIMIT", b"0"]),
        Frame::Integer(3)
    );
    assert_eq!(
        run(&[b"SINTERCARD", b"2", b"sc1", b"nope"]),
        Frame::Integer(0)
    );
    assert!(matches!(run(&[b"SINTERCARD", b"0"]), Frame::Error(_)));
    assert!(matches!(
        run(&[b"SINTERCARD", b"3", b"sc1"]),
        Frame::Error(_)
    ));
}

#[cfg(feature = "redis")]
#[test]
fn hash_field_ttl_family_basic_semantics() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw =
            RespTestHarness::exec_resp_sequence_raw(&store, &[parts], TransactionMode::Disabled);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };

    run(&[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]);

    // HEXPIRE: existing field -> 1, missing field -> -2.
    assert_eq!(
        run(&[b"HEXPIRE", b"h", b"1000", b"FIELDS", b"2", b"f1", b"nope"]),
        Frame::Array(vec![Frame::Integer(1), Frame::Integer(-2)])
    );

    // HTTL: f1 has a positive ttl; f2 has none (-1).
    match run(&[b"HTTL", b"h", b"FIELDS", b"2", b"f1", b"f2"]) {
        Frame::Array(items) => {
            assert!(matches!(items[0], Frame::Integer(n) if (1..=1000).contains(&n)));
            assert_eq!(items[1], Frame::Integer(-1));
        }
        other => panic!("unexpected HTTL reply: {other:?}"),
    }

    // HPERSIST: removes f1's ttl (1), f2 had none (-1).
    assert_eq!(
        run(&[b"HPERSIST", b"h", b"FIELDS", b"2", b"f1", b"f2"]),
        Frame::Array(vec![Frame::Integer(1), Frame::Integer(-1)])
    );

    // HEXPIREAT with a past timestamp deletes the field (code 2).
    assert_eq!(
        run(&[b"HEXPIREAT", b"h", b"1", b"FIELDS", b"1", b"f1"]),
        Frame::Array(vec![Frame::Integer(2)])
    );
    assert_eq!(run(&[b"HGET", b"h", b"f1"]), Frame::Null);
    assert_eq!(run(&[b"HLEN", b"h"]), Frame::Integer(1));

    // Missing key returns one -2 per requested field.
    assert_eq!(
        run(&[b"HEXPIRE", b"absent", b"100", b"FIELDS", b"2", b"a", b"b"]),
        Frame::Array(vec![Frame::Integer(-2), Frame::Integer(-2)])
    );
    assert_eq!(
        run(&[b"HTTL", b"absent", b"FIELDS", b"2", b"a", b"b"]),
        Frame::Array(vec![Frame::Integer(-2), Frame::Integer(-2)])
    );

    // Negative relative TTLs are invalid; absolute past timestamps still delete.
    assert!(matches!(
        run(&[b"HEXPIRE", b"h", b"-1", b"FIELDS", b"1", b"f2"]),
        Frame::Error(_)
    ));
    assert!(matches!(
        run(&[b"HPEXPIRE", b"h", b"-1", b"FIELDS", b"1", b"f2"]),
        Frame::Error(_)
    ));

    // HSET overwriting a field clears its TTL.
    run(&[b"HSET", b"h2", b"x", b"1"]);
    run(&[b"HPEXPIRE", b"h2", b"100000", b"FIELDS", b"1", b"x"]);
    run(&[b"HSET", b"h2", b"x", b"2"]);
    assert_eq!(
        run(&[b"HTTL", b"h2", b"FIELDS", b"1", b"x"]),
        Frame::Array(vec![Frame::Integer(-1)])
    );
}

#[cfg(feature = "redis")]
#[test]
fn hash_field_ttl_elapsed_fields_disappear_from_keyspace_and_writes() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw =
            RespTestHarness::exec_resp_sequence_raw(&store, &[parts], TransactionMode::Disabled);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };

    run(&[b"HSET", b"h3", b"f", b"5"]);
    run(&[b"HPEXPIRE", b"h3", b"1", b"FIELDS", b"1", b"f"]);
    std::thread::sleep(std::time::Duration::from_millis(20));

    assert_eq!(run(&[b"HGET", b"h3", b"f"]), Frame::Null);
    assert_eq!(run(&[b"HLEN", b"h3"]), Frame::Integer(0));
    assert_eq!(run(&[b"EXISTS", b"h3"]), Frame::Integer(0));
    assert_eq!(run(&[b"DBSIZE"]), Frame::Integer(0));
    assert_eq!(run(&[b"GET", b"h3"]), Frame::Null);
    assert_eq!(
        run(&[b"TYPE", b"h3"]),
        Frame::SimpleString("none".to_string())
    );
    assert_eq!(
        run(&[b"HTTL", b"h3", b"FIELDS", b"1", b"f"]),
        Frame::Array(vec![Frame::Integer(-2)])
    );
    assert_eq!(run(&[b"APPEND", b"h3", b"s"]), Frame::Integer(1));
    assert_eq!(run(&[b"GET", b"h3"]), Frame::BlobString(b"s".to_vec()));
    assert_eq!(run(&[b"DEL", b"h3"]), Frame::Integer(1));

    assert_eq!(run(&[b"HSETNX", b"h3", b"f", b"9"]), Frame::Integer(1));
    assert_eq!(
        run(&[b"HGET", b"h3", b"f"]),
        Frame::BlobString(b"9".to_vec())
    );

    run(&[b"HSET", b"list-after-expired-hash", b"f", b"v"]);
    run(&[
        b"HPEXPIRE",
        b"list-after-expired-hash",
        b"1",
        b"FIELDS",
        b"1",
        b"f",
    ]);
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(
        run(&[b"LPUSH", b"list-after-expired-hash", b"x"]),
        Frame::Integer(1)
    );
    assert_eq!(
        run(&[b"TYPE", b"list-after-expired-hash"]),
        Frame::SimpleString("list".to_string())
    );

    run(&[b"HSET", b"hdel-expired", b"f", b"v"]);
    run(&[b"HPEXPIRE", b"hdel-expired", b"1", b"FIELDS", b"1", b"f"]);
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(run(&[b"HDEL", b"hdel-expired", b"f"]), Frame::Integer(0));
    assert_eq!(run(&[b"EXISTS", b"hdel-expired"]), Frame::Integer(0));

    run(&[b"HSET", b"hincr-expired", b"f", b"5"]);
    run(&[b"HPEXPIRE", b"hincr-expired", b"1", b"FIELDS", b"1", b"f"]);
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(
        run(&[b"HINCRBY", b"hincr-expired", b"f", b"2"]),
        Frame::Integer(2)
    );
    assert_eq!(
        run(&[b"HGET", b"hincr-expired", b"f"]),
        Frame::BlobString(b"2".to_vec())
    );
}

#[cfg(feature = "redis")]
#[test]
fn bitfield_ro_allows_get_and_rejects_writes() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw =
            RespTestHarness::exec_resp_sequence_raw(&store, &[parts], TransactionMode::Disabled);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };

    run(&[b"BITFIELD", b"bf", b"SET", b"u8", b"0", b"200"]);
    // GET is allowed and reads the stored field.
    assert_eq!(
        run(&[b"BITFIELD_RO", b"bf", b"GET", b"u8", b"0"]),
        Frame::Array(vec![Frame::Integer(200)])
    );
    // SET / INCRBY are rejected.
    assert_eq!(
        run(&[b"BITFIELD_RO", b"bf", b"SET", b"u8", b"0", b"1"]),
        Frame::Error("ERR BITFIELD_RO only supports the GET subcommand".into())
    );
    assert_eq!(
        run(&[b"BITFIELD_RO", b"bf", b"INCRBY", b"u8", b"0", b"1"]),
        Frame::Error("ERR BITFIELD_RO only supports the GET subcommand".into())
    );
}

#[cfg(feature = "redis")]
#[test]
fn sort_ro_sorts_and_rejects_store() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw =
            RespTestHarness::exec_resp_sequence_raw(&store, &[parts], TransactionMode::Disabled);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };

    run(&[b"RPUSH", b"sr", b"3", b"1", b"2"]);
    // SORT_RO sorts like SORT.
    assert_eq!(
        run(&[b"SORT_RO", b"sr"]),
        Frame::Array(vec![
            Frame::BlobString(b"1".to_vec()),
            Frame::BlobString(b"2".to_vec()),
            Frame::BlobString(b"3".to_vec()),
        ])
    );
    // STORE is rejected.
    assert!(matches!(
        run(&[b"SORT_RO", b"sr", b"STORE", b"dst"]),
        Frame::Error(_)
    ));
}

#[cfg(feature = "redis")]
#[test]
fn geosearch_and_store_cover_radius_box_and_exclusive_shape_syntax() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw =
            RespTestHarness::exec_resp_sequence_raw(&store, &[parts], TransactionMode::Disabled);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };

    run(&[
        b"GEOADD",
        b"geo",
        b"-73.9857",
        b"40.7484",
        b"empire",
        b"-73.9897",
        b"40.7411",
        b"flatiron",
    ]);

    assert_eq!(
        run(&[
            b"GEOSEARCH",
            b"geo",
            b"FROMMEMBER",
            b"empire",
            b"BYRADIUS",
            b"2",
            b"km",
            b"ASC",
        ]),
        Frame::Array(vec![
            Frame::BlobString(b"empire".to_vec()),
            Frame::BlobString(b"flatiron".to_vec()),
        ])
    );

    assert_eq!(
        run(&[
            b"GEOSEARCH",
            b"geo",
            b"FROMLONLAT",
            b"-73.9857",
            b"40.7484",
            b"BYBOX",
            b"2",
            b"2",
            b"km",
            b"ASC",
        ]),
        Frame::Array(vec![
            Frame::BlobString(b"empire".to_vec()),
            Frame::BlobString(b"flatiron".to_vec()),
        ])
    );

    assert_eq!(
        run(&[
            b"GEOSEARCHSTORE",
            b"geo-store",
            b"geo",
            b"FROMMEMBER",
            b"empire",
            b"BYRADIUS",
            b"2",
            b"km",
            b"ASC",
        ]),
        Frame::Integer(2)
    );
    assert_eq!(run(&[b"ZCARD", b"geo-store"]), Frame::Integer(2));

    assert!(matches!(
        run(&[
            b"GEOSEARCH",
            b"geo",
            b"FROMMEMBER",
            b"empire",
            b"FROMLONLAT",
            b"-73.9857",
            b"40.7484",
            b"BYRADIUS",
            b"2",
            b"km",
        ]),
        Frame::Error(_)
    ));
    assert!(matches!(
        run(&[
            b"GEOSEARCH",
            b"geo",
            b"FROMMEMBER",
            b"empire",
            b"BYRADIUS",
            b"2",
            b"km",
            b"BYBOX",
            b"2",
            b"2",
            b"km",
        ]),
        Frame::Error(_)
    ));
}

#[cfg(feature = "redis-functions")]
#[test]
fn function_commands_are_accepted_with_empty_registry_semantics() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw =
            RespTestHarness::exec_resp_sequence_raw(&store, &[parts], TransactionMode::Disabled);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };

    assert_eq!(run(&[b"FUNCTION", b"LIST"]), Frame::Array(Vec::new()));
    assert_eq!(
        run(&[
            b"FUNCTION",
            b"LIST",
            b"LIBRARYNAME",
            b"missing*",
            b"WITHCODE"
        ]),
        Frame::Array(Vec::new())
    );
    assert_eq!(
        run(&[b"FUNCTION", b"FLUSH"]),
        Frame::SimpleString("OK".to_string())
    );
    assert!(matches!(
        run(&[b"FUNCTION", b"STATS"]),
        Frame::Array(items) if !items.is_empty()
    ));
    assert!(matches!(
        run(&[b"FUNCTION", b"LOAD", b"#!lua name=lib\n"]),
        Frame::Error(message) if message.contains("functions are not supported")
    ));
    assert!(matches!(
        run(&[b"FUNCTION", b"LOAD", b"REPLACE", b"#!lua name=lib\n"]),
        Frame::Error(message) if message.contains("functions are not supported")
    ));
    assert!(matches!(
        run(&[b"FUNCTION", b"RESTORE", b"", b"REPLACE"]),
        Frame::Error(message) if message.contains("functions are not supported")
    ));
    assert!(matches!(
        run(&[b"FCALL", b"missing", b"0"]),
        Frame::Error(message) if message.contains("Function not found")
    ));
    assert!(matches!(
        run(&[b"FCALL_RO", b"missing", b"0"]),
        Frame::Error(message) if message.contains("Function not found")
    ));
}

#[cfg(feature = "redis-modules")]
#[test]
fn module_commands_are_accepted_with_empty_registry_semantics() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw =
            RespTestHarness::exec_resp_sequence_raw(&store, &[parts], TransactionMode::Disabled);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };

    assert_module_list_frame_matches_features(run(&[b"MODULE", b"LIST"]));
    assert!(matches!(
        run(&[b"MODULE", b"LOAD", b"/tmp/nope.so"]),
        Frame::Error(message) if message.contains("modules are not supported")
    ));
    assert!(matches!(
        run(&[
            b"MODULE",
            b"LOADEX",
            b"/tmp/nope.so",
            b"CONFIG",
            b"name",
            b"value",
            b"ARGS",
            b"arg"
        ]),
        Frame::Error(message) if message.contains("modules are not supported")
    ));
    assert!(matches!(
        run(&[b"MODULE", b"UNLOAD", b"missing"]),
        Frame::Error(message) if message.contains("modules are not supported")
    ));
}

#[cfg(feature = "redis")]
fn assert_module_list_frame_matches_features(frame: Frame) {
    match frame {
        Frame::Array(modules) => {
            #[cfg(feature = "redis-modules")]
            let expected = crate::commands::redis_modules::enabled_modules().len();
            #[cfg(not(feature = "redis-modules"))]
            let expected = 0;
            assert_eq!(
                modules.len(),
                expected,
                "MODULE LIST should mirror the enabled module feature flags"
            );
        }
        frame => panic!("expected MODULE LIST array, got {frame:?}"),
    }
}

#[cfg(feature = "redis-modules-all")]
#[test]
fn redis_modules_all_commands_and_embedded_apis_are_feature_gated() {
    let store = EmbeddedStore::new(8);
    let run = |parts: &[&[u8]]| -> Frame {
        let raw =
            RespTestHarness::exec_resp_sequence_raw(&store, &[parts], TransactionMode::Disabled);
        decode_resp_stream(&raw)
            .into_iter()
            .next()
            .expect("one frame")
    };

    match run(&[b"MODULE", b"LIST"]) {
        Frame::Array(modules) => assert_eq!(
            modules.len(),
            crate::commands::redis_modules::enabled_modules().len()
        ),
        frame => panic!("expected module list array, got {frame:?}"),
    }
    assert!(matches!(
        run(&[b"FT.SEARCH", b"idx", b"*"]),
        Frame::Array(items) if items == vec![Frame::Integer(0)]
    ));
    assert_eq!(
        run(&[b"JSON.SET", b"doc", b"$", br#"{"a":1}"#]),
        Frame::SimpleString("OK".into())
    );
    assert!(matches!(
        run(&[b"JSON.GET", b"doc", b"$"]),
        Frame::BlobString(value) if value == br#"{"a":1}"#
    ));
    assert_eq!(run(&[b"BF.ADD", b"filter", b"value"]), Frame::Integer(1));
    assert_eq!(run(&[b"BF.EXISTS", b"filter", b"value"]), Frame::Integer(1));
    assert!(matches!(
        run(&[b"TS.ADD", b"series", b"*", b"1.0"]),
        Frame::Integer(_)
    ));
    assert!(matches!(
        run(&[b"TS.GET", b"series"]),
        Frame::Array(items) if items.len() == 2
    ));

    match run(&[b"COMMAND", b"INFO", b"JSON.GET"]) {
        Frame::Array(items) => assert!(matches!(items.first(), Some(Frame::Array(_)))),
        frame => panic!("expected command info array, got {frame:?}"),
    }
    match run(&[
        b"COMMAND",
        b"INFO",
        b"TOPK.INFO",
        b"BF.CARD",
        b"TS.MREVRANGE",
        b"JSON.MERGE",
        b"R64.SETBIT",
    ]) {
        Frame::Array(items) => {
            assert_eq!(items.len(), 5);
            assert!(items.iter().all(|item| matches!(item, Frame::Array(_))));
        }
        frame => panic!("expected command info array, got {frame:?}"),
    }
    match run(&[b"COMMAND", b"INFO", b"TOPK.*", b"ROARING.*"]) {
        Frame::Array(items) => {
            assert_eq!(items.len(), 2);
            assert!(items.iter().all(|item| matches!(item, Frame::Null)));
        }
        frame => panic!("expected command info array, got {frame:?}"),
    }
    assert!(matches!(
        run(&[b"TOPK.RESERVE", b"topk", b"2", b"8", b"7", b"0.9"]),
        Frame::SimpleString(message) if message == "OK"
    ));
    assert!(matches!(
        run(&[b"TOPK.ADD", b"topk", b"a", b"b", b"a"]),
        Frame::Array(items) if items.iter().all(|item| matches!(item, Frame::Null))
    ));
    match run(&[b"TOPK.INCRBY", b"topk", b"c", b"3"]) {
        Frame::Array(items) => {
            assert_eq!(items.len(), 1);
            assert!(matches!(items.first(), Some(Frame::BlobString(item)) if item == b"b"));
        }
        frame => panic!("expected TOPK.INCRBY array, got {frame:?}"),
    }
    assert_eq!(
        run(&[b"TOPK.QUERY", b"topk", b"a", b"b", b"c"]),
        Frame::Array(vec![
            Frame::Integer(1),
            Frame::Integer(0),
            Frame::Integer(1)
        ])
    );
    assert_eq!(
        run(&[b"TOPK.COUNT", b"topk", b"a", b"b", b"c"]),
        Frame::Array(vec![
            Frame::Integer(2),
            Frame::Integer(1),
            Frame::Integer(3)
        ])
    );
    assert_eq!(
        run(&[b"TOPK.LIST", b"topk", b"WITHCOUNT"]),
        Frame::Array(vec![
            Frame::BlobString(b"c".to_vec()),
            Frame::Integer(3),
            Frame::BlobString(b"a".to_vec()),
            Frame::Integer(2),
        ])
    );
    match run(&[b"TOPK.INFO", b"topk"]) {
        Frame::Array(items) => assert_eq!(items.len(), 8),
        frame => panic!("expected TOPK.INFO array, got {frame:?}"),
    }
    assert_eq!(
        run(&[b"CMS.INITBYDIM", b"cms", b"10", b"5"]),
        Frame::SimpleString("OK".into())
    );
    assert_eq!(
        run(&[b"CMS.INCRBY", b"cms", b"a", b"3"]),
        Frame::Array(vec![Frame::Integer(3)])
    );
    assert_eq!(
        run(&[b"CMS.QUERY", b"cms", b"a", b"b"]),
        Frame::Array(vec![Frame::Integer(3), Frame::Integer(0)])
    );
    assert_eq!(
        run(&[b"TDIGEST.CREATE", b"td"]),
        Frame::SimpleString("OK".into())
    );
    assert_eq!(
        run(&[b"TDIGEST.ADD", b"td", b"1", b"2", b"3"]),
        Frame::SimpleString("OK".into())
    );
    assert!(matches!(
        run(&[b"TDIGEST.QUANTILE", b"td", b"0.5"]),
        Frame::Array(items) if items.len() == 1
    ));
    assert_eq!(run(&[b"R.SETBIT", b"rb", b"7"]), Frame::Integer(0));
    assert_eq!(run(&[b"R.GETBIT", b"rb", b"7"]), Frame::Integer(1));
    assert!(matches!(
        run(&[b"CL.THROTTLE", b"cell", b"5", b"10", b"60", b"1"]),
        Frame::Array(items) if items.len() == 5
    ));
    assert!(matches!(run(&[b"SNOWFLAKE.NEXT"]), Frame::Integer(1)));
    assert_eq!(
        run(&[b"FT.CREATE", b"idx", b"SCHEMA", b"title", b"TEXT"]),
        Frame::SimpleString("OK".into())
    );
    assert!(matches!(
        run(&[b"GRAPH.QUERY", b"graph", b"RETURN 1"]),
        Frame::Array(items) if items.len() == 3
    ));
    assert_eq!(
        run(&[b"AI.TENSORSET", b"tensor", b"FLOAT", b"1", b"VALUES", b"1"]),
        Frame::SimpleString("OK".into())
    );
    assert!(matches!(
        run(&[b"RG.PYEXECUTE", b"GB().run()"]),
        Frame::BlobString(value) if value.starts_with(b"exec-")
    ));
    assert_eq!(
        run(&[b"NR.CREATE", b"net"]),
        Frame::SimpleString("OK".into())
    );
    assert!(matches!(
        run(&[b"JS.EVAL", b"1 + 1"]),
        Frame::BlobString(value) if value == b"null"
    ));
    assert_eq!(
        run(&[b"SG.CREATE", b"gate"]),
        Frame::SimpleString("OK".into())
    );
    assert_eq!(run(&[b"SG.VALIDATE", b"gate"]), Frame::Integer(1));
    assert_eq!(
        run(&[b"REDE.CREATE", b"rede"]),
        Frame::SimpleString("OK".into())
    );
    assert!(matches!(
        run(&[b"REDE.GET", b"rede"]),
        Frame::Array(items) if !items.is_empty()
    ));

    assert_eq!(
        store.redis_json().family(),
        crate::storage::RedisModuleFamily::RedisJson
    );
    assert!(matches!(
        store.redis_search().execute("FT.SEARCH", &[b"idx", b"*"]),
        crate::storage::RedisModuleApiResult::Array(items) if items == vec![crate::storage::RedisModuleApiResult::Integer(0)]
    ));
    assert!(matches!(
        store
            .count_min_sketch()
            .execute("CMS.QUERY", &[b"cms", b"a"]),
        crate::storage::RedisModuleApiResult::Array(items) if items == vec![crate::storage::RedisModuleApiResult::Integer(3)]
    ));
    assert!(matches!(
        store.topk().execute("TOPK.INFO", &[b"topk"]),
        crate::storage::RedisModuleApiResult::TopKInfo {
            k: 2,
            width: 8,
            depth: 7,
            decay,
        } if (decay - 0.9).abs() < f64::EPSILON
    ));
    assert!(matches!(
        store.topk().execute("TOPK.LIST", &[b"topk"]),
        crate::storage::RedisModuleApiResult::Array(items) if items.len() == 2
    ));
    assert!(matches!(
        store.topk().execute("TOPK.ADD", &[b"topk", b"d"]),
        crate::storage::RedisModuleApiResult::Array(items) if items.len() == 1
    ));
}
