use super::direct_protocol::*;
use super::transactions::{TransactionCoordinator, TransactionState};
use super::wire::*;
use super::*;
use crate::config::TransactionMode;
#[cfg(feature = "redis-compat")]
use crate::storage::RedisObjectResult;
use crate::storage::{hash_key, hash_key_tag, shift_for, stripe_index};
#[cfg(feature = "redis-compat")]
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
            Instant::now(),
        );
        out.to_vec()
    }

    fn exec_resp_sequence(
        store: &EmbeddedStore,
        commands: &[&[&[u8]]],
        transaction_mode: TransactionMode,
    ) -> Vec<Frame> {
        let mut input = Vec::new();
        for command in commands {
            encode_resp_command(command, &mut input);
        }

        let coordinator =
            TransactionCoordinator::new(store.shard_count(), transaction_mode).map(Arc::new);
        let mut transaction_state = TransactionState::default();
        let mut out = BytesMut::new();
        let consumed = DirectProtocol::process_shared_request_buffer_with_context(
            &input,
            store,
            &mut out,
            None,
            SharedRequestBufferContext {
                single_threaded: false,
                owned_shard_id: None,
                started_at: Instant::now(),
                transaction_coordinator: coordinator.as_deref(),
                transaction_state: &mut transaction_state,
            },
        )
        .expect("request buffer should process");
        assert_eq!(consumed, input.len());
        decode_resp_stream(&out)
    }

    fn exec_fcnp_resp_sequence(
        store: &EmbeddedStore,
        commands: &[&[&[u8]]],
        transaction_mode: TransactionMode,
    ) -> Vec<FastResponse> {
        let mut input = Vec::new();
        for command in commands {
            encode_fcnp_resp_command(command, &mut input);
        }
        Self::process_fcnp_input(store, &input, transaction_mode)
    }

    fn process_fcnp_input(
        store: &EmbeddedStore,
        input: &[u8],
        transaction_mode: TransactionMode,
    ) -> Vec<FastResponse> {
        let coordinator =
            TransactionCoordinator::new(store.shard_count(), transaction_mode).map(Arc::new);
        let mut transaction_state = TransactionState::default();
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

    fn exec_fcnp_resp(store: &EmbeddedStore, parts: Vec<&[u8]>) -> FastResponse {
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
            false,
            Instant::now(),
        );
        FastCodec::decode_response(&out).unwrap().unwrap().0
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

fn encode_fcnp_resp_command(parts: &[&[u8]], out: &mut Vec<u8>) {
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
            .expect("FCNP response decode should succeed")
            .expect("FCNP response should be complete");
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

#[cfg(feature = "redis-compat")]
fn decode_scan_response(raw: &[u8]) -> (u64, Vec<Vec<u8>>) {
    let (frame, consumed) = RespCodec::decode(raw).unwrap().expect("scan response");
    assert_eq!(consumed, raw.len());
    let Frame::Array(items) = frame else {
        panic!("scan response should be an array");
    };
    let [cursor, values]: [Frame; 2] = items.try_into().ok().expect("scan response shape");
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

#[cfg(feature = "redis-compat")]
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

#[cfg(feature = "redis-compat")]
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
#[cfg(feature = "redis-compat")]
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
        decode_bulk_array(&RespTestHarness::exec_resp(
            &store,
            &[b"LRANGE", b"l", b"0", b"-1"]
        )),
        vec![b"c".to_vec(), b"a".to_vec(), b"b".to_vec()]
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
#[cfg(feature = "redis-compat")]
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
#[cfg(feature = "redis-compat")]
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
        store.rpush(b"rpl-src", &[b"a".as_slice(), b"b".as_slice()]),
        RedisObjectResult::Integer(2)
    );
    assert_resp_error_contains(&store, &[b"RPOPLPUSH", b"rpl-src", b"wrong"], "WRONGTYPE");
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
}

#[test]
#[cfg(feature = "redis-compat")]
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
#[cfg(feature = "redis-compat")]
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
#[cfg(feature = "redis-compat")]
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

#[cfg(feature = "redis-compat")]
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

#[cfg(feature = "redis-compat")]
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

#[cfg(feature = "redis-compat")]
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
    ));
    store.set(b"watched".to_vec(), b"outside".to_vec(), None);
    assert!(transaction_state.handle_resp_command(
        Some(&coordinator),
        &store,
        &[b"MULTI"],
        &mut out,
    ));
    assert!(transaction_state.handle_resp_command(
        Some(&coordinator),
        &store,
        &[b"SET", b"watched", b"inside"],
        &mut out,
    ));
    assert!(transaction_state.handle_resp_command(
        Some(&coordinator),
        &store,
        &[b"EXEC"],
        &mut out,
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

#[cfg(feature = "redis-compat")]
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

#[cfg(feature = "redis-compat")]
#[test]
fn raw_resp_transactions_can_be_disabled() {
    let store = EmbeddedStore::new(4);
    let frames =
        RespTestHarness::exec_resp_sequence(&store, &[&[b"MULTI"]], TransactionMode::Disabled);

    assert!(
        matches!(&frames[0], Frame::Error(message) if message.contains("transactions are disabled"))
    );
}

#[cfg(feature = "redis-compat")]
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

#[cfg(feature = "redis-compat")]
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

#[cfg(feature = "redis-compat")]
#[test]
fn fcnp_resp_transactions_queue_and_exec_in_order() {
    let store = EmbeddedStore::new(4);
    let responses = RespTestHarness::exec_fcnp_resp_sequence(
        &store,
        &[
            &[b"MULTI"],
            &[b"SET", b"fcnp-txn", b"1"],
            &[b"GET", b"fcnp-txn"],
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

#[cfg(feature = "redis-compat")]
#[test]
fn fcnp_typed_command_inside_transaction_aborts_exec() {
    let store = EmbeddedStore::new(4);
    let mut input = Vec::new();
    encode_fcnp_resp_command(&[b"MULTI"], &mut input);
    FastCodec::encode_request(
        &FastRequest {
            key_hash: Some(hash_key(b"typed-fcnp-txn")),
            route_shard: None,
            key_tag: None,
            command: FastCommand::Set {
                key: b"typed-fcnp-txn",
                value: b"1",
            },
        },
        &mut input,
    );
    encode_fcnp_resp_command(&[b"EXEC"], &mut input);

    let responses =
        RespTestHarness::process_fcnp_input(&store, &input, TransactionMode::ShardLocal);
    assert_eq!(responses[0], FastResponse::Value(b"+OK\r\n".to_vec()));
    assert!(
        matches!(&responses[1], FastResponse::Error(message) if message.windows("typed FCNP".len()).any(|window| window == b"typed FCNP"))
    );
    assert!(
        matches!(&responses[2], FastResponse::Value(message) if message.starts_with(b"-EXECABORT"))
    );
    assert_eq!(
        RespTestHarness::exec_resp(&store, &[b"GET", b"typed-fcnp-txn"]),
        b"$-1\r\n"
    );
}

#[test]
fn fcnp_resp_command_wraps_redis_reply_bytes() {
    let store = EmbeddedStore::new(4);
    let response = RespTestHarness::exec_fcnp_resp(
        &store,
        vec![b"SET".as_slice(), b"k".as_slice(), b"v".as_slice()],
    );
    assert_eq!(response, FastResponse::Value(b"+OK\r\n".to_vec()));

    let response =
        RespTestHarness::exec_fcnp_resp(&store, vec![b"GET".as_slice(), b"k".as_slice()]);
    assert_eq!(response, FastResponse::Value(b"$1\r\nv\r\n".to_vec()));
}

#[test]
#[cfg(feature = "redis-compat")]
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
#[cfg(feature = "redis-compat")]
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
#[cfg(feature = "redis-compat")]
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
#[cfg(feature = "redis-compat")]
fn raw_resp_object_streaming_commands_round_trip() {
    let store = EmbeddedStore::new(4);

    store.set(b"blob".to_vec(), vec![b'x'; 64], None);
    assert_eq!(
        RespTestHarness::exec_resp_integer(&store, &[b"STRLEN", b"blob"]),
        64
    );

    assert_eq!(store.hset(b"h", b"a", b"1"), RedisObjectResult::Integer(1));
    assert_eq!(store.hset(b"h", b"b", b"2"), RedisObjectResult::Integer(1));
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
#[cfg(feature = "redis-compat")]
fn fcnp_resp_scan_and_shard_scan_return_resp_bytes() {
    let store = EmbeddedStore::new(4);
    store.set(b"k".to_vec(), b"v".to_vec(), None);

    let response = RespTestHarness::exec_fcnp_resp(
        &store,
        vec![
            b"SCAN".as_slice(),
            b"0".as_slice(),
            b"COUNT".as_slice(),
            b"10".as_slice(),
        ],
    );
    let FastResponse::Value(raw) = response else {
        panic!("FCNP SCAN should return RESP bytes");
    };
    let (_cursor, keys) = decode_scan_response(&raw);
    assert_eq!(BTreeSet::from_iter(keys), BTreeSet::from([b"k".to_vec()]));

    let route = store.route_key(b"k");
    let shard_id = route.shard_id.to_string();
    let response = RespTestHarness::exec_fcnp_resp(
        &store,
        vec![
            b"FCNP.SCANSHARD".as_slice(),
            shard_id.as_bytes(),
            b"0".as_slice(),
            b"COUNT".as_slice(),
            b"10".as_slice(),
        ],
    );
    let FastResponse::Value(raw) = response else {
        panic!("FCNP.SCANSHARD should return RESP bytes");
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
                    b"FCNP.SCANSHARD".as_slice(),
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
        panic!("direct shard FCNP.SCANSHARD should return RESP bytes");
    };
    let (_cursor, keys) = decode_scan_response(&raw);
    assert_eq!(BTreeSet::from_iter(keys), BTreeSet::from([b"k".to_vec()]));
}

#[test]
fn fcnp_owned_shard_fast_path_handles_tagged_get_set_del() {
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
fn fcnp_owned_shard_rejects_mismatched_route() {
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
        FastResponse::Error(b"ERR FCNP route shard mismatch".to_vec())
    );
    assert!(store.get(key).is_none());
}
