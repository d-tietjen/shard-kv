use super::direct_protocol::*;
use super::wire::*;
use super::*;
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
        let command = DirectProtocol::parse_resp_direct_command(parts[0], args)
            .expect("command should parse");
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
