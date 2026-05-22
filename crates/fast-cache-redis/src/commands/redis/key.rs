use bytes::BytesMut;

use super::parse::glob_matches;
use super::{
    eq_ignore_ascii_case, error, parse_u64, parse_usize, reserve_resp_bulk_array_hint, write_frame,
    write_resp_array_header, wrong_arity, wrongtype,
};
use crate::commands::redis::RedisCommand;
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{
    DEFAULT_SCAN_COUNT, EmbeddedStore, RedisKeyScanType, RedisObjectArrayItem,
    RedisObjectReadOutcome,
};

#[cfg(feature = "server")]
pub(super) fn write_scan_shard_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
    match parse_fcnp_scan_shard_args(args) {
        Ok((shard_id, options)) => {
            let mut items =
                BytesMut::with_capacity(options.count.saturating_mul(32).min(64 * 1024));
            let result = store.scan_redis_keys_in_shard_visit(
                shard_id,
                options.cursor,
                options.count,
                options.scan_type(),
                &mut |key| {
                    if key_pattern_matches(key, options.pattern) {
                        ServerWire::write_resp_blob_string(&mut items, key);
                        true
                    } else {
                        false
                    }
                },
            );
            match result {
                Some(result) => write_scan_resp_payload(out, result.cursor, result.keys, &items),
                None => write_frame(out, &error("ERR invalid shard")),
            }
        }
        Err(frame) => write_frame(out, &frame),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_scan_resp_from_options(
    store: &EmbeddedStore,
    options: KeyScanOptions<'_>,
    out: &mut BytesMut,
) {
    let mut items = BytesMut::with_capacity(options.count.saturating_mul(32).min(64 * 1024));
    let result = store.scan_redis_keys_visit(
        options.cursor,
        options.count,
        options.scan_type(),
        &mut |key| {
            if key_pattern_matches(key, options.pattern) {
                ServerWire::write_resp_blob_string(&mut items, key);
                true
            } else {
                false
            }
        },
    );
    write_scan_resp_payload(out, result.cursor, result.keys, &items);
}

#[cfg(feature = "server")]
pub(super) fn write_scan_resp_payload(
    out: &mut BytesMut,
    cursor: u64,
    item_count: usize,
    items: &[u8],
) {
    write_resp_array_header(out, 2);
    let mut cursor_buffer = itoa::Buffer::new();
    ServerWire::write_resp_blob_string(out, cursor_buffer.format(cursor).as_bytes());
    write_resp_array_header(out, item_count);
    out.extend_from_slice(items);
}

#[cfg(feature = "server")]
pub(crate) fn write_object_array_item(out: &mut BytesMut, item: RedisObjectArrayItem<'_>) {
    match item {
        RedisObjectArrayItem::Begin(len) => {
            reserve_resp_bulk_array_hint(out, len);
            write_resp_array_header(out, len);
        }
        RedisObjectArrayItem::Bulk(Some(value)) => ServerWire::write_resp_blob_string(out, value),
        RedisObjectArrayItem::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
    }
}

#[cfg(feature = "server")]
pub(crate) fn finish_object_array_visit(out: &mut BytesMut, outcome: RedisObjectReadOutcome) {
    match outcome {
        RedisObjectReadOutcome::Written => {}
        RedisObjectReadOutcome::Missing => write_resp_array_header(out, 0),
        RedisObjectReadOutcome::WrongType => write_frame(out, &wrongtype()),
    }
}

#[cfg(feature = "server")]
pub(crate) fn finish_object_bulk_visit(out: &mut BytesMut, outcome: RedisObjectReadOutcome) {
    match outcome {
        RedisObjectReadOutcome::Written => {}
        RedisObjectReadOutcome::Missing => out.extend_from_slice(b"$-1\r\n"),
        RedisObjectReadOutcome::WrongType => write_frame(out, &wrongtype()),
    }
}

#[cfg(feature = "server")]
pub(crate) fn finish_object_integer_visit(
    out: &mut BytesMut,
    outcome: RedisObjectReadOutcome,
    value: i64,
    missing: i64,
) {
    match outcome {
        RedisObjectReadOutcome::Written => ServerWire::write_resp_integer(out, value),
        RedisObjectReadOutcome::Missing => ServerWire::write_resp_integer(out, missing),
        RedisObjectReadOutcome::WrongType => write_frame(out, &wrongtype()),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_scan_object_array_item(out: &mut BytesMut, item: RedisObjectArrayItem<'_>) {
    match item {
        RedisObjectArrayItem::Begin(len) => {
            reserve_resp_bulk_array_hint(out, len);
            write_resp_array_header(out, 2);
            ServerWire::write_resp_blob_string(out, b"0");
            write_resp_array_header(out, len);
        }
        RedisObjectArrayItem::Bulk(Some(value)) => ServerWire::write_resp_blob_string(out, value),
        RedisObjectArrayItem::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
    }
}

#[cfg(feature = "server")]
pub(crate) fn finish_scan_object_array_visit(out: &mut BytesMut, outcome: RedisObjectReadOutcome) {
    match outcome {
        RedisObjectReadOutcome::Written => {}
        RedisObjectReadOutcome::Missing => {
            write_resp_array_header(out, 2);
            ServerWire::write_resp_blob_string(out, b"0");
            write_resp_array_header(out, 0);
        }
        RedisObjectReadOutcome::WrongType => write_frame(out, &wrongtype()),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_fcnp_scan_fast_response(
    store: &EmbeddedStore,
    args: &[&[u8]],
    out: &mut BytesMut,
) {
    let start = ServerWire::begin_fast_value(out);
    crate::commands::scan::Scan::write_resp(store, args, out);
    ServerWire::finish_fast_value(out, start);
}

#[cfg(feature = "server")]
pub(crate) fn write_fcnp_scan_shard_fast_response(
    store: &EmbeddedStore,
    args: &[&[u8]],
    out: &mut BytesMut,
) {
    let start = ServerWire::begin_fast_value(out);
    write_scan_shard_resp(store, args, out);
    ServerWire::finish_fast_value(out, start);
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_scan_resp_from_options(
    _store: &EmbeddedStore,
    _options: KeyScanOptions<'_>,
    _out: &mut BytesMut,
) {
    unreachable!("RESP scan writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_object_array_item(_out: &mut BytesMut, _item: RedisObjectArrayItem<'_>) {
    unreachable!("RESP object writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn finish_object_array_visit(_out: &mut BytesMut, _outcome: RedisObjectReadOutcome) {
    unreachable!("RESP object writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn finish_object_bulk_visit(_out: &mut BytesMut, _outcome: RedisObjectReadOutcome) {
    unreachable!("RESP object writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn finish_object_integer_visit(
    _out: &mut BytesMut,
    _outcome: RedisObjectReadOutcome,
    _value: i64,
    _missing: i64,
) {
    unreachable!("RESP object writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_scan_object_array_item(_out: &mut BytesMut, _item: RedisObjectArrayItem<'_>) {
    unreachable!("RESP scan writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn finish_scan_object_array_visit(
    _out: &mut BytesMut,
    _outcome: RedisObjectReadOutcome,
) {
    unreachable!("RESP scan writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_fcnp_scan_fast_response(
    _store: &EmbeddedStore,
    _args: &[&[u8]],
    _out: &mut BytesMut,
) {
    unreachable!("FCNP scan writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_fcnp_scan_shard_fast_response(
    _store: &EmbeddedStore,
    _args: &[&[u8]],
    _out: &mut BytesMut,
) {
    unreachable!("FCNP scan writers are only called by the server feature")
}

pub(crate) struct KeyScanOptions<'a> {
    pub(crate) cursor: u64,
    pub(crate) count: usize,
    pub(crate) pattern: &'a [u8],
    type_filter: Option<&'a [u8]>,
}

impl<'a> KeyScanOptions<'a> {
    pub(crate) fn scan_type(&self) -> RedisKeyScanType<'a> {
        match self.type_filter {
            None => RedisKeyScanType::All,
            Some(kind) if kind.eq_ignore_ascii_case(b"string") => RedisKeyScanType::String,
            Some(kind) => RedisKeyScanType::Object(kind),
        }
    }
}

pub(crate) fn parse_key_scan_args<'a>(
    args: &'a [&'a [u8]],
    command_name: &str,
) -> std::result::Result<KeyScanOptions<'a>, Frame> {
    let Some(cursor) = args.first() else {
        return Err(wrong_arity(command_name));
    };
    let cursor = parse_u64(cursor).map_err(|_| error("ERR invalid cursor"))?;
    parse_key_scan_options(cursor, &args[1..])
}

fn parse_fcnp_scan_shard_args<'a>(
    args: &'a [&'a [u8]],
) -> std::result::Result<(usize, KeyScanOptions<'a>), Frame> {
    match args {
        [shard_id, cursor, rest @ ..] => {
            let shard_id = parse_usize(shard_id).map_err(|_| error("ERR invalid shard"))?;
            let cursor = parse_u64(cursor).map_err(|_| error("ERR invalid cursor"))?;
            parse_key_scan_options(cursor, rest).map(|options| (shard_id, options))
        }
        _ => Err(wrong_arity("FCNP.SCANSHARD")),
    }
}

fn parse_key_scan_options<'a>(
    cursor: u64,
    args: &'a [&'a [u8]],
) -> std::result::Result<KeyScanOptions<'a>, Frame> {
    let mut pattern: &[u8] = b"*";
    let mut type_filter = None;
    let mut count = DEFAULT_SCAN_COUNT;
    let mut index = 0;
    while index < args.len() {
        let option = args[index];
        match (option, args.get(index + 1)) {
            (option, Some(value)) if eq_ignore_ascii_case(option, b"MATCH") => {
                pattern = *value;
                index += 2;
            }
            (option, Some(value)) if eq_ignore_ascii_case(option, b"COUNT") => {
                count = parse_usize(value)
                    .map_err(|_| error("ERR value is not an integer or out of range"))?;
                index += 2;
            }
            (option, Some(value)) if eq_ignore_ascii_case(option, b"TYPE") => {
                type_filter = Some(*value);
                index += 2;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok(KeyScanOptions {
        cursor,
        count,
        pattern,
        type_filter,
    })
}

pub(crate) fn filter_key_pattern(keys: Vec<Vec<u8>>, pattern: &[u8]) -> Vec<Vec<u8>> {
    match pattern {
        b"*" => keys,
        pattern => keys
            .into_iter()
            .filter(|key| glob_matches(pattern, key))
            .collect(),
    }
}

pub(crate) fn key_pattern_matches(key: &[u8], pattern: &[u8]) -> bool {
    pattern == b"*" || glob_matches(pattern, key)
}
