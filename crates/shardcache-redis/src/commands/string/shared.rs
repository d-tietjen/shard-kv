use crate::storage::RedisStringStore;
use bytes::BytesMut;

use crate::commands::redis::{error, parse_i64, write_frame, wrongtype};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

pub(crate) fn append_value(
    store: &EmbeddedStore,
    key: &[u8],
    suffix: &[u8],
) -> std::result::Result<i64, Frame> {
    store.transform_string_value_no_ttl(
        key,
        |existing| {
            let existing = existing.unwrap_or_default();
            let mut value = Vec::with_capacity(existing.len().saturating_add(suffix.len()));
            value.extend_from_slice(existing);
            value.extend_from_slice(suffix);
            Ok((value.len() as i64, value))
        },
        wrongtype,
    )
}

pub(crate) fn strlen_value(store: &EmbeddedStore, key: &[u8]) -> std::result::Result<i64, ()> {
    let mut len = 0_i64;
    match store.get_string_value_into(key, |bytes| len = bytes.len() as i64) {
        RedisStringLookup::Hit => Ok(len),
        RedisStringLookup::Miss => Ok(0),
        RedisStringLookup::WrongType => Err(()),
    }
}

pub(crate) fn incrby_value(
    store: &EmbeddedStore,
    key: &[u8],
    delta: i64,
) -> std::result::Result<i64, Frame> {
    integer_transform_value(store, key, |current| current.checked_add(delta))
}

pub(crate) fn decrby_value(
    store: &EmbeddedStore,
    key: &[u8],
    decrement: i64,
) -> std::result::Result<i64, Frame> {
    integer_transform_value(store, key, |current| current.checked_sub(decrement))
}

fn integer_transform_value(
    store: &EmbeddedStore,
    key: &[u8],
    apply: impl FnOnce(i64) -> Option<i64>,
) -> std::result::Result<i64, Frame> {
    store.transform_string_value_no_ttl(
        key,
        |existing| {
            let current = match existing {
                None | Some(b"") => 0,
                Some(value) => match parse_i64(value) {
                    Ok(value) => value,
                    Err(_) => return Err(error("ERR value is not an integer or out of range")),
                },
            };
            let Some(next) = apply(current) else {
                return Err(error("ERR increment or decrement would overflow"));
            };
            let mut buffer = itoa::Buffer::new();
            Ok((next, buffer.format(next).as_bytes().to_vec()))
        },
        wrongtype,
    )
}

#[cfg(feature = "server")]
pub(crate) fn write_integer_result_resp(
    out: &mut BytesMut,
    result: std::result::Result<i64, Frame>,
) {
    match result {
        Ok(value) => ServerWire::write_resp_integer(out, value),
        Err(frame) => write_frame(out, &frame),
    }
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_integer_result_resp(
    _out: &mut BytesMut,
    _result: std::result::Result<i64, Frame>,
) {
    unreachable!("RESP integer writers are only called by the server feature")
}
