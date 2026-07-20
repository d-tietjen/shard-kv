use bytes::BytesMut;
#[cfg(feature = "server")]
use std::cell::Cell;

use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::{RespProtocolVersion, ServerWire};
use crate::storage::{EmbeddedStore, RedisObjectResult};

use super::parse::format_score;

#[cfg(feature = "server")]
thread_local! {
    static CURRENT_RESP_PROTOCOL: Cell<RespProtocolVersion> =
        const { Cell::new(RespProtocolVersion::Resp2) };
}

#[cfg(feature = "server")]
struct RespProtocolScope {
    previous: RespProtocolVersion,
}

#[cfg(feature = "server")]
impl Drop for RespProtocolScope {
    fn drop(&mut self) {
        CURRENT_RESP_PROTOCOL.set(self.previous);
    }
}

#[cfg(feature = "server")]
pub(crate) fn with_resp_protocol<T>(protocol: RespProtocolVersion, f: impl FnOnce() -> T) -> T {
    let previous = CURRENT_RESP_PROTOCOL.replace(protocol);
    let _scope = RespProtocolScope { previous };
    f()
}

#[cfg(feature = "server")]
fn current_resp_protocol() -> RespProtocolVersion {
    CURRENT_RESP_PROTOCOL.get()
}

pub(crate) fn object_result(
    name: &str,
    args: &[&[u8]],
    arity: usize,
    op: impl FnOnce() -> RedisObjectResult,
) -> Frame {
    if args.len() != arity {
        return wrong_arity(name);
    }
    frame_from_result(op())
}

pub(crate) fn string_value(
    store: &EmbeddedStore,
    key: &[u8],
) -> std::result::Result<Vec<u8>, Frame> {
    match optional_string_value(store, key, true)? {
        Some(value) => Ok(value),
        None => Ok(Vec::new()),
    }
}

pub(crate) fn optional_string_value(
    store: &EmbeddedStore,
    key: &[u8],
    wrongtype_errors: bool,
) -> std::result::Result<Option<Vec<u8>>, Frame> {
    let mut value = None;
    match store.get_string_value_into(key, |bytes| value = Some(bytes.to_vec())) {
        crate::storage::RedisStringLookup::Hit => Ok(value),
        crate::storage::RedisStringLookup::Miss => Ok(None),
        crate::storage::RedisStringLookup::WrongType if wrongtype_errors => Err(wrongtype()),
        crate::storage::RedisStringLookup::WrongType => Ok(None),
    }
}

pub(crate) fn frame_from_result(result: RedisObjectResult) -> Frame {
    match result {
        RedisObjectResult::Simple("OK") => simple("OK"),
        RedisObjectResult::Simple(message) if message.starts_with("ERR ") => error(message),
        RedisObjectResult::Simple(message) => bulk(message.as_bytes().to_vec()),
        RedisObjectResult::Integer(value) => int(value),
        RedisObjectResult::IntegerArray(values) => {
            Frame::Array(values.into_iter().map(Frame::Integer).collect())
        }
        RedisObjectResult::Bulk(Some(value)) => bulk(value),
        RedisObjectResult::Bulk(None) => Frame::Null,
        RedisObjectResult::Array(values) => Frame::Array(
            values
                .into_iter()
                .map(|value| value.map_or(Frame::Null, bulk))
                .collect(),
        ),
        RedisObjectResult::WrongType => wrongtype(),
    }
}

pub(crate) fn scan_from_result(result: RedisObjectResult) -> Frame {
    match frame_from_result(result) {
        Frame::Array(items) => Frame::Array(vec![bulk(b"0".to_vec()), Frame::Array(items)]),
        other => other,
    }
}

pub(crate) fn scan_array(values: Vec<Vec<u8>>) -> Frame {
    scan_array_with_cursor(0, values)
}

pub(crate) fn scan_array_with_cursor(cursor: u64, values: Vec<Vec<u8>>) -> Frame {
    Frame::Array(vec![
        bulk(cursor.to_string().into_bytes()),
        array_bulk(values),
    ])
}

pub(crate) fn array_bulk(values: Vec<Vec<u8>>) -> Frame {
    Frame::Array(values.into_iter().map(bulk).collect())
}

pub(crate) fn zentries_frame(entries: Vec<(Vec<u8>, f64)>, with_scores: bool) -> Frame {
    if with_scores {
        array_bulk(zentries_flat(entries))
    } else {
        array_bulk(entries.into_iter().map(|(member, _)| member).collect())
    }
}

pub(crate) fn zentries_flat(entries: Vec<(Vec<u8>, f64)>) -> Vec<Vec<u8>> {
    entries
        .into_iter()
        .flat_map(|(member, score)| [member, format_score(score)])
        .collect()
}

pub(crate) fn wrong_arity(command: &str) -> Frame {
    error(&format!(
        "ERR wrong number of arguments for '{}' command",
        command.to_ascii_lowercase()
    ))
}

pub(crate) fn wrongtype() -> Frame {
    error(crate::storage::WRONGTYPE_MESSAGE)
}

pub(crate) fn error(message: &str) -> Frame {
    Frame::Error(message.into())
}

pub(crate) fn simple(message: &str) -> Frame {
    Frame::SimpleString(message.into())
}

pub(crate) fn bulk(value: Vec<u8>) -> Frame {
    Frame::BlobString(value)
}

pub(crate) fn int(value: i64) -> Frame {
    Frame::Integer(value)
}

#[cfg(feature = "server")]
pub(crate) fn write_fast_frame(out: &mut BytesMut, frame: &Frame) {
    match frame {
        Frame::SimpleString(value) => ServerWire::write_fast_value(out, value.as_bytes()),
        Frame::BlobString(value) => ServerWire::write_fast_value(out, value),
        Frame::Integer(value) => ServerWire::write_fast_integer(out, *value),
        Frame::Null => ServerWire::write_fast_null(out),
        Frame::Error(message) => ServerWire::write_fast_error(out, message),
        Frame::Array(_)
        | Frame::Map(_)
        | Frame::Set(_)
        | Frame::Push(_)
        | Frame::Boolean(_)
        | Frame::Double(_)
        | Frame::BigNumber(_)
        | Frame::VerbatimString { .. }
        | Frame::Attribute { .. } => {
            let mut resp = BytesMut::new();
            write_frame(&mut resp, frame);
            ServerWire::write_fast_value(out, &resp);
        }
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_frame(out: &mut BytesMut, frame: &Frame) {
    match frame {
        Frame::SimpleString(value) => {
            out.extend_from_slice(b"+");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::BlobString(value) => ServerWire::write_resp_blob_string(out, value),
        Frame::Integer(value) => ServerWire::write_resp_integer(out, *value),
        Frame::Array(items) => {
            write_resp_array_header(out, items.len());
            for item in items {
                write_frame(out, item);
            }
        }
        Frame::Map(items) => {
            out.extend_from_slice(b"%");
            let mut len_buf = itoa::Buffer::new();
            out.extend_from_slice(len_buf.format(items.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            for (key, value) in items {
                write_frame(out, key);
                write_frame(out, value);
            }
        }
        Frame::Set(items) => {
            out.extend_from_slice(b"~");
            let mut len_buf = itoa::Buffer::new();
            out.extend_from_slice(len_buf.format(items.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                write_frame(out, item);
            }
        }
        Frame::Push(items) => {
            out.extend_from_slice(b">");
            let mut len_buf = itoa::Buffer::new();
            out.extend_from_slice(len_buf.format(items.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                write_frame(out, item);
            }
        }
        Frame::Null => ServerWire::write_resp_null(out, current_resp_protocol()),
        Frame::Boolean(value) => {
            out.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" });
        }
        Frame::Double(value) => {
            out.extend_from_slice(b",");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::BigNumber(value) => {
            out.extend_from_slice(b"(");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Frame::VerbatimString { format, value } => {
            let mut len_buf = itoa::Buffer::new();
            out.extend_from_slice(b"=");
            out.extend_from_slice(len_buf.format(format.len() + 1 + value.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(format.as_bytes());
            out.extend_from_slice(b":");
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
        }
        Frame::Attribute { attributes, data } => {
            out.extend_from_slice(b"|");
            let mut len_buf = itoa::Buffer::new();
            out.extend_from_slice(len_buf.format(attributes.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            for (key, value) in attributes {
                write_frame(out, key);
                write_frame(out, value);
            }
            write_frame(out, data);
        }
        Frame::Error(message) => ServerWire::write_resp_error(out, message),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_result_resp(out: &mut BytesMut, result: RedisObjectResult) {
    match result {
        RedisObjectResult::Simple("OK") => out.extend_from_slice(b"+OK\r\n"),
        RedisObjectResult::Simple(message) if message.starts_with("ERR ") => {
            ServerWire::write_resp_error(out, message);
        }
        RedisObjectResult::Simple(message) => {
            ServerWire::write_resp_blob_string(out, message.as_bytes());
        }
        RedisObjectResult::Integer(value) => ServerWire::write_resp_integer(out, value),
        RedisObjectResult::IntegerArray(values) => {
            write_resp_array_header(out, values.len());
            for value in values {
                ServerWire::write_resp_integer(out, value);
            }
        }
        RedisObjectResult::Bulk(Some(value)) => ServerWire::write_resp_blob_string(out, &value),
        RedisObjectResult::Bulk(None) => write_resp_null(out),
        RedisObjectResult::Array(values) => {
            write_resp_array_header(out, values.len());
            for value in values {
                match value {
                    Some(value) => ServerWire::write_resp_blob_string(out, &value),
                    None => write_resp_null(out),
                }
            }
        }
        RedisObjectResult::WrongType => write_resp_wrongtype(out),
    }
}

#[cfg(feature = "server")]
#[inline(always)]
pub(crate) fn write_resp_null(out: &mut BytesMut) {
    ServerWire::write_resp_null(out, current_resp_protocol());
}

#[cfg(feature = "server")]
#[inline(always)]
pub(crate) fn write_resp_simple_string(out: &mut BytesMut, value: &str) {
    out.extend_from_slice(b"+");
    out.extend_from_slice(value.as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[cfg(feature = "server")]
pub(crate) fn write_resp_wrong_arity(out: &mut BytesMut, command: &str) {
    ServerWire::write_resp_error(
        out,
        &format!(
            "ERR wrong number of arguments for '{}' command",
            command.to_ascii_lowercase()
        ),
    );
}

#[cfg(feature = "server")]
#[inline(always)]
pub(crate) fn write_resp_wrongtype(out: &mut BytesMut) {
    ServerWire::write_resp_error(out, crate::storage::WRONGTYPE_MESSAGE);
}

#[cfg(feature = "server")]
pub(crate) fn write_resp_array_header(out: &mut BytesMut, len: usize) {
    out.extend_from_slice(b"*");
    let mut len_buf = itoa::Buffer::new();
    out.extend_from_slice(len_buf.format(len).as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[cfg(feature = "server")]
#[inline(always)]
pub(crate) fn reserve_resp_bulk_array_hint(out: &mut BytesMut, len: usize) {
    out.reserve(len.saturating_mul(16).saturating_add(16));
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_frame(_out: &mut BytesMut, _frame: &Frame) {
    unreachable!("RESP writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_result_resp(_out: &mut BytesMut, _result: RedisObjectResult) {
    unreachable!("RESP writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_resp_null(_out: &mut BytesMut) {
    unreachable!("RESP writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_resp_simple_string(_out: &mut BytesMut, _value: &str) {
    unreachable!("RESP writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_resp_wrong_arity(_out: &mut BytesMut, _command: &str) {
    unreachable!("RESP writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_resp_wrongtype(_out: &mut BytesMut) {
    unreachable!("RESP writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_resp_array_header(_out: &mut BytesMut, _len: usize) {
    unreachable!("RESP writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
#[inline(always)]
pub(crate) fn reserve_resp_bulk_array_hint(_out: &mut BytesMut, _len: usize) {
    unreachable!("RESP writers are only called by the server feature")
}
