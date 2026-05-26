use std::fmt;
use std::ops::Range;

use crate::{FastCacheError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    SimpleString(String),
    BlobString(Vec<u8>),
    Integer(i64),
    Array(Vec<Frame>),
    Map(Vec<(Frame, Frame)>),
    Set(Vec<Frame>),
    Push(Vec<Frame>),
    Null,
    Boolean(bool),
    Double(String),
    BigNumber(String),
    VerbatimString {
        format: String,
        value: Vec<u8>,
    },
    Attribute {
        attributes: Vec<(Frame, Frame)>,
        data: Box<Frame>,
    },
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandFrame {
    pub parts: Vec<Vec<u8>>,
}

/// Inline storage for the borrowed command's parts. The benchmarked multi-key
/// shape is MGET with 8 keys or MSET with 8 key/value pairs, so inline enough
/// parts for those requests to avoid a per-command heap allocation.
pub type BorrowedCommandParts<'a> = smallvec::SmallVec<[&'a [u8]; 18]>;
pub type CommandPartSpans = smallvec::SmallVec<[Range<usize>; 18]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BorrowedCommandFrame<'a> {
    pub parts: BorrowedCommandParts<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpanFrame {
    pub parts: CommandPartSpans,
}

pub type RespDecodeResult = Option<(Frame, usize)>;
pub type RespCommandDecodeResult<'a> = Option<(BorrowedCommandFrame<'a>, usize)>;
pub type RespCommandSpanDecodeResult = Option<(CommandSpanFrame, usize)>;

#[derive(Debug, Default, Clone, Copy)]
pub struct RespCodec;

impl RespCodec {
    pub fn decode(buffer: &[u8]) -> Result<RespDecodeResult> {
        if buffer.is_empty() {
            return Ok(None);
        }
        parse_frame(buffer, 0)
    }

    pub fn encode(frame: &Frame, out: &mut Vec<u8>) {
        match frame {
            Frame::SimpleString(value) => {
                out.push(b'+');
                out.extend_from_slice(value.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::BlobString(value) => {
                let mut buf = itoa::Buffer::new();
                out.push(b'$');
                out.extend_from_slice(buf.format(value.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(value);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Integer(value) => {
                let mut buf = itoa::Buffer::new();
                out.push(b':');
                out.extend_from_slice(buf.format(*value).as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::Array(items) => {
                let mut buf = itoa::Buffer::new();
                out.push(b'*');
                out.extend_from_slice(buf.format(items.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    Self::encode(item, out);
                }
            }
            Frame::Map(items) => {
                let mut buf = itoa::Buffer::new();
                out.push(b'%');
                out.extend_from_slice(buf.format(items.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                for (key, value) in items {
                    Self::encode(key, out);
                    Self::encode(value, out);
                }
            }
            Frame::Set(items) => {
                let mut buf = itoa::Buffer::new();
                out.push(b'~');
                out.extend_from_slice(buf.format(items.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    Self::encode(item, out);
                }
            }
            Frame::Push(items) => {
                let mut buf = itoa::Buffer::new();
                out.push(b'>');
                out.extend_from_slice(buf.format(items.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    Self::encode(item, out);
                }
            }
            Frame::Null => {
                out.extend_from_slice(b"_\r\n");
            }
            Frame::Boolean(value) => {
                out.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" });
            }
            Frame::Double(value) => {
                out.push(b',');
                out.extend_from_slice(value.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::BigNumber(value) => {
                out.push(b'(');
                out.extend_from_slice(value.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::VerbatimString { format, value } => {
                let len = format.len() + 1 + value.len();
                let mut buf = itoa::Buffer::new();
                out.push(b'=');
                out.extend_from_slice(buf.format(len).as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(format.as_bytes());
                out.push(b':');
                out.extend_from_slice(value);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Attribute { attributes, data } => {
                let mut buf = itoa::Buffer::new();
                out.push(b'|');
                out.extend_from_slice(buf.format(attributes.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                for (key, value) in attributes {
                    Self::encode(key, out);
                    Self::encode(value, out);
                }
                Self::encode(data, out);
            }
            Frame::Error(message) => {
                out.push(b'-');
                out.extend_from_slice(message.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
        }
    }

    pub fn decode_command(buffer: &[u8]) -> Result<RespCommandDecodeResult<'_>> {
        if buffer.is_empty() {
            return Ok(None);
        }
        parse_command_frame(buffer, 0)
    }

    pub fn decode_command_spans(buffer: &[u8]) -> Result<RespCommandSpanDecodeResult> {
        if buffer.is_empty() {
            return Ok(None);
        }
        parse_command_span_frame(buffer, 0)
    }

    pub fn as_command(frame: Frame) -> Result<CommandFrame> {
        match frame {
            Frame::Array(parts) => {
                let mut output = Vec::with_capacity(parts.len());
                for part in parts {
                    match part {
                        Frame::BlobString(bytes) => output.push(bytes),
                        Frame::SimpleString(text) => output.push(text.into_bytes()),
                        Frame::Integer(value) => output.push(value.to_string().into_bytes()),
                        other => {
                            return Err(FastCacheError::Protocol(format!(
                                "command arrays may only contain bulk strings, simple strings, or integers; got {other:?}"
                            )));
                        }
                    }
                }
                Ok(CommandFrame { parts: output })
            }
            Frame::Attribute { data, .. } => Self::as_command(*data),
            other => Err(FastCacheError::Protocol(format!(
                "expected command array, got {other:?}"
            ))),
        }
    }
}

fn parse_command_frame(buffer: &[u8], offset: usize) -> Result<RespCommandDecodeResult<'_>> {
    if offset >= buffer.len() {
        return Ok(None);
    }
    if buffer[offset] != b'*' {
        return Err(FastCacheError::Protocol(
            "expected RESP array for command frame".into(),
        ));
    }
    let Some((count, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if count < 0 {
        return Err(FastCacheError::Protocol(
            "null command arrays are not supported".into(),
        ));
    }

    let mut cursor = offset + 1 + header_consumed;
    let mut parts: BorrowedCommandParts<'_> = smallvec::SmallVec::with_capacity(count as usize);
    for _ in 0..count as usize {
        let Some((part, consumed)) = parse_command_part(buffer, cursor)? else {
            return Ok(None);
        };
        parts.push(part);
        cursor += consumed;
    }

    Ok(Some((BorrowedCommandFrame { parts }, cursor - offset)))
}

fn parse_command_span_frame(buffer: &[u8], offset: usize) -> Result<RespCommandSpanDecodeResult> {
    if offset >= buffer.len() {
        return Ok(None);
    }
    if buffer[offset] != b'*' {
        return Err(FastCacheError::Protocol(
            "expected RESP array for command frame".into(),
        ));
    }
    let Some((count, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if count < 0 {
        return Err(FastCacheError::Protocol(
            "null command arrays are not supported".into(),
        ));
    }

    let mut cursor = offset + 1 + header_consumed;
    let mut parts = CommandPartSpans::with_capacity(count as usize);
    for _ in 0..count as usize {
        let Some((part, consumed)) = parse_command_part_span(buffer, cursor)? else {
            return Ok(None);
        };
        parts.push(part);
        cursor += consumed;
    }

    Ok(Some((CommandSpanFrame { parts }, cursor - offset)))
}

fn parse_command_part(buffer: &[u8], offset: usize) -> Result<Option<(&[u8], usize)>> {
    if offset >= buffer.len() {
        return Ok(None);
    }

    match buffer[offset] {
        b'$' => parse_command_blob_string(buffer, offset),
        b'+' => parse_command_simple_string(buffer, offset),
        b':' => parse_command_integer(buffer, offset),
        other => Err(FastCacheError::Protocol(format!(
            "unsupported RESP command part prefix byte: {other:#x}"
        ))),
    }
}

fn parse_command_part_span(buffer: &[u8], offset: usize) -> Result<Option<(Range<usize>, usize)>> {
    if offset >= buffer.len() {
        return Ok(None);
    }

    match buffer[offset] {
        b'$' => parse_command_blob_string_span(buffer, offset),
        b'+' => parse_command_line_span(buffer, offset),
        b':' => parse_command_line_span(buffer, offset),
        other => Err(FastCacheError::Protocol(format!(
            "unsupported RESP command part prefix byte: {other:#x}"
        ))),
    }
}

fn parse_command_blob_string(buffer: &[u8], offset: usize) -> Result<Option<(&[u8], usize)>> {
    let Some((length, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if length < 0 {
        return Err(FastCacheError::Protocol(
            "null bulk strings are not supported in command frames".into(),
        ));
    }
    let length = length as usize;
    let start = offset + 1 + header_consumed;
    let end = start + length;
    if buffer.len() < end + 2 {
        return Ok(None);
    }
    if &buffer[end..end + 2] != b"\r\n" {
        return Err(FastCacheError::Protocol(
            "blob string missing CRLF terminator".into(),
        ));
    }
    Ok(Some((&buffer[start..end], (end + 2) - offset)))
}

fn parse_command_blob_string_span(
    buffer: &[u8],
    offset: usize,
) -> Result<Option<(Range<usize>, usize)>> {
    let Some((length, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if length < 0 {
        return Err(FastCacheError::Protocol(
            "null bulk strings are not supported in command frames".into(),
        ));
    }
    let length = length as usize;
    let start = offset + 1 + header_consumed;
    let end = start + length;
    if buffer.len() < end + 2 {
        return Ok(None);
    }
    if &buffer[end..end + 2] != b"\r\n" {
        return Err(FastCacheError::Protocol(
            "blob string missing CRLF terminator".into(),
        ));
    }
    Ok(Some((start..end, (end + 2) - offset)))
}

fn parse_command_simple_string(buffer: &[u8], offset: usize) -> Result<Option<(&[u8], usize)>> {
    let Some(line_end) = find_crlf(&buffer[offset + 1..]) else {
        return Ok(None);
    };
    let start = offset + 1;
    let end = start + line_end;
    Ok(Some((&buffer[start..end], end + 2 - offset)))
}

fn parse_command_line_span(buffer: &[u8], offset: usize) -> Result<Option<(Range<usize>, usize)>> {
    let Some(line_end) = find_crlf(&buffer[offset + 1..]) else {
        return Ok(None);
    };
    let start = offset + 1;
    let end = start + line_end;
    Ok(Some((start..end, end + 2 - offset)))
}

fn parse_command_integer(buffer: &[u8], offset: usize) -> Result<Option<(&[u8], usize)>> {
    let Some(line_end) = find_crlf(&buffer[offset + 1..]) else {
        return Ok(None);
    };
    let start = offset + 1;
    let end = start + line_end;
    Ok(Some((&buffer[start..end], end + 2 - offset)))
}

fn parse_frame(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    if offset >= buffer.len() {
        return Ok(None);
    }
    match buffer[offset] {
        b'+' => parse_simple_string(buffer, offset),
        b'-' => parse_error(buffer, offset),
        b'!' => parse_blob_error(buffer, offset),
        b':' => parse_integer(buffer, offset),
        b'$' => parse_blob_string(buffer, offset),
        b'*' => parse_array(buffer, offset),
        b'%' => parse_map(buffer, offset),
        b'~' => parse_set(buffer, offset),
        b'>' => parse_push(buffer, offset),
        b'_' => parse_null(buffer, offset),
        b'#' => parse_boolean(buffer, offset),
        b',' => parse_double(buffer, offset),
        b'(' => parse_big_number(buffer, offset),
        b'=' => parse_verbatim_string(buffer, offset),
        b'|' => parse_attribute(buffer, offset),
        other => Err(FastCacheError::Protocol(format!(
            "unsupported RESP prefix byte: {other:#x}"
        ))),
    }
}

fn parse_simple_string(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((line, consumed)) = parse_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    Ok(Some((Frame::SimpleString(line.to_string()), consumed + 1)))
}

fn parse_error(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((line, consumed)) = parse_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    Ok(Some((Frame::Error(line.to_string()), consumed + 1)))
}

fn parse_blob_error(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((length, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if length < 0 {
        return Err(FastCacheError::Protocol(
            "RESP3 blob errors cannot be null".into(),
        ));
    }
    let length = length as usize;
    let start = offset + 1 + header_consumed;
    let end = start + length;
    if buffer.len() < end + 2 {
        return Ok(None);
    }
    if &buffer[end..end + 2] != b"\r\n" {
        return Err(FastCacheError::Protocol(
            "blob error missing CRLF terminator".into(),
        ));
    }
    let message = std::str::from_utf8(&buffer[start..end])
        .map_err(|error| FastCacheError::Protocol(format!("invalid utf8 in RESP error: {error}")))?
        .to_string();
    Ok(Some((Frame::Error(message), (end + 2) - offset)))
}

fn parse_integer(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((value, consumed)) = parse_i64_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    Ok(Some((Frame::Integer(value), consumed + 1)))
}

fn parse_blob_string(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((length, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if length < 0 {
        return Ok(Some((Frame::Null, header_consumed + 1)));
    }
    let length = length as usize;
    let start = offset + 1 + header_consumed;
    let end = start + length;
    if buffer.len() < end + 2 {
        return Ok(None);
    }
    if &buffer[end..end + 2] != b"\r\n" {
        return Err(FastCacheError::Protocol(
            "blob string missing CRLF terminator".into(),
        ));
    }
    Ok(Some((
        Frame::BlobString(buffer[start..end].to_vec()),
        (end + 2) - offset,
    )))
}

fn parse_array(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((count, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if count < 0 {
        return Ok(Some((Frame::Null, header_consumed + 1)));
    }
    let count = count as usize;
    let mut cursor = offset + 1 + header_consumed;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let Some((frame, consumed)) = parse_frame(buffer, cursor)? else {
            return Ok(None);
        };
        items.push(frame);
        cursor += consumed;
    }
    Ok(Some((Frame::Array(items), cursor - offset)))
}

fn parse_map(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((count, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if count < 0 {
        return Err(FastCacheError::Protocol("RESP3 maps cannot be null".into()));
    }
    let count = count as usize;
    let mut cursor = offset + 1 + header_consumed;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let Some((key, consumed)) = parse_frame(buffer, cursor)? else {
            return Ok(None);
        };
        cursor += consumed;
        let Some((value, consumed)) = parse_frame(buffer, cursor)? else {
            return Ok(None);
        };
        cursor += consumed;
        items.push((key, value));
    }
    Ok(Some((Frame::Map(items), cursor - offset)))
}

fn parse_set(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((count, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if count < 0 {
        return Err(FastCacheError::Protocol("RESP3 sets cannot be null".into()));
    }
    let count = count as usize;
    let mut cursor = offset + 1 + header_consumed;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let Some((frame, consumed)) = parse_frame(buffer, cursor)? else {
            return Ok(None);
        };
        items.push(frame);
        cursor += consumed;
    }
    Ok(Some((Frame::Set(items), cursor - offset)))
}

fn parse_push(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((count, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if count < 0 {
        return Err(FastCacheError::Protocol(
            "RESP3 pushes cannot be null".into(),
        ));
    }
    let count = count as usize;
    let mut cursor = offset + 1 + header_consumed;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let Some((frame, consumed)) = parse_frame(buffer, cursor)? else {
            return Ok(None);
        };
        items.push(frame);
        cursor += consumed;
    }
    Ok(Some((Frame::Push(items), cursor - offset)))
}

fn parse_null(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    if buffer.len() < offset + 3 {
        return Ok(None);
    }
    if &buffer[offset + 1..offset + 3] != b"\r\n" {
        return Err(FastCacheError::Protocol("invalid null frame".into()));
    }
    Ok(Some((Frame::Null, 3)))
}

fn parse_boolean(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    if buffer.len() < offset + 4 {
        return Ok(None);
    }
    let value = match buffer[offset + 1] {
        b't' => true,
        b'f' => false,
        other => {
            return Err(FastCacheError::Protocol(format!(
                "invalid boolean marker: {other:#x}"
            )));
        }
    };
    if &buffer[offset + 2..offset + 4] != b"\r\n" {
        return Err(FastCacheError::Protocol("invalid boolean frame".into()));
    }
    Ok(Some((Frame::Boolean(value), 4)))
}

fn parse_double(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((line, consumed)) = parse_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if !matches!(line, "inf" | "+inf" | "-inf" | "nan") {
        line.parse::<f64>().map_err(|error| {
            FastCacheError::Protocol(format!("invalid RESP3 double value: {error}"))
        })?;
    }
    Ok(Some((Frame::Double(line.to_string()), consumed + 1)))
}

fn parse_big_number(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((line, consumed)) = parse_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    let digits = match line.as_bytes().first() {
        Some(b'+') | Some(b'-') => &line.as_bytes()[1..],
        _ => line.as_bytes(),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(FastCacheError::Protocol(
            "invalid RESP3 big number value".into(),
        ));
    }
    Ok(Some((Frame::BigNumber(line.to_string()), consumed + 1)))
}

fn parse_verbatim_string(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((length, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if length < 4 {
        return Err(FastCacheError::Protocol(
            "RESP3 verbatim string must include a format prefix".into(),
        ));
    }
    let length = length as usize;
    let start = offset + 1 + header_consumed;
    let end = start + length;
    if buffer.len() < end + 2 {
        return Ok(None);
    }
    if &buffer[end..end + 2] != b"\r\n" {
        return Err(FastCacheError::Protocol(
            "verbatim string missing CRLF terminator".into(),
        ));
    }
    let payload = &buffer[start..end];
    let colon = payload
        .iter()
        .position(|byte| *byte == b':')
        .ok_or_else(|| {
            FastCacheError::Protocol("RESP3 verbatim string missing format separator".into())
        })?;
    if colon != 3 {
        return Err(FastCacheError::Protocol(
            "RESP3 verbatim string format must be exactly 3 bytes".into(),
        ));
    }
    let format = std::str::from_utf8(&payload[..colon])
        .map_err(|error| {
            FastCacheError::Protocol(format!("invalid utf8 in RESP3 verbatim format: {error}"))
        })?
        .to_string();
    Ok(Some((
        Frame::VerbatimString {
            format,
            value: payload[colon + 1..].to_vec(),
        },
        (end + 2) - offset,
    )))
}

fn parse_attribute(buffer: &[u8], offset: usize) -> Result<RespDecodeResult> {
    let Some((Frame::Map(attributes), consumed)) = parse_map_with_prefix(buffer, offset, b'|')?
    else {
        return Ok(None);
    };
    let cursor = offset + consumed;
    let Some((data, data_consumed)) = parse_frame(buffer, cursor)? else {
        return Ok(None);
    };
    Ok(Some((
        Frame::Attribute {
            attributes,
            data: Box::new(data),
        },
        consumed + data_consumed,
    )))
}

fn parse_map_with_prefix(
    buffer: &[u8],
    offset: usize,
    prefix: u8,
) -> Result<Option<(Frame, usize)>> {
    debug_assert_eq!(buffer[offset], prefix);
    let Some((count, header_consumed)) = parse_isize_line(&buffer[offset + 1..])? else {
        return Ok(None);
    };
    if count < 0 {
        return Err(FastCacheError::Protocol(
            "RESP3 attributes cannot be null".into(),
        ));
    }
    let count = count as usize;
    let mut cursor = offset + 1 + header_consumed;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let Some((key, consumed)) = parse_frame(buffer, cursor)? else {
            return Ok(None);
        };
        cursor += consumed;
        let Some((value, consumed)) = parse_frame(buffer, cursor)? else {
            return Ok(None);
        };
        cursor += consumed;
        items.push((key, value));
    }
    Ok(Some((Frame::Map(items), cursor - offset)))
}

fn parse_line(buffer: &[u8]) -> Result<Option<(&str, usize)>> {
    let Some(end) = find_crlf(buffer) else {
        return Ok(None);
    };
    let line = std::str::from_utf8(&buffer[..end])
        .map_err(|error| FastCacheError::Protocol(format!("invalid utf8 in RESP line: {error}")))?;
    Ok(Some((line, end + 2)))
}

#[inline]
fn find_crlf(buffer: &[u8]) -> Option<usize> {
    memchr::memmem::find(buffer, b"\r\n")
}

#[inline]
fn parse_isize_line(buffer: &[u8]) -> Result<Option<(isize, usize)>> {
    // Fast path: 1-4 digit non-negative integers terminated by `\r\n`. This
    // covers the overwhelming majority of RESP integers in practice (array
    // counts, blob string lengths up to 9999) without invoking memchr at all.
    if let Some((value, consumed)) = try_parse_short_uint_line(buffer) {
        return Ok(Some((value as isize, consumed)));
    }
    let Some(end) = find_crlf(buffer) else {
        return Ok(None);
    };
    let value = parse_ascii_isize(&buffer[..end])?;
    Ok(Some((value, end + 2)))
}

/// Parses RESP integer headers of 1-4 ASCII digits followed by `\r\n` without
/// invoking `memchr`. Returns `Some((value, consumed))` when the header is in
/// that range, `None` otherwise. Doesn't validate digits beyond range — the
/// general path will report errors if this returns `None`.
#[inline(always)]
fn try_parse_short_uint_line(buffer: &[u8]) -> Option<(usize, usize)> {
    if buffer.len() < 3 {
        return None;
    }
    let b0 = buffer[0];
    if !b0.is_ascii_digit() {
        return None;
    }
    let d0 = (b0 - b'0') as usize;
    // 1 digit
    if buffer[1] == b'\r' && buffer[2] == b'\n' {
        return Some((d0, 3));
    }
    if buffer.len() < 4 {
        return None;
    }
    let b1 = buffer[1];
    if !b1.is_ascii_digit() {
        return None;
    }
    let d1 = (b1 - b'0') as usize;
    // 2 digits
    if buffer[2] == b'\r' && buffer[3] == b'\n' {
        return Some((d0 * 10 + d1, 4));
    }
    if buffer.len() < 5 {
        return None;
    }
    let b2 = buffer[2];
    if !b2.is_ascii_digit() {
        return None;
    }
    let d2 = (b2 - b'0') as usize;
    // 3 digits
    if buffer[3] == b'\r' && buffer[4] == b'\n' {
        return Some((d0 * 100 + d1 * 10 + d2, 5));
    }
    if buffer.len() < 6 {
        return None;
    }
    let b3 = buffer[3];
    if !b3.is_ascii_digit() {
        return None;
    }
    let d3 = (b3 - b'0') as usize;
    // 4 digits
    if buffer[4] == b'\r' && buffer[5] == b'\n' {
        return Some((d0 * 1000 + d1 * 100 + d2 * 10 + d3, 6));
    }
    None
}

#[inline]
fn parse_i64_line(buffer: &[u8]) -> Result<Option<(i64, usize)>> {
    let Some(end) = find_crlf(buffer) else {
        return Ok(None);
    };
    let value = parse_ascii_i64(&buffer[..end])?;
    Ok(Some((value, end + 2)))
}

#[inline]
fn parse_ascii_isize(bytes: &[u8]) -> Result<isize> {
    let (negative, digits) = split_sign(bytes)?;
    if digits.is_empty() {
        return Err(FastCacheError::Protocol(
            "empty integer in RESP header".into(),
        ));
    }
    let mut value: isize = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return Err(FastCacheError::Protocol(format!(
                "non-digit byte in RESP integer: {b:#x}"
            )));
        }
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as isize))
            .ok_or_else(|| FastCacheError::Protocol("RESP integer overflow".into()))?;
    }
    Ok(if negative { -value } else { value })
}

#[inline]
fn parse_ascii_i64(bytes: &[u8]) -> Result<i64> {
    let (negative, digits) = split_sign(bytes)?;
    if digits.is_empty() {
        return Err(FastCacheError::Protocol(
            "empty integer in RESP header".into(),
        ));
    }
    let mut value: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return Err(FastCacheError::Protocol(format!(
                "non-digit byte in RESP integer: {b:#x}"
            )));
        }
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as i64))
            .ok_or_else(|| FastCacheError::Protocol("RESP integer overflow".into()))?;
    }
    Ok(if negative { -value } else { value })
}

#[inline]
fn split_sign(bytes: &[u8]) -> Result<(bool, &[u8])> {
    Ok(match bytes.first() {
        Some(b'-') => (true, &bytes[1..]),
        Some(b'+') => (false, &bytes[1..]),
        _ => (false, bytes),
    })
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Frame::SimpleString(value) => write!(f, "{value}"),
            Frame::BlobString(value) => write!(f, "{}", String::from_utf8_lossy(value)),
            Frame::Integer(value) => write!(f, "{value}"),
            Frame::Array(value) => write!(f, "{value:?}"),
            Frame::Map(value) => write!(f, "{value:?}"),
            Frame::Set(value) => write!(f, "{value:?}"),
            Frame::Push(value) => write!(f, "{value:?}"),
            Frame::Null => write!(f, "null"),
            Frame::Boolean(value) => write!(f, "{value}"),
            Frame::Double(value) => write!(f, "{value}"),
            Frame::BigNumber(value) => write!(f, "{value}"),
            Frame::VerbatimString { format, value } => {
                write!(f, "{format}:{}", String::from_utf8_lossy(value))
            }
            Frame::Attribute { data, .. } => write!(f, "{data}"),
            Frame::Error(value) => write!(f, "ERR {value}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Frame, RespCodec};

    #[test]
    fn round_trips_array() {
        let frame = Frame::Array(vec![
            Frame::BlobString(b"SET".to_vec()),
            Frame::BlobString(b"alpha".to_vec()),
            Frame::BlobString(b"beta".to_vec()),
        ]);
        let mut encoded = Vec::new();
        RespCodec::encode(&frame, &mut encoded);
        let decoded = RespCodec::decode(&encoded).unwrap().unwrap().0;
        assert_eq!(decoded, frame);
    }

    #[test]
    fn decodes_command_part_spans() {
        let frame = Frame::Array(vec![
            Frame::BlobString(b"MSET".to_vec()),
            Frame::BlobString(b"long-key-name".to_vec()),
            Frame::BlobString(b"value-body".to_vec()),
        ]);
        let mut encoded = Vec::new();
        RespCodec::encode(&frame, &mut encoded);

        let (spans, consumed) = RespCodec::decode_command_spans(&encoded).unwrap().unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(&encoded[spans.parts[0].clone()], b"MSET");
        assert_eq!(&encoded[spans.parts[1].clone()], b"long-key-name");
        assert_eq!(&encoded[spans.parts[2].clone()], b"value-body");
    }
}
