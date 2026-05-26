use std::cmp::Ordering;

#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    array_bulk, bulk, eq_ignore_ascii_case, error, int, optional_string_value, parse_u64,
    parse_usize, simple, wrong_arity, wrongtype,
};
#[cfg(feature = "server")]
use crate::commands::redis::{
    write_frame, write_resp_array_header, write_resp_null, write_resp_simple_string,
    write_resp_wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringStore, now_millis};

const STREAM_PREFIX: &[u8] = b"FC:STREAM:v1\0";
const LAST_ID_MS_OFFSET: usize = STREAM_PREFIX.len();
const LAST_ID_SEQ_OFFSET: usize = LAST_ID_MS_OFFSET + 8;
const ENTRY_COUNT_OFFSET: usize = LAST_ID_SEQ_OFFSET + 8;
const STREAM_HEADER_LEN: usize = ENTRY_COUNT_OFFSET + 4;

macro_rules! define_stream_command {
    ($type:ident, $static_name:ident, $name:literal, $mutates:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static $static_name: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;
        }
    };
}

define_stream_command!(XAck, XACK_COMMAND, "XACK", true);
define_stream_command!(XAdd, XADD_COMMAND, "XADD", true);
define_stream_command!(XClaim, XCLAIM_COMMAND, "XCLAIM", true);
define_stream_command!(XDel, XDEL_COMMAND, "XDEL", true);
define_stream_command!(XGroup, XGROUP_COMMAND, "XGROUP", true);
define_stream_command!(XInfo, XINFO_COMMAND, "XINFO", false);
define_stream_command!(XLen, XLEN_COMMAND, "XLEN", false);
define_stream_command!(XPending, XPENDING_COMMAND, "XPENDING", false);
define_stream_command!(XRange, XRANGE_COMMAND, "XRANGE", false);
define_stream_command!(XRead, XREAD_COMMAND, "XREAD", false);
define_stream_command!(XReadGroup, XREADGROUP_COMMAND, "XREADGROUP", true);
define_stream_command!(XRevRange, XREVRANGE_COMMAND, "XREVRANGE", false);
define_stream_command!(XSetId, XSETID_COMMAND, "XSETID", true);
define_stream_command!(XTrim, XTRIM_COMMAND, "XTRIM", true);

impl crate::commands::redis::RedisCommand for XAdd {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xadd_update(store, args)
            .map(|id| bulk(id.to_string().into_bytes()))
            .unwrap_or_else(|frame| frame)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_stream_result(out, xadd_update(store, args), write_stream_id_bulk_resp);
    }
}

fn xadd_update(store: &EmbeddedStore, args: &[&[u8]]) -> Result<StreamId, Frame> {
    if args.len() < 4 {
        return Err(wrong_arity("XADD"));
    }
    let key = args[0];
    let mut index = 1;
    let mut trim = None;
    if args
        .get(index)
        .is_some_and(|arg| eq_ignore_ascii_case(arg, b"MAXLEN"))
    {
        index += 1;
        let mut approximate = false;
        if args
            .get(index)
            .is_some_and(|arg| eq_ignore_ascii_case(arg, b"~"))
        {
            approximate = true;
            index += 1;
        } else if args
            .get(index)
            .is_some_and(|arg| eq_ignore_ascii_case(arg, b"="))
        {
            index += 1;
        }
        let Some(count) = args.get(index) else {
            return Err(error("ERR syntax error"));
        };
        let Ok(count) = parse_usize(count) else {
            return Err(error("ERR value is not an integer or out of range"));
        };
        trim = Some(StreamTrim {
            max_len: count,
            approximate,
        });
        index += 1;
    }
    let Some(id_arg) = args.get(index) else {
        return Err(wrong_arity("XADD"));
    };
    index += 1;
    if index >= args.len() || (args.len() - index) % 2 != 0 {
        return Err(wrong_arity("XADD"));
    }
    let fields = args[index..]
        .chunks_exact(2)
        .map(|chunk| (chunk[0], chunk[1]))
        .collect::<Vec<_>>();
    let result = store.transform_string_value_no_ttl(
        key,
        |existing| {
            if let Some((id, value)) = try_fast_append_stream(existing, id_arg, &fields, trim)? {
                return Ok((id, value));
            }
            let mut stream = decode_stream(existing)?;
            let id = next_stream_id(stream.last_id, id_arg)?;
            stream.entries.push(StreamEntry {
                id,
                fields: fields
                    .iter()
                    .map(|(field, value)| ((*field).to_vec(), (*value).to_vec()))
                    .collect(),
            });
            stream.last_id = id;
            if let Some(trim) = trim {
                trim_stream(&mut stream, trim.max_len);
            }
            Ok((id, encode_stream(&stream)))
        },
        wrongtype,
    );
    result
}

impl crate::commands::redis::RedisCommand for XLen {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => match load_stream_len(store, key) {
                Ok(len) => int(len as i64),
                Err(frame) => frame,
            },
            _ => wrong_arity("XLEN"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => write_stream_result(out, load_stream_len(store, key), |out, len| {
                ServerWire::write_resp_integer(out, len as i64);
            }),
            _ => write_resp_wrong_arity(out, "XLEN"),
        }
    }
}

impl crate::commands::redis::RedisCommand for XRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xrange(store, args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_xrange_resp(store, args, false, out);
    }
}

impl crate::commands::redis::RedisCommand for XRevRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xrange(store, args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_xrange_resp(store, args, true, out);
    }
}

impl crate::commands::redis::RedisCommand for XDel {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xdel_update(store, args)
            .map(|deleted| int(deleted as i64))
            .unwrap_or_else(|frame| frame)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_stream_result(out, xdel_update(store, args), |out, deleted| {
            ServerWire::write_resp_integer(out, deleted as i64);
        });
    }
}

fn xdel_update(store: &EmbeddedStore, args: &[&[u8]]) -> Result<usize, Frame> {
    let [key, ids @ ..] = args else {
        return Err(wrong_arity("XDEL"));
    };
    if ids.is_empty() {
        return Err(wrong_arity("XDEL"));
    }
    with_stream_mut(store, key, |stream| {
        let ids = ids
            .iter()
            .map(|raw| parse_stream_id(raw))
            .collect::<Result<Vec<_>, _>>()?;
        let before = stream.entries.len();
        stream.entries.retain(|entry| !ids.contains(&entry.id));
        Ok(before.saturating_sub(stream.entries.len()))
    })
}

impl crate::commands::redis::RedisCommand for XTrim {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xtrim_update(store, args)
            .map(|removed| int(removed as i64))
            .unwrap_or_else(|frame| frame)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_stream_result(out, xtrim_update(store, args), |out, removed| {
            ServerWire::write_resp_integer(out, removed as i64);
        });
    }
}

fn xtrim_update(store: &EmbeddedStore, args: &[&[u8]]) -> Result<usize, Frame> {
    if args.len() < 3 || !eq_ignore_ascii_case(args[1], b"MAXLEN") {
        return Err(wrong_arity("XTRIM"));
    }
    let count_index = if args.get(2).is_some_and(|arg| *arg == b"~") {
        3
    } else {
        2
    };
    let Some(count) = args.get(count_index) else {
        return Err(wrong_arity("XTRIM"));
    };
    let Ok(max_len) = parse_usize(count) else {
        return Err(error("ERR value is not an integer or out of range"));
    };
    with_stream_mut(store, args[0], |stream| {
        let removed = trim_stream(stream, max_len);
        Ok(removed)
    })
}

impl crate::commands::redis::RedisCommand for XSetId {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, id] => {
                let id = match parse_stream_id(id) {
                    Ok(id) => id,
                    Err(frame) => return frame,
                };
                set_stream_last_id(store, key, id)
            }
            _ => wrong_arity("XSETID"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, id] => {
                match parse_stream_id(id).and_then(|id| set_stream_last_id_result(store, key, id)) {
                    Ok(()) => write_resp_simple_string(out, "OK"),
                    Err(frame) => write_frame(out, &frame),
                }
            }
            _ => write_resp_wrong_arity(out, "XSETID"),
        }
    }
}

impl crate::commands::redis::RedisCommand for XRead {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xread(store, args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_xread_resp(store, args, false, out);
    }
}

impl crate::commands::redis::RedisCommand for XReadGroup {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xread(store, args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_xread_resp(store, args, true, out);
    }
}

impl crate::commands::redis::RedisCommand for XGroup {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub, key, _group, id, tail @ ..] if eq_ignore_ascii_case(sub, b"CREATE") => {
                let mkstream = tail
                    .iter()
                    .any(|arg| eq_ignore_ascii_case(arg, b"MKSTREAM"));
                if mkstream || store.exists(key) {
                    let id = match parse_stream_id(id) {
                        Ok(id) => id,
                        Err(frame) => return frame,
                    };
                    set_stream_last_id(store, key, id)
                } else {
                    error("ERR The XGROUP subcommand requires the key to exist")
                }
            }
            [sub, key, _group, id] if eq_ignore_ascii_case(sub, b"SETID") => {
                let id = match parse_stream_id(id) {
                    Ok(id) => id,
                    Err(frame) => return frame,
                };
                set_stream_last_id(store, key, id)
            }
            [sub, _key, _group] if eq_ignore_ascii_case(sub, b"DESTROY") => int(0),
            [sub, _key, _group, _consumer] if eq_ignore_ascii_case(sub, b"DELCONSUMER") => int(0),
            [sub, _key, _group, _consumer] if eq_ignore_ascii_case(sub, b"CREATECONSUMER") => {
                int(1)
            }
            [sub] if eq_ignore_ascii_case(sub, b"HELP") => array_bulk(vec![
                b"XGROUP CREATE key group id [MKSTREAM]".to_vec(),
                b"XGROUP SETID key group id".to_vec(),
                b"XGROUP DESTROY key group".to_vec(),
            ]),
            _ => error("ERR unknown XGROUP subcommand or wrong number of arguments"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_xgroup_resp(store, args, out);
    }
}

impl crate::commands::redis::RedisCommand for XInfo {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub, key] if eq_ignore_ascii_case(sub, b"STREAM") => match load_stream(store, key) {
                Ok(stream) => Frame::Array(vec![
                    bulk(b"length".to_vec()),
                    int(stream.entries.len() as i64),
                    bulk(b"radix-tree-keys".to_vec()),
                    int(0),
                    bulk(b"radix-tree-nodes".to_vec()),
                    int(0),
                    bulk(b"groups".to_vec()),
                    int(0),
                    bulk(b"last-generated-id".to_vec()),
                    bulk(stream.last_id.to_string().into_bytes()),
                    bulk(b"first-entry".to_vec()),
                    stream
                        .entries
                        .first()
                        .map(entry_frame)
                        .unwrap_or(Frame::Null),
                    bulk(b"last-entry".to_vec()),
                    stream
                        .entries
                        .last()
                        .map(entry_frame)
                        .unwrap_or(Frame::Null),
                ]),
                Err(frame) => frame,
            },
            [sub, _key] if eq_ignore_ascii_case(sub, b"GROUPS") => Frame::Array(Vec::new()),
            [sub, _key, _group] if eq_ignore_ascii_case(sub, b"CONSUMERS") => {
                Frame::Array(Vec::new())
            }
            [sub] if eq_ignore_ascii_case(sub, b"HELP") => array_bulk(vec![
                b"XINFO STREAM key".to_vec(),
                b"XINFO GROUPS key".to_vec(),
                b"XINFO CONSUMERS key group".to_vec(),
            ]),
            _ => error("ERR unknown XINFO subcommand or wrong number of arguments"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_xinfo_resp(store, args, out);
    }
}

impl crate::commands::redis::RedisCommand for XPending {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [_key, _group] => Frame::Array(vec![
                int(0),
                Frame::Null,
                Frame::Null,
                Frame::Array(Vec::new()),
            ]),
            [_key, _group, _start, _end, _count] => Frame::Array(Vec::new()),
            [_key, _group, _start, _end, _count, _consumer] => Frame::Array(Vec::new()),
            _ => wrong_arity("XPENDING"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [_key, _group] => {
                write_resp_array_header(out, 4);
                ServerWire::write_resp_integer(out, 0);
                write_resp_null(out);
                write_resp_null(out);
                write_resp_array_header(out, 0);
            }
            [_key, _group, _start, _end, _count] => write_resp_array_header(out, 0),
            [_key, _group, _start, _end, _count, _consumer] => write_resp_array_header(out, 0),
            _ => write_resp_wrong_arity(out, "XPENDING"),
        }
    }
}

impl crate::commands::redis::RedisCommand for XClaim {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 5 {
            wrong_arity("XCLAIM")
        } else {
            Frame::Array(Vec::new())
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 5 {
            write_resp_wrong_arity(out, "XCLAIM");
        } else {
            write_resp_array_header(out, 0);
        }
    }
}

impl crate::commands::redis::RedisCommand for XAck {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 3 {
            wrong_arity("XACK")
        } else {
            int(0)
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 3 {
            write_resp_wrong_arity(out, "XACK");
        } else {
            ServerWire::write_resp_integer(out, 0);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamId {
    ms: u64,
    seq: u64,
}

impl StreamId {
    fn to_string(self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

impl Ord for StreamId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ms
            .cmp(&other.ms)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for StreamId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
struct StreamEntry {
    id: StreamId,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone)]
struct StreamState {
    last_id: StreamId,
    entries: Vec<StreamEntry>,
}

#[derive(Debug, Clone, Copy)]
struct StreamHeader {
    last_id: StreamId,
    entry_count: u32,
}

#[derive(Debug, Clone, Copy)]
struct StreamTrim {
    max_len: usize,
    approximate: bool,
}

#[derive(Debug, Clone, Copy)]
enum StreamRangeCommand {
    Range,
    RevRange,
}

impl StreamRangeCommand {
    fn from_rev(rev: bool) -> Self {
        match rev {
            true => Self::RevRange,
            false => Self::Range,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Range => "XRANGE",
            Self::RevRange => "XREVRANGE",
        }
    }

    fn reverse(self) -> bool {
        matches!(self, Self::RevRange)
    }
}

struct StreamRangeArgs<'a> {
    key: &'a [u8],
    start: StreamId,
    end: StreamId,
    count: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamRangeOption {
    Count,
}

impl StreamRangeOption {
    const NAMES: &'static [(&'static [u8], Self)] = &[(b"COUNT", Self::Count)];

    fn from_name(name: &[u8]) -> Option<Self> {
        Self::NAMES.iter().find_map(|(candidate, option)| {
            eq_ignore_ascii_case(name, candidate).then_some(*option)
        })
    }
}

fn xrange(store: &EmbeddedStore, args: &[&[u8]], rev: bool) -> Frame {
    let command = StreamRangeCommand::from_rev(rev);
    let parsed = match parse_stream_range_args(args, command) {
        Ok(parsed) => parsed,
        Err(frame) => return frame,
    };
    match load_stream(store, parsed.key) {
        Ok(stream) => {
            let mut entries = stream
                .entries
                .iter()
                .filter(|entry| entry.id >= parsed.start && entry.id <= parsed.end)
                .collect::<Vec<_>>();
            if command.reverse() {
                entries.reverse();
            }
            if let Some(count) = parsed.count {
                entries.truncate(count);
            }
            Frame::Array(entries.into_iter().map(entry_frame).collect())
        }
        Err(frame) => frame,
    }
}

fn parse_stream_range_args<'a>(
    args: &'a [&'a [u8]],
    command: StreamRangeCommand,
) -> Result<StreamRangeArgs<'a>, Frame> {
    let [key, start, end, options @ ..] = args else {
        return Err(wrong_arity(command.name()));
    };
    Ok(StreamRangeArgs {
        key: *key,
        start: parse_range_bound(start, false)?,
        end: parse_range_bound(end, true)?,
        count: parse_stream_range_options(options)?,
    })
}

fn parse_stream_range_options(options: &[&[u8]]) -> Result<Option<usize>, Frame> {
    match options {
        [] => Ok(None),
        [name, value] if StreamRangeOption::from_name(name) == Some(StreamRangeOption::Count) => {
            parse_usize(value)
                .map(Some)
                .map_err(|_| error("ERR value is not an integer or out of range"))
        }
        _ => Err(error("ERR syntax error")),
    }
}

#[derive(Debug, Clone, Copy)]
enum StreamReadCommand {
    Read,
    ReadGroup,
}

impl StreamReadCommand {
    fn from_group(group: bool) -> Self {
        match group {
            true => Self::ReadGroup,
            false => Self::Read,
        }
    }

    fn without_group_prefix<'a>(self, args: &'a [&'a [u8]]) -> Result<&'a [&'a [u8]], Frame> {
        match self {
            Self::Read => Ok(args),
            Self::ReadGroup
                if args.len() < 3
                    || !args
                        .first()
                        .is_some_and(|arg| eq_ignore_ascii_case(arg, b"GROUP")) =>
            {
                Err(wrong_arity("XREADGROUP"))
            }
            Self::ReadGroup => Ok(&args[3..]),
        }
    }

    fn cursor_from_raw(self, raw_id: &[u8]) -> Result<Option<StreamId>, Frame> {
        match (self, raw_id) {
            (Self::ReadGroup, b">") => Ok(Some(StreamId { ms: 0, seq: 0 })),
            (_, b"$") => Ok(None),
            _ => parse_stream_id(raw_id).map(Some),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamReadOption {
    Count,
    Block,
}

impl StreamReadOption {
    const NAMES: &'static [(&'static [u8], Self)] =
        &[(b"COUNT", Self::Count), (b"BLOCK", Self::Block)];

    fn from_name(name: &[u8]) -> Option<Self> {
        Self::NAMES.iter().find_map(|(candidate, option)| {
            eq_ignore_ascii_case(name, candidate).then_some(*option)
        })
    }
}

struct StreamReadArgs<'a> {
    count: Option<usize>,
    streams: Vec<StreamReadStream<'a>>,
}

struct StreamReadStream<'a> {
    key: &'a [u8],
    after: StreamId,
}

fn xread(store: &EmbeddedStore, args: &[&[u8]], group: bool) -> Frame {
    let parsed = match parse_stream_read_args(args, StreamReadCommand::from_group(group)) {
        Ok(parsed) => parsed,
        Err(frame) => return frame,
    };
    let mut streams = Vec::new();
    for request in parsed.streams {
        let stream = match load_stream(store, request.key) {
            Ok(stream) => stream,
            Err(frame) => return frame,
        };
        let mut entries = stream
            .entries
            .iter()
            .filter(|entry| entry.id > request.after)
            .collect::<Vec<_>>();
        if let Some(count) = parsed.count {
            entries.truncate(count);
        }
        if !entries.is_empty() {
            streams.push(Frame::Array(vec![
                bulk(request.key.to_vec()),
                Frame::Array(entries.into_iter().map(entry_frame).collect()),
            ]));
        }
    }
    if streams.is_empty() {
        Frame::Null
    } else {
        Frame::Array(streams)
    }
}

#[cfg(feature = "server")]
fn write_stream_result<T>(
    out: &mut BytesMut,
    result: Result<T, Frame>,
    write_ok: impl FnOnce(&mut BytesMut, T),
) {
    match result {
        Ok(value) => write_ok(out, value),
        Err(frame) => write_frame(out, &frame),
    }
}

#[cfg(feature = "server")]
fn write_xrange_resp(store: &EmbeddedStore, args: &[&[u8]], rev: bool, out: &mut BytesMut) {
    let command = StreamRangeCommand::from_rev(rev);
    let parsed = match parse_stream_range_args(args, command) {
        Ok(parsed) => parsed,
        Err(frame) => {
            write_frame(out, &frame);
            return;
        }
    };
    match load_stream(store, parsed.key) {
        Ok(stream) => {
            let mut entries = stream
                .entries
                .iter()
                .filter(|entry| entry.id >= parsed.start && entry.id <= parsed.end)
                .collect::<Vec<_>>();
            if command.reverse() {
                entries.reverse();
            }
            if let Some(count) = parsed.count {
                entries.truncate(count);
            }
            write_resp_array_header(out, entries.len());
            for entry in entries {
                write_stream_entry_resp(out, entry);
            }
        }
        Err(frame) => write_frame(out, &frame),
    }
}

#[cfg(feature = "server")]
fn write_xread_resp(store: &EmbeddedStore, args: &[&[u8]], group: bool, out: &mut BytesMut) {
    let parsed = match parse_stream_read_args(args, StreamReadCommand::from_group(group)) {
        Ok(parsed) => parsed,
        Err(frame) => {
            write_frame(out, &frame);
            return;
        }
    };
    let mut streams = Vec::new();
    for request in parsed.streams {
        let stream = match load_stream(store, request.key) {
            Ok(stream) => stream,
            Err(frame) => {
                write_frame(out, &frame);
                return;
            }
        };
        let mut entries = stream
            .entries
            .into_iter()
            .filter(|entry| entry.id > request.after)
            .collect::<Vec<_>>();
        if let Some(count) = parsed.count {
            entries.truncate(count);
        }
        if !entries.is_empty() {
            streams.push((request.key, entries));
        }
    }
    if streams.is_empty() {
        write_resp_null(out);
        return;
    }
    write_resp_array_header(out, streams.len());
    for (key, entries) in streams {
        write_resp_array_header(out, 2);
        ServerWire::write_resp_blob_string(out, key);
        write_resp_array_header(out, entries.len());
        for entry in &entries {
            write_stream_entry_resp(out, entry);
        }
    }
}

#[cfg(feature = "server")]
fn write_xgroup_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
    match args {
        [sub, key, _group, id, tail @ ..] if eq_ignore_ascii_case(sub, b"CREATE") => {
            let mkstream = tail
                .iter()
                .any(|arg| eq_ignore_ascii_case(arg, b"MKSTREAM"));
            if mkstream || store.exists(key) {
                match parse_stream_id(id).and_then(|id| set_stream_last_id_result(store, key, id)) {
                    Ok(()) => write_resp_simple_string(out, "OK"),
                    Err(frame) => write_frame(out, &frame),
                }
            } else {
                ServerWire::write_resp_error(
                    out,
                    "ERR The XGROUP subcommand requires the key to exist",
                );
            }
        }
        [sub, key, _group, id] if eq_ignore_ascii_case(sub, b"SETID") => {
            match parse_stream_id(id).and_then(|id| set_stream_last_id_result(store, key, id)) {
                Ok(()) => write_resp_simple_string(out, "OK"),
                Err(frame) => write_frame(out, &frame),
            }
        }
        [sub, _key, _group] if eq_ignore_ascii_case(sub, b"DESTROY") => {
            ServerWire::write_resp_integer(out, 0);
        }
        [sub, _key, _group, _consumer] if eq_ignore_ascii_case(sub, b"DELCONSUMER") => {
            ServerWire::write_resp_integer(out, 0);
        }
        [sub, _key, _group, _consumer] if eq_ignore_ascii_case(sub, b"CREATECONSUMER") => {
            ServerWire::write_resp_integer(out, 1);
        }
        [sub] if eq_ignore_ascii_case(sub, b"HELP") => write_bulk_array_resp(
            out,
            &[
                b"XGROUP CREATE key group id [MKSTREAM]",
                b"XGROUP SETID key group id",
                b"XGROUP DESTROY key group",
            ],
        ),
        _ => ServerWire::write_resp_error(
            out,
            "ERR unknown XGROUP subcommand or wrong number of arguments",
        ),
    }
}

#[cfg(feature = "server")]
fn write_xinfo_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
    match args {
        [sub, key] if eq_ignore_ascii_case(sub, b"STREAM") => match load_stream(store, key) {
            Ok(stream) => write_xinfo_stream_resp(out, &stream),
            Err(frame) => write_frame(out, &frame),
        },
        [sub, _key] if eq_ignore_ascii_case(sub, b"GROUPS") => write_resp_array_header(out, 0),
        [sub, _key, _group] if eq_ignore_ascii_case(sub, b"CONSUMERS") => {
            write_resp_array_header(out, 0);
        }
        [sub] if eq_ignore_ascii_case(sub, b"HELP") => write_bulk_array_resp(
            out,
            &[
                b"XINFO STREAM key",
                b"XINFO GROUPS key",
                b"XINFO CONSUMERS key group",
            ],
        ),
        _ => ServerWire::write_resp_error(
            out,
            "ERR unknown XINFO subcommand or wrong number of arguments",
        ),
    }
}

#[cfg(feature = "server")]
fn write_xinfo_stream_resp(out: &mut BytesMut, stream: &StreamState) {
    write_resp_array_header(out, 14);
    ServerWire::write_resp_blob_string(out, b"length");
    ServerWire::write_resp_integer(out, stream.entries.len() as i64);
    ServerWire::write_resp_blob_string(out, b"radix-tree-keys");
    ServerWire::write_resp_integer(out, 0);
    ServerWire::write_resp_blob_string(out, b"radix-tree-nodes");
    ServerWire::write_resp_integer(out, 0);
    ServerWire::write_resp_blob_string(out, b"groups");
    ServerWire::write_resp_integer(out, 0);
    ServerWire::write_resp_blob_string(out, b"last-generated-id");
    write_stream_id_bulk_resp(out, stream.last_id);
    ServerWire::write_resp_blob_string(out, b"first-entry");
    match stream.entries.first() {
        Some(entry) => write_stream_entry_resp(out, entry),
        None => write_resp_null(out),
    }
    ServerWire::write_resp_blob_string(out, b"last-entry");
    match stream.entries.last() {
        Some(entry) => write_stream_entry_resp(out, entry),
        None => write_resp_null(out),
    }
}

#[cfg(feature = "server")]
fn write_stream_entry_resp(out: &mut BytesMut, entry: &StreamEntry) {
    write_resp_array_header(out, 2);
    write_stream_id_bulk_resp(out, entry.id);
    write_resp_array_header(out, entry.fields.len().saturating_mul(2));
    for (field, value) in &entry.fields {
        ServerWire::write_resp_blob_string(out, field);
        ServerWire::write_resp_blob_string(out, value);
    }
}

#[cfg(feature = "server")]
fn write_stream_id_bulk_resp(out: &mut BytesMut, id: StreamId) {
    let mut ms_buf = itoa::Buffer::new();
    let ms = ms_buf.format(id.ms).as_bytes();
    let mut seq_buf = itoa::Buffer::new();
    let seq = seq_buf.format(id.seq).as_bytes();
    let mut len_buf = itoa::Buffer::new();
    let len = len_buf.format(ms.len() + 1 + seq.len()).as_bytes();

    out.extend_from_slice(b"$");
    out.extend_from_slice(len);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(ms);
    out.extend_from_slice(b"-");
    out.extend_from_slice(seq);
    out.extend_from_slice(b"\r\n");
}

#[cfg(feature = "server")]
fn write_bulk_array_resp(out: &mut BytesMut, values: &[&[u8]]) {
    write_resp_array_header(out, values.len());
    for value in values {
        ServerWire::write_resp_blob_string(out, value);
    }
}

fn parse_stream_read_args<'a>(
    args: &'a [&'a [u8]],
    command: StreamReadCommand,
) -> Result<StreamReadArgs<'a>, Frame> {
    let args = command.without_group_prefix(args)?;
    let (count, args) = parse_stream_read_options(args)?;
    let args = strip_streams_keyword(args)?;
    Ok(StreamReadArgs {
        count,
        streams: parse_stream_read_streams(args, command)?,
    })
}

fn parse_stream_read_options<'a>(
    mut args: &'a [&'a [u8]],
) -> Result<(Option<usize>, &'a [&'a [u8]]), Frame> {
    let mut count = None;
    while let Some((name, rest)) = args.split_first() {
        let Some(option) = StreamReadOption::from_name(name) else {
            break;
        };
        let (value, tail) = rest
            .split_first()
            .ok_or_else(|| error("ERR syntax error"))?;
        match option {
            StreamReadOption::Count => {
                count = Some(
                    parse_usize(value)
                        .map_err(|_| error("ERR value is not an integer or out of range"))?,
                );
            }
            StreamReadOption::Block => {}
        }
        args = tail;
    }
    Ok((count, args))
}

fn strip_streams_keyword<'a>(args: &'a [&'a [u8]]) -> Result<&'a [&'a [u8]], Frame> {
    match args.split_first() {
        Some((keyword, rest)) if eq_ignore_ascii_case(keyword, b"STREAMS") => Ok(rest),
        _ => Err(error("ERR syntax error")),
    }
}

fn parse_stream_read_streams<'a>(
    args: &'a [&'a [u8]],
    command: StreamReadCommand,
) -> Result<Vec<StreamReadStream<'a>>, Frame> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(error("ERR Unbalanced XREAD list of streams"));
    }
    let key_count = args.len() / 2;
    let (keys, ids) = args.split_at(key_count);
    keys.iter()
        .zip(ids.iter())
        .filter_map(|(key, raw_id)| match command.cursor_from_raw(raw_id) {
            Ok(Some(after)) => Some(Ok(StreamReadStream { key: *key, after })),
            Ok(None) => None,
            Err(frame) => Some(Err(frame)),
        })
        .collect()
}

fn with_stream_mut<R>(
    store: &EmbeddedStore,
    key: &[u8],
    mutate: impl FnOnce(&mut StreamState) -> Result<R, Frame>,
) -> Result<R, Frame> {
    store.transform_string_value_no_ttl(
        key,
        |existing| {
            let mut stream = decode_stream(existing)?;
            let frame = mutate(&mut stream)?;
            Ok((frame, encode_stream(&stream)))
        },
        wrongtype,
    )
}

fn try_fast_append_stream(
    existing: Option<&[u8]>,
    id_arg: &[u8],
    fields: &[(&[u8], &[u8])],
    trim: Option<StreamTrim>,
) -> Result<Option<(StreamId, Vec<u8>)>, Frame> {
    let header = match existing {
        Some(value) => parse_stream_header(value)?,
        None => StreamHeader {
            last_id: StreamId { ms: 0, seq: 0 },
            entry_count: 0,
        },
    };
    let id = next_stream_id(header.last_id, id_arg)?;
    let next_count = header
        .entry_count
        .checked_add(1)
        .ok_or_else(|| error("ERR stream length overflow"))?;
    if trim_requires_rebuild(trim, next_count as usize) {
        return Ok(None);
    }

    let entry_len = encoded_borrowed_entry_len(fields);
    let mut out = match existing {
        Some(value) => {
            let mut out = Vec::with_capacity(value.len().saturating_add(entry_len));
            out.extend_from_slice(value);
            out
        }
        None => {
            let mut out = Vec::with_capacity(STREAM_HEADER_LEN.saturating_add(entry_len));
            write_stream_header(
                &mut out,
                StreamHeader {
                    last_id: StreamId { ms: 0, seq: 0 },
                    entry_count: 0,
                },
            );
            out
        }
    };
    write_stream_last_id(&mut out, id);
    write_u32_at(&mut out, ENTRY_COUNT_OFFSET, next_count);
    append_encoded_borrowed_entry(&mut out, id, fields);
    Ok(Some((id, out)))
}

fn trim_requires_rebuild(trim: Option<StreamTrim>, next_len: usize) -> bool {
    match trim {
        Some(trim) if next_len > trim.max_len => {
            !trim.approximate || next_len > approximate_trim_threshold(trim.max_len)
        }
        _ => false,
    }
}

fn approximate_trim_threshold(max_len: usize) -> usize {
    if max_len == 0 {
        return 0;
    }
    let slack = (max_len / 10).max(64);
    max_len.saturating_add(slack)
}

fn set_stream_last_id(store: &EmbeddedStore, key: &[u8], id: StreamId) -> Frame {
    set_stream_last_id_result(store, key, id)
        .map(|()| simple("OK"))
        .unwrap_or_else(|frame| frame)
}

fn set_stream_last_id_result(store: &EmbeddedStore, key: &[u8], id: StreamId) -> Result<(), Frame> {
    store.transform_string_value_no_ttl(
        key,
        |existing| {
            let out = match existing {
                Some(value) => {
                    let mut out = value.to_vec();
                    parse_stream_header(&out)?;
                    write_stream_last_id(&mut out, id);
                    out
                }
                None => {
                    let mut out = Vec::with_capacity(STREAM_HEADER_LEN);
                    write_stream_header(
                        &mut out,
                        StreamHeader {
                            last_id: id,
                            entry_count: 0,
                        },
                    );
                    out
                }
            };
            Ok(((), out))
        },
        wrongtype,
    )
}

fn load_stream_len(store: &EmbeddedStore, key: &[u8]) -> Result<usize, Frame> {
    let mut parsed = None;
    match store.get_string_value_into(key, |value| {
        parsed =
            Some(parse_stream_header(value.as_ref()).map(|header| header.entry_count as usize));
    }) {
        crate::storage::RedisStringLookup::Hit => {
            parsed.expect("hit callback records stream parse result")
        }
        crate::storage::RedisStringLookup::Miss => Ok(0),
        crate::storage::RedisStringLookup::WrongType => Err(wrongtype()),
    }
}

fn load_stream(store: &EmbeddedStore, key: &[u8]) -> Result<StreamState, Frame> {
    match optional_string_value(store, key, true) {
        Ok(Some(value)) => decode_stream(Some(&value)),
        Ok(None) => Ok(StreamState {
            last_id: StreamId { ms: 0, seq: 0 },
            entries: Vec::new(),
        }),
        Err(frame) => Err(frame),
    }
}

fn decode_stream(existing: Option<&[u8]>) -> Result<StreamState, Frame> {
    let Some(value) = existing else {
        return Ok(StreamState {
            last_id: StreamId { ms: 0, seq: 0 },
            entries: Vec::new(),
        });
    };
    let header = parse_stream_header(value)?;
    let mut cursor = STREAM_HEADER_LEN;
    let count = header.entry_count as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let id = StreamId {
            ms: read_u64(value, &mut cursor)?,
            seq: read_u64(value, &mut cursor)?,
        };
        let field_count = read_u32(value, &mut cursor)? as usize;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            fields.push((
                read_bytes(value, &mut cursor)?,
                read_bytes(value, &mut cursor)?,
            ));
        }
        entries.push(StreamEntry { id, fields });
    }
    if cursor != value.len() {
        return Err(error("WRONGTYPE Key is not a valid stream value."));
    }
    Ok(StreamState {
        last_id: header.last_id,
        entries,
    })
}

fn encode_stream(stream: &StreamState) -> Vec<u8> {
    let mut out = Vec::with_capacity(encoded_stream_len(stream));
    write_stream_header(
        &mut out,
        StreamHeader {
            last_id: stream.last_id,
            entry_count: stream.entries.len() as u32,
        },
    );
    for entry in &stream.entries {
        append_encoded_entry(&mut out, entry.id, &entry.fields);
    }
    out
}

fn encoded_stream_len(stream: &StreamState) -> usize {
    STREAM_HEADER_LEN.saturating_add(
        stream
            .entries
            .iter()
            .map(|entry| encoded_entry_len(&entry.fields))
            .sum::<usize>(),
    )
}

fn encoded_entry_len(fields: &[(Vec<u8>, Vec<u8>)]) -> usize {
    fields.iter().fold(8 + 8 + 4, |len, (field, value)| {
        len.saturating_add(4)
            .saturating_add(field.len())
            .saturating_add(4)
            .saturating_add(value.len())
    })
}

fn encoded_borrowed_entry_len(fields: &[(&[u8], &[u8])]) -> usize {
    fields.iter().fold(8 + 8 + 4, |len, (field, value)| {
        len.saturating_add(4)
            .saturating_add(field.len())
            .saturating_add(4)
            .saturating_add(value.len())
    })
}

fn append_encoded_entry(out: &mut Vec<u8>, id: StreamId, fields: &[(Vec<u8>, Vec<u8>)]) {
    out.extend_from_slice(&id.ms.to_le_bytes());
    out.extend_from_slice(&id.seq.to_le_bytes());
    out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
    for (field, value) in fields {
        write_bytes(field, out);
        write_bytes(value, out);
    }
}

fn append_encoded_borrowed_entry(out: &mut Vec<u8>, id: StreamId, fields: &[(&[u8], &[u8])]) {
    out.extend_from_slice(&id.ms.to_le_bytes());
    out.extend_from_slice(&id.seq.to_le_bytes());
    out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
    for (field, value) in fields {
        write_bytes(field, out);
        write_bytes(value, out);
    }
}

fn next_stream_id(last_id: StreamId, raw: &[u8]) -> Result<StreamId, Frame> {
    if raw == b"*" {
        let ms = now_millis();
        let seq = if ms > last_id.ms {
            0
        } else {
            last_id.seq.saturating_add(1)
        };
        return Ok(StreamId { ms, seq });
    }
    if let Some(ms) = raw.strip_suffix(b"-*") {
        let ms = parse_u64(ms).map_err(|_| error("ERR Invalid stream ID specified"))?;
        let seq = if ms == last_id.ms {
            last_id.seq.saturating_add(1)
        } else {
            0
        };
        let id = StreamId { ms, seq };
        validate_new_id(last_id, id)?;
        return Ok(id);
    }
    let id = parse_stream_id(raw)?;
    validate_new_id(last_id, id)?;
    Ok(id)
}

fn validate_new_id(last_id: StreamId, id: StreamId) -> Result<(), Frame> {
    if id.ms == 0 && id.seq == 0 {
        return Err(error(
            "ERR The ID specified in XADD must be greater than 0-0",
        ));
    }
    if id <= last_id {
        return Err(error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ));
    }
    Ok(())
}

fn parse_stream_id(raw: &[u8]) -> Result<StreamId, Frame> {
    let parts = raw.split(|byte| *byte == b'-').collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(error("ERR Invalid stream ID specified"));
    }
    Ok(StreamId {
        ms: parse_u64(parts[0]).map_err(|_| error("ERR Invalid stream ID specified"))?,
        seq: parse_u64(parts[1]).map_err(|_| error("ERR Invalid stream ID specified"))?,
    })
}

fn parse_range_bound(raw: &[u8], end: bool) -> Result<StreamId, Frame> {
    if raw == b"-" {
        Ok(StreamId { ms: 0, seq: 0 })
    } else if raw == b"+" {
        Ok(StreamId {
            ms: u64::MAX,
            seq: u64::MAX,
        })
    } else if !raw.contains(&b'-') {
        let ms = parse_u64(raw).map_err(|_| error("ERR Invalid stream ID specified"))?;
        Ok(StreamId {
            ms,
            seq: if end { u64::MAX } else { 0 },
        })
    } else {
        parse_stream_id(raw)
    }
}

fn trim_stream(stream: &mut StreamState, max_len: usize) -> usize {
    let removed = stream.entries.len().saturating_sub(max_len);
    if removed > 0 {
        stream.entries.drain(0..removed);
    }
    removed
}

fn entry_frame(entry: &StreamEntry) -> Frame {
    Frame::Array(vec![
        bulk(entry.id.to_string().into_bytes()),
        Frame::Array(
            entry
                .fields
                .iter()
                .flat_map(|(field, value)| [bulk(field.clone()), bulk(value.clone())])
                .collect(),
        ),
    ])
}

fn parse_stream_header(value: &[u8]) -> Result<StreamHeader, Frame> {
    if !value.starts_with(STREAM_PREFIX) || value.len() < STREAM_HEADER_LEN {
        return Err(error("WRONGTYPE Key is not a valid stream value."));
    }
    let mut cursor = STREAM_PREFIX.len();
    Ok(StreamHeader {
        last_id: StreamId {
            ms: read_u64(value, &mut cursor)?,
            seq: read_u64(value, &mut cursor)?,
        },
        entry_count: read_u32(value, &mut cursor)?,
    })
}

fn write_stream_header(out: &mut Vec<u8>, header: StreamHeader) {
    out.extend_from_slice(STREAM_PREFIX);
    out.extend_from_slice(&header.last_id.ms.to_le_bytes());
    out.extend_from_slice(&header.last_id.seq.to_le_bytes());
    out.extend_from_slice(&header.entry_count.to_le_bytes());
}

fn write_stream_last_id(out: &mut [u8], id: StreamId) {
    write_u64_at(out, LAST_ID_MS_OFFSET, id.ms);
    write_u64_at(out, LAST_ID_SEQ_OFFSET, id.seq);
}

fn write_u32_at(out: &mut [u8], offset: usize, value: u32) {
    out[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64_at(out: &mut [u8], offset: usize, value: u64) {
    out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u32(value: &[u8], cursor: &mut usize) -> Result<u32, Frame> {
    let bytes = read_fixed::<4>(value, cursor)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(value: &[u8], cursor: &mut usize) -> Result<u64, Frame> {
    let bytes = read_fixed::<8>(value, cursor)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_fixed<const N: usize>(value: &[u8], cursor: &mut usize) -> Result<[u8; N], Frame> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| error("WRONGTYPE Key is not a valid stream value."))?;
    let bytes = value
        .get(*cursor..end)
        .ok_or_else(|| error("WRONGTYPE Key is not a valid stream value."))?;
    *cursor = end;
    Ok(bytes.try_into().expect("slice length was checked"))
}

fn read_bytes(value: &[u8], cursor: &mut usize) -> Result<Vec<u8>, Frame> {
    let len = read_u32(value, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| error("WRONGTYPE Key is not a valid stream value."))?;
    let bytes = value
        .get(*cursor..end)
        .ok_or_else(|| error("WRONGTYPE Key is not a valid stream value."))?;
    *cursor = end;
    Ok(bytes.to_vec())
}

fn write_bytes(value: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}
