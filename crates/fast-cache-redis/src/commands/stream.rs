use std::cmp::Ordering;

use crate::commands::redis::{
    array_bulk, bulk, eq_ignore_ascii_case, error, int, optional_string_value, parse_u64,
    parse_usize, simple, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
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
        if args.len() < 4 {
            return wrong_arity("XADD");
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
                return error("ERR syntax error");
            };
            let Ok(count) = parse_usize(count) else {
                return error("ERR value is not an integer or out of range");
            };
            trim = Some(StreamTrim {
                max_len: count,
                approximate,
            });
            index += 1;
        }
        let Some(id_arg) = args.get(index) else {
            return wrong_arity("XADD");
        };
        index += 1;
        if index >= args.len() || (args.len() - index) % 2 != 0 {
            return wrong_arity("XADD");
        }
        let fields = args[index..]
            .chunks_exact(2)
            .map(|chunk| (chunk[0].to_vec(), chunk[1].to_vec()))
            .collect::<Vec<_>>();
        let result = store.transform_string_value_no_ttl(
            key,
            |existing| {
                if let Some((id, value)) = try_fast_append_stream(existing, id_arg, &fields, trim)?
                {
                    return Ok((bulk(id.to_string().into_bytes()), value));
                }
                let mut stream = decode_stream(existing)?;
                let id = next_stream_id(stream.last_id, id_arg)?;
                let frame = bulk(id.to_string().into_bytes());
                stream.entries.push(StreamEntry { id, fields });
                stream.last_id = id;
                if let Some(trim) = trim {
                    trim_stream(&mut stream, trim.max_len);
                }
                Ok((frame, encode_stream(&stream)))
            },
            wrongtype,
        );
        result.unwrap_or_else(|frame| frame)
    }
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
}

impl crate::commands::redis::RedisCommand for XRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xrange(store, args, false)
    }
}

impl crate::commands::redis::RedisCommand for XRevRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xrange(store, args, true)
    }
}

impl crate::commands::redis::RedisCommand for XDel {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, ids @ ..] = args else {
            return wrong_arity("XDEL");
        };
        if ids.is_empty() {
            return wrong_arity("XDEL");
        }
        with_stream_mut(store, key, |stream| {
            let ids = ids
                .iter()
                .map(|raw| parse_stream_id(raw))
                .collect::<Result<Vec<_>, _>>()?;
            let before = stream.entries.len();
            stream.entries.retain(|entry| !ids.contains(&entry.id));
            Ok(int(before.saturating_sub(stream.entries.len()) as i64))
        })
        .unwrap_or_else(|frame| frame)
    }
}

impl crate::commands::redis::RedisCommand for XTrim {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 3 || !eq_ignore_ascii_case(args[1], b"MAXLEN") {
            return wrong_arity("XTRIM");
        }
        let count_index = if args.get(2).is_some_and(|arg| *arg == b"~") {
            3
        } else {
            2
        };
        let Some(count) = args.get(count_index) else {
            return wrong_arity("XTRIM");
        };
        let Ok(max_len) = parse_usize(count) else {
            return error("ERR value is not an integer or out of range");
        };
        with_stream_mut(store, args[0], |stream| {
            let removed = trim_stream(stream, max_len);
            Ok(int(removed as i64))
        })
        .unwrap_or_else(|frame| frame)
    }
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
}

impl crate::commands::redis::RedisCommand for XRead {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xread(store, args, false)
    }
}

impl crate::commands::redis::RedisCommand for XReadGroup {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        xread(store, args, true)
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
}

impl crate::commands::redis::RedisCommand for XClaim {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 5 {
            wrong_arity("XCLAIM")
        } else {
            Frame::Array(Vec::new())
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

fn xrange(store: &EmbeddedStore, args: &[&[u8]], rev: bool) -> Frame {
    if args.len() < 3 {
        return wrong_arity(if rev { "XREVRANGE" } else { "XRANGE" });
    }
    let key = args[0];
    let start = match parse_range_bound(args[1], false) {
        Ok(id) => id,
        Err(frame) => return frame,
    };
    let end = match parse_range_bound(args[2], true) {
        Ok(id) => id,
        Err(frame) => return frame,
    };
    let mut count = None;
    if args.len() > 3 {
        if args.len() != 5 || !eq_ignore_ascii_case(args[3], b"COUNT") {
            return error("ERR syntax error");
        }
        let Ok(parsed) = parse_usize(args[4]) else {
            return error("ERR value is not an integer or out of range");
        };
        count = Some(parsed);
    }
    match load_stream(store, key) {
        Ok(stream) => {
            let mut entries = stream
                .entries
                .iter()
                .filter(|entry| entry.id >= start && entry.id <= end)
                .collect::<Vec<_>>();
            if rev {
                entries.reverse();
            }
            if let Some(count) = count {
                entries.truncate(count);
            }
            Frame::Array(entries.into_iter().map(entry_frame).collect())
        }
        Err(frame) => frame,
    }
}

fn xread(store: &EmbeddedStore, args: &[&[u8]], group: bool) -> Frame {
    let mut index = 0;
    if group {
        if args.len() < 3 || !eq_ignore_ascii_case(args[0], b"GROUP") {
            return wrong_arity("XREADGROUP");
        }
        index = 3;
    }
    let mut count = None;
    while index < args.len() {
        if eq_ignore_ascii_case(args[index], b"COUNT") {
            let Some(raw) = args.get(index + 1) else {
                return error("ERR syntax error");
            };
            let Ok(parsed) = parse_usize(raw) else {
                return error("ERR value is not an integer or out of range");
            };
            count = Some(parsed);
            index += 2;
        } else if eq_ignore_ascii_case(args[index], b"BLOCK") {
            index += 2;
        } else {
            break;
        }
    }
    if index >= args.len() || !eq_ignore_ascii_case(args[index], b"STREAMS") {
        return error("ERR syntax error");
    }
    let rest = &args[index + 1..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return error("ERR Unbalanced XREAD list of streams");
    }
    let key_count = rest.len() / 2;
    let (keys, ids) = rest.split_at(key_count);
    let mut streams = Vec::new();
    for (key, raw_id) in keys.iter().zip(ids.iter()) {
        let after = if group && *raw_id == b">" {
            StreamId { ms: 0, seq: 0 }
        } else if *raw_id == b"$" {
            continue;
        } else {
            match parse_stream_id(raw_id) {
                Ok(id) => id,
                Err(frame) => return frame,
            }
        };
        let stream = match load_stream(store, key) {
            Ok(stream) => stream,
            Err(frame) => return frame,
        };
        let mut entries = stream
            .entries
            .iter()
            .filter(|entry| entry.id > after)
            .collect::<Vec<_>>();
        if let Some(count) = count {
            entries.truncate(count);
        }
        if !entries.is_empty() {
            streams.push(Frame::Array(vec![
                bulk((*key).to_vec()),
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

fn with_stream_mut(
    store: &EmbeddedStore,
    key: &[u8],
    mutate: impl FnOnce(&mut StreamState) -> Result<Frame, Frame>,
) -> Result<Frame, Frame> {
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
    fields: &[(Vec<u8>, Vec<u8>)],
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

    let entry_len = encoded_entry_len(fields);
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
    append_encoded_entry(&mut out, id, fields);
    Ok(Some((id, out)))
}

fn trim_requires_rebuild(trim: Option<StreamTrim>, next_len: usize) -> bool {
    let Some(trim) = trim else {
        return false;
    };
    if next_len <= trim.max_len {
        return false;
    }
    !trim.approximate || next_len > approximate_trim_threshold(trim.max_len)
}

fn approximate_trim_threshold(max_len: usize) -> usize {
    if max_len == 0 {
        return 0;
    }
    let slack = (max_len / 10).max(64);
    max_len.saturating_add(slack)
}

fn set_stream_last_id(store: &EmbeddedStore, key: &[u8], id: StreamId) -> Frame {
    store
        .transform_string_value_no_ttl(
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
                Ok((simple("OK"), out))
            },
            wrongtype,
        )
        .unwrap_or_else(|frame| frame)
}

fn load_stream_len(store: &EmbeddedStore, key: &[u8]) -> Result<usize, Frame> {
    match optional_string_value(store, key, true) {
        Ok(Some(value)) => parse_stream_header(&value).map(|header| header.entry_count as usize),
        Ok(None) => Ok(0),
        Err(frame) => Err(frame),
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

fn append_encoded_entry(out: &mut Vec<u8>, id: StreamId, fields: &[(Vec<u8>, Vec<u8>)]) {
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
