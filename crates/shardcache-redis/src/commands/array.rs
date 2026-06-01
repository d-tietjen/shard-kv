use std::collections::BTreeMap;

use crate::commands::redis::{
    array_bulk, bulk, eq_ignore_ascii_case, error, int, optional_string_value, parse_i64,
    parse_usize, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
use crate::storage::{Bytes, EmbeddedStore, RedisStringStore};

const ARRAY_PREFIX: &[u8] = b"FC:ARRAY:v1\0";

macro_rules! define_array_command {
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

define_array_command!(ArCount, ARCOUNT_COMMAND, "ARCOUNT", false);
define_array_command!(ArDel, ARDEL_COMMAND, "ARDEL", true);
define_array_command!(ArDelRange, ARDELRANGE_COMMAND, "ARDELRANGE", true);
define_array_command!(ArGet, ARGET_COMMAND, "ARGET", false);
define_array_command!(ArGetRange, ARGETRANGE_COMMAND, "ARGETRANGE", false);
define_array_command!(ArGrep, ARGREP_COMMAND, "ARGREP", false);
define_array_command!(ArInfo, ARINFO_COMMAND, "ARINFO", false);
define_array_command!(ArInsert, ARINSERT_COMMAND, "ARINSERT", true);
define_array_command!(ArLastItems, ARLASTITEMS_COMMAND, "ARLASTITEMS", false);
define_array_command!(ArLen, ARLEN_COMMAND, "ARLEN", false);
define_array_command!(ArMGet, ARMGET_COMMAND, "ARMGET", false);
define_array_command!(ArMSet, ARMSET_COMMAND, "ARMSET", true);
define_array_command!(ArNext, ARNEXT_COMMAND, "ARNEXT", false);
define_array_command!(ArOp, AROP_COMMAND, "AROP", false);
define_array_command!(ArRing, ARRING_COMMAND, "ARRING", true);
define_array_command!(ArScan, ARSCAN_COMMAND, "ARSCAN", false);
define_array_command!(ArSeek, ARSEEK_COMMAND, "ARSEEK", true);
define_array_command!(ArSet, ARSET_COMMAND, "ARSET", true);

#[derive(Debug, Clone, Default)]
struct ArrayState {
    next_index: i64,
    ring_size: Option<i64>,
    entries: BTreeMap<i64, Bytes>,
    last: Vec<(i64, Bytes)>,
}

impl crate::commands::redis::RedisCommand for ArSet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, index_raw, values @ ..] = args else {
            return wrong_arity("ARSET");
        };
        if values.is_empty() {
            return wrong_arity("ARSET");
        }
        let index = match parse_i64(index_raw) {
            Ok(index) if index >= 0 => index,
            _ => return error("ERR value is not an integer or out of range"),
        };
        array_write(store, key, |array| {
            let mut inserted = 0i64;
            for (offset, value) in values.iter().enumerate() {
                let index = index + offset as i64;
                if array.entries.insert(index, (*value).to_vec()).is_none() {
                    inserted += 1;
                }
                array.next_index = array.next_index.max(index + 1);
            }
            int(inserted)
        })
    }
}

impl crate::commands::redis::RedisCommand for ArMSet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, pairs @ ..] = args else {
            return wrong_arity("ARMSET");
        };
        if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
            return wrong_arity("ARMSET");
        }
        let parsed = match parse_index_value_pairs(pairs) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        array_write(store, key, |array| {
            let mut inserted = 0i64;
            for (index, value) in parsed {
                if array.entries.insert(index, value.to_vec()).is_none() {
                    inserted += 1;
                }
                array.next_index = array.next_index.max(index + 1);
            }
            int(inserted)
        })
    }
}

impl crate::commands::redis::RedisCommand for ArInsert {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, values @ ..] = args else {
            return wrong_arity("ARINSERT");
        };
        if values.is_empty() {
            return wrong_arity("ARINSERT");
        }
        array_write(store, key, |array| {
            let mut last = Vec::with_capacity(values.len());
            let start = array.next_index.max(0);
            for (offset, value) in values.iter().enumerate() {
                let index = start + offset as i64;
                array.entries.insert(index, (*value).to_vec());
                last.push((index, (*value).to_vec()));
            }
            array.next_index = start + values.len() as i64;
            array.last = last;
            int(start)
        })
    }
}

impl crate::commands::redis::RedisCommand for ArRing {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, size_raw, values @ ..] = args else {
            return wrong_arity("ARRING");
        };
        if values.is_empty() {
            return wrong_arity("ARRING");
        }
        let size = match parse_i64(size_raw) {
            Ok(size) if size > 0 => size,
            _ => return error("ERR value is not an integer or out of range"),
        };
        array_write(store, key, |array| {
            array.ring_size = Some(size);
            array.entries.retain(|index, _| *index < size);
            let mut last = Vec::with_capacity(values.len());
            for value in values {
                let index = array.next_index.rem_euclid(size);
                array.entries.insert(index, (*value).to_vec());
                last.push((index, (*value).to_vec()));
                array.next_index = (index + 1).rem_euclid(size);
            }
            array.last = last;
            int(array.next_index)
        })
    }
}

impl crate::commands::redis::RedisCommand for ArSeek {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, index_raw] = args else {
            return wrong_arity("ARSEEK");
        };
        let index = match parse_i64(index_raw) {
            Ok(index) if index >= 0 => index,
            _ => return error("ERR value is not an integer or out of range"),
        };
        array_write(store, key, |array| {
            array.next_index = index;
            crate::commands::redis::simple("OK")
        })
    }
}

impl crate::commands::redis::RedisCommand for ArDel {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, indices @ ..] = args else {
            return wrong_arity("ARDEL");
        };
        if indices.is_empty() {
            return wrong_arity("ARDEL");
        }
        let parsed = match parse_indices(indices) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        array_write(store, key, |array| {
            let removed = parsed
                .iter()
                .filter(|index| array.entries.remove(index).is_some())
                .count();
            int(removed as i64)
        })
    }
}

impl crate::commands::redis::RedisCommand for ArDelRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, ranges @ ..] = args else {
            return wrong_arity("ARDELRANGE");
        };
        if ranges.is_empty() || !ranges.len().is_multiple_of(2) {
            return wrong_arity("ARDELRANGE");
        }
        let ranges = match parse_ranges(ranges) {
            Ok(ranges) => ranges,
            Err(frame) => return frame,
        };
        array_write(store, key, |array| {
            let mut removed = 0usize;
            for (start, end) in ranges {
                let keys = range_keys(&array.entries, start, end);
                for index in keys {
                    if array.entries.remove(&index).is_some() {
                        removed += 1;
                    }
                }
            }
            int(removed as i64)
        })
    }
}

impl crate::commands::redis::RedisCommand for ArGet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, index_raw] = args else {
            return wrong_arity("ARGET");
        };
        let index = match parse_i64(index_raw) {
            Ok(index) if index >= 0 => index,
            _ => return error("ERR value is not an integer or out of range"),
        };
        match array_read(store, key) {
            Ok(Some(array)) => array.entries.get(&index).cloned().map_or(Frame::Null, bulk),
            Ok(None) => Frame::Null,
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for ArMGet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, indices @ ..] = args else {
            return wrong_arity("ARMGET");
        };
        if indices.is_empty() {
            return wrong_arity("ARMGET");
        }
        let indices = match parse_indices(indices) {
            Ok(indices) => indices,
            Err(frame) => return frame,
        };
        match array_read(store, key) {
            Ok(Some(array)) => Frame::Array(
                indices
                    .into_iter()
                    .map(|index| array.entries.get(&index).cloned().map_or(Frame::Null, bulk))
                    .collect(),
            ),
            Ok(None) => Frame::Array(vec![Frame::Null; indices.len()]),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for ArGetRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, start_raw, end_raw] = args else {
            return wrong_arity("ARGETRANGE");
        };
        let (start, end) = match parse_start_end(start_raw, end_raw) {
            Ok(range) => range,
            Err(frame) => return frame,
        };
        match array_read(store, key) {
            Ok(Some(array)) => Frame::Array(
                inclusive_indices(start, end)
                    .into_iter()
                    .map(|index| array.entries.get(&index).cloned().map_or(Frame::Null, bulk))
                    .collect(),
            ),
            Ok(None) => Frame::Array(Vec::new()),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for ArScan {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, start_raw, end_raw, options @ ..] = args else {
            return wrong_arity("ARSCAN");
        };
        let (start, end) = match parse_start_end(start_raw, end_raw) {
            Ok(range) => range,
            Err(frame) => return frame,
        };
        let limit = match parse_limit_option(options) {
            Ok(limit) => limit,
            Err(frame) => return frame,
        };
        match array_read(store, key) {
            Ok(Some(array)) => {
                let mut out = Vec::new();
                for index in inclusive_indices(start, end) {
                    if let Some(value) = array.entries.get(&index) {
                        out.push(index.to_string().into_bytes());
                        out.push(value.clone());
                        if limit.is_some_and(|limit| out.len() / 2 >= limit) {
                            break;
                        }
                    }
                }
                array_bulk(out)
            }
            Ok(None) => Frame::Array(Vec::new()),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for ArGrep {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let parsed = match parse_argrep_args(args) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        match array_read(store, parsed.key) {
            Ok(Some(array)) => {
                let mut out = Vec::new();
                for index in inclusive_indices(parsed.start, parsed.end) {
                    let Some(value) = array.entries.get(&index) else {
                        continue;
                    };
                    if !parsed.matches(value) {
                        continue;
                    }
                    out.push(index.to_string().into_bytes());
                    if parsed.with_values {
                        out.push(value.clone());
                    }
                    if parsed.limit.is_some_and(|limit| {
                        let logical = if parsed.with_values {
                            out.len() / 2
                        } else {
                            out.len()
                        };
                        logical >= limit
                    }) {
                        break;
                    }
                }
                array_bulk(out)
            }
            Ok(None) => Frame::Array(Vec::new()),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for ArOp {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, start_raw, end_raw, op, op_arg @ ..] = args else {
            return wrong_arity("AROP");
        };
        let (start, end) = match parse_start_end(start_raw, end_raw) {
            Ok(range) => range,
            Err(frame) => return frame,
        };
        let array = match array_read(store, key) {
            Ok(Some(array)) => array,
            Ok(None) => return int(0),
            Err(frame) => return frame,
        };
        let values = inclusive_indices(start, end)
            .into_iter()
            .filter_map(|index| array.entries.get(&index))
            .collect::<Vec<_>>();
        match *op {
            op if eq_ignore_ascii_case(op, b"USED") => int(values.len() as i64),
            op if eq_ignore_ascii_case(op, b"MATCH") => {
                let [expected] = op_arg else {
                    return error("ERR syntax error");
                };
                int(values
                    .iter()
                    .filter(|value| value.as_slice() == *expected)
                    .count() as i64)
            }
            op if eq_ignore_ascii_case(op, b"SUM") => numeric_aggregate(values, NumericOp::Sum),
            op if eq_ignore_ascii_case(op, b"MIN") => numeric_aggregate(values, NumericOp::Min),
            op if eq_ignore_ascii_case(op, b"MAX") => numeric_aggregate(values, NumericOp::Max),
            op if eq_ignore_ascii_case(op, b"AND") => integer_aggregate(values, IntegerOp::And),
            op if eq_ignore_ascii_case(op, b"OR") => integer_aggregate(values, IntegerOp::Or),
            op if eq_ignore_ascii_case(op, b"XOR") => integer_aggregate(values, IntegerOp::Xor),
            _ => error("ERR syntax error"),
        }
    }
}

impl crate::commands::redis::RedisCommand for ArCount {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key] = args else {
            return wrong_arity("ARCOUNT");
        };
        match array_read(store, key) {
            Ok(Some(array)) => int(array.entries.len() as i64),
            Ok(None) => int(0),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for ArLen {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key] = args else {
            return wrong_arity("ARLEN");
        };
        match array_read(store, key) {
            Ok(Some(array)) => int(array_len(&array)),
            Ok(None) => int(0),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for ArNext {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key] = args else {
            return wrong_arity("ARNEXT");
        };
        match array_read(store, key) {
            Ok(Some(array)) => int(array.next_index),
            Ok(None) => int(0),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for ArLastItems {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, count_raw, options @ ..] = args else {
            return wrong_arity("ARLASTITEMS");
        };
        let count = match parse_usize(count_raw) {
            Ok(count) => count,
            Err(()) => return error("ERR value is not an integer or out of range"),
        };
        let rev = match options {
            [] => false,
            [option] if eq_ignore_ascii_case(option, b"REV") => true,
            _ => return error("ERR syntax error"),
        };
        match array_read(store, key) {
            Ok(Some(array)) => {
                let mut values = array
                    .last
                    .iter()
                    .rev()
                    .take(count)
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>();
                if rev {
                    values.reverse();
                }
                array_bulk(values)
            }
            Ok(None) => Frame::Array(Vec::new()),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for ArInfo {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let key = match args {
            [key] => *key,
            [key, _] => *key,
            _ => return wrong_arity("ARINFO"),
        };
        match array_read(store, key) {
            Ok(Some(array)) => Frame::Array(vec![
                bulk(b"length".to_vec()),
                int(array_len(&array)),
                bulk(b"count".to_vec()),
                int(array.entries.len() as i64),
                bulk(b"next-index".to_vec()),
                int(array.next_index),
                bulk(b"ring-size".to_vec()),
                int(array.ring_size.unwrap_or(0)),
            ]),
            Ok(None) => Frame::Null,
            Err(frame) => frame,
        }
    }
}

#[derive(Debug, Clone)]
struct ArGrepArgs<'a> {
    key: &'a [u8],
    start: i64,
    end: i64,
    predicates: Vec<TextPredicate<'a>>,
    any: bool,
    limit: Option<usize>,
    with_values: bool,
    nocase: bool,
}

impl ArGrepArgs<'_> {
    fn matches(&self, value: &[u8]) -> bool {
        if self.predicates.is_empty() {
            return false;
        }
        let mut matches = self.predicates.iter().map(|predicate| {
            let value = maybe_lower(value, self.nocase);
            predicate.matches(&value, self.nocase)
        });
        if self.any {
            matches.any(|matched| matched)
        } else {
            matches.all(|matched| matched)
        }
    }
}

#[derive(Debug, Clone)]
enum TextPredicate<'a> {
    Exact(&'a [u8]),
    Match(&'a [u8]),
    Glob(&'a [u8]),
    Re(&'a [u8]),
}

impl TextPredicate<'_> {
    fn matches(&self, value: &[u8], nocase: bool) -> bool {
        match self {
            Self::Exact(expected) => value == maybe_lower(expected, nocase).as_slice(),
            Self::Match(expected) | Self::Re(expected) => {
                contains_bytes(value, &maybe_lower(expected, nocase))
            }
            Self::Glob(pattern) => glob_matches_bytes(&maybe_lower(pattern, nocase), value),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum NumericOp {
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy)]
enum IntegerOp {
    And,
    Or,
    Xor,
}

fn array_write(
    store: &EmbeddedStore,
    key: &[u8],
    op: impl FnOnce(&mut ArrayState) -> Frame,
) -> Frame {
    match store.transform_string_value_no_ttl(
        key,
        |existing| {
            let mut array = match decode_array(existing) {
                Ok(array) => array,
                Err(()) => return Err(wrongtype()),
            };
            let frame = op(&mut array);
            Ok((frame, encode_array(&array)))
        },
        wrongtype,
    ) {
        Ok(frame) => frame,
        Err(frame) => frame,
    }
}

fn array_read(store: &EmbeddedStore, key: &[u8]) -> Result<Option<ArrayState>, Frame> {
    match optional_string_value(store, key, false)? {
        Some(value) if value.starts_with(ARRAY_PREFIX) => decode_array(Some(&value))
            .map(Some)
            .map_err(|_| wrongtype()),
        Some(_) => Err(wrongtype()),
        None => Ok(None),
    }
}

fn decode_array(existing: Option<&[u8]>) -> Result<ArrayState, ()> {
    let Some(mut raw) = existing else {
        return Ok(ArrayState::default());
    };
    if !raw.starts_with(ARRAY_PREFIX) {
        return Err(());
    }
    raw = &raw[ARRAY_PREFIX.len()..];
    let next_index = read_i64(&mut raw)?;
    let ring_size = match read_i64(&mut raw)? {
        value if value < 0 => None,
        value => Some(value),
    };
    let entry_count = read_u32(&mut raw)? as usize;
    let mut entries = BTreeMap::new();
    for _ in 0..entry_count {
        let index = read_i64(&mut raw)?;
        let value = read_bytes(&mut raw)?;
        entries.insert(index, value);
    }
    let last_count = read_u32(&mut raw)? as usize;
    let mut last = Vec::with_capacity(last_count);
    for _ in 0..last_count {
        let index = read_i64(&mut raw)?;
        let value = read_bytes(&mut raw)?;
        last.push((index, value));
    }
    Ok(ArrayState {
        next_index,
        ring_size,
        entries,
        last,
    })
}

fn encode_array(array: &ArrayState) -> Bytes {
    let mut out = Vec::new();
    out.extend_from_slice(ARRAY_PREFIX);
    out.extend_from_slice(&array.next_index.to_le_bytes());
    out.extend_from_slice(&array.ring_size.unwrap_or(-1).to_le_bytes());
    out.extend_from_slice(&(array.entries.len() as u32).to_le_bytes());
    for (index, value) in &array.entries {
        out.extend_from_slice(&index.to_le_bytes());
        write_bytes(&mut out, value);
    }
    out.extend_from_slice(&(array.last.len() as u32).to_le_bytes());
    for (index, value) in &array.last {
        out.extend_from_slice(&index.to_le_bytes());
        write_bytes(&mut out, value);
    }
    out
}

fn parse_indices(args: &[&[u8]]) -> Result<Vec<i64>, Frame> {
    args.iter()
        .map(|raw| {
            parse_i64(raw)
                .ok()
                .filter(|index| *index >= 0)
                .ok_or_else(|| error("ERR value is not an integer or out of range"))
        })
        .collect()
}

fn parse_index_value_pairs<'a>(args: &'a [&'a [u8]]) -> Result<Vec<(i64, &'a [u8])>, Frame> {
    args.chunks_exact(2)
        .map(|pair| {
            let index = parse_i64(pair[0])
                .ok()
                .filter(|index| *index >= 0)
                .ok_or_else(|| error("ERR value is not an integer or out of range"))?;
            Ok((index, pair[1]))
        })
        .collect()
}

fn parse_ranges(args: &[&[u8]]) -> Result<Vec<(i64, i64)>, Frame> {
    args.chunks_exact(2)
        .map(|pair| parse_start_end(pair[0], pair[1]))
        .collect()
}

fn parse_start_end(start: &[u8], end: &[u8]) -> Result<(i64, i64), Frame> {
    let start =
        parse_i64(start).map_err(|_| error("ERR value is not an integer or out of range"))?;
    let end = parse_i64(end).map_err(|_| error("ERR value is not an integer or out of range"))?;
    Ok((start.max(0), end.max(0)))
}

fn parse_limit_option(args: &[&[u8]]) -> Result<Option<usize>, Frame> {
    match args {
        [] => Ok(None),
        [limit, raw] if eq_ignore_ascii_case(limit, b"LIMIT") => parse_usize(raw)
            .map(Some)
            .map_err(|_| error("ERR value is not an integer or out of range")),
        _ => Err(error("ERR syntax error")),
    }
}

fn parse_argrep_args<'a>(args: &'a [&'a [u8]]) -> Result<ArGrepArgs<'a>, Frame> {
    let [key, start_raw, end_raw, rest @ ..] = args else {
        return Err(wrong_arity("ARGREP"));
    };
    let (start, end) = parse_start_end(start_raw, end_raw)?;
    let mut predicates = Vec::new();
    let mut any = false;
    let mut limit = None;
    let mut with_values = false;
    let mut nocase = false;
    let mut index = 0usize;
    while index < rest.len() {
        match rest[index] {
            token
                if eq_ignore_ascii_case(token, b"EXACT")
                    || eq_ignore_ascii_case(token, b"MATCH")
                    || eq_ignore_ascii_case(token, b"GLOB")
                    || eq_ignore_ascii_case(token, b"RE") =>
            {
                let Some(value) = rest.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                predicates.push(match token {
                    token if eq_ignore_ascii_case(token, b"EXACT") => TextPredicate::Exact(value),
                    token if eq_ignore_ascii_case(token, b"MATCH") => TextPredicate::Match(value),
                    token if eq_ignore_ascii_case(token, b"GLOB") => TextPredicate::Glob(value),
                    _ => TextPredicate::Re(value),
                });
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"AND") => {
                any = false;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"OR") => {
                any = true;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"LIMIT") => {
                let Some(raw) = rest.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                limit = Some(
                    parse_usize(raw)
                        .map_err(|_| error("ERR value is not an integer or out of range"))?,
                );
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"WITHVALUES") => {
                with_values = true;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"NOCASE") => {
                nocase = true;
                index += 1;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok(ArGrepArgs {
        key,
        start,
        end,
        predicates,
        any,
        limit,
        with_values,
        nocase,
    })
}

fn inclusive_indices(start: i64, end: i64) -> Vec<i64> {
    if start <= end {
        (start..=end).collect()
    } else {
        (end..=start).rev().collect()
    }
}

fn range_keys(entries: &BTreeMap<i64, Bytes>, start: i64, end: i64) -> Vec<i64> {
    inclusive_indices(start, end)
        .into_iter()
        .filter(|index| entries.contains_key(index))
        .collect()
}

fn array_len(array: &ArrayState) -> i64 {
    array
        .entries
        .keys()
        .next_back()
        .map(|index| index + 1)
        .unwrap_or(0)
}

fn numeric_aggregate(values: Vec<&Bytes>, op: NumericOp) -> Frame {
    let parsed = values
        .iter()
        .map(|value| {
            std::str::from_utf8(value)
                .ok()
                .and_then(|text| text.parse::<f64>().ok())
        })
        .collect::<Option<Vec<_>>>();
    let Some(parsed) = parsed else {
        return error("ERR value is not a float");
    };
    if parsed.is_empty() {
        return bulk(b"0".to_vec());
    }
    let value = match op {
        NumericOp::Sum => parsed.iter().sum(),
        NumericOp::Min => parsed
            .iter()
            .copied()
            .fold(f64::INFINITY, |left, right| left.min(right)),
        NumericOp::Max => parsed
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, |left, right| left.max(right)),
    };
    bulk(format_number(value).into_bytes())
}

fn integer_aggregate(values: Vec<&Bytes>, op: IntegerOp) -> Frame {
    let parsed = values
        .iter()
        .map(|value| {
            std::str::from_utf8(value)
                .ok()
                .and_then(|text| text.parse::<i64>().ok())
        })
        .collect::<Option<Vec<_>>>();
    let Some(mut parsed) = parsed else {
        return error("ERR value is not an integer or out of range");
    };
    let Some(first) = parsed.first().copied() else {
        return int(0);
    };
    let value = parsed.drain(1..).fold(first, |left, right| match op {
        IntegerOp::And => left & right,
        IntegerOp::Or => left | right,
        IntegerOp::Xor => left ^ right,
    });
    int(value)
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 && value.is_finite() {
        (value as i64).to_string()
    } else {
        value.to_string()
    }
}

fn maybe_lower(value: &[u8], enabled: bool) -> Vec<u8> {
    if enabled {
        value.to_ascii_lowercase()
    } else {
        value.to_vec()
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty()
        || haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

fn glob_matches_bytes(pattern: &[u8], value: &[u8]) -> bool {
    if pattern == b"*" {
        return true;
    }
    match pattern.iter().position(|byte| *byte == b'*') {
        Some(index) => {
            value.starts_with(&pattern[..index]) && value.ends_with(&pattern[index + 1..])
        }
        None => pattern == value,
    }
}

fn read_i64(raw: &mut &[u8]) -> Result<i64, ()> {
    if raw.len() < 8 {
        return Err(());
    }
    let (head, tail) = raw.split_at(8);
    *raw = tail;
    Ok(i64::from_le_bytes(head.try_into().map_err(|_| ())?))
}

fn read_u32(raw: &mut &[u8]) -> Result<u32, ()> {
    if raw.len() < 4 {
        return Err(());
    }
    let (head, tail) = raw.split_at(4);
    *raw = tail;
    Ok(u32::from_le_bytes(head.try_into().map_err(|_| ())?))
}

fn read_bytes(raw: &mut &[u8]) -> Result<Bytes, ()> {
    let len = read_u32(raw)? as usize;
    if raw.len() < len {
        return Err(());
    }
    let (head, tail) = raw.split_at(len);
    *raw = tail;
    Ok(head.to_vec())
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}
