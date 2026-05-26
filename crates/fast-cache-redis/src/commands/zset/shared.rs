use crate::storage::{RedisKeyStore, RedisZSetStore};
use bytes::BytesMut;

use crate::commands::formal_range::normalize_redis_range;
#[cfg(feature = "server")]
use crate::commands::redis::write_result_resp;
use crate::commands::redis::{
    array_bulk, bulk, error, int, parse_f64, parse_i64, parse_usize, reserve_resp_bulk_array_hint,
    write_frame, write_resp_array_header, write_resp_null, write_resp_wrong_arity,
    write_resp_wrongtype, wrong_arity, wrongtype, zentries_frame,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{
    EmbeddedStore, RedisObjectError, RedisObjectReadOutcome, RedisObjectResult, RedisObjectValue,
    RedisObjectZSetRangeItem,
};

#[cfg(feature = "server")]
pub(crate) fn write_zrange_rank_resp(
    store: &EmbeddedStore,
    key: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
    with_scores: bool,
    out: &mut BytesMut,
) {
    match store.zrange_entries_visit(key, start, stop, rev, |item| match item {
        RedisObjectZSetRangeItem::Begin(count) => {
            let len = if with_scores {
                count.saturating_mul(2)
            } else {
                count
            };
            reserve_resp_bulk_array_hint(out, len);
            write_resp_array_header(out, len);
        }
        RedisObjectZSetRangeItem::Entry { member, score } => {
            ServerWire::write_resp_blob_string(out, member);
            if with_scores {
                write_resp_score(out, score);
            }
        }
    }) {
        RedisObjectReadOutcome::Written => {}
        RedisObjectReadOutcome::Missing => write_resp_array_header(out, 0),
        RedisObjectReadOutcome::WrongType => write_frame(out, &wrongtype()),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_zrange_rank_fast(
    store: &EmbeddedStore,
    key: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
    with_scores: bool,
    out: &mut BytesMut,
) {
    let mut array_start = None;
    match store.zrange_entries_visit(key, start, stop, rev, |item| match item {
        RedisObjectZSetRangeItem::Begin(count) => {
            let len = if with_scores {
                count.saturating_mul(2)
            } else {
                count
            };
            array_start = Some(ServerWire::begin_fast_array(out, len));
        }
        RedisObjectZSetRangeItem::Entry { member, score } => {
            ServerWire::write_fast_array_item(out, Some(member));
            if with_scores {
                write_fast_score_array_item(out, score);
            }
        }
    }) {
        RedisObjectReadOutcome::Written => {
            if let Some(start) = array_start {
                ServerWire::finish_fast_array(out, start);
            } else {
                ServerWire::write_fast_empty_array(out);
            }
        }
        RedisObjectReadOutcome::Missing => ServerWire::write_fast_empty_array(out),
        RedisObjectReadOutcome::WrongType => {
            ServerWire::write_fast_error(out, crate::storage::WRONGTYPE_MESSAGE)
        }
    }
}

pub(crate) fn zrange_by_rank_impl(
    store: &EmbeddedStore,
    key: &[u8],
    start: i64,
    stop: i64,
    rev: bool,
    with_scores: bool,
) -> Frame {
    let mut entries = match store.zentries(key) {
        Ok(entries) => entries,
        Err(RedisObjectError::WrongType) => return wrongtype(),
        Err(RedisObjectError::MissingKey) => Vec::new(),
    };
    if rev {
        entries.reverse();
    }
    let Some(range) = normalize_redis_range(entries.len(), start, stop) else {
        return Frame::Array(Vec::new());
    };
    let (start, stop) = range.into_bounds();
    zentries_frame(entries[start..=stop].to_vec(), with_scores)
}

pub(crate) fn zrange_by_score_impl(
    store: &EmbeddedStore,
    key: &[u8],
    min: &[u8],
    max: &[u8],
    rev: bool,
    with_scores: bool,
    limit: Option<(usize, usize)>,
) -> Frame {
    let lower = if rev { max } else { min };
    let upper_bound = if rev { min } else { max };
    let Ok(lower) = crate::commands::redis::parse_score_bound(lower) else {
        return error("ERR min or max is not a float");
    };
    let Ok(upper) = crate::commands::redis::parse_score_bound(upper_bound) else {
        return error("ERR min or max is not a float");
    };
    let mut entries = match store.zentries(key) {
        Ok(entries) => entries,
        Err(RedisObjectError::WrongType) => return wrongtype(),
        Err(RedisObjectError::MissingKey) => Vec::new(),
    };
    entries.retain(|(_, score)| lower.contains(*score, true) && upper.contains(*score, false));
    if rev {
        entries.reverse();
    }
    if let Some((offset, count)) = limit {
        entries = entries.into_iter().skip(offset).take(count).collect();
    }
    zentries_frame(entries, with_scores)
}

#[cfg(feature = "server")]
pub(crate) fn write_zrange_score_resp(
    store: &EmbeddedStore,
    key: &[u8],
    min: &[u8],
    max: &[u8],
    rev: bool,
    with_scores: bool,
    limit: Option<(usize, usize)>,
    out: &mut BytesMut,
) {
    let lower = if rev { max } else { min };
    let upper_bound = if rev { min } else { max };
    let Ok(lower) = crate::commands::redis::parse_score_bound(lower) else {
        ServerWire::write_resp_error(out, "ERR min or max is not a float");
        return;
    };
    let Ok(upper) = crate::commands::redis::parse_score_bound(upper_bound) else {
        ServerWire::write_resp_error(out, "ERR min or max is not a float");
        return;
    };
    let mut entries = match store.zentries(key) {
        Ok(entries) => entries,
        Err(RedisObjectError::WrongType) => {
            write_resp_wrongtype(out);
            return;
        }
        Err(RedisObjectError::MissingKey) => Vec::new(),
    };
    entries.retain(|(_, score)| lower.contains(*score, true) && upper.contains(*score, false));
    if rev {
        entries.reverse();
    }
    if let Some((offset, count)) = limit {
        entries = entries.into_iter().skip(offset).take(count).collect();
    }
    write_zentries_resp(out, entries, with_scores);
}

#[cfg(feature = "server")]
pub(crate) fn write_zrange_score_fast(
    store: &EmbeddedStore,
    key: &[u8],
    min: &[u8],
    max: &[u8],
    rev: bool,
    with_scores: bool,
    limit: Option<(usize, usize)>,
    out: &mut BytesMut,
) {
    let lower = if rev { max } else { min };
    let upper_bound = if rev { min } else { max };
    let Ok(lower) = crate::commands::redis::parse_score_bound(lower) else {
        ServerWire::write_fast_error(out, "ERR min or max is not a float");
        return;
    };
    let Ok(upper) = crate::commands::redis::parse_score_bound(upper_bound) else {
        ServerWire::write_fast_error(out, "ERR min or max is not a float");
        return;
    };
    let mut entries = match store.zentries(key) {
        Ok(entries) => entries,
        Err(RedisObjectError::WrongType) => {
            ServerWire::write_fast_error(out, crate::storage::WRONGTYPE_MESSAGE);
            return;
        }
        Err(RedisObjectError::MissingKey) => Vec::new(),
    };
    entries.retain(|(_, score)| lower.contains(*score, true) && upper.contains(*score, false));
    if rev {
        entries.reverse();
    }
    if let Some((offset, count)) = limit {
        entries = entries.into_iter().skip(offset).take(count).collect();
    }
    write_zentries_fast(out, entries, with_scores);
}

#[cfg(feature = "server")]
pub(crate) fn write_resp_score(out: &mut BytesMut, score: f64) {
    if score.fract() == 0.0 && score.is_finite() {
        let mut buffer = itoa::Buffer::new();
        ServerWire::write_resp_blob_string(out, buffer.format(score as i64).as_bytes());
    } else {
        let score = score.to_string();
        ServerWire::write_resp_blob_string(out, score.as_bytes());
    }
}

#[cfg(feature = "server")]
fn write_fast_score_array_item(out: &mut BytesMut, score: f64) {
    if score.fract() == 0.0 && score.is_finite() {
        let mut buffer = itoa::Buffer::new();
        ServerWire::write_fast_array_item(out, Some(buffer.format(score as i64).as_bytes()));
    } else {
        let score = score.to_string();
        ServerWire::write_fast_array_item(out, Some(score.as_bytes()));
    }
}

pub(crate) fn zrank(store: &EmbeddedStore, args: &[&[u8]], rev: bool) -> Frame {
    match args {
        [key, member] => match store.zrank_value(key, member, rev) {
            Ok(Some(rank)) => int(rank as i64),
            Ok(None) | Err(RedisObjectError::MissingKey) => Frame::Null,
            Err(RedisObjectError::WrongType) => wrongtype(),
        },
        _ => wrong_arity(if rev { "ZREVRANK" } else { "ZRANK" }),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_zrank_like_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    rev: bool,
    out: &mut BytesMut,
) {
    match args {
        [key, member] => match store.zrank_value(key, member, rev) {
            Ok(Some(rank)) => ServerWire::write_resp_integer(out, rank as i64),
            Ok(None) | Err(RedisObjectError::MissingKey) => write_resp_null(out),
            Err(RedisObjectError::WrongType) => write_frame(out, &wrongtype()),
        },
        _ => write_frame(out, &wrong_arity(if rev { "ZREVRANK" } else { "ZRANK" })),
    }
}

pub(crate) fn zpop(store: &EmbeddedStore, args: &[&[u8]], max: bool) -> Frame {
    match args {
        [key] => crate::commands::redis::frame_from_result(store.zpop(key, 1, max)),
        [key, count] => match parse_usize(count) {
            Ok(count) => crate::commands::redis::frame_from_result(store.zpop(key, count, max)),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity(if max { "ZPOPMAX" } else { "ZPOPMIN" }),
    }
}

pub(crate) fn zmpop(store: &EmbeddedStore, args: &[&[u8]], blocking: bool) -> Frame {
    let parsed = match parse_zmpop_args(args, blocking) {
        Ok(parsed) => parsed,
        Err(frame) => return frame,
    };
    for key in parsed.keys {
        match store.zpop(key, parsed.count, parsed.max) {
            RedisObjectResult::Array(values) if values.is_empty() => {}
            RedisObjectResult::Array(values) => {
                return Frame::Array(vec![bulk((*key).to_vec()), zmpop_entries_frame(values)]);
            }
            RedisObjectResult::WrongType => return wrongtype(),
            _ => {}
        }
    }
    Frame::Null
}

#[derive(Clone, Copy)]
struct ZMpopArgs<'a> {
    keys: &'a [&'a [u8]],
    max: bool,
    count: usize,
}

#[derive(Clone, Copy)]
enum ZMpopCommand {
    Blocking,
    NonBlocking,
}

impl ZMpopCommand {
    fn from_blocking(blocking: bool) -> Self {
        match blocking {
            true => Self::Blocking,
            false => Self::NonBlocking,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Blocking => "BZMPOP",
            Self::NonBlocking => "ZMPOP",
        }
    }

    fn minimum_arity(self) -> usize {
        match self {
            Self::Blocking => 4,
            Self::NonBlocking => 3,
        }
    }

    fn validate_arity(self, args: &[&[u8]]) -> std::result::Result<(), Frame> {
        match args.len() >= self.minimum_arity() {
            true => Ok(()),
            false => Err(wrong_arity(self.name())),
        }
    }

    fn without_timeout<'a>(
        self,
        args: &'a [&'a [u8]],
    ) -> std::result::Result<&'a [&'a [u8]], Frame> {
        match self {
            Self::NonBlocking => Ok(args),
            Self::Blocking => {
                let (timeout, rest) = args.split_first().ok_or_else(|| wrong_arity(self.name()))?;
                validate_zmpop_timeout(timeout)?;
                Ok(rest)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ZMpopDirection {
    Min,
    Max,
}

impl ZMpopDirection {
    const NAMES: &'static [(&'static [u8], Self)] = &[(b"MIN", Self::Min), (b"MAX", Self::Max)];

    fn from_name(name: &[u8]) -> Option<Self> {
        Self::NAMES.iter().find_map(|(candidate, direction)| {
            crate::commands::redis::eq_ignore_ascii_case(name, candidate).then_some(*direction)
        })
    }

    fn pops_max(self) -> bool {
        match self {
            Self::Min => false,
            Self::Max => true,
        }
    }
}

#[derive(Clone, Copy)]
enum ZMpopOption {
    Count,
}

impl ZMpopOption {
    const NAMES: &'static [(&'static [u8], Self)] = &[(b"COUNT", Self::Count)];

    fn from_name(name: &[u8]) -> Option<Self> {
        Self::NAMES.iter().find_map(|(candidate, option)| {
            crate::commands::redis::eq_ignore_ascii_case(name, candidate).then_some(*option)
        })
    }
}

fn parse_zmpop_args<'a>(
    args: &'a [&'a [u8]],
    blocking: bool,
) -> std::result::Result<ZMpopArgs<'a>, Frame> {
    let command = ZMpopCommand::from_blocking(blocking);
    command.validate_arity(args)?;
    let args = command.without_timeout(args)?;
    let (keys, direction, options) = split_zmpop_key_direction(args)?;
    let direction = ZMpopDirection::from_name(direction).ok_or_else(zmpop_syntax_error)?;
    let count = parse_zmpop_options(options)?;
    Ok(ZMpopArgs {
        keys,
        max: direction.pops_max(),
        count,
    })
}

fn validate_zmpop_timeout(timeout: &[u8]) -> std::result::Result<(), Frame> {
    let timeout =
        parse_f64(timeout).map_err(|_| error("ERR timeout is not a float or out of range"))?;
    match timeout < 0.0 {
        true => Err(error("ERR timeout is negative")),
        false => Ok(()),
    }
}

fn split_zmpop_key_direction<'a>(
    args: &'a [&'a [u8]],
) -> std::result::Result<(&'a [&'a [u8]], &'a [u8], &'a [&'a [u8]]), Frame> {
    let (numkeys, rest) = args.split_first().ok_or_else(zmpop_syntax_error)?;
    let numkeys = parse_nonzero_zmpop_usize(numkeys, "ERR numkeys should be greater than 0")?;
    let (direction, options) = rest
        .get(numkeys..)
        .and_then(|remaining| remaining.split_first())
        .ok_or_else(zmpop_syntax_error)?;
    let keys = rest.get(..numkeys).ok_or_else(zmpop_syntax_error)?;
    Ok((keys, *direction, options))
}

fn parse_zmpop_options(mut args: &[&[u8]]) -> std::result::Result<usize, Frame> {
    let mut count = 1;
    while let Some((name, rest)) = args.split_first() {
        match ZMpopOption::from_name(name).ok_or_else(zmpop_syntax_error)? {
            ZMpopOption::Count => {
                let (value, tail) = rest.split_first().ok_or_else(zmpop_syntax_error)?;
                count = parse_nonzero_zmpop_usize(value, "ERR count should be greater than 0")?;
                args = tail;
            }
        }
    }
    Ok(count)
}

fn parse_nonzero_zmpop_usize(
    value: &[u8],
    zero_error: &'static str,
) -> std::result::Result<usize, Frame> {
    let value =
        parse_usize(value).map_err(|_| error("ERR value is not an integer or out of range"))?;
    match value {
        0 => Err(error(zero_error)),
        value => Ok(value),
    }
}

fn zmpop_syntax_error() -> Frame {
    error("ERR syntax error")
}

fn zmpop_entries_frame(values: Vec<Option<Vec<u8>>>) -> Frame {
    let mut entries = Vec::with_capacity(values.len() / 2);
    for pair in values.chunks(2) {
        let member = pair.first().and_then(|value| value.clone());
        let score = pair.get(1).and_then(|value| value.clone());
        entries.push(Frame::Array(vec![
            member.map_or(Frame::Null, bulk),
            score.map_or(Frame::Null, bulk),
        ]));
    }
    Frame::Array(entries)
}

#[cfg(feature = "server")]
pub(crate) fn write_zpop_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    max: bool,
    out: &mut BytesMut,
) {
    match args {
        [key] => write_result_resp(out, store.zpop(key, 1, max)),
        [key, count] => match parse_usize(count) {
            Ok(count) => write_result_resp(out, store.zpop(key, count, max)),
            Err(_) => {
                ServerWire::write_resp_error(out, "ERR value is not an integer or out of range")
            }
        },
        _ => write_frame(out, &wrong_arity(if max { "ZPOPMAX" } else { "ZPOPMIN" })),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_zmpop_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    blocking: bool,
    out: &mut BytesMut,
) {
    write_frame(out, &zmpop(store, args, blocking));
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_zrange_rank_resp(
    _store: &EmbeddedStore,
    _key: &[u8],
    _start: i64,
    _stop: i64,
    _rev: bool,
    _with_scores: bool,
    _out: &mut BytesMut,
) {
    unreachable!("RESP zset writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_zrange_rank_fast(
    _store: &EmbeddedStore,
    _key: &[u8],
    _start: i64,
    _stop: i64,
    _rev: bool,
    _with_scores: bool,
    _out: &mut BytesMut,
) {
    unreachable!("FCNP zset writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_zrange_score_resp(
    _store: &EmbeddedStore,
    _key: &[u8],
    _min: &[u8],
    _max: &[u8],
    _rev: bool,
    _with_scores: bool,
    _limit: Option<(usize, usize)>,
    _out: &mut BytesMut,
) {
    unreachable!("RESP zset writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_zrange_score_fast(
    _store: &EmbeddedStore,
    _key: &[u8],
    _min: &[u8],
    _max: &[u8],
    _rev: bool,
    _with_scores: bool,
    _limit: Option<(usize, usize)>,
    _out: &mut BytesMut,
) {
    unreachable!("FCNP zset writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_resp_score(_out: &mut BytesMut, _score: f64) {
    unreachable!("RESP zset writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_zrank_like_resp(
    _store: &EmbeddedStore,
    _args: &[&[u8]],
    _rev: bool,
    _out: &mut BytesMut,
) {
    unreachable!("RESP zset writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_zpop_resp(
    _store: &EmbeddedStore,
    _args: &[&[u8]],
    _max: bool,
    _out: &mut BytesMut,
) {
    unreachable!("RESP zset writers are only called by the server feature")
}

#[cfg(not(feature = "server"))]
pub(crate) fn write_zmpop_resp(
    _store: &EmbeddedStore,
    _args: &[&[u8]],
    _blocking: bool,
    _out: &mut BytesMut,
) {
    unreachable!("RESP zset writers are only called by the server feature")
}

pub(crate) fn zrangebylex(store: &EmbeddedStore, args: &[&[u8]], rev: bool) -> Frame {
    match args {
        [key, min, max] => {
            let lower = if rev { *max } else { *min };
            let upper = if rev { *min } else { *max };
            let (Ok(min), Ok(max)) = (
                crate::commands::redis::parse_lex_bound(lower),
                crate::commands::redis::parse_lex_bound(upper),
            ) else {
                return error("ERR min or max not valid string range item");
            };
            let mut entries = match store.zentries(key) {
                Ok(entries) => entries,
                Err(RedisObjectError::WrongType) => return wrongtype(),
                Err(RedisObjectError::MissingKey) => Vec::new(),
            };
            entries.retain(|(member, _)| {
                min.contains(member.as_slice(), true) && max.contains(member.as_slice(), false)
            });
            if rev {
                entries.reverse();
            }
            array_bulk(entries.into_iter().map(|(member, _)| member).collect())
        }
        _ => wrong_arity(if rev { "ZREVRANGEBYLEX" } else { "ZRANGEBYLEX" }),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_zrange_lex_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    rev: bool,
    out: &mut BytesMut,
) {
    match args {
        [key, min, max] => {
            let lower = if rev { *max } else { *min };
            let upper = if rev { *min } else { *max };
            let (Ok(min), Ok(max)) = (
                crate::commands::redis::parse_lex_bound(lower),
                crate::commands::redis::parse_lex_bound(upper),
            ) else {
                ServerWire::write_resp_error(out, "ERR min or max not valid string range item");
                return;
            };
            let mut entries = match store.zentries(key) {
                Ok(entries) => entries,
                Err(RedisObjectError::WrongType) => {
                    write_resp_wrongtype(out);
                    return;
                }
                Err(RedisObjectError::MissingKey) => Vec::new(),
            };
            entries.retain(|(member, _)| {
                min.contains(member.as_slice(), true) && max.contains(member.as_slice(), false)
            });
            if rev {
                entries.reverse();
            }
            reserve_resp_bulk_array_hint(out, entries.len());
            write_resp_array_header(out, entries.len());
            for (member, _) in entries {
                ServerWire::write_resp_blob_string(out, &member);
            }
        }
        _ => write_resp_wrong_arity(out, if rev { "ZREVRANGEBYLEX" } else { "ZRANGEBYLEX" }),
    }
}

#[cfg(feature = "server")]
fn write_zentries_resp(out: &mut BytesMut, entries: Vec<(Vec<u8>, f64)>, with_scores: bool) {
    let len = if with_scores {
        entries.len().saturating_mul(2)
    } else {
        entries.len()
    };
    reserve_resp_bulk_array_hint(out, len);
    write_resp_array_header(out, len);
    for (member, score) in entries {
        ServerWire::write_resp_blob_string(out, &member);
        if with_scores {
            write_resp_score(out, score);
        }
    }
}

#[cfg(feature = "server")]
fn write_zentries_fast(out: &mut BytesMut, entries: Vec<(Vec<u8>, f64)>, with_scores: bool) {
    let len = if with_scores {
        entries.len().saturating_mul(2)
    } else {
        entries.len()
    };
    let start = ServerWire::begin_fast_array(out, len);
    for (member, score) in entries {
        ServerWire::write_fast_array_item(out, Some(&member));
        if with_scores {
            write_fast_score_array_item(out, score);
        }
    }
    ServerWire::finish_fast_array(out, start);
}

pub(crate) fn zrangestore_len(
    store: &EmbeddedStore,
    args: &[&[u8]],
) -> std::result::Result<usize, Frame> {
    match args {
        [dest, source, start, stop] => {
            let (Ok(start), Ok(stop)) = (parse_i64(start), parse_i64(stop)) else {
                return Err(error("ERR value is not an integer or out of range"));
            };
            let entries = match store.zentries(source) {
                Ok(entries) => entries,
                Err(RedisObjectError::WrongType) => return Err(wrongtype()),
                Err(RedisObjectError::MissingKey) => Vec::new(),
            };
            let selected = normalize_redis_range(entries.len(), start, stop)
                .map(|range| {
                    let (start, stop) = range.into_bounds();
                    entries[start..=stop].to_vec()
                })
                .unwrap_or_default();
            let len = selected.len();
            store.set_object_value(dest, RedisObjectValue::ZSet(selected), None);
            Ok(len)
        }
        _ => Err(wrong_arity("ZRANGESTORE")),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ZAggregateKind {
    Union,
    Inter,
    Diff,
}

impl ZAggregateKind {
    pub(crate) fn store_name(self) -> &'static str {
        match self {
            Self::Union => "ZUNIONSTORE",
            Self::Inter => "ZINTERSTORE",
            Self::Diff => "ZDIFFSTORE",
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Union => "ZUNION",
            Self::Inter => "ZINTER",
            Self::Diff => "ZDIFF",
        }
    }
}

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

pub(crate) fn zaggregate_store(
    store: &EmbeddedStore,
    args: &[&[u8]],
    kind: ZAggregateKind,
) -> Frame {
    if args.len() < 3 {
        return wrong_arity(kind.store_name());
    }
    let Ok(numkeys) = parse_usize(args[1]) else {
        return error("ERR value is not an integer or out of range");
    };
    if args.len() < 2 + numkeys {
        return error("ERR syntax error");
    }
    let dest = args[0];
    let keys = &args[2..2 + numkeys];
    let mut weights = vec![1.0; numkeys];
    let mut aggregate = Aggregate::Sum;
    let mut index = 2 + numkeys;
    while index < args.len() {
        let option = args[index];
        match option {
            option
                if crate::commands::redis::eq_ignore_ascii_case(option, b"WEIGHTS")
                    && index + numkeys < args.len() =>
            {
                for (weight, raw) in weights
                    .iter_mut()
                    .zip(&args[index + 1..index + 1 + numkeys])
                {
                    let Ok(parsed) = parse_f64(raw) else {
                        return error("ERR weight value is not a float");
                    };
                    *weight = parsed;
                }
                index += 1 + numkeys;
            }
            option
                if crate::commands::redis::eq_ignore_ascii_case(option, b"AGGREGATE")
                    && index + 1 < args.len() =>
            {
                aggregate = match args[index + 1] {
                    raw if crate::commands::redis::eq_ignore_ascii_case(raw, b"SUM") => {
                        Aggregate::Sum
                    }
                    raw if crate::commands::redis::eq_ignore_ascii_case(raw, b"MIN") => {
                        Aggregate::Min
                    }
                    raw if crate::commands::redis::eq_ignore_ascii_case(raw, b"MAX") => {
                        Aggregate::Max
                    }
                    _ => return error("ERR syntax error"),
                };
                index += 2;
            }
            _ => return error("ERR syntax error"),
        }
    }
    let entries = match compute_zaggregate(store, keys, &weights, kind, aggregate) {
        Ok(entries) => entries,
        Err(frame) => return frame,
    };
    store.set_object_value(dest, RedisObjectValue::ZSet(entries.clone()), None);
    int(entries.len() as i64)
}

#[cfg(feature = "server")]
pub(crate) fn write_zaggregate_store_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    kind: ZAggregateKind,
    out: &mut BytesMut,
) {
    if args.len() < 3 {
        write_resp_wrong_arity(out, kind.store_name());
        return;
    }
    let Ok(numkeys) = parse_usize(args[1]) else {
        ServerWire::write_resp_error(out, "ERR value is not an integer or out of range");
        return;
    };
    if args.len() < 2 + numkeys {
        ServerWire::write_resp_error(out, "ERR syntax error");
        return;
    }
    let dest = args[0];
    let keys = &args[2..2 + numkeys];
    let mut weights = vec![1.0; numkeys];
    let mut aggregate = Aggregate::Sum;
    let mut index = 2 + numkeys;
    while index < args.len() {
        let option = args[index];
        match option {
            option
                if crate::commands::redis::eq_ignore_ascii_case(option, b"WEIGHTS")
                    && index + numkeys < args.len() =>
            {
                for (weight, raw) in weights
                    .iter_mut()
                    .zip(&args[index + 1..index + 1 + numkeys])
                {
                    let Ok(parsed) = parse_f64(raw) else {
                        ServerWire::write_resp_error(out, "ERR weight value is not a float");
                        return;
                    };
                    *weight = parsed;
                }
                index += 1 + numkeys;
            }
            option
                if crate::commands::redis::eq_ignore_ascii_case(option, b"AGGREGATE")
                    && index + 1 < args.len() =>
            {
                aggregate = match args[index + 1] {
                    raw if crate::commands::redis::eq_ignore_ascii_case(raw, b"SUM") => {
                        Aggregate::Sum
                    }
                    raw if crate::commands::redis::eq_ignore_ascii_case(raw, b"MIN") => {
                        Aggregate::Min
                    }
                    raw if crate::commands::redis::eq_ignore_ascii_case(raw, b"MAX") => {
                        Aggregate::Max
                    }
                    _ => {
                        ServerWire::write_resp_error(out, "ERR syntax error");
                        return;
                    }
                };
                index += 2;
            }
            _ => {
                ServerWire::write_resp_error(out, "ERR syntax error");
                return;
            }
        }
    }
    let entries = match compute_zaggregate(store, keys, &weights, kind, aggregate) {
        Ok(entries) => entries,
        Err(_) => {
            write_resp_wrongtype(out);
            return;
        }
    };
    let len = entries.len();
    store.set_object_value(dest, RedisObjectValue::ZSet(entries), None);
    ServerWire::write_resp_integer(out, len as i64);
}

pub(crate) fn zaggregate(store: &EmbeddedStore, args: &[&[u8]], kind: ZAggregateKind) -> Frame {
    let parsed = match parse_zaggregate_args(args, kind, false) {
        Ok(parsed) => parsed,
        Err(frame) => return frame,
    };
    let entries =
        match compute_zaggregate(store, parsed.keys, &parsed.weights, kind, parsed.aggregate) {
            Ok(entries) => entries,
            Err(frame) => return frame,
        };
    zentries_frame(entries, parsed.with_scores)
}

pub(crate) fn zintercard(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let parsed = match parse_zaggregate_args(args, ZAggregateKind::Inter, true) {
        Ok(parsed) => parsed,
        Err(frame) => return frame,
    };
    let entries = match compute_zaggregate(
        store,
        parsed.keys,
        &parsed.weights,
        ZAggregateKind::Inter,
        Aggregate::Sum,
    ) {
        Ok(entries) => entries,
        Err(frame) => return frame,
    };
    let len = match parsed.limit {
        Some(0) | None => entries.len(),
        Some(limit) => entries.len().min(limit),
    };
    int(len as i64)
}

struct ParsedZAggregate<'a> {
    keys: &'a [&'a [u8]],
    weights: Vec<f64>,
    aggregate: Aggregate,
    with_scores: bool,
    limit: Option<usize>,
}

fn parse_zaggregate_args<'a>(
    args: &'a [&'a [u8]],
    kind: ZAggregateKind,
    cardinality_only: bool,
) -> std::result::Result<ParsedZAggregate<'a>, Frame> {
    if args.len() < 2 {
        return Err(wrong_arity(match cardinality_only {
            true => "ZINTERCARD",
            false => kind.name(),
        }));
    }
    let Ok(numkeys) = parse_usize(args[0]) else {
        return Err(error("ERR value is not an integer or out of range"));
    };
    if numkeys == 0 || args.len() < 1 + numkeys {
        return Err(error("ERR syntax error"));
    }
    let keys = &args[1..1 + numkeys];
    let mut weights = vec![1.0; numkeys];
    let mut aggregate = Aggregate::Sum;
    let mut with_scores = false;
    let mut limit = None;
    let mut index = 1 + numkeys;
    while index < args.len() {
        let option = args[index];
        if cardinality_only {
            if crate::commands::redis::eq_ignore_ascii_case(option, b"LIMIT")
                && index + 1 < args.len()
            {
                let Ok(parsed) = parse_usize(args[index + 1]) else {
                    return Err(error("ERR value is not an integer or out of range"));
                };
                limit = Some(parsed);
                index += 2;
                continue;
            }
            return Err(error("ERR syntax error"));
        }
        match option {
            option if crate::commands::redis::eq_ignore_ascii_case(option, b"WITHSCORES") => {
                with_scores = true;
                index += 1;
            }
            option
                if kind != ZAggregateKind::Diff
                    && crate::commands::redis::eq_ignore_ascii_case(option, b"WEIGHTS")
                    && index + numkeys < args.len() =>
            {
                for (weight, raw) in weights
                    .iter_mut()
                    .zip(&args[index + 1..index + 1 + numkeys])
                {
                    let Ok(parsed) = parse_f64(raw) else {
                        return Err(error("ERR weight value is not a float"));
                    };
                    *weight = parsed;
                }
                index += 1 + numkeys;
            }
            option
                if kind != ZAggregateKind::Diff
                    && crate::commands::redis::eq_ignore_ascii_case(option, b"AGGREGATE")
                    && index + 1 < args.len() =>
            {
                aggregate = match args[index + 1] {
                    raw if crate::commands::redis::eq_ignore_ascii_case(raw, b"SUM") => {
                        Aggregate::Sum
                    }
                    raw if crate::commands::redis::eq_ignore_ascii_case(raw, b"MIN") => {
                        Aggregate::Min
                    }
                    raw if crate::commands::redis::eq_ignore_ascii_case(raw, b"MAX") => {
                        Aggregate::Max
                    }
                    _ => return Err(error("ERR syntax error")),
                };
                index += 2;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok(ParsedZAggregate {
        keys,
        weights,
        aggregate,
        with_scores,
        limit,
    })
}

fn compute_zaggregate(
    store: &EmbeddedStore,
    keys: &[&[u8]],
    weights: &[f64],
    kind: ZAggregateKind,
    aggregate: Aggregate,
) -> std::result::Result<Vec<(Vec<u8>, f64)>, Frame> {
    let mut maps = Vec::with_capacity(keys.len());
    for (key, weight) in keys.iter().zip(weights.iter().copied()) {
        let entries = match store.zentries(key) {
            Ok(entries) => entries,
            Err(RedisObjectError::WrongType) => return Err(wrongtype()),
            Err(RedisObjectError::MissingKey) => Vec::new(),
        };
        maps.push(
            entries
                .into_iter()
                .map(|(member, score)| (member, score * weight))
                .collect::<std::collections::BTreeMap<_, _>>(),
        );
    }

    let mut out = std::collections::BTreeMap::<Vec<u8>, f64>::new();
    match kind {
        ZAggregateKind::Union => {
            for map in maps {
                for (member, score) in map {
                    out.entry(member)
                        .and_modify(|existing| {
                            *existing = aggregate_score(*existing, score, aggregate)
                        })
                        .or_insert(score);
                }
            }
        }
        ZAggregateKind::Inter => {
            if let Some((first, rest)) = maps.split_first() {
                for (member, score) in first {
                    if rest.iter().all(|map| map.contains_key(member)) {
                        let combined = rest.iter().fold(*score, |acc, map| {
                            aggregate_score(acc, map[member], aggregate)
                        });
                        out.insert(member.clone(), combined);
                    }
                }
            }
        }
        ZAggregateKind::Diff => {
            if let Some((first, rest)) = maps.split_first() {
                for (member, score) in first {
                    if !rest.iter().any(|map| map.contains_key(member)) {
                        out.insert(member.clone(), *score);
                    }
                }
            }
        }
    }
    let mut entries = out.into_iter().collect::<Vec<_>>();
    entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
        left_score
            .total_cmp(right_score)
            .then_with(|| left_member.cmp(right_member))
    });
    Ok(entries)
}

fn aggregate_score(left: f64, right: f64, aggregate: Aggregate) -> f64 {
    match aggregate {
        Aggregate::Sum => left + right,
        Aggregate::Min => left.min(right),
        Aggregate::Max => left.max(right),
    }
}

pub(crate) fn bzpop(store: &EmbeddedStore, args: &[&[u8]], max: bool) -> Frame {
    if args.len() < 2 {
        return wrong_arity(if max { "BZPOPMAX" } else { "BZPOPMIN" });
    }
    for key in &args[..args.len() - 1] {
        let mut entries = match store.zentries(key) {
            Ok(entries) => entries,
            Err(RedisObjectError::WrongType) => return wrongtype(),
            Err(RedisObjectError::MissingKey) => Vec::new(),
        };
        if entries.is_empty() {
            continue;
        }
        if max {
            entries.reverse();
        }
        let (member, score) = entries[0].clone();
        let _ = store.zrem(key, &member);
        return Frame::Array(vec![
            bulk((*key).to_vec()),
            bulk(member),
            bulk(score.to_string().into_bytes()),
        ]);
    }
    Frame::Null
}

#[cfg(feature = "server")]
pub(crate) fn write_bzpop_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    max: bool,
    out: &mut BytesMut,
) {
    if args.len() < 2 {
        write_resp_wrong_arity(out, if max { "BZPOPMAX" } else { "BZPOPMIN" });
        return;
    }
    for key in &args[..args.len() - 1] {
        let mut entries = match store.zentries(key) {
            Ok(entries) => entries,
            Err(RedisObjectError::WrongType) => {
                write_resp_wrongtype(out);
                return;
            }
            Err(RedisObjectError::MissingKey) => Vec::new(),
        };
        if entries.is_empty() {
            continue;
        }
        if max {
            entries.reverse();
        }
        let (member, score) = entries[0].clone();
        let _ = store.zrem(key, &member);
        write_resp_array_header(out, 3);
        ServerWire::write_resp_blob_string(out, key);
        ServerWire::write_resp_blob_string(out, &member);
        write_resp_score(out, score);
        return;
    }
    write_resp_null(out);
}
