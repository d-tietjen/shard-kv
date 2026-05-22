use crate::storage::{RedisKeyStore, RedisZSetStore};
use bytes::BytesMut;

use crate::commands::formal_range::normalize_redis_range;
#[cfg(feature = "server")]
use crate::commands::redis::write_result_resp;
use crate::commands::redis::{
    array_bulk, bulk, error, int, parse_f64, parse_i64, parse_usize, reserve_resp_bulk_array_hint,
    write_frame, write_resp_array_header, wrong_arity, wrongtype, zentries_frame,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{
    EmbeddedStore, RedisObjectError, RedisObjectReadOutcome, RedisObjectValue,
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
pub(crate) fn write_resp_score(out: &mut BytesMut, score: f64) {
    if score.fract() == 0.0 && score.is_finite() {
        let mut buffer = itoa::Buffer::new();
        ServerWire::write_resp_blob_string(out, buffer.format(score as i64).as_bytes());
    } else {
        let score = score.to_string();
        ServerWire::write_resp_blob_string(out, score.as_bytes());
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
            Ok(None) | Err(RedisObjectError::MissingKey) => out.extend_from_slice(b"$-1\r\n"),
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

#[derive(Clone, Copy)]
pub(crate) enum ZAggregateKind {
    Union,
    Inter,
    Diff,
}

impl ZAggregateKind {
    fn name(self) -> &'static str {
        match self {
            Self::Union => "ZUNIONSTORE",
            Self::Inter => "ZINTERSTORE",
            Self::Diff => "ZDIFFSTORE",
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
        return wrong_arity(kind.name());
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
