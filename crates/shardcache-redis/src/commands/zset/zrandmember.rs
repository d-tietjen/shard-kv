#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    array_bulk, bulk, define_redis_command, error, parse_i64, write_frame, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectError};

define_redis_command!(ZRandMember, "ZRANDMEMBER", false);

impl crate::commands::redis::RedisCommand for ZRandMember {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let parsed = match parse_zrandmember_args(args) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        let entries = match store.zentries(parsed.key) {
            Ok(entries) => entries,
            Err(RedisObjectError::MissingKey) => Vec::new(),
            Err(RedisObjectError::WrongType) => return crate::commands::redis::wrongtype(),
        };
        zrandmember_frame(entries, parsed.count, parsed.with_scores)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_frame(out, &Self::execute(store, args));
    }
}

#[derive(Clone, Copy)]
struct ZRandMemberArgs<'a> {
    key: &'a [u8],
    count: Option<i64>,
    with_scores: bool,
}

fn parse_zrandmember_args<'a>(
    args: &'a [&'a [u8]],
) -> std::result::Result<ZRandMemberArgs<'a>, Frame> {
    match args {
        [key] => Ok(ZRandMemberArgs {
            key,
            count: None,
            with_scores: false,
        }),
        [key, count] => Ok(ZRandMemberArgs {
            key,
            count: Some(
                parse_i64(count)
                    .map_err(|_| error("ERR value is not an integer or out of range"))?,
            ),
            with_scores: false,
        }),
        [key, count, withscores]
            if crate::commands::redis::eq_ignore_ascii_case(withscores, b"WITHSCORES") =>
        {
            Ok(ZRandMemberArgs {
                key,
                count: Some(
                    parse_i64(count)
                        .map_err(|_| error("ERR value is not an integer or out of range"))?,
                ),
                with_scores: true,
            })
        }
        _ => Err(wrong_arity("ZRANDMEMBER")),
    }
}

fn zrandmember_frame(entries: Vec<(Vec<u8>, f64)>, count: Option<i64>, with_scores: bool) -> Frame {
    if entries.is_empty() {
        return match count {
            None => Frame::Null,
            Some(_) => Frame::Array(Vec::new()),
        };
    }
    match count {
        None => {
            let (member, score) = entries[0].clone();
            if with_scores {
                Frame::Array(vec![bulk(member), bulk(score.to_string().into_bytes())])
            } else {
                bulk(member)
            }
        }
        Some(count) if count >= 0 => {
            let selected = entries.into_iter().take(count as usize).collect::<Vec<_>>();
            zrandmember_array(selected, with_scores)
        }
        Some(count) => {
            let wanted = count.unsigned_abs() as usize;
            let mut selected = Vec::with_capacity(wanted);
            for index in 0..wanted {
                selected.push(entries[index % entries.len()].clone());
            }
            zrandmember_array(selected, with_scores)
        }
    }
}

fn zrandmember_array(entries: Vec<(Vec<u8>, f64)>, with_scores: bool) -> Frame {
    if !with_scores {
        return array_bulk(entries.into_iter().map(|(member, _)| member).collect());
    }
    let mut out = Vec::with_capacity(entries.len().saturating_mul(2));
    for (member, score) in entries {
        out.push(bulk(member));
        out.push(bulk(score.to_string().into_bytes()));
    }
    Frame::Array(out)
}
