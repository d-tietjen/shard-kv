#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, int, parse_i64, wrong_arity, wrongtype,
};
#[cfg(feature = "server")]
use crate::commands::redis::{
    write_frame, write_resp_array_header, write_resp_null, write_resp_wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisListStore, RedisObjectReadOutcome};

define_redis_command!(LPos, "LPOS", false);

struct LPosArgs<'a> {
    key: &'a [u8],
    element: &'a [u8],
    rank: i64,
    count: Option<i64>,
    maxlen: i64,
}

impl crate::commands::redis::RedisCommand for LPos {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let parsed = match parse_lpos_args(args) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };

        let mut matches = Vec::new();
        let outcome = store.lpos_visit(
            parsed.key,
            parsed.element,
            parsed.rank,
            parsed.count,
            parsed.maxlen,
            |found| matches = found,
        );

        match outcome {
            RedisObjectReadOutcome::WrongType => wrongtype(),
            RedisObjectReadOutcome::Missing => match parsed.count {
                Some(_) => Frame::Array(Vec::new()),
                None => Frame::Null,
            },
            RedisObjectReadOutcome::Written => match parsed.count {
                Some(_) => Frame::Array(matches.into_iter().map(int).collect()),
                None => matches.first().map_or(Frame::Null, |&pos| int(pos)),
            },
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let parsed = match parse_lpos_args(args) {
            Ok(parsed) => parsed,
            Err(frame) => {
                write_frame(out, &frame);
                return;
            }
        };

        let mut matches = Vec::new();
        let outcome = store.lpos_visit(
            parsed.key,
            parsed.element,
            parsed.rank,
            parsed.count,
            parsed.maxlen,
            |found| matches = found,
        );

        match outcome {
            RedisObjectReadOutcome::WrongType => write_resp_wrongtype(out),
            RedisObjectReadOutcome::Missing => match parsed.count {
                Some(_) => write_resp_array_header(out, 0),
                None => write_resp_null(out),
            },
            RedisObjectReadOutcome::Written => match parsed.count {
                Some(_) => {
                    write_resp_array_header(out, matches.len());
                    for pos in matches {
                        ServerWire::write_resp_integer(out, pos);
                    }
                }
                None => match matches.first() {
                    Some(&pos) => ServerWire::write_resp_integer(out, pos),
                    None => write_resp_null(out),
                },
            },
        }
    }
}

fn parse_lpos_args<'a>(args: &'a [&'a [u8]]) -> Result<LPosArgs<'a>, Frame> {
    let [key, element, options @ ..] = args else {
        return Err(wrong_arity("LPOS"));
    };

    let mut rank: i64 = 1;
    let mut count: Option<i64> = None;
    let mut maxlen: i64 = 0;
    let mut index = 0;
    while index < options.len() {
        let option = options[index];
        let Some(raw) = options.get(index + 1) else {
            return Err(error("ERR syntax error"));
        };
        match option {
            option if eq_ignore_ascii_case(option, b"RANK") => match parse_i64(raw) {
                Ok(value) => rank = value,
                Err(_) => return Err(error("ERR value is not an integer or out of range")),
            },
            option if eq_ignore_ascii_case(option, b"COUNT") => match parse_i64(raw) {
                Ok(value) if value >= 0 => count = Some(value),
                Ok(_) => return Err(error("ERR COUNT can't be negative")),
                Err(_) => return Err(error("ERR value is not an integer or out of range")),
            },
            option if eq_ignore_ascii_case(option, b"MAXLEN") => match parse_i64(raw) {
                Ok(value) if value >= 0 => maxlen = value,
                Ok(_) => return Err(error("ERR MAXLEN can't be negative")),
                Err(_) => return Err(error("ERR value is not an integer or out of range")),
            },
            _ => {
                return Err(error("ERR syntax error"));
            }
        }
        index += 2;
    }

    if rank == 0 {
        return Err(error(
            "ERR RANK can't be zero: use 1 to start searching from the first match. Negative ranks can be used to search backward.",
        ));
    }

    Ok(LPosArgs {
        key,
        element,
        rank,
        count,
        maxlen,
    })
}
