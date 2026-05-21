#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, parse_i64, parse_usize, write_frame,
    wrong_arity,
};
use crate::commands::zset_shared::{
    write_zrange_rank_resp, zrange_by_rank_impl, zrange_by_score_impl,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZRange, "ZRANGE", false);

impl crate::commands::redis::RedisCommand for ZRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 3 {
            return wrong_arity("ZRANGE");
        }
        let mut by_score = false;
        let mut rev = false;
        let mut with_scores = false;
        let mut limit = None;
        let mut index = 3;
        while index < args.len() {
            let option = args[index];
            match (option, args.get(index + 1), args.get(index + 2)) {
                (option, _, _) if eq_ignore_ascii_case(option, b"BYSCORE") => {
                    by_score = true;
                    index += 1;
                }
                (option, _, _) if eq_ignore_ascii_case(option, b"REV") => {
                    rev = true;
                    index += 1;
                }
                (option, _, _) if eq_ignore_ascii_case(option, b"WITHSCORES") => {
                    with_scores = true;
                    index += 1;
                }
                (option, Some(offset), Some(count)) if eq_ignore_ascii_case(option, b"LIMIT") => {
                    let (Ok(offset), Ok(count)) = (parse_usize(offset), parse_usize(count)) else {
                        return error("ERR value is not an integer or out of range");
                    };
                    limit = Some((offset, count));
                    index += 3;
                }
                _ => return error("ERR syntax error"),
            }
        }
        if by_score {
            zrange_by_score_impl(store, args[0], args[1], args[2], rev, with_scores, limit)
        } else {
            let (Ok(start), Ok(stop)) = (parse_i64(args[1]), parse_i64(args[2])) else {
                return error("ERR value is not an integer or out of range");
            };
            zrange_by_rank_impl(store, args[0], start, stop, rev, with_scores)
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 3 {
            write_frame(out, &wrong_arity("ZRANGE"));
            return;
        }
        let mut rev = false;
        let mut with_scores = false;
        let mut index = 3;
        while index < args.len() {
            match args[index] {
                option if eq_ignore_ascii_case(option, b"REV") => {
                    rev = true;
                    index += 1;
                }
                option if eq_ignore_ascii_case(option, b"WITHSCORES") => {
                    with_scores = true;
                    index += 1;
                }
                _ => {
                    write_frame(out, &Self::execute(store, args));
                    return;
                }
            }
        }
        let (Ok(start), Ok(stop)) = (parse_i64(args[1]), parse_i64(args[2])) else {
            write_frame(out, &error("ERR value is not an integer or out of range"));
            return;
        };
        write_zrange_rank_resp(store, args[0], start, stop, rev, with_scores, out);
    }
}
