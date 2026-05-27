use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, parse_i64, write_frame,
    write_zrange_rank_resp, wrong_arity, zrange_by_rank_impl,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(ZRevRange, "ZREVRANGE", false);

impl crate::commands::redis::RedisCommand for ZRevRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 3 {
            return wrong_arity("ZREVRANGE");
        }
        let with_scores = match &args[3..] {
            [] => false,
            [option] if eq_ignore_ascii_case(option, b"WITHSCORES") => true,
            _ => return error("ERR syntax error"),
        };
        match (parse_i64(args[1]), parse_i64(args[2])) {
            (Ok(start), Ok(stop)) => {
                zrange_by_rank_impl(store, args[0], start, stop, true, with_scores)
            }
            _ => error("ERR value is not an integer or out of range"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 3 {
            write_frame(out, &wrong_arity("ZREVRANGE"));
            return;
        }
        let with_scores = match &args[3..] {
            [] => false,
            [option] if eq_ignore_ascii_case(option, b"WITHSCORES") => true,
            _ => {
                ServerWire::write_resp_error(out, "ERR syntax error");
                return;
            }
        };
        let (Ok(start), Ok(stop)) = (parse_i64(args[1]), parse_i64(args[2])) else {
            ServerWire::write_resp_error(out, "ERR value is not an integer or out of range");
            return;
        };
        write_zrange_rank_resp(store, args[0], start, stop, true, with_scores, out);
    }
}
