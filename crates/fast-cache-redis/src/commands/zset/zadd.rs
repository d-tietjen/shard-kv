use crate::commands::redis::{
    bulk, define_redis_command, eq_ignore_ascii_case, error, int, parse_f64, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectResult};

define_redis_command!(ZAdd, "ZADD", true);

impl crate::commands::redis::RedisCommand for ZAdd {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 3 {
            return wrong_arity("ZADD");
        }
        let key = args[0];
        let mut index = 1;
        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;
        let mut incr = false;
        while index < args.len() {
            let option = args[index];
            match option {
                option if eq_ignore_ascii_case(option, b"NX") => nx = true,
                option if eq_ignore_ascii_case(option, b"XX") => xx = true,
                option if eq_ignore_ascii_case(option, b"GT") => gt = true,
                option if eq_ignore_ascii_case(option, b"LT") => lt = true,
                option if eq_ignore_ascii_case(option, b"CH") => ch = true,
                option if eq_ignore_ascii_case(option, b"INCR") => incr = true,
                _ => break,
            }
            index += 1;
        }
        if index >= args.len() || !(args.len() - index).is_multiple_of(2) {
            return error("ERR syntax error");
        }
        let mut total = 0_i64;
        let mut last_bulk = None;
        for pair in args[index..].chunks_exact(2) {
            let Ok(score) = parse_f64(pair[0]) else {
                return error("ERR value is not a valid float");
            };
            match store.zadd_cond(key, score, pair[1], nx, xx, gt, lt, ch, incr) {
                RedisObjectResult::Integer(value) => total += value,
                RedisObjectResult::Bulk(value) => last_bulk = value,
                RedisObjectResult::WrongType => return wrongtype(),
                RedisObjectResult::Simple(message) if message.starts_with("ERR ") => {
                    return error(message);
                }
                _ => {}
            }
        }
        if incr {
            last_bulk.map_or(Frame::Null, bulk)
        } else {
            int(total)
        }
    }
}
