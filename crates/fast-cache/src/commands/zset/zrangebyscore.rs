use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, parse_usize, wrong_arity,
};
use crate::commands::zset_shared::zrange_by_score_impl;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZRangeByScore, "ZRANGEBYSCORE", false);

impl crate::commands::redis::RedisCommand for ZRangeByScore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 3 {
            return wrong_arity("ZRANGEBYSCORE");
        }
        let mut with_scores = false;
        let mut limit = None;
        let mut index = 3;
        while index < args.len() {
            let option = args[index];
            match (option, args.get(index + 1), args.get(index + 2)) {
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
        zrange_by_score_impl(store, args[0], args[1], args[2], false, with_scores, limit)
    }
}
