use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_i64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LRange, "LRANGE", false);

impl crate::commands::redis::RedisCommand for LRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, start, stop] => match (parse_i64(start), parse_i64(stop)) {
                (Ok(start), Ok(stop)) => frame_from_result(store.lrange(key, start, stop)),
                _ => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("LRANGE"),
        }
    }
}
