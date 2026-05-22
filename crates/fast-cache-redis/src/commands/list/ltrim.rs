use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_i64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LTrim, "LTRIM", true);

impl crate::commands::redis::RedisCommand for LTrim {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, start, stop] => match (parse_i64(start), parse_i64(stop)) {
                (Ok(start), Ok(stop)) => frame_from_result(store.ltrim(key, start, stop)),
                _ => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("LTRIM"),
        }
    }
}
