use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_i64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LRem, "LREM", true);

impl crate::commands::redis::RedisCommand for LRem {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, count, value] => match parse_i64(count) {
                Ok(count) => frame_from_result(store.lrem(key, count, value)),
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("LREM"),
        }
    }
}
