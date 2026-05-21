use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_usize, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(SPop, "SPOP", true);

impl crate::commands::redis::RedisCommand for SPop {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => frame_from_result(store.spop(key, None)),
            [key, count] => match parse_usize(count) {
                Ok(count) => frame_from_result(store.spop(key, Some(count))),
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("SPOP"),
        }
    }
}
