use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_i64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HIncrBy, "HINCRBY", true);

impl crate::commands::redis::RedisCommand for HIncrBy {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, field, delta] => match parse_i64(delta) {
                Ok(delta) => frame_from_result(store.hincrby(key, field, delta)),
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("HINCRBY"),
        }
    }
}
