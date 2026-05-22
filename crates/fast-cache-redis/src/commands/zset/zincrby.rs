use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_f64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZIncrBy, "ZINCRBY", true);

impl crate::commands::redis::RedisCommand for ZIncrBy {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, delta, member] => match parse_f64(delta) {
                Ok(delta) => frame_from_result(store.zincrby(key, delta, member)),
                Err(_) => error("ERR value is not a valid float"),
            },
            _ => wrong_arity("ZINCRBY"),
        }
    }
}
