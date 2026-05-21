use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_f64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HIncrByFloat, "HINCRBYFLOAT", true);

impl crate::commands::redis::RedisCommand for HIncrByFloat {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, field, delta] => match parse_f64(delta) {
                Ok(delta) => frame_from_result(store.hincrbyfloat(key, field, delta)),
                Err(_) => error("ERR value is not a valid float"),
            },
            _ => wrong_arity("HINCRBYFLOAT"),
        }
    }
}
