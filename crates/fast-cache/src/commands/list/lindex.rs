use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_i64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LIndex, "LINDEX", false);

impl crate::commands::redis::RedisCommand for LIndex {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, index] => match parse_i64(index) {
                Ok(index) => frame_from_result(store.lindex(key, index)),
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("LINDEX"),
        }
    }
}
