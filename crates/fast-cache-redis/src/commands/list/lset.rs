use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_i64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LSet, "LSET", true);

impl crate::commands::redis::RedisCommand for LSet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, index, value] => match parse_i64(index) {
                Ok(index) => frame_from_result(store.lset(key, index, value)),
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("LSET"),
        }
    }
}
