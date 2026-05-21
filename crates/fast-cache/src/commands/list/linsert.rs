use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, frame_from_result, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LInsert, "LINSERT", true);

impl crate::commands::redis::RedisCommand for LInsert {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, where_arg, pivot, value] => {
                let before = match *where_arg {
                    value if eq_ignore_ascii_case(value, b"BEFORE") => true,
                    value if eq_ignore_ascii_case(value, b"AFTER") => false,
                    _ => return error("ERR syntax error"),
                };
                frame_from_result(store.linsert(key, before, pivot, value))
            }
            _ => wrong_arity("LINSERT"),
        }
    }
}
