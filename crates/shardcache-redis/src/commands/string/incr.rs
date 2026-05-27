#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, int, write_frame, wrong_arity};
use crate::commands::string_shared::{incrby_value, write_integer_result_resp};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Incr, "INCR", true);

impl crate::commands::redis::RedisCommand for Incr {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => match incrby_value(store, key, 1) {
                Ok(value) => int(value),
                Err(frame) => frame,
            },
            _ => wrong_arity("INCR"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => write_integer_result_resp(out, incrby_value(store, key, 1)),
            _ => write_frame(out, &wrong_arity("INCR")),
        }
    }
}
