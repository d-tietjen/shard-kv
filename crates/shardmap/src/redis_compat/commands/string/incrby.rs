#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, parse_i64, write_frame, wrong_arity,
};
use crate::commands::string_shared::{incrby_value, write_integer_result_resp};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(IncrBy, "INCRBY", true);

impl crate::commands::redis::RedisCommand for IncrBy {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, delta] => match parse_i64(delta) {
                Ok(delta) => match incrby_value(store, key, delta) {
                    Ok(value) => int(value),
                    Err(frame) => frame,
                },
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("INCRBY"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, delta] => match parse_i64(delta) {
                Ok(delta) => write_integer_result_resp(out, incrby_value(store, key, delta)),
                Err(_) => {
                    ServerWire::write_resp_error(out, "ERR value is not an integer or out of range")
                }
            },
            _ => write_frame(out, &wrong_arity("INCRBY")),
        }
    }
}
