#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, parse_i64, write_frame, wrong_arity,
};
use crate::commands::string_shared::{decrby_value, write_integer_result_resp};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(DecrBy, "DECRBY", true);

impl crate::commands::redis::RedisCommand for DecrBy {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, delta] => match parse_i64(delta) {
                Ok(delta) => match decrby_value(store, key, delta) {
                    Ok(value) => int(value),
                    Err(frame) => frame,
                },
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("DECRBY"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, decrement] => match parse_i64(decrement) {
                Ok(decrement) => {
                    write_integer_result_resp(out, decrby_value(store, key, decrement))
                }
                Err(_) => {
                    ServerWire::write_resp_error(out, "ERR value is not an integer or out of range")
                }
            },
            _ => write_frame(out, &wrong_arity("DECRBY")),
        }
    }
}
