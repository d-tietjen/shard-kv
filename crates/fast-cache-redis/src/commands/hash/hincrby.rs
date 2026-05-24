#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_i64, write_resp_wrong_arity,
    write_result_resp, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
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

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, field, delta] => match parse_i64(delta) {
                Ok(delta) => write_result_resp(out, store.hincrby(key, field, delta)),
                Err(_) => {
                    ServerWire::write_resp_error(out, "ERR value is not an integer or out of range")
                }
            },
            _ => write_resp_wrong_arity(out, "HINCRBY"),
        }
    }
}
