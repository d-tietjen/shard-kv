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

define_redis_command!(LRem, "LREM", true);

impl crate::commands::redis::RedisCommand for LRem {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, count, value] => match parse_i64(count) {
                Ok(count) => frame_from_result(store.lrem(key, count, value)),
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("LREM"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, count, value] => match parse_i64(count) {
                Ok(count) => write_result_resp(out, store.lrem(key, count, value)),
                Err(_) => {
                    ServerWire::write_resp_error(out, "ERR value is not an integer or out of range")
                }
            },
            _ => write_resp_wrong_arity(out, "LREM"),
        }
    }
}
