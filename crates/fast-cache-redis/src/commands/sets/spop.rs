#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_usize, write_resp_wrong_arity,
    write_result_resp, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(SPop, "SPOP", true);

impl crate::commands::redis::RedisCommand for SPop {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => frame_from_result(store.spop(key, None)),
            [key, count] => match parse_usize(count) {
                Ok(count) => frame_from_result(store.spop(key, Some(count))),
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("SPOP"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => write_result_resp(out, store.spop(key, None)),
            [key, count] => match parse_usize(count) {
                Ok(count) => write_result_resp(out, store.spop(key, Some(count))),
                Err(_) => {
                    ServerWire::write_resp_error(out, "ERR value is not an integer or out of range")
                }
            },
            _ => write_resp_wrong_arity(out, "SPOP"),
        }
    }
}
