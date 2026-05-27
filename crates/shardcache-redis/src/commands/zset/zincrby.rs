#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_f64, write_resp_wrong_arity,
    write_result_resp, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(ZIncrBy, "ZINCRBY", true);

impl crate::commands::redis::RedisCommand for ZIncrBy {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, delta, member] => match parse_f64(delta) {
                Ok(delta) => frame_from_result(store.zincrby(key, delta, member)),
                Err(_) => error("ERR value is not a valid float"),
            },
            _ => wrong_arity("ZINCRBY"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, delta, member] => match parse_f64(delta) {
                Ok(delta) => write_result_resp(out, store.zincrby(key, delta, member)),
                Err(_) => ServerWire::write_resp_error(out, "ERR value is not a valid float"),
            },
            _ => write_resp_wrong_arity(out, "ZINCRBY"),
        }
    }
}
