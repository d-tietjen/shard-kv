#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, int, write_resp_wrong_arity, write_resp_wrongtype, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectResult};

define_redis_command!(HStrLen, "HSTRLEN", false);

impl crate::commands::redis::RedisCommand for HStrLen {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, field] => match store.hget(key, field) {
                RedisObjectResult::Bulk(Some(value)) => int(value.len() as i64),
                RedisObjectResult::Bulk(None) => int(0),
                RedisObjectResult::WrongType => wrongtype(),
                _ => int(0),
            },
            _ => wrong_arity("HSTRLEN"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, field] => match store.hget(key, field) {
                RedisObjectResult::Bulk(Some(value)) => {
                    ServerWire::write_resp_integer(out, value.len() as i64)
                }
                RedisObjectResult::WrongType => write_resp_wrongtype(out),
                _ => ServerWire::write_resp_integer(out, 0),
            },
            _ => write_resp_wrong_arity(out, "HSTRLEN"),
        }
    }
}
