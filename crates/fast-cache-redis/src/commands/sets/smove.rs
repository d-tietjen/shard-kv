#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, int, write_resp_wrong_arity, write_resp_wrongtype, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectError, RedisObjectResult};

define_redis_command!(SMove, "SMOVE", true);

impl crate::commands::redis::RedisCommand for SMove {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [source, dest, member] => match store.set_members(source) {
                Ok(members) if members.iter().any(|item| item.as_slice() == *member) => {
                    match store.sadd(dest, &[*member]) {
                        RedisObjectResult::WrongType => wrongtype(),
                        _ => {
                            let _ = store.srem(source, &[*member]);
                            int(1)
                        }
                    }
                }
                Ok(_) => int(0),
                Err(RedisObjectError::WrongType) => wrongtype(),
                Err(RedisObjectError::MissingKey) => int(0),
            },
            _ => wrong_arity("SMOVE"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [source, dest, member] => match store.set_members(source) {
                Ok(members) if members.iter().any(|item| item.as_slice() == *member) => {
                    match store.sadd(dest, &[*member]) {
                        RedisObjectResult::WrongType => write_resp_wrongtype(out),
                        _ => {
                            let _ = store.srem(source, &[*member]);
                            ServerWire::write_resp_integer(out, 1);
                        }
                    }
                }
                Ok(_) | Err(RedisObjectError::MissingKey) => ServerWire::write_resp_integer(out, 0),
                Err(RedisObjectError::WrongType) => write_resp_wrongtype(out),
            },
            _ => write_resp_wrong_arity(out, "SMOVE"),
        }
    }
}
