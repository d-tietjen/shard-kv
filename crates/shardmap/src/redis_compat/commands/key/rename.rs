use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, int, simple, write_frame, write_resp_simple_string, wrong_arity,
    wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectError};

define_redis_command!(Rename, "RENAME", true);

impl crate::commands::redis::RedisCommand for Rename {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        execute_rename(store, args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_rename_resp(store, args, false, out);
    }
}

pub(crate) fn execute_rename(store: &EmbeddedStore, args: &[&[u8]], nx: bool) -> Frame {
    match args {
        [source, dest] => match store.rename_key(source, dest, nx) {
            Ok(true) if nx => int(1),
            Ok(false) if nx => int(0),
            Ok(_) => simple("OK"),
            Err(RedisObjectError::MissingKey) => crate::commands::redis::error("ERR no such key"),
            Err(RedisObjectError::WrongType) => wrongtype(),
        },
        _ => wrong_arity(if nx { "RENAMENX" } else { "RENAME" }),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_rename_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    nx: bool,
    out: &mut BytesMut,
) {
    match args {
        [source, dest] => match store.rename_key(source, dest, nx) {
            Ok(true) if nx => ServerWire::write_resp_integer(out, 1),
            Ok(false) if nx => ServerWire::write_resp_integer(out, 0),
            Ok(_) => write_resp_simple_string(out, "OK"),
            Err(RedisObjectError::MissingKey) => {
                ServerWire::write_resp_error(out, "ERR no such key")
            }
            Err(RedisObjectError::WrongType) => write_frame(out, &wrongtype()),
        },
        _ => write_frame(out, &wrong_arity(if nx { "RENAMENX" } else { "RENAME" })),
    }
}
