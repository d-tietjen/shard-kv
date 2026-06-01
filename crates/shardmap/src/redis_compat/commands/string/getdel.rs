#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    bulk, define_redis_command, optional_string_value, write_resp_null, write_resp_wrong_arity,
    write_resp_wrongtype, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

define_redis_command!(GetDel, "GETDEL", true);

impl crate::commands::redis::RedisCommand for GetDel {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => match optional_string_value(store, key, true) {
                Ok(old) => {
                    if old.is_some() {
                        store.delete(key);
                    }
                    old.map_or(Frame::Null, bulk)
                }
                Err(frame) => frame,
            },
            _ => wrong_arity("GETDEL"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => match store.get_string_value_into(key, |bytes| {
                ServerWire::write_resp_blob_string(out, bytes);
            }) {
                RedisStringLookup::Hit => {
                    store.delete(key);
                }
                RedisStringLookup::Miss => write_resp_null(out),
                RedisStringLookup::WrongType => write_resp_wrongtype(out),
            },
            _ => write_resp_wrong_arity(out, "GETDEL"),
        }
    }
}
