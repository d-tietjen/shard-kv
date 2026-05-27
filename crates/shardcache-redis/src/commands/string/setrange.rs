#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, parse_usize, string_value, write_resp_wrong_arity,
    write_resp_wrongtype, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(SetRange, "SETRANGE", true);

impl crate::commands::redis::RedisCommand for SetRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, offset, replacement] => {
                let Ok(offset) = parse_usize(offset) else {
                    return error("ERR offset is not an integer or out of range");
                };
                match string_value(store, key) {
                    Ok(mut value) => {
                        let required = offset.saturating_add(replacement.len());
                        if value.len() < required {
                            value.resize(required, 0);
                        }
                        value[offset..offset + replacement.len()].copy_from_slice(replacement);
                        let len = value.len() as i64;
                        store.set((*key).to_vec(), value, None);
                        int(len)
                    }
                    Err(frame) => frame,
                }
            }
            _ => wrong_arity("SETRANGE"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, offset, replacement] => {
                let Ok(offset) = parse_usize(offset) else {
                    ServerWire::write_resp_error(
                        out,
                        "ERR offset is not an integer or out of range",
                    );
                    return;
                };
                match string_value(store, key) {
                    Ok(mut value) => {
                        let required = offset.saturating_add(replacement.len());
                        if value.len() < required {
                            value.resize(required, 0);
                        }
                        value[offset..offset + replacement.len()].copy_from_slice(replacement);
                        let len = value.len() as i64;
                        store.set((*key).to_vec(), value, None);
                        ServerWire::write_resp_integer(out, len);
                    }
                    Err(_) => write_resp_wrongtype(out),
                }
            }
            _ => write_resp_wrong_arity(out, "SETRANGE"),
        }
    }
}
