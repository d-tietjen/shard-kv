#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    bulk, define_redis_command, error, parse_f64, string_value, write_resp_wrong_arity,
    write_resp_wrongtype, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

define_redis_command!(IncrByFloat, "INCRBYFLOAT", true);

impl crate::commands::redis::RedisCommand for IncrByFloat {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, delta] => {
                let Ok(delta) = parse_f64(delta) else {
                    return error("ERR value is not a valid float");
                };
                match string_value(store, key) {
                    Ok(value) => {
                        let current = if value.is_empty() {
                            0.0
                        } else {
                            match parse_f64(&value) {
                                Ok(value) => value,
                                Err(_) => return error("ERR value is not a valid float"),
                            }
                        };
                        let next = current + delta;
                        if !next.is_finite() {
                            return error("ERR increment would produce NaN or Infinity");
                        }
                        let bytes = next.to_string().into_bytes();
                        store.set((*key).to_vec(), bytes.clone(), None);
                        bulk(bytes)
                    }
                    Err(frame) => frame,
                }
            }
            _ => wrong_arity("INCRBYFLOAT"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, delta] => {
                let Ok(delta) = parse_f64(delta) else {
                    ServerWire::write_resp_error(out, "ERR value is not a valid float");
                    return;
                };
                let mut current = 0.0;
                match store.get_string_value_into(key, |bytes| {
                    current = parse_f64(bytes).unwrap_or(f64::NAN);
                }) {
                    RedisStringLookup::Hit if current.is_nan() => {
                        ServerWire::write_resp_error(out, "ERR value is not a valid float");
                    }
                    RedisStringLookup::Hit | RedisStringLookup::Miss => {
                        let next = current + delta;
                        if !next.is_finite() {
                            ServerWire::write_resp_error(
                                out,
                                "ERR increment would produce NaN or Infinity",
                            );
                            return;
                        }
                        let bytes = next.to_string().into_bytes();
                        store.set((*key).to_vec(), bytes.clone(), None);
                        ServerWire::write_resp_blob_string(out, &bytes);
                    }
                    RedisStringLookup::WrongType => write_resp_wrongtype(out),
                }
            }
            _ => write_resp_wrong_arity(out, "INCRBYFLOAT"),
        }
    }
}
