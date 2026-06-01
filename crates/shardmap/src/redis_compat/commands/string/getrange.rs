#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::formal_range::normalize_redis_range;
use crate::commands::redis::{
    bulk, define_redis_command, error, parse_i64, string_value, write_frame, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

define_redis_command!(GetRange, "GETRANGE", false, aliases: ["SUBSTR"]);

impl crate::commands::redis::RedisCommand for GetRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, start, stop] => {
                let Ok(start) = parse_i64(start) else {
                    return error("ERR value is not an integer or out of range");
                };
                let Ok(stop) = parse_i64(stop) else {
                    return error("ERR value is not an integer or out of range");
                };
                match string_value(store, key) {
                    Ok(value) => match normalize_redis_range(value.len(), start, stop) {
                        Some(range) => {
                            let (start, stop) = range.into_bounds();
                            bulk(value[start..=stop].to_vec())
                        }
                        None => bulk(Vec::new()),
                    },
                    Err(frame) => frame,
                }
            }
            _ => wrong_arity("GETRANGE"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, start, stop] => {
                if *start == b"0" && *stop == b"-1" {
                    match store.get_string_value_into(key, |bytes| {
                        ServerWire::write_resp_blob_string(out, bytes);
                    }) {
                        RedisStringLookup::Hit => {}
                        RedisStringLookup::Miss => ServerWire::write_resp_blob_string(out, b""),
                        RedisStringLookup::WrongType => write_frame(out, &wrongtype()),
                    }
                    return;
                }
                let Ok(start) = parse_i64(start) else {
                    ServerWire::write_resp_error(
                        out,
                        "ERR value is not an integer or out of range",
                    );
                    return;
                };
                let Ok(stop) = parse_i64(stop) else {
                    ServerWire::write_resp_error(
                        out,
                        "ERR value is not an integer or out of range",
                    );
                    return;
                };
                match store.get_string_value_into(key, |bytes| {
                    match normalize_redis_range(bytes.len(), start, stop) {
                        Some(range) => {
                            let (start, stop) = range.into_bounds();
                            ServerWire::write_resp_blob_string(out, &bytes[start..=stop]);
                        }
                        None => ServerWire::write_resp_blob_string(out, b""),
                    }
                }) {
                    RedisStringLookup::Hit => {}
                    RedisStringLookup::Miss => ServerWire::write_resp_blob_string(out, b""),
                    RedisStringLookup::WrongType => write_frame(out, &wrongtype()),
                }
            }
            _ => write_frame(out, &wrong_arity("GETRANGE")),
        }
    }
}
