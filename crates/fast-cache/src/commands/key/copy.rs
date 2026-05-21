use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, int, parse_i64, write_frame, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Copy, "COPY", true);

impl crate::commands::redis::RedisCommand for Copy {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [source, dest, options @ ..] = args else {
            return wrong_arity("COPY");
        };
        let replace = match parse_copy_options(options) {
            Ok(replace) => replace,
            Err(frame) => return frame,
        };

        if !replace && store.exists(dest) {
            return int(0);
        }
        int(copy_existing_key(store, source, dest, replace) as i64)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [source, dest, options @ ..] = args else {
            write_frame(out, &wrong_arity("COPY"));
            return;
        };
        let replace = match parse_copy_options(options) {
            Ok(replace) => replace,
            Err(frame) => {
                write_frame(out, &frame);
                return;
            }
        };

        let copied = if !replace && store.exists(dest) {
            0
        } else {
            copy_existing_key(store, source, dest, replace) as i64
        };
        ServerWire::write_resp_integer(out, copied);
    }
}

fn parse_copy_options(options: &[&[u8]]) -> std::result::Result<bool, Frame> {
    let mut replace = false;
    let mut cursor = 0;
    while cursor < options.len() {
        let option = options[cursor];
        match (option, options.get(cursor + 1)) {
            (option, _) if eq_ignore_ascii_case(option, b"REPLACE") => {
                replace = true;
                cursor += 1;
            }
            (option, Some(db)) if eq_ignore_ascii_case(option, b"DB") => {
                let Ok(db) = parse_i64(db) else {
                    return Err(error("ERR value is not an integer or out of range"));
                };
                if db != 0 {
                    return Err(error("ERR DB index is out of range"));
                }
                cursor += 2;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok(replace)
}

fn copy_existing_key(store: &EmbeddedStore, source: &[u8], dest: &[u8], replace: bool) -> bool {
    if source == dest {
        return replace && store.exists(source);
    }
    let ttl_ms = match store.pttl_millis(source) {
        ttl if ttl >= 0 => Some(ttl as u64),
        -1 => None,
        _ => return false,
    };
    if let Some(value) = store.get_value_bytes(source) {
        store.set_value_bytes(dest, value, ttl_ms);
        return true;
    }
    if let Some(value) = store.clone_object_value(source) {
        store.set_object_value(dest, value, ttl_ms);
        return true;
    }
    false
}
