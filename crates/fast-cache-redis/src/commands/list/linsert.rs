#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, frame_from_result, write_resp_wrong_arity,
    write_result_resp, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(LInsert, "LINSERT", true);

impl crate::commands::redis::RedisCommand for LInsert {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, where_arg, pivot, value] => {
                let before = match *where_arg {
                    value if eq_ignore_ascii_case(value, b"BEFORE") => true,
                    value if eq_ignore_ascii_case(value, b"AFTER") => false,
                    _ => return error("ERR syntax error"),
                };
                frame_from_result(store.linsert(key, before, pivot, value))
            }
            _ => wrong_arity("LINSERT"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, where_, pivot, value] => {
                let before = if eq_ignore_ascii_case(where_, b"BEFORE") {
                    true
                } else if eq_ignore_ascii_case(where_, b"AFTER") {
                    false
                } else {
                    ServerWire::write_resp_error(out, "ERR syntax error");
                    return;
                };
                write_result_resp(out, store.linsert(key, before, pivot, value));
            }
            _ => write_resp_wrong_arity(out, "LINSERT"),
        }
    }
}
