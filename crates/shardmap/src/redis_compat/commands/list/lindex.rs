use crate::commands::redis::{
    define_redis_command, error, finish_object_bulk_visit, frame_from_result, parse_i64,
    write_frame, write_resp_null, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;
use crate::storage::RedisListStore;
#[cfg(feature = "server")]
use bytes::BytesMut;

define_redis_command!(LIndex, "LINDEX", false);

impl crate::commands::redis::RedisCommand for LIndex {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, index] => match parse_i64(index) {
                Ok(index) => frame_from_result(store.lindex(key, index)),
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("LINDEX"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, index] => {
                let Ok(index) = parse_i64(index) else {
                    write_frame(out, &error("ERR value is not an integer or out of range"));
                    return;
                };
                let outcome = store.lindex_visit(key, index, |value| match value {
                    Some(value) => ServerWire::write_resp_blob_string(out, value),
                    None => write_resp_null(out),
                });
                finish_object_bulk_visit(out, outcome);
            }
            _ => write_frame(out, &wrong_arity("LINDEX")),
        }
    }
}
