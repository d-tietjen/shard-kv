use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, optional_string_value, write_frame, wrong_arity,
};
use crate::commands::string_bits::{BitOpKind, apply_bitop};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(BitOp, "BITOP", true);

impl crate::commands::redis::RedisCommand for BitOp {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match bitop_value(store, args) {
            Ok(len) => int(len),
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match bitop_value(store, args) {
            Ok(len) => ServerWire::write_resp_integer(out, len),
            Err(frame) => write_frame(out, &frame),
        }
    }
}

fn bitop_value(store: &EmbeddedStore, args: &[&[u8]]) -> std::result::Result<i64, Frame> {
    let [operation, dest, sources @ ..] = args else {
        return Err(wrong_arity("BITOP"));
    };
    let Some(operation) = BitOpKind::parse(operation) else {
        return Err(error("ERR syntax error"));
    };
    if sources.is_empty() || (operation == BitOpKind::Not && sources.len() != 1) {
        return Err(wrong_arity("BITOP"));
    }

    let mut values = Vec::with_capacity(sources.len());
    for source in sources {
        match optional_string_value(store, source, true) {
            Ok(Some(value)) => values.push(value),
            Ok(None) => values.push(Vec::new()),
            Err(frame) => return Err(frame),
        }
    }
    let result = apply_bitop(operation, &values);
    let len = result.len() as i64;
    store.set((*dest).to_vec(), result, None);
    Ok(len)
}
