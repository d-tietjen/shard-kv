#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, int, write_frame, wrong_arity};
use crate::commands::string_shared::append_value;
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Append, "APPEND", true);

impl crate::commands::redis::RedisCommand for Append {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, suffix] => match append_value(store, key, suffix) {
                Ok(len) => int(len),
                Err(frame) => frame,
            },
            _ => wrong_arity("APPEND"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, suffix] => match append_value(store, key, suffix) {
                Ok(len) => ServerWire::write_resp_integer(out, len),
                Err(frame) => write_frame(out, &frame),
            },
            _ => write_frame(out, &wrong_arity("APPEND")),
        }
    }
}
