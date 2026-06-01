#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_resp_wrong_arity;
use crate::commands::redis::{define_redis_command, simple, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Reset, "RESET", false);

impl crate::commands::redis::RedisCommand for Reset {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => simple("RESET"),
            _ => wrong_arity("RESET"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [] => out.extend_from_slice(b"+RESET\r\n"),
            _ => write_resp_wrong_arity(out, "RESET"),
        }
    }
}
