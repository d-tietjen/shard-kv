#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, error, simple, write_frame, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Select, "SELECT", false);

impl crate::commands::redis::RedisCommand for Select {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [b"0"] => simple("OK"),
            [_] => error("ERR DB index is out of range"),
            _ => wrong_arity("SELECT"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [b"0"] => out.extend_from_slice(b"+OK\r\n"),
            [_] => ServerWire::write_resp_error(out, "ERR DB index is out of range"),
            _ => write_frame(out, &wrong_arity("SELECT")),
        }
    }
}
