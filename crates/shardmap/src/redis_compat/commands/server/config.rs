#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_resp_array_header;
use crate::commands::redis::{define_redis_command, eq_ignore_ascii_case, simple};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Config, "CONFIG", false);

impl crate::commands::redis::RedisCommand for Config {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub, ..] if eq_ignore_ascii_case(sub, b"GET") => Frame::Array(Vec::new()),
            _ => simple("OK"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [sub, ..] if eq_ignore_ascii_case(sub, b"GET") => write_resp_array_header(out, 0),
            _ => out.extend_from_slice(b"+OK\r\n"),
        }
    }
}
