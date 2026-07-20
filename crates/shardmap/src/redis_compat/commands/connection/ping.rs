#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_fast_frame;
use crate::commands::redis::{bulk, define_redis_command, simple, write_frame, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Ping, "PING", false);

impl crate::commands::redis::RedisCommand for Ping {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => simple("PONG"),
            [payload] => bulk((*payload).to_vec()),
            _ => wrong_arity("PING"),
        }
    }

    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_fast_frame(out, &Self::execute(store, args));
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [] => out.extend_from_slice(b"+PONG\r\n"),
            [payload] => ServerWire::write_resp_blob_string(out, payload),
            _ => write_frame(out, &wrong_arity("PING")),
        }
    }
}
