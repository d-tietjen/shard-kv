#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{bulk, define_redis_command, write_frame, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Echo, "ECHO", false);

impl crate::commands::redis::RedisCommand for Echo {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [payload] => bulk((*payload).to_vec()),
            _ => wrong_arity("ECHO"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [payload] => ServerWire::write_resp_blob_string(out, payload),
            _ => write_frame(out, &wrong_arity("ECHO")),
        }
    }
}
