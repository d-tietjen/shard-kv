#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_frame;
use crate::commands::redis::{define_redis_command, int, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(DbSize, "DBSIZE", false);

impl crate::commands::redis::RedisCommand for DbSize {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => int(store.len() as i64),
            _ => wrong_arity("DBSIZE"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [] => ServerWire::write_resp_integer(out, store.len() as i64),
            _ => write_frame(out, &wrong_arity("DBSIZE")),
        }
    }
}
