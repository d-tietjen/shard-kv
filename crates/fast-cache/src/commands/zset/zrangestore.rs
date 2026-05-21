#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, int, write_frame};
use crate::commands::zset_shared::zrangestore_len;
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(ZRangeStore, "ZRANGESTORE", true);

impl crate::commands::redis::RedisCommand for ZRangeStore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match zrangestore_len(store, args) {
            Ok(len) => int(len as i64),
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match zrangestore_len(store, args) {
            Ok(len) => ServerWire::write_resp_integer(out, len as i64),
            Err(frame) => write_frame(out, &frame),
        }
    }
}
