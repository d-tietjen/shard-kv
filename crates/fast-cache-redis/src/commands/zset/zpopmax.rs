#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::{write_zpop_resp, zpop};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZPopMax, "ZPOPMAX", true);

impl crate::commands::redis::RedisCommand for ZPopMax {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zpop(store, args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_zpop_resp(store, args, true, out);
    }
}
