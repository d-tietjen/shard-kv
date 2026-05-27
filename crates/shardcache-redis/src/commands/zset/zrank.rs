#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::{write_zrank_like_resp, zrank};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZRank, "ZRANK", false);

impl crate::commands::redis::RedisCommand for ZRank {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zrank(store, args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_zrank_like_resp(store, args, false, out);
    }
}
