#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::{write_zrank_like_resp, zrank};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZRevRank, "ZREVRANK", false);

impl crate::commands::redis::RedisCommand for ZRevRank {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zrank(store, args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_zrank_like_resp(store, args, true, out);
    }
}
