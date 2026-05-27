#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, write_frame};
use crate::commands::zset_shared::zintercard;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZInterCard, "ZINTERCARD", false);

impl crate::commands::redis::RedisCommand for ZInterCard {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zintercard(store, args)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_frame(out, &Self::execute(store, args));
    }
}
