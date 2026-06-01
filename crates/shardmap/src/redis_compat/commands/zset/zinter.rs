#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, write_frame};
use crate::commands::zset_shared::{ZAggregateKind, zaggregate};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZInter, "ZINTER", false);

impl crate::commands::redis::RedisCommand for ZInter {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zaggregate(store, args, ZAggregateKind::Inter)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_frame(out, &Self::execute(store, args));
    }
}
