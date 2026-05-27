#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::{ZAggregateKind, zaggregate_store};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZDiffStore, "ZDIFFSTORE", true);

impl crate::commands::redis::RedisCommand for ZDiffStore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zaggregate_store(store, args, ZAggregateKind::Diff)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        crate::commands::zset_shared::write_zaggregate_store_resp(
            store,
            args,
            ZAggregateKind::Diff,
            out,
        );
    }
}
