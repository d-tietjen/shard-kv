use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::{ZAggregateKind, zaggregate_store};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZDiffStore, "ZDIFFSTORE", true);

impl crate::commands::redis::RedisCommand for ZDiffStore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zaggregate_store(store, args, ZAggregateKind::Diff)
    }
}
