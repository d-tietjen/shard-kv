use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::{ZAggregateKind, zaggregate_store};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZInterStore, "ZINTERSTORE", true);

impl crate::commands::redis::RedisCommand for ZInterStore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zaggregate_store(store, args, ZAggregateKind::Inter)
    }
}
