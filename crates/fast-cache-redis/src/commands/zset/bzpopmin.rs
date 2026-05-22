use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::bzpop;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BZPopMin, "BZPOPMIN", true);

impl crate::commands::redis::RedisCommand for BZPopMin {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        bzpop(store, args, false)
    }
}
