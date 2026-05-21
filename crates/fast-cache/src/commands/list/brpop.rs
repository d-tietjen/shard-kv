use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BRPop, "BRPOP", true);

impl crate::commands::redis::RedisCommand for BRPop {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        crate::commands::list_shared::blocking_pop(store, args, false, "BRPOP")
    }
}
