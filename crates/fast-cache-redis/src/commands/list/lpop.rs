use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LPop, "LPOP", true);

impl crate::commands::redis::RedisCommand for LPop {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        crate::commands::list_shared::pop_list(store, args, true, "LPOP")
    }
}
