use crate::commands::redis::{define_redis_command, object_result};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LLen, "LLEN", false);

impl crate::commands::redis::RedisCommand for LLen {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("LLEN", args, 1, || store.llen(args[0]))
    }
}
