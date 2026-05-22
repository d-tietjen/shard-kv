use crate::commands::redis::{define_redis_command, object_result};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HSetNx, "HSETNX", true);

impl crate::commands::redis::RedisCommand for HSetNx {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("HSETNX", args, 3, || {
            store.hsetnx(args[0], args[1], args[2])
        })
    }
}
