use crate::commands::redis::define_redis_command;
use crate::commands::set_shared::{SetOp, set_op};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(SDiff, "SDIFF", false);

impl crate::commands::redis::RedisCommand for SDiff {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        set_op(store, args, SetOp::Diff, None)
    }
}
