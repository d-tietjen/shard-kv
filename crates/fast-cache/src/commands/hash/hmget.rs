use crate::commands::redis::{define_redis_command, frame_from_result, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HMGet, "HMGET", false);

impl crate::commands::redis::RedisCommand for HMGet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 2 {
            return wrong_arity("HMGET");
        }
        frame_from_result(store.hmget(args[0], &args[1..]))
    }
}
