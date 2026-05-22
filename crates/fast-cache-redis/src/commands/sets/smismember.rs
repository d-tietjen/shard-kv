use crate::commands::redis::{define_redis_command, frame_from_result, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(SMIsMember, "SMISMEMBER", false);

impl crate::commands::redis::RedisCommand for SMIsMember {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 2 {
            return wrong_arity("SMISMEMBER");
        }
        frame_from_result(store.smismember(args[0], &args[1..]))
    }
}
