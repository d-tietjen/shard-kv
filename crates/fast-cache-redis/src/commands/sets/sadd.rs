use crate::commands::redis::{define_redis_command, frame_from_result, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(SAdd, "SADD", true);

impl crate::commands::redis::RedisCommand for SAdd {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 2 {
            return wrong_arity("SADD");
        }
        frame_from_result(store.sadd(args[0], &args[1..]))
    }
}
