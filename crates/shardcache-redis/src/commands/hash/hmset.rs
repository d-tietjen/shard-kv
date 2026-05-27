use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, simple, write_frame, write_resp_simple_string, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectResult};

define_redis_command!(HMSet, "HMSET", true);

impl crate::commands::redis::RedisCommand for HMSet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 3 || !args[1..].len().is_multiple_of(2) {
            return wrong_arity("HMSET");
        }
        let fields = args[1..]
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .collect::<Vec<_>>();
        match store.hset_many(args[0], &fields) {
            RedisObjectResult::WrongType => wrongtype(),
            _ => simple("OK"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 3 || !args[1..].len().is_multiple_of(2) {
            write_frame(out, &wrong_arity("HMSET"));
            return;
        }

        let fields = args[1..]
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .collect::<Vec<_>>();
        match store.hset_many(args[0], &fields) {
            RedisObjectResult::WrongType => write_frame(out, &wrongtype()),
            _ => write_resp_simple_string(out, "OK"),
        }
    }
}
