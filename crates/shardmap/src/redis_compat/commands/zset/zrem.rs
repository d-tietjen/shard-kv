#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, frame_from_result, write_resp_wrong_arity, write_result_resp, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZRem, "ZREM", true);

impl crate::commands::redis::RedisCommand for ZRem {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 2 {
            return wrong_arity("ZREM");
        }
        frame_from_result(store.zrem_many(args[0], &args[1..]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 2 {
            write_resp_wrong_arity(out, "ZREM");
            return;
        }
        write_result_resp(out, store.zrem_many(args[0], &args[1..]));
    }
}
