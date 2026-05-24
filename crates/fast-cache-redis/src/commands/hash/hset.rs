#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, frame_from_result, write_resp_wrong_arity, write_result_resp, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HSet, "HSET", true);

impl crate::commands::redis::RedisCommand for HSet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 3 || !args[1..].len().is_multiple_of(2) {
            return wrong_arity("HSET");
        }
        frame_from_result(
            store.hset_many(
                args[0],
                &args[1..]
                    .chunks_exact(2)
                    .map(|pair| (pair[0], pair[1]))
                    .collect::<Vec<_>>(),
            ),
        )
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 3 || !args[1..].len().is_multiple_of(2) {
            write_resp_wrong_arity(out, "HSET");
            return;
        }
        let fields = args[1..]
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .collect::<Vec<_>>();
        write_result_resp(out, store.hset_many(args[0], &fields));
    }
}
