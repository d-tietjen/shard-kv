#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, finish_object_integer_visit, object_result, write_frame, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;
#[cfg(feature = "server")]
use crate::storage::hash_key;

define_redis_command!(ZCard, "ZCARD", false);

impl crate::commands::redis::RedisCommand for ZCard {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("ZCARD", args, 1, || store.zcard(args[0]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => {
                let mut value = 0;
                let outcome = store.object_read_hashed_visit(hash_key(key), key, |bucket| {
                    bucket.zcard_visit(key, |cardinality| value = cardinality)
                });
                finish_object_integer_visit(out, outcome, value, 0);
            }
            _ => write_frame(out, &wrong_arity("ZCARD")),
        }
    }
}
