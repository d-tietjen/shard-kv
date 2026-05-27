use crate::storage::RedisListStore;
#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, finish_object_integer_visit, object_result, write_frame, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LLen, "LLEN", false);

impl crate::commands::redis::RedisCommand for LLen {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("LLEN", args, 1, || store.llen(args[0]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => {
                let mut value = 0;
                let outcome = store.llen_visit(key, |len| value = len);
                finish_object_integer_visit(out, outcome, value, 0);
            }
            _ => write_frame(out, &wrong_arity("LLEN")),
        }
    }
}
