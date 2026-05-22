use crate::storage::RedisHashStore;
#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, object_result};
#[cfg(feature = "server")]
use crate::commands::redis::{
    finish_object_array_visit, write_frame, write_object_array_item, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HVals, "HVALS", false);

impl crate::commands::redis::RedisCommand for HVals {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("HVALS", args, 1, || store.hvals(args[0]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => {
                let outcome = store.hvals_visit(key, |item| write_object_array_item(out, item));
                finish_object_array_visit(out, outcome);
            }
            _ => write_frame(out, &wrong_arity("HVALS")),
        }
    }
}
