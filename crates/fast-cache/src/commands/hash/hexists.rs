#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, object_result};
#[cfg(feature = "server")]
use crate::commands::redis::{finish_object_integer_visit, write_frame, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;
#[cfg(feature = "server")]
use crate::storage::hash_key;

define_redis_command!(HExists, "HEXISTS", false);

impl crate::commands::redis::RedisCommand for HExists {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("HEXISTS", args, 2, || store.hexists(args[0], args[1]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, field] => {
                let mut value = 0;
                let outcome = store.object_read_hashed_visit(hash_key(key), key, |bucket| {
                    bucket.hexists_visit(key, field, |exists| value = exists)
                });
                finish_object_integer_visit(out, outcome, value, 0);
            }
            _ => write_frame(out, &wrong_arity("HEXISTS")),
        }
    }
}
