use crate::storage::RedisObjectStoreAccess;
#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, object_result};
#[cfg(feature = "server")]
use crate::commands::redis::{finish_object_integer_visit, write_frame, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;
#[cfg(feature = "server")]
use crate::storage::hash_key;

define_redis_command!(HLen, "HLEN", false);

impl crate::commands::redis::RedisCommand for HLen {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("HLEN", args, 1, || store.hlen(args[0]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => {
                let mut value = 0;
                let outcome = store.object_read_hashed_visit(hash_key(key), key, |bucket| {
                    bucket.hlen_visit(key, |len| value = len)
                });
                finish_object_integer_visit(out, outcome, value, 0);
            }
            _ => write_frame(out, &wrong_arity("HLEN")),
        }
    }
}
