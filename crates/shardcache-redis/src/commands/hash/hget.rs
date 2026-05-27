use crate::storage::RedisObjectStoreAccess;
#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, object_result};
#[cfg(feature = "server")]
use crate::commands::redis::{finish_object_bulk_visit, write_frame, write_resp_null, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;
#[cfg(feature = "server")]
use crate::storage::hash_key;

define_redis_command!(HGet, "HGET", false);

impl crate::commands::redis::RedisCommand for HGet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("HGET", args, 2, || store.hget(args[0], args[1]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, field] => {
                let outcome = store.object_read_hashed_visit(hash_key(key), key, |bucket| {
                    bucket.hget_visit(key, field, |value| match value {
                        Some(value) => ServerWire::write_resp_blob_string(out, value),
                        None => write_resp_null(out),
                    })
                });
                finish_object_bulk_visit(out, outcome);
            }
            _ => write_frame(out, &wrong_arity("HGET")),
        }
    }
}
