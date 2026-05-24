#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, object_result, write_resp_wrong_arity, write_result_resp,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HSetNx, "HSETNX", true);

impl crate::commands::redis::RedisCommand for HSetNx {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("HSETNX", args, 3, || {
            store.hsetnx(args[0], args[1], args[2])
        })
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, field, value] => write_result_resp(out, store.hsetnx(key, field, value)),
            _ => write_resp_wrong_arity(out, "HSETNX"),
        }
    }
}
