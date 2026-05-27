#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, simple, write_frame, write_resp_simple_string, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Type, "TYPE", false);

impl crate::commands::redis::RedisCommand for Type {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => simple(store.redis_type(key)),
            _ => wrong_arity("TYPE"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => write_resp_simple_string(out, store.redis_type(key)),
            _ => write_frame(out, &wrong_arity("TYPE")),
        }
    }
}
