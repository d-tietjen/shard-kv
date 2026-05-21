use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, int, write_frame, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(SetNx, "SETNX", true);

impl crate::commands::redis::RedisCommand for SetNx {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, value] => int(setnx_value(store, key, value)),
            _ => wrong_arity("SETNX"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, value] => ServerWire::write_resp_integer(out, setnx_value(store, key, value)),
            _ => write_frame(out, &wrong_arity("SETNX")),
        }
    }
}

fn setnx_value(store: &EmbeddedStore, key: &[u8], value: &[u8]) -> i64 {
    match store.exists(key) {
        true => 0,
        _ => {
            store.set(key.to_vec(), value.to_vec(), None);
            1
        }
    }
}
