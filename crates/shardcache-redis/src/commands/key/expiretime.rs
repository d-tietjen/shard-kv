#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, int, write_frame, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, now_millis};

define_redis_command!(ExpireTime, "EXPIRETIME", false);

impl crate::commands::redis::RedisCommand for ExpireTime {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => int(expire_time(store, key, false)),
            _ => wrong_arity("EXPIRETIME"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => ServerWire::write_resp_integer(out, expire_time(store, key, false)),
            _ => write_frame(out, &wrong_arity("EXPIRETIME")),
        }
    }
}

pub(crate) fn expire_time(store: &EmbeddedStore, key: &[u8], millis: bool) -> i64 {
    let pttl = store.pttl_millis(key);
    match pttl {
        -2 | -1 => pttl,
        ttl if ttl >= 0 => {
            let expire_at_ms = now_millis().saturating_add(ttl as u64);
            if millis {
                expire_at_ms as i64
            } else {
                (expire_at_ms / 1_000) as i64
            }
        }
        _ => -2,
    }
}
