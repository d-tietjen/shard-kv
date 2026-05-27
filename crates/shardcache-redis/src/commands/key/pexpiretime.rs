#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::expiretime::expire_time;
use crate::commands::redis::{define_redis_command, int, write_frame, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(PExpireTime, "PEXPIRETIME", false);

impl crate::commands::redis::RedisCommand for PExpireTime {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => int(expire_time(store, key, true)),
            _ => wrong_arity("PEXPIRETIME"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => ServerWire::write_resp_integer(out, expire_time(store, key, true)),
            _ => write_frame(out, &wrong_arity("PEXPIRETIME")),
        }
    }
}
