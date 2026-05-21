#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, int, write_frame, wrong_arity, wrongtype};
use crate::commands::string_shared::strlen_value;
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(StrLen, "STRLEN", false);

impl crate::commands::redis::RedisCommand for StrLen {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => strlen_value(store, key)
                .map(int)
                .unwrap_or_else(|()| wrongtype()),
            _ => wrong_arity("STRLEN"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => match strlen_value(store, key) {
                Ok(len) => ServerWire::write_resp_integer(out, len),
                Err(()) => write_frame(out, &wrongtype()),
            },
            _ => write_frame(out, &wrong_arity("STRLEN")),
        }
    }
}
