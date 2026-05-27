#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::list_shared::write_push_list_resp;
use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LPush, "LPUSH", true);

impl crate::commands::redis::RedisCommand for LPush {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        crate::commands::list_shared::push_list(store, args, true, false, "LPUSH")
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_push_list_resp(store, args, true, false, "LPUSH", out);
    }
}
