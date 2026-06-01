#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BLMPop, "BLMPOP", true);

impl crate::commands::redis::RedisCommand for BLMPop {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        crate::commands::list_shared::list_mpop(store, args, true, "BLMPOP")
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        crate::commands::list_shared::write_list_mpop_resp(store, args, true, "BLMPOP", out);
    }
}
