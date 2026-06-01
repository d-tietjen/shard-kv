#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(LMPop, "LMPOP", true);

impl crate::commands::redis::RedisCommand for LMPop {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        crate::commands::list_shared::list_mpop(store, args, false, "LMPOP")
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        crate::commands::list_shared::write_list_mpop_resp(store, args, false, "LMPOP", out);
    }
}
