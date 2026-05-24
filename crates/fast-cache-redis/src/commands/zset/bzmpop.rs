#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::{write_zmpop_resp, zmpop};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BZMPop, "BZMPOP", true);

impl crate::commands::redis::RedisCommand for BZMPop {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zmpop(store, args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_zmpop_resp(store, args, true, out);
    }
}
