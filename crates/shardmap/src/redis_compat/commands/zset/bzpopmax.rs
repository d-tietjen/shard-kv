#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::bzpop;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BZPopMax, "BZPOPMAX", true);

impl crate::commands::redis::RedisCommand for BZPopMax {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        bzpop(store, args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        crate::commands::zset_shared::write_bzpop_resp(store, args, true, out);
    }
}
