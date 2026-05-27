#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::bzpop;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BZPopMin, "BZPOPMIN", true);

impl crate::commands::redis::RedisCommand for BZPopMin {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        bzpop(store, args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        crate::commands::zset_shared::write_bzpop_resp(store, args, false, out);
    }
}
