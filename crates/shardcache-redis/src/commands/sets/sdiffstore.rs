#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
#[cfg(feature = "server")]
use crate::commands::set_shared::write_set_store_resp;
use crate::commands::set_shared::{SetOp, set_store};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(SDiffStore, "SDIFFSTORE", true);

impl crate::commands::redis::RedisCommand for SDiffStore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        set_store(store, args, SetOp::Diff)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_set_store_resp(store, args, SetOp::Diff, out);
    }
}
