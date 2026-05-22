use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(RenameNx, "RENAMENX", true);

impl crate::commands::redis::RedisCommand for RenameNx {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        crate::commands::rename::execute_rename(store, args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        crate::commands::rename::write_rename_resp(store, args, true, out);
    }
}
