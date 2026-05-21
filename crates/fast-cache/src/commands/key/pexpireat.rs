use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(PExpireAt, "PEXPIREAT", true);

impl crate::commands::redis::RedisCommand for PExpireAt {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        crate::commands::expireat::execute_absolute_expire(store, args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        crate::commands::expireat::write_absolute_expire_resp(store, args, true, out);
    }
}
