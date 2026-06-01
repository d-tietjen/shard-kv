#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::zrangebylex;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZRangeByLex, "ZRANGEBYLEX", false);

impl crate::commands::redis::RedisCommand for ZRangeByLex {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zrangebylex(store, args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        crate::commands::zset_shared::write_zrange_lex_resp(store, args, false, out);
    }
}
