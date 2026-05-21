use crate::commands::redis::define_redis_command;
use crate::commands::zset_shared::zrangebylex;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(ZRevRangeByLex, "ZREVRANGEBYLEX", false);

impl crate::commands::redis::RedisCommand for ZRevRangeByLex {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        zrangebylex(store, args, true)
    }
}
