use crate::commands::httl::run_hash_field_ttl_query;
use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HPExpireTime, "HPEXPIRETIME", false);

impl crate::commands::redis::RedisCommand for HPExpireTime {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        run_hash_field_ttl_query(store, args, "HPEXPIRETIME", true, true)
    }
}
