use crate::commands::httl::run_hash_field_ttl_query;
use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HPTtl, "HPTTL", false);

impl crate::commands::redis::RedisCommand for HPTtl {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        run_hash_field_ttl_query(store, args, "HPTTL", true, false)
    }
}
