use crate::commands::hexpire::{TimeBase, TimeUnit, run_hash_field_expire};
use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HExpireAt, "HEXPIREAT", true);

impl crate::commands::redis::RedisCommand for HExpireAt {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        run_hash_field_expire(
            store,
            args,
            "HEXPIREAT",
            TimeUnit::Seconds,
            TimeBase::Absolute,
        )
    }
}
