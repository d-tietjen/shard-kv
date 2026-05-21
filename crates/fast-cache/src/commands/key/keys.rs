use crate::commands::redis::{array_bulk, define_redis_command, filter_key_pattern, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Keys, "KEYS", false);

impl crate::commands::redis::RedisCommand for Keys {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [pattern] => array_bulk(filter_key_pattern(store.key_snapshot_unsorted(), pattern)),
            _ => wrong_arity("KEYS"),
        }
    }
}
