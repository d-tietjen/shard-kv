use crate::commands::redis::{define_redis_command, int, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(DbSize, "DBSIZE", false);

impl crate::commands::redis::RedisCommand for DbSize {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => int(store.len() as i64),
            _ => wrong_arity("DBSIZE"),
        }
    }
}
