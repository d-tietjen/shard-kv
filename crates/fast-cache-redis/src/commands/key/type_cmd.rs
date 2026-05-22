use crate::commands::redis::{define_redis_command, simple, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Type, "TYPE", false);

impl crate::commands::redis::RedisCommand for Type {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => simple(store.redis_type(key)),
            _ => wrong_arity("TYPE"),
        }
    }
}
