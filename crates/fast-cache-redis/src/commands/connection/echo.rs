use crate::commands::redis::{bulk, define_redis_command, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Echo, "ECHO", false);

impl crate::commands::redis::RedisCommand for Echo {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [payload] => bulk((*payload).to_vec()),
            _ => wrong_arity("ECHO"),
        }
    }
}
