use crate::commands::redis::{define_redis_command, simple};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Quit, "QUIT", false);

impl crate::commands::redis::RedisCommand for Quit {
    fn execute(_store: &EmbeddedStore, _args: &[&[u8]]) -> Frame {
        simple("OK")
    }
}
