use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(CommandInfo, "COMMAND", false);

impl crate::commands::redis::RedisCommand for CommandInfo {
    fn execute(_store: &EmbeddedStore, _args: &[&[u8]]) -> Frame {
        Frame::Array(Vec::new())
    }
}
