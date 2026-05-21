use crate::commands::redis::{bulk, define_redis_command, simple, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Ping, "PING", false);

impl crate::commands::redis::RedisCommand for Ping {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => simple("PONG"),
            [payload] => bulk((*payload).to_vec()),
            _ => wrong_arity("PING"),
        }
    }
}
