use crate::commands::redis::{define_redis_command, error, simple, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Select, "SELECT", false);

impl crate::commands::redis::RedisCommand for Select {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [b"0"] => simple("OK"),
            [_] => error("ERR DB index is out of range"),
            _ => wrong_arity("SELECT"),
        }
    }
}
