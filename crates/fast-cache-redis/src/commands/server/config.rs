use crate::commands::redis::{define_redis_command, eq_ignore_ascii_case, simple};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Config, "CONFIG", false);

impl crate::commands::redis::RedisCommand for Config {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub, ..] if eq_ignore_ascii_case(sub, b"GET") => Frame::Array(Vec::new()),
            _ => simple("OK"),
        }
    }
}
