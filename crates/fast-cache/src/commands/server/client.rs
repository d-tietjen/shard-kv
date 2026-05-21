use crate::commands::redis::{bulk, define_redis_command, eq_ignore_ascii_case, int, simple};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Client, "CLIENT", false);

impl crate::commands::redis::RedisCommand for Client {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub] if eq_ignore_ascii_case(sub, b"GETNAME") => Frame::Null,
            [sub, _] if eq_ignore_ascii_case(sub, b"SETNAME") => simple("OK"),
            [sub] if eq_ignore_ascii_case(sub, b"ID") => int(0),
            [sub] if eq_ignore_ascii_case(sub, b"LIST") => bulk(Vec::new()),
            [sub, ..] if eq_ignore_ascii_case(sub, b"KILL") => int(0),
            _ => simple("OK"),
        }
    }
}
