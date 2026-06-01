use crate::commands::redis::{
    array_bulk, define_redis_command, eq_ignore_ascii_case, simple, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HotKeys, "HOTKEYS", false);

impl crate::commands::redis::RedisCommand for HotKeys {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"START") => simple("OK"),
            [subcommand] if eq_ignore_ascii_case(subcommand, b"STOP") => simple("OK"),
            [subcommand] if eq_ignore_ascii_case(subcommand, b"RESET") => simple("OK"),
            [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"GET") => {
                Frame::Array(Vec::new())
            }
            [subcommand] if eq_ignore_ascii_case(subcommand, b"HELP") => array_bulk(vec![
                b"HOTKEYS START [options]".to_vec(),
                b"HOTKEYS GET".to_vec(),
                b"HOTKEYS RESET".to_vec(),
                b"HOTKEYS STOP".to_vec(),
            ]),
            _ => wrong_arity("HOTKEYS"),
        }
    }
}
