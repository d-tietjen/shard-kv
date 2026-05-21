use crate::commands::redis::{bulk, define_redis_command, optional_string_value, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(GetDel, "GETDEL", true);

impl crate::commands::redis::RedisCommand for GetDel {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => match optional_string_value(store, key, true) {
                Ok(old) => {
                    if old.is_some() {
                        store.delete(key);
                    }
                    old.map_or(Frame::Null, bulk)
                }
                Err(frame) => frame,
            },
            _ => wrong_arity("GETDEL"),
        }
    }
}
