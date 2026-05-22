use crate::commands::redis::{bulk, define_redis_command, optional_string_value, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(GetSet, "GETSET", true);

impl crate::commands::redis::RedisCommand for GetSet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, value] => match optional_string_value(store, key, true) {
                Ok(old) => {
                    store.set((*key).to_vec(), (*value).to_vec(), None);
                    old.map_or(Frame::Null, bulk)
                }
                Err(frame) => frame,
            },
            _ => wrong_arity("GETSET"),
        }
    }
}
