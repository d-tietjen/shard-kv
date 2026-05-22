use crate::commands::redis::{
    define_redis_command, error, int, parse_usize, string_value, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(SetRange, "SETRANGE", true);

impl crate::commands::redis::RedisCommand for SetRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, offset, replacement] => {
                let Ok(offset) = parse_usize(offset) else {
                    return error("ERR offset is not an integer or out of range");
                };
                match string_value(store, key) {
                    Ok(mut value) => {
                        let required = offset.saturating_add(replacement.len());
                        if value.len() < required {
                            value.resize(required, 0);
                        }
                        value[offset..offset + replacement.len()].copy_from_slice(replacement);
                        let len = value.len() as i64;
                        store.set((*key).to_vec(), value, None);
                        int(len)
                    }
                    Err(frame) => frame,
                }
            }
            _ => wrong_arity("SETRANGE"),
        }
    }
}
