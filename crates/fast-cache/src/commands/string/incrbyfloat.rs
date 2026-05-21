use crate::commands::redis::{
    bulk, define_redis_command, error, parse_f64, string_value, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(IncrByFloat, "INCRBYFLOAT", true);

impl crate::commands::redis::RedisCommand for IncrByFloat {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, delta] => {
                let Ok(delta) = parse_f64(delta) else {
                    return error("ERR value is not a valid float");
                };
                match string_value(store, key) {
                    Ok(value) => {
                        let current = if value.is_empty() {
                            0.0
                        } else {
                            match parse_f64(&value) {
                                Ok(value) => value,
                                Err(_) => return error("ERR value is not a valid float"),
                            }
                        };
                        let next = current + delta;
                        if !next.is_finite() {
                            return error("ERR increment would produce NaN or Infinity");
                        }
                        let bytes = next.to_string().into_bytes();
                        store.set((*key).to_vec(), bytes.clone(), None);
                        bulk(bytes)
                    }
                    Err(frame) => frame,
                }
            }
            _ => wrong_arity("INCRBYFLOAT"),
        }
    }
}
