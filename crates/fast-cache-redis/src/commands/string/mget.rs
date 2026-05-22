use crate::commands::redis::{bulk, define_redis_command, optional_string_value, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(MGet, "MGET", false);

impl crate::commands::redis::RedisCommand for MGet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.is_empty() {
            return wrong_arity("MGET");
        }
        Frame::Array(
            args.iter()
                .map(|key| match optional_string_value(store, key, false) {
                    Ok(Some(value)) => bulk(value),
                    _ => Frame::Null,
                })
                .collect(),
        )
    }
}
