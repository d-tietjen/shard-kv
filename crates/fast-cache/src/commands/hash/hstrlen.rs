use crate::commands::redis::{define_redis_command, int, wrong_arity, wrongtype};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectResult};

define_redis_command!(HStrLen, "HSTRLEN", false);

impl crate::commands::redis::RedisCommand for HStrLen {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, field] => match store.hget(key, field) {
                RedisObjectResult::Bulk(Some(value)) => int(value.len() as i64),
                RedisObjectResult::Bulk(None) => int(0),
                RedisObjectResult::WrongType => wrongtype(),
                _ => int(0),
            },
            _ => wrong_arity("HSTRLEN"),
        }
    }
}
