use crate::commands::redis::{define_redis_command, int, wrong_arity, wrongtype};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectError, RedisObjectResult};

define_redis_command!(SMove, "SMOVE", true);

impl crate::commands::redis::RedisCommand for SMove {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [source, dest, member] => match store.set_members(source) {
                Ok(members) if members.iter().any(|item| item.as_slice() == *member) => {
                    match store.sadd(dest, &[*member]) {
                        RedisObjectResult::WrongType => wrongtype(),
                        _ => {
                            let _ = store.srem(source, &[*member]);
                            int(1)
                        }
                    }
                }
                Ok(_) => int(0),
                Err(RedisObjectError::WrongType) => wrongtype(),
                Err(RedisObjectError::MissingKey) => int(0),
            },
            _ => wrong_arity("SMOVE"),
        }
    }
}
