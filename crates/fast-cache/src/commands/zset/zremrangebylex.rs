use crate::commands::redis::{
    define_redis_command, error, frame_from_result, int, parse_lex_bound, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectError};

define_redis_command!(ZRemRangeByLex, "ZREMRANGEBYLEX", true);

impl crate::commands::redis::RedisCommand for ZRemRangeByLex {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, min, max] => {
                let (Ok(min), Ok(max)) = (parse_lex_bound(min), parse_lex_bound(max)) else {
                    return error("ERR min or max not valid string range item");
                };
                let entries = match store.zentries(key) {
                    Ok(entries) => entries,
                    Err(RedisObjectError::WrongType) => return wrongtype(),
                    Err(RedisObjectError::MissingKey) => return int(0),
                };
                let members = entries
                    .iter()
                    .filter(|(member, _)| {
                        min.contains(member.as_slice(), true)
                            && max.contains(member.as_slice(), false)
                    })
                    .map(|(member, _)| member.as_slice())
                    .collect::<Vec<_>>();
                if members.is_empty() {
                    int(0)
                } else {
                    frame_from_result(store.zrem_many(key, &members))
                }
            }
            _ => wrong_arity("ZREMRANGEBYLEX"),
        }
    }
}
