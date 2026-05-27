#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, frame_from_result, int, parse_score_bound, write_resp_wrong_arity,
    write_resp_wrongtype, write_result_resp, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectError};

define_redis_command!(ZRemRangeByScore, "ZREMRANGEBYSCORE", true);

impl crate::commands::redis::RedisCommand for ZRemRangeByScore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, min, max] => {
                let (Ok(min), Ok(max)) = (parse_score_bound(min), parse_score_bound(max)) else {
                    return error("ERR min or max is not a float");
                };
                let entries = match store.zentries(key) {
                    Ok(entries) => entries,
                    Err(RedisObjectError::WrongType) => return wrongtype(),
                    Err(RedisObjectError::MissingKey) => return int(0),
                };
                let members = entries
                    .iter()
                    .filter(|(_, score)| min.contains(*score, true) && max.contains(*score, false))
                    .map(|(member, _)| member.as_slice())
                    .collect::<Vec<_>>();
                if members.is_empty() {
                    int(0)
                } else {
                    frame_from_result(store.zrem_many(key, &members))
                }
            }
            _ => wrong_arity("ZREMRANGEBYSCORE"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, min, max] => {
                let (Ok(min), Ok(max)) = (parse_score_bound(min), parse_score_bound(max)) else {
                    ServerWire::write_resp_error(out, "ERR min or max is not a float");
                    return;
                };
                let entries = match store.zentries(key) {
                    Ok(entries) => entries,
                    Err(RedisObjectError::WrongType) => {
                        write_resp_wrongtype(out);
                        return;
                    }
                    Err(RedisObjectError::MissingKey) => {
                        ServerWire::write_resp_integer(out, 0);
                        return;
                    }
                };
                let members = entries
                    .iter()
                    .filter(|(_, score)| min.contains(*score, true) && max.contains(*score, false))
                    .map(|(member, _)| member.as_slice())
                    .collect::<Vec<_>>();
                if members.is_empty() {
                    ServerWire::write_resp_integer(out, 0);
                } else {
                    write_result_resp(out, store.zrem_many(key, &members));
                }
            }
            _ => write_resp_wrong_arity(out, "ZREMRANGEBYSCORE"),
        }
    }
}
