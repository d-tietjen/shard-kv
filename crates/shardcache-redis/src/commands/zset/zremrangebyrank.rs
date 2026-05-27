use crate::commands::formal_range::normalize_redis_range;
#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, frame_from_result, int, parse_i64, write_resp_wrong_arity,
    write_resp_wrongtype, write_result_resp, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectError};

define_redis_command!(ZRemRangeByRank, "ZREMRANGEBYRANK", true);

impl crate::commands::redis::RedisCommand for ZRemRangeByRank {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, start, stop] => {
                let (Ok(start), Ok(stop)) = (parse_i64(start), parse_i64(stop)) else {
                    return error("ERR value is not an integer or out of range");
                };
                let entries = match store.zentries(key) {
                    Ok(entries) => entries,
                    Err(RedisObjectError::WrongType) => return wrongtype(),
                    Err(RedisObjectError::MissingKey) => return int(0),
                };
                let Some(range) = normalize_redis_range(entries.len(), start, stop) else {
                    return int(0);
                };
                let (start, stop) = range.into_bounds();
                let members = entries[start..=stop]
                    .iter()
                    .map(|(member, _)| member.as_slice())
                    .collect::<Vec<_>>();
                frame_from_result(store.zrem_many(key, &members))
            }
            _ => wrong_arity("ZREMRANGEBYRANK"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, start, stop] => {
                let (Ok(start), Ok(stop)) = (parse_i64(start), parse_i64(stop)) else {
                    ServerWire::write_resp_error(
                        out,
                        "ERR value is not an integer or out of range",
                    );
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
                let Some(range) = normalize_redis_range(entries.len(), start, stop) else {
                    ServerWire::write_resp_integer(out, 0);
                    return;
                };
                let (start, stop) = range.into_bounds();
                let members = entries[start..=stop]
                    .iter()
                    .map(|(member, _)| member.as_slice())
                    .collect::<Vec<_>>();
                write_result_resp(out, store.zrem_many(key, &members));
            }
            _ => write_resp_wrong_arity(out, "ZREMRANGEBYRANK"),
        }
    }
}
