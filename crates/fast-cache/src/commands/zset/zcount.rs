#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, parse_score_bound, write_frame, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectError};

define_redis_command!(ZCount, "ZCOUNT", false);

impl crate::commands::redis::RedisCommand for ZCount {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, min, max] => {
                let (Ok(min), Ok(max)) = (parse_score_bound(min), parse_score_bound(max)) else {
                    return error("ERR min or max is not a float");
                };
                let (min, min_inclusive) = min.score_parts();
                let (max, max_inclusive) = max.score_parts();
                match store.zcount_range(key, min, min_inclusive, max, max_inclusive) {
                    Ok(count) => int(count),
                    Err(RedisObjectError::WrongType) => wrongtype(),
                    Err(RedisObjectError::MissingKey) => int(0),
                }
            }
            _ => wrong_arity("ZCOUNT"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, min, max] => {
                let (Ok(min), Ok(max)) = (parse_score_bound(min), parse_score_bound(max)) else {
                    write_frame(out, &error("ERR min or max is not a float"));
                    return;
                };
                let (min, min_inclusive) = min.score_parts();
                let (max, max_inclusive) = max.score_parts();
                match store.zcount_range(key, min, min_inclusive, max, max_inclusive) {
                    Ok(count) => ServerWire::write_resp_integer(out, count),
                    Err(RedisObjectError::WrongType) => write_frame(out, &wrongtype()),
                    Err(RedisObjectError::MissingKey) => ServerWire::write_resp_integer(out, 0),
                }
            }
            _ => write_frame(out, &wrong_arity("ZCOUNT")),
        }
    }
}
