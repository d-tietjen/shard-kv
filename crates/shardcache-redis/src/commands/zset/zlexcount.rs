#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, parse_lex_bound, write_resp_wrong_arity,
    write_resp_wrongtype, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectError};

define_redis_command!(ZLexCount, "ZLEXCOUNT", false);

impl crate::commands::redis::RedisCommand for ZLexCount {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, min, max] => {
                let (Ok(min), Ok(max)) = (parse_lex_bound(min), parse_lex_bound(max)) else {
                    return error("ERR min or max not valid string range item");
                };
                match store.zentries(key) {
                    Ok(entries) => int(entries
                        .iter()
                        .filter(|(member, _)| {
                            min.contains(member.as_slice(), true)
                                && max.contains(member.as_slice(), false)
                        })
                        .count() as i64),
                    Err(RedisObjectError::WrongType) => wrongtype(),
                    Err(RedisObjectError::MissingKey) => int(0),
                }
            }
            _ => wrong_arity("ZLEXCOUNT"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, min, max] => {
                let (Ok(min), Ok(max)) = (parse_lex_bound(min), parse_lex_bound(max)) else {
                    ServerWire::write_resp_error(out, "ERR min or max not valid string range item");
                    return;
                };
                match store.zentries(key) {
                    Ok(entries) => {
                        let count = entries
                            .iter()
                            .filter(|(member, _)| {
                                min.contains(member.as_slice(), true)
                                    && max.contains(member.as_slice(), false)
                            })
                            .count();
                        ServerWire::write_resp_integer(out, count as i64);
                    }
                    Err(RedisObjectError::WrongType) => write_resp_wrongtype(out),
                    Err(RedisObjectError::MissingKey) => ServerWire::write_resp_integer(out, 0),
                }
            }
            _ => write_resp_wrong_arity(out, "ZLEXCOUNT"),
        }
    }
}
