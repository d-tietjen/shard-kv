use crate::commands::hexpire::{fields_clause_error, parse_fields_clause};
use crate::commands::redis::{define_redis_command, frame_from_result, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(HGetDel, "HGETDEL", true);

impl crate::commands::redis::RedisCommand for HGetDel {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, tail @ ..] = args else {
            return wrong_arity("HGETDEL");
        };
        let Some(fields) = parse_fields_clause(tail) else {
            return fields_clause_error();
        };
        frame_from_result(store.hgetdel(key, &fields))
    }
}
