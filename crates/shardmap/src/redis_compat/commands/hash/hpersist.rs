use crate::commands::hexpire::{fields_clause_error, parse_fields_clause};
use crate::commands::redis::{define_redis_command, frame_from_result, wrong_arity};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisHashStore};

define_redis_command!(HPersist, "HPERSIST", true);

impl crate::commands::redis::RedisCommand for HPersist {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, tail @ ..] = args else {
            return wrong_arity("HPERSIST");
        };
        let Some(fields) = parse_fields_clause(tail) else {
            return fields_clause_error();
        };
        frame_from_result(store.hash_field_persist(key, &fields))
    }
}
