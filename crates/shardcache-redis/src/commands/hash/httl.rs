use crate::commands::hexpire::{fields_clause_error, parse_fields_clause};
use crate::commands::redis::{define_redis_command, frame_from_result, wrong_arity};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisHashStore};

define_redis_command!(HTtl, "HTTL", false);

impl crate::commands::redis::RedisCommand for HTtl {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        run_hash_field_ttl_query(store, args, "HTTL", false, false)
    }
}

/// Shared driver for HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME.
///
/// `key FIELDS numfields field [field ...]`
pub(crate) fn run_hash_field_ttl_query(
    store: &EmbeddedStore,
    args: &[&[u8]],
    name: &str,
    as_millis: bool,
    absolute: bool,
) -> Frame {
    let [key, tail @ ..] = args else {
        return wrong_arity(name);
    };
    let Some(fields) = parse_fields_clause(tail) else {
        return fields_clause_error();
    };
    frame_from_result(store.hash_field_ttl_query(key, &fields, as_millis, absolute))
}
