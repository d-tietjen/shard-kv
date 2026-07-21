//! Redis-compatible command registry and shared command support.

#[macro_use]
#[path = "redis/macros.rs"]
mod macros;
#[path = "redis/adapter.rs"]
mod adapter;
#[path = "redis/dispatch.rs"]
mod dispatch;
#[path = "redis/frame.rs"]
mod frame;
#[path = "redis/parse.rs"]
mod parse;

#[path = "redis/key.rs"]
mod key;

pub(crate) use crate::commands::zset_shared::{
    write_zrange_rank_resp, zrange_by_rank_impl, zrange_by_score_impl,
};
pub(crate) use adapter::RedisCommand;
pub(crate) use define_redis_command;
pub(crate) use dispatch::dispatch as dispatch_redis_command;
pub(crate) use frame::{
    array_bulk, bulk, error, frame_from_result, int, object_result, optional_string_value,
    reserve_resp_bulk_array_hint, scan_array, scan_array_with_cursor, scan_from_result, simple,
    string_value, write_frame, write_resp_array_header, write_resp_null, write_resp_simple_string,
    write_resp_wrong_arity, write_resp_wrongtype, write_result_resp, wrong_arity, wrongtype,
    zentries_flat, zentries_frame,
};
#[cfg(feature = "server")]
#[allow(unused_imports)]
pub(crate) use frame::{with_resp_protocol, write_fast_frame};
#[cfg(feature = "server")]
pub(crate) use key::FastObjectArrayWriter;
pub(crate) use key::{
    filter_key_pattern, finish_object_array_visit, finish_object_bulk_visit,
    finish_object_integer_visit, finish_scan_object_array_visit, key_pattern_matches,
    parse_key_scan_args, write_object_array_item, write_scan_object_array_item,
    write_scan_resp_from_options, write_scnp_scan_fast_response,
    write_scnp_scan_shard_fast_response,
};
pub(crate) use parse::{
    eq_ignore_ascii_case, parse_f64, parse_i64, parse_lex_bound, parse_score_bound, parse_u64,
    parse_usize,
};
