use bytes::BytesMut;

use crate::commands::expire::{ExpireCondition, expire_at_changed, parse_expire_condition_frame};
use crate::commands::redis::{
    define_redis_command, error, int, parse_i64, write_frame, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(ExpireAt, "EXPIREAT", true);

impl crate::commands::redis::RedisCommand for ExpireAt {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        execute_absolute_expire(store, args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_absolute_expire_resp(store, args, false, out);
    }
}

pub(crate) fn execute_absolute_expire(
    store: &EmbeddedStore,
    args: &[&[u8]],
    millis: bool,
) -> Frame {
    let command = if millis { "PEXPIREAT" } else { "EXPIREAT" };
    match args {
        [key, timestamp, options @ ..] if options.len() <= 1 => match parse_i64(timestamp) {
            Ok(timestamp) => {
                let expire_at_ms = match millis {
                    true => timestamp,
                    false => timestamp.saturating_mul(1_000),
                };
                let Ok(condition) = parse_expire_condition_frame(options) else {
                    return error("ERR syntax error");
                };
                int(expire_at_changed_i64(store, key, expire_at_ms, condition))
            }
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity(command),
    }
}

pub(crate) fn expire_at_changed_i64(
    store: &EmbeddedStore,
    key: &[u8],
    expire_at_ms: i64,
    condition: ExpireCondition,
) -> i64 {
    match expire_at_ms <= 0 {
        true => expire_at_changed(store, key, 0, condition),
        false => expire_at_changed(store, key, expire_at_ms as u64, condition),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_absolute_expire_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    millis: bool,
    out: &mut BytesMut,
) {
    let command = if millis { "PEXPIREAT" } else { "EXPIREAT" };
    match args {
        [key, timestamp, options @ ..] if options.len() <= 1 => match parse_i64(timestamp) {
            Ok(timestamp) => {
                let expire_at_ms = match millis {
                    true => timestamp,
                    false => timestamp.saturating_mul(1_000),
                };
                let Ok(condition) = parse_expire_condition_frame(options) else {
                    ServerWire::write_resp_error(out, "ERR syntax error");
                    return;
                };
                ServerWire::write_resp_integer(
                    out,
                    expire_at_changed_i64(store, key, expire_at_ms, condition),
                );
            }
            Err(_) => {
                ServerWire::write_resp_error(out, "ERR value is not an integer or out of range")
            }
        },
        _ => write_frame(out, &wrong_arity(command)),
    }
}
