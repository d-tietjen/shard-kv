use crate::commands::hexpire::{
    TimeBase, TimeUnit, fields_clause_error, parse_fields_clause, resolve_deadline_ms,
};
use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, frame_from_result, parse_i64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, HashFieldGetExpireAction};

define_redis_command!(HGetEx, "HGETEX", true);

impl crate::commands::redis::RedisCommand for HGetEx {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, rest @ ..] = args else {
            return wrong_arity("HGETEX");
        };
        let (action, fields_tail) = match parse_hgetex_action(rest) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        let Some(fields) = parse_fields_clause(fields_tail) else {
            return fields_clause_error();
        };
        frame_from_result(store.hgetex(key, &fields, action))
    }
}

fn parse_hgetex_action<'a>(
    args: &'a [&'a [u8]],
) -> Result<(HashFieldGetExpireAction, &'a [&'a [u8]]), Frame> {
    match args {
        [fields, ..] if eq_ignore_ascii_case(fields, b"FIELDS") => {
            Ok((HashFieldGetExpireAction::Keep, args))
        }
        [option, fields_tail @ ..] if eq_ignore_ascii_case(option, b"PERSIST") => {
            Ok((HashFieldGetExpireAction::Persist, fields_tail))
        }
        [option, raw_ttl, fields_tail @ ..] => {
            let (unit, base) = match *option {
                option if eq_ignore_ascii_case(option, b"EX") => {
                    (TimeUnit::Seconds, TimeBase::Relative)
                }
                option if eq_ignore_ascii_case(option, b"PX") => {
                    (TimeUnit::Millis, TimeBase::Relative)
                }
                option if eq_ignore_ascii_case(option, b"EXAT") => {
                    (TimeUnit::Seconds, TimeBase::Absolute)
                }
                option if eq_ignore_ascii_case(option, b"PXAT") => {
                    (TimeUnit::Millis, TimeBase::Absolute)
                }
                _ => return Ok((HashFieldGetExpireAction::Keep, args)),
            };
            let Ok(ttl) = parse_i64(raw_ttl) else {
                return Err(error("ERR value is not an integer or out of range"));
            };
            let Some(expire_at_ms) = resolve_deadline_ms(ttl, unit, base) else {
                return Err(error("ERR invalid expire time, must be >= 0"));
            };
            Ok((
                HashFieldGetExpireAction::ExpireAt(expire_at_ms),
                fields_tail,
            ))
        }
        _ => Ok((HashFieldGetExpireAction::Keep, args)),
    }
}
