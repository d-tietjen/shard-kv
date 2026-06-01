use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, frame_from_result, parse_i64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, HashFieldExpireCond, RedisHashStore, now_millis};

define_redis_command!(HExpire, "HEXPIRE", true);

impl crate::commands::redis::RedisCommand for HExpire {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        run_hash_field_expire(
            store,
            args,
            "HEXPIRE",
            TimeUnit::Seconds,
            TimeBase::Relative,
        )
    }
}

#[derive(Clone, Copy)]
pub(crate) enum TimeUnit {
    Seconds,
    Millis,
}

#[derive(Clone, Copy)]
pub(crate) enum TimeBase {
    Relative,
    Absolute,
}

/// Shared driver for HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT.
///
/// `key ttl [NX|XX|GT|LT] FIELDS numfields field [field ...]`
pub(crate) fn run_hash_field_expire(
    store: &EmbeddedStore,
    args: &[&[u8]],
    name: &str,
    unit: TimeUnit,
    base: TimeBase,
) -> Frame {
    let [key, ttl_raw, rest @ ..] = args else {
        return wrong_arity(name);
    };
    let Ok(ttl) = parse_i64(ttl_raw) else {
        return error("ERR value is not an integer or out of range");
    };

    let (cond, index) = match rest.first().copied() {
        Some(tok) if eq_ignore_ascii_case(tok, b"NX") => (HashFieldExpireCond::Nx, 1),
        Some(tok) if eq_ignore_ascii_case(tok, b"XX") => (HashFieldExpireCond::Xx, 1),
        Some(tok) if eq_ignore_ascii_case(tok, b"GT") => (HashFieldExpireCond::Gt, 1),
        Some(tok) if eq_ignore_ascii_case(tok, b"LT") => (HashFieldExpireCond::Lt, 1),
        _ => (HashFieldExpireCond::None, 0),
    };

    let Some(fields) = parse_fields_clause(&rest[index..]) else {
        return fields_clause_error();
    };

    let expire_at_ms = match resolve_deadline_ms(ttl, unit, base) {
        Some(value) => value,
        None => return error("ERR invalid expire time, must be >= 0"),
    };

    frame_from_result(store.hash_field_expire(key, &fields, expire_at_ms, cond))
}

/// Parse the mandatory `FIELDS numfields field [field ...]` tail. Returns the
/// field slice, or None on any structural error.
pub(crate) fn parse_fields_clause<'a>(tail: &[&'a [u8]]) -> Option<Vec<&'a [u8]>> {
    let [fields_kw, numfields_raw, fields @ ..] = tail else {
        return None;
    };
    if !eq_ignore_ascii_case(fields_kw, b"FIELDS") {
        return None;
    }
    let numfields = parse_i64(numfields_raw).ok()?;
    if numfields <= 0 {
        return None;
    }
    let numfields = numfields as usize;
    if numfields != fields.len() {
        return None;
    }
    Some(fields.to_vec())
}

pub(crate) fn fields_clause_error() -> Frame {
    error("ERR Mandatory keyword FIELDS is missing or not at the right position")
}

/// Convert (ttl, unit, base) into an absolute unix-ms deadline.
pub(crate) fn resolve_deadline_ms(ttl: i64, unit: TimeUnit, base: TimeBase) -> Option<u64> {
    match base {
        TimeBase::Absolute => {
            // Absolute timestamps may be in the past (that deletes the field);
            // negative is rejected by Redis.
            if ttl < 0 {
                return None;
            }
            let ms = match unit {
                TimeUnit::Seconds => (ttl as i128) * 1000,
                TimeUnit::Millis => ttl as i128,
            };
            u64::try_from(ms).ok()
        }
        TimeBase::Relative => {
            if ttl < 0 {
                return None;
            }
            let now = now_millis() as i128;
            let delta = match unit {
                TimeUnit::Seconds => (ttl as i128) * 1000,
                TimeUnit::Millis => ttl as i128,
            };
            let deadline = now + delta;
            u64::try_from(deadline).ok()
        }
    }
}
