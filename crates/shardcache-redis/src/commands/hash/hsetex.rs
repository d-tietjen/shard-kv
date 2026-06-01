use crate::commands::hexpire::{TimeBase, TimeUnit, fields_clause_error, resolve_deadline_ms};
use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, frame_from_result, parse_i64, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, HashFieldSetCondition, HashFieldSetExpireAction};

define_redis_command!(HSetEx, "HSETEX", true);

impl crate::commands::redis::RedisCommand for HSetEx {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, rest @ ..] = args else {
            return wrong_arity("HSETEX");
        };
        let parsed = match parse_hsetex_args(rest) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        frame_from_result(store.hsetex(key, &parsed.fields, parsed.condition, parsed.expiration))
    }
}

struct HSetExArgs<'a> {
    condition: HashFieldSetCondition,
    expiration: HashFieldSetExpireAction,
    fields: Vec<(&'a [u8], &'a [u8])>,
}

fn parse_hsetex_args<'a>(args: &'a [&'a [u8]]) -> Result<HSetExArgs<'a>, Frame> {
    let mut condition = HashFieldSetCondition::Always;
    let mut expiration = HashFieldSetExpireAction::Clear;
    let mut saw_expiration = false;
    let mut index = 0usize;

    while index < args.len() {
        let token = args[index];
        if eq_ignore_ascii_case(token, b"FIELDS") {
            break;
        }
        match token {
            token if eq_ignore_ascii_case(token, b"FNX") => {
                if !matches!(condition, HashFieldSetCondition::Always) {
                    return Err(error("ERR syntax error"));
                }
                condition = HashFieldSetCondition::FnX;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"FXX") => {
                if !matches!(condition, HashFieldSetCondition::Always) {
                    return Err(error("ERR syntax error"));
                }
                condition = HashFieldSetCondition::FxX;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"KEEPTTL") => {
                if saw_expiration {
                    return Err(error("ERR syntax error"));
                }
                saw_expiration = true;
                expiration = HashFieldSetExpireAction::KeepTtl;
                index += 1;
            }
            token
                if eq_ignore_ascii_case(token, b"EX")
                    || eq_ignore_ascii_case(token, b"PX")
                    || eq_ignore_ascii_case(token, b"EXAT")
                    || eq_ignore_ascii_case(token, b"PXAT") =>
            {
                if saw_expiration {
                    return Err(error("ERR syntax error"));
                }
                let Some(raw_ttl) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                let Ok(ttl) = parse_i64(raw_ttl) else {
                    return Err(error("ERR value is not an integer or out of range"));
                };
                let (unit, base) = match token {
                    token if eq_ignore_ascii_case(token, b"EX") => {
                        (TimeUnit::Seconds, TimeBase::Relative)
                    }
                    token if eq_ignore_ascii_case(token, b"PX") => {
                        (TimeUnit::Millis, TimeBase::Relative)
                    }
                    token if eq_ignore_ascii_case(token, b"EXAT") => {
                        (TimeUnit::Seconds, TimeBase::Absolute)
                    }
                    _ => (TimeUnit::Millis, TimeBase::Absolute),
                };
                let Some(expire_at_ms) = resolve_deadline_ms(ttl, unit, base) else {
                    return Err(error("ERR invalid expire time, must be >= 0"));
                };
                saw_expiration = true;
                expiration = HashFieldSetExpireAction::ExpireAt(expire_at_ms);
                index += 2;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }

    let fields = parse_field_value_clause(&args[index..]).ok_or_else(fields_clause_error)?;
    Ok(HSetExArgs {
        condition,
        expiration,
        fields,
    })
}

fn parse_field_value_clause<'a>(tail: &'a [&'a [u8]]) -> Option<Vec<(&'a [u8], &'a [u8])>> {
    let [fields_kw, numfields_raw, pairs @ ..] = tail else {
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
    if pairs.len() != numfields.saturating_mul(2) {
        return None;
    }
    Some(
        pairs
            .chunks_exact(2)
            .map(|pair| (pair[0], pair[1]))
            .collect(),
    )
}
