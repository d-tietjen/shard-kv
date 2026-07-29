use crate::commands::redis::{
    bulk, eq_ignore_ascii_case, error, int, optional_string_value, parse_f64, parse_i64, parse_u64,
    value_digest_hex, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, now_millis};

macro_rules! define_string_v8_command {
    ($type:ident, $static_name:ident, $name:literal, $mutates:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static $static_name: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;
        }
    };
}

define_string_v8_command!(Delex, DELEX_COMMAND, "DELEX", true);
define_string_v8_command!(Digest, DIGEST_COMMAND, "DIGEST", false);
define_string_v8_command!(IncrEx, INCREX_COMMAND, "INCREX", true);
define_string_v8_command!(MSetEx, MSETEX_COMMAND, "MSETEX", true);

impl crate::commands::redis::RedisCommand for Digest {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key] = args else {
            return wrong_arity("DIGEST");
        };
        match optional_string_value(store, key, true) {
            Ok(Some(value)) => bulk(value_digest_hex(&value).to_vec()),
            Ok(None) => Frame::Null,
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for Delex {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, rest @ ..] = args else {
            return wrong_arity("DELEX");
        };
        if rest.is_empty() {
            return int(store.delete(key) as i64);
        }
        let condition = match parse_value_condition(rest) {
            Ok(condition) => condition,
            Err(frame) => return frame,
        };
        match optional_string_value(store, key, true) {
            Ok(Some(value)) if condition.matches(&value) => int(store.delete(key) as i64),
            Ok(Some(_)) | Ok(None) => int(0),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for MSetEx {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let parsed = match parse_msetex_args(args) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        let exists = parsed
            .pairs
            .iter()
            .map(|(key, _)| store.exists(key))
            .collect::<Vec<_>>();
        let allowed = match parsed.condition {
            SetCondition::Always => true,
            SetCondition::Nx => exists.iter().all(|exists| !*exists),
            SetCondition::Xx => exists.iter().all(|exists| *exists),
        };
        if !allowed {
            return int(0);
        }
        for (key, value) in parsed.pairs {
            let ttl_ms = parsed.expiration.ttl_for_key(store, key);
            store.set(key.to_vec(), value.to_vec(), ttl_ms);
        }
        int(1)
    }
}

impl crate::commands::redis::RedisCommand for IncrEx {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let parsed = match parse_increx_args(args) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        let old_value = match optional_string_value(store, parsed.key, true) {
            Ok(value) => value,
            Err(frame) => return frame,
        };
        let old_text = old_value
            .as_ref()
            .and_then(|value| std::str::from_utf8(value).ok())
            .unwrap_or("0");
        let outcome = match parsed.increment {
            IncrAmount::Integer(delta) => increment_integer(old_text, delta, parsed.bounds),
            IncrAmount::Float(delta) => increment_float(old_text, delta, parsed.bounds),
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(frame) => return frame,
        };
        if outcome.applied {
            let ttl_ms = match (parsed.expiration, parsed.enx, old_value.is_some()) {
                (ExpirationAction::Keep, _, _) => None,
                (ExpirationAction::KeepTtl, _, _) => match store.pttl_millis(parsed.key) {
                    ttl if ttl >= 0 => Some(ttl as u64),
                    _ => None,
                },
                (ExpirationAction::Persist, _, _) => {
                    store.persist(parsed.key);
                    None
                }
                (ExpirationAction::Set(_), true, true) => match store.pttl_millis(parsed.key) {
                    ttl if ttl >= 0 => Some(ttl as u64),
                    _ => None,
                },
                (ExpirationAction::Set(ttl_ms), _, _) => Some(ttl_ms),
            };
            store.set(
                parsed.key.to_vec(),
                outcome.new_value.clone().into_bytes(),
                ttl_ms,
            );
        }
        Frame::Array(vec![
            bulk(outcome.new_value.into_bytes()),
            bulk(outcome.applied_delta.into_bytes()),
        ])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetCondition {
    Always,
    Nx,
    Xx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpirationAction {
    Keep,
    KeepTtl,
    Persist,
    Set(u64),
}

impl ExpirationAction {
    fn ttl_for_key(self, store: &EmbeddedStore, key: &[u8]) -> Option<u64> {
        match self {
            Self::Keep | Self::Persist => None,
            Self::KeepTtl => match store.pttl_millis(key) {
                ttl if ttl >= 0 => Some(ttl as u64),
                _ => None,
            },
            Self::Set(ttl_ms) => Some(ttl_ms),
        }
    }
}

struct MSetExArgs<'a> {
    pairs: Vec<(&'a [u8], &'a [u8])>,
    condition: SetCondition,
    expiration: ExpirationAction,
}

fn parse_msetex_args<'a>(args: &'a [&'a [u8]]) -> Result<MSetExArgs<'a>, Frame> {
    let [numkeys_raw, tail @ ..] = args else {
        return Err(wrong_arity("MSETEX"));
    };
    let numkeys = parse_u64(numkeys_raw)
        .ok()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| error("ERR value is not an integer or out of range"))?;
    let pair_len = numkeys.saturating_mul(2);
    if tail.len() < pair_len {
        return Err(wrong_arity("MSETEX"));
    }
    let pairs = tail[..pair_len]
        .chunks_exact(2)
        .map(|pair| (pair[0], pair[1]))
        .collect::<Vec<_>>();
    let (condition, expiration) = parse_setlike_options(&tail[pair_len..])?;
    Ok(MSetExArgs {
        pairs,
        condition,
        expiration,
    })
}

#[derive(Debug, Clone, Copy)]
enum ValueCondition<'a> {
    Eq(&'a [u8]),
    Ne(&'a [u8]),
    DigestEq(&'a [u8]),
    DigestNe(&'a [u8]),
}

impl ValueCondition<'_> {
    fn matches(self, value: &[u8]) -> bool {
        match self {
            Self::Eq(expected) => value == expected,
            Self::Ne(expected) => value != expected,
            Self::DigestEq(expected) => value_digest_hex(value)
                .as_slice()
                .eq_ignore_ascii_case(expected),
            Self::DigestNe(expected) => !value_digest_hex(value)
                .as_slice()
                .eq_ignore_ascii_case(expected),
        }
    }
}

fn parse_value_condition<'a>(args: &'a [&'a [u8]]) -> Result<ValueCondition<'a>, Frame> {
    let [option, value] = args else {
        return Err(error("ERR syntax error"));
    };
    match *option {
        option if eq_ignore_ascii_case(option, b"IFEQ") => Ok(ValueCondition::Eq(value)),
        option if eq_ignore_ascii_case(option, b"IFNE") => Ok(ValueCondition::Ne(value)),
        option if eq_ignore_ascii_case(option, b"IFDEQ") => Ok(ValueCondition::DigestEq(value)),
        option if eq_ignore_ascii_case(option, b"IFDNE") => Ok(ValueCondition::DigestNe(value)),
        _ => Err(error("ERR syntax error")),
    }
}

#[derive(Debug, Clone, Copy)]
enum IncrAmount {
    Integer(i64),
    Float(f64),
}

#[derive(Debug, Clone, Copy, Default)]
struct Bounds {
    lower: Option<f64>,
    upper: Option<f64>,
    saturate: bool,
}

struct IncrExArgs<'a> {
    key: &'a [u8],
    increment: IncrAmount,
    bounds: Bounds,
    expiration: ExpirationAction,
    enx: bool,
}

struct IncrOutcome {
    new_value: String,
    applied_delta: String,
    applied: bool,
}

fn parse_increx_args<'a>(args: &'a [&'a [u8]]) -> Result<IncrExArgs<'a>, Frame> {
    let [key, options @ ..] = args else {
        return Err(wrong_arity("INCREX"));
    };
    let mut increment = IncrAmount::Integer(1);
    let mut saw_increment = false;
    let mut bounds = Bounds::default();
    let mut expiration = ExpirationAction::Keep;
    let mut saw_expiration = false;
    let mut enx = false;
    let mut index = 0usize;
    while index < options.len() {
        match options[index] {
            token if eq_ignore_ascii_case(token, b"BYINT") => {
                if saw_increment {
                    return Err(error("ERR syntax error"));
                }
                let Some(raw) = options.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                increment = IncrAmount::Integer(
                    parse_i64(raw)
                        .map_err(|_| error("ERR value is not an integer or out of range"))?,
                );
                saw_increment = true;
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"BYFLOAT") => {
                if saw_increment {
                    return Err(error("ERR syntax error"));
                }
                let Some(raw) = options.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                increment = IncrAmount::Float(
                    parse_f64(raw).map_err(|_| error("ERR value is not a float"))?,
                );
                saw_increment = true;
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"LBOUND") => {
                let Some(raw) = options.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                bounds.lower = Some(parse_f64(raw).map_err(|_| error("ERR value is not a float"))?);
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"UBOUND") => {
                let Some(raw) = options.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                bounds.upper = Some(parse_f64(raw).map_err(|_| error("ERR value is not a float"))?);
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"SATURATE") => {
                bounds.saturate = true;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"ENX") => {
                enx = true;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"PERSIST") => {
                if saw_expiration {
                    return Err(error("ERR syntax error"));
                }
                saw_expiration = true;
                expiration = ExpirationAction::Persist;
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
                let Some(raw) = options.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                expiration = ExpirationAction::Set(parse_expiration_ttl_ms(token, raw)?);
                saw_expiration = true;
                index += 2;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok(IncrExArgs {
        key,
        increment,
        bounds,
        expiration,
        enx,
    })
}

fn parse_setlike_options(args: &[&[u8]]) -> Result<(SetCondition, ExpirationAction), Frame> {
    let mut condition = SetCondition::Always;
    let mut expiration = ExpirationAction::Keep;
    let mut saw_expiration = false;
    let mut index = 0usize;
    while index < args.len() {
        match args[index] {
            token if eq_ignore_ascii_case(token, b"NX") => {
                if matches!(condition, SetCondition::Xx) {
                    return Err(error("ERR syntax error"));
                }
                condition = SetCondition::Nx;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"XX") => {
                if matches!(condition, SetCondition::Nx) {
                    return Err(error("ERR syntax error"));
                }
                condition = SetCondition::Xx;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"KEEPTTL") => {
                if saw_expiration {
                    return Err(error("ERR syntax error"));
                }
                saw_expiration = true;
                expiration = ExpirationAction::KeepTtl;
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
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                expiration = ExpirationAction::Set(parse_expiration_ttl_ms(token, raw)?);
                saw_expiration = true;
                index += 2;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok((condition, expiration))
}

fn parse_expiration_ttl_ms(option: &[u8], raw: &[u8]) -> Result<u64, Frame> {
    let value = parse_u64(raw).map_err(|_| error("ERR value is not an integer or out of range"))?;
    let ttl_ms = match option {
        option if eq_ignore_ascii_case(option, b"EX") => value.saturating_mul(1_000),
        option if eq_ignore_ascii_case(option, b"PX") => value,
        option if eq_ignore_ascii_case(option, b"EXAT") => {
            value.saturating_mul(1_000).saturating_sub(now_millis())
        }
        _ => value.saturating_sub(now_millis()),
    };
    Ok(ttl_ms)
}

fn increment_integer(current: &str, delta: i64, bounds: Bounds) -> Result<IncrOutcome, Frame> {
    let current_value = current
        .parse::<i64>()
        .map_err(|_| error("ERR value is not an integer or out of range"))?;
    let candidate = current_value
        .checked_add(delta)
        .ok_or_else(|| error("ERR increment or decrement would overflow"))?;
    let bounded = apply_bounds(candidate as f64, current_value as f64, delta as f64, bounds);
    Ok(IncrOutcome {
        new_value: (bounded.value as i64).to_string(),
        applied_delta: (bounded.applied_delta as i64).to_string(),
        applied: bounded.applied,
    })
}

fn increment_float(current: &str, delta: f64, bounds: Bounds) -> Result<IncrOutcome, Frame> {
    let current_value = current
        .parse::<f64>()
        .map_err(|_| error("ERR value is not a float"))?;
    let candidate = current_value + delta;
    if !candidate.is_finite() {
        return Err(error("ERR increment would produce NaN or Infinity"));
    }
    let bounded = apply_bounds(candidate, current_value, delta, bounds);
    Ok(IncrOutcome {
        new_value: format_number(bounded.value),
        applied_delta: format_number(bounded.applied_delta),
        applied: bounded.applied,
    })
}

struct BoundedValue {
    value: f64,
    applied_delta: f64,
    applied: bool,
}

fn apply_bounds(candidate: f64, current: f64, delta: f64, bounds: Bounds) -> BoundedValue {
    let below = bounds.lower.is_some_and(|lower| candidate < lower);
    let above = bounds.upper.is_some_and(|upper| candidate > upper);
    if !below && !above {
        return BoundedValue {
            value: candidate,
            applied_delta: delta,
            applied: true,
        };
    }
    if !bounds.saturate {
        return BoundedValue {
            value: current,
            applied_delta: 0.0,
            applied: false,
        };
    }
    let value = match (bounds.lower, bounds.upper) {
        (Some(lower), _) if below => lower,
        (_, Some(upper)) if above => upper,
        _ => candidate,
    };
    BoundedValue {
        value,
        applied_delta: value - current,
        applied: (value - current) != 0.0,
    }
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 && value.is_finite() {
        (value as i64).to_string()
    } else {
        value.to_string()
    }
}
