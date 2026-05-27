use crate::commands::formal_bounds::RedisRangeBound;

pub(crate) fn parse_score_bound(raw: &[u8]) -> std::result::Result<RedisRangeBound<f64>, ()> {
    let (inclusive, raw) = match raw.split_first() {
        Some((b'(', tail)) => (false, tail),
        _ => (true, raw),
    };
    match raw {
        raw if eq_ignore_ascii_case(raw, b"-inf") => Ok(RedisRangeBound::NegInfinity),
        raw if eq_ignore_ascii_case(raw, b"+inf") || eq_ignore_ascii_case(raw, b"inf") => {
            Ok(RedisRangeBound::PosInfinity)
        }
        raw => parse_f64(raw).map(|value| RedisRangeBound::Finite { value, inclusive }),
    }
}

pub(crate) fn parse_lex_bound(raw: &[u8]) -> std::result::Result<RedisRangeBound<&[u8]>, ()> {
    match raw {
        b"-" => Ok(RedisRangeBound::NegInfinity),
        b"+" => Ok(RedisRangeBound::PosInfinity),
        [b'[', tail @ ..] => Ok(RedisRangeBound::Finite {
            value: tail,
            inclusive: true,
        }),
        [b'(', tail @ ..] => Ok(RedisRangeBound::Finite {
            value: tail,
            inclusive: false,
        }),
        _ => Err(()),
    }
}

pub(crate) fn parse_i64(raw: &[u8]) -> std::result::Result<i64, ()> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(())
}

pub(crate) fn parse_usize(raw: &[u8]) -> std::result::Result<usize, ()> {
    let value = parse_i64(raw)?;
    usize::try_from(value).map_err(|_| ())
}

pub(crate) fn parse_u64(raw: &[u8]) -> std::result::Result<u64, ()> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(())
}

pub(crate) fn parse_f64(raw: &[u8]) -> std::result::Result<f64, ()> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .ok_or(())?;
    value.is_finite().then_some(value).ok_or(())
}

pub(super) fn format_score(score: f64) -> Vec<u8> {
    score.to_string().into_bytes()
}

pub(super) fn glob_matches(pattern: &[u8], value: &[u8]) -> bool {
    if pattern == b"*" {
        return true;
    }
    match pattern.iter().position(|byte| *byte == b'*') {
        Some(index) => {
            value.starts_with(&pattern[..index]) && value.ends_with(&pattern[index + 1..])
        }
        None => pattern == value,
    }
}

pub(crate) fn eq_ignore_ascii_case(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}
