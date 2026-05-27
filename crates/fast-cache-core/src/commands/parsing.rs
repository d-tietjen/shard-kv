use crate::{FastCacheError, Result};

use super::CommandSpec;

pub(crate) struct CommandArity<C> {
    _command: std::marker::PhantomData<C>,
}

pub(crate) struct StorageInteger<C> {
    _command: std::marker::PhantomData<C>,
}

#[cfg(feature = "server")]
pub(crate) struct AsciiU64;

pub(crate) struct TtlMillis<C> {
    _command: std::marker::PhantomData<C>,
}

impl<C> CommandArity<C>
where
    C: CommandSpec,
{
    pub(crate) fn exact(actual: usize, expected: usize) -> Result<()> {
        match actual == expected {
            true => Ok(()),
            false => Err(FastCacheError::Command(format!(
                "{} expects {expected} arguments, got {actual}",
                C::NAME
            ))),
        }
    }

    pub(crate) fn range(actual: usize, min: usize, max: usize) -> Result<()> {
        match (min..=max).contains(&actual) {
            true => Ok(()),
            false => Err(FastCacheError::Command(format!(
                "{} expects {min} to {max} arguments, got {actual}",
                C::NAME
            ))),
        }
    }

    pub(crate) fn at_least(actual: usize, min: usize, requirement: &str) -> Result<()> {
        match actual >= min {
            true => Ok(()),
            false => Err(FastCacheError::Command(format!(
                "{} requires at least {requirement}",
                C::NAME
            ))),
        }
    }
}

impl<C> StorageInteger<C>
where
    C: CommandSpec,
{
    pub(crate) fn parse_u64(option: &str, value: &[u8]) -> Result<u64> {
        let value = std::str::from_utf8(value).map_err(|error| {
            FastCacheError::Command(format!(
                "{} {option} expects utf8 integer: {error}",
                C::NAME
            ))
        })?;
        value.parse::<u64>().map_err(|error| {
            FastCacheError::Command(format!(
                "{} {option} expects positive integer: {error}",
                C::NAME
            ))
        })
    }
}

impl<C> TtlMillis<C>
where
    C: CommandSpec,
{
    pub(crate) fn seconds(value: &[u8]) -> Result<u64> {
        StorageInteger::<C>::parse_u64("seconds", value).map(|ttl| ttl.saturating_mul(1_000))
    }

    pub(crate) fn millis(value: &[u8]) -> Result<u64> {
        StorageInteger::<C>::parse_u64("milliseconds", value)
    }
}

#[cfg(feature = "server")]
impl TtlMillis<()> {
    pub(crate) fn ascii_seconds(value: &[u8]) -> Option<u64> {
        AsciiU64::parse(value).map(|ttl| ttl.saturating_mul(1_000))
    }

    pub(crate) fn ascii_millis(value: &[u8]) -> Option<u64> {
        AsciiU64::parse(value)
    }
}

#[cfg(feature = "server")]
impl AsciiU64 {
    pub(crate) fn parse(value: &[u8]) -> Option<u64> {
        match value {
            [] => None,
            value if value.len() > 19 => None,
            value => Self::parse_digits(value),
        }
    }

    fn parse_digits(value: &[u8]) -> Option<u64> {
        value
            .iter()
            .try_fold(0u64, |parsed, digit| match digit.is_ascii_digit() {
                true => parsed.checked_mul(10)?.checked_add((digit - b'0') as u64),
                false => None,
            })
    }
}
