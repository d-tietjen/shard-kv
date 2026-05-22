#[cfg(feature = "server")]
use crate::storage::EmbeddedStore;
use crate::{FastCacheError, Result};

#[cfg(feature = "server")]
use crate::commands::parsing::AsciiU64;
use crate::commands::parsing::StorageInteger;

use super::Set;
use crate::commands::CommandSpec;

/// SET options supported by the storage command parser.
#[derive(Debug, Clone, Copy)]
pub(super) struct StorageSetOptions {
    pub(super) ttl_ms: Option<u64>,
}

impl StorageSetOptions {
    pub(super) fn parse<'a>(args: impl IntoIterator<Item = &'a [u8]>) -> Result<Self> {
        let mut ttl_ms = None;
        let mut args = args.into_iter();
        while let Some(option) = args.next() {
            match SetOption::parse_storage(option)? {
                SetOption::Ex => {
                    let value = args.next().ok_or_else(|| {
                        FastCacheError::Command(format!(
                            "{} EX is missing an integer",
                            <Set as CommandSpec>::NAME
                        ))
                    })?;
                    ttl_ms =
                        Some(StorageInteger::<Set>::parse_u64("EX", value)?.saturating_mul(1_000));
                }
                SetOption::Px => {
                    let value = args.next().ok_or_else(|| {
                        FastCacheError::Command(format!(
                            "{} PX is missing an integer",
                            <Set as CommandSpec>::NAME
                        ))
                    })?;
                    ttl_ms = Some(StorageInteger::<Set>::parse_u64("PX", value)?);
                }
                SetOption::Nx | SetOption::Xx | SetOption::KeepTtl => unreachable!(),
            }
        }
        Ok(Self { ttl_ms })
    }
}

#[cfg(feature = "server")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SetCondition {
    Always,
    Nx,
    Xx,
}

#[cfg(feature = "server")]
impl SetCondition {
    pub(super) fn allows(self, exists: bool) -> bool {
        match self {
            Self::Always => true,
            Self::Nx => !exists,
            Self::Xx => exists,
        }
    }

    fn merge(self, next: Self) -> Option<Self> {
        match (self, next) {
            (Self::Always, condition) => Some(condition),
            (condition, Self::Always) => Some(condition),
            (Self::Nx, Self::Nx) => Some(Self::Nx),
            (Self::Xx, Self::Xx) => Some(Self::Xx),
            (Self::Nx, Self::Xx) | (Self::Xx, Self::Nx) => None,
        }
    }
}

#[cfg(feature = "server")]
#[derive(Debug, Clone, Copy)]
pub(super) struct SetOptions {
    ttl_ms: Option<u64>,
    pub(super) condition: SetCondition,
    keep_ttl: bool,
}

#[cfg(feature = "server")]
impl SetOptions {
    pub(super) fn parse(args: &[&[u8]]) -> Option<Self> {
        let mut options = Self {
            ttl_ms: None,
            condition: SetCondition::Always,
            keep_ttl: false,
        };
        let mut cursor = 0usize;
        while cursor < args.len() {
            let option = args[cursor];
            match SetOption::parse(option)? {
                SetOption::Ex => {
                    let value = *args.get(cursor + 1)?;
                    options.ttl_ms = Some(AsciiU64::parse(value)?.saturating_mul(1_000));
                    cursor += 2;
                }
                SetOption::Px => {
                    let value = *args.get(cursor + 1)?;
                    options.ttl_ms = Some(AsciiU64::parse(value)?);
                    cursor += 2;
                }
                SetOption::Nx => {
                    options.condition = options.condition.merge(SetCondition::Nx)?;
                    cursor += 1;
                }
                SetOption::Xx => {
                    options.condition = options.condition.merge(SetCondition::Xx)?;
                    cursor += 1;
                }
                SetOption::KeepTtl => {
                    options.keep_ttl = true;
                    cursor += 1;
                }
            }
        }
        options.finish()
    }

    fn finish(self) -> Option<Self> {
        match (self.keep_ttl, self.ttl_ms) {
            (true, Some(_)) => None,
            _ => Some(self),
        }
    }

    pub(super) fn ttl_ms(self, store: &EmbeddedStore, key: &[u8]) -> Option<u64> {
        match self.keep_ttl {
            true => match store.pttl_millis(key) {
                ttl if ttl >= 0 => Some(ttl as u64),
                _ => None,
            },
            false => self.ttl_ms,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum SetOption {
    Ex,
    Px,
    Nx,
    Xx,
    KeepTtl,
}

impl SetOption {
    pub(super) fn parse_storage(value: &[u8]) -> Result<Self> {
        match Self::parse(value) {
            Some(option @ (Self::Ex | Self::Px)) => Ok(option),
            Some(Self::Nx | Self::Xx | Self::KeepTtl) | None => {
                Err(FastCacheError::Command(format!(
                    "unsupported {} option: {}",
                    <Set as CommandSpec>::NAME,
                    String::from_utf8_lossy(value)
                )))
            }
        }
    }

    pub(super) fn parse(value: &[u8]) -> Option<Self> {
        match value {
            option if option.eq_ignore_ascii_case(b"EX") => Some(Self::Ex),
            option if option.eq_ignore_ascii_case(b"PX") => Some(Self::Px),
            option if option.eq_ignore_ascii_case(b"NX") => Some(Self::Nx),
            option if option.eq_ignore_ascii_case(b"XX") => Some(Self::Xx),
            option if option.eq_ignore_ascii_case(b"KEEPTTL") => Some(Self::KeepTtl),
            _ => None,
        }
    }
}
