use std::future::Future;

use crate::{FastCacheError, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MonoioDriverMode {
    IoUring,
    Legacy,
}

pub(crate) struct MonoioRuntime;

impl MonoioRuntime {
    pub(crate) fn enabled_by_env(name: &str) -> bool {
        std::env::var(name).is_ok_and(|value| value != "0")
    }

    pub(crate) fn block_on<F, Fut, T>(label: &'static str, build: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        match MonoioDriverMode::configured() {
            MonoioDriverMode::IoUring => {
                let mut runtime = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .with_entries(MonoioDriverMode::runtime_entries())
                    .enable_timer()
                    .build()
                    .map_err(|error| {
                        FastCacheError::Config(format!(
                            "{label} monoio io_uring runtime build failed: {error}"
                        ))
                    })?;
                Ok(runtime.block_on(build()))
            }
            MonoioDriverMode::Legacy => {
                let mut runtime = monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
                    .with_entries(MonoioDriverMode::runtime_entries())
                    .enable_timer()
                    .build()
                    .map_err(|error| {
                        FastCacheError::Config(format!(
                            "{label} monoio legacy runtime build failed: {error}"
                        ))
                    })?;
                Ok(runtime.block_on(build()))
            }
        }
    }
}

impl MonoioDriverMode {
    fn configured() -> Self {
        match std::env::var("FAST_CACHE_MONOIO_DRIVER") {
            Ok(value) if value.eq_ignore_ascii_case("legacy") => Self::Legacy,
            Ok(value) if value.eq_ignore_ascii_case("iouring") => Self::IoUring,
            Ok(value) if value.eq_ignore_ascii_case("io_uring") => Self::IoUring,
            Ok(value) => {
                tracing::warn!("unknown FAST_CACHE_MONOIO_DRIVER={value}; using io_uring");
                Self::IoUring
            }
            Err(_) => Self::IoUring,
        }
    }

    fn runtime_entries() -> u32 {
        std::env::var("FAST_CACHE_MONOIO_ENTRIES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(8192)
    }
}
