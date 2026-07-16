use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::sleep;

use crate::{Result, SimEnvError, splitmix64};

pub const CHAOS_RATE_DENOMINATOR: u32 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkChaosConfig {
    pub seed: u64,
    pub latency_ms: u64,
    pub jitter_ms: u64,
    pub drop_ppm: u32,
    pub connect_crash_ppm: u32,
    pub response_crash_ppm: u32,
}

impl Default for NetworkChaosConfig {
    fn default() -> Self {
        Self {
            seed: 0x626c_6f73_736f_6d31,
            latency_ms: 0,
            jitter_ms: 0,
            drop_ppm: 0,
            connect_crash_ppm: 0,
            response_crash_ppm: 0,
        }
    }
}

impl NetworkChaosConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn is_enabled(&self) -> bool {
        self.latency_ms > 0
            || self.jitter_ms > 0
            || self.drop_ppm > 0
            || self.connect_crash_ppm > 0
            || self.response_crash_ppm > 0
    }

    pub fn validate(&self) -> Result<()> {
        for (label, value) in [
            ("drop_ppm", self.drop_ppm),
            ("connect_crash_ppm", self.connect_crash_ppm),
            ("response_crash_ppm", self.response_crash_ppm),
        ] {
            if value > CHAOS_RATE_DENOMINATOR {
                return Err(SimEnvError::InvalidRate {
                    label,
                    value,
                    max: CHAOS_RATE_DENOMINATOR,
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NetworkChaosReport {
    pub attempts: u64,
    pub successes: u64,
    pub dropped: u64,
    pub connect_crashes: u64,
    pub response_crashes: u64,
    pub injected_delay_ms: u64,
}

#[derive(Clone)]
pub struct NetworkChaos {
    inner: Arc<NetworkChaosInner>,
}

struct NetworkChaosInner {
    config: NetworkChaosConfig,
    sequence: AtomicU64,
    attempts: AtomicU64,
    successes: AtomicU64,
    dropped: AtomicU64,
    connect_crashes: AtomicU64,
    response_crashes: AtomicU64,
    injected_delay_ms: AtomicU64,
}

impl fmt::Debug for NetworkChaos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NetworkChaos")
            .field("config", &self.inner.config)
            .field("report", &self.report())
            .finish()
    }
}

impl NetworkChaos {
    pub fn new(config: NetworkChaosConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(NetworkChaosInner {
                config,
                sequence: AtomicU64::new(0),
                attempts: AtomicU64::new(0),
                successes: AtomicU64::new(0),
                dropped: AtomicU64::new(0),
                connect_crashes: AtomicU64::new(0),
                response_crashes: AtomicU64::new(0),
                injected_delay_ms: AtomicU64::new(0),
            }),
        })
    }

    pub fn config(&self) -> &NetworkChaosConfig {
        &self.inner.config
    }

    pub fn report(&self) -> NetworkChaosReport {
        NetworkChaosReport {
            attempts: self.inner.attempts.load(Ordering::Relaxed),
            successes: self.inner.successes.load(Ordering::Relaxed),
            dropped: self.inner.dropped.load(Ordering::Relaxed),
            connect_crashes: self.inner.connect_crashes.load(Ordering::Relaxed),
            response_crashes: self.inner.response_crashes.load(Ordering::Relaxed),
            injected_delay_ms: self.inner.injected_delay_ms.load(Ordering::Relaxed),
        }
    }

    pub fn sample(&self, node_index: u64) -> ChaosSample {
        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        self.inner.attempts.fetch_add(1, Ordering::Relaxed);
        self.sample_from_sequence(node_index, sequence)
    }

    pub fn sample_with_ordinal(&self, node_index: u64, ordinal: u64) -> ChaosSample {
        self.inner.attempts.fetch_add(1, Ordering::Relaxed);
        self.sample_from_sequence(node_index, ordinal)
    }

    fn sample_from_sequence(&self, node_index: u64, sequence: u64) -> ChaosSample {
        let seed = self.inner.config.seed ^ sequence.rotate_left(17) ^ node_index.rotate_left(37);
        ChaosSample {
            request_delay_ms: self.delay_ms(seed, 0x11),
            response_delay_ms: self.delay_ms(seed, 0x22),
            drop: self.sample_rate(seed, 0x33, self.inner.config.drop_ppm),
            connect_crash: self.sample_rate(seed, 0x44, self.inner.config.connect_crash_ppm),
            response_crash: self.sample_rate(seed, 0x55, self.inner.config.response_crash_ppm),
        }
    }

    pub async fn before_connect(&self, sample: &ChaosSample) -> Result<()> {
        if sample.drop {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(SimEnvError::App(
                "simulated network drop before TCP connect".to_string(),
            ));
        }
        self.sleep_ms(sample.request_delay_ms).await;
        Ok(())
    }

    pub async fn after_connect(&self, sample: &ChaosSample) -> Result<()> {
        if sample.connect_crash {
            self.inner.connect_crashes.fetch_add(1, Ordering::Relaxed);
            return Err(SimEnvError::App(
                "simulated TCP connection crash before request write".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn before_response(&self, sample: &ChaosSample) -> Result<()> {
        if sample.response_crash {
            self.inner.response_crashes.fetch_add(1, Ordering::Relaxed);
            return Err(SimEnvError::App(
                "simulated TCP connection crash before response read".to_string(),
            ));
        }
        self.sleep_ms(sample.response_delay_ms).await;
        Ok(())
    }

    pub fn record_success(&self) {
        self.inner.successes.fetch_add(1, Ordering::Relaxed);
    }

    fn delay_ms(&self, seed: u64, salt: u64) -> u64 {
        let jitter = if self.inner.config.jitter_ms == 0 {
            0
        } else {
            splitmix64(seed ^ salt) % (self.inner.config.jitter_ms + 1)
        };
        self.inner.config.latency_ms + jitter
    }

    fn sample_rate(&self, seed: u64, salt: u64, ppm: u32) -> bool {
        ppm > 0 && (splitmix64(seed ^ salt) % CHAOS_RATE_DENOMINATOR as u64) < ppm as u64
    }

    async fn sleep_ms(&self, ms: u64) {
        if ms == 0 {
            return;
        }
        self.inner
            .injected_delay_ms
            .fetch_add(ms, Ordering::Relaxed);
        sleep(Duration::from_millis(ms)).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChaosSample {
    pub request_delay_ms: u64,
    pub response_delay_ms: u64,
    pub drop: bool,
    pub connect_crash: bool,
    pub response_crash: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn network_chaos_records_drops_and_successes() {
        let chaos = NetworkChaos::new(NetworkChaosConfig {
            drop_ppm: CHAOS_RATE_DENOMINATOR,
            ..NetworkChaosConfig::default()
        })
        .unwrap();
        let sample = chaos.sample(7);
        assert!(chaos.before_connect(&sample).await.is_err());
        assert_eq!(chaos.report().attempts, 1);
        assert_eq!(chaos.report().dropped, 1);

        let chaos = NetworkChaos::new(NetworkChaosConfig::default()).unwrap();
        let sample = chaos.sample(7);
        chaos.before_connect(&sample).await.unwrap();
        chaos.after_connect(&sample).await.unwrap();
        chaos.before_response(&sample).await.unwrap();
        chaos.record_success();
        assert_eq!(chaos.report().successes, 1);
    }

    #[tokio::test]
    async fn ordinal_sampling_is_independent_of_async_call_order() {
        let config = NetworkChaosConfig {
            seed: 99,
            latency_ms: 7,
            jitter_ms: 13,
            drop_ppm: 400_000,
            connect_crash_ppm: 200_000,
            response_crash_ppm: 100_000,
        };
        let first = NetworkChaos::new(config.clone()).unwrap();
        let second = NetworkChaos::new(config).unwrap();

        let sample_a = first.sample_with_ordinal(3, 41);
        let sample_b = first.sample_with_ordinal(5, 42);
        let replay_b = second.sample_with_ordinal(5, 42);
        let replay_a = second.sample_with_ordinal(3, 41);

        assert_eq!(sample_a, replay_a);
        assert_eq!(sample_b, replay_b);
        assert_eq!(first.report().attempts, 2);
        assert_eq!(second.report().attempts, 2);
    }
}
