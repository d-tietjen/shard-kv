use std::sync::Arc;

use shardmap::config::EvictionPolicy;
use shardmap::storage::SharedEmbeddedLockPolicy;
#[cfg(feature = "telemetry")]
use shardmap::storage::{CacheTelemetry, CacheTelemetryClock};

use crate::backend::{Backend, BoxError, ReadMode};

mod dashmap_bk;
mod dashmap_ref;
mod fc_codec;
mod fc_embed;
mod fc_shared;
mod fc_typed;
mod lru_bk;
mod memcached;
mod moka_bk;
mod resp;
mod rwlock_hashmap;
mod scnp;

/// All supported backend ids. Order matches the standard reporting order.
pub const BACKEND_IDS: &[&str] = &[
    "fc-embed",
    "fc-embed-telemetry",
    "fc-embed-telemetry-instant-full",
    "fc-embed-telemetry-shared-full",
    "fc-typed",
    "fc-typed-ref",
    "fc-codec",
    "fc-codec-ref",
    "fc-codec-ns",
    "fc-codec-ns-ref",
    "fc-codec-multi-ns",
    "fc-codec-multi-ns-ref",
    "fc-shared",
    "fc-shared-copy-locked",
    "fc-shared-copy-unlocked",
    "fc-shared-prepared",
    "fc-shared-worker-stripes",
    "fc-shared-fair",
    "fc-shared-fair-worker-stripes",
    "fc-shared-ref",
    "fc-shared-ref-telemetry",
    "fc-shared-ref-telemetry-instant-full",
    "fc-shared-ref-telemetry-shared-full",
    "fc-shared-prepared-ref",
    "fc-shared-worker-stripes-ref",
    "fc-shared-fair-ref",
    "fc-shared-fair-worker-stripes-ref",
    "fc-shared-hot-ref",
    "fc-shared-fair-hot-ref",
    "dashmap",
    "dashmap-worker-shards",
    "dashmap-ref",
    "moka",
    "moka-weighted",
    "lru",
    "rwlock-hashmap",
    "fc-server-resp",
    "fc-server-scnp",
    "fc-server-scnp-direct",
    "memcached",
    "redis",
    "valkey",
    "dragonfly",
];

const SHARED_STRIPE_MULTIPLIER: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkTelemetryMode {
    Off,
    Default,
    InstantEveryRequest,
    SharedEveryRequest,
}

#[cfg(feature = "telemetry")]
fn cache_telemetry(
    shard_count: usize,
    mode: BenchmarkTelemetryMode,
) -> Option<Arc<CacheTelemetry>> {
    match mode {
        BenchmarkTelemetryMode::Off => None,
        BenchmarkTelemetryMode::Default => Some(CacheTelemetry::new(shard_count)),
        BenchmarkTelemetryMode::InstantEveryRequest => {
            Some(CacheTelemetry::new_with_latency_sample_rate_and_clock(
                shard_count,
                1,
                CacheTelemetryClock::Instant,
            ))
        }
        BenchmarkTelemetryMode::SharedEveryRequest => {
            Some(CacheTelemetry::new_with_latency_sample_rate_and_clock(
                shard_count,
                1,
                CacheTelemetryClock::SharedMicroseconds,
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BenchmarkCacheConfig {
    pub eviction_policy: EvictionPolicy,
    pub cache_capacity_keys: Option<usize>,
    pub cache_memory_bytes: Option<usize>,
    pub read_mode: ReadMode,
}

impl Default for BenchmarkCacheConfig {
    fn default() -> Self {
        Self {
            eviction_policy: EvictionPolicy::None,
            cache_capacity_keys: None,
            cache_memory_bytes: None,
            read_mode: ReadMode::Ref,
        }
    }
}

impl BenchmarkCacheConfig {
    pub fn entry_capacity(self, default_capacity: usize) -> usize {
        self.cache_capacity_keys.unwrap_or(default_capacity).max(1)
    }

    pub fn total_memory_bytes(self) -> Option<usize> {
        self.cache_memory_bytes.filter(|limit| *limit > 0)
    }

    pub fn per_shard_memory_bytes(self, shard_count: usize) -> Option<usize> {
        let shard_count = shard_count.max(1);
        self.total_memory_bytes()
            .map(|limit| limit.div_ceil(shard_count).max(1))
    }
}

fn worker_stripes(vcpu_budget: usize) -> usize {
    vcpu_budget.max(1).next_power_of_two()
}

fn default_shared_stripes(vcpu_budget: usize, worker_count: usize) -> usize {
    vcpu_budget
        .max(worker_count)
        .saturating_mul(SHARED_STRIPE_MULTIPLIER)
        .max(1)
        .next_power_of_two()
}

/// Resolve a backend id to an instance. `vcpu_budget` controls the
/// server-side worker count for backends that have shards; for in-process
/// backends the worker count also informs local shard ownership. `addr` is
/// required for networked backends.
pub fn make(
    id: &str,
    vcpu_budget: usize,
    worker_count: usize,
    addr: Option<&str>,
    key_count: usize,
    cache_config: BenchmarkCacheConfig,
    scnp_shards: usize,
) -> Result<Arc<dyn Backend>, BoxError> {
    // The SCNP direct-shard client must fan out to exactly the server's shard
    // count so every key's owning shard has a connection. This is decoupled from
    // vcpu_budget; 0 means "fall back to vcpu_budget" for backward compatibility.
    let scnp_shard_count = if scnp_shards == 0 {
        vcpu_budget.max(1).next_power_of_two()
    } else {
        scnp_shards.next_power_of_two()
    };
    Ok(match id {
        "fc-embed" => Arc::new(fc_embed::FcEmbed::new(
            vcpu_budget.max(worker_count).max(1).next_power_of_two(),
            cache_config,
        )) as Arc<dyn Backend>,
        #[cfg(feature = "telemetry")]
        "fc-embed-telemetry" => Arc::new(fc_embed::FcEmbed::new_telemetry(
            vcpu_budget.max(worker_count).max(1).next_power_of_two(),
            cache_config,
        )) as Arc<dyn Backend>,
        #[cfg(feature = "telemetry")]
        "fc-embed-telemetry-instant-full" => Arc::new(fc_embed::FcEmbed::new_telemetry_with_mode(
            "fc-embed-telemetry-instant-full",
            vcpu_budget.max(worker_count).max(1).next_power_of_two(),
            cache_config,
            BenchmarkTelemetryMode::InstantEveryRequest,
        )) as Arc<dyn Backend>,
        #[cfg(feature = "telemetry")]
        "fc-embed-telemetry-shared-full" => Arc::new(fc_embed::FcEmbed::new_telemetry_with_mode(
            "fc-embed-telemetry-shared-full",
            vcpu_budget.max(worker_count).max(1).next_power_of_two(),
            cache_config,
            BenchmarkTelemetryMode::SharedEveryRequest,
        )) as Arc<dyn Backend>,
        #[cfg(not(feature = "telemetry"))]
        "fc-embed-telemetry"
        | "fc-embed-telemetry-instant-full"
        | "fc-embed-telemetry-shared-full" => {
            return Err(format!("backend `{id}` requires benchmark feature `telemetry`").into());
        }
        "fc-typed" => fc_typed::new(
            "fc-typed",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            fc_typed::TypedReadMode::Owned,
            cache_config,
        )?,
        "fc-typed-ref" => fc_typed::new(
            "fc-typed-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            fc_typed::TypedReadMode::Ref,
            cache_config,
        )?,
        "fc-codec" => fc_codec::new(
            "fc-codec",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            fc_codec::CodecReadMode::Owned,
            fc_codec::CodecNamespaceMode::None,
            cache_config,
        )?,
        "fc-codec-ref" => fc_codec::new(
            "fc-codec-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            fc_codec::CodecReadMode::Ref,
            fc_codec::CodecNamespaceMode::None,
            cache_config,
        )?,
        "fc-codec-ns" => fc_codec::new(
            "fc-codec-ns",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            fc_codec::CodecReadMode::Owned,
            fc_codec::CodecNamespaceMode::Single,
            cache_config,
        )?,
        "fc-codec-ns-ref" => fc_codec::new(
            "fc-codec-ns-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            fc_codec::CodecReadMode::Ref,
            fc_codec::CodecNamespaceMode::Single,
            cache_config,
        )?,
        "fc-codec-multi-ns" => fc_codec::new(
            "fc-codec-multi-ns",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            fc_codec::CodecReadMode::Owned,
            fc_codec::CodecNamespaceMode::Multi,
            cache_config,
        )?,
        "fc-codec-multi-ns-ref" => fc_codec::new(
            "fc-codec-multi-ns-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            fc_codec::CodecReadMode::Ref,
            fc_codec::CodecNamespaceMode::Multi,
            cache_config,
        )?,
        "fc-shared" => fc_shared::new(
            "fc-shared",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            true,
            cache_config,
        )?,
        "fc-shared-copy-locked" => fc_shared::new_copy_locked(
            "fc-shared-copy-locked",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            cache_config,
        )?,
        "fc-shared-copy-unlocked" => fc_shared::new_copy_unlocked(
            "fc-shared-copy-unlocked",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            cache_config,
        )?,
        "fc-shared-prepared" => fc_shared::new_prepared(
            "fc-shared-prepared",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            true,
            cache_config,
        )?,
        "fc-shared-worker-stripes" => fc_shared::new(
            "fc-shared-worker-stripes",
            worker_stripes(vcpu_budget),
            key_count,
            true,
            cache_config,
        )?,
        // Backward-compatible aliases from when the recommended shared stripe
        // multiplier was encoded in the backend id. Keep output canonical.
        "fc-shared-x4" => fc_shared::new(
            "fc-shared",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            true,
            cache_config,
        )?,
        "fc-shared-fair" => fc_shared::new_with_policy(
            "fc-shared-fair",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            true,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        )?,
        "fc-shared-fair-worker-stripes" => fc_shared::new_with_policy(
            "fc-shared-fair-worker-stripes",
            worker_stripes(vcpu_budget),
            key_count,
            true,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        )?,
        "fc-shared-x4-fair" => fc_shared::new_with_policy(
            "fc-shared-fair",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            true,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        )?,
        "fc-shared-ref" => fc_shared::new(
            "fc-shared-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            cache_config,
        )?,
        #[cfg(feature = "telemetry")]
        "fc-shared-ref-telemetry" => fc_shared::new_telemetry(
            "fc-shared-ref-telemetry",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            cache_config,
        )?,
        #[cfg(feature = "telemetry")]
        "fc-shared-ref-telemetry-instant-full" => fc_shared::new_telemetry_with_mode(
            "fc-shared-ref-telemetry-instant-full",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            cache_config,
            BenchmarkTelemetryMode::InstantEveryRequest,
        )?,
        #[cfg(feature = "telemetry")]
        "fc-shared-ref-telemetry-shared-full" => fc_shared::new_telemetry_with_mode(
            "fc-shared-ref-telemetry-shared-full",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            cache_config,
            BenchmarkTelemetryMode::SharedEveryRequest,
        )?,
        #[cfg(not(feature = "telemetry"))]
        "fc-shared-ref-telemetry"
        | "fc-shared-ref-telemetry-instant-full"
        | "fc-shared-ref-telemetry-shared-full" => {
            return Err(format!("backend `{id}` requires benchmark feature `telemetry`").into());
        }
        "fc-shared-prepared-ref" => fc_shared::new_prepared(
            "fc-shared-prepared-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            cache_config,
        )?,
        "fc-shared-worker-stripes-ref" => fc_shared::new(
            "fc-shared-worker-stripes-ref",
            worker_stripes(vcpu_budget),
            key_count,
            false,
            cache_config,
        )?,
        "fc-shared-x4-ref" => fc_shared::new(
            "fc-shared-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            cache_config,
        )?,
        "fc-shared-fair-ref" => fc_shared::new_with_policy(
            "fc-shared-fair-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        )?,
        "fc-shared-fair-worker-stripes-ref" => fc_shared::new_with_policy(
            "fc-shared-fair-worker-stripes-ref",
            worker_stripes(vcpu_budget),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        )?,
        "fc-shared-x4-fair-ref" => fc_shared::new_with_policy(
            "fc-shared-fair-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        )?,
        "fc-shared-hot-ref" => fc_shared::new_hot_shard(
            "fc-shared-hot-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            cache_config,
        )?,
        "fc-shared-x4-hot-ref" => fc_shared::new_hot_shard(
            "fc-shared-hot-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            cache_config,
        )?,
        "fc-shared-fair-hot-ref" => fc_shared::new_hot_shard_with_policy(
            "fc-shared-fair-hot-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        )?,
        "fc-shared-x4-fair-hot-ref" => fc_shared::new_hot_shard_with_policy(
            "fc-shared-fair-hot-ref",
            default_shared_stripes(vcpu_budget, worker_count),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        )?,
        "dashmap" => Arc::new(dashmap_bk::DashMapBk::new(key_count)),
        "dashmap-worker-shards" => Arc::new(dashmap_bk::DashMapBk::with_shard_amount(
            key_count,
            worker_count.max(2).next_power_of_two(),
        )),
        "dashmap-ref" => Arc::new(dashmap_ref::DashMapRef::new(key_count)),
        "moka" => Arc::new(moka_bk::MokaBk::new(cache_config.entry_capacity(key_count))),
        "moka-weighted" => Arc::new(moka_bk::MokaBk::weighted_bytes(
            cache_config
                .total_memory_bytes()
                .unwrap_or_else(|| cache_config.entry_capacity(key_count)),
        )),
        "lru" => Arc::new(lru_bk::LruBk::new(cache_config.entry_capacity(key_count))),
        "rwlock-hashmap" => Arc::new(rwlock_hashmap::RwLockHashMapBk::new(key_count)),
        "fc-server-resp" | "redis" | "valkey" | "dragonfly" => {
            let addr = addr.ok_or_else(|| {
                format!("backend `{id}` requires --addr host:port (Docker compose recommended)")
            })?;
            Arc::new(resp::RespBackend::new(id, addr)?)
        }
        "memcached" => {
            let addr = addr.ok_or_else(|| {
                format!("backend `{id}` requires --addr host:port (Docker compose recommended)")
            })?;
            Arc::new(memcached::MemcachedBackend::new(addr)?)
        }
        "fc-server-scnp" => {
            let addr = addr.ok_or_else(|| {
                format!("backend `{id}` requires --addr host:port (shardcache listener)")
            })?;
            Arc::new(scnp::ScnpBackend::new(addr)?)
        }
        "fc-server-scnp-direct" => {
            let addr = addr.ok_or_else(|| {
                format!("backend `{id}` requires --addr host:port (shardcache direct shard port 0)")
            })?;
            Arc::new(scnp::ScnpBackend::new_direct_shards(
                addr,
                scnp_shard_count,
            )?)
        }
        other => return Err(format!("unknown backend id: {other}").into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_embedded_shard_counts_are_reported_as_config_errors() {
        for backend_id in ["fc-typed", "fc-codec", "fc-shared"] {
            let error = match make(
                backend_id,
                128,
                128,
                None,
                1024,
                BenchmarkCacheConfig::default(),
                0,
            ) {
                Ok(_) => panic!("{backend_id} unexpectedly accepted an oversized shard count"),
                Err(error) => error,
            };
            let message = error.to_string();

            assert!(
                message.contains("supports up to 256 shards"),
                "{backend_id} error did not explain the shard limit: {message}"
            );
            assert!(
                message.contains("resolved to 512"),
                "{backend_id} error did not include the resolved shard count: {message}"
            );
        }
    }
}
