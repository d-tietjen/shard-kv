use std::sync::Arc;

use fast_cache::config::EvictionPolicy;
use fast_cache::storage::SharedEmbeddedLockPolicy;

use crate::backend::{Backend, BoxError, ReadMode};

mod dashmap_bk;
mod dashmap_ref;
mod fc_embed;
mod fc_shared;
mod fcnp;
mod lru_bk;
mod moka_bk;
mod resp;
mod rwlock_hashmap;

/// All supported backend ids. Order matches the standard reporting order.
pub const BACKEND_IDS: &[&str] = &[
    "fc-embed",
    "fc-shared",
    "fc-shared-worker-stripes",
    "fc-shared-fair",
    "fc-shared-fair-worker-stripes",
    "fc-shared-ref",
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
    "fc-server-fcnp",
    "fc-server-fcnp-direct",
    "redis",
    "valkey",
    "dragonfly",
];

const SHARED_STRIPE_MULTIPLIER: usize = 4;

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

fn default_shared_stripes(vcpu_budget: usize) -> usize {
    vcpu_budget
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
) -> Result<Arc<dyn Backend>, BoxError> {
    Ok(match id {
        "fc-embed" => Arc::new(fc_embed::FcEmbed::new(
            vcpu_budget.max(worker_count).max(1).next_power_of_two(),
            cache_config,
        )) as Arc<dyn Backend>,
        "fc-shared" => fc_shared::new(
            "fc-shared",
            default_shared_stripes(vcpu_budget),
            key_count,
            true,
            cache_config,
        ),
        "fc-shared-worker-stripes" => fc_shared::new(
            "fc-shared-worker-stripes",
            worker_stripes(vcpu_budget),
            key_count,
            true,
            cache_config,
        ),
        // Backward-compatible aliases from when the recommended shared stripe
        // multiplier was encoded in the backend id. Keep output canonical.
        "fc-shared-x4" => fc_shared::new(
            "fc-shared",
            default_shared_stripes(vcpu_budget),
            key_count,
            true,
            cache_config,
        ),
        "fc-shared-fair" => fc_shared::new_with_policy(
            "fc-shared-fair",
            default_shared_stripes(vcpu_budget),
            key_count,
            true,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        ),
        "fc-shared-fair-worker-stripes" => fc_shared::new_with_policy(
            "fc-shared-fair-worker-stripes",
            worker_stripes(vcpu_budget),
            key_count,
            true,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        ),
        "fc-shared-x4-fair" => fc_shared::new_with_policy(
            "fc-shared-fair",
            default_shared_stripes(vcpu_budget),
            key_count,
            true,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        ),
        "fc-shared-ref" => fc_shared::new(
            "fc-shared-ref",
            default_shared_stripes(vcpu_budget),
            key_count,
            false,
            cache_config,
        ),
        "fc-shared-worker-stripes-ref" => fc_shared::new(
            "fc-shared-worker-stripes-ref",
            worker_stripes(vcpu_budget),
            key_count,
            false,
            cache_config,
        ),
        "fc-shared-x4-ref" => fc_shared::new(
            "fc-shared-ref",
            default_shared_stripes(vcpu_budget),
            key_count,
            false,
            cache_config,
        ),
        "fc-shared-fair-ref" => fc_shared::new_with_policy(
            "fc-shared-fair-ref",
            default_shared_stripes(vcpu_budget),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        ),
        "fc-shared-fair-worker-stripes-ref" => fc_shared::new_with_policy(
            "fc-shared-fair-worker-stripes-ref",
            worker_stripes(vcpu_budget),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        ),
        "fc-shared-x4-fair-ref" => fc_shared::new_with_policy(
            "fc-shared-fair-ref",
            default_shared_stripes(vcpu_budget),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        ),
        "fc-shared-hot-ref" => fc_shared::new_hot_shard(
            "fc-shared-hot-ref",
            default_shared_stripes(vcpu_budget),
            key_count,
            false,
            cache_config,
        ),
        "fc-shared-x4-hot-ref" => fc_shared::new_hot_shard(
            "fc-shared-hot-ref",
            default_shared_stripes(vcpu_budget),
            key_count,
            false,
            cache_config,
        ),
        "fc-shared-fair-hot-ref" => fc_shared::new_hot_shard_with_policy(
            "fc-shared-fair-hot-ref",
            default_shared_stripes(vcpu_budget),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        ),
        "fc-shared-x4-fair-hot-ref" => fc_shared::new_hot_shard_with_policy(
            "fc-shared-fair-hot-ref",
            default_shared_stripes(vcpu_budget),
            key_count,
            false,
            SharedEmbeddedLockPolicy::Fair,
            cache_config,
        ),
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
        "fc-server-fcnp" => {
            let addr = addr.ok_or_else(|| {
                format!("backend `{id}` requires --addr host:port (fast-cache-server listener)")
            })?;
            Arc::new(fcnp::FcnpBackend::new(addr)?)
        }
        "fc-server-fcnp-direct" => {
            let addr = addr.ok_or_else(|| {
                format!("backend `{id}` requires --addr host:port (fast-cache direct shard port 0)")
            })?;
            Arc::new(fcnp::FcnpBackend::new_direct_shards(
                addr,
                vcpu_budget.max(1).next_power_of_two(),
            )?)
        }
        other => return Err(format!("unknown backend id: {other}").into()),
    })
}
