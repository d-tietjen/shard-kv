#[cfg(feature = "object-overflow")]
use super::ObjectOverflowBackend;
use super::{
    EvictionPolicy, KvOverflowConfig, ObjectOverflowConfig, PersistenceConfig, ReplicationConfig,
    ReplicationRole, ServerEndpointMode, ShardCacheConfig, WalTcpExportConfig, WalTcpExportMode,
};
#[cfg(feature = "kv-overflow")]
use super::{KvOverflowBackend, KvOverflowCompression, MAX_KV_OVERFLOW_SLOT_COUNT};
use crate::{Result, ShardCacheError};

#[cfg(feature = "kv-overflow")]
fn is_loopback_endpoint(endpoint: &str) -> bool {
    endpoint
        .parse::<std::net::SocketAddr>()
        .is_ok_and(|address| address.ip().is_loopback())
        || endpoint
            .strip_prefix("localhost:")
            .is_some_and(|port| port.parse::<u16>().is_ok())
}

#[cfg(feature = "prefix-eviction")]
fn memory_limit_eviction_policy_message() -> &'static str {
    "max_memory_bytes requires eviction_policy to be set to lru, lfu, or prefix"
}

#[cfg(not(feature = "prefix-eviction"))]
fn memory_limit_eviction_policy_message() -> &'static str {
    "max_memory_bytes requires eviction_policy to be set to lru or lfu"
}

pub(super) struct ConfigValidator<'a> {
    config: &'a ShardCacheConfig,
}

enum ConfigValidationRule {
    ShardCount,
    MaxConnections,
    MemoryLimit,
    TierCapacities,
    Persistence,
    ObjectOverflow,
    KvOverflow,
    KvOverflowReplica,
    Replication,
    Cuda,
}

struct PersistenceValidation<'a> {
    config: &'a PersistenceConfig,
}

struct WalTcpExportValidation<'a> {
    config: &'a WalTcpExportConfig,
}

struct ObjectOverflowValidation<'a> {
    config: &'a ObjectOverflowConfig,
    #[cfg(feature = "object-overflow")]
    root: &'a ShardCacheConfig,
}

struct ReplicationValidation<'a> {
    config: &'a ReplicationConfig,
}

struct KvOverflowValidation<'a> {
    config: &'a KvOverflowConfig,
    #[cfg(feature = "kv-overflow")]
    root: &'a ShardCacheConfig,
}

struct ConfigCheck;

impl<'a> ConfigValidator<'a> {
    pub(super) fn new(config: &'a ShardCacheConfig) -> Self {
        Self { config }
    }

    pub(super) fn validate(&self) -> Result<()> {
        for rule in ConfigValidationRule::all() {
            rule.validate(self.config)?;
        }
        Ok(())
    }
}

impl ConfigValidationRule {
    fn all() -> &'static [Self] {
        &[
            Self::ShardCount,
            Self::MaxConnections,
            Self::MemoryLimit,
            Self::TierCapacities,
            Self::Persistence,
            Self::ObjectOverflow,
            Self::KvOverflow,
            Self::KvOverflowReplica,
            Self::Replication,
            Self::Cuda,
        ]
    }

    fn validate(&self, config: &ShardCacheConfig) -> Result<()> {
        match self {
            Self::ShardCount => ConfigCheck::require(
                config.shard_count > 0 && config.shard_count.is_power_of_two(),
                format!(
                    "shard_count must be a non-zero power of two; got {}",
                    config.shard_count
                ),
            ),
            Self::MaxConnections => {
                ConfigCheck::require(config.max_connections > 0, "max_connections must be > 0")
            }
            Self::MemoryLimit => Self::validate_memory_limit(config),
            Self::TierCapacities => ConfigCheck::require(
                [
                    config.tiers.hot_capacity,
                    config.tiers.warm_capacity,
                    config.tiers.cold_capacity,
                ]
                .into_iter()
                .all(|capacity| capacity > 0),
                "tier capacities must be > 0",
            ),
            Self::Persistence => PersistenceValidation::new(&config.persistence).validate(),
            Self::ObjectOverflow => {
                ObjectOverflowValidation::new(&config.object_overflow, config).validate()
            }
            Self::KvOverflow => KvOverflowValidation::new(&config.kv_overflow, config).validate(),
            Self::KvOverflowReplica => {
                if !config.kv_overflow_replica.enabled {
                    return Ok(());
                }
                ConfigCheck::require(
                    !config.kv_overflow_replica.node_id.trim().is_empty(),
                    "kv_overflow_replica.node_id must not be empty",
                )?;
                ConfigCheck::require(
                    matches!(config.server_endpoint_mode, ServerEndpointMode::DirectShard),
                    "kv_overflow_replica requires server_endpoint_mode = \"direct_shard\"",
                )?;
                ConfigCheck::require(
                    !config.kv_overflow.enabled,
                    "a server cannot be both a kv overflow primary and replica",
                )?;
                ConfigCheck::require(
                    !config.persistence.enabled || config.kv_overflow_replica.encrypted_persistence,
                    "durable kv overflow replicas require encrypted_persistence = true after provisioning an encrypted filesystem or volume",
                )?;
                ConfigCheck::optional_token(
                    config.kv_overflow_replica.auth_token_env.as_deref(),
                    "kv_overflow_replica.auth_token_env must not be empty",
                )?;
                ConfigCheck::require(
                    config.kv_overflow_replica.auth_token_env.is_none()
                        || config.kv_overflow_replica.auth_token_path.is_none(),
                    "configure only one of kv_overflow_replica.auth_token_env or auth_token_path",
                )?;
                let tls = &config.kv_overflow_replica.tls;
                if tls.enabled {
                    #[cfg(not(feature = "scnp-tls"))]
                    {
                        return Err(ShardCacheError::Config(
                            "kv_overflow_replica.tls requires the scnp-tls feature".into(),
                        ));
                    }
                    #[cfg(feature = "scnp-tls")]
                    {
                        ConfigCheck::require(
                            !tls.cert_path.as_os_str().is_empty()
                                && !tls.key_path.as_os_str().is_empty(),
                            "kv_overflow_replica.tls cert_path and key_path are required",
                        )?;
                        ConfigCheck::require(
                            tls.handshake_timeout_ms > 0
                                && tls.reload_interval_ms > 0
                                && tls.max_concurrent_handshakes > 0,
                            "kv_overflow_replica TLS handshake and reload intervals must be > 0",
                        )?;
                        ConfigCheck::require(
                            tls.max_concurrent_handshakes < config.max_connections,
                            "kv_overflow_replica TLS max_concurrent_handshakes must be lower than max_connections",
                        )?;
                        ConfigCheck::require(
                            tls.client_cert_sha256.is_empty() || tls.client_ca_path.is_some(),
                            "kv_overflow_replica TLS client fingerprints require client_ca_path",
                        )?;
                        ConfigCheck::require(
                            tls.client_ca_path.is_none() || !tls.client_cert_sha256.is_empty(),
                            "kv_overflow_replica mTLS requires at least one authorized client certificate fingerprint",
                        )?;
                        ConfigCheck::require(
                            tls.client_cert_sha256.iter().all(|fingerprint| {
                                let compact = fingerprint.replace(':', "");
                                compact.len() == 64
                                    && compact.bytes().all(|byte| byte.is_ascii_hexdigit())
                            }),
                            "kv_overflow_replica TLS client fingerprints must be SHA-256 hexadecimal values",
                        )?;
                    }
                }
                let loopback = config
                    .bind_addr
                    .parse::<std::net::SocketAddr>()
                    .is_ok_and(|address| address.ip().is_loopback());
                ConfigCheck::require(
                    loopback || tls.enabled || config.kv_overflow_replica.allow_insecure_scnp,
                    "non-loopback kv overflow replicas require TLS or allow_insecure_scnp = true",
                )?;
                ConfigCheck::require(
                    loopback
                        || config.kv_overflow_replica.auth_token_env.is_some()
                        || tls.client_ca_path.is_some()
                        || config.kv_overflow_replica.allow_insecure_scnp,
                    "non-loopback kv overflow replicas require token authentication, mTLS, or allow_insecure_scnp = true",
                )?;
                ConfigCheck::require(
                    config.max_memory_bytes == 0
                        || matches!(config.eviction_policy, EvictionPolicy::None)
                        || config.object_overflow.enabled
                        || config.kv_overflow_replica.allow_lossy_eviction,
                    "kv overflow replica eviction requires object_overflow or allow_lossy_eviction = true",
                )
            }
            Self::Replication => ReplicationValidation::new(&config.replication).validate(),
            Self::Cuda => Self::validate_cuda(config),
        }
    }

    fn validate_memory_limit(config: &ShardCacheConfig) -> Result<()> {
        ConfigCheck::require(
            usize::try_from(config.max_memory_bytes).is_ok(),
            "max_memory_bytes exceeds platform addressable size",
        )?;

        match config.max_memory_bytes {
            0 => Ok(()),
            _ => ConfigCheck::require(
                config.eviction_policy != EvictionPolicy::None,
                memory_limit_eviction_policy_message(),
            ),
        }
    }

    fn validate_cuda(config: &ShardCacheConfig) -> Result<()> {
        match config.cuda.enabled {
            false => Ok(()),
            true => {
                ConfigCheck::require(
                    config.cuda.hot_tier_bytes > 0,
                    "cuda.hot_tier_bytes must be > 0 when cuda is enabled",
                )?;
                ConfigCheck::require(
                    config.cuda.transfer_stream_count > 0,
                    "cuda.transfer_stream_count must be > 0 when cuda is enabled",
                )?;
                ConfigCheck::require(
                    config.cuda.pinned_host_bytes > 0 || config.cuda.prefer_direct_host_dma,
                    "cuda.pinned_host_bytes must be > 0 when direct host dma is disabled",
                )
            }
        }
    }
}

impl<'a> KvOverflowValidation<'a> {
    fn new(config: &'a KvOverflowConfig, root: &'a ShardCacheConfig) -> Self {
        #[cfg(not(feature = "kv-overflow"))]
        let _ = root;
        Self {
            config,
            #[cfg(feature = "kv-overflow")]
            root,
        }
    }

    fn validate(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        #[cfg(not(feature = "kv-overflow"))]
        return Err(ShardCacheError::Config(
            "kv_overflow.enabled requires the kv-overflow feature".into(),
        ));
        #[cfg(feature = "kv-overflow")]
        {
            ConfigCheck::require(
                !self.root.object_overflow.enabled,
                "kv_overflow and object_overflow cannot be enabled together",
            )?;
            ConfigCheck::require(
                !self.config.replicas.is_empty() || !self.config.endpoints.is_empty(),
                "kv_overflow.replicas must contain at least one server",
            )?;
            ConfigCheck::require(
                self.config.replicas.is_empty() || self.config.endpoints.is_empty(),
                "configure kv_overflow.replicas or legacy endpoints, not both",
            )?;
            ConfigCheck::require(
                self.config.previous_replicas.is_empty()
                    || self.config.previous_endpoints.is_empty(),
                "configure previous_replicas or legacy previous_endpoints, not both",
            )?;
            ConfigCheck::require(
                !self.config.cluster_id.is_empty()
                    && self.config.cluster_id.len() <= u16::MAX as usize,
                "kv_overflow.cluster_id must contain 1..=65535 bytes",
            )?;
            let mut replica_ids = self
                .config
                .replicas
                .iter()
                .map(|replica| replica.id.as_str())
                .collect::<Vec<_>>();
            replica_ids.sort_unstable();
            ConfigCheck::require(
                replica_ids.iter().all(|id| !id.trim().is_empty())
                    && replica_ids.windows(2).all(|pair| pair[0] != pair[1]),
                "kv_overflow replica IDs must be non-empty and unique",
            )?;
            let mut previous_replica_ids = self
                .config
                .previous_replicas
                .iter()
                .map(|replica| replica.id.as_str())
                .collect::<Vec<_>>();
            previous_replica_ids.sort_unstable();
            ConfigCheck::require(
                previous_replica_ids
                    .windows(2)
                    .all(|pair| pair[0] != pair[1]),
                "kv_overflow previous replica IDs must be unique",
            )?;
            for replica in self
                .config
                .replicas
                .iter()
                .chain(&self.config.previous_replicas)
            {
                ConfigCheck::require(
                    !replica.id.trim().is_empty()
                        && !replica.addresses.is_empty()
                        && replica
                            .addresses
                            .iter()
                            .all(|address| !address.trim().is_empty()),
                    "kv_overflow replicas require an ID and at least one address",
                )?;
                let mut addresses = replica.addresses.clone();
                addresses.sort();
                addresses.dedup();
                ConfigCheck::require(
                    addresses.len() == replica.addresses.len(),
                    "kv_overflow replica addresses must be unique per replica",
                )?;
                if self.config.backend == KvOverflowBackend::Scnp {
                    ConfigCheck::require(
                        replica.shard_count > 0 && replica.shard_count.is_power_of_two(),
                        "SCNP overflow replica shard_count must be a non-zero power of two",
                    )?;
                    ConfigCheck::require(
                        replica.direct_shard_base_port == 0
                            || replica
                                .direct_shard_base_port
                                .checked_add(
                                    u16::try_from(replica.shard_count.saturating_sub(1))
                                        .unwrap_or(u16::MAX),
                                )
                                .is_some(),
                        "SCNP overflow replica direct shard port range overflows u16",
                    )?;
                }
            }
            ConfigCheck::require(
                self.config
                    .endpoints
                    .iter()
                    .all(|endpoint| !endpoint.trim().is_empty()),
                "kv_overflow.endpoints must not contain empty addresses",
            )?;
            let mut endpoints = self.config.endpoints.clone();
            endpoints.sort();
            endpoints.dedup();
            ConfigCheck::require(
                endpoints.len() == self.config.endpoints.len(),
                "kv_overflow.endpoints must be unique",
            )?;
            ConfigCheck::require(
                self.config
                    .previous_endpoints
                    .iter()
                    .all(|endpoint| !endpoint.trim().is_empty()),
                "kv_overflow.previous_endpoints must not contain empty addresses",
            )?;
            let mut previous_endpoints = self.config.previous_endpoints.clone();
            previous_endpoints.sort();
            previous_endpoints.dedup();
            ConfigCheck::require(
                previous_endpoints.len() == self.config.previous_endpoints.len(),
                "kv_overflow.previous_endpoints must be unique",
            )?;
            ConfigCheck::require(
                self.config.slot_count > 0 && self.config.slot_count.is_power_of_two(),
                "kv_overflow.slot_count must be a non-zero power of two",
            )?;
            ConfigCheck::require(
                self.root.shard_count <= self.config.slot_count as usize,
                "kv_overflow.slot_count must be at least the current primary shard count",
            )?;
            ConfigCheck::require(
                self.config.slot_count <= MAX_KV_OVERFLOW_SLOT_COUNT,
                "kv_overflow.slot_count exceeds the supported maximum",
            )?;
            if let Some(previous_primary_shard_count) = self.config.previous_primary_shard_count {
                ConfigCheck::require(
                    !self.config.previous_replicas.is_empty()
                        || !self.config.previous_endpoints.is_empty(),
                    "kv_overflow.previous_primary_shard_count requires a previous membership",
                )?;
                ConfigCheck::require(
                    previous_primary_shard_count > 0
                        && previous_primary_shard_count.is_power_of_two()
                        && previous_primary_shard_count <= self.config.slot_count as usize,
                    "kv_overflow.previous_primary_shard_count must be a non-zero power of two no greater than slot_count",
                )?;
            }
            if self.config.backend == KvOverflowBackend::Scnp && !self.config.replicas.is_empty() {
                let current_targets = self
                    .config
                    .replicas
                    .iter()
                    .try_fold(0usize, |total, replica| {
                        total.checked_add(replica.shard_count)
                    })
                    .ok_or_else(|| {
                        ShardCacheError::Config(
                            "kv_overflow current remote shard count exceeds platform limits".into(),
                        )
                    })?;
                ConfigCheck::require(
                    current_targets >= self.root.shard_count,
                    "kv_overflow requires at least one current SCNP remote shard per primary shard",
                )?;
                if !self.config.previous_replicas.is_empty() {
                    let previous_targets = self
                        .config
                        .previous_replicas
                        .iter()
                        .try_fold(0usize, |total, replica| {
                            total.checked_add(replica.shard_count)
                        })
                        .ok_or_else(|| {
                            ShardCacheError::Config(
                                "kv_overflow previous remote shard count exceeds platform limits"
                                    .into(),
                            )
                        })?;
                    ConfigCheck::require(
                        previous_targets
                            >= self
                                .config
                                .previous_primary_shard_count
                                .unwrap_or(self.root.shard_count),
                        "kv_overflow requires at least one previous SCNP remote shard per previous primary shard",
                    )?;
                }
            }
            match self.config.backend {
                KvOverflowBackend::Scnp => {
                    ConfigCheck::require(
                        self.config.redis_username_env.is_none()
                            && self.config.redis_password_env.is_none(),
                        "kv_overflow Redis credential env vars require backend = \"redis\"",
                    )?;
                    ConfigCheck::optional_token(
                        self.config.scnp_auth_token_env.as_deref(),
                        "kv_overflow.scnp_auth_token_env must not be empty",
                    )?;
                    ConfigCheck::require(
                        self.config.scnp_auth_token_env.is_none()
                            || self.config.scnp_auth_token_path.is_none(),
                        "configure only one of kv_overflow.scnp_auth_token_env or scnp_auth_token_path",
                    )?;
                    let tls = &self.config.scnp_tls;
                    if tls.enabled {
                        #[cfg(not(feature = "scnp-tls"))]
                        {
                            return Err(ShardCacheError::Config(
                                "kv_overflow.scnp_tls requires the scnp-tls feature".into(),
                            ));
                        }
                        #[cfg(feature = "scnp-tls")]
                        {
                            ConfigCheck::require(
                                !tls.ca_path.as_os_str().is_empty(),
                                "kv_overflow.scnp_tls.ca_path is required",
                            )?;
                            ConfigCheck::require(
                                tls.client_cert_path.is_some() == tls.client_key_path.is_some(),
                                "kv_overflow.scnp_tls client certificate and key must be configured together",
                            )?;
                            if self.config.replicas.is_empty() {
                                ConfigCheck::require(
                                    tls.server_name
                                        .as_deref()
                                        .is_some_and(|name| !name.trim().is_empty()),
                                    "legacy SCNP endpoints require kv_overflow.scnp_tls.server_name",
                                )?;
                            }
                            ConfigCheck::require(
                                self.config
                                    .replicas
                                    .iter()
                                    .chain(&self.config.previous_replicas)
                                    .all(|replica| {
                                        replica
                                            .tls_server_name
                                            .as_deref()
                                            .is_none_or(|name| !name.trim().is_empty())
                                    }),
                                "kv_overflow replica tls_server_name must not be empty",
                            )?;
                        }
                    }
                    let all_loopback = self
                        .config
                        .replicas
                        .iter()
                        .chain(&self.config.previous_replicas)
                        .flat_map(|replica| replica.addresses.iter())
                        .chain(self.config.endpoints.iter())
                        .chain(self.config.previous_endpoints.iter())
                        .all(|endpoint| is_loopback_endpoint(endpoint));
                    ConfigCheck::require(
                        all_loopback || tls.enabled || self.config.allow_insecure_scnp,
                        "non-loopback SCNP overflow requires TLS or allow_insecure_scnp = true",
                    )?;
                    ConfigCheck::require(
                        all_loopback
                            || self.config.scnp_auth_token_env.is_some()
                            || self.config.scnp_auth_token_path.is_some()
                            || tls.client_cert_path.is_some()
                            || self.config.allow_insecure_scnp,
                        "non-loopback SCNP overflow requires token authentication, mTLS, or allow_insecure_scnp = true",
                    )?;
                }
                KvOverflowBackend::Redis => {
                    ConfigCheck::require(
                        self.config.scnp_auth_token_env.is_none()
                            && self.config.scnp_auth_token_path.is_none(),
                        "kv_overflow SCNP authentication requires backend = \"scnp\"",
                    )?;
                    #[cfg(not(feature = "kv-overflow-redis"))]
                    return Err(ShardCacheError::Config(
                        "kv_overflow.backend = \"redis\" requires the kv-overflow-redis feature"
                            .into(),
                    ));
                    #[cfg(feature = "kv-overflow-redis")]
                    self.validate_redis_backend()?;
                }
            }
            ConfigCheck::require(
                self.config.max_memory_bytes > 0,
                "kv_overflow.max_memory_bytes must be > 0",
            )?;
            ConfigCheck::require(
                self.config.max_key_bytes > 0,
                "kv_overflow.max_key_bytes must be > 0",
            )?;
            ConfigCheck::require(
                self.config.max_metadata_bytes == 0 || self.config.max_metadata_bytes > 128,
                "kv_overflow.max_metadata_bytes must be zero or greater than per-key overhead",
            )?;
            ConfigCheck::require(
                matches!(
                    self.config.eviction_policy,
                    EvictionPolicy::Lru | EvictionPolicy::Lfu
                ),
                "kv_overflow.eviction_policy must be lru or lfu",
            )?;
            ConfigCheck::require(
                self.config.connections_per_endpoint > 0,
                "kv_overflow.connections_per_endpoint must be > 0",
            )?;
            ConfigCheck::require(
                self.config.queue_capacity_per_shard > 0
                    && self.config.pipeline_max_items > 0
                    && self.config.pipeline_max_bytes > 0
                    && self.config.max_inflight_per_target > 0,
                "kv_overflow per-shard queue and pipeline limits must be > 0",
            )?;
            ConfigCheck::require(
                self.config.pipeline_target_micros > 0
                    && self.config.circuit_breaker_failure_threshold > 0
                    && self.config.circuit_breaker_cooldown_ms > 0,
                "kv_overflow adaptive pipeline and circuit-breaker limits must be > 0",
            )?;
            ConfigCheck::require(
                self.config.compression_min_value_bytes > 0
                    && self.config.compression_min_savings_percent <= 100
                    && self.config.max_value_bytes > 0
                    && self.config.compression_max_expansion_ratio > 0,
                "kv_overflow compression and decoded-value limits must be > 0 and savings percent must be <= 100",
            )?;
            ConfigCheck::require(
                !matches!(self.config.compression, KvOverflowCompression::Lz4)
                    || self.config.allow_envelope_v2_writes,
                "kv_overflow compression requires allow_envelope_v2_writes = true after all readers support v2",
            )?;
            ConfigCheck::require(
                self.config.handoff_max_concurrency > 0 && self.config.retained_buffer_bytes > 0,
                "kv_overflow handoff concurrency and retained buffer limits must be > 0",
            )?;
            ConfigCheck::require(
                self.config.connect_timeout_ms > 0
                    && self.config.operation_timeout_ms > 0
                    && self.config.retry_backoff_ms > 0
                    && self.config.cleanup_interval_ms > 0,
                "kv_overflow timeouts, retry_backoff_ms, and cleanup_interval_ms must be > 0",
            )
        }
    }

    #[cfg(all(feature = "kv-overflow", feature = "kv-overflow-redis"))]
    fn validate_redis_backend(&self) -> Result<()> {
        use redis_client::IntoConnectionInfo;

        ConfigCheck::require(
            !self.config.redis_key_prefix.is_empty(),
            "kv_overflow.redis_key_prefix must not be empty for the Redis backend",
        )?;
        ConfigCheck::optional_token(
            self.config.redis_username_env.as_deref(),
            "kv_overflow.redis_username_env must not be empty",
        )?;
        ConfigCheck::optional_token(
            self.config.redis_password_env.as_deref(),
            "kv_overflow.redis_password_env must not be empty",
        )?;
        ConfigCheck::require(
            self.config.redis_username_env.is_none() || self.config.redis_password_env.is_some(),
            "kv_overflow.redis_password_env is required with redis_username_env",
        )?;
        let replica_addresses = self
            .config
            .replicas
            .iter()
            .chain(&self.config.previous_replicas)
            .flat_map(|replica| &replica.addresses);
        for endpoint in self
            .config
            .endpoints
            .iter()
            .chain(&self.config.previous_endpoints)
            .chain(replica_addresses)
        {
            let connection = endpoint.as_str().into_connection_info().map_err(|error| {
                ShardCacheError::Config(format!("invalid Redis overflow endpoint: {error}"))
            })?;
            ConfigCheck::require(
                connection.redis.username.is_none() && connection.redis.password.is_none(),
                "Redis overflow credentials must use configured environment variables, not endpoint URLs",
            )?;
        }
        Ok(())
    }
}

impl<'a> ObjectOverflowValidation<'a> {
    fn new(config: &'a ObjectOverflowConfig, root: &'a ShardCacheConfig) -> Self {
        #[cfg(not(feature = "object-overflow"))]
        let _ = root;
        Self {
            config,
            #[cfg(feature = "object-overflow")]
            root,
        }
    }

    fn validate(&self) -> Result<()> {
        match self.config.enabled {
            false => Ok(()),
            true => {
                #[cfg(not(feature = "object-overflow"))]
                {
                    Err(ShardCacheError::Config(
                        "object_overflow.enabled requires the object-overflow feature".into(),
                    ))
                }
                #[cfg(feature = "object-overflow")]
                {
                    ConfigCheck::require(
                        self.root.max_memory_bytes > 0
                            && self.root.eviction_policy != EvictionPolicy::None,
                        "object_overflow.enabled requires max_memory_bytes and an eviction policy",
                    )?;
                    ConfigCheck::require(
                        !self.config.endpoint.trim().is_empty(),
                        "object_overflow.endpoint must be set when object overflow is enabled",
                    )?;
                    ConfigCheck::require(
                        !self.config.bucket.trim().is_empty(),
                        "object_overflow.bucket must be set when object overflow is enabled",
                    )?;
                    ConfigCheck::require(
                        self.config.min_value_bytes > 0,
                        "object_overflow.min_value_bytes must be > 0",
                    )?;
                    ConfigCheck::require(
                        self.config.offload_max_frequency > 0,
                        "object_overflow.offload_max_frequency must be > 0",
                    )?;
                    ConfigCheck::require(
                        self.config.zstd_level >= -7 && self.config.zstd_level <= 22,
                        "object_overflow.zstd_level must be between -7 and 22",
                    )?;
                    ConfigCheck::require(
                        self.config.retry_backoff_ms > 0,
                        "object_overflow.retry_backoff_ms must be > 0",
                    )?;
                    ConfigCheck::require(
                        self.config.operation_timeout_ms > 0,
                        "object_overflow.operation_timeout_ms must be > 0",
                    )?;
                    ConfigCheck::require(
                        self.config.worker_threads > 0,
                        "object_overflow.worker_threads must be > 0",
                    )?;
                    ConfigCheck::require(
                        self.config.queue_capacity > 0,
                        "object_overflow.queue_capacity must be > 0",
                    )?;
                    ConfigCheck::require(
                        self.config.degraded_failure_threshold > 0,
                        "object_overflow.degraded_failure_threshold must be > 0",
                    )?;
                    ConfigCheck::require(
                        self.config.degraded_cooldown_ms > 0,
                        "object_overflow.degraded_cooldown_ms must be > 0",
                    )?;
                    ConfigCheck::require(
                        self.config.cleanup_grace_seconds > 0,
                        "object_overflow.cleanup_grace_seconds must be > 0",
                    )?;
                    if self.config.cleanup_on_start || self.config.cleanup_interval_seconds > 0 {
                        ConfigCheck::optional_token(
                            self.config.node_id.as_deref(),
                            "object_overflow.node_id is required when object overflow cleanup is enabled",
                        )?;
                    }
                    ConfigCheck::optional_token(
                        self.config.node_id.as_deref(),
                        "object_overflow.node_id must not be empty",
                    )?;
                    ConfigCheck::optional_token(
                        Some(self.config.region.as_str()),
                        "object_overflow.region must not be empty",
                    )?;
                    ConfigCheck::optional_token(
                        self.config.server_side_encryption.as_deref(),
                        "object_overflow.server_side_encryption must not be empty",
                    )?;
                    match self.config.backend {
                        ObjectOverflowBackend::File => {}
                        ObjectOverflowBackend::S3 => {
                            #[cfg(not(feature = "object-overflow-s3"))]
                            return Err(ShardCacheError::Config(
                                "object_overflow.backend = \"s3\" requires the object-overflow-s3 feature".into(),
                            ));
                        }
                    }
                    ConfigCheck::optional_token(
                        self.config.access_key_env.as_deref(),
                        "object_overflow.access_key_env must not be empty",
                    )?;
                    ConfigCheck::optional_token(
                        self.config.secret_key_env.as_deref(),
                        "object_overflow.secret_key_env must not be empty",
                    )
                }
            }
        }
    }
}

impl<'a> PersistenceValidation<'a> {
    fn new(config: &'a PersistenceConfig) -> Self {
        Self { config }
    }

    fn validate(&self) -> Result<()> {
        match self.config.enabled {
            false => Ok(()),
            true => {
                ConfigCheck::require(
                    self.config.segment_size_bytes >= 4 * 1024,
                    "persistence.segment_size_bytes must be at least 4096",
                )?;
                ConfigCheck::require(
                    self.config.fsync_interval_ms > 0,
                    "persistence.fsync_interval_ms must be > 0",
                )?;
                ConfigCheck::require(
                    self.config.wal_channel_capacity > 0,
                    "persistence.wal_channel_capacity must be > 0",
                )?;
                ConfigCheck::require(
                    self.config.wal_block_max_records > 0,
                    "persistence.wal_block_max_records must be > 0",
                )?;
                ConfigCheck::require(
                    self.config.wal_block_max_bytes > 0,
                    "persistence.wal_block_max_bytes must be > 0",
                )?;
                WalTcpExportValidation::new(&self.config.tcp_export).validate()
            }
        }
    }
}

impl<'a> WalTcpExportValidation<'a> {
    fn new(config: &'a WalTcpExportConfig) -> Self {
        Self { config }
    }

    fn validate(&self) -> Result<()> {
        match self.config.enabled {
            false => Ok(()),
            true => {
                ConfigCheck::require(
                    !self.config.addr.trim().is_empty(),
                    "persistence.tcp_export.addr must be set when TCP WAL export is enabled",
                )?;
                ConfigCheck::require(
                    self.config.channel_capacity > 0,
                    "persistence.tcp_export.channel_capacity must be > 0",
                )?;
                ConfigCheck::require(
                    self.config.max_subscribers > 0,
                    "persistence.tcp_export.max_subscribers must be > 0",
                )?;
                ConfigCheck::optional_token(
                    self.config.auth_token.as_deref(),
                    "persistence.tcp_export.auth_token must not be empty",
                )?;
                self.validate_mode()?;
                self.validate_timeouts()
            }
        }
    }

    fn validate_mode(&self) -> Result<()> {
        match self.config.mode {
            WalTcpExportMode::Connect => Ok(()),
            WalTcpExportMode::Listen => ConfigCheck::require(
                self.config.auth_token.is_some(),
                "persistence.tcp_export.auth_token is required in listen mode",
            ),
        }
    }

    fn validate_timeouts(&self) -> Result<()> {
        ConfigCheck::require(
            [
                self.config.connect_timeout_ms,
                self.config.write_timeout_ms,
                self.config.reconnect_backoff_ms,
            ]
            .into_iter()
            .all(|timeout| timeout > 0),
            "persistence.tcp_export timeouts must be > 0",
        )
    }
}

impl<'a> ReplicationValidation<'a> {
    fn new(config: &'a ReplicationConfig) -> Self {
        Self { config }
    }

    fn validate(&self) -> Result<()> {
        match self.config.enabled {
            false => Ok(()),
            true => {
                ConfigCheck::require(
                    !self.config.bind_addr.trim().is_empty(),
                    "replication.bind_addr must be set when replication is enabled",
                )?;
                self.validate_role()?;
                ConfigCheck::optional_token(
                    self.config.auth_token.as_deref(),
                    "replication.auth_token must not be empty",
                )?;
                ConfigCheck::require(
                    self.config.auth_token.is_none() || self.config.auth_token_path.is_none(),
                    "configure only one of replication.auth_token or auth_token_path",
                )?;
                ConfigCheck::require(
                    self.config.previous_auth_token_path.is_none()
                        || self.config.auth_token_path.is_some(),
                    "replication.previous_auth_token_path requires auth_token_path",
                )?;
                self.validate_tls()?;
                self.validate_batch_limits()?;
                self.validate_export_limits()?;
                self.validate_timeouts()
            }
        }
    }

    fn validate_tls(&self) -> Result<()> {
        let server = &self.config.tls_server;
        let client = &self.config.tls_client;
        if server.enabled || client.enabled {
            #[cfg(not(feature = "scnp-tls"))]
            return Err(ShardCacheError::Config(
                "replication TLS requires the scnp-tls feature".into(),
            ));
        }
        #[cfg(feature = "scnp-tls")]
        {
            if server.enabled {
                ConfigCheck::require(
                    !server.cert_path.as_os_str().is_empty()
                        && !server.key_path.as_os_str().is_empty()
                        && server.client_ca_path.is_some(),
                    "replication.tls_server requires cert_path, key_path, and client_ca_path for mTLS",
                )?;
                ConfigCheck::require(
                    server.handshake_timeout_ms > 0
                        && server.reload_interval_ms > 0
                        && server.max_concurrent_handshakes > 0,
                    "replication TLS handshake and reload limits must be > 0",
                )?;
                ConfigCheck::require(
                    self.config.max_replicas <= server.max_concurrent_handshakes,
                    "replication max_replicas must not exceed TLS max_concurrent_handshakes",
                )?;
                ConfigCheck::require(
                    server.client_cert_sha256.iter().all(|fingerprint| {
                        let compact = fingerprint.replace(':', "");
                        compact.len() == 64 && compact.bytes().all(|byte| byte.is_ascii_hexdigit())
                    }),
                    "replication TLS client fingerprints must be SHA-256 hexadecimal values",
                )?;
            }
            if client.enabled {
                ConfigCheck::require(
                    !client.ca_path.as_os_str().is_empty()
                        && client
                            .server_name
                            .as_deref()
                            .is_some_and(|name| !name.trim().is_empty()),
                    "replication.tls_client requires ca_path and server_name",
                )?;
                ConfigCheck::require(
                    client.client_cert_path.is_some() && client.client_key_path.is_some(),
                    "replication.tls_client requires a client certificate and key for mTLS",
                )?;
            }
        }
        let primary_non_loopback = self.config.role == ReplicationRole::Primary
            && !is_loopback_replication_endpoint(&self.config.bind_addr);
        let replica_non_loopback = self.config.role == ReplicationRole::Replica
            && self
                .config
                .replica_of
                .as_deref()
                .is_some_and(|address| !is_loopback_replication_endpoint(address));
        ConfigCheck::require(
            !primary_non_loopback || server.enabled,
            "non-loopback replication primary listeners require TLS",
        )?;
        ConfigCheck::require(
            !replica_non_loopback || client.enabled,
            "non-loopback replication replica connections require TLS",
        )?;
        ConfigCheck::require(
            !(primary_non_loopback || replica_non_loopback)
                || self.config.auth_token.is_some()
                || self.config.auth_token_path.is_some(),
            "non-loopback replication requires token authentication in addition to mTLS",
        )
    }

    fn validate_role(&self) -> Result<()> {
        match self.config.role {
            ReplicationRole::Primary => Ok(()),
            ReplicationRole::Replica => ConfigCheck::require(
                self.config
                    .replica_of
                    .as_deref()
                    .is_some_and(|addr| !addr.is_empty()),
                "replication.replica_of must be set for replica role",
            ),
        }
    }

    fn validate_batch_limits(&self) -> Result<()> {
        ConfigCheck::require(
            [
                self.config.batch_max_records,
                self.config.batch_max_bytes,
                self.config.backlog_bytes,
                self.config.snapshot_chunk_bytes,
                self.config.snapshot_receive_max_bytes,
                self.config.snapshot_receive_max_entries,
            ]
            .into_iter()
            .all(|limit| limit > 0),
            "replication batch, backlog, and snapshot limits must be > 0",
        )
    }

    fn validate_export_limits(&self) -> Result<()> {
        ConfigCheck::require(
            [
                self.config.queue_capacity,
                self.config.max_replicas,
                self.config.subscriber_channel_capacity,
            ]
            .into_iter()
            .all(|limit| limit > 0),
            "replication queue and subscriber limits must be > 0",
        )
    }

    fn validate_timeouts(&self) -> Result<()> {
        ConfigCheck::require(
            [
                self.config.connect_timeout_ms,
                self.config.write_timeout_ms,
                self.config.reconnect_backoff_ms,
            ]
            .into_iter()
            .all(|timeout| timeout > 0),
            "replication timeouts must be > 0",
        )
    }
}

fn is_loopback_replication_endpoint(endpoint: &str) -> bool {
    endpoint
        .parse::<std::net::SocketAddr>()
        .is_ok_and(|address| address.ip().is_loopback())
        || endpoint
            .strip_prefix("localhost:")
            .is_some_and(|port| port.parse::<u16>().is_ok())
}

impl ConfigCheck {
    fn require(condition: bool, message: impl Into<String>) -> Result<()> {
        condition
            .then_some(())
            .ok_or_else(|| ShardCacheError::Config(message.into()))
    }

    fn optional_token(token: Option<&str>, message: &'static str) -> Result<()> {
        match token {
            Some(token) => Self::require(!token.is_empty(), message),
            None => Ok(()),
        }
    }
}
