#[cfg(feature = "object-overflow")]
use super::ObjectOverflowBackend;
use super::{
    EvictionPolicy, KvOverflowConfig, ObjectOverflowConfig, PersistenceConfig, ReplicationConfig,
    ReplicationRole, ShardCacheConfig, WalTcpExportConfig, WalTcpExportMode,
};
#[cfg(feature = "kv-overflow")]
use super::{KvOverflowBackend, MAX_KV_OVERFLOW_SLOT_COUNT};
use crate::{Result, ShardCacheError};

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
                !self.config.endpoints.is_empty(),
                "kv_overflow.endpoints must contain at least one server",
            )?;
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
                self.config.slot_count <= MAX_KV_OVERFLOW_SLOT_COUNT,
                "kv_overflow.slot_count exceeds the supported maximum",
            )?;
            match self.config.backend {
                KvOverflowBackend::Scnp => {
                    ConfigCheck::require(
                        self.config.redis_username_env.is_none()
                            && self.config.redis_password_env.is_none(),
                        "kv_overflow Redis credential env vars require backend = \"redis\"",
                    )?;
                }
                KvOverflowBackend::Redis => {
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
                self.config.worker_threads > 0 && self.config.queue_capacity > 0,
                "kv_overflow.worker_threads and queue_capacity must be > 0",
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
        for endpoint in self
            .config
            .endpoints
            .iter()
            .chain(&self.config.previous_endpoints)
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
                self.validate_batch_limits()?;
                self.validate_export_limits()?;
                self.validate_timeouts()
            }
        }
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
