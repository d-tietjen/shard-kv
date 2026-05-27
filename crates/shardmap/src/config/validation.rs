use super::{
    EvictionPolicy, PersistenceConfig, ReplicationConfig, ReplicationRole, ShardCacheConfig,
    WalTcpExportConfig, WalTcpExportMode,
};
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
    Replication,
    Cuda,
}

struct PersistenceValidation<'a> {
    config: &'a PersistenceConfig,
}

struct WalTcpExportValidation<'a> {
    config: &'a WalTcpExportConfig,
}

struct ReplicationValidation<'a> {
    config: &'a ReplicationConfig,
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
