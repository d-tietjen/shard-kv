#![doc = include_str!("../README.md")]

mod client;
mod commands;
mod connection;
mod error;
mod protocol;
#[cfg(feature = "redis")]
mod redis;
mod routing;
#[cfg(feature = "tls")]
mod tls;

pub use client::{
    ShardCacheClient, ShardCacheDirectClient, ShardCacheDirectShardClient, ShardCacheTopology,
};
#[cfg(feature = "redis")]
pub use commands::redis::{RedisCommandKind, RedisResponse};
pub use error::{Result, ShardCacheClientError};
#[cfg(feature = "redis")]
pub use redis::{Redis, RedisArg, RedisCmd, RedisCommandExecutor};
pub use routing::{
    ShardCacheDirectRouter, ShardCacheRoute, ShardCacheRouteMode, hash_key, hash_key_tag,
    shard_index,
};
#[cfg(feature = "tls")]
pub use tls::ScnpTlsClientConfig;
