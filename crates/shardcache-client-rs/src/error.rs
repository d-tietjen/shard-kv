use thiserror::Error;

/// SCNP client error.
#[derive(Debug, Error)]
pub enum ShardCacheClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("config error: {0}")]
    Config(String),
}

/// SCNP client result.
pub type Result<T> = std::result::Result<T, ShardCacheClientError>;
