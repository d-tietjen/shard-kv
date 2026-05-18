use thiserror::Error;

/// FCNP client error.
#[derive(Debug, Error)]
pub enum FcnpClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("config error: {0}")]
    Config(String),
}

/// FCNP client result.
pub type Result<T> = std::result::Result<T, FcnpClientError>;
