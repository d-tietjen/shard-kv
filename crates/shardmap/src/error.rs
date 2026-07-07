use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShardCacheError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("toml decode error: {0}")]
    TomlDecode(#[from] toml::de::Error),

    #[error("toml encode error: {0}")]
    TomlEncode(#[from] toml::ser::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("command error: {0}")]
    Command(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("persistence error: {0}")]
    Persistence(String),

    #[error("object integrity error: {0}")]
    ObjectIntegrity(String),

    #[error("channel closed: {0}")]
    ChannelClosed(&'static str),

    #[error("task join error: {0}")]
    TaskJoin(String),
}

pub type Result<T> = std::result::Result<T, ShardCacheError>;
