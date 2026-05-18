use super::*;

/// Error returned when installing a worker-local store into thread-local state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalStoreInstallError {
    AlreadyInstalled,
}

impl fmt::Display for LocalStoreInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyInstalled => {
                f.write_str("a worker-local embedded store is already installed on this thread")
            }
        }
    }
}

impl std::error::Error for LocalStoreInstallError {}

/// Error returned when accessing thread-local worker storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalStoreAccessError {
    NotInstalled,
}

impl fmt::Display for LocalStoreAccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInstalled => {
                f.write_str("no worker-local embedded store is installed on this thread")
            }
        }
    }
}

impl std::error::Error for LocalStoreAccessError {}

/// Indicates that a requested key or session routes to another worker's shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalRouteError {
    KeyNotLocal { shard_id: usize },
    SessionNotLocal { shard_id: usize },
}

impl fmt::Display for LocalRouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyNotLocal { shard_id } => {
                write!(
                    f,
                    "key routes to shard {shard_id}, which is not owned by this thread"
                )
            }
            Self::SessionNotLocal { shard_id } => write!(
                f,
                "session routes to shard {shard_id}, which is not owned by this thread"
            ),
        }
    }
}

impl std::error::Error for LocalRouteError {}
