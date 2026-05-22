use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use bytes_handoff::{HandoffBuffer, HandoffBufferConfig, WriteHandoff, WriteHandoffConfig};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, LocalSet, spawn_local};
use tokio::time::{MissedTickBehavior, interval};

use crate::Result;
use crate::config::FastCacheConfig;
use crate::protocol::{
    FAST_FLAG_KEY_HASH, FAST_FLAG_KEY_TAG, FAST_FLAG_ROUTE_SHARD, FAST_PROTOCOL_VERSION,
    FAST_REQUEST_MAGIC, FAST_RESPONSE_MAGIC, FastCodec, FastCommand, FastRequest, FastResponse,
    Frame, RespCodec,
};
#[cfg(not(feature = "embedded"))]
use crate::storage::FlatMap;
use crate::storage::{BorrowedCommand, Bytes, EngineHandle, now_millis};
#[cfg(feature = "embedded")]
use crate::storage::{EmbeddedRouteMode, EmbeddedStore, LocalEmbeddedStore};
#[cfg(all(feature = "embedded", feature = "redis-compat"))]
use crate::storage::{RedisObjectReadOutcome, RedisObjectResult, WRONGTYPE_MESSAGE};

pub(crate) mod commands;
mod connection;
mod direct;
#[cfg(feature = "embedded")]
mod direct_protocol;
mod fast_write;
mod lifecycle;
mod transactions;
mod transport;
pub(crate) mod wire;

#[cfg(all(test, feature = "embedded"))]
mod tests;

pub use lifecycle::ServerRuntime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerMode {
    Auto,
    Engine,
    Direct,
}

pub struct FastCacheServer {
    config: FastCacheConfig,
    engine: Option<EngineHandle>,
    mode: ServerMode,
    unix_socket_path: Option<PathBuf>,
}

const READ_CHUNK_SIZE: usize = 64 * 1024;
const READ_RESERVE_THRESHOLD: usize = 16 * 1024;
const CONNECTION_BUFFER_CAPACITY: usize = 64 * 1024;
const HANDOFF_BUFFER_MAX: usize = 4 * 1024 * 1024;
const WRITE_HANDOFF_MAX_ITEMS: usize = 1024;
const WRITE_HANDOFF_MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;
const FAST_STATUS_OK: u8 = 0;
const FAST_STATUS_NULL: u8 = 1;
const FAST_STATUS_ERROR: u8 = 2;
#[allow(dead_code)]
const FAST_STATUS_INTEGER: u8 = 3;
const FAST_STATUS_VALUE: u8 = 4;
#[allow(dead_code)]
const FAST_STATUS_ARRAY: u8 = 6;
#[allow(dead_code)]
const FAST_STATUS_FLOAT: u8 = 7;

// FCNP GET response size classes:
// - 64 B values are common enough to get a dedicated inline copy path.
// - Sub-2 KiB values stay in the contiguous write buffer; writev overhead is
//   higher than the saved copy at that size on the reference Linux host.
// - 2 KiB+ values use header+payload writev so the stored Bytes payload is not
//   copied into the response buffer.
const FCNP_ZERO_COPY_VALUE_THRESHOLD: usize = 1024;
const RESP_ZERO_COPY_VALUE_THRESHOLD: usize = 2048;
const RESP_HEADER_MAX_LEN: usize = 32;
static RESP_CRLF: &[u8; 2] = b"\r\n";
