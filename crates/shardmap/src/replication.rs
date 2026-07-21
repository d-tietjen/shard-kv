//! Native shardcache replication.
//!
//! Replication is intentionally separate from WAL/AOF/PSYNC. WAL remains the
//! local crash-recovery path; this module emits compact storage-level mutation
//! batches for async read replicas and service subscribers.

mod backlog;
mod batcher;
mod embedded;
mod metrics;
mod protocol;
mod transport;

use crate::config::ReplicationConfig;
use crate::{Result, ShardCacheError};

fn validate_frame_limits(config: &ReplicationConfig) -> Result<()> {
    if config.batch_max_records == 0
        || config.batch_max_bytes == 0
        || config.snapshot_chunk_bytes == 0
        || config.receive_max_frame_bytes == 0
    {
        return Err(ShardCacheError::Config(
            "replication batch, snapshot, and frame limits must be > 0".into(),
        ));
    }
    if config.receive_max_frame_bytes > protocol::MAX_FCRP_PAYLOAD_BYTES {
        return Err(ShardCacheError::Config(
            "replication receive frame limit exceeds the FCRP protocol maximum".into(),
        ));
    }
    if config.receive_max_frame_bytes < config.batch_max_bytes.saturating_add(4) {
        return Err(ShardCacheError::Config(
            "replication receive frame limit cannot fit the configured mutation batch".into(),
        ));
    }
    if config.receive_max_frame_bytes < config.snapshot_chunk_bytes.max(4 * 1024) {
        return Err(ShardCacheError::Config(
            "replication receive frame limit cannot fit the configured snapshot chunk".into(),
        ));
    }
    Ok(())
}

/// Immutable FCRP wire frame shared by backlog, subscribers, and transports.
///
/// Keeping the encoded frame in the same byte owner used by `bytes-handoff`
/// lets broadcast/retry paths pass it through without copying the payload.
pub(super) type ReplicationFrameBytes = bytes::Bytes;

pub use backlog::{BacklogCatchUp, ReplicationBacklog};
pub(crate) use batcher::ReplicationBatchBuilder;
pub use batcher::ReplicationPrimary;
pub use embedded::{ReplicatedEmbeddedStore, ReplicationReplica};
pub use metrics::{ReplicationMetrics, ReplicationMetricsSnapshot};
pub use protocol::{
    BorrowedReplicationMutation, FCRP_MAGIC, FCRP_VERSION, FrameKind, HelloRole,
    ReplicationCompressionMode, ReplicationFrame, ReplicationFramePayload, ReplicationHello,
    ReplicationMutation, ReplicationSnapshot, ReplicationSnapshotChunk, ShardWatermarks,
    decode_ack, decode_error, decode_frame, decode_frame_payload, decode_hello,
    decode_mutation_batch, decode_snapshot_chunk, encode_ack, encode_error, encode_frame,
    encode_hello, encode_mutation_batch, encode_snapshot_chunk, visit_mutation_batch_payload,
};
pub use transport::{ReplicationPrimaryServer, ReplicationReplicaClient, SnapshotProvider};
