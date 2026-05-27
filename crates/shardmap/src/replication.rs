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
