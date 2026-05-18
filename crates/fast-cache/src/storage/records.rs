use crate::storage::Bytes;

pub type MutationBytes = bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEntry {
    pub key: Bytes,
    pub value: Bytes,
    pub expire_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum MutationOp {
    Set,
    Del,
    Expire,
}

#[derive(Debug, Clone)]
pub struct MutationRecord {
    pub shard_id: usize,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub op: MutationOp,
    pub key: MutationBytes,
    pub value: MutationBytes,
    pub expire_at_ms: Option<u64>,
}
