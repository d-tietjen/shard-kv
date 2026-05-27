use std::borrow::Cow;
use std::ptr;

use crate::config::ReplicationCompression;
use bytes::Bytes as SharedBytes;

use crate::storage::{MutationOp, MutationRecord, StoredEntry, hash_key, hash_key_tag_from_hash};
use crate::{Result, ShardCacheError};

pub const FCRP_MAGIC: &[u8; 4] = b"FCRP";
pub const FCRP_VERSION: u8 = 1;

const HEADER_LEN: usize = 16;
pub(crate) const FRAME_HEADER_LEN: usize = HEADER_LEN;
const FLAG_COMPRESSED: u8 = 0x01;
// u64::MAX is reserved as the wire sentinel for "no expiry". A legitimate
// `expire_at_ms` of u64::MAX would map to year ~584,942,417, so it is safe to
// reserve.
const EXPIRE_NONE: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Hello = 1,
    SnapshotBegin = 2,
    SnapshotChunk = 3,
    SnapshotEnd = 4,
    MutationBatch = 5,
    Ack = 6,
    Error = 7,
}

impl FrameKind {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::SnapshotBegin),
            3 => Ok(Self::SnapshotChunk),
            4 => Ok(Self::SnapshotEnd),
            5 => Ok(Self::MutationBatch),
            6 => Ok(Self::Ack),
            7 => Ok(Self::Error),
            other => Err(ShardCacheError::Protocol(format!(
                "unsupported FCRP frame kind: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationCompressionMode {
    None,
    Zstd,
}

impl From<ReplicationCompression> for ReplicationCompressionMode {
    fn from(value: ReplicationCompression) -> Self {
        match value {
            ReplicationCompression::None => Self::None,
            ReplicationCompression::Zstd => Self::Zstd,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationFrame {
    pub kind: FrameKind,
    pub compressed: bool,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct ReplicationFramePayload<'a> {
    pub kind: FrameKind,
    pub compressed: bool,
    pub payload: Cow<'a, [u8]>,
}

#[derive(Debug, Clone)]
pub struct ReplicationFrameBytesPayload {
    pub kind: FrameKind,
    pub compressed: bool,
    pub payload: SharedBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardWatermarks {
    values: Vec<u64>,
}

impl ShardWatermarks {
    pub fn new(shard_count: usize) -> Self {
        Self {
            values: vec![0; shard_count],
        }
    }

    pub fn from_vec(values: Vec<u64>) -> Self {
        Self { values }
    }

    pub fn as_slice(&self) -> &[u64] {
        &self.values
    }

    pub fn into_vec(self) -> Vec<u64> {
        self.values
    }

    pub fn get(&self, shard_id: usize) -> u64 {
        self.values.get(shard_id).copied().unwrap_or(0)
    }

    pub fn observe(&mut self, shard_id: usize, sequence: u64) {
        if shard_id >= self.values.len() {
            self.values.resize(shard_id + 1, 0);
        }
        self.values[shard_id] = self.values[shard_id].max(sequence);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMutationOp {
    Set,
    Del,
    Expire,
}

impl From<&MutationOp> for ReplicationMutationOp {
    fn from(value: &MutationOp) -> Self {
        match value {
            MutationOp::Set => Self::Set,
            MutationOp::Del => Self::Del,
            MutationOp::Expire => Self::Expire,
        }
    }
}

impl ReplicationMutationOp {
    fn to_byte(self) -> u8 {
        match self {
            Self::Set => 1,
            Self::Del => 2,
            Self::Expire => 3,
        }
    }

    fn from_byte(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Set),
            2 => Ok(Self::Del),
            3 => Ok(Self::Expire),
            other => Err(ShardCacheError::Protocol(format!(
                "unsupported FCRP mutation op: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationMutation {
    pub shard_id: usize,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub op: ReplicationMutationOp,
    pub key_hash: u64,
    pub key_tag: u64,
    pub key: SharedBytes,
    pub value: SharedBytes,
    pub expire_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct BorrowedReplicationMutation<'a> {
    pub shard_id: usize,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub op: ReplicationMutationOp,
    pub key_hash: u64,
    pub key_tag: u64,
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub expire_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct FrameBackedReplicationMutation<'a> {
    pub shard_id: usize,
    pub sequence: u64,
    pub op: ReplicationMutationOp,
    pub key_hash: u64,
    pub key: &'a [u8],
    pub value: SharedBytes,
    pub expire_at_ms: Option<u64>,
}

impl ReplicationMutation {
    pub fn from_record(record: &MutationRecord) -> Self {
        let key_hash = hash_key(record.key.as_ref());
        Self::from_record_with_key_hash(record, key_hash)
    }

    pub fn from_record_with_key_hash(record: &MutationRecord, key_hash: u64) -> Self {
        Self {
            shard_id: record.shard_id,
            sequence: record.sequence,
            timestamp_ms: record.timestamp_ms,
            op: ReplicationMutationOp::from(&record.op),
            key_hash,
            key_tag: hash_key_tag_from_hash(key_hash),
            key: record.key.clone(),
            value: record.value.clone(),
            expire_at_ms: record.expire_at_ms,
        }
    }

    pub fn estimated_uncompressed_len(&self) -> usize {
        mutation_record_payload_len(self.key.len(), self.value.len())
    }

    pub(crate) fn as_borrowed(&self) -> BorrowedReplicationMutation<'_> {
        BorrowedReplicationMutation {
            shard_id: self.shard_id,
            sequence: self.sequence,
            timestamp_ms: self.timestamp_ms,
            op: self.op,
            key_hash: self.key_hash,
            key_tag: self.key_tag,
            key: self.key.as_ref(),
            value: self.value.as_ref(),
            expire_at_ms: self.expire_at_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationSnapshot {
    pub entries: Vec<StoredEntry>,
    pub watermarks: ShardWatermarks,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationSnapshotChunk {
    pub watermarks: ShardWatermarks,
    pub chunk_index: u64,
    pub is_last: bool,
    pub entries: Vec<StoredEntry>,
}

pub fn encode_frame(
    kind: FrameKind,
    compression: ReplicationCompressionMode,
    zstd_level: i32,
    payload: &[u8],
) -> Result<Vec<u8>> {
    match compression {
        ReplicationCompressionMode::None => {
            let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
            write_header(
                &mut out,
                kind,
                0,
                payload.len() as u32,
                payload.len() as u32,
            );
            out.extend_from_slice(payload);
            Ok(out)
        }
        ReplicationCompressionMode::Zstd => {
            let compressed = zstd::bulk::compress(payload, zstd_level).map_err(|error| {
                ShardCacheError::Protocol(format!("FCRP zstd compression failed: {error}"))
            })?;
            let mut out = Vec::with_capacity(HEADER_LEN + compressed.len());
            write_header(
                &mut out,
                kind,
                FLAG_COMPRESSED,
                compressed.len() as u32,
                payload.len() as u32,
            );
            out.extend_from_slice(&compressed);
            Ok(out)
        }
    }
}

fn write_header(
    out: &mut Vec<u8>,
    kind: FrameKind,
    flags: u8,
    payload_len: u32,
    uncompressed_len: u32,
) {
    out.extend_from_slice(FCRP_MAGIC);
    out.push(FCRP_VERSION);
    out.push(kind as u8);
    out.push(flags);
    out.push(0);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&uncompressed_len.to_le_bytes());
}

pub(crate) fn write_uncompressed_frame_header_at(
    frame: &mut [u8],
    kind: FrameKind,
    payload_len: usize,
) {
    debug_assert!(frame.len() >= HEADER_LEN);
    frame[..4].copy_from_slice(FCRP_MAGIC);
    frame[4] = FCRP_VERSION;
    frame[5] = kind as u8;
    frame[6] = 0;
    frame[7] = 0;
    frame[8..12].copy_from_slice(&(payload_len as u32).to_le_bytes());
    frame[12..16].copy_from_slice(&(payload_len as u32).to_le_bytes());
}

pub fn decode_frame(bytes: &[u8]) -> Result<ReplicationFrame> {
    let frame = decode_frame_payload(bytes)?;
    Ok(ReplicationFrame {
        kind: frame.kind,
        compressed: frame.compressed,
        payload: frame.payload.into_owned(),
    })
}

pub fn decode_frame_payload(bytes: &[u8]) -> Result<ReplicationFramePayload<'_>> {
    if bytes.len() < HEADER_LEN {
        return Err(ShardCacheError::Protocol("FCRP frame is truncated".into()));
    }
    if &bytes[..4] != FCRP_MAGIC {
        return Err(ShardCacheError::Protocol("FCRP magic mismatch".into()));
    }
    if bytes[4] != FCRP_VERSION {
        return Err(ShardCacheError::Protocol(format!(
            "unsupported FCRP version: {}",
            bytes[4]
        )));
    }
    let kind = FrameKind::from_u8(bytes[5])?;
    let flags = bytes[6];
    let payload_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let uncompressed_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    if HEADER_LEN + payload_len != bytes.len() {
        return Err(ShardCacheError::Protocol(
            "FCRP frame length mismatch".into(),
        ));
    }
    let raw = &bytes[HEADER_LEN..];
    let compressed = flags & FLAG_COMPRESSED != 0;
    let payload = if compressed {
        Cow::Owned(
            zstd::bulk::decompress(raw, uncompressed_len).map_err(|error| {
                ShardCacheError::Protocol(format!("FCRP zstd decompression failed: {error}"))
            })?,
        )
    } else {
        Cow::Borrowed(raw)
    };
    Ok(ReplicationFramePayload {
        kind,
        compressed,
        payload,
    })
}

pub fn decode_frame_payload_bytes(bytes: SharedBytes) -> Result<ReplicationFrameBytesPayload> {
    if bytes.len() < HEADER_LEN {
        return Err(ShardCacheError::Protocol("FCRP frame is truncated".into()));
    }
    if &bytes[..4] != FCRP_MAGIC {
        return Err(ShardCacheError::Protocol("FCRP magic mismatch".into()));
    }
    if bytes[4] != FCRP_VERSION {
        return Err(ShardCacheError::Protocol(format!(
            "unsupported FCRP version: {}",
            bytes[4]
        )));
    }

    let kind = FrameKind::from_u8(bytes[5])?;
    let flags = bytes[6];
    let payload_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let uncompressed_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    if HEADER_LEN + payload_len != bytes.len() {
        return Err(ShardCacheError::Protocol(
            "FCRP frame length mismatch".into(),
        ));
    }

    let compressed = flags & FLAG_COMPRESSED != 0;
    let payload = match compressed {
        true => {
            let raw = &bytes[HEADER_LEN..];
            SharedBytes::from(
                zstd::bulk::decompress(raw, uncompressed_len).map_err(|error| {
                    ShardCacheError::Protocol(format!("FCRP zstd decompression failed: {error}"))
                })?,
            )
        }
        false => bytes.slice(HEADER_LEN..),
    };
    Ok(ReplicationFrameBytesPayload {
        kind,
        compressed,
        payload,
    })
}

pub fn encode_mutation_batch(mutations: &[ReplicationMutation]) -> Vec<u8> {
    let payload_len = mutation_batch_payload_len(mutations);
    let mut out = Vec::with_capacity(payload_len);
    write_mutation_batch_payload(&mut out, mutations);
    out
}

pub(crate) fn encode_mutation_batch_frame_with_payload_len(
    mutations: &[ReplicationMutation],
    payload_len: usize,
    compression: ReplicationCompressionMode,
    zstd_level: i32,
) -> Result<(Vec<u8>, usize)> {
    match compression {
        ReplicationCompressionMode::None => {
            let mut out = Vec::with_capacity(HEADER_LEN + payload_len);
            write_header(
                &mut out,
                FrameKind::MutationBatch,
                0,
                payload_len as u32,
                payload_len as u32,
            );
            write_mutation_batch_payload(&mut out, mutations);
            Ok((out, payload_len))
        }
        ReplicationCompressionMode::Zstd => {
            let mut payload = Vec::with_capacity(payload_len);
            write_mutation_batch_payload(&mut payload, mutations);
            encode_frame(FrameKind::MutationBatch, compression, zstd_level, &payload)
                .map(|frame| (frame, payload_len))
        }
    }
}

fn mutation_batch_payload_len(mutations: &[ReplicationMutation]) -> usize {
    4 + mutations
        .iter()
        .map(ReplicationMutation::estimated_uncompressed_len)
        .sum::<usize>()
}

pub(crate) fn mutation_record_payload_len(key_len: usize, value_len: usize) -> usize {
    4 + 8 + 8 + 1 + 8 + 8 + 8 + 4 + 4 + key_len + value_len
}

pub(crate) fn borrowed_mutation_record_payload_len(
    mutation: BorrowedReplicationMutation<'_>,
) -> usize {
    mutation_record_payload_len(mutation.key.len(), mutation.value.len())
}

pub(crate) fn mutation_batch_record_count(bytes: &[u8]) -> Result<usize> {
    let Some(count) = bytes.get(..4) else {
        return Err(ShardCacheError::Protocol(
            "FCRP payload is truncated".into(),
        ));
    };
    Ok(u32::from_le_bytes(count.try_into().unwrap()) as usize)
}

pub(crate) fn write_borrowed_mutation_payload_record(
    out: &mut Vec<u8>,
    mutation: BorrowedReplicationMutation<'_>,
) {
    let start = out.len();
    let record_len = borrowed_mutation_record_payload_len(mutation);
    out.reserve(record_len);
    // The frame builder just reserved `record_len`, and every write below
    // advances by exactly the encoded field width. This avoids one bounds and
    // capacity check per scalar field on the replication hot path.
    unsafe {
        let mut cursor = out.as_mut_ptr().add(start);
        write_u32_le(&mut cursor, mutation.shard_id as u32);
        write_u64_le(&mut cursor, mutation.sequence);
        write_u64_le(&mut cursor, mutation.timestamp_ms);
        write_u8(&mut cursor, mutation.op.to_byte());
        write_u64_le(&mut cursor, mutation.key_hash);
        write_u64_le(&mut cursor, mutation.key_tag);
        write_u64_le(&mut cursor, mutation.expire_at_ms.unwrap_or(EXPIRE_NONE));
        write_u32_le(&mut cursor, mutation.key.len() as u32);
        write_u32_le(&mut cursor, mutation.value.len() as u32);
        write_bytes(&mut cursor, mutation.key);
        write_bytes(&mut cursor, mutation.value);
        debug_assert_eq!(
            cursor.offset_from(out.as_ptr().add(start)),
            record_len as isize
        );
        out.set_len(start + record_len);
    }
}

#[inline(always)]
unsafe fn write_u8(cursor: &mut *mut u8, value: u8) {
    // SAFETY: the caller reserved enough capacity for the whole encoded record.
    unsafe {
        ptr::write(*cursor, value);
        *cursor = (*cursor).add(1);
    }
}

#[inline(always)]
unsafe fn write_u32_le(cursor: &mut *mut u8, value: u32) {
    // SAFETY: the caller reserved enough capacity for the whole encoded record;
    // unaligned writes are intentional because the frame is byte packed.
    unsafe {
        ptr::write_unaligned((*cursor).cast::<u32>(), value.to_le());
        *cursor = (*cursor).add(4);
    }
}

#[inline(always)]
unsafe fn write_u64_le(cursor: &mut *mut u8, value: u64) {
    // SAFETY: the caller reserved enough capacity for the whole encoded record;
    // unaligned writes are intentional because the frame is byte packed.
    unsafe {
        ptr::write_unaligned((*cursor).cast::<u64>(), value.to_le());
        *cursor = (*cursor).add(8);
    }
}

#[inline(always)]
unsafe fn write_bytes(cursor: &mut *mut u8, bytes: &[u8]) {
    // SAFETY: the caller reserved enough capacity for the whole encoded record.
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), *cursor, bytes.len());
        *cursor = (*cursor).add(bytes.len());
    }
}

fn write_mutation_batch_payload(out: &mut Vec<u8>, mutations: &[ReplicationMutation]) {
    out.extend_from_slice(&(mutations.len() as u32).to_le_bytes());
    for mutation in mutations {
        write_borrowed_mutation_payload_record(out, mutation.as_borrowed());
    }
}

pub fn decode_mutation_batch(bytes: &[u8]) -> Result<Vec<ReplicationMutation>> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.u32()? as usize;
    let mut mutations = Vec::with_capacity(count);
    for _ in 0..count {
        let shard_id = cursor.u32()? as usize;
        let sequence = cursor.u64()?;
        let timestamp_ms = cursor.u64()?;
        let op = ReplicationMutationOp::from_byte(cursor.u8()?)?;
        let key_hash = cursor.u64()?;
        let key_tag = cursor.u64()?;
        let expire_raw = cursor.u64()?;
        let key_len = cursor.u32()? as usize;
        let value_len = cursor.u32()? as usize;
        let key = SharedBytes::from(cursor.bytes(key_len)?.to_vec());
        let value = SharedBytes::from(cursor.bytes(value_len)?.to_vec());
        mutations.push(ReplicationMutation {
            shard_id,
            sequence,
            timestamp_ms,
            op,
            key_hash,
            key_tag,
            key,
            value,
            expire_at_ms: (expire_raw != EXPIRE_NONE).then_some(expire_raw),
        });
    }
    cursor.finish()?;
    Ok(mutations)
}

pub fn visit_mutation_batch_payload<F>(bytes: &[u8], mut visit: F) -> Result<()>
where
    F: FnMut(BorrowedReplicationMutation<'_>) -> Result<()>,
{
    let mut cursor = Cursor::new(bytes);
    let count = cursor.u32()? as usize;
    for _ in 0..count {
        let shard_id = cursor.u32()? as usize;
        let sequence = cursor.u64()?;
        let timestamp_ms = cursor.u64()?;
        let op = ReplicationMutationOp::from_byte(cursor.u8()?)?;
        let key_hash = cursor.u64()?;
        let key_tag = cursor.u64()?;
        let expire_raw = cursor.u64()?;
        let key_len = cursor.u32()? as usize;
        let value_len = cursor.u32()? as usize;
        let key = cursor.bytes(key_len)?;
        let value = cursor.bytes(value_len)?;
        visit(BorrowedReplicationMutation {
            shard_id,
            sequence,
            timestamp_ms,
            op,
            key_hash,
            key_tag,
            key,
            value,
            expire_at_ms: (expire_raw != EXPIRE_NONE).then_some(expire_raw),
        })?;
    }
    cursor.finish()
}

pub fn visit_mutation_batch_payload_bytes<F>(bytes: SharedBytes, mut visit: F) -> Result<()>
where
    F: FnMut(FrameBackedReplicationMutation<'_>) -> Result<()>,
{
    let mut cursor = Cursor::new(bytes.as_ref());
    let count = cursor.u32()? as usize;
    for _ in 0..count {
        let shard_id = cursor.u32()? as usize;
        let sequence = cursor.u64()?;
        let _timestamp_ms = cursor.u64()?;
        let op = ReplicationMutationOp::from_byte(cursor.u8()?)?;
        let key_hash = cursor.u64()?;
        let _key_tag = cursor.u64()?;
        let expire_raw = cursor.u64()?;
        let key_len = cursor.u32()? as usize;
        let value_len = cursor.u32()? as usize;
        let key = cursor.bytes(key_len)?;
        let value_start = cursor.pos;
        let _ = cursor.bytes(value_len)?;
        let value = bytes.slice(value_start..value_start + value_len);
        visit(FrameBackedReplicationMutation {
            shard_id,
            sequence,
            op,
            key_hash,
            key,
            value,
            expire_at_ms: (expire_raw != EXPIRE_NONE).then_some(expire_raw),
        })?;
    }
    cursor.finish()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelloRole {
    Replica = 1,
    ServiceSubscriber = 2,
}

impl HelloRole {
    fn to_byte(self) -> u8 {
        self as u8
    }

    fn from_byte(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Replica),
            2 => Ok(Self::ServiceSubscriber),
            other => Err(ShardCacheError::Protocol(format!(
                "unsupported FCRP hello role: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationHello {
    pub version: u8,
    pub role: HelloRole,
    pub auth_token: Option<String>,
    pub since: Option<ShardWatermarks>,
}

pub fn encode_hello(hello: &ReplicationHello) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(hello.version);
    out.push(hello.role.to_byte());
    let token = hello.auth_token.as_deref().unwrap_or("");
    out.extend_from_slice(&(token.len() as u32).to_le_bytes());
    out.extend_from_slice(token.as_bytes());
    match &hello.since {
        Some(watermarks) => {
            out.push(1);
            out.extend_from_slice(&(watermarks.as_slice().len() as u32).to_le_bytes());
            for value in watermarks.as_slice() {
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        None => out.push(0),
    }
    out
}

pub fn decode_hello(bytes: &[u8]) -> Result<ReplicationHello> {
    let mut cursor = Cursor::new(bytes);
    let version = cursor.u8()?;
    let role = HelloRole::from_byte(cursor.u8()?)?;
    let token_len = cursor.u32()? as usize;
    let token_bytes = cursor.bytes(token_len)?;
    let auth_token = if token_len == 0 {
        None
    } else {
        Some(
            std::str::from_utf8(token_bytes)
                .map_err(|error| {
                    ShardCacheError::Protocol(format!(
                        "FCRP hello auth token is not UTF-8: {error}"
                    ))
                })?
                .to_string(),
        )
    };
    let has_since = cursor.u8()? != 0;
    let since = if has_since {
        let count = cursor.u32()? as usize;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(cursor.u64()?);
        }
        Some(ShardWatermarks::from_vec(values))
    } else {
        None
    };
    cursor.finish()?;
    Ok(ReplicationHello {
        version,
        role,
        auth_token,
        since,
    })
}

pub fn encode_error(message: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + message.len());
    out.extend_from_slice(&(message.len() as u32).to_le_bytes());
    out.extend_from_slice(message.as_bytes());
    out
}

pub fn decode_error(bytes: &[u8]) -> Result<String> {
    let mut cursor = Cursor::new(bytes);
    let len = cursor.u32()? as usize;
    let body = cursor.bytes(len)?;
    cursor.finish()?;
    std::str::from_utf8(body)
        .map(|s| s.to_string())
        .map_err(|error| {
            ShardCacheError::Protocol(format!("FCRP error payload not UTF-8: {error}"))
        })
}

pub fn encode_ack(watermarks: &ShardWatermarks) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + watermarks.as_slice().len() * 8);
    out.extend_from_slice(&(watermarks.as_slice().len() as u32).to_le_bytes());
    for value in watermarks.as_slice() {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

pub fn decode_ack(bytes: &[u8]) -> Result<ShardWatermarks> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.u32()? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(cursor.u64()?);
    }
    cursor.finish()?;
    Ok(ShardWatermarks::from_vec(values))
}

pub fn encode_snapshot_chunk(chunk: &ReplicationSnapshotChunk) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&chunk.chunk_index.to_le_bytes());
    out.push(u8::from(chunk.is_last));
    out.extend_from_slice(&(chunk.watermarks.as_slice().len() as u32).to_le_bytes());
    for watermark in chunk.watermarks.as_slice() {
        out.extend_from_slice(&watermark.to_le_bytes());
    }
    out.extend_from_slice(&(chunk.entries.len() as u32).to_le_bytes());
    for entry in &chunk.entries {
        out.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
        out.extend_from_slice(&(entry.value.len() as u32).to_le_bytes());
        out.extend_from_slice(&entry.expire_at_ms.unwrap_or(EXPIRE_NONE).to_le_bytes());
        out.extend_from_slice(entry.key.as_ref());
        out.extend_from_slice(entry.value.as_ref());
    }
    out
}

pub fn decode_snapshot_chunk(bytes: &[u8]) -> Result<ReplicationSnapshotChunk> {
    let mut cursor = Cursor::new(bytes);
    let chunk_index = cursor.u64()?;
    let is_last = cursor.u8()? != 0;
    let watermark_count = cursor.u32()? as usize;
    let mut watermarks = Vec::with_capacity(watermark_count);
    for _ in 0..watermark_count {
        watermarks.push(cursor.u64()?);
    }
    let entry_count = cursor.u32()? as usize;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let key_len = cursor.u32()? as usize;
        let value_len = cursor.u32()? as usize;
        let expire_raw = cursor.u64()?;
        let key = cursor.bytes(key_len)?.to_vec();
        let value = cursor.bytes(value_len)?.to_vec();
        entries.push(StoredEntry {
            key,
            value,
            expire_at_ms: (expire_raw != EXPIRE_NONE).then_some(expire_raw),
        });
    }
    cursor.finish()?;
    Ok(ReplicationSnapshotChunk {
        watermarks: ShardWatermarks::from_vec(watermarks),
        chunk_index,
        is_last,
        entries,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn u8(&mut self) -> Result<u8> {
        let Some(value) = self.bytes.get(self.pos).copied() else {
            return Err(ShardCacheError::Protocol(
                "FCRP payload is truncated".into(),
            ));
        };
        self.pos += 1;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes = self.bytes(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes = self.bytes(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.pos + len > self.bytes.len() {
            return Err(ShardCacheError::Protocol(
                "FCRP payload is truncated".into(),
            ));
        }
        let bytes = &self.bytes[self.pos..self.pos + len];
        self.pos += len;
        Ok(bytes)
    }

    fn finish(&self) -> Result<()> {
        if self.pos != self.bytes.len() {
            return Err(ShardCacheError::Protocol(
                "FCRP payload contains trailing bytes".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::hash_key_tag;

    use super::*;

    fn sample_mutation(sequence: u64) -> ReplicationMutation {
        let key = b"alpha".to_vec();
        ReplicationMutation {
            shard_id: 2,
            sequence,
            timestamp_ms: 42,
            op: ReplicationMutationOp::Set,
            key_hash: hash_key(&key),
            key_tag: hash_key_tag(&key),
            key: SharedBytes::from(key),
            value: SharedBytes::from_static(b"value"),
            expire_at_ms: Some(99),
        }
    }

    #[test]
    fn mutation_batch_round_trips() {
        let mutations = vec![sample_mutation(1), sample_mutation(2)];
        let encoded = encode_mutation_batch(&mutations);
        let decoded = decode_mutation_batch(&encoded).expect("decode");
        assert_eq!(decoded, mutations);
    }

    #[test]
    fn mutation_batch_frame_round_trips_without_payload_copy() {
        let mutations = vec![sample_mutation(1), sample_mutation(2)];
        let payload = encode_mutation_batch(&mutations);
        let (encoded, uncompressed_len) = encode_mutation_batch_frame_with_payload_len(
            &mutations,
            payload.len(),
            ReplicationCompressionMode::None,
            0,
        )
        .expect("encode");
        let decoded = decode_frame(&encoded).expect("decode frame");
        assert_eq!(decoded.kind, FrameKind::MutationBatch);
        assert!(!decoded.compressed);
        assert_eq!(uncompressed_len, payload.len());
        assert_eq!(decoded.payload, payload);
        assert_eq!(
            decode_mutation_batch(&decoded.payload).expect("decode batch"),
            mutations
        );
    }

    #[test]
    fn mutation_batch_payload_bytes_visits_frame_backed_values() {
        let mutations = vec![sample_mutation(1), sample_mutation(2)];
        let payload = encode_mutation_batch(&mutations);
        let (encoded, _) = encode_mutation_batch_frame_with_payload_len(
            &mutations,
            payload.len(),
            ReplicationCompressionMode::None,
            0,
        )
        .expect("encode");

        let decoded = decode_frame_payload_bytes(SharedBytes::from(encoded)).expect("decode");
        let payload_start = decoded.payload.as_ptr() as usize;
        let payload_end = payload_start + decoded.payload.len();
        let mut visited = Vec::new();
        visit_mutation_batch_payload_bytes(decoded.payload, |mutation| {
            let value_start = mutation.value.as_ptr() as usize;
            let value_end = value_start + mutation.value.len();
            assert!(value_start >= payload_start);
            assert!(value_end <= payload_end);
            visited.push(mutation.value);
            Ok(())
        })
        .expect("visit");

        assert_eq!(
            visited,
            vec![mutations[0].value.clone(), mutations[1].value.clone()]
        );
    }

    #[test]
    fn zstd_frame_round_trips() {
        let payload = encode_mutation_batch(&[sample_mutation(1)]);
        let encoded = encode_frame(
            FrameKind::MutationBatch,
            ReplicationCompressionMode::Zstd,
            3,
            &payload,
        )
        .expect("encode");
        let decoded = decode_frame(&encoded).expect("decode");
        assert_eq!(decoded.kind, FrameKind::MutationBatch);
        assert!(decoded.compressed);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn snapshot_chunk_round_trips() {
        let chunk = ReplicationSnapshotChunk {
            watermarks: ShardWatermarks::from_vec(vec![1, 2]),
            chunk_index: 3,
            is_last: true,
            entries: vec![StoredEntry {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expire_at_ms: None,
            }],
        };
        let encoded = encode_snapshot_chunk(&chunk);
        let decoded = decode_snapshot_chunk(&encoded).expect("decode");
        assert_eq!(decoded, chunk);
    }
}
