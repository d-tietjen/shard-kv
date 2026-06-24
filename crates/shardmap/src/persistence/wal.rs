use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use crate::storage::{MutationBytes, MutationOp, MutationRecord};
use crate::{Result, ShardCacheError};

const WAL_FILE_EXT: &str = "wal";
const WAL_COMPRESSED_SUFFIX: &str = ".wal.lz4";
const WAL_FRAME_MAGIC: &[u8; 4] = b"FCW2";
const WAL_FLAG_COMPRESSED: u8 = 0x01;
const WAL_FRAME_HEADER_LEN: usize = 4 + 1 + 4;
const WAL_CRC_LEN: usize = 4;
const LEGACY_RECORD_HEADER_TAIL_LEN: usize = 4 + 4 + 8 + 8;

/// Filesystem repository for WAL segment discovery, replay, and pruning.
#[derive(Debug, Clone)]
pub(super) struct SegmentStore {
    data_dir: PathBuf,
}

enum SegmentPrune {
    Remove,
    Keep,
}

impl SegmentStore {
    pub(super) fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
        }
    }

    pub(super) fn open_writer(&self, compress: bool) -> Result<SegmentWriter> {
        fs::create_dir_all(&self.data_dir)?;
        let segment_sequence = self.next_sequence()?;
        let path = WalSegmentName::path(
            &self.data_dir,
            segment_sequence,
            crate::storage::now_millis(),
            compress,
        );
        let file = BufWriter::new(OpenOptions::new().create(true).append(true).open(&path)?);
        Ok(SegmentWriter {
            data_dir: self.data_dir.clone(),
            file,
            compress,
            segment_sequence,
            bytes_written: fs::metadata(path)?.len(),
            last_entry_size: 0,
            rotation_count: 0,
        })
    }

    pub(super) fn read_all(&self) -> Result<Vec<MutationRecord>> {
        let mut records = Vec::new();
        for path in self.paths()? {
            records.extend(self.read_segment(&path)?);
        }
        Ok(records)
    }

    pub(super) fn prune_through(&self, snapshot_timestamp_ms: u64) -> Result<()> {
        match self.paths()? {
            paths if paths.len() <= 1 => Ok(()),
            paths => self.prune_old_paths(paths, snapshot_timestamp_ms),
        }
    }

    fn prune_old_paths(&self, paths: Vec<PathBuf>, snapshot_timestamp_ms: u64) -> Result<()> {
        let keep_last = paths.len().saturating_sub(1);
        for path in paths.into_iter().take(keep_last) {
            self.prune_path(path, snapshot_timestamp_ms)?;
        }
        Ok(())
    }

    fn prune_path(&self, path: PathBuf, snapshot_timestamp_ms: u64) -> Result<()> {
        match self.prune_decision(&path, snapshot_timestamp_ms)? {
            SegmentPrune::Remove => {
                fs::remove_file(path)?;
                Ok(())
            }
            SegmentPrune::Keep => Ok(()),
        }
    }

    fn prune_decision(&self, path: &Path, snapshot_timestamp_ms: u64) -> Result<SegmentPrune> {
        let records = self.read_segment(path)?;
        match records.as_slice() {
            [] => Ok(SegmentPrune::Remove),
            records
                if records
                    .iter()
                    .all(|record| record.timestamp_ms <= snapshot_timestamp_ms) =>
            {
                Ok(SegmentPrune::Remove)
            }
            _ => Ok(SegmentPrune::Keep),
        }
    }

    fn read_segment(&self, path: &Path) -> Result<Vec<MutationRecord>> {
        let bytes = MutationBytes::from(fs::read(path)?);
        WalRecordCodec::decode_records(bytes)
    }

    fn paths(&self) -> Result<Vec<PathBuf>> {
        let mut paths = fs::read_dir(&self.data_dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|path| WalSegmentName::matches(path))
            .collect::<Vec<_>>();
        paths.sort();
        Ok(paths)
    }

    fn next_sequence(&self) -> Result<u64> {
        let max = self
            .paths()?
            .iter()
            .filter_map(|path| WalSegmentName::sequence(path))
            .max();
        Ok(max.map_or(0, |value| value.saturating_add(1)))
    }
}

pub struct SegmentWriter {
    data_dir: PathBuf,
    file: BufWriter<File>,
    compress: bool,
    segment_sequence: u64,
    bytes_written: u64,
    last_entry_size: usize,
    rotation_count: u64,
}

impl SegmentWriter {
    #[cfg(test)]
    pub fn open(data_dir: &Path, compress: bool) -> Result<Self> {
        SegmentStore::new(data_dir).open_writer(compress)
    }

    #[cfg(test)]
    pub fn append(&mut self, record: &MutationRecord, segment_size_bytes: u64) -> Result<()> {
        let encoded = WalRecordCodec::encode_frame(record, self.compress);
        self.append_encoded(&encoded, segment_size_bytes)
    }

    pub fn append_encoded(&mut self, encoded: &[u8], segment_size_bytes: u64) -> Result<()> {
        self.file.write_all(encoded)?;
        self.bytes_written = self.bytes_written.saturating_add(encoded.len() as u64);
        self.last_entry_size = encoded.len();
        match self.bytes_written >= segment_size_bytes {
            true => self.rotate(),
            false => Ok(()),
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }

    pub fn last_entry_size(&self) -> usize {
        self.last_entry_size
    }

    pub fn take_rotation_count(&mut self) -> u64 {
        let count = self.rotation_count;
        self.rotation_count = 0;
        count
    }

    fn rotate(&mut self) -> Result<()> {
        self.file.flush()?;
        self.segment_sequence = self.segment_sequence.saturating_add(1);
        let path = WalSegmentName::path(
            &self.data_dir,
            self.segment_sequence,
            crate::storage::now_millis(),
            self.compress,
        );
        self.file = BufWriter::new(OpenOptions::new().create(true).append(true).open(path)?);
        self.bytes_written = 0;
        self.rotation_count = self.rotation_count.saturating_add(1);
        Ok(())
    }
}

pub(super) struct WalRecordCodec;

struct EncodedFramePayload {
    flags: u8,
    bytes: Vec<u8>,
}

enum WalRecordFormat {
    Framed,
    Legacy,
}

struct WalFrameCursor<'a> {
    bytes: &'a MutationBytes,
    cursor: usize,
}

struct WalFrameHeader {
    start: usize,
    flags: u8,
    payload_len: usize,
}

struct LegacyRecordCursor<'a> {
    owner: &'a MutationBytes,
    cursor: usize,
}

struct LegacyRecordHeader {
    op_byte: u8,
    key_len: usize,
    value_len: usize,
    timestamp_ms: u64,
    expire_raw: i64,
}

struct WalBytes;

impl WalRecordCodec {
    pub(super) fn encode_frame(record: &MutationRecord, compress: bool) -> Vec<u8> {
        let raw = Self::encode_legacy(record);
        let payload = EncodedFramePayload::from_raw(raw, compress);
        let mut bytes =
            Vec::with_capacity(WAL_FRAME_HEADER_LEN + payload.bytes.len() + WAL_CRC_LEN);
        bytes.extend_from_slice(WAL_FRAME_MAGIC);
        bytes.push(payload.flags);
        bytes.extend_from_slice(&(payload.bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload.bytes);
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    }

    pub(super) fn decode_records(bytes: MutationBytes) -> Result<Vec<MutationRecord>> {
        match WalRecordFormat::detect(&bytes) {
            WalRecordFormat::Framed => Self::decode_framed_records(&bytes),
            WalRecordFormat::Legacy => Self::decode_legacy_records(&bytes),
        }
    }

    fn encode_legacy(record: &MutationRecord) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(1 + 4 + 4 + 8 + 8 + record.key.len() + record.value.len() + 4);
        bytes.push(Self::encode_op(&record.op));
        bytes.extend_from_slice(&(record.key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(record.value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&record.timestamp_ms.to_le_bytes());
        bytes.extend_from_slice(
            &(record.expire_at_ms.map_or(-1_i64, |value| value as i64)).to_le_bytes(),
        );
        bytes.extend_from_slice(record.key.as_ref());
        bytes.extend_from_slice(record.value.as_ref());
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn encode_op(op: &MutationOp) -> u8 {
        match op {
            MutationOp::Set => 1,
            MutationOp::Del => 2,
            MutationOp::Expire => 3,
        }
    }

    fn decode_op(value: u8) -> Result<MutationOp> {
        match value {
            1 => Ok(MutationOp::Set),
            2 => Ok(MutationOp::Del),
            3 => Ok(MutationOp::Expire),
            other => Err(ShardCacheError::Persistence(format!(
                "unsupported WAL op code: {other}"
            ))),
        }
    }

    fn decode_framed_records(bytes: &MutationBytes) -> Result<Vec<MutationRecord>> {
        let mut records = Vec::new();
        let mut cursor = WalFrameCursor::new(bytes);
        while let Some(raw) = cursor.next_payload()? {
            records.push(Self::decode_single_legacy_record(&raw)?);
        }
        Ok(records)
    }

    fn decode_legacy_records(bytes: &MutationBytes) -> Result<Vec<MutationRecord>> {
        let mut records = Vec::new();
        let mut cursor = LegacyRecordCursor::new(bytes);
        while let Some(record) = cursor.next_record()? {
            records.push(record);
        }
        Ok(records)
    }

    fn decode_single_legacy_record(bytes: &MutationBytes) -> Result<MutationRecord> {
        let mut cursor = LegacyRecordCursor::new(bytes);
        match cursor.next_record()? {
            Some(record) if cursor.is_exhausted() => Ok(record),
            Some(_) => Err(ShardCacheError::Persistence(
                "WAL frame payload contains trailing bytes".into(),
            )),
            None => Err(ShardCacheError::Persistence(
                "WAL frame payload is truncated".into(),
            )),
        }
    }
}

impl EncodedFramePayload {
    fn from_raw(raw: Vec<u8>, compress: bool) -> Self {
        match compress {
            true => Self::compressed_when_smaller(raw),
            false => Self::plain(raw),
        }
    }

    fn compressed_when_smaller(raw: Vec<u8>) -> Self {
        let compressed = compress_prepend_size(&raw);
        match compressed.len() < raw.len() {
            true => Self {
                flags: WAL_FLAG_COMPRESSED,
                bytes: compressed,
            },
            false => Self::plain(raw),
        }
    }

    fn plain(bytes: Vec<u8>) -> Self {
        Self { flags: 0, bytes }
    }
}

impl WalRecordFormat {
    fn detect(bytes: &MutationBytes) -> Self {
        match bytes.starts_with(WAL_FRAME_MAGIC) {
            true => Self::Framed,
            false => Self::Legacy,
        }
    }
}

impl<'a> WalFrameCursor<'a> {
    fn new(bytes: &'a MutationBytes) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn next_payload(&mut self) -> Result<Option<MutationBytes>> {
        match self.read_header() {
            Some(header) => self.read_payload(header),
            None => Ok(None),
        }
    }

    fn read_header(&mut self) -> Option<WalFrameHeader> {
        let start = self.cursor;
        match self.bytes.len().saturating_sub(self.cursor) < WAL_FRAME_HEADER_LEN {
            true => None,
            false => match &self.bytes[self.cursor..self.cursor + WAL_FRAME_MAGIC.len()]
                == WAL_FRAME_MAGIC
            {
                true => {
                    self.cursor += WAL_FRAME_MAGIC.len();
                    let flags = self.bytes[self.cursor];
                    self.cursor += 1;
                    let payload_len =
                        WalBytes::read_u32_at(self.bytes.as_ref(), self.cursor) as usize;
                    self.cursor += 4;
                    Some(WalFrameHeader {
                        start,
                        flags,
                        payload_len,
                    })
                }
                false => None,
            },
        }
    }

    fn read_payload(&mut self, header: WalFrameHeader) -> Result<Option<MutationBytes>> {
        match self
            .bytes
            .len()
            .saturating_sub(self.cursor)
            .checked_sub(header.payload_len + WAL_CRC_LEN)
        {
            Some(_) => self.read_complete_payload(header),
            None => Ok(None),
        }
    }

    fn read_complete_payload(&mut self, header: WalFrameHeader) -> Result<Option<MutationBytes>> {
        let payload_start = self.cursor;
        let payload_end = payload_start + header.payload_len;
        let payload = self.bytes.slice(payload_start..payload_end);
        self.cursor = payload_end;

        let expected_crc = WalBytes::read_u32_at(self.bytes.as_ref(), self.cursor);
        let actual_crc = crc32fast::hash(&self.bytes[header.start..self.cursor]);
        self.cursor += WAL_CRC_LEN;

        match expected_crc == actual_crc {
            true => Self::decode_payload(header.flags, payload).map(Some),
            false => Ok(None),
        }
    }

    fn decode_payload(flags: u8, payload: MutationBytes) -> Result<MutationBytes> {
        match flags & WAL_FLAG_COMPRESSED != 0 {
            true => decompress_size_prepended(payload.as_ref())
                .map(MutationBytes::from)
                .map_err(|error| {
                    ShardCacheError::Persistence(format!("invalid compressed WAL frame: {error}"))
                }),
            false => Ok(payload),
        }
    }
}

impl<'a> LegacyRecordCursor<'a> {
    fn new(owner: &'a MutationBytes) -> Self {
        Self { owner, cursor: 0 }
    }

    fn next_record(&mut self) -> Result<Option<MutationRecord>> {
        let start = self.cursor;
        match self.read_header() {
            Some(header) => self.read_body(start, header),
            None => Ok(None),
        }
    }

    fn is_exhausted(&self) -> bool {
        self.cursor == self.owner.len()
    }

    fn read_header(&mut self) -> Option<LegacyRecordHeader> {
        let bytes = self.owner.as_ref();
        match bytes.get(self.cursor).copied() {
            Some(op_byte)
                if bytes.len().saturating_sub(self.cursor + 1) >= LEGACY_RECORD_HEADER_TAIL_LEN =>
            {
                self.cursor += 1;
                let key_len = WalBytes::read_u32_at(bytes, self.cursor) as usize;
                self.cursor += 4;
                let value_len = WalBytes::read_u32_at(bytes, self.cursor) as usize;
                self.cursor += 4;
                let timestamp_ms = WalBytes::read_u64_at(bytes, self.cursor);
                self.cursor += 8;
                let expire_raw = WalBytes::read_i64_at(bytes, self.cursor);
                self.cursor += 8;
                Some(LegacyRecordHeader {
                    op_byte,
                    key_len,
                    value_len,
                    timestamp_ms,
                    expire_raw,
                })
            }
            Some(_) | None => None,
        }
    }

    fn read_body(
        &mut self,
        record_start: usize,
        header: LegacyRecordHeader,
    ) -> Result<Option<MutationRecord>> {
        let body_len = header
            .key_len
            .saturating_add(header.value_len)
            .saturating_add(WAL_CRC_LEN);
        match self.owner.len().saturating_sub(self.cursor) < body_len {
            true => Ok(None),
            false => self.read_complete_body(record_start, header),
        }
    }

    fn read_complete_body(
        &mut self,
        record_start: usize,
        header: LegacyRecordHeader,
    ) -> Result<Option<MutationRecord>> {
        let key_start = self.cursor;
        let key_end = key_start + header.key_len;
        let value_start = key_end;
        let value_end = value_start + header.value_len;
        self.cursor = value_end;

        let expected_crc = WalBytes::read_u32_at(self.owner.as_ref(), self.cursor);
        let actual_crc = crc32fast::hash(&self.owner[record_start..self.cursor]);
        self.cursor += WAL_CRC_LEN;

        match expected_crc == actual_crc {
            true => self.build_record(header, key_start, key_end, value_start, value_end),
            false => Ok(None),
        }
    }

    fn build_record(
        &self,
        header: LegacyRecordHeader,
        key_start: usize,
        key_end: usize,
        value_start: usize,
        value_end: usize,
    ) -> Result<Option<MutationRecord>> {
        let op = WalRecordCodec::decode_op(header.op_byte)?;
        Ok(Some(MutationRecord {
            shard_id: 0,
            sequence: 0,
            timestamp_ms: header.timestamp_ms,
            op,
            key: self.owner.slice(key_start..key_end),
            value: self.owner.slice(value_start..value_end),
            expire_at_ms: header.expire_at_ms(),
        }))
    }
}

impl LegacyRecordHeader {
    fn expire_at_ms(&self) -> Option<u64> {
        match self.expire_raw {
            value if value < 0 => None,
            value => Some(value as u64),
        }
    }
}

struct WalSegmentName;

impl WalSegmentName {
    fn path(data_dir: &Path, sequence: u64, timestamp_ms: u64, compress: bool) -> PathBuf {
        if compress {
            data_dir.join(format!(
                "segment-{sequence:020}-{timestamp_ms}.{WAL_FILE_EXT}.lz4"
            ))
        } else {
            data_dir.join(format!(
                "segment-{sequence:020}-{timestamp_ms}.{WAL_FILE_EXT}"
            ))
        }
    }

    fn matches(path: &Path) -> bool {
        match path.file_name().and_then(|value| value.to_str()) {
            Some(name) => {
                name.ends_with(&format!(".{WAL_FILE_EXT}")) || name.ends_with(WAL_COMPRESSED_SUFFIX)
            }
            None => false,
        }
    }

    fn stem(name: &str) -> Option<&str> {
        name.strip_suffix(WAL_COMPRESSED_SUFFIX)
            .or_else(|| name.strip_suffix(&format!(".{WAL_FILE_EXT}")))
    }

    fn sequence(path: &Path) -> Option<u64> {
        let name = path.file_name().and_then(|value| value.to_str())?;
        let stem = Self::stem(name)?;
        let mut parts = stem.split('-');
        match (parts.next(), parts.next(), parts.next()) {
            (Some("segment"), Some(sequence), Some(_timestamp)) => sequence.parse().ok(),
            _ => None,
        }
    }
}

impl WalBytes {
    fn read_u32_at(bytes: &[u8], cursor: usize) -> u32 {
        let mut value = [0; 4];
        value.copy_from_slice(&bytes[cursor..cursor + 4]);
        u32::from_le_bytes(value)
    }

    fn read_u64_at(bytes: &[u8], cursor: usize) -> u64 {
        let mut value = [0; 8];
        value.copy_from_slice(&bytes[cursor..cursor + 8]);
        u64::from_le_bytes(value)
    }

    fn read_i64_at(bytes: &[u8], cursor: usize) -> i64 {
        let mut value = [0; 8];
        value.copy_from_slice(&bytes[cursor..cursor + 8]);
        i64::from_le_bytes(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(value_len: usize) -> MutationRecord {
        MutationRecord {
            shard_id: 0,
            sequence: 0,
            timestamp_ms: 42,
            op: MutationOp::Set,
            key: MutationBytes::from_static(b"alpha"),
            value: MutationBytes::from(vec![b'x'; value_len]),
            expire_at_ms: Some(99),
        }
    }

    #[test]
    fn compressed_frame_round_trips() {
        let record = sample_record(8 * 1024);
        let bytes = MutationBytes::from(WalRecordCodec::encode_frame(&record, true));
        assert!(bytes.starts_with(WAL_FRAME_MAGIC));
        let decoded = WalRecordCodec::decode_records(bytes).expect("decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].key, record.key);
        assert_eq!(decoded[0].value, record.value);
        assert_eq!(decoded[0].timestamp_ms, record.timestamp_ms);
        assert_eq!(decoded[0].expire_at_ms, record.expire_at_ms);
    }

    #[test]
    fn legacy_segments_still_decode() {
        let first = sample_record(128);
        let second = MutationRecord {
            shard_id: 0,
            sequence: 0,
            timestamp_ms: 43,
            op: MutationOp::Del,
            key: MutationBytes::from_static(b"beta"),
            value: MutationBytes::new(),
            expire_at_ms: None,
        };
        let mut bytes = WalRecordCodec::encode_legacy(&first);
        bytes.extend_from_slice(&WalRecordCodec::encode_legacy(&second));
        let decoded = WalRecordCodec::decode_records(MutationBytes::from(bytes)).expect("decode");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].key, first.key);
        assert_eq!(decoded[0].value, first.value);
        assert!(matches!(decoded[1].op, MutationOp::Del));
        assert_eq!(decoded[1].key, second.key);
    }

    #[test]
    fn compressed_segment_uses_lz4_suffix() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let mut writer = SegmentWriter::open(temp_dir.path(), true).expect("writer");
        writer
            .append(&sample_record(8 * 1024), 64 * 1024 * 1024)
            .expect("append");
        writer.flush().expect("flush");
        let paths = SegmentStore::new(temp_dir.path()).paths().expect("paths");
        assert_eq!(paths.len(), 1);
        let name = paths[0]
            .file_name()
            .and_then(|value| value.to_str())
            .expect("name");
        assert!(name.ends_with(WAL_COMPRESSED_SUFFIX));
    }
}
