use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use crate::storage::{Bytes, StoredEntry, hash_key};
use crate::{Result, ShardCacheError};

const SNAPSHOT_MAGIC: &[u8; 8] = b"FCSNAP1\0";
const SNAPSHOT_VERSION_V1: u32 = 1;
const SNAPSHOT_VERSION: u32 = 2;
const SNAPSHOT_HEADER_LEN: usize = 8 + 4 + 8 + 8;
const SNAPSHOT_ENTRY_HEADER_V1_LEN: usize = 4 + 4 + 8;
const SNAPSHOT_ENTRY_HEADER_LEN: usize = 4 + 4 + 4 + 8;
const GOVERNANCE_NONE: u32 = u32::MAX;
const SNAPSHOT_COMPRESSED_EXT: &str = "lz4";
const SNAPSHOT_FRAMED_COMPRESSED_EXT: &str = "lz4f";
static SNAPSHOT_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
    /// Path of the snapshot file that was loaded.
    pub path: PathBuf,
    /// Snapshot timestamp captured when the file was written.
    pub timestamp_ms: u64,
    /// Live cache entries encoded in the snapshot.
    pub entries: Vec<StoredEntry>,
}

/// Compression mode used when writing a snapshot file.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SnapshotCompression {
    /// Store the snapshot body directly.
    None,
    /// Store the snapshot body with lz4 size-prepended compression.
    Lz4,
}

impl SnapshotCompression {
    pub const fn from_enabled(enabled: bool) -> Self {
        if enabled { Self::Lz4 } else { Self::None }
    }

    fn file_name(self, timestamp_ms: u64) -> String {
        match self {
            Self::None => format!("snapshot-{timestamp_ms}.bin"),
            Self::Lz4 => format!("snapshot-{timestamp_ms}.bin.lz4"),
        }
    }

    fn streaming_file_name(self, timestamp_ms: u64) -> String {
        match self {
            Self::None => self.file_name(timestamp_ms),
            Self::Lz4 => format!("snapshot-{timestamp_ms}.bin.lz4f"),
        }
    }

    fn encode(self, body: Vec<u8>) -> Vec<u8> {
        match self {
            Self::None => body,
            Self::Lz4 => compress_prepend_size(&body),
        }
    }

    fn decode_path(path: &Path, bytes: Vec<u8>) -> Result<Vec<u8>> {
        if path
            .extension()
            .is_some_and(|ext| ext == SNAPSHOT_COMPRESSED_EXT)
        {
            decompress_size_prepended(&bytes).map_err(|error| {
                ShardCacheError::Persistence(format!("invalid compressed snapshot: {error}"))
            })
        } else if path
            .extension()
            .is_some_and(|ext| ext == SNAPSHOT_FRAMED_COMPRESSED_EXT)
        {
            let mut decoded = Vec::new();
            FrameDecoder::new(bytes.as_slice())
                .read_to_end(&mut decoded)
                .map_err(|error| {
                    ShardCacheError::Persistence(format!(
                        "invalid framed compressed snapshot: {error}"
                    ))
                })?;
            Ok(decoded)
        } else {
            Ok(bytes)
        }
    }
}

/// Filesystem-backed snapshot repository.
///
/// `SnapshotStore` owns the directory scanning and file IO concerns. The
/// binary snapshot format stays isolated in `SnapshotCodec`, which makes it
/// easier to evolve the on-disk representation without spreading parsing logic
/// through persistence recovery.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    data_dir: PathBuf,
}

impl SnapshotStore {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
        }
    }

    fn write(
        &self,
        entries: &[StoredEntry],
        timestamp_ms: u64,
        compression: SnapshotCompression,
    ) -> Result<PathBuf> {
        fs::create_dir_all(&self.data_dir)?;

        let body = SnapshotCodec::encode(entries, timestamp_ms)?;
        let bytes = compression.encode(body);
        let path = SnapshotName::path(&self.data_dir, timestamp_ms, compression);
        fs::write(&path, bytes)?;
        Ok(path)
    }

    /// Atomically writes entries supplied one at a time with bounded memory.
    ///
    /// LZ4 snapshots use the framed stream format. Existing size-prepended
    /// `.lz4` snapshots remain readable for backward compatibility.
    pub fn write_snapshot_streaming<F>(
        &self,
        timestamp_ms: u64,
        compression: SnapshotCompression,
        produce: F,
    ) -> Result<PathBuf>
    where
        F: FnOnce(&mut dyn FnMut(StoredEntry) -> Result<()>) -> Result<()>,
    {
        fs::create_dir_all(&self.data_dir)?;
        let sequence = SNAPSHOT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp_stem = format!(".snapshot-{timestamp_ms}-{}-{sequence}", std::process::id());
        let raw_temp = self.data_dir.join(format!("{temp_stem}.raw.tmp"));
        let compressed_temp = self.data_dir.join(format!("{temp_stem}.lz4f.tmp"));
        let result = self.write_streaming_staged(
            &raw_temp,
            &compressed_temp,
            timestamp_ms,
            compression,
            produce,
        );
        if result.is_err() {
            let _ = fs::remove_file(&raw_temp);
            let _ = fs::remove_file(&compressed_temp);
        }
        result
    }

    fn write_streaming_staged<F>(
        &self,
        raw_temp: &Path,
        compressed_temp: &Path,
        timestamp_ms: u64,
        compression: SnapshotCompression,
        produce: F,
    ) -> Result<PathBuf>
    where
        F: FnOnce(&mut dyn FnMut(StoredEntry) -> Result<()>) -> Result<()>,
    {
        let raw_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(raw_temp)?;
        let mut raw = BufWriter::new(raw_file);
        SnapshotCodec::write_header(&mut raw, timestamp_ms, 0)?;
        let mut count = 0u64;
        produce(&mut |entry| {
            SnapshotCodec::write_entry(&mut raw, &entry)?;
            count = count.checked_add(1).ok_or_else(|| {
                ShardCacheError::Persistence("snapshot entry count overflow".into())
            })?;
            Ok(())
        })?;
        raw.flush()?;
        let mut raw_file = raw.into_inner().map_err(|error| error.into_error())?;
        raw_file.seek(SeekFrom::Start((SNAPSHOT_MAGIC.len() + 4 + 8) as u64))?;
        raw_file.write_all(&count.to_le_bytes())?;
        raw_file.sync_all()?;

        let final_path = SnapshotName::streaming_path(&self.data_dir, timestamp_ms, compression);
        match compression {
            SnapshotCompression::None => fs::rename(raw_temp, &final_path)?,
            SnapshotCompression::Lz4 => {
                raw_file.seek(SeekFrom::Start(0))?;
                let compressed_file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(compressed_temp)?;
                let mut encoder = FrameEncoder::new(BufWriter::new(compressed_file));
                std::io::copy(&mut BufReader::new(raw_file), &mut encoder)?;
                let mut compressed = encoder.finish().map_err(|error| {
                    ShardCacheError::Persistence(format!(
                        "could not finish compressed snapshot: {error}"
                    ))
                })?;
                compressed.flush()?;
                compressed
                    .into_inner()
                    .map_err(|error| error.into_error())?
                    .sync_all()?;
                fs::rename(compressed_temp, &final_path)?;
                fs::remove_file(raw_temp)?;
            }
        }
        Self::sync_directory(&self.data_dir)?;
        Ok(final_path)
    }

    #[cfg(unix)]
    fn sync_directory(path: &Path) -> Result<()> {
        File::open(path)?.sync_all()?;
        Ok(())
    }

    #[cfg(not(unix))]
    fn sync_directory(_path: &Path) -> Result<()> {
        Ok(())
    }

    fn load_latest(&self) -> Result<Option<LoadedSnapshot>> {
        let Some(path) = self.latest_path()? else {
            return Ok(None);
        };
        let bytes = fs::read(&path)?;
        let raw = SnapshotCompression::decode_path(&path, bytes)?;
        let (timestamp_ms, entries) = SnapshotCodec::decode(&raw)?;
        Ok(Some(LoadedSnapshot {
            path,
            timestamp_ms,
            entries,
        }))
    }

    /// Visits the latest snapshot one entry at a time.
    ///
    /// New framed LZ4 and uncompressed snapshots are decoded with bounded
    /// memory. Legacy size-prepended LZ4 snapshots use the compatibility
    /// decoder and should be rewritten after upgrade.
    pub fn visit_latest_snapshot(
        &self,
        mut visit: impl FnMut(StoredEntry) -> Result<()>,
    ) -> Result<Option<(PathBuf, u64)>> {
        let Some(path) = self.latest_path()? else {
            return Ok(None);
        };
        let timestamp_ms = if path
            .extension()
            .is_some_and(|ext| ext == SNAPSHOT_FRAMED_COMPRESSED_EXT)
        {
            let file = File::open(&path)?;
            SnapshotCodec::decode_stream(FrameDecoder::new(BufReader::new(file)), &mut visit)?
        } else if path
            .extension()
            .is_some_and(|ext| ext == SNAPSHOT_COMPRESSED_EXT)
        {
            let bytes = fs::read(&path)?;
            let raw = SnapshotCompression::decode_path(&path, bytes)?;
            let (timestamp_ms, entries) = SnapshotCodec::decode(&raw)?;
            for entry in entries {
                visit(entry)?;
            }
            timestamp_ms
        } else {
            SnapshotCodec::decode_stream(BufReader::new(File::open(&path)?), &mut visit)?
        };
        Ok(Some((path, timestamp_ms)))
    }

    fn latest_path(&self) -> Result<Option<PathBuf>> {
        let mut snapshots = Vec::new();
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if SnapshotName::matches(&path) {
                snapshots.push(path);
            }
        }
        Ok(snapshots.into_iter().max_by_key(|path| {
            (
                SnapshotName::timestamp(path).unwrap_or(0),
                usize::from(
                    path.extension()
                        .is_some_and(|ext| ext == SNAPSHOT_FRAMED_COMPRESSED_EXT),
                ),
            )
        }))
    }
}

/// Snapshot persistence behavior used by WAL recovery and embedded callers.
pub trait SnapshotRepository {
    /// Writes `entries` into a timestamped snapshot file and returns its path.
    fn write_snapshot(
        &self,
        entries: &[StoredEntry],
        timestamp_ms: u64,
        compression: SnapshotCompression,
    ) -> Result<PathBuf>;

    /// Loads the newest snapshot file in the repository, if one exists.
    fn load_latest_snapshot(&self) -> Result<Option<LoadedSnapshot>>;
}

impl SnapshotRepository for SnapshotStore {
    fn write_snapshot(
        &self,
        entries: &[StoredEntry],
        timestamp_ms: u64,
        compression: SnapshotCompression,
    ) -> Result<PathBuf> {
        self.write(entries, timestamp_ms, compression)
    }

    fn load_latest_snapshot(&self) -> Result<Option<LoadedSnapshot>> {
        self.load_latest()
    }
}

struct SnapshotCodec;

impl SnapshotCodec {
    fn encode(entries: &[StoredEntry], timestamp_ms: u64) -> Result<Vec<u8>> {
        let mut entries = entries.to_vec();
        entries.sort_by_key(|entry| hash_key(entry.key.as_ref()));

        let mut body = Vec::with_capacity(
            SNAPSHOT_HEADER_LEN + entries.len().saturating_mul(SNAPSHOT_ENTRY_HEADER_LEN),
        );
        body.extend_from_slice(SNAPSHOT_MAGIC);
        body.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        body.extend_from_slice(&timestamp_ms.to_le_bytes());
        body.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for entry in entries {
            Self::encode_entry(&mut body, &entry)?;
        }
        Ok(body)
    }

    fn encode_entry(body: &mut Vec<u8>, entry: &StoredEntry) -> Result<()> {
        Self::write_entry(body, entry)
    }

    fn write_header(output: &mut impl Write, timestamp_ms: u64, entry_count: u64) -> Result<()> {
        output.write_all(SNAPSHOT_MAGIC)?;
        output.write_all(&SNAPSHOT_VERSION.to_le_bytes())?;
        output.write_all(&timestamp_ms.to_le_bytes())?;
        output.write_all(&entry_count.to_le_bytes())?;
        Ok(())
    }

    fn write_entry(output: &mut impl Write, entry: &StoredEntry) -> Result<()> {
        let key_len = Self::encoded_len(entry.key.len(), "snapshot key is too large")?;
        let value_len = Self::encoded_len(entry.value.len(), "snapshot value is too large")?;
        let governance_len = entry
            .governance
            .as_ref()
            .map(|metadata| Self::encoded_len(metadata.len(), "snapshot governance is too large"))
            .transpose()?
            .unwrap_or(GOVERNANCE_NONE);
        output.write_all(&key_len.to_le_bytes())?;
        output.write_all(&value_len.to_le_bytes())?;
        output.write_all(&governance_len.to_le_bytes())?;
        output.write_all(&entry.expire_at_ms.unwrap_or(u64::MAX).to_le_bytes())?;
        output.write_all(entry.key.as_ref())?;
        output.write_all(entry.value.as_ref())?;
        if let Some(governance) = &entry.governance {
            output.write_all(governance.as_ref())?;
        }
        Ok(())
    }

    fn encoded_len(len: usize, message: &'static str) -> Result<u32> {
        u32::try_from(len).map_err(|_| ShardCacheError::Persistence(message.into()))
    }

    fn decode(raw: &[u8]) -> Result<(u64, Vec<StoredEntry>)> {
        match raw {
            bytes if bytes.len() < SNAPSHOT_HEADER_LEN => Err(ShardCacheError::Persistence(
                "snapshot header is truncated".into(),
            )),
            bytes if !bytes.starts_with(SNAPSHOT_MAGIC) => Err(ShardCacheError::Persistence(
                "snapshot magic mismatch".into(),
            )),
            bytes => Self::decode_validated(bytes),
        }
    }

    fn decode_stream(
        mut input: impl Read,
        visit: &mut impl FnMut(StoredEntry) -> Result<()>,
    ) -> Result<u64> {
        let mut header = [0u8; SNAPSHOT_HEADER_LEN];
        input.read_exact(&mut header).map_err(|error| {
            ShardCacheError::Persistence(format!("could not read snapshot header: {error}"))
        })?;
        if !header.starts_with(SNAPSHOT_MAGIC) {
            return Err(ShardCacheError::Persistence(
                "snapshot magic mismatch".into(),
            ));
        }
        let version = u32::from_le_bytes(header[8..12].try_into().expect("fixed version"));
        if !matches!(version, SNAPSHOT_VERSION_V1 | SNAPSHOT_VERSION) {
            return Err(ShardCacheError::Persistence(format!(
                "unsupported snapshot version: {version}"
            )));
        }
        let timestamp_ms = u64::from_le_bytes(header[12..20].try_into().expect("fixed timestamp"));
        let entry_count = u64::from_le_bytes(header[20..28].try_into().expect("fixed entry count"));
        for _ in 0..entry_count {
            visit(Self::decode_stream_entry(&mut input, version)?)?;
        }
        let mut trailing = [0u8; 1];
        if input.read(&mut trailing)? != 0 {
            return Err(ShardCacheError::Persistence(
                "snapshot has trailing bytes".into(),
            ));
        }
        Ok(timestamp_ms)
    }

    fn decode_stream_entry(input: &mut impl Read, version: u32) -> Result<StoredEntry> {
        let header_len = if version == SNAPSHOT_VERSION_V1 {
            SNAPSHOT_ENTRY_HEADER_V1_LEN
        } else {
            SNAPSHOT_ENTRY_HEADER_LEN
        };
        let mut header = [0u8; SNAPSHOT_ENTRY_HEADER_LEN];
        input
            .read_exact(&mut header[..header_len])
            .map_err(|error| {
                ShardCacheError::Persistence(format!(
                    "could not read snapshot entry header: {error}"
                ))
            })?;
        let key_len = u32::from_le_bytes(header[0..4].try_into().expect("fixed key length"));
        let value_len = u32::from_le_bytes(header[4..8].try_into().expect("fixed value length"));
        let (governance_len, expire_offset) = if version == SNAPSHOT_VERSION_V1 {
            (GOVERNANCE_NONE, 8)
        } else {
            (
                u32::from_le_bytes(header[8..12].try_into().expect("fixed governance length")),
                12,
            )
        };
        let expire_raw = u64::from_le_bytes(
            header[expire_offset..expire_offset + 8]
                .try_into()
                .expect("fixed expiration"),
        );
        let key = Self::read_stream_bytes(input, key_len as usize, "key")?;
        let value = Self::read_stream_bytes(input, value_len as usize, "value")?;
        let governance = (governance_len != GOVERNANCE_NONE)
            .then(|| Self::read_stream_bytes(input, governance_len as usize, "governance"))
            .transpose()?;
        Ok(StoredEntry {
            key,
            value,
            expire_at_ms: (expire_raw != u64::MAX).then_some(expire_raw),
            governance,
        })
    }

    fn read_stream_bytes(input: &mut impl Read, len: usize, field: &str) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| {
            ShardCacheError::Persistence(format!(
                "snapshot {field} allocation of {len} bytes failed"
            ))
        })?;
        bytes.resize(len, 0);
        input.read_exact(&mut bytes).map_err(|error| {
            ShardCacheError::Persistence(format!("could not read snapshot {field}: {error}"))
        })?;
        Ok(bytes)
    }

    fn decode_validated(raw: &[u8]) -> Result<(u64, Vec<StoredEntry>)> {
        let mut cursor = SNAPSHOT_MAGIC.len();
        match Self::read_u32(raw, &mut cursor, "snapshot version")? {
            version @ (SNAPSHOT_VERSION_V1 | SNAPSHOT_VERSION) => {
                Self::decode_body(raw, &mut cursor, version)
            }
            version => Err(ShardCacheError::Persistence(format!(
                "unsupported snapshot version: {version}"
            ))),
        }
    }

    fn decode_body(
        raw: &[u8],
        cursor: &mut usize,
        version: u32,
    ) -> Result<(u64, Vec<StoredEntry>)> {
        let timestamp_ms = Self::read_u64(raw, cursor, "snapshot timestamp")?;
        let entry_count = usize::try_from(Self::read_u64(raw, cursor, "snapshot entry count")?)
            .map_err(|_| {
                ShardCacheError::Persistence("snapshot entry count is too large".into())
            })?;
        if entry_count > raw.len().saturating_sub(*cursor) / SNAPSHOT_ENTRY_HEADER_V1_LEN {
            return Err(ShardCacheError::Persistence(
                "snapshot entry count exceeds the remaining body".into(),
            ));
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(entry_count)
            .map_err(|_| ShardCacheError::Persistence("snapshot entry allocation failed".into()))?;
        for _ in 0..entry_count {
            entries.push(Self::decode_entry(raw, cursor, version)?);
        }
        Ok((timestamp_ms, entries))
    }

    fn decode_entry(raw: &[u8], cursor: &mut usize, version: u32) -> Result<StoredEntry> {
        let header_len = if version == SNAPSHOT_VERSION_V1 {
            SNAPSHOT_ENTRY_HEADER_V1_LEN
        } else {
            SNAPSHOT_ENTRY_HEADER_LEN
        };
        if raw.len().saturating_sub(*cursor) < header_len {
            return Err(ShardCacheError::Persistence(
                "snapshot entry header is truncated".into(),
            ));
        }

        let key_len = Self::read_u32(raw, cursor, "snapshot key length")? as usize;
        let value_len = Self::read_u32(raw, cursor, "snapshot value length")? as usize;
        let governance_len = if version == SNAPSHOT_VERSION_V1 {
            GOVERNANCE_NONE
        } else {
            Self::read_u32(raw, cursor, "snapshot governance length")?
        };
        let expire_raw = Self::read_u64(raw, cursor, "snapshot expiration")?;
        let governance_body_len = if governance_len == GOVERNANCE_NONE {
            0
        } else {
            governance_len as usize
        };
        let body_len = key_len
            .saturating_add(value_len)
            .saturating_add(governance_body_len);
        if raw.len().saturating_sub(*cursor) < body_len {
            return Err(ShardCacheError::Persistence(
                "snapshot entry body is truncated".into(),
            ));
        }

        let key = raw[*cursor..*cursor + key_len].to_vec();
        *cursor += key_len;
        let value = raw[*cursor..*cursor + value_len].to_vec();
        *cursor += value_len;
        let governance = if governance_len == GOVERNANCE_NONE {
            None
        } else {
            let governance = raw[*cursor..*cursor + governance_body_len].to_vec();
            *cursor += governance_body_len;
            Some(governance)
        };
        Ok(StoredEntry {
            key: Bytes::from(key),
            value: Bytes::from(value),
            expire_at_ms: if expire_raw == u64::MAX {
                None
            } else {
                Some(expire_raw)
            },
            governance,
        })
    }

    fn read_u32(raw: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
        let bytes = Self::read_exact(raw, cursor, 4, field)?;
        let mut value = [0; 4];
        value.copy_from_slice(bytes);
        Ok(u32::from_le_bytes(value))
    }

    fn read_u64(raw: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
        let bytes = Self::read_exact(raw, cursor, 8, field)?;
        let mut value = [0; 8];
        value.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(value))
    }

    fn read_exact<'a>(
        raw: &'a [u8],
        cursor: &mut usize,
        len: usize,
        field: &str,
    ) -> Result<&'a [u8]> {
        if raw.len().saturating_sub(*cursor) < len {
            return Err(ShardCacheError::Persistence(format!(
                "{field} is truncated"
            )));
        }
        let bytes = &raw[*cursor..*cursor + len];
        *cursor += len;
        Ok(bytes)
    }
}

struct SnapshotName;

impl SnapshotName {
    fn path(data_dir: &Path, timestamp_ms: u64, compression: SnapshotCompression) -> PathBuf {
        data_dir.join(compression.file_name(timestamp_ms))
    }

    fn streaming_path(
        data_dir: &Path,
        timestamp_ms: u64,
        compression: SnapshotCompression,
    ) -> PathBuf {
        data_dir.join(compression.streaming_file_name(timestamp_ms))
    }

    fn matches(path: &Path) -> bool {
        Self::timestamp(path).is_some()
    }

    fn timestamp(path: &Path) -> Option<u64> {
        path.file_name()?
            .to_str()?
            .strip_prefix("snapshot-")?
            .split('.')
            .next()?
            .parse()
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries() -> Vec<StoredEntry> {
        vec![
            StoredEntry {
                key: b"alpha".to_vec(),
                value: vec![1; 128],
                expire_at_ms: None,
                governance: None,
            },
            StoredEntry {
                key: b"beta".to_vec(),
                value: vec![2; 256],
                expire_at_ms: Some(42),
                governance: Some(b"policy".to_vec()),
            },
        ]
    }

    fn v1_snapshot(entries: &[StoredEntry], timestamp_ms: u64) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(SNAPSHOT_MAGIC);
        raw.extend_from_slice(&SNAPSHOT_VERSION_V1.to_le_bytes());
        raw.extend_from_slice(&timestamp_ms.to_le_bytes());
        raw.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for entry in entries {
            assert!(entry.governance.is_none());
            raw.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
            raw.extend_from_slice(&(entry.value.len() as u32).to_le_bytes());
            raw.extend_from_slice(&entry.expire_at_ms.unwrap_or(u64::MAX).to_le_bytes());
            raw.extend_from_slice(&entry.key);
            raw.extend_from_slice(&entry.value);
        }
        raw
    }

    #[test]
    fn streaming_snapshots_round_trip_uncompressed_and_framed_lz4() {
        for compression in [SnapshotCompression::None, SnapshotCompression::Lz4] {
            let directory = tempfile::tempdir().unwrap();
            let snapshots = SnapshotStore::new(directory.path());
            let expected = entries();
            snapshots
                .write_snapshot_streaming(7, compression, |sink| {
                    for entry in expected.clone() {
                        sink(entry)?;
                    }
                    Ok(())
                })
                .unwrap();

            let loaded = snapshots.load_latest_snapshot().unwrap().unwrap();
            assert_eq!(loaded.timestamp_ms, 7);
            assert_eq!(loaded.entries, expected);
            let mut visited = Vec::new();
            let (_, timestamp_ms) = snapshots
                .visit_latest_snapshot(|entry| {
                    visited.push(entry);
                    Ok(())
                })
                .unwrap()
                .unwrap();
            assert_eq!(timestamp_ms, 7);
            assert_eq!(visited, expected);
        }
    }

    #[test]
    fn version_one_snapshots_remain_readable() {
        let expected = vec![StoredEntry {
            key: b"legacy".to_vec(),
            value: b"value".to_vec(),
            expire_at_ms: Some(42),
            governance: None,
        }];
        let raw = v1_snapshot(&expected, 7);

        let (timestamp_ms, decoded) = SnapshotCodec::decode(&raw).unwrap();
        assert_eq!(timestamp_ms, 7);
        assert_eq!(decoded, expected);

        let mut streamed = Vec::new();
        let timestamp_ms = SnapshotCodec::decode_stream(&raw[..], &mut |entry| {
            streamed.push(entry);
            Ok(())
        })
        .unwrap();
        assert_eq!(timestamp_ms, 7);
        assert_eq!(streamed, expected);
    }

    #[test]
    fn failed_streaming_snapshot_leaves_no_discoverable_partial_file() {
        let directory = tempfile::tempdir().unwrap();
        let snapshots = SnapshotStore::new(directory.path());
        let result = snapshots.write_snapshot_streaming(9, SnapshotCompression::Lz4, |_sink| {
            Err(ShardCacheError::Persistence("injected failure".into()))
        });

        assert!(result.is_err());
        assert!(snapshots.load_latest_snapshot().unwrap().is_none());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn latest_snapshot_uses_numeric_timestamp_order() {
        let directory = tempfile::tempdir().unwrap();
        let snapshots = SnapshotStore::new(directory.path());
        for timestamp in [9, 10] {
            snapshots
                .write_snapshot_streaming(timestamp, SnapshotCompression::None, |_sink| Ok(()))
                .unwrap();
        }

        assert_eq!(
            snapshots
                .load_latest_snapshot()
                .unwrap()
                .unwrap()
                .timestamp_ms,
            10
        );
    }
}
