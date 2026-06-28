use std::fs;
use std::path::{Path, PathBuf};

use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use zerocopy::FromBytes;
use zerocopy::byteorder::{LE, U32, U64};

use crate::storage::{Bytes, StoredEntry, hash_key};
use crate::{Result, ShardCacheError};

const SNAPSHOT_MAGIC: &[u8; 8] = b"FCSNAP1\0";
const SNAPSHOT_VERSION: u32 = 1;
const SNAPSHOT_HEADER_LEN: usize = 8 + 4 + 8 + 8;
const SNAPSHOT_ENTRY_HEADER_LEN: usize = 4 + 4 + 8;
const SNAPSHOT_COMPRESSED_EXT: &str = "lz4";

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

    fn latest_path(&self) -> Result<Option<PathBuf>> {
        let mut snapshots = Vec::new();
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if SnapshotName::matches(&path) {
                snapshots.push(path);
            }
        }
        snapshots.sort();
        Ok(snapshots.pop())
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
        let key_len = Self::encoded_len(entry.key.len(), "snapshot key is too large")?;
        let value_len = Self::encoded_len(entry.value.len(), "snapshot value is too large")?;
        body.extend_from_slice(&key_len.to_le_bytes());
        body.extend_from_slice(&value_len.to_le_bytes());
        body.extend_from_slice(&entry.expire_at_ms.unwrap_or(u64::MAX).to_le_bytes());
        body.extend_from_slice(entry.key.as_ref());
        body.extend_from_slice(entry.value.as_ref());
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

    fn decode_validated(raw: &[u8]) -> Result<(u64, Vec<StoredEntry>)> {
        let mut cursor = SNAPSHOT_MAGIC.len();
        match Self::read_u32(raw, &mut cursor, "snapshot version")? {
            SNAPSHOT_VERSION => Self::decode_body(raw, &mut cursor),
            version => Err(ShardCacheError::Persistence(format!(
                "unsupported snapshot version: {version}"
            ))),
        }
    }

    fn decode_body(raw: &[u8], cursor: &mut usize) -> Result<(u64, Vec<StoredEntry>)> {
        let timestamp_ms = Self::read_u64(raw, cursor, "snapshot timestamp")?;
        let entry_count = usize::try_from(Self::read_u64(raw, cursor, "snapshot entry count")?)
            .map_err(|_| {
                ShardCacheError::Persistence("snapshot entry count is too large".into())
            })?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(Self::decode_entry(raw, cursor)?);
        }
        Ok((timestamp_ms, entries))
    }

    fn decode_entry(raw: &[u8], cursor: &mut usize) -> Result<StoredEntry> {
        if raw.len().saturating_sub(*cursor) < SNAPSHOT_ENTRY_HEADER_LEN {
            return Err(ShardCacheError::Persistence(
                "snapshot entry header is truncated".into(),
            ));
        }

        let key_len = Self::read_u32(raw, cursor, "snapshot key length")? as usize;
        let value_len = Self::read_u32(raw, cursor, "snapshot value length")? as usize;
        let expire_raw = Self::read_u64(raw, cursor, "snapshot expiration")?;
        let body_len = key_len.saturating_add(value_len);
        if raw.len().saturating_sub(*cursor) < body_len {
            return Err(ShardCacheError::Persistence(
                "snapshot entry body is truncated".into(),
            ));
        }

        let key = raw[*cursor..*cursor + key_len].to_vec();
        *cursor += key_len;
        let value = raw[*cursor..*cursor + value_len].to_vec();
        *cursor += value_len;
        Ok(StoredEntry {
            key: Bytes::from(key),
            value: Bytes::from(value),
            expire_at_ms: if expire_raw == u64::MAX {
                None
            } else {
                Some(expire_raw)
            },
        })
    }

    fn read_u32(raw: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
        let (value, _) = U32::<LE>::read_from_prefix(&raw[*cursor..])
            .map_err(|_| ShardCacheError::Persistence(format!("{field} is truncated")))?;
        *cursor += 4;
        Ok(value.get())
    }

    fn read_u64(raw: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
        let (value, _) = U64::<LE>::read_from_prefix(&raw[*cursor..])
            .map_err(|_| ShardCacheError::Persistence(format!("{field} is truncated")))?;
        *cursor += 8;
        Ok(value.get())
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

    fn matches(path: &Path) -> bool {
        path.file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| name.starts_with("snapshot-"))
    }
}
