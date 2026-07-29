//! WAL and snapshot persistence support.
//!
//! The server uses [`PersistenceRuntime`] to append mutation records and write
//! periodic snapshots. Embedded applications can use [`SnapshotStore`] through
//! [`SnapshotRepository`] when they want to control persistence orchestration
//! themselves.

mod recovery;
mod runtime;
mod snapshot;
mod stats;
mod tcp_export;
mod wal;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use parking_lot::Mutex;

use crate::config::PersistenceConfig;
#[cfg(feature = "telemetry")]
use crate::storage::{CacheTelemetry, CacheTelemetryHandle};
use crate::storage::{MutationBytes, MutationRecord, StoredEntry, WalStatsSnapshot};
use crate::{Result, ShardCacheError};

use recovery::{PersistenceDataDir, RecoveryLoader};
use runtime::{PersistenceStartup, StartedPersistenceRuntime, StopSignal};
pub use snapshot::{LoadedSnapshot, SnapshotCompression, SnapshotRepository, SnapshotStore};
use stats::WalStats;

/// Immutable WAL wire frame shared across disk append and TCP export paths.
///
/// This matches the byte owner used by `bytes-handoff`, so retry and fanout
/// paths can pass the encoded frame through without rebuilding it.
pub type WalFrameBytes = bytes::Bytes;

#[derive(Debug, Clone)]
pub struct RecoveryState {
    pub entries: Vec<StoredEntry>,
    pub snapshot_timestamp_ms: u64,
}

#[derive(Debug)]
pub struct WalAppender {
    sender: Sender<WalBlock>,
    pending: Vec<MutationRecord>,
    pending_bytes: usize,
    max_records: usize,
    max_bytes: usize,
    flush_interval: Duration,
    last_publish: Instant,
}

#[derive(Debug)]
struct WalBlock {
    records: Vec<MutationRecord>,
}

pub struct PersistenceRuntime {
    config: PersistenceConfig,
    appenders: Vec<Mutex<Option<WalAppender>>>,
    stats: Arc<WalStats>,
    #[cfg(feature = "telemetry")]
    _metrics_owner: Option<Arc<CacheTelemetry>>,
    stop_tx: Option<Sender<()>>,
    join_handle: Mutex<Option<JoinHandle<Result<()>>>>,
    tcp_export_stop_tx: Option<Sender<()>>,
    tcp_export_join_handle: Mutex<Option<JoinHandle<()>>>,
}

impl PersistenceRuntime {
    pub fn start(shard_count: usize, config: PersistenceConfig) -> Result<Self> {
        #[cfg(feature = "telemetry")]
        {
            Self::start_with_metrics(shard_count, config, None)
        }
        #[cfg(not(feature = "telemetry"))]
        {
            Self::start_inner(shard_count, config, ())
        }
    }

    #[cfg(feature = "telemetry")]
    pub fn start_with_metrics(
        shard_count: usize,
        config: PersistenceConfig,
        metrics: Option<Arc<CacheTelemetry>>,
    ) -> Result<Self> {
        Self::start_inner(shard_count, config, metrics)
    }

    #[cfg(feature = "telemetry")]
    fn start_inner(
        shard_count: usize,
        config: PersistenceConfig,
        metrics: Option<Arc<CacheTelemetry>>,
    ) -> Result<Self> {
        Self::start_impl(shard_count, config, metrics)
    }

    #[cfg(not(feature = "telemetry"))]
    fn start_inner(shard_count: usize, config: PersistenceConfig, _unit: ()) -> Result<Self> {
        Self::start_impl(shard_count, config)
    }

    #[cfg(feature = "telemetry")]
    fn start_impl(
        shard_count: usize,
        config: PersistenceConfig,
        metrics: Option<Arc<CacheTelemetry>>,
    ) -> Result<Self> {
        match config.enabled {
            true => Self::start_enabled(shard_count, config, metrics),
            false => Ok(Self::disabled(config, None)),
        }
    }

    #[cfg(not(feature = "telemetry"))]
    fn start_impl(shard_count: usize, config: PersistenceConfig) -> Result<Self> {
        match config.enabled {
            true => Self::start_enabled(shard_count, config),
            false => Ok(Self::disabled(config)),
        }
    }

    #[cfg(feature = "telemetry")]
    fn disabled(config: PersistenceConfig, metrics: Option<Arc<CacheTelemetry>>) -> Self {
        Self {
            config,
            appenders: Vec::new(),
            stats: Arc::new(WalStats::disabled()),
            _metrics_owner: metrics,
            stop_tx: None,
            join_handle: Mutex::new(None),
            tcp_export_stop_tx: None,
            tcp_export_join_handle: Mutex::new(None),
        }
    }

    #[cfg(not(feature = "telemetry"))]
    fn disabled(config: PersistenceConfig) -> Self {
        Self {
            config,
            appenders: Vec::new(),
            stats: Arc::new(WalStats::disabled()),
            stop_tx: None,
            join_handle: Mutex::new(None),
            tcp_export_stop_tx: None,
            tcp_export_join_handle: Mutex::new(None),
        }
    }

    #[cfg(feature = "telemetry")]
    fn start_enabled(
        shard_count: usize,
        config: PersistenceConfig,
        metrics: Option<Arc<CacheTelemetry>>,
    ) -> Result<Self> {
        let writer_metrics = metrics.as_ref().map(CacheTelemetryHandle::from_arc);
        let runtime = Self::start_enabled_parts(shard_count, &config, writer_metrics)?;
        Ok(Self {
            config,
            appenders: runtime
                .appenders
                .into_iter()
                .map(|appender| Mutex::new(Some(appender)))
                .collect(),
            stats: runtime.stats,
            _metrics_owner: metrics,
            stop_tx: Some(runtime.stop_tx),
            join_handle: Mutex::new(Some(runtime.join_handle)),
            tcp_export_stop_tx: runtime.tcp_export.stop_tx,
            tcp_export_join_handle: Mutex::new(runtime.tcp_export.join_handle),
        })
    }

    #[cfg(not(feature = "telemetry"))]
    fn start_enabled(shard_count: usize, config: PersistenceConfig) -> Result<Self> {
        let runtime = Self::start_enabled_parts(shard_count, &config)?;
        Ok(Self {
            config,
            appenders: runtime
                .appenders
                .into_iter()
                .map(|appender| Mutex::new(Some(appender)))
                .collect(),
            stats: runtime.stats,
            stop_tx: Some(runtime.stop_tx),
            join_handle: Mutex::new(Some(runtime.join_handle)),
            tcp_export_stop_tx: runtime.tcp_export.stop_tx,
            tcp_export_join_handle: Mutex::new(runtime.tcp_export.join_handle),
        })
    }

    #[cfg(feature = "telemetry")]
    fn start_enabled_parts(
        shard_count: usize,
        config: &PersistenceConfig,
        metrics: Option<CacheTelemetryHandle>,
    ) -> Result<StartedPersistenceRuntime> {
        PersistenceStartup::new(shard_count, config)?.start_writer(config.clone(), metrics)
    }

    #[cfg(not(feature = "telemetry"))]
    fn start_enabled_parts(
        shard_count: usize,
        config: &PersistenceConfig,
    ) -> Result<StartedPersistenceRuntime> {
        PersistenceStartup::new(shard_count, config)?.start_writer(config.clone())
    }

    /// Takes exclusive ownership of one shard's WAL block appender.
    ///
    /// A shard appender can be taken only once. This preserves mutation order
    /// without a shared producer lock. The owner must flush or drop it before
    /// shutting down the persistence runtime.
    pub fn appender(&self, shard_id: usize) -> Option<WalAppender> {
        self.appenders
            .get(shard_id)
            .and_then(|appender| appender.lock().take())
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn stats_snapshot(&self) -> WalStatsSnapshot {
        self.stats.snapshot()
    }

    pub fn snapshot(&self, entries: &[StoredEntry], timestamp_ms: u64) -> Result<PathBuf> {
        match self.config.enabled {
            true => self.write_snapshot(entries, timestamp_ms),
            false => Err(ShardCacheError::Persistence(
                "persistence is disabled".into(),
            )),
        }
    }

    /// Writes an atomic snapshot from a bounded entry producer and prunes WAL
    /// only after the snapshot and containing directory are durable.
    pub fn snapshot_streaming<F>(&self, timestamp_ms: u64, produce: F) -> Result<PathBuf>
    where
        F: FnOnce(&mut dyn FnMut(StoredEntry) -> Result<()>) -> Result<()>,
    {
        if !self.config.enabled {
            return Err(ShardCacheError::Persistence(
                "persistence is disabled".into(),
            ));
        }
        fs::create_dir_all(&self.config.data_dir)?;
        let path = SnapshotStore::new(&self.config.data_dir).write_snapshot_streaming(
            timestamp_ms,
            SnapshotCompression::from_enabled(self.config.compress_snapshots),
            produce,
        )?;
        self.stats.record_snapshot_written();
        wal::SegmentStore::new(&self.config.data_dir).prune_through(timestamp_ms)?;
        Ok(path)
    }

    fn write_snapshot(&self, entries: &[StoredEntry], timestamp_ms: u64) -> Result<PathBuf> {
        fs::create_dir_all(&self.config.data_dir)?;
        let snapshots = SnapshotStore::new(&self.config.data_dir);
        let path = snapshots.write_snapshot(
            entries,
            timestamp_ms,
            SnapshotCompression::from_enabled(self.config.compress_snapshots),
        )?;
        self.stats.record_snapshot_written();
        wal::SegmentStore::new(&self.config.data_dir).prune_through(timestamp_ms)?;
        Ok(path)
    }

    pub fn shutdown(&self) -> Result<()> {
        Self::signal_stop(&self.stop_tx);
        Self::signal_stop(&self.tcp_export_stop_tx);
        let wal_result = Self::join_wal_thread(&self.join_handle);
        let tcp_result = Self::join_thread(
            &self.tcp_export_join_handle,
            "TCP WAL exporter thread panicked",
        );
        wal_result.and(tcp_result)
    }

    fn signal_stop(stop_tx: &Option<Sender<()>>) {
        match StopSignal::from_option(stop_tx) {
            StopSignal::Enabled(stop_tx) => {
                let _ = stop_tx.send(());
            }
            StopSignal::Disabled => {}
        }
    }

    fn join_thread(handle: &Mutex<Option<JoinHandle<()>>>, panic_message: &str) -> Result<()> {
        match handle.lock().take() {
            Some(join_handle) => join_handle
                .join()
                .map_err(|_| ShardCacheError::TaskJoin(panic_message.into())),
            None => Ok(()),
        }
    }

    fn join_wal_thread(handle: &Mutex<Option<JoinHandle<Result<()>>>>) -> Result<()> {
        match handle.lock().take() {
            Some(join_handle) => join_handle
                .join()
                .map_err(|_| ShardCacheError::TaskJoin("WAL thread panicked".into()))?,
            None => Ok(()),
        }
    }
}

impl WalAppender {
    pub fn append(&mut self, record: MutationRecord) -> Result<()> {
        self.pending_bytes = self
            .pending_bytes
            .saturating_add(estimated_record_bytes(&record));
        self.pending.push(record);
        if self.pending.len() >= self.max_records || self.pending_bytes >= self.max_bytes {
            self.flush()?;
        }
        Ok(())
    }

    /// Publishes the current shard-local block to the background WAL merger.
    pub fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let next_capacity = self.pending.len().min(self.max_records);
        let pending_bytes = self.pending_bytes;
        let records = std::mem::replace(&mut self.pending, Vec::with_capacity(next_capacity));
        match self.sender.send(WalBlock { records }) {
            Ok(()) => {
                self.pending_bytes = 0;
                self.last_publish = Instant::now();
                Ok(())
            }
            Err(error) => {
                self.pending = error.0.records;
                self.pending_bytes = pending_bytes;
                Err(ShardCacheError::ChannelClosed("wal appender"))
            }
        }
    }

    pub(crate) fn flush_due(&mut self) -> Result<()> {
        if !self.pending.is_empty() && self.last_publish.elapsed() >= self.flush_interval {
            self.flush()?;
        }
        Ok(())
    }

    pub(crate) fn next_flush_timeout(&self) -> Option<Duration> {
        (!self.pending.is_empty()).then(|| {
            self.flush_interval
                .checked_sub(self.last_publish.elapsed())
                .unwrap_or_default()
        })
    }
}

impl Drop for WalAppender {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

fn estimated_record_bytes(record: &MutationRecord) -> usize {
    64usize
        .saturating_add(record.key.len())
        .saturating_add(record.value.len())
        .saturating_add(record.governance.as_ref().map_or(0, MutationBytes::len))
}

impl Drop for PersistenceRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

pub fn load_recovery_state(config: &PersistenceConfig) -> Result<RecoveryState> {
    RecoveryLoader::new(config).load()
}

pub fn decode_wal_records(bytes: impl Into<MutationBytes>) -> Result<Vec<MutationRecord>> {
    wal::WalRecordCodec::decode_records(bytes.into())
}

pub fn encode_wal_record_frame(record: &MutationRecord, compress: bool) -> Vec<u8> {
    wal::WalRecordCodec::encode_frame(record, compress)
}

pub fn data_dir_path(path: &Path) -> PathBuf {
    PersistenceDataDir::normalize(path)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread;
    use std::time::{Duration, Instant};

    use crate::config::{PersistenceConfig, WalTcpExportMode};
    use bytes::Bytes as SharedBytes;

    use crate::storage::{MutationOp, MutationRecord, StoredEntry};

    use super::{PersistenceRuntime, SnapshotRepository, SnapshotStore};

    #[test]
    fn persistence_runtime_streams_durable_snapshot() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_path_buf(),
            compress_snapshots: true,
            ..PersistenceConfig::default()
        };
        let runtime = PersistenceRuntime::start(1, config).expect("persistence runtime");
        runtime
            .snapshot_streaming(17, |sink| {
                sink(StoredEntry {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                    expire_at_ms: None,
                    governance: None,
                })
            })
            .expect("streaming snapshot");

        let loaded = SnapshotStore::new(temp_dir.path())
            .load_latest_snapshot()
            .expect("load snapshot")
            .expect("snapshot exists");
        assert_eq!(loaded.timestamp_ms, 17);
        assert_eq!(loaded.entries[0].value, b"value");
        assert_eq!(runtime.stats_snapshot().snapshots_written, 1);
        runtime.shutdown().expect("shutdown");
    }

    #[test]
    fn shard_local_wal_buffer_publishes_full_blocks() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_path_buf(),
            compress_wal: false,
            fsync_interval_ms: 1_000,
            wal_block_max_records: 3,
            wal_block_max_bytes: usize::MAX,
            ..PersistenceConfig::default()
        };
        let runtime = PersistenceRuntime::start(1, config).expect("persistence runtime");
        let mut appender = runtime.appender(0).expect("appender");
        appender.append(wal_record(0, 1)).expect("append one");
        appender.append(wal_record(0, 2)).expect("append two");
        assert_eq!(runtime.stats_snapshot().entries_written, 0);

        appender.append(wal_record(0, 3)).expect("append three");
        wait_for_wal_entries(&runtime, 3);
        assert_eq!(runtime.stats_snapshot().blocks_merged, 1);
        drop(appender);
        runtime.shutdown().expect("shutdown");
    }

    #[test]
    fn each_shard_wal_appender_has_one_exclusive_owner() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..PersistenceConfig::default()
        };
        let runtime = PersistenceRuntime::start(1, config).expect("persistence runtime");
        let appender = runtime.appender(0).expect("first owner");
        assert!(runtime.appender(0).is_none());
        drop(appender);
        runtime.shutdown().expect("shutdown");
    }

    #[test]
    fn persistence_runtime_rejects_invalid_block_limits() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_block_max_records: 0,
            ..PersistenceConfig::default()
        };
        let error = PersistenceRuntime::start(1, config)
            .err()
            .expect("invalid block limit");
        assert!(error.to_string().contains("limits must be > 0"));
    }

    #[test]
    fn failed_block_publish_retains_shard_records() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_path_buf(),
            wal_block_max_records: 64,
            ..PersistenceConfig::default()
        };
        let runtime = PersistenceRuntime::start(1, config).expect("persistence runtime");
        let mut appender = runtime.appender(0).expect("appender");
        appender.append(wal_record(0, 1)).expect("buffer record");
        runtime.shutdown().expect("shutdown");

        assert!(appender.flush().is_err());
        assert_eq!(appender.pending.len(), 1);
        assert!(appender.pending_bytes > 0);
    }

    #[test]
    fn shard_local_wal_buffer_publishes_on_deadline() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_path_buf(),
            compress_wal: false,
            fsync_interval_ms: 1,
            wal_block_max_records: 64,
            ..PersistenceConfig::default()
        };
        let runtime = PersistenceRuntime::start(1, config).expect("persistence runtime");
        let mut appender = runtime.appender(0).expect("appender");
        appender.append(wal_record(0, 1)).expect("append");
        thread::sleep(Duration::from_millis(2));
        appender.flush_due().expect("deadline flush");
        wait_for_wal_entries(&runtime, 1);
        drop(appender);
        runtime.shutdown().expect("shutdown");
    }

    #[test]
    fn wal_merger_preserves_order_within_each_shard() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let config = PersistenceConfig {
            data_dir: temp_dir.path().to_path_buf(),
            compress_wal: false,
            wal_block_max_records: 2,
            ..PersistenceConfig::default()
        };
        let runtime = PersistenceRuntime::start(2, config).expect("persistence runtime");
        let mut left = runtime.appender(0).expect("left appender");
        let mut right = runtime.appender(1).expect("right appender");
        for sequence in 1..=4 {
            left.append(wal_record(0, sequence)).expect("append left");
            right.append(wal_record(1, sequence)).expect("append right");
        }
        left.flush().expect("flush left");
        right.flush().expect("flush right");
        drop((left, right));
        runtime.shutdown().expect("shutdown");

        let records = super::wal::SegmentStore::new(temp_dir.path())
            .read_all()
            .expect("read WAL");
        for shard_id in 0..=1 {
            let keys = records
                .iter()
                .map(|record| String::from_utf8_lossy(&record.key))
                .filter(|key| key.starts_with(&format!("key-{shard_id}-")))
                .map(|key| key.into_owned())
                .collect::<Vec<_>>();
            assert_eq!(
                keys,
                (1..=4)
                    .map(|sequence| format!("key-{shard_id}-{sequence}"))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn tcp_export_streams_framed_wal_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind tcp collector");
        let addr = listener.local_addr().expect("local addr");
        let reader = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept WAL stream");
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .expect("read timeout");
            let mut header = [0_u8; 9];
            stream.read_exact(&mut header).expect("read WAL header");
            let payload_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;
            let mut tail = vec![0_u8; payload_len + 4];
            stream.read_exact(&mut tail).expect("read WAL payload");
            let mut frame = header.to_vec();
            frame.extend_from_slice(&tail);
            frame
        });

        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let mut config = PersistenceConfig {
            data_dir: temp_dir.path().to_path_buf(),
            compress_wal: false,
            ..PersistenceConfig::default()
        };
        config.tcp_export.enabled = true;
        config.tcp_export.addr = addr.to_string();
        config.tcp_export.connect_timeout_ms = 500;
        config.tcp_export.write_timeout_ms = 500;
        config.tcp_export.reconnect_backoff_ms = 10;

        let runtime = PersistenceRuntime::start(1, config).expect("persistence runtime");
        runtime
            .appender(0)
            .expect("appender")
            .append(MutationRecord {
                shard_id: 0,
                sequence: 1,
                timestamp_ms: 42,
                op: MutationOp::Set,
                key: SharedBytes::from_static(b"alpha"),
                value: SharedBytes::from_static(b"one"),
                expire_at_ms: None,
                governance: None,
            })
            .expect("append");

        let frame = reader.join().expect("reader thread");
        runtime.shutdown().expect("shutdown");

        assert!(frame.starts_with(b"FCW2"));
        assert_eq!(
            frame.len(),
            9 + u32::from_le_bytes(frame[5..9].try_into().unwrap()) as usize + 4
        );
    }

    #[test]
    fn tcp_export_listen_mode_authenticates_subscribers() {
        let addr = free_local_addr();
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let mut config = PersistenceConfig {
            data_dir: temp_dir.path().to_path_buf(),
            compress_wal: false,
            ..PersistenceConfig::default()
        };
        config.tcp_export.enabled = true;
        config.tcp_export.mode = WalTcpExportMode::Listen;
        config.tcp_export.addr = addr.to_string();
        config.tcp_export.auth_token = Some("secret".to_string());
        config.tcp_export.connect_timeout_ms = 500;
        config.tcp_export.write_timeout_ms = 500;
        config.tcp_export.reconnect_backoff_ms = 10;

        let runtime = PersistenceRuntime::start(1, config).expect("persistence runtime");
        let mut subscriber = connect_with_retry(addr);
        subscriber
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("read timeout");
        subscriber
            .write_all(b"FCWAL-AUTH/1 secret\n")
            .expect("write auth");
        wait_for_subscriber(&runtime);

        runtime
            .appender(0)
            .expect("appender")
            .append(MutationRecord {
                shard_id: 0,
                sequence: 1,
                timestamp_ms: 42,
                op: MutationOp::Set,
                key: SharedBytes::from_static(b"alpha"),
                value: SharedBytes::from_static(b"one"),
                expire_at_ms: None,
                governance: None,
            })
            .expect("append");

        let frame = read_wal_frame(&mut subscriber);
        runtime.shutdown().expect("shutdown");

        assert!(frame.starts_with(b"FCW2"));
        assert_eq!(
            frame.len(),
            9 + u32::from_le_bytes(frame[5..9].try_into().unwrap()) as usize + 4
        );
    }

    fn free_local_addr() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        listener.local_addr().expect("local addr")
    }

    fn connect_with_retry(addr: SocketAddr) -> TcpStream {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match TcpStream::connect(addr) {
                Ok(stream) => return stream,
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("connect to TCP WAL listener failed: {error}"),
            }
        }
    }

    fn wait_for_subscriber(runtime: &PersistenceRuntime) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if runtime.stats_snapshot().tcp_export_active_subscribers > 0 {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("TCP WAL subscriber was not accepted");
    }

    fn read_wal_frame(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0_u8; 9];
        stream.read_exact(&mut header).expect("read WAL header");
        let payload_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;
        let mut tail = vec![0_u8; payload_len + 4];
        stream.read_exact(&mut tail).expect("read WAL payload");
        let mut frame = header.to_vec();
        frame.extend_from_slice(&tail);
        frame
    }

    fn wal_record(shard_id: usize, sequence: u64) -> MutationRecord {
        MutationRecord {
            shard_id,
            sequence,
            timestamp_ms: sequence,
            op: MutationOp::Set,
            key: SharedBytes::from(format!("key-{shard_id}-{sequence}")),
            value: SharedBytes::from_static(b"value"),
            expire_at_ms: None,
            governance: None,
        }
    }

    fn wait_for_wal_entries(runtime: &PersistenceRuntime, expected: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if runtime.stats_snapshot().entries_written >= expected {
                return;
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!(
            "WAL writer did not reach {expected} entries; observed {}",
            runtime.stats_snapshot().entries_written
        );
    }
}
