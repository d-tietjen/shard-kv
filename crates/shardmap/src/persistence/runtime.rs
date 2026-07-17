use std::fs;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded};

use crate::config::PersistenceConfig;
#[cfg(feature = "telemetry")]
use crate::storage::CacheTelemetryHandle;
use crate::storage::MutationRecord;
use crate::{Result, ShardCacheError};

use super::{WalAppender, WalBlock, WalFrameBytes, stats::WalStats, tcp_export, wal};

struct WalChannels {
    appenders: Vec<WalAppender>,
    receivers: Vec<Receiver<WalBlock>>,
}

pub(super) struct PersistenceStartup {
    stats: Arc<WalStats>,
    channels: WalChannels,
    stop_tx: Sender<()>,
    stop_rx: Receiver<()>,
    tcp_export: TcpExportRuntime,
}

pub(super) struct StartedPersistenceRuntime {
    pub(super) stats: Arc<WalStats>,
    pub(super) appenders: Vec<WalAppender>,
    pub(super) stop_tx: Sender<()>,
    pub(super) join_handle: JoinHandle<Result<()>>,
    pub(super) tcp_export: TcpExportRuntime,
}

pub(super) struct TcpExportRuntime {
    sender: Option<Sender<WalFrameBytes>>,
    pub(super) stop_tx: Option<Sender<()>>,
    pub(super) join_handle: Option<JoinHandle<()>>,
}

struct TcpExportQueue {
    sender: Option<Sender<WalFrameBytes>>,
}

pub(super) enum StopSignal<'a> {
    Enabled(&'a Sender<()>),
    Disabled,
}

enum TcpExportQueueState<'a> {
    Enabled(&'a Sender<WalFrameBytes>),
    Disabled,
}

enum TcpExportQueueResult {
    Queued,
    Dropped,
}

struct WalWriter {
    receivers: Vec<Receiver<WalBlock>>,
    stop_rx: Receiver<()>,
    config: PersistenceConfig,
    stats: Arc<WalStats>,
    tcp_export: TcpExportQueue,
    receiver_index: usize,
    stopping: bool,
    #[cfg(feature = "telemetry")]
    metrics: Option<CacheTelemetryHandle>,
}

enum WalFlushDecision {
    Flush,
    Skip,
}

enum WalFlushKind {
    Periodic,
    Final,
}

impl WalChannels {
    fn new(shard_count: usize, config: &PersistenceConfig) -> Result<Self> {
        if shard_count == 0 {
            return Err(ShardCacheError::Config(
                "persistence shard_count must be > 0".into(),
            ));
        }
        if config.wal_channel_capacity == 0
            || config.wal_block_max_records == 0
            || config.wal_block_max_bytes == 0
            || config.fsync_interval_ms == 0
        {
            return Err(ShardCacheError::Config(
                "persistence WAL channel, block, and flush limits must be > 0".into(),
            ));
        }
        let mut appenders = Vec::with_capacity(shard_count);
        let mut receivers = Vec::with_capacity(shard_count);
        let block_capacity = config
            .wal_channel_capacity
            .div_ceil(config.wal_block_max_records)
            .max(1);
        let flush_interval = Duration::from_millis(config.fsync_interval_ms.max(1));
        for _ in 0..shard_count {
            let (tx, rx) = bounded::<WalBlock>(block_capacity);
            appenders.push(WalAppender {
                sender: tx,
                pending: Vec::with_capacity(config.wal_block_max_records),
                pending_bytes: 0,
                max_records: config.wal_block_max_records,
                max_bytes: config.wal_block_max_bytes,
                flush_interval,
                last_publish: Instant::now(),
            });
            receivers.push(rx);
        }
        Ok(Self {
            appenders,
            receivers,
        })
    }
}

impl PersistenceStartup {
    pub(super) fn new(shard_count: usize, config: &PersistenceConfig) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;
        let stats = Arc::new(WalStats::enabled());
        let channels = WalChannels::new(shard_count, config)?;
        let (stop_tx, stop_rx) = bounded::<()>(1);
        let tcp_export = TcpExportRuntime::start(config, Arc::clone(&stats))?;
        Ok(Self {
            stats,
            channels,
            stop_tx,
            stop_rx,
            tcp_export,
        })
    }

    #[cfg(feature = "telemetry")]
    pub(super) fn start_writer(
        self,
        config: PersistenceConfig,
        metrics: Option<CacheTelemetryHandle>,
    ) -> Result<StartedPersistenceRuntime> {
        let Self {
            stats,
            channels,
            stop_tx,
            stop_rx,
            tcp_export,
        } = self;
        let WalChannels {
            appenders,
            receivers,
        } = channels;
        let TcpExportRuntime {
            sender,
            stop_tx: tcp_export_stop_tx,
            join_handle: tcp_export_join_handle,
        } = tcp_export;
        let join_handle = WalWriter::spawn(
            receivers,
            stop_rx,
            config,
            Arc::clone(&stats),
            TcpExportQueue::new(sender),
            metrics,
        )?;
        Ok(StartedPersistenceRuntime {
            stats,
            appenders,
            stop_tx,
            join_handle,
            tcp_export: TcpExportRuntime {
                sender: None,
                stop_tx: tcp_export_stop_tx,
                join_handle: tcp_export_join_handle,
            },
        })
    }

    #[cfg(not(feature = "telemetry"))]
    pub(super) fn start_writer(
        self,
        config: PersistenceConfig,
    ) -> Result<StartedPersistenceRuntime> {
        let Self {
            stats,
            channels,
            stop_tx,
            stop_rx,
            tcp_export,
        } = self;
        let WalChannels {
            appenders,
            receivers,
        } = channels;
        let TcpExportRuntime {
            sender,
            stop_tx: tcp_export_stop_tx,
            join_handle: tcp_export_join_handle,
        } = tcp_export;
        let join_handle = WalWriter::spawn(
            receivers,
            stop_rx,
            config,
            Arc::clone(&stats),
            TcpExportQueue::new(sender),
        )?;
        Ok(StartedPersistenceRuntime {
            stats,
            appenders,
            stop_tx,
            join_handle,
            tcp_export: TcpExportRuntime {
                sender: None,
                stop_tx: tcp_export_stop_tx,
                join_handle: tcp_export_join_handle,
            },
        })
    }
}

impl TcpExportRuntime {
    fn start(config: &PersistenceConfig, stats: Arc<WalStats>) -> Result<Self> {
        match config.tcp_export.enabled {
            true => Self::start_enabled(config, stats),
            false => Ok(Self::disabled()),
        }
    }

    fn disabled() -> Self {
        Self {
            sender: None,
            stop_tx: None,
            join_handle: None,
        }
    }

    fn start_enabled(config: &PersistenceConfig, stats: Arc<WalStats>) -> Result<Self> {
        // Cross-field validation lives in `ShardCacheConfig::validate` and runs
        // before this is reached.
        stats.enable_tcp_export();
        let (sender, receiver) = bounded::<WalFrameBytes>(config.tcp_export.channel_capacity);
        let (stop_tx, stop_rx) = bounded::<()>(1);
        let join_handle = tcp_export::spawn(config.tcp_export.clone(), receiver, stop_rx, stats)?;
        Ok(Self {
            sender: Some(sender),
            stop_tx: Some(stop_tx),
            join_handle: Some(join_handle),
        })
    }
}

impl<'a> StopSignal<'a> {
    pub(super) fn from_option(stop_tx: &'a Option<Sender<()>>) -> Self {
        match stop_tx.as_ref() {
            Some(stop_tx) => Self::Enabled(stop_tx),
            None => Self::Disabled,
        }
    }
}

impl TcpExportQueue {
    fn new(sender: Option<Sender<WalFrameBytes>>) -> Self {
        Self { sender }
    }

    fn enqueue(&self, frame: WalFrameBytes, config: &PersistenceConfig, stats: &WalStats) {
        match TcpExportQueueState::from_sender(&self.sender) {
            TcpExportQueueState::Enabled(sender) => {
                Self::record_result(stats, Self::send(sender, frame, config))
            }
            TcpExportQueueState::Disabled => {}
        }
    }

    fn send(
        sender: &Sender<WalFrameBytes>,
        frame: WalFrameBytes,
        config: &PersistenceConfig,
    ) -> TcpExportQueueResult {
        let result = match config.tcp_export.backpressure_on_full {
            true => sender
                .send(frame)
                .map_err(|error| TrySendError::Disconnected(error.0)),
            false => sender.try_send(frame),
        };
        match result {
            Ok(()) => TcpExportQueueResult::Queued,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                TcpExportQueueResult::Dropped
            }
        }
    }

    fn record_result(stats: &WalStats, result: TcpExportQueueResult) {
        match result {
            TcpExportQueueResult::Queued => stats.record_tcp_export_queued(),
            TcpExportQueueResult::Dropped => stats.record_tcp_export_dropped(),
        }
    }
}

impl<'a> TcpExportQueueState<'a> {
    fn from_sender(sender: &'a Option<Sender<WalFrameBytes>>) -> Self {
        match sender.as_ref() {
            Some(sender) => Self::Enabled(sender),
            None => Self::Disabled,
        }
    }
}

impl WalWriter {
    #[cfg(feature = "telemetry")]
    fn spawn(
        receivers: Vec<Receiver<WalBlock>>,
        stop_rx: Receiver<()>,
        config: PersistenceConfig,
        stats: Arc<WalStats>,
        tcp_export: TcpExportQueue,
        metrics: Option<CacheTelemetryHandle>,
    ) -> Result<JoinHandle<Result<()>>> {
        Self::spawn_writer(Self {
            receivers,
            stop_rx,
            config,
            stats,
            tcp_export,
            receiver_index: 0,
            stopping: false,
            metrics,
        })
    }

    #[cfg(not(feature = "telemetry"))]
    fn spawn(
        receivers: Vec<Receiver<WalBlock>>,
        stop_rx: Receiver<()>,
        config: PersistenceConfig,
        stats: Arc<WalStats>,
        tcp_export: TcpExportQueue,
    ) -> Result<JoinHandle<Result<()>>> {
        Self::spawn_writer(Self {
            receivers,
            stop_rx,
            config,
            stats,
            tcp_export,
            receiver_index: 0,
            stopping: false,
        })
    }

    fn spawn_writer(writer: Self) -> Result<JoinHandle<Result<()>>> {
        thread::Builder::new()
            .name("shardcache-wal".into())
            .spawn(move || {
                let result = writer.run();
                if let Err(error) = &result {
                    tracing::error!("WAL writer stopped: {error}");
                }
                result
            })
            .map_err(|error| {
                ShardCacheError::Persistence(format!("failed to start WAL thread: {error}"))
            })
    }

    fn run(mut self) -> Result<()> {
        let writer = self.open_segment_writer()?;
        self.run_with_writer(writer)
    }

    fn open_segment_writer(&self) -> Result<wal::SegmentWriter> {
        wal::SegmentStore::new(&self.config.data_dir).open_writer(self.config.compress_wal)
    }

    fn run_with_writer(&mut self, mut writer: wal::SegmentWriter) -> Result<()> {
        let mut last_flush = Instant::now();
        let mut dirty = false;
        while self.is_running() {
            let progressed = self.poll_receivers(&mut writer)?;
            dirty |= progressed;
            self.flush_if_due(&mut writer, &mut dirty, &mut last_flush)?;
            self.sleep_when_idle(progressed);
        }
        self.drain_remaining(&mut writer)?;
        self.flush_writer(&mut writer, WalFlushKind::Final, &mut last_flush)
    }

    fn poll_receivers(&mut self, writer: &mut wal::SegmentWriter) -> Result<bool> {
        match self.receivers.is_empty() {
            true => Ok(false),
            false => self.poll_round_robin(writer),
        }
    }

    fn poll_round_robin(&mut self, writer: &mut wal::SegmentWriter) -> Result<bool> {
        let mut progressed = false;
        for _ in 0..self.receivers.len() {
            progressed |= self.poll_next_receiver(writer)?;
        }
        Ok(progressed)
    }

    fn poll_next_receiver(&mut self, writer: &mut wal::SegmentWriter) -> Result<bool> {
        let index = self.next_receiver_index();
        match self.receivers[index].try_recv() {
            Ok(block) => {
                self.append_block(writer, block)?;
                Ok(true)
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => Ok(false),
        }
    }

    fn next_receiver_index(&mut self) -> usize {
        let index = self.receiver_index % self.receivers.len();
        self.receiver_index = self.receiver_index.wrapping_add(1);
        index
    }

    fn append_record(
        &mut self,
        writer: &mut wal::SegmentWriter,
        record: &MutationRecord,
    ) -> Result<()> {
        let encoded = wal::WalRecordCodec::encode_frame(record, self.config.compress_wal);
        writer.append_encoded(&encoded, self.config.segment_size_bytes)?;
        self.record_append_success(writer, encoded);
        Ok(())
    }

    fn append_block(&mut self, writer: &mut wal::SegmentWriter, block: WalBlock) -> Result<()> {
        for record in &block.records {
            self.append_record(writer, record)?;
        }
        self.stats.record_block_merged();
        Ok(())
    }

    fn record_append_success(&mut self, writer: &mut wal::SegmentWriter, encoded: Vec<u8>) {
        let bytes = writer.last_entry_size();
        let rotations = writer.take_rotation_count();
        self.record_append_metrics(bytes);
        self.stats.record_append(bytes, rotations);
        self.tcp_export.enqueue(
            WalFrameBytes::from(encoded),
            &self.config,
            self.stats.as_ref(),
        );
    }

    #[cfg(feature = "telemetry")]
    fn record_append_metrics(&self, bytes: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.record_wal_append(bytes);
        }
    }

    #[cfg(not(feature = "telemetry"))]
    fn record_append_metrics(&self, _bytes: usize) {}

    fn flush_if_due(
        &mut self,
        writer: &mut wal::SegmentWriter,
        dirty: &mut bool,
        last_flush: &mut Instant,
    ) -> Result<()> {
        match WalFlushDecision::from_dirty(*dirty, *last_flush, &self.config) {
            WalFlushDecision::Flush => {
                self.flush_writer(writer, WalFlushKind::Periodic, last_flush)?;
                *dirty = false;
            }
            WalFlushDecision::Skip => {}
        }
        Ok(())
    }

    fn flush_writer(
        &self,
        writer: &mut wal::SegmentWriter,
        kind: WalFlushKind,
        last_flush: &mut Instant,
    ) -> Result<()> {
        let started = Instant::now();
        match writer.flush() {
            Ok(()) => {
                self.record_flush_success(kind, started, last_flush);
                Ok(())
            }
            Err(error) => {
                self.record_flush_error(kind, &error);
                Err(error)
            }
        }
    }

    fn record_flush_success(&self, kind: WalFlushKind, started: Instant, last_flush: &mut Instant) {
        self.record_flush_metrics(started);
        match kind {
            WalFlushKind::Periodic => {
                self.stats.record_flush(crate::storage::now_millis());
                *last_flush = Instant::now();
            }
            WalFlushKind::Final => {}
        }
    }

    #[cfg(feature = "telemetry")]
    fn record_flush_metrics(&self, started: Instant) {
        if let Some(metrics) = &self.metrics {
            metrics.record_wal_flush(started.elapsed().as_nanos() as u64);
        }
    }

    #[cfg(not(feature = "telemetry"))]
    fn record_flush_metrics(&self, _started: Instant) {}

    fn record_flush_error(&self, kind: WalFlushKind, error: &ShardCacheError) {
        match kind {
            WalFlushKind::Periodic => {}
            WalFlushKind::Final => tracing::error!("failed final WAL flush: {error}"),
        }
    }

    fn sleep_when_idle(&mut self, progressed: bool) {
        match progressed || !self.is_running() {
            true => {}
            false => thread::sleep(Duration::from_millis(2)),
        }
    }

    fn is_running(&mut self) -> bool {
        match self.stopping {
            true => false,
            false => match self.stop_rx.try_recv() {
                Ok(()) => {
                    self.stopping = true;
                    false
                }
                Err(_) => true,
            },
        }
    }

    fn drain_remaining(&mut self, writer: &mut wal::SegmentWriter) -> Result<()> {
        for receiver in self.receivers.clone() {
            while let Ok(block) = receiver.try_recv() {
                self.append_block(writer, block)?;
            }
        }
        Ok(())
    }
}

impl WalFlushDecision {
    fn from_dirty(dirty: bool, last_flush: Instant, config: &PersistenceConfig) -> Self {
        let flush_interval = Duration::from_millis(config.fsync_interval_ms.max(1));
        match dirty && last_flush.elapsed() >= flush_interval {
            true => Self::Flush,
            false => Self::Skip,
        }
    }
}
