//! TCP wire transport for native FCRP replication.
//!
//! The transport is intentionally minimal and synchronous: one accept thread
//! per primary, one per-replica worker thread for streaming, and one connect
//! thread per replica. Frames on the wire are encoded by [`encode_frame`] in
//! [`super::protocol`], so capture/replay tools that already understand FCRP
//! frames work unchanged.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{RecvTimeoutError, TryRecvError};
use parking_lot::Mutex;

use crate::config::ReplicationConfig;
use crate::storage::StoredEntry;
use crate::{Result, ShardCacheError};

use super::ReplicationFrameBytes;
use super::backlog::BacklogCatchUp;
use super::batcher::ReplicationPrimary;
use super::embedded::{ReplicatedEmbeddedStore, ReplicationReplica};
use super::protocol::{
    FCRP_VERSION, FrameKind, HelloRole, ReplicationCompressionMode, ReplicationHello,
    ReplicationSnapshotChunk, ShardWatermarks, decode_ack, decode_error, decode_frame,
    decode_frame_payload_bytes, decode_hello, decode_snapshot_chunk, encode_ack, encode_error,
    encode_frame, encode_hello, encode_snapshot_chunk,
};

#[cfg(all(target_os = "linux", feature = "monoio"))]
mod monoio_transport;

const FRAME_HEADER_LEN: usize = 16;
const MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;

/// Provides consistent snapshots to the replication transport.
pub trait SnapshotProvider: Send + Sync + 'static {
    /// Returns a consistent snapshot together with the watermarks captured at
    /// the same logical point.
    fn snapshot(&self) -> super::protocol::ReplicationSnapshot;
}

impl SnapshotProvider for ReplicatedEmbeddedStore {
    fn snapshot(&self) -> super::protocol::ReplicationSnapshot {
        ReplicatedEmbeddedStore::snapshot(self)
    }
}

/// Handle to a primary's TCP listener thread.
#[derive(Debug)]
pub struct ReplicationPrimaryServer {
    stop: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl ReplicationPrimaryServer {
    /// Binds a TCP listener and serves replicas using `primary` and `snapshots`.
    pub fn start(
        config: ReplicationConfig,
        primary: Arc<ReplicationPrimary>,
        snapshots: Arc<dyn SnapshotProvider>,
    ) -> Result<Self> {
        if !config.enabled {
            return Err(ShardCacheError::Config(
                "replication primary server requires replication.enabled = true".into(),
            ));
        }
        #[cfg(all(target_os = "linux", feature = "monoio"))]
        if monoio_transport::should_use() {
            return monoio_transport::start_primary(config, primary, snapshots);
        }

        let listener = TcpListener::bind(&config.bind_addr).map_err(|error| {
            ShardCacheError::Config(format!(
                "replication primary failed to bind {}: {error}",
                config.bind_addr
            ))
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            ShardCacheError::Config(format!(
                "replication primary set_nonblocking failed: {error}"
            ))
        })?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let cfg = config;
        let join = thread::Builder::new()
            .name("shardcache-replication-listener".into())
            .spawn(move || run_listener(listener, cfg, primary, snapshots, stop_clone))
            .map_err(|error| {
                ShardCacheError::Config(format!("failed to start replication listener: {error}"))
            })?;
        Ok(Self {
            stop,
            join: Mutex::new(Some(join)),
        })
    }

    #[cfg(all(target_os = "linux", feature = "monoio"))]
    fn from_join(stop: Arc<AtomicBool>, join: JoinHandle<()>) -> Self {
        Self {
            stop,
            join: Mutex::new(Some(join)),
        }
    }

    pub fn shutdown(&self) -> Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.lock().take() {
            join.join()
                .map_err(|_| ShardCacheError::TaskJoin("replication listener panicked".into()))?;
        }
        Ok(())
    }
}

impl Drop for ReplicationPrimaryServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn run_listener(
    listener: TcpListener,
    config: ReplicationConfig,
    primary: Arc<ReplicationPrimary>,
    snapshots: Arc<dyn SnapshotProvider>,
    stop: Arc<AtomicBool>,
) {
    let active = Arc::new(parking_lot::Mutex::new(Vec::<JoinHandle<()>>::new()));
    while !stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, peer)) => {
                // Cap simultaneous replicas. Drain finished workers first.
                let mut handles = active.lock();
                handles.retain(|h| !h.is_finished());
                if handles.len() >= config.max_replicas {
                    drop(handles);
                    tracing::warn!(
                        "rejecting replication client {peer}: max_replicas {} reached",
                        config.max_replicas
                    );
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                    continue;
                }
                let cfg = config.clone();
                let primary = Arc::clone(&primary);
                let snapshots = Arc::clone(&snapshots);
                let stop = Arc::clone(&stop);
                let handle = thread::Builder::new()
                    .name(format!("shardcache-replication-worker-{peer}"))
                    .spawn(move || {
                        if let Err(error) =
                            serve_replica(stream, peer, cfg, primary, snapshots, stop)
                        {
                            tracing::warn!("replication worker for {peer} terminated: {error}");
                        }
                    });
                match handle {
                    Ok(h) => handles.push(h),
                    Err(error) => tracing::warn!("failed to spawn replication worker: {error}"),
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => {
                tracing::warn!("replication listener accept failed: {error}");
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    // Best-effort join of workers; they observe the stop flag and exit.
    let mut handles = active.lock();
    for h in handles.drain(..) {
        let _ = h.join();
    }
}

fn serve_replica(
    mut stream: TcpStream,
    peer: SocketAddr,
    config: ReplicationConfig,
    primary: Arc<ReplicationPrimary>,
    snapshots: Arc<dyn SnapshotProvider>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    stream.set_nodelay(true).ok();
    stream
        .set_read_timeout(Some(Duration::from_millis(
            config.connect_timeout_ms.max(1),
        )))
        .ok();
    stream
        .set_write_timeout(Some(Duration::from_millis(config.write_timeout_ms.max(1))))
        .ok();

    let hello_frame = match read_frame_bytes_interruptible(&mut stream, &stop)? {
        Some(bytes) => bytes,
        None => return Ok(()),
    };
    let frame = decode_frame(&hello_frame)?;
    if frame.kind != FrameKind::Hello {
        send_error(&mut stream, "expected Hello frame")?;
        return Err(ShardCacheError::Protocol(format!(
            "replica {peer} sent {:?} before Hello",
            frame.kind
        )));
    }
    let hello = decode_hello(&frame.payload)?;
    if hello.version != FCRP_VERSION {
        send_error(&mut stream, "unsupported FCRP version")?;
        return Err(ShardCacheError::Protocol(format!(
            "replica {peer} requested FCRP version {}",
            hello.version
        )));
    }
    if !auth_ok(config.auth_token.as_deref(), hello.auth_token.as_deref()) {
        send_error(&mut stream, "invalid auth token")?;
        return Err(ShardCacheError::Protocol(format!(
            "replica {peer} sent invalid auth token"
        )));
    }
    // After authentication clear the read timeout so the worker can wait
    // indefinitely on the (silent) reverse channel without spuriously dying.
    stream.set_read_timeout(None).ok();
    let ack = ReplicationHello {
        version: FCRP_VERSION,
        role: HelloRole::Replica,
        auth_token: None,
        since: Some(primary.current_watermarks()),
    };
    write_full_frame(
        &mut stream,
        FrameKind::Hello,
        ReplicationCompressionMode::None,
        0,
        &encode_hello(&ack),
    )?;

    // Subscribe BEFORE deciding whether to snapshot, so we don't drop frames
    // that flush during the snapshot window.
    let subscription = primary.subscribe(config.subscriber_channel_capacity);

    let since = hello
        .since
        .clone()
        .unwrap_or_else(|| ShardWatermarks::new(primary.shard_count()));
    let live_start = match primary.catch_up_since(&since)? {
        BacklogCatchUp::Available(frames) => {
            for frame in &frames {
                write_raw(&mut stream, frame.as_ref())?;
            }
            primary.current_watermarks()
        }
        BacklogCatchUp::NeedsSnapshot => {
            let snapshot = snapshots.snapshot();
            stream_snapshot(&mut stream, &snapshot, &config)?;
            snapshot.watermarks
        }
    };

    // Drain any frames that were broadcast while we were sending the
    // snapshot, then enter the steady-state forwarding loop.
    drain_buffered(&mut stream, &subscription, &live_start, &primary)?;

    while !stop.load(Ordering::SeqCst) {
        match subscription.recv_timeout(Duration::from_millis(100)) {
            Ok(frame) => write_raw(&mut stream, frame.as_ref())?,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    Ok(())
}

fn drain_buffered(
    stream: &mut TcpStream,
    subscription: &crossbeam_channel::Receiver<ReplicationFrameBytes>,
    bootstrap_high: &ShardWatermarks,
    primary: &Arc<ReplicationPrimary>,
) -> Result<()> {
    loop {
        match subscription.try_recv() {
            Ok(frame) => write_raw(stream, frame.as_ref())?,
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => break,
        }
    }
    // The subscriber channel may have started filling AFTER we read the
    // bootstrap watermarks; the inflight backlog covers that gap.
    if let BacklogCatchUp::Available(frames) = primary.catch_up_since(bootstrap_high)? {
        for frame in frames {
            // De-duplication happens on the replica side via per-shard
            // watermark comparison, so re-sending these frames is safe.
            write_raw(stream, frame.as_ref())?;
        }
    }
    Ok(())
}

fn stream_snapshot(
    stream: &mut TcpStream,
    snapshot: &super::protocol::ReplicationSnapshot,
    config: &ReplicationConfig,
) -> Result<()> {
    write_full_frame(
        stream,
        FrameKind::SnapshotBegin,
        ReplicationCompressionMode::None,
        0,
        &encode_ack(&snapshot.watermarks),
    )?;

    let target = config.snapshot_chunk_bytes.max(4 * 1024);
    let mut chunk_index = 0u64;
    let mut buffer: Vec<crate::storage::StoredEntry> = Vec::new();
    let mut buffer_bytes = 0usize;
    let total = snapshot.entries.len();
    let compression = ReplicationCompressionMode::from(config.compression);

    for (idx, entry) in snapshot.entries.iter().enumerate() {
        let entry_bytes = entry.key.len() + entry.value.len() + 32;
        buffer.push(entry.clone());
        buffer_bytes = buffer_bytes.saturating_add(entry_bytes);
        let is_last_entry = idx + 1 == total;
        if buffer_bytes >= target || is_last_entry {
            let chunk = ReplicationSnapshotChunk {
                watermarks: snapshot.watermarks.clone(),
                chunk_index,
                is_last: is_last_entry,
                entries: std::mem::take(&mut buffer),
            };
            buffer_bytes = 0;
            chunk_index += 1;
            let payload = encode_snapshot_chunk(&chunk);
            write_full_frame(
                stream,
                FrameKind::SnapshotChunk,
                compression,
                config.zstd_level,
                &payload,
            )?;
        }
    }
    if total == 0 {
        let chunk = ReplicationSnapshotChunk {
            watermarks: snapshot.watermarks.clone(),
            chunk_index: 0,
            is_last: true,
            entries: Vec::new(),
        };
        let payload = encode_snapshot_chunk(&chunk);
        write_full_frame(
            stream,
            FrameKind::SnapshotChunk,
            ReplicationCompressionMode::None,
            0,
            &payload,
        )?;
    }
    write_full_frame(
        stream,
        FrameKind::SnapshotEnd,
        ReplicationCompressionMode::None,
        0,
        &encode_ack(&snapshot.watermarks),
    )?;
    Ok(())
}

fn send_error(stream: &mut TcpStream, message: &str) -> Result<()> {
    write_full_frame(
        stream,
        FrameKind::Error,
        ReplicationCompressionMode::None,
        0,
        &encode_error(message),
    )
}

fn auth_ok(expected: Option<&str>, presented: Option<&str>) -> bool {
    match (expected, presented) {
        (None, _) => true,
        (Some(want), Some(got)) => want == got,
        (Some(_), None) => false,
    }
}

fn write_full_frame(
    stream: &mut TcpStream,
    kind: FrameKind,
    compression: ReplicationCompressionMode,
    zstd_level: i32,
    payload: &[u8],
) -> Result<()> {
    let frame = encode_frame(kind, compression, zstd_level, payload)?;
    write_raw(stream, &frame)
}

fn write_raw(stream: &mut TcpStream, bytes: &[u8]) -> Result<()> {
    stream.write_all(bytes).map_err(ShardCacheError::Io)
}

fn read_frame_bytes(stream: &mut TcpStream) -> Result<Vec<u8>> {
    read_frame_inner(stream, None).and_then(|opt| {
        opt.ok_or_else(|| {
            ShardCacheError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "FCRP stream closed before frame completed",
            ))
        })
    })
}

fn read_frame_bytes_interruptible(
    stream: &mut TcpStream,
    stop: &Arc<AtomicBool>,
) -> Result<Option<Vec<u8>>> {
    read_frame_inner(stream, Some(stop))
}

fn read_frame_inner(
    stream: &mut TcpStream,
    stop: Option<&Arc<AtomicBool>>,
) -> Result<Option<Vec<u8>>> {
    let mut header = [0_u8; FRAME_HEADER_LEN];
    match read_fully(stream, &mut header, stop)? {
        ReadResult::Done => {}
        ReadResult::Stopped => return Ok(None),
        ReadResult::Eof => {
            return Err(ShardCacheError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "FCRP stream closed mid-header",
            )));
        }
    }
    let payload_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    if payload_len > MAX_FRAME_BYTES {
        return Err(ShardCacheError::Protocol(format!(
            "FCRP frame payload exceeds limit ({payload_len} bytes)"
        )));
    }
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload_len);
    frame.extend_from_slice(&header);
    frame.resize(FRAME_HEADER_LEN + payload_len, 0);
    match read_fully(stream, &mut frame[FRAME_HEADER_LEN..], stop)? {
        ReadResult::Done => Ok(Some(frame)),
        ReadResult::Stopped => Ok(None),
        ReadResult::Eof => Err(ShardCacheError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "FCRP stream closed mid-payload",
        ))),
    }
}

enum ReadResult {
    Done,
    Stopped,
    Eof,
}

fn read_fully(
    stream: &mut TcpStream,
    buffer: &mut [u8],
    stop: Option<&Arc<AtomicBool>>,
) -> Result<ReadResult> {
    let mut filled = 0;
    while filled < buffer.len() {
        match stream.read(&mut buffer[filled..]) {
            Ok(0) => return Ok(ReadResult::Eof),
            Ok(n) => filled += n,
            Err(error) if is_timeout(&error) => match stop {
                Some(stop) => {
                    if stop.load(Ordering::SeqCst) {
                        return Ok(ReadResult::Stopped);
                    }
                    continue;
                }
                None => return Err(ShardCacheError::Io(error)),
            },
            Err(error) => return Err(ShardCacheError::Io(error)),
        }
    }
    Ok(ReadResult::Done)
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

/// Handle to a replica's TCP connector thread.
#[derive(Debug)]
pub struct ReplicationReplicaClient {
    stop: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
    state: Arc<Mutex<ReplicationReplica>>,
}

impl ReplicationReplicaClient {
    /// Starts a replica that connects to `config.replica_of`, bootstraps via
    /// snapshot or backlog, and streams live mutations.
    pub fn start(config: ReplicationConfig) -> Result<Self> {
        if !config.enabled {
            return Err(ShardCacheError::Config(
                "replication replica requires replication.enabled = true".into(),
            ));
        }
        let upstream = config.replica_of.clone().ok_or_else(|| {
            ShardCacheError::Config("replication.replica_of is required for replica role".into())
        })?;
        #[cfg(all(target_os = "linux", feature = "monoio"))]
        if monoio_transport::should_use() {
            return monoio_transport::start_replica(upstream, config);
        }

        let stop = Arc::new(AtomicBool::new(false));
        let state = Arc::new(Mutex::new(ReplicationReplica::new(1)));
        let cfg = config;
        let stop_clone = Arc::clone(&stop);
        let state_clone = Arc::clone(&state);
        let join = thread::Builder::new()
            .name("shardcache-replication-replica".into())
            .spawn(move || run_replica_client(upstream, cfg, state_clone, stop_clone))
            .map_err(|error| {
                ShardCacheError::Config(format!("failed to start replica client: {error}"))
            })?;
        Ok(Self {
            stop,
            join: Mutex::new(Some(join)),
            state,
        })
    }

    #[cfg(all(target_os = "linux", feature = "monoio"))]
    fn from_join(
        stop: Arc<AtomicBool>,
        join: JoinHandle<()>,
        state: Arc<Mutex<ReplicationReplica>>,
    ) -> Self {
        Self {
            stop,
            join: Mutex::new(Some(join)),
            state,
        }
    }

    /// Returns the live replica handle. Holds a mutex while in use, so prefer
    /// short, read-only operations.
    pub fn replica(&self) -> Arc<Mutex<ReplicationReplica>> {
        Arc::clone(&self.state)
    }

    pub fn shutdown(&self) -> Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.lock().take() {
            join.join()
                .map_err(|_| ShardCacheError::TaskJoin("replication replica panicked".into()))?;
        }
        Ok(())
    }
}

impl Drop for ReplicationReplicaClient {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn run_replica_client(
    upstream: String,
    config: ReplicationConfig,
    state: Arc<Mutex<ReplicationReplica>>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::SeqCst) {
        match connect_and_stream(&upstream, &config, &state, &stop) {
            Ok(()) => {}
            Err(error) => {
                tracing::warn!("replication replica disconnected: {error}");
            }
        }
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let backoff = Duration::from_millis(config.reconnect_backoff_ms.max(1));
        let step = Duration::from_millis(25);
        let mut slept = Duration::ZERO;
        while slept < backoff && !stop.load(Ordering::SeqCst) {
            let chunk = step.min(backoff.saturating_sub(slept));
            thread::sleep(chunk);
            slept = slept.saturating_add(chunk);
        }
    }
}

fn connect_and_stream(
    upstream: &str,
    config: &ReplicationConfig,
    state: &Arc<Mutex<ReplicationReplica>>,
    stop: &Arc<AtomicBool>,
) -> Result<()> {
    let addr = upstream
        .to_socket_addrs()
        .map_err(|error| {
            ShardCacheError::Config(format!("replica address {upstream} unresolvable: {error}"))
        })?
        .next()
        .ok_or_else(|| {
            ShardCacheError::Config(format!("replica address {upstream} had no entries"))
        })?;
    let mut stream = TcpStream::connect_timeout(
        &addr,
        Duration::from_millis(config.connect_timeout_ms.max(1)),
    )?;
    stream.set_nodelay(true).ok();
    stream
        .set_read_timeout(Some(Duration::from_millis(
            config.connect_timeout_ms.max(1),
        )))
        .ok();
    stream
        .set_write_timeout(Some(Duration::from_millis(config.write_timeout_ms.max(1))))
        .ok();

    let since = state.lock().watermarks().clone();
    let hello = ReplicationHello {
        version: FCRP_VERSION,
        role: HelloRole::Replica,
        auth_token: config.auth_token.clone(),
        since: Some(since),
    };
    write_full_frame(
        &mut stream,
        FrameKind::Hello,
        ReplicationCompressionMode::None,
        0,
        &encode_hello(&hello),
    )?;

    // Read Hello-ack.
    let ack_bytes = read_frame_bytes(&mut stream)?;
    let ack = decode_frame(&ack_bytes)?;
    match ack.kind {
        FrameKind::Hello => {}
        FrameKind::Error => {
            let message = decode_error(&ack.payload).unwrap_or_else(|_| "unknown".to_string());
            return Err(ShardCacheError::Protocol(format!(
                "primary rejected handshake: {message}"
            )));
        }
        other => {
            return Err(ShardCacheError::Protocol(format!(
                "expected Hello ack, got {other:?}"
            )));
        }
    }

    // Use a short read timeout so the loop polls the stop flag without
    // blocking indefinitely.
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .ok();
    let mut pending_snapshot: Option<PendingSnapshot> = None;
    while !stop.load(Ordering::SeqCst) {
        let bytes = match read_frame_bytes_interruptible(&mut stream, stop) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(()),
            Err(ShardCacheError::Io(error))
                if error.kind() == io::ErrorKind::UnexpectedEof
                    || error.kind() == io::ErrorKind::ConnectionReset =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let frame = decode_frame_payload_bytes(bytes::Bytes::from(bytes))?;
        match frame.kind {
            FrameKind::MutationBatch => {
                let mut replica = state.lock();
                replica.apply_frame_bytes_payload(frame)?;
            }
            FrameKind::SnapshotBegin => {
                let watermarks = decode_ack(frame.payload.as_ref())?;
                pending_snapshot = Some(PendingSnapshot {
                    watermarks,
                    entries: Vec::new(),
                });
            }
            FrameKind::SnapshotChunk => {
                let chunk = decode_snapshot_chunk(frame.payload.as_ref())?;
                let Some(slot) = pending_snapshot.as_mut() else {
                    return Err(ShardCacheError::Protocol(
                        "SnapshotChunk arrived without SnapshotBegin".into(),
                    ));
                };
                slot.entries.extend(chunk.entries);
            }
            FrameKind::SnapshotEnd => {
                let Some(snapshot) = pending_snapshot.take() else {
                    return Err(ShardCacheError::Protocol(
                        "SnapshotEnd arrived without SnapshotBegin".into(),
                    ));
                };
                let mut replica = state.lock();
                replica.replace_with_snapshot(super::protocol::ReplicationSnapshot {
                    entries: snapshot.entries,
                    watermarks: snapshot.watermarks,
                });
            }
            FrameKind::Hello => {
                // Ignore unexpected mid-stream Hello frames.
            }
            FrameKind::Ack => {
                // Replica doesn't act on Ack frames today.
            }
            FrameKind::Error => {
                let message =
                    decode_error(frame.payload.as_ref()).unwrap_or_else(|_| "unknown".to_string());
                return Err(ShardCacheError::Protocol(format!(
                    "primary error frame: {message}"
                )));
            }
        }
    }
    Ok(())
}

struct PendingSnapshot {
    watermarks: ShardWatermarks,
    entries: Vec<StoredEntry>,
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::time::Duration;

    use crate::config::{
        ReplicationCompression, ReplicationConfig, ReplicationRole, ReplicationSendPolicy,
    };

    use super::*;

    fn ephemeral_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
        let addr = listener.local_addr().expect("local_addr");
        drop(listener);
        addr.to_string()
    }

    fn primary_config(addr: &str, auth_token: Option<&str>) -> ReplicationConfig {
        ReplicationConfig {
            enabled: true,
            role: ReplicationRole::Primary,
            bind_addr: addr.to_string(),
            replica_of: None,
            auth_token: auth_token.map(str::to_string),
            compression: ReplicationCompression::None,
            send_policy: ReplicationSendPolicy::Immediate,
            batch_max_records: 1,
            batch_max_delay_us: 1_000,
            snapshot_chunk_bytes: 4 * 1024,
            ..ReplicationConfig::default()
        }
    }

    fn replica_config(upstream: &str, auth_token: Option<&str>) -> ReplicationConfig {
        ReplicationConfig {
            enabled: true,
            role: ReplicationRole::Replica,
            bind_addr: String::new(),
            replica_of: Some(upstream.to_string()),
            auth_token: auth_token.map(str::to_string),
            compression: ReplicationCompression::None,
            ..ReplicationConfig::default()
        }
    }

    fn await_value(
        client: &ReplicationReplicaClient,
        key: &[u8],
        deadline: Duration,
    ) -> Option<Vec<u8>> {
        let start = std::time::Instant::now();
        while start.elapsed() < deadline {
            if let Some(value) = client.replica().lock().get(key) {
                return Some(value);
            }
            thread::sleep(Duration::from_millis(10));
        }
        None
    }

    #[test]
    fn live_streaming_round_trip() {
        let addr = ephemeral_addr();
        let primary = Arc::new(
            ReplicatedEmbeddedStore::new(2, primary_config(&addr, None)).expect("primary"),
        );
        let server = ReplicationPrimaryServer::start(
            primary_config(&addr, None),
            primary.primary(),
            Arc::clone(&primary) as Arc<dyn SnapshotProvider>,
        )
        .expect("server");
        let client = ReplicationReplicaClient::start(replica_config(&addr, None)).expect("replica");

        primary.set(b"alpha".to_vec(), b"one".to_vec(), None);
        primary.set(b"beta".to_vec(), b"two".to_vec(), None);
        assert_eq!(
            await_value(&client, b"alpha", Duration::from_secs(3)),
            Some(b"one".to_vec())
        );
        assert_eq!(
            await_value(&client, b"beta", Duration::from_secs(3)),
            Some(b"two".to_vec())
        );
        client.shutdown().ok();
        server.shutdown().ok();
    }

    #[test]
    fn snapshot_bootstrap_when_backlog_empty() {
        let addr = ephemeral_addr();
        let primary = Arc::new(
            ReplicatedEmbeddedStore::new(2, primary_config(&addr, None)).expect("primary"),
        );
        // Populate before the replica connects.
        for i in 0..32 {
            primary.set(format!("key-{i}").into_bytes(), b"v".to_vec(), None);
        }
        thread::sleep(Duration::from_millis(20));
        let mut tight_cfg = primary_config(&addr, None);
        tight_cfg.backlog_bytes = 1; // force snapshot path
        let primary =
            Arc::new(ReplicatedEmbeddedStore::new(2, tight_cfg.clone()).expect("primary2"));
        for i in 0..32 {
            primary.set(format!("key-{i}").into_bytes(), b"v".to_vec(), None);
        }
        let server = ReplicationPrimaryServer::start(
            tight_cfg,
            primary.primary(),
            Arc::clone(&primary) as Arc<dyn SnapshotProvider>,
        )
        .expect("server");
        let client = ReplicationReplicaClient::start(replica_config(&addr, None)).expect("replica");

        for i in 0..32 {
            let key = format!("key-{i}").into_bytes();
            assert_eq!(
                await_value(&client, &key, Duration::from_secs(5)),
                Some(b"v".to_vec()),
                "missing {i}"
            );
        }
        client.shutdown().ok();
        server.shutdown().ok();
    }

    #[test]
    fn auth_token_required_when_configured() {
        let addr = ephemeral_addr();
        let primary = Arc::new(
            ReplicatedEmbeddedStore::new(2, primary_config(&addr, Some("secret")))
                .expect("primary"),
        );
        let server = ReplicationPrimaryServer::start(
            primary_config(&addr, Some("secret")),
            primary.primary(),
            Arc::clone(&primary) as Arc<dyn SnapshotProvider>,
        )
        .expect("server");
        // Wrong token — replica connect_and_stream will return Err and the
        // client retries; we just confirm no data leaks.
        let client = ReplicationReplicaClient::start(replica_config(&addr, Some("wrong")))
            .expect("client-start");
        primary.set(b"alpha".to_vec(), b"one".to_vec(), None);
        thread::sleep(Duration::from_millis(200));
        assert!(client.replica().lock().get(b"alpha").is_none());
        client.shutdown().ok();
        server.shutdown().ok();
    }
}
