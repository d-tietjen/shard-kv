use std::io;
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use crossbeam_channel::{Receiver, TryRecvError};
use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
use parking_lot::Mutex;

use crate::config::ReplicationConfig;
use crate::monoio_runtime::MonoioRuntime;
use crate::storage::StoredEntry;
use crate::{FastCacheError, Result};

use super::super::ReplicationFrameBytes;
use super::super::backlog::BacklogCatchUp;
use super::super::batcher::ReplicationPrimary;
use super::super::embedded::ReplicationReplica;
use super::super::protocol::{
    FCRP_VERSION, FrameKind, HelloRole, ReplicationCompressionMode, ReplicationHello,
    ReplicationSnapshotChunk, ShardWatermarks, decode_ack, decode_error, decode_frame,
    decode_frame_payload_bytes, decode_hello, decode_snapshot_chunk, encode_ack, encode_error,
    encode_frame, encode_hello, encode_snapshot_chunk,
};
use super::{
    FRAME_HEADER_LEN, MAX_FRAME_BYTES, ReplicationPrimaryServer, ReplicationReplicaClient,
    SnapshotProvider, auth_ok,
};

const USE_MONOIO_ENV: &str = "FAST_CACHE_REPLICATION_USE_MONOIO";
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const LIVE_POLL_INTERVAL: Duration = Duration::from_micros(100);
const REPLICA_READ_POLL_INTERVAL: Duration = Duration::from_millis(200);

pub(super) fn should_use() -> bool {
    MonoioRuntime::enabled_by_env(USE_MONOIO_ENV)
}

pub(super) fn start_primary(
    config: ReplicationConfig,
    primary: Arc<ReplicationPrimary>,
    snapshots: Arc<dyn SnapshotProvider>,
) -> Result<ReplicationPrimaryServer> {
    let listener = TcpListener::bind(&config.bind_addr).map_err(|error| {
        FastCacheError::Config(format!(
            "replication primary failed to bind {}: {error}",
            config.bind_addr
        ))
    })?;
    listener.set_nonblocking(true).map_err(|error| {
        FastCacheError::Config(format!(
            "replication primary set_nonblocking failed: {error}"
        ))
    })?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);
    let join = thread::Builder::new()
        .name("fast-cache-replication-listener-monoio".into())
        .spawn(move || {
            let result = MonoioRuntime::block_on("replication primary", || async move {
                run_primary_listener(listener, config, primary, snapshots, stop_clone).await
            });
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!("monoio replication primary stopped: {error}"),
                Err(error) => tracing::error!("monoio replication primary failed: {error}"),
            }
        })
        .map_err(|error| {
            FastCacheError::Config(format!(
                "failed to start monoio replication listener: {error}"
            ))
        })?;
    Ok(ReplicationPrimaryServer::from_join(stop, join))
}

pub(super) fn start_replica(
    upstream: String,
    config: ReplicationConfig,
) -> Result<ReplicationReplicaClient> {
    let stop = Arc::new(AtomicBool::new(false));
    let state = Arc::new(Mutex::new(ReplicationReplica::new(1)));
    let stop_clone = Arc::clone(&stop);
    let state_clone = Arc::clone(&state);
    let join = thread::Builder::new()
        .name("fast-cache-replication-replica-monoio".into())
        .spawn(move || {
            let result = MonoioRuntime::block_on("replication replica", || async move {
                run_replica_client(upstream, config, state_clone, stop_clone).await
            });
            if let Err(error) = result {
                tracing::error!("monoio replication replica failed: {error}");
            }
        })
        .map_err(|error| {
            FastCacheError::Config(format!("failed to start monoio replica client: {error}"))
        })?;
    Ok(ReplicationReplicaClient::from_join(stop, join, state))
}

async fn run_primary_listener(
    listener: TcpListener,
    config: ReplicationConfig,
    primary: Arc<ReplicationPrimary>,
    snapshots: Arc<dyn SnapshotProvider>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let listener = monoio::net::TcpListener::from_std(listener).map_err(FastCacheError::Io)?;
    let active = Arc::new(AtomicUsize::new(0));
    while !stop.load(Ordering::SeqCst) {
        monoio::select! {
            accepted = listener.accept() => {
                accept_replica(
                    accepted,
                    &config,
                    Arc::clone(&primary),
                    Arc::clone(&snapshots),
                    Arc::clone(&stop),
                    Arc::clone(&active),
                );
            }
            _ = monoio::time::sleep(ACCEPT_POLL_INTERVAL) => {}
        }
    }
    Ok(())
}

fn accept_replica(
    accepted: io::Result<(monoio::net::TcpStream, SocketAddr)>,
    config: &ReplicationConfig,
    primary: Arc<ReplicationPrimary>,
    snapshots: Arc<dyn SnapshotProvider>,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
) {
    match accepted {
        Ok((stream, peer)) if active.load(Ordering::SeqCst) >= config.max_replicas => {
            tracing::warn!(
                "rejecting monoio replication client {peer}: max_replicas {} reached",
                config.max_replicas
            );
            drop(stream);
        }
        Ok((stream, peer)) => {
            let cfg = config.clone();
            active.fetch_add(1, Ordering::SeqCst);
            monoio::spawn(async move {
                if let Err(error) = serve_replica(stream, peer, cfg, primary, snapshots, stop).await
                {
                    tracing::warn!("monoio replication worker for {peer} terminated: {error}");
                }
                active.fetch_sub(1, Ordering::SeqCst);
            });
        }
        Err(error) => tracing::warn!("monoio replication listener accept failed: {error}"),
    }
}

async fn serve_replica(
    mut stream: monoio::net::TcpStream,
    peer: SocketAddr,
    config: ReplicationConfig,
    primary: Arc<ReplicationPrimary>,
    snapshots: Arc<dyn SnapshotProvider>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    stream.set_nodelay(true).ok();
    let timeout = Duration::from_millis(config.connect_timeout_ms.max(1));
    let hello_frame = match monoio::time::timeout(timeout, read_frame_bytes(&mut stream)).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => return Err(error),
        Err(_) => return Ok(()),
    };
    let frame = decode_frame(&hello_frame)?;
    match frame.kind {
        FrameKind::Hello => {}
        _ => {
            send_error(&mut stream, "expected Hello frame").await?;
            return Err(FastCacheError::Protocol(format!(
                "replica {peer} sent {:?} before Hello",
                frame.kind
            )));
        }
    }
    let hello = decode_hello(&frame.payload)?;
    match hello.version == FCRP_VERSION {
        true => {}
        false => {
            send_error(&mut stream, "unsupported FCRP version").await?;
            return Err(FastCacheError::Protocol(format!(
                "replica {peer} requested FCRP version {}",
                hello.version
            )));
        }
    }
    match auth_ok(config.auth_token.as_deref(), hello.auth_token.as_deref()) {
        true => {}
        false => {
            send_error(&mut stream, "invalid auth token").await?;
            return Err(FastCacheError::Protocol(format!(
                "replica {peer} sent invalid auth token"
            )));
        }
    }

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
    )
    .await?;

    let subscription = primary.subscribe(config.subscriber_channel_capacity);
    let since = hello
        .since
        .clone()
        .unwrap_or_else(|| ShardWatermarks::new(primary.shard_count()));
    let live_start = match primary.catch_up_since(&since)? {
        BacklogCatchUp::Available(frames) => {
            for frame in frames {
                write_raw_frame(&mut stream, frame).await?;
            }
            primary.current_watermarks()
        }
        BacklogCatchUp::NeedsSnapshot => {
            let snapshot = snapshots.snapshot();
            stream_snapshot(&mut stream, &snapshot, &config).await?;
            snapshot.watermarks
        }
    };

    drain_buffered(&mut stream, &subscription, &live_start, &primary).await?;
    forward_live_frames(&mut stream, &subscription, &stop).await
}

async fn drain_buffered(
    stream: &mut monoio::net::TcpStream,
    subscription: &Receiver<ReplicationFrameBytes>,
    bootstrap_high: &ShardWatermarks,
    primary: &Arc<ReplicationPrimary>,
) -> Result<()> {
    while let Ok(frame) = subscription.try_recv() {
        write_raw_frame(stream, frame).await?;
    }
    if let BacklogCatchUp::Available(frames) = primary.catch_up_since(bootstrap_high)? {
        for frame in frames {
            write_raw_frame(stream, frame).await?;
        }
    }
    Ok(())
}

async fn forward_live_frames(
    stream: &mut monoio::net::TcpStream,
    subscription: &Receiver<ReplicationFrameBytes>,
    stop: &Arc<AtomicBool>,
) -> Result<()> {
    while !stop.load(Ordering::SeqCst) {
        match subscription.try_recv() {
            Ok(frame) => write_raw_frame(stream, frame).await?,
            Err(TryRecvError::Empty) => monoio::time::sleep(LIVE_POLL_INTERVAL).await,
            Err(TryRecvError::Disconnected) => break,
        }
    }
    Ok(())
}

async fn stream_snapshot(
    stream: &mut monoio::net::TcpStream,
    snapshot: &super::super::protocol::ReplicationSnapshot,
    config: &ReplicationConfig,
) -> Result<()> {
    write_full_frame(
        stream,
        FrameKind::SnapshotBegin,
        ReplicationCompressionMode::None,
        0,
        &encode_ack(&snapshot.watermarks),
    )
    .await?;

    let target = config.snapshot_chunk_bytes.max(4 * 1024);
    let mut chunk_index = 0u64;
    let mut buffer: Vec<StoredEntry> = Vec::new();
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
            )
            .await?;
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
        )
        .await?;
    }
    write_full_frame(
        stream,
        FrameKind::SnapshotEnd,
        ReplicationCompressionMode::None,
        0,
        &encode_ack(&snapshot.watermarks),
    )
    .await
}

async fn send_error(stream: &mut monoio::net::TcpStream, message: &str) -> Result<()> {
    write_full_frame(
        stream,
        FrameKind::Error,
        ReplicationCompressionMode::None,
        0,
        &encode_error(message),
    )
    .await
}

async fn write_full_frame(
    stream: &mut monoio::net::TcpStream,
    kind: FrameKind,
    compression: ReplicationCompressionMode,
    zstd_level: i32,
    payload: &[u8],
) -> Result<()> {
    let frame = encode_frame(kind, compression, zstd_level, payload)?;
    write_raw_vec(stream, frame).await
}

async fn write_raw_vec(stream: &mut monoio::net::TcpStream, bytes: Vec<u8>) -> Result<()> {
    write_all_owned(stream, Bytes::from(bytes)).await
}

async fn write_raw_frame(
    stream: &mut monoio::net::TcpStream,
    bytes: ReplicationFrameBytes,
) -> Result<()> {
    write_all_owned(stream, bytes).await
}

async fn write_all_owned<T>(stream: &mut monoio::net::TcpStream, buffer: T) -> Result<()>
where
    T: monoio::buf::IoBuf + 'static,
{
    let (result, _buffer) = stream.write_all(buffer).await;
    result.map(|_| ()).map_err(FastCacheError::Io)
}

async fn read_frame_bytes(stream: &mut monoio::net::TcpStream) -> Result<Vec<u8>> {
    let header = read_exact_vec(stream, FRAME_HEADER_LEN).await?;
    let payload_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    if payload_len > MAX_FRAME_BYTES {
        return Err(FastCacheError::Protocol(format!(
            "FCRP frame payload exceeds limit ({payload_len} bytes)"
        )));
    }
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload_len);
    frame.extend_from_slice(&header);
    match payload_len {
        0 => {}
        len => frame.extend_from_slice(&read_exact_vec(stream, len).await?),
    }
    Ok(frame)
}

async fn read_exact_vec(stream: &mut monoio::net::TcpStream, len: usize) -> Result<Vec<u8>> {
    let (result, buffer) = stream.read_exact(vec![0_u8; len]).await;
    result.map(|_| buffer).map_err(FastCacheError::Io)
}

async fn run_replica_client(
    upstream: String,
    config: ReplicationConfig,
    state: Arc<Mutex<ReplicationReplica>>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::SeqCst) {
        match connect_and_stream(&upstream, &config, &state, &stop).await {
            Ok(()) => {}
            Err(error) => tracing::warn!("monoio replication replica disconnected: {error}"),
        }
        if stop.load(Ordering::SeqCst) {
            break;
        }
        sleep_backoff(config.reconnect_backoff_ms.max(1), &stop).await;
    }
}

async fn connect_and_stream(
    upstream: &str,
    config: &ReplicationConfig,
    state: &Arc<Mutex<ReplicationReplica>>,
    stop: &Arc<AtomicBool>,
) -> Result<()> {
    let addr = upstream
        .to_socket_addrs()
        .map_err(|error| {
            FastCacheError::Config(format!("replica address {upstream} unresolvable: {error}"))
        })?
        .next()
        .ok_or_else(|| {
            FastCacheError::Config(format!("replica address {upstream} had no entries"))
        })?;
    let timeout = Duration::from_millis(config.connect_timeout_ms.max(1));
    let mut stream = monoio::time::timeout(timeout, monoio::net::TcpStream::connect_addr(addr))
        .await
        .map_err(|_| {
            FastCacheError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "replication connect timed out",
            ))
        })??;
    stream.set_nodelay(true).ok();

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
    )
    .await?;

    let ack_bytes = read_frame_bytes(&mut stream).await?;
    let ack = decode_frame(&ack_bytes)?;
    match ack.kind {
        FrameKind::Hello => {}
        FrameKind::Error => {
            let message = decode_error(&ack.payload).unwrap_or_else(|_| "unknown".to_string());
            return Err(FastCacheError::Protocol(format!(
                "primary rejected handshake: {message}"
            )));
        }
        other => {
            return Err(FastCacheError::Protocol(format!(
                "expected Hello ack, got {other:?}"
            )));
        }
    }

    stream_replica_frames(&mut stream, state, stop).await
}

async fn stream_replica_frames(
    stream: &mut monoio::net::TcpStream,
    state: &Arc<Mutex<ReplicationReplica>>,
    stop: &Arc<AtomicBool>,
) -> Result<()> {
    let mut pending_snapshot: Option<PendingSnapshot> = None;
    while !stop.load(Ordering::SeqCst) {
        let bytes =
            match monoio::time::timeout(REPLICA_READ_POLL_INTERVAL, read_frame_bytes(stream)).await
            {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(FastCacheError::Io(error)))
                    if error.kind() == io::ErrorKind::UnexpectedEof
                        || error.kind() == io::ErrorKind::ConnectionReset =>
                {
                    return Ok(());
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => continue,
            };
        let frame = decode_frame_payload_bytes(Bytes::from(bytes))?;
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
                    return Err(FastCacheError::Protocol(
                        "SnapshotChunk arrived without SnapshotBegin".into(),
                    ));
                };
                slot.entries.extend(chunk.entries);
            }
            FrameKind::SnapshotEnd => {
                let Some(snapshot) = pending_snapshot.take() else {
                    return Err(FastCacheError::Protocol(
                        "SnapshotEnd arrived without SnapshotBegin".into(),
                    ));
                };
                let mut replica = state.lock();
                replica.replace_with_snapshot(super::super::protocol::ReplicationSnapshot {
                    entries: snapshot.entries,
                    watermarks: snapshot.watermarks,
                });
            }
            FrameKind::Hello | FrameKind::Ack => {}
            FrameKind::Error => {
                let message =
                    decode_error(frame.payload.as_ref()).unwrap_or_else(|_| "unknown".to_string());
                return Err(FastCacheError::Protocol(format!(
                    "primary error frame: {message}"
                )));
            }
        }
    }
    Ok(())
}

async fn sleep_backoff(backoff_ms: u64, stop: &Arc<AtomicBool>) {
    let backoff = Duration::from_millis(backoff_ms);
    let step = Duration::from_millis(25);
    let mut slept = Duration::ZERO;
    while slept < backoff && !stop.load(Ordering::SeqCst) {
        let sleep_for = step.min(backoff.saturating_sub(slept));
        monoio::time::sleep(sleep_for).await;
        slept = slept.saturating_add(sleep_for);
    }
}

struct PendingSnapshot {
    watermarks: ShardWatermarks,
    entries: Vec<StoredEntry>,
}
