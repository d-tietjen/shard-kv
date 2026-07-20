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
use crate::{Result, ShardCacheError};

use super::super::ReplicationFrameBytes;
use super::super::backlog::BacklogCatchUp;
use super::super::batcher::ReplicationPrimary;
use super::super::embedded::ReplicationReplica;
use super::super::protocol::{
    FCRP_VERSION, FrameKind, HelloRole, ReplicationCompressionMode, ReplicationHello,
    ReplicationSnapshotChunk, ShardWatermarks, decode_ack, decode_error, decode_frame,
    decode_frame_payload_bytes, decode_hello, decode_snapshot_chunk_limited, encode_ack,
    encode_error, encode_frame, encode_hello, encode_snapshot_chunk,
};
use super::{
    FRAME_HEADER_LEN, MAX_FRAME_BYTES, MAX_HELLO_FRAME_BYTES, PendingSnapshot,
    ReplicationPrimaryServer, ReplicationReplicaClient, SnapshotProvider, auth_ok,
    validate_primary_hello,
};

const USE_MONOIO_ENV: &str = "SHARDCACHE_REPLICATION_USE_MONOIO";
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const LIVE_POLL_INTERVAL: Duration = Duration::from_micros(100);

pub(super) fn should_use() -> bool {
    MonoioRuntime::enabled_by_env(USE_MONOIO_ENV)
}

fn require_loopback(address: SocketAddr, endpoint: &str) -> Result<()> {
    if address.ip().is_loopback() {
        return Ok(());
    }
    Err(ShardCacheError::Config(format!(
        "non-loopback replication {endpoint} requires TLS"
    )))
}

pub(super) fn start_primary(
    config: ReplicationConfig,
    primary: Arc<ReplicationPrimary>,
    snapshots: Arc<dyn SnapshotProvider>,
) -> Result<ReplicationPrimaryServer> {
    let listener = TcpListener::bind(&config.bind_addr).map_err(|error| {
        ShardCacheError::Config(format!(
            "replication primary failed to bind {}: {error}",
            config.bind_addr
        ))
    })?;
    require_loopback(listener.local_addr()?, "primary listener")?;
    listener.set_nonblocking(true).map_err(|error| {
        ShardCacheError::Config(format!(
            "replication primary set_nonblocking failed: {error}"
        ))
    })?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);
    let join = thread::Builder::new()
        .name("shardcache-replication-listener-monoio".into())
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
            ShardCacheError::Config(format!(
                "failed to start monoio replication listener: {error}"
            ))
        })?;
    Ok(ReplicationPrimaryServer::from_join(stop, join))
}

pub(super) fn start_replica(
    upstream: String,
    config: ReplicationConfig,
) -> Result<ReplicationReplicaClient> {
    let addresses = upstream.to_socket_addrs().map_err(|error| {
        ShardCacheError::Config(format!("replica address {upstream} unresolvable: {error}"))
    })?;
    for address in addresses {
        require_loopback(address, "replica connection")?;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let state = Arc::new(Mutex::new(ReplicationReplica::uninitialized()));
    let stop_clone = Arc::clone(&stop);
    let state_clone = Arc::clone(&state);
    let join = thread::Builder::new()
        .name("shardcache-replication-replica-monoio".into())
        .spawn(move || {
            let result = MonoioRuntime::block_on("replication replica", || async move {
                run_replica_client(upstream, config, state_clone, stop_clone).await
            });
            if let Err(error) = result {
                tracing::error!("monoio replication replica failed: {error}");
            }
        })
        .map_err(|error| {
            ShardCacheError::Config(format!("failed to start monoio replica client: {error}"))
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
    let listener = monoio::net::TcpListener::from_std(listener).map_err(ShardCacheError::Io)?;
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
    let write_timeout = Duration::from_millis(config.write_timeout_ms.max(1));
    let hello_frame = match monoio::time::timeout(
        timeout,
        read_frame_bytes_limited(&mut stream, MAX_HELLO_FRAME_BYTES),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => return Err(error),
        Err(_) => return Ok(()),
    };
    let frame = decode_frame(&hello_frame)?;
    match frame.kind {
        FrameKind::Hello => {}
        _ => {
            send_error(&mut stream, "expected Hello frame", write_timeout).await?;
            return Err(ShardCacheError::Protocol(format!(
                "replica {peer} sent {:?} before Hello",
                frame.kind
            )));
        }
    }
    let hello = decode_hello(&frame.payload)?;
    match hello.version == FCRP_VERSION {
        true => {}
        false => {
            send_error(&mut stream, "unsupported FCRP version", write_timeout).await?;
            return Err(ShardCacheError::Protocol(format!(
                "replica {peer} requested FCRP version {}",
                hello.version
            )));
        }
    }
    if hello.role != HelloRole::Replica {
        send_error(&mut stream, "invalid replication Hello role", write_timeout).await?;
        return Err(ShardCacheError::Protocol(format!(
            "replica {peer} sent an invalid Hello role"
        )));
    }
    if hello
        .since
        .as_ref()
        .is_some_and(|watermarks| watermarks.as_slice().len() != primary.shard_count())
    {
        send_error(
            &mut stream,
            "replica shard topology does not match primary",
            write_timeout,
        )
        .await?;
        return Err(ShardCacheError::Protocol(format!(
            "replica {peer} sent {} watermarks for a {}-shard primary",
            hello
                .since
                .as_ref()
                .map_or(0, |value| value.as_slice().len()),
            primary.shard_count()
        )));
    }
    match auth_ok(config.auth_token.as_deref(), hello.auth_token.as_deref()) {
        true => {}
        false => {
            send_error(&mut stream, "invalid auth token", write_timeout).await?;
            return Err(ShardCacheError::Protocol(format!(
                "replica {peer} sent invalid auth token"
            )));
        }
    }

    let ack = ReplicationHello {
        version: FCRP_VERSION,
        role: HelloRole::Primary,
        auth_token: None,
        since: Some(primary.current_watermarks()),
    };
    write_full_frame_with_timeout(
        &mut stream,
        FrameKind::Hello,
        ReplicationCompressionMode::None,
        0,
        &encode_hello(&ack),
        write_timeout,
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
                write_raw_frame_with_timeout(
                    &mut stream,
                    frame,
                    Duration::from_millis(config.write_timeout_ms.max(1)),
                )
                .await?;
            }
            primary.current_watermarks()
        }
        BacklogCatchUp::NeedsSnapshot => {
            let snapshot = snapshots.snapshot();
            stream_snapshot(&mut stream, &snapshot, &config).await?;
            snapshot.watermarks
        }
    };

    drain_buffered(&mut stream, &subscription, &live_start, &primary, &config).await?;
    forward_live_frames(&mut stream, &subscription, &stop, &config).await
}

async fn drain_buffered(
    stream: &mut monoio::net::TcpStream,
    subscription: &Receiver<ReplicationFrameBytes>,
    bootstrap_high: &ShardWatermarks,
    primary: &Arc<ReplicationPrimary>,
    config: &ReplicationConfig,
) -> Result<()> {
    while let Ok(frame) = subscription.try_recv() {
        write_raw_frame_with_timeout(
            stream,
            frame,
            Duration::from_millis(config.write_timeout_ms.max(1)),
        )
        .await?;
    }
    if let BacklogCatchUp::Available(frames) = primary.catch_up_since(bootstrap_high)? {
        for frame in frames {
            write_raw_frame_with_timeout(
                stream,
                frame,
                Duration::from_millis(config.write_timeout_ms.max(1)),
            )
            .await?;
        }
    }
    Ok(())
}

async fn forward_live_frames(
    stream: &mut monoio::net::TcpStream,
    subscription: &Receiver<ReplicationFrameBytes>,
    stop: &Arc<AtomicBool>,
    config: &ReplicationConfig,
) -> Result<()> {
    while !stop.load(Ordering::SeqCst) {
        match subscription.try_recv() {
            Ok(frame) => {
                write_raw_frame_with_timeout(
                    stream,
                    frame,
                    Duration::from_millis(config.write_timeout_ms.max(1)),
                )
                .await?
            }
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
    write_full_frame_with_timeout(
        stream,
        FrameKind::SnapshotBegin,
        ReplicationCompressionMode::None,
        0,
        &encode_ack(&snapshot.watermarks),
        Duration::from_millis(config.write_timeout_ms.max(1)),
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
            write_full_frame_with_timeout(
                stream,
                FrameKind::SnapshotChunk,
                compression,
                config.zstd_level,
                &payload,
                Duration::from_millis(config.write_timeout_ms.max(1)),
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
        write_full_frame_with_timeout(
            stream,
            FrameKind::SnapshotChunk,
            ReplicationCompressionMode::None,
            0,
            &payload,
            Duration::from_millis(config.write_timeout_ms.max(1)),
        )
        .await?;
    }
    write_full_frame_with_timeout(
        stream,
        FrameKind::SnapshotEnd,
        ReplicationCompressionMode::None,
        0,
        &encode_ack(&snapshot.watermarks),
        Duration::from_millis(config.write_timeout_ms.max(1)),
    )
    .await
}

async fn send_error(
    stream: &mut monoio::net::TcpStream,
    message: &str,
    timeout: Duration,
) -> Result<()> {
    write_full_frame_with_timeout(
        stream,
        FrameKind::Error,
        ReplicationCompressionMode::None,
        0,
        &encode_error(message),
        timeout,
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

async fn write_full_frame_with_timeout(
    stream: &mut monoio::net::TcpStream,
    kind: FrameKind,
    compression: ReplicationCompressionMode,
    zstd_level: i32,
    payload: &[u8],
    timeout: Duration,
) -> Result<()> {
    match monoio::time::timeout(
        timeout,
        write_full_frame(stream, kind, compression, zstd_level, payload),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(ShardCacheError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "replication frame write deadline exceeded",
        ))),
    }
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

async fn write_raw_frame_with_timeout(
    stream: &mut monoio::net::TcpStream,
    bytes: ReplicationFrameBytes,
    timeout: Duration,
) -> Result<()> {
    match monoio::time::timeout(timeout, write_raw_frame(stream, bytes)).await {
        Ok(result) => result,
        Err(_) => Err(ShardCacheError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "replication frame write deadline exceeded",
        ))),
    }
}

async fn write_all_owned<T>(stream: &mut monoio::net::TcpStream, buffer: T) -> Result<()>
where
    T: monoio::buf::IoBuf + 'static,
{
    let (result, _buffer) = stream.write_all(buffer).await;
    result.map(|_| ()).map_err(ShardCacheError::Io)
}

async fn read_frame_bytes_limited(
    stream: &mut monoio::net::TcpStream,
    max_payload_bytes: usize,
) -> Result<Vec<u8>> {
    let header = read_exact_vec(stream, FRAME_HEADER_LEN).await?;
    let kind = FrameKind::from_u8(header[5])?;
    let max_payload_bytes = match kind {
        FrameKind::Hello
        | FrameKind::SnapshotBegin
        | FrameKind::SnapshotEnd
        | FrameKind::Ack
        | FrameKind::Error => max_payload_bytes.min(MAX_HELLO_FRAME_BYTES),
        FrameKind::SnapshotChunk | FrameKind::MutationBatch => max_payload_bytes,
    };
    let payload_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    let uncompressed_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
    if payload_len > max_payload_bytes || uncompressed_len > max_payload_bytes {
        return Err(ShardCacheError::Protocol(format!(
            "FCRP frame payload exceeds limit ({payload_len}/{uncompressed_len} bytes)"
        )));
    }
    let frame_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| ShardCacheError::Protocol("FCRP frame length overflow".into()))?;
    let mut frame = Vec::new();
    frame.try_reserve_exact(frame_len).map_err(|_| {
        ShardCacheError::Protocol(format!("FCRP frame allocation failed ({frame_len} bytes)"))
    })?;
    frame.extend_from_slice(&header);
    match payload_len {
        0 => {}
        len => frame.extend_from_slice(&read_exact_vec(stream, len).await?),
    }
    Ok(frame)
}

async fn read_exact_vec(stream: &mut monoio::net::TcpStream, len: usize) -> Result<Vec<u8>> {
    let (result, buffer) = stream.read_exact(vec![0_u8; len]).await;
    result.map(|_| buffer).map_err(ShardCacheError::Io)
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
            ShardCacheError::Config(format!("replica address {upstream} unresolvable: {error}"))
        })?
        .next()
        .ok_or_else(|| {
            ShardCacheError::Config(format!("replica address {upstream} had no entries"))
        })?;
    require_loopback(addr, "replica connection")?;
    let timeout = Duration::from_millis(config.connect_timeout_ms.max(1));
    let mut stream = monoio::time::timeout(timeout, monoio::net::TcpStream::connect_addr(addr))
        .await
        .map_err(|_| {
            ShardCacheError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "replication connect timed out",
            ))
        })??;
    stream.set_nodelay(true).ok();

    let since = {
        let replica = state.lock();
        replica
            .topology_initialized()
            .then(|| replica.watermarks().clone())
    };
    let hello = ReplicationHello {
        version: FCRP_VERSION,
        role: HelloRole::Replica,
        auth_token: config.auth_token.clone(),
        since,
    };
    write_full_frame_with_timeout(
        &mut stream,
        FrameKind::Hello,
        ReplicationCompressionMode::None,
        0,
        &encode_hello(&hello),
        Duration::from_millis(config.write_timeout_ms.max(1)),
    )
    .await?;

    let ack_bytes = monoio::time::timeout(
        timeout,
        read_frame_bytes_limited(&mut stream, MAX_HELLO_FRAME_BYTES),
    )
    .await
    .map_err(|_| {
        ShardCacheError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "replication Hello acknowledgement timed out",
        ))
    })??;
    let ack = decode_frame(&ack_bytes)?;
    match ack.kind {
        FrameKind::Hello => {
            let hello = validate_primary_hello(&ack.payload)?;
            let shard_count = hello
                .since
                .as_ref()
                .map(|watermarks| watermarks.as_slice().len())
                .unwrap_or(0);
            state.lock().ensure_topology(shard_count)?;
        }
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

    stream_replica_frames(&mut stream, config, state, stop).await
}

async fn stream_replica_frames(
    stream: &mut monoio::net::TcpStream,
    config: &ReplicationConfig,
    state: &Arc<Mutex<ReplicationReplica>>,
    stop: &Arc<AtomicBool>,
) -> Result<()> {
    let mut pending_snapshot: Option<PendingSnapshot> = None;
    while !stop.load(Ordering::SeqCst) {
        let bytes = match monoio::time::timeout(
            Duration::from_millis(config.read_timeout_ms.max(1)),
            read_frame_bytes_limited(stream, config.receive_max_frame_bytes.min(MAX_FRAME_BYTES)),
        )
        .await
        {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(ShardCacheError::Io(error)))
                if error.kind() == io::ErrorKind::UnexpectedEof
                    || error.kind() == io::ErrorKind::ConnectionReset =>
            {
                return Ok(());
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(ShardCacheError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "replication frame read deadline exceeded",
                )));
            }
        };
        let frame = decode_frame_payload_bytes(Bytes::from(bytes))?;
        match frame.kind {
            FrameKind::MutationBatch => {
                if pending_snapshot.is_some() {
                    return Err(ShardCacheError::Protocol(
                        "MutationBatch arrived during snapshot bootstrap".into(),
                    ));
                }
                let mut replica = state.lock();
                replica.apply_frame_bytes_payload(frame)?;
            }
            FrameKind::SnapshotBegin => {
                if pending_snapshot.is_some() {
                    return Err(ShardCacheError::Protocol(
                        "duplicate SnapshotBegin during snapshot bootstrap".into(),
                    ));
                }
                let watermarks = decode_ack(frame.payload.as_ref())?;
                pending_snapshot = Some(PendingSnapshot::new(
                    watermarks,
                    config.snapshot_receive_max_bytes,
                    config.snapshot_receive_max_entries,
                ));
            }
            FrameKind::SnapshotChunk => {
                let Some(slot) = pending_snapshot.as_mut() else {
                    return Err(ShardCacheError::Protocol(
                        "SnapshotChunk arrived without SnapshotBegin".into(),
                    ));
                };
                let chunk = decode_snapshot_chunk_limited(
                    frame.payload.as_ref(),
                    slot.remaining_entries(),
                    slot.remaining_bytes(),
                    Some(slot.watermarks.as_slice().len()),
                )?;
                slot.push(chunk)?;
            }
            FrameKind::SnapshotEnd => {
                let end_watermarks = decode_ack(frame.payload.as_ref())?;
                let Some(snapshot) = pending_snapshot.take() else {
                    return Err(ShardCacheError::Protocol(
                        "SnapshotEnd arrived without SnapshotBegin".into(),
                    ));
                };
                let snapshot = snapshot.finish(end_watermarks)?;
                let mut replica = state.lock();
                replica.try_replace_with_snapshot(snapshot)?;
            }
            FrameKind::Hello | FrameKind::Ack => {
                return Err(ShardCacheError::Protocol(
                    "unexpected control frame after replication handshake".into(),
                ));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monoio_transport_rejects_every_non_loopback_address() {
        assert!(require_loopback("127.0.0.1:7631".parse().unwrap(), "replica connection").is_ok());
        assert!(require_loopback("[::1]:7631".parse().unwrap(), "replica connection").is_ok());
        assert!(require_loopback("0.0.0.0:7631".parse().unwrap(), "primary listener").is_err());
        assert!(require_loopback("192.0.2.1:7631".parse().unwrap(), "replica connection").is_err());
    }
}
