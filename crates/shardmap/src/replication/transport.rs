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
#[cfg(feature = "scnp-tls")]
use std::time::Instant;

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
const MAX_HELLO_FRAME_BYTES: usize = 128 * 1024;
#[cfg(feature = "scnp-tls")]
const FCRP_ALPN: &[u8] = b"fcrp/2";

trait ReplicationIo: Read + Write {}
impl<T: Read + Write> ReplicationIo for T {}

fn validate_primary_hello(payload: &[u8]) -> Result<ReplicationHello> {
    let hello = decode_hello(payload)?;
    if hello.version != FCRP_VERSION || hello.role != HelloRole::Primary {
        return Err(ShardCacheError::Protocol(
            "replication peer returned an invalid primary Hello acknowledgement".into(),
        ));
    }
    if hello.auth_token.is_some() || hello.since.is_none() {
        return Err(ShardCacheError::Protocol(
            "replication primary Hello acknowledgement contains invalid fields".into(),
        ));
    }
    Ok(hello)
}

fn read_replication_token(path: &std::path::Path) -> Result<String> {
    let token = std::fs::read_to_string(path).map_err(|error| {
        ShardCacheError::Config(format!(
            "failed to read replication auth token {}: {error}",
            path.display()
        ))
    })?;
    let token = token.trim_end_matches(['\r', '\n']).to_string();
    if token.is_empty() || token.len() > u16::MAX as usize {
        return Err(ShardCacheError::Config(format!(
            "replication auth token {} must contain 1..=65535 bytes",
            path.display()
        )));
    }
    Ok(token)
}

fn current_replication_token(config: &ReplicationConfig) -> Result<Option<String>> {
    match config.auth_token_path.as_deref() {
        Some(path) => read_replication_token(path).map(Some),
        None => Ok(config.auth_token.clone()),
    }
}

fn replication_auth_ok(config: &ReplicationConfig, presented: Option<&str>) -> Result<bool> {
    let current = current_replication_token(config)?;
    if auth_ok(current.as_deref(), presented) {
        return Ok(true);
    }
    match config.previous_auth_token_path.as_deref() {
        Some(path) => {
            let previous = read_replication_token(path)?;
            Ok(presented.is_some_and(|presented| {
                constant_time_equal(presented.as_bytes(), previous.as_bytes())
            }))
        }
        None => Ok(false),
    }
}

fn validate_replication_credentials(config: &ReplicationConfig) -> Result<()> {
    let _ = current_replication_token(config)?;
    if let Some(path) = config.previous_auth_token_path.as_deref() {
        let _ = read_replication_token(path)?;
    }
    Ok(())
}

fn wrap_server_stream(
    socket: TcpStream,
    config: &ReplicationConfig,
) -> Result<Box<dyn ReplicationIo>> {
    if !config.tls_server.enabled {
        return Ok(Box::new(socket));
    }
    #[cfg(not(feature = "scnp-tls"))]
    return Err(ShardCacheError::Config(
        "replication TLS requires the scnp-tls feature".into(),
    ));
    #[cfg(feature = "scnp-tls")]
    {
        let mut socket = socket;
        let tls = build_replication_server_tls_config(&config.tls_server)?;
        let mut connection = rustls::ServerConnection::new(tls).map_err(|error| {
            ShardCacheError::Config(format!("failed to create replication TLS server: {error}"))
        })?;
        let deadline =
            Instant::now() + Duration::from_millis(config.tls_server.handshake_timeout_ms.max(1));
        while connection.is_handshaking() {
            match connection.complete_io(&mut socket) {
                Ok(_) => {}
                Err(error) if is_timeout(&error) && Instant::now() < deadline => continue,
                Err(error) => return Err(ShardCacheError::Io(error)),
            }
        }
        if connection.protocol_version() != Some(rustls::ProtocolVersion::TLSv1_3)
            || connection.alpn_protocol() != Some(FCRP_ALPN)
        {
            return Err(ShardCacheError::Protocol(
                "replication TLS requires TLS 1.3 and fcrp/2 ALPN".into(),
            ));
        }
        authorize_replication_client_certificate(&connection, &config.tls_server)?;
        Ok(Box::new(rustls::StreamOwned::new(connection, socket)))
    }
}

#[cfg(feature = "scnp-tls")]
fn authorize_replication_client_certificate(
    connection: &rustls::ServerConnection,
    tls: &crate::config::ScnpTlsServerConfig,
) -> Result<()> {
    use sha2::{Digest, Sha256};

    if tls.client_cert_sha256.is_empty() {
        return Ok(());
    }
    let certificate = connection
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .ok_or_else(|| {
            ShardCacheError::Protocol("replication mTLS client certificate is required".into())
        })?;
    let actual: [u8; 32] = Sha256::digest(certificate.as_ref()).into();
    let authorized = tls.client_cert_sha256.iter().try_fold(
        false,
        |authorized, configured| -> Result<bool> {
            let compact = configured.replace(':', "");
            if compact.len() != 64 || !compact.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(ShardCacheError::Config(
                    "replication TLS client fingerprints must contain 64 hexadecimal digits".into(),
                ));
            }
            let mut expected = [0_u8; 32];
            for (index, byte) in expected.iter_mut().enumerate() {
                *byte =
                    u8::from_str_radix(&compact[index * 2..index * 2 + 2], 16).map_err(|_| {
                        ShardCacheError::Config(
                            "invalid replication TLS client certificate fingerprint".into(),
                        )
                    })?;
            }
            Ok(authorized || constant_time_equal(&actual, &expected))
        },
    )?;
    if !authorized {
        return Err(ShardCacheError::Protocol(
            "replication mTLS client identity is not authorized".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "scnp-tls")]
fn build_replication_server_tls_config(
    tls: &crate::config::ScnpTlsServerConfig,
) -> Result<Arc<rustls::ServerConfig>> {
    use std::fs::File;
    use std::io::BufReader;

    use rustls::RootCertStore;
    use rustls::server::WebPkiClientVerifier;

    if tls.client_cert_sha256.iter().any(|fingerprint| {
        let compact = fingerprint.replace(':', "");
        compact.len() != 64 || !compact.bytes().all(|byte| byte.is_ascii_hexdigit())
    }) {
        return Err(ShardCacheError::Config(
            "replication TLS client fingerprints must contain 64 hexadecimal digits".into(),
        ));
    }

    let client_ca_path = tls.client_ca_path.as_deref().ok_or_else(|| {
        ShardCacheError::Config("replication TLS requires client_ca_path for mTLS".into())
    })?;
    let mut cert_reader = BufReader::new(File::open(&tls.cert_path)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<io::Result<Vec<_>>>()?;
    if certs.is_empty() {
        return Err(ShardCacheError::Config(
            "replication TLS certificate file contains no certificates".into(),
        ));
    }
    let mut key_reader = BufReader::new(File::open(&tls.key_path)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
        ShardCacheError::Config("replication TLS key file contains no private key".into())
    })?;

    let mut roots = RootCertStore::empty();
    let mut ca_reader = BufReader::new(File::open(client_ca_path)?);
    let client_certs = rustls_pemfile::certs(&mut ca_reader).collect::<io::Result<Vec<_>>>()?;
    if client_certs.is_empty() {
        return Err(ShardCacheError::Config(
            "replication TLS client CA file contains no certificates".into(),
        ));
    }
    for certificate in client_certs {
        roots.add(certificate).map_err(|error| {
            ShardCacheError::Config(format!(
                "invalid replication TLS client CA certificate: {error}"
            ))
        })?;
    }
    let verifier = WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .map_err(|error| {
        ShardCacheError::Config(format!("invalid replication TLS client verifier: {error}"))
    })?;
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|error| {
        ShardCacheError::Config(format!("invalid replication TLS protocol config: {error}"))
    })?
    .with_client_cert_verifier(verifier)
    .with_single_cert(certs, key)
    .map_err(|error| {
        ShardCacheError::Config(format!("invalid replication TLS server identity: {error}"))
    })?;
    config.alpn_protocols = vec![FCRP_ALPN.to_vec()];
    Ok(Arc::new(config))
}

#[cfg(feature = "scnp-tls")]
fn build_replication_client_tls_config(
    tls: &crate::config::ScnpTlsClientConfig,
) -> Result<(
    Arc<rustls::ClientConfig>,
    rustls::pki_types::ServerName<'static>,
)> {
    use std::fs::File;
    use std::io::BufReader;

    use rustls::RootCertStore;

    let server_name = tls.server_name.as_deref().ok_or_else(|| {
        ShardCacheError::Config("replication TLS requires tls_client.server_name".into())
    })?;
    let server_name =
        rustls::pki_types::ServerName::try_from(server_name.to_owned()).map_err(|error| {
            ShardCacheError::Config(format!("invalid replication TLS server name: {error}"))
        })?;
    let mut roots = RootCertStore::empty();
    let mut ca_reader = BufReader::new(File::open(&tls.ca_path)?);
    let certificates = rustls_pemfile::certs(&mut ca_reader).collect::<io::Result<Vec<_>>>()?;
    if certificates.is_empty() {
        return Err(ShardCacheError::Config(
            "replication TLS CA file contains no certificates".into(),
        ));
    }
    for certificate in certificates {
        roots.add(certificate).map_err(|error| {
            ShardCacheError::Config(format!("invalid replication TLS CA certificate: {error}"))
        })?;
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|error| {
        ShardCacheError::Config(format!("invalid replication TLS protocol config: {error}"))
    })?
    .with_root_certificates(roots);
    let cert_path = tls.client_cert_path.as_deref().ok_or_else(|| {
        ShardCacheError::Config("replication TLS requires a client certificate for mTLS".into())
    })?;
    let key_path = tls.client_key_path.as_deref().ok_or_else(|| {
        ShardCacheError::Config("replication TLS requires a client key for mTLS".into())
    })?;
    let mut cert_reader = BufReader::new(File::open(cert_path)?);
    let certs = rustls_pemfile::certs(&mut cert_reader).collect::<io::Result<Vec<_>>>()?;
    if certs.is_empty() {
        return Err(ShardCacheError::Config(
            "replication TLS client certificate file contains no certificates".into(),
        ));
    }
    let mut key_reader = BufReader::new(File::open(key_path)?);
    let key = rustls_pemfile::private_key(&mut key_reader)?.ok_or_else(|| {
        ShardCacheError::Config("replication TLS client key file contains no private key".into())
    })?;
    let mut config = builder.with_client_auth_cert(certs, key).map_err(|error| {
        ShardCacheError::Config(format!("invalid replication TLS client identity: {error}"))
    })?;
    config.alpn_protocols = vec![FCRP_ALPN.to_vec()];
    Ok((Arc::new(config), server_name))
}

fn wrap_client_stream(
    socket: TcpStream,
    config: &ReplicationConfig,
) -> Result<Box<dyn ReplicationIo>> {
    if !config.tls_client.enabled {
        return Ok(Box::new(socket));
    }
    #[cfg(not(feature = "scnp-tls"))]
    return Err(ShardCacheError::Config(
        "replication TLS requires the scnp-tls feature".into(),
    ));
    #[cfg(feature = "scnp-tls")]
    {
        let mut socket = socket;
        let (tls, server_name) = build_replication_client_tls_config(&config.tls_client)?;
        let mut connection = rustls::ClientConnection::new(tls, server_name).map_err(|error| {
            ShardCacheError::Config(format!("failed to create replication TLS client: {error}"))
        })?;
        let deadline = Instant::now() + Duration::from_millis(config.connect_timeout_ms.max(1));
        while connection.is_handshaking() {
            match connection.complete_io(&mut socket) {
                Ok(_) => {}
                Err(error) if is_timeout(&error) && Instant::now() < deadline => continue,
                Err(error) => return Err(ShardCacheError::Io(error)),
            }
        }
        if connection.protocol_version() != Some(rustls::ProtocolVersion::TLSv1_3)
            || connection.alpn_protocol() != Some(FCRP_ALPN)
        {
            return Err(ShardCacheError::Protocol(
                "replication TLS requires TLS 1.3 and fcrp/2 ALPN".into(),
            ));
        }
        socket
            .set_read_timeout(Some(Duration::from_millis(200)))
            .ok();
        Ok(Box::new(rustls::StreamOwned::new(connection, socket)))
    }
}

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
        validate_replication_credentials(&config)?;
        #[cfg(feature = "scnp-tls")]
        if config.tls_server.enabled {
            let _ = build_replication_server_tls_config(&config.tls_server)?;
        }
        #[cfg(all(target_os = "linux", feature = "monoio"))]
        if monoio_transport::should_use()
            && !config.tls_server.enabled
            && config.auth_token_path.is_none()
            && config.previous_auth_token_path.is_none()
        {
            return monoio_transport::start_primary(config, primary, snapshots);
        }

        let listener = TcpListener::bind(&config.bind_addr).map_err(|error| {
            ShardCacheError::Config(format!(
                "replication primary failed to bind {}: {error}",
                config.bind_addr
            ))
        })?;
        if !listener.local_addr()?.ip().is_loopback() && !config.tls_server.enabled {
            return Err(ShardCacheError::Config(
                "non-loopback replication primary listeners require TLS".into(),
            ));
        }
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
                let connection_limit = if config.tls_server.enabled {
                    config
                        .max_replicas
                        .min(config.tls_server.max_concurrent_handshakes.max(1))
                } else {
                    config.max_replicas
                };
                if handles.len() >= connection_limit {
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
    stream: TcpStream,
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

    if config.tls_server.enabled {
        stream
            .set_read_timeout(Some(Duration::from_millis(
                config.tls_server.handshake_timeout_ms.max(1),
            )))
            .ok();
    }
    let mut stream = wrap_server_stream(stream, &config)?;
    let hello_frame =
        match read_frame_bytes_interruptible_limited(&mut stream, &stop, MAX_HELLO_FRAME_BYTES)? {
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
    if hello.role != HelloRole::Replica {
        send_error(&mut stream, "invalid replication Hello role")?;
        return Err(ShardCacheError::Protocol(format!(
            "replica {peer} sent an invalid Hello role"
        )));
    }
    if !replication_auth_ok(&config, hello.auth_token.as_deref())? {
        send_error(&mut stream, "invalid auth token")?;
        return Err(ShardCacheError::Protocol(format!(
            "replica {peer} sent invalid auth token"
        )));
    }
    let ack = ReplicationHello {
        version: FCRP_VERSION,
        role: HelloRole::Primary,
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
    stream: &mut (impl Read + Write),
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
    stream: &mut (impl Read + Write),
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

fn send_error(stream: &mut (impl Read + Write), message: &str) -> Result<()> {
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
        (Some(want), Some(got)) => constant_time_equal(want.as_bytes(), got.as_bytes()),
        (Some(_), None) => false,
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

fn write_full_frame(
    stream: &mut (impl Read + Write),
    kind: FrameKind,
    compression: ReplicationCompressionMode,
    zstd_level: i32,
    payload: &[u8],
) -> Result<()> {
    let frame = encode_frame(kind, compression, zstd_level, payload)?;
    write_raw(stream, &frame)
}

fn write_raw(stream: &mut (impl Read + Write), bytes: &[u8]) -> Result<()> {
    stream.write_all(bytes).map_err(ShardCacheError::Io)
}

fn read_frame_bytes(stream: &mut (impl Read + Write)) -> Result<Vec<u8>> {
    read_frame_inner(stream, None, MAX_FRAME_BYTES).and_then(|opt| {
        opt.ok_or_else(|| {
            ShardCacheError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "FCRP stream closed before frame completed",
            ))
        })
    })
}

fn read_frame_bytes_interruptible(
    stream: &mut (impl Read + Write),
    stop: &Arc<AtomicBool>,
) -> Result<Option<Vec<u8>>> {
    read_frame_inner(stream, Some(stop), MAX_FRAME_BYTES)
}

fn read_frame_bytes_interruptible_limited(
    stream: &mut (impl Read + Write),
    stop: &Arc<AtomicBool>,
    max_payload_bytes: usize,
) -> Result<Option<Vec<u8>>> {
    read_frame_inner(stream, Some(stop), max_payload_bytes)
}

fn read_frame_inner(
    stream: &mut (impl Read + Write),
    stop: Option<&Arc<AtomicBool>>,
    max_payload_bytes: usize,
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
    frame.resize(frame_len, 0);
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
    stream: &mut (impl Read + Write),
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
        validate_replication_credentials(&config)?;
        #[cfg(feature = "scnp-tls")]
        if config.tls_client.enabled {
            let _ = build_replication_client_tls_config(&config.tls_client)?;
        }
        let upstream = config.replica_of.clone().ok_or_else(|| {
            ShardCacheError::Config("replication.replica_of is required for replica role".into())
        })?;
        #[cfg(all(target_os = "linux", feature = "monoio"))]
        if monoio_transport::should_use()
            && !config.tls_client.enabled
            && config.auth_token_path.is_none()
            && config.previous_auth_token_path.is_none()
        {
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
    if !addr.ip().is_loopback() && !config.tls_client.enabled {
        return Err(ShardCacheError::Config(
            "non-loopback replication replica connections require TLS".into(),
        ));
    }
    let stream = TcpStream::connect_timeout(
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

    let mut stream = wrap_client_stream(stream, config)?;
    let since = state.lock().watermarks().clone();
    let hello = ReplicationHello {
        version: FCRP_VERSION,
        role: HelloRole::Replica,
        auth_token: current_replication_token(config)?,
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
        FrameKind::Hello => {
            let _ = validate_primary_hello(&ack.payload)?;
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
                let chunk = decode_snapshot_chunk(frame.payload.as_ref())?;
                let Some(slot) = pending_snapshot.as_mut() else {
                    return Err(ShardCacheError::Protocol(
                        "SnapshotChunk arrived without SnapshotBegin".into(),
                    ));
                };
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

pub(super) struct PendingSnapshot {
    watermarks: ShardWatermarks,
    entries: Vec<StoredEntry>,
    next_chunk_index: u64,
    saw_last: bool,
    retained_bytes: usize,
    max_bytes: usize,
    max_entries: usize,
}

impl PendingSnapshot {
    pub(super) fn new(watermarks: ShardWatermarks, max_bytes: usize, max_entries: usize) -> Self {
        Self {
            watermarks,
            entries: Vec::new(),
            next_chunk_index: 0,
            saw_last: false,
            retained_bytes: 0,
            max_bytes,
            max_entries,
        }
    }

    pub(super) fn push(&mut self, chunk: ReplicationSnapshotChunk) -> Result<()> {
        if self.saw_last {
            return Err(ShardCacheError::Protocol(
                "SnapshotChunk arrived after terminal chunk".into(),
            ));
        }
        if chunk.chunk_index != self.next_chunk_index {
            return Err(ShardCacheError::Protocol(format!(
                "out-of-order SnapshotChunk: expected {}, received {}",
                self.next_chunk_index, chunk.chunk_index
            )));
        }
        if chunk.watermarks != self.watermarks {
            return Err(ShardCacheError::Protocol(
                "SnapshotChunk watermarks do not match SnapshotBegin".into(),
            ));
        }
        let next_entries = self
            .entries
            .len()
            .checked_add(chunk.entries.len())
            .ok_or_else(|| ShardCacheError::Protocol("snapshot entry count overflow".into()))?;
        if next_entries > self.max_entries {
            return Err(ShardCacheError::Protocol(format!(
                "replication snapshot exceeds configured entry limit {}",
                self.max_entries
            )));
        }
        let chunk_bytes = chunk.entries.iter().try_fold(0usize, |total, entry| {
            total
                .checked_add(std::mem::size_of::<StoredEntry>())
                .and_then(|total| total.checked_add(entry.key.len()))
                .and_then(|total| total.checked_add(entry.value.len()))
                .and_then(|total| {
                    total.checked_add(entry.governance.as_ref().map_or(0, |value| value.len()))
                })
                .ok_or_else(|| ShardCacheError::Protocol("snapshot byte count overflow".into()))
        })?;
        let next_bytes = self
            .retained_bytes
            .checked_add(chunk_bytes)
            .ok_or_else(|| ShardCacheError::Protocol("snapshot byte count overflow".into()))?;
        if next_bytes > self.max_bytes {
            return Err(ShardCacheError::Protocol(format!(
                "replication snapshot exceeds configured byte limit {}",
                self.max_bytes
            )));
        }
        self.entries
            .try_reserve(chunk.entries.len())
            .map_err(|_| ShardCacheError::Protocol("snapshot entry allocation failed".into()))?;
        self.entries.extend(chunk.entries);
        self.retained_bytes = next_bytes;
        self.next_chunk_index = self
            .next_chunk_index
            .checked_add(1)
            .ok_or_else(|| ShardCacheError::Protocol("snapshot chunk index overflow".into()))?;
        self.saw_last = chunk.is_last;
        Ok(())
    }

    pub(super) fn finish(
        self,
        end_watermarks: ShardWatermarks,
    ) -> Result<super::protocol::ReplicationSnapshot> {
        if !self.saw_last {
            return Err(ShardCacheError::Protocol(
                "SnapshotEnd arrived before terminal SnapshotChunk".into(),
            ));
        }
        if end_watermarks != self.watermarks {
            return Err(ShardCacheError::Protocol(
                "SnapshotEnd watermarks do not match SnapshotBegin".into(),
            ));
        }
        Ok(super::protocol::ReplicationSnapshot {
            entries: self.entries,
            watermarks: self.watermarks,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::time::Duration;

    use crate::config::{
        ReplicationCompression, ReplicationConfig, ReplicationRole, ReplicationSendPolicy,
    };

    #[cfg(feature = "scnp-tls")]
    use crate::config::{ScnpTlsClientConfig, ScnpTlsServerConfig};

    use super::*;

    fn snapshot_entry(key: &'static [u8], value: &'static [u8]) -> StoredEntry {
        StoredEntry {
            key: key.to_vec(),
            value: value.to_vec(),
            expire_at_ms: None,
            governance: None,
        }
    }

    #[test]
    fn snapshot_receiver_rejects_reordering_mismatched_watermarks_and_missing_terminal_chunk() {
        let watermarks = ShardWatermarks::from_vec(vec![3, 7]);
        let mut reordered = PendingSnapshot::new(watermarks.clone(), 4096, 4);
        assert!(
            reordered
                .push(ReplicationSnapshotChunk {
                    watermarks: watermarks.clone(),
                    chunk_index: 1,
                    is_last: true,
                    entries: vec![snapshot_entry(b"a", b"value")],
                })
                .is_err()
        );

        let mut mismatched = PendingSnapshot::new(watermarks.clone(), 4096, 4);
        assert!(
            mismatched
                .push(ReplicationSnapshotChunk {
                    watermarks: ShardWatermarks::from_vec(vec![3, 8]),
                    chunk_index: 0,
                    is_last: true,
                    entries: vec![snapshot_entry(b"a", b"value")],
                })
                .is_err()
        );

        let mut incomplete = PendingSnapshot::new(watermarks.clone(), 4096, 4);
        incomplete
            .push(ReplicationSnapshotChunk {
                watermarks: watermarks.clone(),
                chunk_index: 0,
                is_last: false,
                entries: vec![snapshot_entry(b"a", b"value")],
            })
            .unwrap();
        assert!(incomplete.finish(watermarks).is_err());
    }

    #[test]
    fn snapshot_receiver_enforces_entry_and_retained_byte_limits() {
        let watermarks = ShardWatermarks::from_vec(vec![1]);
        let entry = snapshot_entry(b"key", b"value");
        let retained = std::mem::size_of::<StoredEntry>() + entry.key.len() + entry.value.len();

        let mut entry_limited = PendingSnapshot::new(watermarks.clone(), retained * 2, 1);
        assert!(
            entry_limited
                .push(ReplicationSnapshotChunk {
                    watermarks: watermarks.clone(),
                    chunk_index: 0,
                    is_last: true,
                    entries: vec![entry.clone(), entry.clone()],
                })
                .is_err()
        );

        let mut byte_limited = PendingSnapshot::new(watermarks.clone(), retained - 1, 2);
        assert!(
            byte_limited
                .push(ReplicationSnapshotChunk {
                    watermarks,
                    chunk_index: 0,
                    is_last: true,
                    entries: vec![entry],
                })
                .is_err()
        );
    }

    #[test]
    fn primary_hello_acknowledgement_is_role_and_field_checked() {
        let valid = ReplicationHello {
            version: FCRP_VERSION,
            role: HelloRole::Primary,
            auth_token: None,
            since: Some(ShardWatermarks::from_vec(vec![0, 0])),
        };
        assert!(validate_primary_hello(&encode_hello(&valid)).is_ok());

        let mut invalid = valid;
        invalid.role = HelloRole::Replica;
        assert!(validate_primary_hello(&encode_hello(&invalid)).is_err());
    }

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

    #[test]
    fn unauthenticated_hello_payload_is_bounded_before_body_allocation() {
        let mut header = Vec::from(*b"FCRP");
        header.push(FCRP_VERSION);
        header.push(FrameKind::Hello as u8);
        header.extend_from_slice(&[0, 0]);
        header.extend_from_slice(&((MAX_HELLO_FRAME_BYTES as u32) + 1).to_le_bytes());
        header.extend_from_slice(&0_u32.to_le_bytes());
        let mut stream = std::io::Cursor::new(header);
        let stop = Arc::new(AtomicBool::new(false));
        assert!(
            read_frame_bytes_interruptible_limited(&mut stream, &stop, MAX_HELLO_FRAME_BYTES,)
                .is_err()
        );
        assert_eq!(stream.position(), FRAME_HEADER_LEN as u64);

        let mut compressed_header = Vec::from(*b"FCRP");
        compressed_header.push(FCRP_VERSION);
        compressed_header.push(FrameKind::Hello as u8);
        compressed_header.extend_from_slice(&[1, 0]);
        compressed_header.extend_from_slice(&1_u32.to_le_bytes());
        compressed_header.extend_from_slice(&((MAX_HELLO_FRAME_BYTES as u32) + 1).to_le_bytes());
        let mut stream = std::io::Cursor::new(compressed_header);
        assert!(
            read_frame_bytes_interruptible_limited(&mut stream, &stop, MAX_HELLO_FRAME_BYTES,)
                .is_err()
        );
        assert_eq!(stream.position(), FRAME_HEADER_LEN as u64);
    }

    #[test]
    fn auth_token_files_support_overlap_rotation_and_are_redacted() {
        let directory = tempfile::tempdir().unwrap();
        let current = directory.path().join("current-token");
        let previous = directory.path().join("previous-token");
        let old_client_token = directory.path().join("old-client-token");
        let new_client_token = directory.path().join("new-client-token");
        std::fs::write(&current, "new-secret\n").unwrap();
        std::fs::write(&previous, "old-secret\n").unwrap();
        std::fs::write(&old_client_token, "old-secret\n").unwrap();
        std::fs::write(&new_client_token, "new-secret\n").unwrap();

        let addr = ephemeral_addr();
        let mut server_config = primary_config(&addr, None);
        server_config.auth_token_path = Some(current.clone());
        server_config.previous_auth_token_path = Some(previous.clone());
        let debug = format!("{server_config:?}");
        assert!(!debug.contains("new-secret"));
        assert!(!debug.contains("old-secret"));

        let primary =
            Arc::new(ReplicatedEmbeddedStore::new(1, server_config.clone()).expect("primary"));
        let server = ReplicationPrimaryServer::start(
            server_config,
            primary.primary(),
            Arc::clone(&primary) as Arc<dyn SnapshotProvider>,
        )
        .expect("server");

        let mut old_config = replica_config(&addr, None);
        old_config.auth_token_path = Some(old_client_token.clone());
        let old_client = ReplicationReplicaClient::start(old_config.clone()).expect("old client");
        primary.set(b"during-overlap".to_vec(), b"accepted".to_vec(), None);
        assert_eq!(
            await_value(&old_client, b"during-overlap", Duration::from_secs(3)),
            Some(b"accepted".to_vec())
        );
        old_client.shutdown().unwrap();

        std::fs::write(&previous, "retired-secret\n").unwrap();
        let rejected = ReplicationReplicaClient::start(old_config).expect("rejected client loop");
        primary.set(b"after-retirement".to_vec(), b"hidden".to_vec(), None);
        thread::sleep(Duration::from_millis(300));
        assert!(rejected.replica().lock().get(b"after-retirement").is_none());
        rejected.shutdown().unwrap();

        let mut new_config = replica_config(&addr, None);
        new_config.auth_token_path = Some(new_client_token);
        let new_client = ReplicationReplicaClient::start(new_config).expect("new client");
        assert_eq!(
            await_value(&new_client, b"after-retirement", Duration::from_secs(3)),
            Some(b"hidden".to_vec())
        );
        new_client.shutdown().unwrap();
        server.shutdown().unwrap();
    }

    #[cfg(feature = "scnp-tls")]
    #[test]
    fn tls13_mtls_replication_negotiates_fcrp_alpn_and_streams_data() {
        use rcgen::{
            BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
            KeyPair, KeyUsagePurpose,
        };
        use sha2::{Digest, Sha256};

        let directory = tempfile::tempdir().unwrap();
        let ca_path = directory.path().join("ca.pem");
        let server_cert_path = directory.path().join("server-cert.pem");
        let server_key_path = directory.path().join("server-key.pem");
        let client_cert_path = directory.path().join("client-cert.pem");
        let client_key_path = directory.path().join("client-key.pem");

        let mut ca_params = CertificateParams::new(vec!["replication-test-ca".into()]).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::CrlSign,
        ];
        let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();
        let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().unwrap();
        let server_cert = server_params.signed_by(&server_key, &ca).unwrap();
        let mut client_params = CertificateParams::new(vec!["replication-client".into()]).unwrap();
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_key = KeyPair::generate().unwrap();
        let client_cert = client_params.signed_by(&client_key, &ca).unwrap();
        let mut unauthorized_params =
            CertificateParams::new(vec!["unauthorized-client".into()]).unwrap();
        unauthorized_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let unauthorized_key = KeyPair::generate().unwrap();
        let unauthorized_cert = unauthorized_params
            .signed_by(&unauthorized_key, &ca)
            .unwrap();
        let client_fingerprint = Sha256::digest(client_cert.der())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();

        std::fs::write(&ca_path, ca.pem()).unwrap();
        std::fs::write(&server_cert_path, server_cert.pem()).unwrap();
        std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
        std::fs::write(&client_cert_path, client_cert.pem()).unwrap();
        std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();

        let addr = ephemeral_addr();
        let mut server_config = primary_config(&addr, Some("replication-secret"));
        server_config.tls_server = ScnpTlsServerConfig {
            enabled: true,
            cert_path: server_cert_path,
            key_path: server_key_path,
            client_ca_path: Some(ca_path.clone()),
            client_cert_sha256: vec![client_fingerprint],
            ..ScnpTlsServerConfig::default()
        };
        let primary =
            Arc::new(ReplicatedEmbeddedStore::new(1, server_config.clone()).expect("primary"));
        let server = ReplicationPrimaryServer::start(
            server_config,
            primary.primary(),
            Arc::clone(&primary) as Arc<dyn SnapshotProvider>,
        )
        .expect("TLS server");

        let mut client_config = replica_config(&addr, Some("replication-secret"));
        client_config.tls_client = ScnpTlsClientConfig {
            enabled: true,
            ca_path,
            client_cert_path: Some(client_cert_path.clone()),
            client_key_path: Some(client_key_path.clone()),
            server_name: Some("localhost".into()),
        };
        let client = ReplicationReplicaClient::start(client_config.clone()).expect("TLS client");
        primary.set(b"encrypted".to_vec(), b"replicated".to_vec(), None);
        assert_eq!(
            await_value(&client, b"encrypted", Duration::from_secs(3)),
            Some(b"replicated".to_vec())
        );
        client.shutdown().unwrap();

        std::fs::write(&client_cert_path, unauthorized_cert.pem()).unwrap();
        std::fs::write(&client_key_path, unauthorized_key.serialize_pem()).unwrap();
        let rejected = ReplicationReplicaClient::start(client_config).expect("rejected TLS loop");
        primary.set(b"pinned".to_vec(), b"must-not-replicate".to_vec(), None);
        thread::sleep(Duration::from_millis(300));
        assert!(rejected.replica().lock().get(b"pinned").is_none());
        rejected.shutdown().unwrap();
        server.shutdown().unwrap();
    }
}
