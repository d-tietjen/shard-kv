use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use rustls::pki_types::ServerName;
use sha2::{Digest, Sha256};

use super::*;

const WIRE_MAGIC: &[u8; 4] = b"AAS1";
const WIRE_VERSION: u8 = 1;
const WIRE_HEADER_BYTES: usize = 10;
const ACTIVE_SYNC_ALPN: &[u8] = b"shardmap-active-sync/1";
const MAX_MANIFEST_ORIGINS: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum WireFrameKind {
    Hello = 1,
    Block = 2,
    Done = 3,
    Ack = 4,
    Error = 5,
    SnapshotBegin = 6,
    State = 7,
    SnapshotEnd = 8,
}

impl WireFrameKind {
    fn decode(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::Block),
            3 => Ok(Self::Done),
            4 => Ok(Self::Ack),
            5 => Ok(Self::Error),
            6 => Ok(Self::SnapshotBegin),
            7 => Ok(Self::State),
            8 => Ok(Self::SnapshotEnd),
            _ => Err(ShardCacheError::Protocol(
                "unsupported active-sync TLS frame kind".into(),
            )),
        }
    }
}

/// One certificate fingerprint authorized to claim a stable active-sync node ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveSyncAuthorizedPeer {
    pub node_id: NodeId,
    pub certificate_sha256: [u8; 32],
}

#[derive(Clone)]
struct ServerCredentialGeneration {
    id: u64,
    config: Arc<rustls::ServerConfig>,
    peers: HashMap<[u8; 32], NodeId>,
}

impl fmt::Debug for ServerCredentialGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerCredentialGeneration")
            .field("id", &self.id)
            .field("authorized_peer_count", &self.peers.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct PreviousServerCredentials {
    generation: ServerCredentialGeneration,
    expires_at: Instant,
}

#[derive(Debug)]
struct ServerCredentialState {
    current: ServerCredentialGeneration,
    previous: Option<PreviousServerCredentials>,
    revoked: HashSet<NodeId>,
}

/// Atomically replaceable mTLS identity and peer authorization.
///
/// Debug output contains generation and peer counts only. Certificate and key
/// material remain inside rustls configurations and are never logged here.
#[derive(Debug)]
pub struct ActiveSyncTlsServerCredentials {
    state: RwLock<ServerCredentialState>,
}

impl ActiveSyncTlsServerCredentials {
    pub fn new(
        config: Arc<rustls::ServerConfig>,
        peers: Vec<ActiveSyncAuthorizedPeer>,
    ) -> Result<Self> {
        Ok(Self {
            state: RwLock::new(ServerCredentialState {
                current: server_generation(1, config, peers)?,
                previous: None,
                revoked: HashSet::new(),
            }),
        })
    }

    pub fn rotate(
        &self,
        config: Arc<rustls::ServerConfig>,
        peers: Vec<ActiveSyncAuthorizedPeer>,
        overlap: Duration,
    ) -> Result<u64> {
        let mut state = self.state.write();
        let next_id = state.current.id.checked_add(1).ok_or_else(|| {
            ShardCacheError::Config("active-sync credential generation exhausted".into())
        })?;
        let next = server_generation(next_id, config, peers)?;
        let previous = std::mem::replace(&mut state.current, next);
        state.previous = (!overlap.is_zero()).then_some(PreviousServerCredentials {
            generation: previous,
            expires_at: Instant::now() + overlap,
        });
        Ok(next_id)
    }

    pub fn revoke(&self, node_id: NodeId) {
        self.state.write().revoked.insert(node_id);
    }

    pub fn clear_revocation(&self, node_id: &NodeId) {
        self.state.write().revoked.remove(node_id);
    }

    pub fn generation(&self) -> u64 {
        self.state.read().current.id
    }

    fn server_config(&self) -> Arc<rustls::ServerConfig> {
        Arc::clone(&self.state.read().current.config)
    }

    fn authorize_certificate(&self, certificate: &[u8]) -> Result<(NodeId, [u8; 32])> {
        let fingerprint: [u8; 32] = Sha256::digest(certificate).into();
        let state = self.state.read();
        let node_id = state
            .current
            .peers
            .get(&fingerprint)
            .or_else(|| {
                state.previous.as_ref().and_then(|previous| {
                    (previous.expires_at > Instant::now())
                        .then(|| previous.generation.peers.get(&fingerprint))
                        .flatten()
                })
            })
            .cloned()
            .ok_or_else(|| {
                ShardCacheError::Protocol("active-sync client certificate is not authorized".into())
            })?;
        if state.revoked.contains(&node_id) {
            return Err(ShardCacheError::Protocol(
                "active-sync peer is revoked".into(),
            ));
        }
        Ok((node_id, fingerprint))
    }

    fn still_authorized(&self, node_id: &NodeId, fingerprint: &[u8; 32]) -> bool {
        let state = self.state.read();
        if state.revoked.contains(node_id) {
            return false;
        }
        state.current.peers.get(fingerprint) == Some(node_id)
            || state.previous.as_ref().is_some_and(|previous| {
                previous.expires_at > Instant::now()
                    && previous.generation.peers.get(fingerprint) == Some(node_id)
            })
    }
}

fn server_generation(
    id: u64,
    config: Arc<rustls::ServerConfig>,
    peers: Vec<ActiveSyncAuthorizedPeer>,
) -> Result<ServerCredentialGeneration> {
    if peers.is_empty() {
        return Err(ShardCacheError::Config(
            "active-sync mTLS requires at least one authorized peer".into(),
        ));
    }
    let mut peer_map = HashMap::with_capacity(peers.len());
    let mut node_ids = HashSet::with_capacity(peers.len());
    for peer in peers {
        if !node_ids.insert(peer.node_id.clone()) {
            return Err(ShardCacheError::Config(
                "active-sync authorized peer node IDs must be unique".into(),
            ));
        }
        if peer_map
            .insert(peer.certificate_sha256, peer.node_id)
            .is_some()
        {
            return Err(ShardCacheError::Config(
                "active-sync certificate fingerprints must be unique".into(),
            ));
        }
    }
    let mut config = (*config).clone();
    config.alpn_protocols = vec![ACTIVE_SYNC_ALPN.to_vec()];
    Ok(ServerCredentialGeneration {
        id,
        config: Arc::new(config),
        peers: peer_map,
    })
}

/// Atomically replaceable client certificate, key, and trust configuration.
pub struct ActiveSyncTlsClientCredentials {
    config: RwLock<Arc<rustls::ClientConfig>>,
    generation: AtomicU64,
}

impl fmt::Debug for ActiveSyncTlsClientCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveSyncTlsClientCredentials")
            .field("generation", &self.generation())
            .finish_non_exhaustive()
    }
}

impl ActiveSyncTlsClientCredentials {
    pub fn new(config: Arc<rustls::ClientConfig>) -> Self {
        Self {
            config: RwLock::new(client_config_with_alpn(config)),
            generation: AtomicU64::new(1),
        }
    }

    pub fn rotate(&self, config: Arc<rustls::ClientConfig>) -> Result<u64> {
        let next = self
            .generation
            .load(AtomicOrdering::Acquire)
            .checked_add(1)
            .ok_or_else(|| {
                ShardCacheError::Config("active-sync client credential generation exhausted".into())
            })?;
        *self.config.write() = client_config_with_alpn(config);
        self.generation.store(next, AtomicOrdering::Release);
        Ok(next)
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(AtomicOrdering::Acquire)
    }

    fn snapshot(&self) -> Arc<rustls::ClientConfig> {
        Arc::clone(&self.config.read())
    }
}

fn client_config_with_alpn(config: Arc<rustls::ClientConfig>) -> Arc<rustls::ClientConfig> {
    let mut config = (*config).clone();
    config.alpn_protocols = vec![ACTIVE_SYNC_ALPN.to_vec()];
    Arc::new(config)
}

/// Direct per-shard address set for one active-sync peer.
#[derive(Debug, Clone)]
pub struct ActiveSyncTlsPeer {
    pub node_id: NodeId,
    pub shard_addresses: Arc<[SocketAddr]>,
    pub server_name: Box<str>,
    pub credentials: Arc<ActiveSyncTlsClientCredentials>,
}

impl ActiveSyncTlsPeer {
    pub fn new(
        node_id: NodeId,
        shard_addresses: Vec<SocketAddr>,
        server_name: impl Into<Box<str>>,
        credentials: Arc<ActiveSyncTlsClientCredentials>,
    ) -> Result<Self> {
        let server_name = server_name.into();
        ServerName::try_from(server_name.to_string()).map_err(|_| {
            ShardCacheError::Config("active-sync TLS server name is invalid".into())
        })?;
        if shard_addresses.is_empty() {
            return Err(ShardCacheError::Config(
                "active-sync peer requires direct shard addresses".into(),
            ));
        }
        Ok(Self {
            node_id,
            shard_addresses: shard_addresses.into(),
            server_name,
            credentials,
        })
    }
}

/// Resource and deadline bounds for direct active-sync listeners.
#[derive(Debug, Clone)]
pub struct ActiveSyncTlsServerOptions {
    pub handshake_timeout: Duration,
    pub io_timeout: Duration,
    pub max_connection_age: Duration,
    pub max_frame_bytes: usize,
    pub max_blocks_per_round: usize,
    pub max_bytes_per_round: usize,
}

impl Default for ActiveSyncTlsServerOptions {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(10),
            max_connection_age: Duration::from_secs(30),
            max_frame_bytes: 8 * 1024 * 1024,
            max_blocks_per_round: 128,
            max_bytes_per_round: 16 * 1024 * 1024,
        }
    }
}

/// One dedicated blocking listener thread per storage shard.
pub struct ActiveSyncTlsServer {
    local_addresses: Arc<[SocketAddr]>,
    shutdown: Arc<AtomicBool>,
    joins: Mutex<Vec<JoinHandle<()>>>,
}

impl fmt::Debug for ActiveSyncTlsServer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveSyncTlsServer")
            .field("local_addresses", &self.local_addresses)
            .field("active_threads", &self.joins.lock().len())
            .finish_non_exhaustive()
    }
}

impl ActiveSyncTlsServer {
    pub fn start(
        map: ActiveShardMap,
        shard_addresses: Vec<SocketAddr>,
        credentials: Arc<ActiveSyncTlsServerCredentials>,
        options: ActiveSyncTlsServerOptions,
    ) -> Result<Self> {
        validate_server_options(&map, &shard_addresses, &options)?;
        let mut listeners = Vec::with_capacity(shard_addresses.len());
        let mut local_addresses = Vec::with_capacity(shard_addresses.len());
        for address in shard_addresses {
            if !address.ip().is_loopback() && address.ip().is_unspecified() {
                // An unspecified bind is allowed because transport security is
                // mandatory, but keeping this branch explicit makes the policy visible.
            }
            let listener = TcpListener::bind(address)?;
            listener.set_nonblocking(true)?;
            local_addresses.push(listener.local_addr()?);
            listeners.push(listener);
        }
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut joins = Vec::with_capacity(listeners.len());
        for (shard_id, listener) in listeners.into_iter().enumerate() {
            let map = map.clone();
            let credentials = Arc::clone(&credentials);
            let options = options.clone();
            let shutdown = Arc::clone(&shutdown);
            let join = thread::Builder::new()
                .name(format!("active-sync-shard-{shard_id}"))
                .spawn(move || {
                    run_shard_listener(map, shard_id, listener, credentials, options, shutdown);
                })
                .map_err(|error| {
                    ShardCacheError::Config(format!(
                        "failed to start active-sync shard listener: {error}"
                    ))
                })?;
            joins.push(join);
        }
        Ok(Self {
            local_addresses: local_addresses.into(),
            shutdown,
            joins: Mutex::new(joins),
        })
    }

    pub fn local_addresses(&self) -> &[SocketAddr] {
        &self.local_addresses
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        for address in self.local_addresses.iter() {
            let _ = TcpStream::connect_timeout(address, Duration::from_millis(50));
        }
        for join in self.joins.lock().drain(..) {
            if join.join().is_err() {
                tracing::warn!("active-sync shard listener panicked during shutdown");
            }
        }
    }
}

impl Drop for ActiveSyncTlsServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn validate_server_options(
    map: &ActiveShardMap,
    addresses: &[SocketAddr],
    options: &ActiveSyncTlsServerOptions,
) -> Result<()> {
    if addresses.len() != map.shard_count() {
        return Err(ShardCacheError::Config(
            "active-sync requires exactly one direct listener address per shard".into(),
        ));
    }
    if options.handshake_timeout.is_zero()
        || options.io_timeout.is_zero()
        || options.max_connection_age.is_zero()
        || options.max_frame_bytes < 1024
        || options.max_blocks_per_round == 0
        || options.max_bytes_per_round == 0
    {
        return Err(ShardCacheError::Config(
            "active-sync TLS server limits and deadlines must be nonzero".into(),
        ));
    }
    Ok(())
}

fn run_shard_listener(
    map: ActiveShardMap,
    shard_id: usize,
    listener: TcpListener,
    credentials: Arc<ActiveSyncTlsServerCredentials>,
    options: ActiveSyncTlsServerOptions,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                if shutdown.load(Ordering::Acquire) {
                    break;
                }
                if let Err(error) =
                    handle_server_connection(&map, shard_id, stream, &credentials, &options)
                {
                    tracing::warn!(shard_id, "active-sync connection rejected: {error}");
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => {
                tracing::warn!(shard_id, "active-sync accept failed: {error}");
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

fn handle_server_connection(
    map: &ActiveShardMap,
    shard_id: usize,
    stream: TcpStream,
    credentials: &ActiveSyncTlsServerCredentials,
    options: &ActiveSyncTlsServerOptions,
) -> Result<()> {
    let started = Instant::now();
    configure_stream(&stream, options.handshake_timeout)?;
    let mut connection =
        rustls::ServerConnection::new(credentials.server_config()).map_err(|error| {
            ShardCacheError::Protocol(format!("active-sync TLS server setup failed: {error}"))
        })?;
    let mut socket = stream;
    while connection.is_handshaking() {
        connection.complete_io(&mut socket).map_err(|error| {
            ShardCacheError::Protocol(format!("active-sync TLS handshake failed: {error}"))
        })?;
        ensure_deadline(started, options.handshake_timeout)?;
    }
    validate_negotiated_tls(&connection)?;
    let certificate = connection
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .ok_or_else(|| {
            ShardCacheError::Protocol("active-sync mTLS client certificate is required".into())
        })?;
    let (authorized_node, fingerprint) = credentials.authorize_certificate(certificate.as_ref())?;
    configure_stream(&socket, options.io_timeout)?;
    let mut stream = rustls::StreamOwned::new(connection, socket);

    let (kind, payload) = read_frame(&mut stream, options.max_frame_bytes)?;
    if kind != WireFrameKind::Hello {
        return Err(ShardCacheError::Protocol(
            "active-sync expected client hello".into(),
        ));
    }
    let client_hello = decode_hello(&payload)?;
    validate_hello(map, shard_id, &client_hello, Some(&authorized_node))?;
    map.seal_pending_shard(shard_id)?;
    ensure_connection_authorized(
        credentials,
        &authorized_node,
        &fingerprint,
        started,
        options,
    )?;
    let server_hello = WireHello {
        cluster_id: map.inner.config.cluster_id.to_string(),
        node_id: map.node_id().clone(),
        incarnation_id: map.inner.config.incarnation_id,
        shard_id: shard_id as u32,
        frontiers: map.shard_frontiers(shard_id)?,
    };
    write_frame(
        &mut stream,
        WireFrameKind::Hello,
        &encode_hello(&server_hello)?,
        options.max_frame_bytes,
    )?;

    receive_blocks(
        map,
        shard_id,
        &authorized_node,
        &mut stream,
        options,
        started,
    )?;
    write_frame(
        &mut stream,
        WireFrameKind::Ack,
        &[],
        options.max_frame_bytes,
    )?;
    ensure_connection_authorized(
        credentials,
        &authorized_node,
        &fingerprint,
        started,
        options,
    )?;
    send_blocks(
        map,
        shard_id,
        &client_hello.frontiers,
        &mut stream,
        options,
        started,
    )?;
    let (kind, payload) = read_frame(&mut stream, options.max_frame_bytes)?;
    handle_ack_or_error(kind, &payload)?;
    Ok(())
}

impl ActiveShardMap {
    /// Synchronizes all shards over dedicated mTLS connections.
    ///
    /// Each shard is handled independently, so a slow remote shard cannot hold
    /// another shard's network or storage lock.
    pub fn sync_with_tls_peer(
        &self,
        peer: &ActiveSyncTlsPeer,
        options: SyncOptions,
        io_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<BidirectionalSyncReport> {
        if peer.shard_addresses.len() != self.shard_count() {
            return Err(ShardCacheError::Config(
                "active-sync peer address count does not match local shard count".into(),
            ));
        }
        if peer.node_id == *self.node_id() {
            return Err(ShardCacheError::Config(
                "active-sync peers must use unique node IDs".into(),
            ));
        }
        if io_timeout.is_zero() || max_frame_bytes < 1024 {
            return Err(ShardCacheError::Config(
                "active-sync client deadlines and frame bounds must be nonzero".into(),
            ));
        }
        self.seal_pending()?;
        let reports = Mutex::new(Vec::with_capacity(self.shard_count()));
        thread::scope(|scope| {
            for shard_id in 0..self.shard_count() {
                let reports = &reports;
                let map = self.clone();
                let peer = peer.clone();
                let options = options.clone();
                scope.spawn(move || {
                    reports.lock().push(sync_tls_shard(
                        &map,
                        &peer,
                        shard_id,
                        &options,
                        io_timeout,
                        max_frame_bytes,
                    ));
                });
            }
        });
        let mut combined = BidirectionalSyncReport::default();
        for report in reports.into_inner() {
            let report = report?;
            combined.blocks_to_local += report.blocks_to_local;
            combined.blocks_to_peer += report.blocks_to_peer;
            combined.bytes_to_local += report.bytes_to_local;
            combined.bytes_to_peer += report.bytes_to_peer;
            combined.applied_mutations += report.applied_mutations;
            combined.duplicate_mutations += report.duplicate_mutations;
            combined.conflicts += report.conflicts;
            combined.state_snapshot_fallbacks += report.state_snapshot_fallbacks;
            combined.truncated |= report.truncated;
        }
        Ok(combined)
    }
}

fn sync_tls_shard(
    map: &ActiveShardMap,
    peer: &ActiveSyncTlsPeer,
    shard_id: usize,
    options: &SyncOptions,
    io_timeout: Duration,
    max_frame_bytes: usize,
) -> Result<BidirectionalSyncReport> {
    let started = Instant::now();
    let address = peer.shard_addresses[shard_id];
    let socket = TcpStream::connect_timeout(&address, io_timeout)?;
    configure_stream(&socket, io_timeout)?;
    let server_name = ServerName::try_from(peer.server_name.to_string())
        .map_err(|_| ShardCacheError::Config("active-sync TLS server name is invalid".into()))?;
    let mut connection = rustls::ClientConnection::new(peer.credentials.snapshot(), server_name)
        .map_err(|error| {
            ShardCacheError::Protocol(format!("active-sync TLS client setup failed: {error}"))
        })?;
    let mut socket = socket;
    while connection.is_handshaking() {
        connection.complete_io(&mut socket).map_err(|error| {
            ShardCacheError::Protocol(format!("active-sync TLS handshake failed: {error}"))
        })?;
        ensure_deadline(started, io_timeout)?;
    }
    validate_negotiated_tls(&connection)?;
    let mut stream = rustls::StreamOwned::new(connection, socket);
    let hello = WireHello {
        cluster_id: map.inner.config.cluster_id.to_string(),
        node_id: map.node_id().clone(),
        incarnation_id: map.inner.config.incarnation_id,
        shard_id: shard_id as u32,
        frontiers: map.shard_frontiers(shard_id)?,
    };
    write_frame(
        &mut stream,
        WireFrameKind::Hello,
        &encode_hello(&hello)?,
        max_frame_bytes,
    )?;
    let (kind, payload) = read_frame(&mut stream, max_frame_bytes)?;
    if kind != WireFrameKind::Hello {
        return handle_unexpected_frame(kind, &payload, "server hello");
    }
    let server_hello = decode_hello(&payload)?;
    validate_hello(map, shard_id, &server_hello, Some(&peer.node_id))?;

    let mut report = send_blocks(
        map,
        shard_id,
        &server_hello.frontiers,
        &mut stream,
        &ActiveSyncTlsServerOptions {
            io_timeout,
            max_connection_age: io_timeout,
            max_frame_bytes,
            max_blocks_per_round: options.max_blocks,
            max_bytes_per_round: options.max_bytes,
            ..ActiveSyncTlsServerOptions::default()
        },
        started,
    )?;
    let (kind, payload) = read_frame(&mut stream, max_frame_bytes)?;
    handle_ack_or_error(kind, &payload)?;
    let received = receive_blocks(
        map,
        shard_id,
        &peer.node_id,
        &mut stream,
        &ActiveSyncTlsServerOptions {
            io_timeout,
            max_connection_age: io_timeout,
            max_frame_bytes,
            max_blocks_per_round: options.max_blocks,
            max_bytes_per_round: options.max_bytes,
            ..ActiveSyncTlsServerOptions::default()
        },
        started,
    )?;
    report.blocks_to_local += received.blocks_to_local;
    report.bytes_to_local += received.bytes_to_local;
    report.applied_mutations += received.applied_mutations;
    report.duplicate_mutations += received.duplicate_mutations;
    report.conflicts += received.conflicts;
    report.state_snapshot_fallbacks += received.state_snapshot_fallbacks;
    write_frame(&mut stream, WireFrameKind::Ack, &[], max_frame_bytes)?;
    Ok(report)
}

fn send_blocks<S: Read + Write>(
    map: &ActiveShardMap,
    shard_id: usize,
    receiver_frontiers: &BTreeMap<BlockOrigin, u64>,
    stream: &mut S,
    options: &ActiveSyncTlsServerOptions,
    started: Instant,
) -> Result<BidirectionalSyncReport> {
    let sync_options = SyncOptions {
        max_blocks: options.max_blocks_per_round,
        max_bytes: options.max_bytes_per_round,
    };
    let (blocks, gap, truncated) =
        map.missing_blocks_in_shard(shard_id, map.node_id(), receiver_frontiers, &sync_options)?;
    if gap {
        return send_state_snapshot(map, shard_id, stream, options, started);
    }
    let mut report = BidirectionalSyncReport {
        truncated,
        ..BidirectionalSyncReport::default()
    };
    for block in blocks {
        ensure_deadline(started, options.max_connection_age)?;
        let payload = encode_block(&block)?;
        if report.bytes_to_peer.saturating_add(payload.len()) > options.max_bytes_per_round {
            report.truncated = true;
            break;
        }
        write_frame(
            stream,
            WireFrameKind::Block,
            &payload,
            options.max_frame_bytes,
        )?;
        report.blocks_to_peer += 1;
        report.bytes_to_peer = report.bytes_to_peer.saturating_add(payload.len());
    }
    write_frame(stream, WireFrameKind::Done, &[], options.max_frame_bytes)?;
    Ok(report)
}

fn receive_blocks<S: Read + Write>(
    map: &ActiveShardMap,
    shard_id: usize,
    source_peer: &NodeId,
    stream: &mut S,
    options: &ActiveSyncTlsServerOptions,
    started: Instant,
) -> Result<BidirectionalSyncReport> {
    let mut report = BidirectionalSyncReport::default();
    let mut in_snapshot = false;
    loop {
        ensure_deadline(started, options.max_connection_age)?;
        let (kind, payload) = read_frame(stream, options.max_frame_bytes)?;
        match kind {
            WireFrameKind::Done if !in_snapshot => return Ok(report),
            WireFrameKind::Done => {
                return Err(ShardCacheError::Protocol(
                    "active-sync state snapshot ended before its frontier".into(),
                ));
            }
            WireFrameKind::Error => return Err(remote_error(&payload)),
            WireFrameKind::SnapshotBegin if !in_snapshot && payload.is_empty() => {
                in_snapshot = true;
                report.state_snapshot_fallbacks += 1;
            }
            WireFrameKind::State if in_snapshot => {
                if report
                    .applied_mutations
                    .saturating_add(report.duplicate_mutations)
                    >= options.max_blocks_per_round.saturating_mul(1024)
                    || report.bytes_to_local.saturating_add(payload.len())
                        > options.max_bytes_per_round
                {
                    return Err(ShardCacheError::Backpressure(
                        "active-sync state transfer budget exhausted",
                    ));
                }
                let mutation = decode_standalone_mutation(&payload)?;
                if mutation.dot.shard_id as usize != shard_id
                    || mutation.dot.node_id != *source_peer
                {
                    return Err(ShardCacheError::Protocol(
                        "active-sync state provenance does not match authenticated peer".into(),
                    ));
                }
                let stats = map.apply_state_mutation(&mutation, source_peer)?;
                report.bytes_to_local = report.bytes_to_local.saturating_add(payload.len());
                report.applied_mutations += stats.applied;
                report.duplicate_mutations += stats.duplicates;
                report.conflicts += stats.conflicts;
            }
            WireFrameKind::SnapshotEnd if in_snapshot => {
                let frontiers = decode_frontiers(&payload, shard_id)?;
                if frontiers
                    .keys()
                    .any(|origin| origin.node_id != *source_peer)
                {
                    return Err(ShardCacheError::Protocol(
                        "active-sync snapshot frontier provenance is invalid".into(),
                    ));
                }
                map.accept_snapshot_frontiers(shard_id, &frontiers)?;
                in_snapshot = false;
            }
            WireFrameKind::Block => {
                if report.blocks_to_local >= options.max_blocks_per_round
                    || report.bytes_to_local.saturating_add(payload.len())
                        > options.max_bytes_per_round
                {
                    return Err(ShardCacheError::Backpressure(
                        "active-sync receive budget exhausted",
                    ));
                }
                let block = Arc::new(decode_block(&payload)?);
                if block.origin.shard_id as usize != shard_id
                    || block.origin.node_id != *source_peer
                {
                    return Err(ShardCacheError::Protocol(
                        "active-sync block provenance does not match authenticated peer".into(),
                    ));
                }
                let stats = map.apply_block(block, source_peer)?;
                report.blocks_to_local += 1;
                report.bytes_to_local = report.bytes_to_local.saturating_add(payload.len());
                report.applied_mutations += stats.applied;
                report.duplicate_mutations += stats.duplicates;
                report.conflicts += stats.conflicts;
            }
            _ => {
                return Err(ShardCacheError::Protocol(
                    "active-sync received an invalid transfer frame".into(),
                ));
            }
        }
    }
}

fn send_state_snapshot<S: Read + Write>(
    map: &ActiveShardMap,
    shard_id: usize,
    stream: &mut S,
    options: &ActiveSyncTlsServerOptions,
    started: Instant,
) -> Result<BidirectionalSyncReport> {
    let max_records = options.max_blocks_per_round.saturating_mul(1024);
    let records = map.materialized_shard_snapshot(
        shard_id,
        map.node_id(),
        max_records,
        options.max_bytes_per_round,
    )?;
    let mut frontiers = map.shard_frontiers(shard_id)?;
    frontiers.retain(|origin, _| origin.node_id == *map.node_id());
    let mut report = BidirectionalSyncReport {
        state_snapshot_fallbacks: 1,
        ..BidirectionalSyncReport::default()
    };
    write_frame(
        stream,
        WireFrameKind::SnapshotBegin,
        &[],
        options.max_frame_bytes,
    )?;
    for (record_count, mutation) in records.into_iter().enumerate() {
        ensure_deadline(started, options.max_connection_age)?;
        let mut payload = Vec::with_capacity(mutation.estimated_bytes());
        encode_mutation(&mut payload, &mutation)?;
        if report.bytes_to_peer.saturating_add(payload.len()) > options.max_bytes_per_round
            || record_count >= options.max_blocks_per_round.saturating_mul(1024)
        {
            return Err(ShardCacheError::Backpressure(
                "active-sync state transfer budget exhausted",
            ));
        }
        write_frame(
            stream,
            WireFrameKind::State,
            &payload,
            options.max_frame_bytes,
        )?;
        report.bytes_to_peer = report.bytes_to_peer.saturating_add(payload.len());
    }
    let frontier_payload = encode_frontiers(&frontiers)?;
    write_frame(
        stream,
        WireFrameKind::SnapshotEnd,
        &frontier_payload,
        options.max_frame_bytes,
    )?;
    write_frame(stream, WireFrameKind::Done, &[], options.max_frame_bytes)?;
    Ok(report)
}

fn configure_stream(stream: &TcpStream, timeout: Duration) -> Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    Ok(())
}

fn ensure_deadline(started: Instant, maximum: Duration) -> Result<()> {
    if started.elapsed() > maximum {
        return Err(ShardCacheError::Protocol(
            "active-sync connection deadline exceeded".into(),
        ));
    }
    Ok(())
}

trait TlsConnectionInfo {
    fn protocol_version(&self) -> Option<rustls::ProtocolVersion>;
    fn alpn_protocol(&self) -> Option<&[u8]>;
}

impl TlsConnectionInfo for rustls::ServerConnection {
    fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        std::ops::Deref::deref(self).protocol_version()
    }

    fn alpn_protocol(&self) -> Option<&[u8]> {
        std::ops::Deref::deref(self).alpn_protocol()
    }
}

impl TlsConnectionInfo for rustls::ClientConnection {
    fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        std::ops::Deref::deref(self).protocol_version()
    }

    fn alpn_protocol(&self) -> Option<&[u8]> {
        std::ops::Deref::deref(self).alpn_protocol()
    }
}

fn validate_negotiated_tls(connection: &impl TlsConnectionInfo) -> Result<()> {
    if connection.protocol_version() != Some(rustls::ProtocolVersion::TLSv1_3) {
        return Err(ShardCacheError::Protocol(
            "active-sync requires TLS 1.3".into(),
        ));
    }
    if connection.alpn_protocol() != Some(ACTIVE_SYNC_ALPN) {
        return Err(ShardCacheError::Protocol(
            "active-sync ALPN negotiation failed".into(),
        ));
    }
    Ok(())
}

fn ensure_connection_authorized(
    credentials: &ActiveSyncTlsServerCredentials,
    node_id: &NodeId,
    fingerprint: &[u8; 32],
    started: Instant,
    options: &ActiveSyncTlsServerOptions,
) -> Result<()> {
    ensure_deadline(started, options.max_connection_age)?;
    if !credentials.still_authorized(node_id, fingerprint) {
        return Err(ShardCacheError::Protocol(
            "active-sync peer authorization expired or was revoked".into(),
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct WireHello {
    cluster_id: String,
    node_id: NodeId,
    incarnation_id: IncarnationId,
    shard_id: u32,
    frontiers: BTreeMap<BlockOrigin, u64>,
}

fn validate_hello(
    map: &ActiveShardMap,
    shard_id: usize,
    hello: &WireHello,
    expected_node: Option<&NodeId>,
) -> Result<()> {
    if hello.cluster_id.as_str() != map.inner.config.cluster_id.as_ref()
        || hello.shard_id as usize != shard_id
        || expected_node.is_some_and(|expected| expected != &hello.node_id)
        || hello.node_id == *map.node_id()
    {
        return Err(ShardCacheError::Protocol(
            "active-sync peer hello identity or topology mismatch".into(),
        ));
    }
    if hello
        .frontiers
        .keys()
        .any(|origin| origin.shard_id as usize != shard_id)
    {
        return Err(ShardCacheError::Protocol(
            "active-sync manifest contains another shard".into(),
        ));
    }
    Ok(())
}

fn write_frame<S: Write>(
    stream: &mut S,
    kind: WireFrameKind,
    payload: &[u8],
    max_frame_bytes: usize,
) -> Result<()> {
    if payload.len() > max_frame_bytes || payload.len() > u32::MAX as usize {
        return Err(ShardCacheError::Protocol(
            "active-sync frame exceeds configured bounds".into(),
        ));
    }
    let mut header = [0u8; WIRE_HEADER_BYTES];
    header[..4].copy_from_slice(WIRE_MAGIC);
    header[4] = WIRE_VERSION;
    header[5] = kind as u8;
    header[6..10].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    stream.write_all(&header)?;
    stream.write_all(payload)?;
    stream.flush()?;
    Ok(())
}

fn read_frame<S: Read>(stream: &mut S, max_frame_bytes: usize) -> Result<(WireFrameKind, Vec<u8>)> {
    let mut header = [0u8; WIRE_HEADER_BYTES];
    stream.read_exact(&mut header)?;
    if &header[..4] != WIRE_MAGIC || header[4] != WIRE_VERSION {
        return Err(ShardCacheError::Protocol(
            "active-sync frame header is invalid".into(),
        ));
    }
    let kind = WireFrameKind::decode(header[5])?;
    let payload_len = u32::from_le_bytes(
        header[6..10]
            .try_into()
            .map_err(|_| ShardCacheError::Protocol("active-sync frame length is invalid".into()))?,
    ) as usize;
    if payload_len > max_frame_bytes {
        return Err(ShardCacheError::Protocol(
            "active-sync frame exceeds configured bounds".into(),
        ));
    }
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload)?;
    Ok((kind, payload))
}

fn encode_hello(hello: &WireHello) -> Result<Vec<u8>> {
    if hello.frontiers.len() > MAX_MANIFEST_ORIGINS {
        return Err(ShardCacheError::Backpressure(
            "active-sync manifest origin limit reached",
        ));
    }
    let mut out = Vec::new();
    put_string(&mut out, &hello.cluster_id)?;
    put_string(&mut out, hello.node_id.as_str())?;
    out.extend_from_slice(&hello.incarnation_id.0.to_le_bytes());
    out.extend_from_slice(&hello.shard_id.to_le_bytes());
    put_u32(&mut out, hello.frontiers.len())?;
    for (origin, sequence) in &hello.frontiers {
        put_string(&mut out, origin.node_id.as_str())?;
        out.extend_from_slice(&origin.incarnation_id.0.to_le_bytes());
        out.extend_from_slice(&origin.shard_id.to_le_bytes());
        out.extend_from_slice(&sequence.to_le_bytes());
    }
    Ok(out)
}

fn decode_hello(payload: &[u8]) -> Result<WireHello> {
    let mut decoder = Decoder::new(payload);
    let cluster_id = decoder.string()?.to_string();
    let node_id = NodeId::new(decoder.string()?.to_string())?;
    let incarnation_id = IncarnationId(decoder.u128()?);
    let shard_id = decoder.u32()?;
    let count = decoder.u32()? as usize;
    if count > MAX_MANIFEST_ORIGINS {
        return Err(ShardCacheError::Protocol(
            "active-sync manifest origin count exceeds bounds".into(),
        ));
    }
    let mut frontiers = BTreeMap::new();
    for _ in 0..count {
        let origin = BlockOrigin {
            node_id: NodeId::new(decoder.string()?.to_string())?,
            incarnation_id: IncarnationId(decoder.u128()?),
            shard_id: decoder.u32()?,
        };
        let sequence = decoder.u64()?;
        if frontiers.insert(origin, sequence).is_some() {
            return Err(ShardCacheError::Protocol(
                "active-sync manifest contains a duplicate origin".into(),
            ));
        }
    }
    decoder.finish()?;
    Ok(WireHello {
        cluster_id,
        node_id,
        incarnation_id,
        shard_id,
        frontiers,
    })
}

fn encode_frontiers(frontiers: &BTreeMap<BlockOrigin, u64>) -> Result<Vec<u8>> {
    if frontiers.len() > MAX_MANIFEST_ORIGINS {
        return Err(ShardCacheError::Backpressure(
            "active-sync manifest origin limit reached",
        ));
    }
    let mut out = Vec::new();
    put_u32(&mut out, frontiers.len())?;
    for (origin, sequence) in frontiers {
        put_string(&mut out, origin.node_id.as_str())?;
        out.extend_from_slice(&origin.incarnation_id.0.to_le_bytes());
        out.extend_from_slice(&origin.shard_id.to_le_bytes());
        out.extend_from_slice(&sequence.to_le_bytes());
    }
    Ok(out)
}

fn decode_frontiers(payload: &[u8], shard_id: usize) -> Result<BTreeMap<BlockOrigin, u64>> {
    let mut decoder = Decoder::new(payload);
    let count = decoder.u32()? as usize;
    if count > MAX_MANIFEST_ORIGINS {
        return Err(ShardCacheError::Protocol(
            "active-sync manifest origin count exceeds bounds".into(),
        ));
    }
    let mut frontiers = BTreeMap::new();
    for _ in 0..count {
        let origin = BlockOrigin {
            node_id: NodeId::new(decoder.string()?.to_string())?,
            incarnation_id: IncarnationId(decoder.u128()?),
            shard_id: decoder.u32()?,
        };
        if origin.shard_id as usize != shard_id {
            return Err(ShardCacheError::Protocol(
                "active-sync snapshot frontier belongs to another shard".into(),
            ));
        }
        let sequence = decoder.u64()?;
        if frontiers.insert(origin, sequence).is_some() {
            return Err(ShardCacheError::Protocol(
                "active-sync snapshot contains a duplicate frontier".into(),
            ));
        }
    }
    decoder.finish()?;
    Ok(frontiers)
}

fn encode_block(block: &SyncBlock) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(block.encoded_bytes);
    put_string(&mut out, &block.cluster_id)?;
    put_string(&mut out, block.origin.node_id.as_str())?;
    out.extend_from_slice(&block.origin.incarnation_id.0.to_le_bytes());
    out.extend_from_slice(&block.origin.shard_id.to_le_bytes());
    out.extend_from_slice(&block.sequence.to_le_bytes());
    out.extend_from_slice(&block.digest());
    put_u32(&mut out, block.records.len())?;
    for record in block.records.iter() {
        encode_mutation(&mut out, record)?;
    }
    Ok(out)
}

fn decode_block(payload: &[u8]) -> Result<SyncBlock> {
    let mut decoder = Decoder::new(payload);
    let cluster_id = decoder.string()?.to_string().into_boxed_str();
    let origin = BlockOrigin {
        node_id: NodeId::new(decoder.string()?.to_string())?,
        incarnation_id: IncarnationId(decoder.u128()?),
        shard_id: decoder.u32()?,
    };
    let sequence = decoder.u64()?;
    let digest: [u8; 32] = decoder
        .bytes_exact(32)?
        .try_into()
        .map_err(|_| ShardCacheError::Protocol("active-sync block digest is invalid".into()))?;
    let count = decoder.u32()? as usize;
    let minimum_record_bytes = 64usize;
    if count > payload.len() / minimum_record_bytes {
        return Err(ShardCacheError::Protocol(
            "active-sync block record count exceeds payload bounds".into(),
        ));
    }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(decode_mutation(&mut decoder)?);
    }
    decoder.finish()?;
    Ok(SyncBlock {
        cluster_id,
        origin,
        sequence,
        digest: OnceLock::from(digest),
        records: Arc::new(records),
        encoded_bytes: payload.len(),
    })
}

fn encode_mutation(out: &mut Vec<u8>, mutation: &ActiveMutation) -> Result<()> {
    put_dot(out, &mutation.dot)?;
    out.extend_from_slice(&mutation.hlc.physical_ms.to_le_bytes());
    out.extend_from_slice(&mutation.hlc.logical.to_le_bytes());
    put_u32(out, mutation.context.len())?;
    for (origin, sequence) in mutation.context.iter() {
        put_string(out, origin.node_id.as_str())?;
        out.extend_from_slice(&origin.incarnation_id.0.to_le_bytes());
        out.extend_from_slice(&origin.shard_id.to_le_bytes());
        out.extend_from_slice(&sequence.to_le_bytes());
    }
    out.extend_from_slice(&mutation.key_hash.to_le_bytes());
    put_bytes(out, &mutation.key)?;
    put_optional_bytes(out, mutation.value.as_deref())?;
    out.extend_from_slice(&mutation.expire_at_ms.unwrap_or(u64::MAX).to_le_bytes());
    put_optional_bytes(out, mutation.governance.as_deref())?;
    match &mutation.kind {
        MutationKind::Set => out.push(1),
        MutationKind::Tombstone(TombstoneKind::Delete) => out.push(2),
        MutationKind::Tombstone(TombstoneKind::Expired) => out.push(3),
        MutationKind::ClusterEvict { target } => {
            out.push(4);
            put_dot(out, target)?;
        }
    }
    Ok(())
}

fn decode_mutation(decoder: &mut Decoder<'_>) -> Result<ActiveMutation> {
    let dot = decoder.dot()?;
    let hlc = HybridLogicalClock {
        physical_ms: decoder.u64()?,
        logical: decoder.u32()?,
    };
    let context_count = decoder.u32()? as usize;
    if context_count > MAX_MANIFEST_ORIGINS {
        return Err(ShardCacheError::Protocol(
            "active-sync mutation context exceeds wire bounds".into(),
        ));
    }
    let mut context = CausalContext::default();
    for _ in 0..context_count {
        let origin = CausalOrigin {
            node_id: NodeId::new(decoder.string()?.to_string())?,
            incarnation_id: IncarnationId(decoder.u128()?),
            shard_id: decoder.u32()?,
        };
        let sequence = decoder.u64()?;
        if !context.observe_origin(origin, sequence) {
            return Err(ShardCacheError::Protocol(
                "active-sync mutation contains a duplicate causal origin".into(),
            ));
        }
    }
    let key_hash = decoder.u64()?;
    let key = SharedBytes::copy_from_slice(decoder.bytes()?);
    let value = decoder.optional_bytes()?.map(SharedBytes::copy_from_slice);
    let expire_at_ms = match decoder.u64()? {
        u64::MAX => None,
        deadline => Some(deadline),
    };
    let governance = decoder.optional_bytes()?.map(SharedBytes::copy_from_slice);
    let kind = match decoder.u8()? {
        1 => MutationKind::Set,
        2 => MutationKind::Tombstone(TombstoneKind::Delete),
        3 => MutationKind::Tombstone(TombstoneKind::Expired),
        4 => MutationKind::ClusterEvict {
            target: Box::new(decoder.dot()?),
        },
        _ => {
            return Err(ShardCacheError::Protocol(
                "active-sync mutation kind is invalid".into(),
            ));
        }
    };
    Ok(ActiveMutation {
        dot,
        hlc,
        context,
        key_hash,
        key,
        value,
        expire_at_ms,
        governance,
        kind,
    })
}

fn decode_standalone_mutation(payload: &[u8]) -> Result<ActiveMutation> {
    let mut decoder = Decoder::new(payload);
    let mutation = decode_mutation(&mut decoder)?;
    decoder.finish()?;
    Ok(mutation)
}

fn put_dot(out: &mut Vec<u8>, dot: &MutationDot) -> Result<()> {
    put_string(out, dot.node_id.as_str())?;
    out.extend_from_slice(&dot.incarnation_id.0.to_le_bytes());
    out.extend_from_slice(&dot.shard_id.to_le_bytes());
    out.extend_from_slice(&dot.sequence.to_le_bytes());
    Ok(())
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<()> {
    if value.len() > u16::MAX as usize {
        return Err(ShardCacheError::Protocol(
            "active-sync string exceeds wire bounds".into(),
        ));
    }
    out.extend_from_slice(&(value.len() as u16).to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u32(out: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| {
        ShardCacheError::Protocol("active-sync collection exceeds wire bounds".into())
    })?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_u32(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}

fn put_optional_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) -> Result<()> {
    match value {
        Some(value) => put_bytes(out, value),
        None => {
            out.extend_from_slice(&u32::MAX.to_le_bytes());
            Ok(())
        }
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return Err(ShardCacheError::Protocol(
                "active-sync frame contains trailing bytes".into(),
            ));
        }
        Ok(())
    }

    fn bytes_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| ShardCacheError::Protocol("active-sync frame offset overflow".into()))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| ShardCacheError::Protocol("active-sync frame is truncated".into()))?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes_exact(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.bytes_exact(4)?
                .try_into()
                .map_err(|_| ShardCacheError::Protocol("active-sync u32 is truncated".into()))?,
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.bytes_exact(8)?
                .try_into()
                .map_err(|_| ShardCacheError::Protocol("active-sync u64 is truncated".into()))?,
        ))
    }

    fn u128(&mut self) -> Result<u128> {
        Ok(u128::from_le_bytes(
            self.bytes_exact(16)?
                .try_into()
                .map_err(|_| ShardCacheError::Protocol("active-sync u128 is truncated".into()))?,
        ))
    }

    fn string(&mut self) -> Result<&'a str> {
        let len = u16::from_le_bytes(self.bytes_exact(2)?.try_into().map_err(|_| {
            ShardCacheError::Protocol("active-sync string length is truncated".into())
        })?) as usize;
        std::str::from_utf8(self.bytes_exact(len)?)
            .map_err(|_| ShardCacheError::Protocol("active-sync string is not valid UTF-8".into()))
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.u32()? as usize;
        self.bytes_exact(len)
    }

    fn optional_bytes(&mut self) -> Result<Option<&'a [u8]>> {
        let len = self.u32()?;
        if len == u32::MAX {
            return Ok(None);
        }
        self.bytes_exact(len as usize).map(Some)
    }

    fn dot(&mut self) -> Result<MutationDot> {
        Ok(MutationDot {
            node_id: NodeId::new(self.string()?.to_string())?,
            incarnation_id: IncarnationId(self.u128()?),
            shard_id: self.u32()?,
            sequence: self.u64()?,
        })
    }
}

fn handle_ack_or_error(kind: WireFrameKind, payload: &[u8]) -> Result<()> {
    match kind {
        WireFrameKind::Ack if payload.is_empty() => Ok(()),
        WireFrameKind::Error => Err(remote_error(payload)),
        _ => Err(ShardCacheError::Protocol(
            "active-sync expected acknowledgement".into(),
        )),
    }
}

fn handle_unexpected_frame<T>(kind: WireFrameKind, payload: &[u8], expected: &str) -> Result<T> {
    if kind == WireFrameKind::Error {
        return Err(remote_error(payload));
    }
    Err(ShardCacheError::Protocol(format!(
        "active-sync expected {expected}"
    )))
}

fn remote_error(payload: &[u8]) -> ShardCacheError {
    let message = std::str::from_utf8(payload)
        .unwrap_or("remote active-sync protocol error")
        .chars()
        .take(1024)
        .collect::<String>();
    ShardCacheError::Protocol(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose,
    };
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::server::WebPkiClientVerifier;

    struct TestTlsIdentity {
        server: Arc<rustls::ServerConfig>,
        client: Arc<rustls::ClientConfig>,
        client_fingerprint: [u8; 32],
    }

    fn test_tls_identity() -> TestTlsIdentity {
        let mut ca_params = CertificateParams::new(vec!["active-sync-ca".into()]).unwrap();
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
        let mut client_params = CertificateParams::new(vec!["active-client".into()]).unwrap();
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_key = KeyPair::generate().unwrap();
        let client_cert = client_params.signed_by(&client_key, &ca).unwrap();

        let mut roots = RootCertStore::empty();
        roots.add(ca.der().clone()).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            Arc::clone(&provider),
        )
        .build()
        .unwrap();
        let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(
                vec![server_cert.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der())),
            )
            .unwrap();
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![CertificateDer::from(client_cert.der().to_vec())],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(client_key.serialize_der())),
            )
            .unwrap();
        TestTlsIdentity {
            server: Arc::new(server),
            client: Arc::new(client),
            client_fingerprint: Sha256::digest(client_cert.der()).into(),
        }
    }

    #[test]
    fn block_codec_round_trip_and_truncation() {
        let mut config = ActiveSyncConfig::new("cluster", NodeId::new("left").unwrap());
        config.incarnation_id = IncarnationId(1);
        let map = ActiveShardMap::new(1, config).unwrap();
        map.set("key", "value").unwrap();
        map.seal_pending().unwrap();
        let block = map.inner.shards[0].lock().blocks[0].clone();
        let encoded = encode_block(&block).unwrap();
        let decoded = decode_block(&encoded).unwrap();
        assert_eq!(decoded.digest(), block.digest());
        assert_eq!(decoded.records.len(), 1);
        assert!(decode_block(&encoded[..encoded.len() - 1]).is_err());
    }

    #[test]
    fn manifest_codec_rejects_trailing_bytes() {
        let hello = WireHello {
            cluster_id: "cluster".into(),
            node_id: NodeId::new("left").unwrap(),
            incarnation_id: IncarnationId(1),
            shard_id: 0,
            frontiers: BTreeMap::new(),
        };
        let mut encoded = encode_hello(&hello).unwrap();
        assert_eq!(decode_hello(&encoded).unwrap().node_id, hello.node_id);
        encoded.push(0);
        assert!(decode_hello(&encoded).is_err());
    }

    #[test]
    fn live_mtls_sync_converges_and_revocation_blocks_reconnect() {
        let identity = test_tls_identity();
        let left = ActiveShardMap::new(
            1,
            ActiveSyncConfig {
                incarnation_id: IncarnationId(1),
                ..ActiveSyncConfig::new("cluster", NodeId::new("left").unwrap())
            },
        )
        .unwrap();
        let right = ActiveShardMap::new(
            1,
            ActiveSyncConfig {
                incarnation_id: IncarnationId(2),
                ..ActiveSyncConfig::new("cluster", NodeId::new("right").unwrap())
            },
        )
        .unwrap();
        let server_credentials = Arc::new(
            ActiveSyncTlsServerCredentials::new(
                Arc::clone(&identity.server),
                vec![ActiveSyncAuthorizedPeer {
                    node_id: left.node_id().clone(),
                    certificate_sha256: identity.client_fingerprint,
                }],
            )
            .unwrap(),
        );
        let server = ActiveSyncTlsServer::start(
            right.clone(),
            vec!["127.0.0.1:0".parse().unwrap()],
            Arc::clone(&server_credentials),
            ActiveSyncTlsServerOptions::default(),
        )
        .unwrap();
        let peer = ActiveSyncTlsPeer::new(
            right.node_id().clone(),
            server.local_addresses().to_vec(),
            "localhost",
            Arc::new(ActiveSyncTlsClientCredentials::new(Arc::clone(
                &identity.client,
            ))),
        )
        .unwrap();
        left.set("left-key", "left-value").unwrap();
        right.set("right-key", "right-value").unwrap();

        let report = left
            .sync_with_tls_peer(
                &peer,
                SyncOptions::default(),
                Duration::from_secs(2),
                8 * 1024 * 1024,
            )
            .unwrap();
        assert_eq!(report.blocks_to_local, 1);
        assert_eq!(report.blocks_to_peer, 1);
        assert_eq!(left.get("right-key"), Some(b"right-value".to_vec()));
        assert_eq!(right.get("left-key"), Some(b"left-value".to_vec()));

        assert_eq!(
            server_credentials
                .rotate(
                    Arc::clone(&identity.server),
                    vec![ActiveSyncAuthorizedPeer {
                        node_id: left.node_id().clone(),
                        certificate_sha256: identity.client_fingerprint,
                    }],
                    Duration::ZERO,
                )
                .unwrap(),
            2
        );
        assert_eq!(
            peer.credentials
                .rotate(Arc::clone(&identity.client))
                .unwrap(),
            2
        );
        left.set("rotated", "credentials").unwrap();
        left.sync_with_tls_peer(
            &peer,
            SyncOptions::default(),
            Duration::from_secs(2),
            8 * 1024 * 1024,
        )
        .unwrap();
        assert_eq!(right.get("rotated"), Some(b"credentials".to_vec()));

        server_credentials.revoke(left.node_id().clone());
        left.set("blocked", "value").unwrap();
        assert!(
            left.sync_with_tls_peer(
                &peer,
                SyncOptions::default(),
                Duration::from_secs(1),
                8 * 1024 * 1024,
            )
            .is_err()
        );
        assert_eq!(right.get("blocked"), None);
        server.shutdown();
    }

    #[test]
    fn live_mtls_sync_repairs_compacted_origin_with_bounded_state_transfer() {
        let identity = test_tls_identity();
        let mut source_config = ActiveSyncConfig::new("cluster", NodeId::new("source").unwrap());
        source_config.incarnation_id = IncarnationId(10);
        source_config.max_block_records = 1;
        source_config.max_retained_block_bytes_per_shard = 1;
        let source = ActiveShardMap::new(1, source_config).unwrap();
        let sink = ActiveShardMap::new(
            1,
            ActiveSyncConfig {
                incarnation_id: IncarnationId(20),
                ..ActiveSyncConfig::new("cluster", NodeId::new("sink").unwrap())
            },
        )
        .unwrap();
        source.set("one", "1").unwrap();
        source.set("two", "2").unwrap();
        source.seal_pending().unwrap();
        assert_eq!(source.health_snapshot().retained_blocks, 0);

        let server_credentials = Arc::new(
            ActiveSyncTlsServerCredentials::new(
                Arc::clone(&identity.server),
                vec![ActiveSyncAuthorizedPeer {
                    node_id: source.node_id().clone(),
                    certificate_sha256: identity.client_fingerprint,
                }],
            )
            .unwrap(),
        );
        let server = ActiveSyncTlsServer::start(
            sink.clone(),
            vec!["127.0.0.1:0".parse().unwrap()],
            server_credentials,
            ActiveSyncTlsServerOptions::default(),
        )
        .unwrap();
        let peer = ActiveSyncTlsPeer::new(
            sink.node_id().clone(),
            server.local_addresses().to_vec(),
            "localhost",
            Arc::new(ActiveSyncTlsClientCredentials::new(identity.client)),
        )
        .unwrap();
        let report = source
            .sync_with_tls_peer(
                &peer,
                SyncOptions::default(),
                Duration::from_secs(2),
                8 * 1024 * 1024,
            )
            .unwrap();
        assert_eq!(report.state_snapshot_fallbacks, 1);
        assert_eq!(sink.get("one"), Some(b"1".to_vec()));
        assert_eq!(sink.get("two"), Some(b"2".to_vec()));
        server.shutdown();
    }
}
