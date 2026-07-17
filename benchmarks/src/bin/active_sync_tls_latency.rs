//! Two-process active-sync mTLS latency benchmark.

use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand};
use hdrhistogram::Histogram;
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use sha2::{Digest, Sha256};
use shardcache_benchmarks::histogram::format_ns;
use shardmap::{
    ActiveShardMap, ActiveSyncAuthorizedPeer, ActiveSyncConfig, ActiveSyncTlsClientCredentials,
    ActiveSyncTlsPeer, ActiveSyncTlsServer, ActiveSyncTlsServerCredentials,
    ActiveSyncTlsServerOptions, NodeId, SyncOptions,
};

type BoxError = Box<dyn Error + Send + Sync>;

const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(about = "Measure active-sync latency between two real mTLS nodes")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Receive and apply writes until the source sends the completion marker.
    Sink(SinkArgs),
    /// Write locally and synchronously replicate each batch to the sink.
    Source(SourceArgs),
}

#[derive(Debug, Args)]
struct CommonTlsArgs {
    /// PEM certificate chain for this node.
    #[arg(long)]
    cert: PathBuf,

    /// PEM private key for this node.
    #[arg(long)]
    key: PathBuf,

    /// PEM certificate authorities trusted for the peer.
    #[arg(long)]
    ca: PathBuf,
}

#[derive(Debug, Args)]
struct SinkArgs {
    #[command(flatten)]
    tls: CommonTlsArgs,

    #[arg(long, default_value = "active-sync-latency")]
    cluster_id: String,

    #[arg(long, default_value = "adam")]
    node_id: String,

    #[arg(long, default_value = "laptop")]
    authorized_node_id: String,

    /// Source leaf certificate used for node-ID authorization.
    #[arg(long)]
    authorized_cert: PathBuf,

    /// Comma-separated direct shard listener addresses.
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:18443")]
    listen: Vec<SocketAddr>,

    #[arg(long, default_value_t = 300)]
    timeout_seconds: u64,
}

#[derive(Debug, Args)]
struct SourceArgs {
    #[command(flatten)]
    tls: CommonTlsArgs,

    #[arg(long, default_value = "active-sync-latency")]
    cluster_id: String,

    #[arg(long, default_value = "laptop")]
    node_id: String,

    #[arg(long, default_value = "adam")]
    peer_node_id: String,

    /// Comma-separated direct shard addresses, normally SSH-forwarded locally.
    #[arg(long, value_delimiter = ',', default_value = "127.0.0.1:18443")]
    peer: Vec<SocketAddr>,

    #[arg(long, default_value = "localhost")]
    server_name: String,

    /// Comma-separated write counts per synchronization round.
    #[arg(long, value_delimiter = ',', default_value = "1,64")]
    batch_sizes: Vec<usize>,

    #[arg(long, default_value_t = 200)]
    rounds: usize,

    #[arg(long, default_value_t = 20)]
    warmup_rounds: usize,

    #[arg(long, default_value_t = 1024)]
    value_size: usize,

    #[arg(long, default_value_t = 10)]
    io_timeout_seconds: u64,
}

fn main() -> Result<(), BoxError> {
    match Cli::parse().command {
        Command::Sink(args) => run_sink(args),
        Command::Source(args) => run_source(args),
    }
}

fn run_sink(args: SinkArgs) -> Result<(), BoxError> {
    let shard_count = validate_shard_count(args.listen.len())?;
    let map = active_map(shard_count, &args.cluster_id, &args.node_id)?;
    let server_config = server_config(&args.tls)?;
    let authorized = ActiveSyncAuthorizedPeer {
        node_id: NodeId::new(args.authorized_node_id)?,
        certificate_sha256: certificate_fingerprint(&args.authorized_cert)?,
    };
    let credentials = Arc::new(ActiveSyncTlsServerCredentials::new(
        server_config,
        vec![authorized],
    )?);
    let server = ActiveSyncTlsServer::start(
        map.clone(),
        args.listen,
        credentials,
        ActiveSyncTlsServerOptions::default(),
    )?;
    println!(
        "READY node={} shards={} addresses={:?}",
        args.node_id,
        shard_count,
        server.local_addresses()
    );

    let deadline = Instant::now() + Duration::from_secs(args.timeout_seconds);
    loop {
        if let Some(value) = map.get(completion_key()) {
            let expected = std::str::from_utf8(&value)?.parse::<usize>()?;
            let health = map.health_snapshot();
            if health.live_versions != expected.saturating_add(1) {
                return Err(format!(
                    "sink applied {} live values, expected {} data values plus completion marker",
                    health.live_versions, expected
                )
                .into());
            }
            println!(
                "COMPLETE node={} replicated_values={} live_versions={}",
                args.node_id, expected, health.live_versions
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("sink timed out waiting for completion marker".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn run_source(args: SourceArgs) -> Result<(), BoxError> {
    let shard_count = validate_shard_count(args.peer.len())?;
    if args.batch_sizes.is_empty() || args.batch_sizes.contains(&0) {
        return Err("batch sizes must be nonzero".into());
    }
    if args.rounds == 0 || args.value_size == 0 || args.io_timeout_seconds == 0 {
        return Err("rounds, value size, and I/O timeout must be nonzero".into());
    }

    let map = active_map(shard_count, &args.cluster_id, &args.node_id)?;
    let credentials = Arc::new(ActiveSyncTlsClientCredentials::new(client_config(
        &args.tls,
    )?));
    let peer = ActiveSyncTlsPeer::new(
        NodeId::new(args.peer_node_id)?,
        args.peer,
        args.server_name,
        credentials,
    )?;
    let value = vec![0x5a; args.value_size];
    let io_timeout = Duration::from_secs(args.io_timeout_seconds);
    let mut total_values = 0usize;

    println!(
        "active-sync mTLS: shards={} rounds={} warmup={} value_bytes={}",
        shard_count, args.rounds, args.warmup_rounds, args.value_size
    );
    println!("| batch | writes | p50 | p95 | p99 | p99.9 | writes/s |");
    println!("|---:|---:|---:|---:|---:|---:|---:|");

    for batch_size in args.batch_sizes {
        for round in 0..args.warmup_rounds {
            write_batch(&map, &value, "warmup", batch_size, round, total_values)?;
            total_values = total_values.saturating_add(batch_size);
            sync_round(&map, &peer, io_timeout)?;
        }

        let histogram_max_ns = io_timeout.as_nanos().min(u128::from(u64::MAX)) as u64;
        let mut latency = Histogram::<u64>::new_with_bounds(1, histogram_max_ns, 3)?;
        let started = Instant::now();
        for round in 0..args.rounds {
            let round_started = Instant::now();
            write_batch(&map, &value, "measure", batch_size, round, total_values)?;
            total_values = total_values.saturating_add(batch_size);
            let report = sync_round(&map, &peer, io_timeout)?;
            if report.blocks_to_peer == 0 {
                return Err(format!(
                    "sync round did not acknowledge an outbound block: {report:?}"
                )
                .into());
            }
            latency.record(round_started.elapsed().as_nanos() as u64)?;
        }
        let elapsed = started.elapsed();
        let writes = args.rounds.saturating_mul(batch_size);
        let writes_per_second = writes as f64 / elapsed.as_secs_f64();
        println!(
            "| {batch_size} | {writes} | {} | {} | {} | {} | {:.0} |",
            format_ns(latency.value_at_quantile(0.5)),
            format_ns(latency.value_at_quantile(0.95)),
            format_ns(latency.value_at_quantile(0.99)),
            format_ns(latency.value_at_quantile(0.999)),
            writes_per_second
        );
    }

    map.set(completion_key(), total_values.to_string())?;
    let report = sync_round(&map, &peer, io_timeout)?;
    if report.blocks_to_peer == 0 {
        return Err("completion marker was not acknowledged".into());
    }
    println!("ACKNOWLEDGED replicated_values={total_values}");
    Ok(())
}

fn active_map(
    shard_count: usize,
    cluster_id: &str,
    node_id: &str,
) -> Result<ActiveShardMap, BoxError> {
    let config = ActiveSyncConfig::new(cluster_id, NodeId::new(node_id)?);
    Ok(ActiveShardMap::new_causal_eventual(shard_count, config)?)
}

fn validate_shard_count(shard_count: usize) -> Result<usize, BoxError> {
    if shard_count == 0 || !shard_count.is_power_of_two() {
        return Err("direct shard address count must be a nonzero power of two".into());
    }
    Ok(shard_count)
}

fn write_batch(
    map: &ActiveShardMap,
    value: &[u8],
    phase: &str,
    batch_size: usize,
    round: usize,
    offset: usize,
) -> Result<(), BoxError> {
    for item in 0..batch_size {
        let sequence = offset.saturating_add(item);
        map.set(
            format!("active-sync-latency/{phase}/{round}/{sequence}"),
            value,
        )?;
    }
    Ok(())
}

fn sync_round(
    map: &ActiveShardMap,
    peer: &ActiveSyncTlsPeer,
    io_timeout: Duration,
) -> Result<shardmap::BidirectionalSyncReport, BoxError> {
    Ok(map.sync_with_tls_peer(
        peer,
        SyncOptions::default(),
        io_timeout,
        DEFAULT_MAX_FRAME_BYTES,
    )?)
}

fn completion_key() -> &'static [u8] {
    b"active-sync-latency/complete"
}

fn server_config(args: &CommonTlsArgs) -> Result<Arc<rustls::ServerConfig>, BoxError> {
    let roots = root_store(&args.ca)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
            .build()?;
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(read_certificates(&args.cert)?, read_private_key(&args.key)?)?;
    Ok(Arc::new(config))
}

fn client_config(args: &CommonTlsArgs) -> Result<Arc<rustls::ClientConfig>, BoxError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(root_store(&args.ca)?)
        .with_client_auth_cert(read_certificates(&args.cert)?, read_private_key(&args.key)?)?;
    Ok(Arc::new(config))
}

fn root_store(path: &Path) -> Result<RootCertStore, BoxError> {
    let mut roots = RootCertStore::empty();
    for certificate in read_certificates(path)? {
        roots.add(certificate)?;
    }
    if roots.is_empty() {
        return Err(format!("CA file {} contains no certificates", path.display()).into());
    }
    Ok(roots)
}

fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, BoxError> {
    let mut reader = BufReader::new(File::open(path)?);
    let certificates = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(format!("certificate file {} is empty", path.display()).into());
    }
    Ok(certificates)
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, BoxError> {
    let mut reader = BufReader::new(File::open(path)?);
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| format!("private key file {} is empty", path.display()).into())
}

fn certificate_fingerprint(path: &Path) -> Result<[u8; 32], BoxError> {
    let certificate = read_certificates(path)?
        .into_iter()
        .next()
        .ok_or("authorized certificate file is empty")?;
    Ok(Sha256::digest(certificate.as_ref()).into())
}
