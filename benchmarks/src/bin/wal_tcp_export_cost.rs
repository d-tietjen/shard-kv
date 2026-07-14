//! TCP WAL export benchmark.
//!
//! This measures the production persistence runtime with disk WAL append plus
//! live TCP export. Run once with the std exporter and once with
//! `SHARDCACHE_WAL_TCP_USE_MONOIO=1`.

use std::error::Error;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use shardcache_benchmarks::cpu::{process_cpu_time, vcpu};
use shardcache_benchmarks::workload::{KeyDistribution, KeyPattern, Workload, WorkloadSpec};
use shardmap::config::{PersistenceConfig, WalTcpExportMode};
use shardmap::persistence::PersistenceRuntime;
use shardmap::storage::{MutationBytes, MutationOp, MutationRecord};

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Measure TCP WAL export cost")]
struct Args {
    #[arg(long, default_value = "64,4096")]
    value_sizes: String,

    #[arg(long, default_value_t = 16)]
    shards: usize,

    #[arg(long, default_value_t = 16)]
    clients: usize,

    #[arg(long, default_value_t = 100_000)]
    key_count: usize,

    #[arg(long, default_value_t = 10)]
    duration: u64,

    #[arg(long, default_value_t = 2)]
    warmup: u64,

    #[arg(long, default_value_t = 65_536)]
    wal_channel_capacity: usize,

    #[arg(long, default_value_t = 65_536)]
    tcp_channel_capacity: usize,

    #[arg(long, default_value_t = 4_096)]
    value_pool_count: usize,

    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    value_pool_max_bytes: usize,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let value_sizes = parse_usize_list(&args.value_sizes)?;
    println!(
        "wal_export={} shards={} clients={} duration={} warmup={}",
        runtime_label(),
        args.shards,
        args.clients,
        args.duration,
        args.warmup
    );
    println!(
        "| value | pool | append/s | vCPU | ns/op | WAL MiB/s | TCP MiB/s | collector MiB/s | queued | sent | dropped | failures | collector frames |"
    );
    println!(
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );

    for value_size in value_sizes {
        let values = Arc::new(ValueCorpus::build(
            value_size,
            args.key_count,
            args.value_pool_count,
            args.value_pool_max_bytes,
        ));
        let workload = Arc::new(Workload::build(&WorkloadSpec {
            key_count: args.key_count,
            value_size,
            mix: shardcache_benchmarks::workload::Mix::write_only(),
            key_pattern: KeyPattern::Point,
            key_distribution: KeyDistribution::Uniform,
        }));
        let result = run_once(&args, Arc::clone(&workload), Arc::clone(&values))?;
        println!(
            "| {} | {} | {:.2} | {:.3} | {:.1} | {:.2} | {:.2} | {:.2} | {} | {} | {} | {} | {} |",
            value_size,
            values.len(),
            result.ops_per_sec,
            result.vcpu,
            result.ns_per_op,
            result.wal_mib_per_sec,
            result.tcp_mib_per_sec,
            result.collector_mib_per_sec,
            result.tcp_queued,
            result.tcp_sent,
            result.tcp_dropped,
            result.tcp_failures,
            result.collector_frames,
        );
    }
    Ok(())
}

struct ValueCorpus {
    values: Vec<Vec<u8>>,
}

impl ValueCorpus {
    fn build(
        value_size: usize,
        key_count: usize,
        requested_pool_count: usize,
        max_pool_bytes: usize,
    ) -> Self {
        let byte_limited = max_pool_bytes
            .checked_div(value_size)
            .unwrap_or(requested_pool_count);
        let pool_count = requested_pool_count
            .min(key_count)
            .min(byte_limited.max(1))
            .max(1);
        let mut values = Vec::with_capacity(pool_count);
        for idx in 0..pool_count {
            values.push(semi_random_value(value_size, idx as u64));
        }
        Self { values }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn value_for(&self, key_index: usize) -> &[u8] {
        &self.values[key_index % self.values.len()]
    }
}

struct RunResult {
    ops_per_sec: f64,
    vcpu: f64,
    ns_per_op: f64,
    wal_mib_per_sec: f64,
    tcp_mib_per_sec: f64,
    collector_mib_per_sec: f64,
    tcp_queued: u64,
    tcp_sent: u64,
    tcp_dropped: u64,
    tcp_failures: u64,
    collector_frames: u64,
}

fn run_once(
    args: &Args,
    workload: Arc<Workload>,
    values: Arc<ValueCorpus>,
) -> Result<RunResult, BoxError> {
    let collector = Collector::start()?;
    let data_dir = bench_data_dir();
    let _ = std::fs::remove_dir_all(&data_dir);
    let mut config = PersistenceConfig {
        data_dir: data_dir.clone(),
        compress_wal: false,
        compress_snapshots: false,
        segment_size_bytes: 1024 * 1024 * 1024,
        fsync_interval_ms: 10_000,
        wal_channel_capacity: args.wal_channel_capacity,
        ..PersistenceConfig::default()
    };
    config.tcp_export.enabled = true;
    config.tcp_export.mode = WalTcpExportMode::Connect;
    config.tcp_export.addr = collector.addr.to_string();
    config.tcp_export.channel_capacity = args.tcp_channel_capacity;
    config.tcp_export.backpressure_on_full = true;
    config.tcp_export.connect_timeout_ms = 250;
    config.tcp_export.write_timeout_ms = 5_000;
    config.tcp_export.reconnect_backoff_ms = 10;

    let runtime = Arc::new(PersistenceRuntime::start(
        args.shards.next_power_of_two(),
        config,
    )?);
    let ops = run_appenders(args, Arc::clone(&runtime), workload, values)?;
    thread::sleep(Duration::from_millis(250));

    let stats = runtime.stats_snapshot();
    runtime.shutdown().ok();
    let collector_stats = collector.shutdown();
    let _ = std::fs::remove_dir_all(data_dir);

    Ok(RunResult {
        ops_per_sec: ops.ops_per_sec,
        vcpu: ops.vcpu,
        ns_per_op: ops.ns_per_op,
        wal_mib_per_sec: stats.bytes_written as f64 / 1024.0 / 1024.0 / ops.duration,
        tcp_mib_per_sec: stats.tcp_export_bytes_sent as f64 / 1024.0 / 1024.0 / ops.duration,
        collector_mib_per_sec: collector_stats.bytes as f64 / 1024.0 / 1024.0 / ops.duration,
        tcp_queued: stats.tcp_export_frames_queued,
        tcp_sent: stats.tcp_export_frames_sent,
        tcp_dropped: stats.tcp_export_frames_dropped,
        tcp_failures: stats.tcp_export_write_failures + stats.tcp_export_connect_failures,
        collector_frames: collector_stats.frames,
    })
}

struct OpsResult {
    ops_per_sec: f64,
    vcpu: f64,
    ns_per_op: f64,
    duration: f64,
}

fn run_appenders(
    args: &Args,
    runtime: Arc<PersistenceRuntime>,
    workload: Arc<Workload>,
    values: Arc<ValueCorpus>,
) -> Result<OpsResult, BoxError> {
    let stop = Arc::new(AtomicBool::new(false));
    let measured = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(args.clients);
    for worker in 0..args.clients {
        let shard_id = worker % args.shards.next_power_of_two();
        let appender = runtime.appender(shard_id).ok_or("missing WAL appender")?;
        let workload = Arc::clone(&workload);
        let values = Arc::clone(&values);
        let stop = Arc::clone(&stop);
        let measured = Arc::clone(&measured);
        let ops = Arc::clone(&ops);
        handles.push(thread::spawn(move || {
            let mut local_ops = 0_u64;
            let mut sequence = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                let key_index = sequence as usize % workload.keys().len();
                sequence = sequence.wrapping_add(1);
                let record = MutationRecord {
                    shard_id,
                    sequence,
                    timestamp_ms: 42,
                    op: MutationOp::Set,
                    key: MutationBytes::copy_from_slice(&workload.keys()[key_index]),
                    value: MutationBytes::copy_from_slice(values.value_for(key_index)),
                    expire_at_ms: None,
                    governance: None,
                };
                if appender.append(record).is_err() {
                    break;
                }
                if measured.load(Ordering::Relaxed) {
                    local_ops += 1;
                }
            }
            ops.fetch_add(local_ops, Ordering::Relaxed);
        }));
    }
    thread::sleep(Duration::from_secs(args.warmup));
    let cpu_start = process_cpu_time();
    let started = Instant::now();
    measured.store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_secs(args.duration));
    measured.store(false, Ordering::Relaxed);
    let elapsed = started.elapsed();
    let cpu_end = process_cpu_time();
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().map_err(|_| "WAL bench worker panicked")?;
    }
    let total = ops.load(Ordering::Relaxed);
    let secs = elapsed.as_secs_f64();
    Ok(OpsResult {
        ops_per_sec: total as f64 / secs,
        vcpu: vcpu(cpu_end.saturating_sub(cpu_start), elapsed),
        ns_per_op: if total == 0 {
            0.0
        } else {
            elapsed.as_nanos() as f64 / total as f64
        },
        duration: secs,
    })
}

struct Collector {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    frames: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    join: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct CollectorStats {
    frames: u64,
    bytes: u64,
}

impl Collector {
    fn start() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let frames = Arc::new(AtomicU64::new(0));
        let bytes = Arc::new(AtomicU64::new(0));
        let join = {
            let stop = Arc::clone(&stop);
            let frames = Arc::clone(&frames);
            let bytes = Arc::clone(&bytes);
            thread::spawn(move || run_collector(listener, stop, frames, bytes))
        };
        Ok(Self {
            addr,
            stop,
            frames,
            bytes,
            join: Some(join),
        })
    }

    fn shutdown(mut self) -> CollectorStats {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        CollectorStats {
            frames: self.frames.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

fn run_collector(
    listener: TcpListener,
    stop: Arc<AtomicBool>,
    frames: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
) {
    let Ok((mut stream, _peer)) = listener.accept() else {
        return;
    };
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .ok();
    while !stop.load(Ordering::Relaxed) {
        match read_wal_frame_len(&mut stream) {
            Ok(Some(len)) => {
                frames.fetch_add(1, Ordering::Relaxed);
                bytes.fetch_add(len as u64, Ordering::Relaxed);
            }
            Ok(None) => break,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(_) => break,
        }
    }
}

fn read_wal_frame_len(stream: &mut TcpStream) -> io::Result<Option<usize>> {
    let mut header = [0_u8; 9];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let payload_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;
    let mut tail = vec![0_u8; payload_len + 4];
    stream.read_exact(&mut tail)?;
    Ok(Some(header.len() + tail.len()))
}

fn semi_random_value(value_size: usize, seed: u64) -> Vec<u8> {
    let mut rng = SmallRng::seed_from_u64(0xFCA5_2026 ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut value = vec![0u8; value_size];
    rng.fill_bytes(&mut value);
    for (block_idx, block) in value.chunks_mut(64).enumerate() {
        let marker = seed.wrapping_add(block_idx as u64).to_le_bytes();
        for (idx, byte) in marker.iter().copied().enumerate().take(block.len().min(8)) {
            block[idx] = byte;
        }
    }
    value
}

fn parse_usize_list(values: &str) -> Result<Vec<usize>, BoxError> {
    values
        .split(',')
        .map(|value| {
            value
                .trim()
                .parse::<usize>()
                .map_err(|error| format!("invalid usize `{value}`: {error}").into())
        })
        .collect()
}

fn bench_data_dir() -> PathBuf {
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    PathBuf::from(format!("/tmp/shardcache-wal-bench-{pid}-{nanos}"))
}

fn runtime_label() -> &'static str {
    match std::env::var("SHARDCACHE_WAL_TCP_USE_MONOIO").is_ok_and(|value| value != "0") {
        true => "monoio",
        false => "std",
    }
}
