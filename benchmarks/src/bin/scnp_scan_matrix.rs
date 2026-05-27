//! SCNP SCAN benchmark for the native shardcache protocol.
//!
//! This complements `redis_command_matrix`: it exercises the SCNP wrapper
//! directly, including shard-local scans that can be routed by the client.

use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use shardcache_client_rs::{ShardCacheClient, ShardCacheDirectRouter, ShardCacheDirectShardClient};

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Head-to-head SCNP SCAN benchmark")]
struct Args {
    /// Fanout SCNP/RESP listener, usually the server bind address.
    #[arg(long, default_value = "127.0.0.1:6383")]
    fanout_addr: String,

    /// First direct shard SCNP listener.
    #[arg(long, default_value = "127.0.0.1:6384")]
    direct_addr: String,

    /// Server shard count.
    #[arg(long, default_value_t = 4)]
    shard_count: usize,

    /// Comma-separated modes to run.
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_value = "generic,shard-fanout,shard-direct"
    )]
    modes: Vec<ScanMode>,

    /// Concurrent client connections per mode.
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Warmup seconds before recording.
    #[arg(long, default_value_t = 1)]
    warmup: u64,

    /// Measurement duration in seconds.
    #[arg(long, default_value_t = 5)]
    duration: u64,

    /// Number of string keys to seed before measuring.
    #[arg(long, default_value_t = 65_536)]
    key_count: usize,

    /// COUNT argument sent to SCAN.
    #[arg(long, default_value_t = 1_000)]
    count: usize,

    /// Bytes per seeded value.
    #[arg(long, default_value_t = 1)]
    value_size: usize,

    /// Optional CSV output path.
    #[arg(long)]
    csv: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ScanMode {
    Generic,
    ShardFanout,
    ShardDirect,
}

impl ScanMode {
    fn label(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::ShardFanout => "shard-fanout",
            Self::ShardDirect => "shard-direct",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Stats {
    ops: u64,
    errors: u64,
    elapsed_ns: u128,
    bytes: u128,
}

impl Stats {
    fn add(&mut self, other: &Self) {
        self.ops = self.ops.saturating_add(other.ops);
        self.errors = self.errors.saturating_add(other.errors);
        self.elapsed_ns = self.elapsed_ns.saturating_add(other.elapsed_ns);
        self.bytes = self.bytes.saturating_add(other.bytes);
    }

    fn ops_per_sec(&self, duration: Duration) -> f64 {
        self.ops as f64 / duration.as_secs_f64()
    }

    fn avg_us(&self) -> f64 {
        match self.ops {
            0 => 0.0,
            ops => self.elapsed_ns as f64 / ops as f64 / 1_000.0,
        }
    }

    fn avg_bytes(&self) -> f64 {
        match self.ops {
            0 => 0.0,
            ops => self.bytes as f64 / ops as f64,
        }
    }
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    if args.clients == 0 {
        return Err("--clients must be at least 1".into());
    }
    if args.key_count == 0 {
        return Err("--key-count must be at least 1".into());
    }

    seed_keys(&args.fanout_addr, args.key_count, args.value_size)?;

    let duration = Duration::from_secs(args.duration);
    let warmup = Duration::from_secs(args.warmup);
    let router = ShardCacheDirectRouter::new(args.direct_addr.as_str(), args.shard_count)?;

    println!(
        "scnp-scan-matrix: fanout={} direct={} modes={} clients={} keys={} count={} warmup={}s duration={}s",
        args.fanout_addr,
        args.direct_addr,
        args.modes
            .iter()
            .map(|mode| mode.label())
            .collect::<Vec<_>>()
            .join(","),
        args.clients,
        args.key_count,
        args.count,
        args.warmup,
        args.duration,
    );
    println!();
    println!("| mode | ops/sec | avg us | avg bytes | errors |");
    println!("| --- | ---: | ---: | ---: | ---: |");

    let mut csv = match args.csv.as_deref() {
        Some(path) => Some(File::create(path)?),
        None => None,
    };
    if let Some(csv) = csv.as_mut() {
        writeln!(
            csv,
            "mode,clients,duration_s,key_count,count,ops,ops_per_sec,avg_us,avg_bytes,errors"
        )?;
    }

    for mode in args.modes {
        let stats = run_mode(
            mode,
            &args.fanout_addr,
            router,
            args.clients,
            args.count,
            warmup,
            duration,
        )?;
        println!(
            "| {} | {:.0} | {:.2} | {:.0} | {} |",
            mode.label(),
            stats.ops_per_sec(duration),
            stats.avg_us(),
            stats.avg_bytes(),
            stats.errors,
        );
        if let Some(csv) = csv.as_mut() {
            writeln!(
                csv,
                "{},{},{},{},{},{},{:.3},{:.3},{:.3},{}",
                mode.label(),
                args.clients,
                args.duration,
                args.key_count,
                args.count,
                stats.ops,
                stats.ops_per_sec(duration),
                stats.avg_us(),
                stats.avg_bytes(),
                stats.errors,
            )?;
        }
    }

    Ok(())
}

fn seed_keys(addr: &str, key_count: usize, value_size: usize) -> Result<(), BoxError> {
    let mut client = ShardCacheClient::connect(addr)?;
    let value = make_value(value_size);
    for index in 0..key_count {
        let key = format!("ks:{index:08}");
        client.set(key.as_bytes(), &value)?;
    }
    Ok(())
}

fn make_value(size: usize) -> Vec<u8> {
    const PATTERN: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._";
    (0..size)
        .map(|index| PATTERN[index % PATTERN.len()])
        .collect()
}

fn run_mode(
    mode: ScanMode,
    fanout_addr: &str,
    router: ShardCacheDirectRouter,
    clients: usize,
    count: usize,
    warmup: Duration,
    duration: Duration,
) -> Result<Stats, BoxError> {
    let fanout_addr = Arc::<str>::from(fanout_addr);
    let mut handles = Vec::with_capacity(clients);
    for worker_id in 0..clients {
        let fanout_addr = Arc::clone(&fanout_addr);
        handles.push(thread::spawn(move || {
            let mut worker = ScanWorker::connect(mode, fanout_addr.as_ref(), router, worker_id)?;
            let mut out = Vec::with_capacity(64 * 1024);
            if !warmup.is_zero() {
                run_scan_loop(&mut worker, count, Instant::now() + warmup, &mut out, None)?;
            }
            let mut stats = Stats::default();
            run_scan_loop(
                &mut worker,
                count,
                Instant::now() + duration,
                &mut out,
                Some(&mut stats),
            )?;
            Ok::<Stats, BoxError>(stats)
        }));
    }

    let mut total = Stats::default();
    for handle in handles {
        total.add(&handle.join().map_err(|_| "SCNP scan worker panicked")??);
    }
    Ok(total)
}

enum ScanWorker {
    Generic(ShardCacheClient),
    ShardFanout {
        client: ShardCacheClient,
        shard_id: usize,
    },
    ShardDirect(ShardCacheDirectShardClient),
}

impl ScanWorker {
    fn connect(
        mode: ScanMode,
        fanout_addr: &str,
        router: ShardCacheDirectRouter,
        worker_id: usize,
    ) -> Result<Self, BoxError> {
        let shard_id = worker_id % router.shard_count();
        Ok(match mode {
            ScanMode::Generic => Self::Generic(ShardCacheClient::connect(fanout_addr)?),
            ScanMode::ShardFanout => Self::ShardFanout {
                client: ShardCacheClient::connect(fanout_addr)?,
                shard_id,
            },
            ScanMode::ShardDirect => Self::ShardDirect(router.connect_shard(shard_id)?),
        })
    }

    fn scan_into(&mut self, count: usize, out: &mut Vec<u8>) -> Result<bool, BoxError> {
        match self {
            Self::Generic(client) => Ok(client.scan_resp_into(0, count, out)?),
            Self::ShardFanout { client, shard_id } => {
                Ok(client.scan_shard_resp_into(*shard_id, 0, count, out)?)
            }
            Self::ShardDirect(client) => Ok(client.scan_resp_into(0, count, out)?),
        }
    }
}

fn run_scan_loop(
    worker: &mut ScanWorker,
    count: usize,
    deadline: Instant,
    out: &mut Vec<u8>,
    mut stats: Option<&mut Stats>,
) -> Result<(), BoxError> {
    while Instant::now() < deadline {
        let started = Instant::now();
        let ok = worker.scan_into(count, out)?;
        let elapsed = started.elapsed();
        if let Some(stats) = stats.as_deref_mut() {
            stats.ops = stats.ops.saturating_add(1);
            stats.elapsed_ns = stats.elapsed_ns.saturating_add(elapsed.as_nanos());
            stats.bytes = stats.bytes.saturating_add(out.len() as u128);
            if !ok {
                stats.errors = stats.errors.saturating_add(1);
            }
        }
    }
    Ok(())
}
