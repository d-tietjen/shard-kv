//! Closed-loop saturation driver.
//!
//! For each requested backend, spawn `--clients` worker threads, run for
//! `--duration` seconds, and report:
//!
//!   peak ops/sec, logical payload GB/s, CPU at peak (vCPU), p50/p99/p99.9 at peak
//!
//! Output goes to stdout as a markdown table and optionally to a CSV.

use std::error::Error;
use std::io::{self, Write};

type BoxError = Box<dyn Error + Send + Sync>;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use shardcache_benchmarks::backend::{Backend, BackendClass, Op, ReadMode};
use shardcache_benchmarks::backends::{BACKEND_IDS, BenchmarkCacheConfig, make};
use shardcache_benchmarks::clock::FastClock;
use shardcache_benchmarks::cpu::{external_cpu_time, process_cpu_time, vcpu};
use shardcache_benchmarks::csv::CsvWriter;
use shardcache_benchmarks::histogram::{LatencyHistogram, format_ns};
use shardcache_benchmarks::workload::{
    KeyDistribution, KeyPattern, Mix, OpStream, Workload, WorkloadSpec,
};
use shardmap::config::EvictionPolicy;

#[derive(Parser, Debug, Clone)]
#[command(about = "Closed-loop saturation benchmark")]
struct Args {
    /// Comma-separated backend ids. Default: all.
    #[arg(long)]
    backends: Option<String>,

    /// host:port for networked backends.
    #[arg(long)]
    addr: Option<String>,

    /// External server PID for CPU sampling (Linux).
    #[arg(long)]
    server_pid: Option<u32>,

    /// Server shard count where applicable (advisory for in-process embedded).
    #[arg(long, default_value_t = 4)]
    vcpu_budget: usize,

    /// SCNP direct-shard client fan-out: number of shard-owned ports to connect
    /// and route keys across. Must equal the server's `--shard-count`. 0 means
    /// "use --vcpu-budget" (back-compat with the historical coupling).
    #[arg(long, default_value_t = 0)]
    scnp_shards: usize,

    /// Concurrent client threads / connections.
    #[arg(long, default_value_t = 16)]
    clients: usize,

    /// Requests to write before flushing and reading responses.
    ///
    /// A depth of 1 is strict closed-loop request/response. Values greater
    /// than 1 require a network backend with explicit pipelining support.
    #[arg(long, default_value_t = 1)]
    pipeline_depth: usize,

    /// Value size in bytes.
    #[arg(long, default_value_t = 512)]
    value_size: usize,

    /// Mix: "get", "set", "80-20", or "<get_pct>-<set_pct>".
    #[arg(long, default_value = "80-20")]
    mix: String,

    /// Key set cardinality.
    #[arg(long, default_value_t = 100_000)]
    key_count: usize,

    /// Key pattern: point or session.
    #[arg(long, default_value = "point")]
    key_pattern: String,

    /// Key distribution: uniform, zipf[:theta], or hot:<keys>[:pct].
    #[arg(long, default_value = "uniform")]
    key_distribution: String,

    /// Measurement duration in seconds (after warmup).
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Warmup duration in seconds.
    #[arg(long, default_value_t = 2)]
    warmup: u64,

    /// Record one latency sample every N successful measured operations.
    /// Use 0 to disable latency timing when measuring raw embedded throughput.
    #[arg(long, default_value_t = 1)]
    latency_sample_rate: u64,

    /// Optional CSV path to append results.
    #[arg(long)]
    csv: Option<String>,

    /// Relative TTL in milliseconds for SET operations and warmup inserts.
    /// Backends that do not implement TTL workloads are skipped.
    #[arg(long)]
    ttl_ms: Option<u64>,

    /// Memory eviction policy for capacity-bounded cache runs.
    #[arg(long, value_enum, default_value_t = BenchEvictionPolicy::None)]
    eviction_policy: BenchEvictionPolicy,

    /// Entry-capacity hint for backends with entry-count capacity.
    #[arg(long)]
    cache_capacity_keys: Option<usize>,

    /// Total byte capacity for backends with memory-budget eviction.
    #[arg(long)]
    cache_memory_bytes: Option<usize>,

    /// GET behavior for backends that can choose between borrowed references
    /// and materialized copy-out reads.
    #[arg(long, value_enum, default_value_t = ReadMode::Ref)]
    read_mode: ReadMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BenchEvictionPolicy {
    None,
    Lru,
    #[cfg(feature = "prefix-eviction")]
    Prefix,
}

impl From<BenchEvictionPolicy> for EvictionPolicy {
    fn from(value: BenchEvictionPolicy) -> Self {
        match value {
            BenchEvictionPolicy::None => Self::None,
            BenchEvictionPolicy::Lru => Self::Lru,
            #[cfg(feature = "prefix-eviction")]
            BenchEvictionPolicy::Prefix => Self::Prefix,
        }
    }
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let command_started = Instant::now();
    if args.pipeline_depth == 0 {
        return Err("--pipeline-depth must be at least 1".into());
    }

    let backend_ids: Vec<String> = match &args.backends {
        Some(s) => s.split(',').map(|s| s.trim().to_string()).collect(),
        None => BACKEND_IDS.iter().map(|s| s.to_string()).collect(),
    };

    let mix = Mix::parse(&args.mix).map_err(|e| -> BoxError { e.into() })?;
    let key_pattern = KeyPattern::parse(&args.key_pattern).map_err(|e| -> BoxError { e.into() })?;
    let key_distribution =
        KeyDistribution::parse(&args.key_distribution).map_err(|e| -> BoxError { e.into() })?;

    let spec = WorkloadSpec {
        key_count: args.key_count,
        value_size: args.value_size,
        mix,
        key_pattern,
        key_distribution,
    };
    let workload = Arc::new(Workload::build(&spec));

    println!(
        "saturation: value_size={}B mix={} key_pattern={} key_distribution={} vcpu_budget={} clients={} pipeline_depth={} keys={} duration={}s ttl={} eviction={} capacity_keys={} memory_bytes={} read_mode={}",
        args.value_size,
        mix.label(),
        key_pattern.label(),
        key_distribution.label(),
        args.vcpu_budget,
        args.clients,
        args.pipeline_depth,
        args.key_count,
        args.duration,
        args.ttl_ms
            .map(|ttl_ms| format!("{ttl_ms}ms"))
            .unwrap_or_else(|| "none".to_string()),
        args.eviction_policy.label(),
        args.cache_capacity_keys
            .map_or_else(|| "default".to_string(), |value| value.to_string()),
        args.cache_memory_bytes
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
        args.read_mode.label()
    );
    println!();
    print_header();

    let csv_header = vec![
        "backend",
        "build_variant",
        "ttl_mode",
        "routing_mode",
        "eviction_policy",
        "cache_capacity_keys",
        "cache_memory_bytes",
        "read_mode",
        "value_size",
        "mix",
        "vcpu_budget",
        "clients",
        "pipeline_depth",
        "duration_s",
        "ops_total",
        "ops_per_sec",
        "logical_payload_gb_per_sec",
        "vcpu_consumed",
        "p50_ns",
        "p99_ns",
        "p999_ns",
        "read_ops",
        "read_p99_ns",
        "read_p999_ns",
        "write_ops",
        "write_p99_ns",
        "write_p999_ns",
        "errors",
        "latency_sample_rate",
        "ttl_ms",
        "key_pattern",
        "key_distribution",
    ];
    let mut csv = CsvWriter::new(args.csv.as_ref(), csv_header);
    let cache_config = BenchmarkCacheConfig {
        eviction_policy: args.eviction_policy.into(),
        cache_capacity_keys: args.cache_capacity_keys,
        cache_memory_bytes: args.cache_memory_bytes,
        read_mode: args.read_mode,
    };

    for id in &backend_ids {
        println!(
            "progress: backend-start id={} elapsed={:.1}s",
            id,
            command_started.elapsed().as_secs_f64()
        );
        io::stdout().flush()?;
        match make(
            id,
            args.vcpu_budget,
            args.clients,
            args.addr.as_deref(),
            args.key_count,
            cache_config,
            args.scnp_shards,
        ) {
            Ok(backend) => match run_one(&args, backend.as_ref(), workload.clone(), mix) {
                Ok(result) => {
                    print_row(&result);
                    csv.write_row(&result.to_csv_row(&args, mix))?;
                    println!(
                        "progress: backend-done id={} elapsed={:.1}s",
                        id,
                        command_started.elapsed().as_secs_f64()
                    );
                    io::stdout().flush()?;
                }
                Err(error) => {
                    eprintln!(
                        "skipping {id}: {error} elapsed={:.1}s",
                        command_started.elapsed().as_secs_f64()
                    );
                }
            },
            Err(e) => {
                eprintln!(
                    "skipping {id}: {e} elapsed={:.1}s",
                    command_started.elapsed().as_secs_f64()
                );
            }
        }
    }

    Ok(())
}

struct RunResult {
    backend_id: String,
    ops_total: u64,
    duration: Duration,
    bytes_moved: u64,
    vcpu_consumed: f64,
    p50: u64,
    p99: u64,
    p999: u64,
    read_ops: u64,
    read_p99: u64,
    read_p999: u64,
    write_ops: u64,
    write_p99: u64,
    write_p999: u64,
    errors: u64,
}

struct WorkerResult {
    all: LatencyHistogram,
    reads: LatencyHistogram,
    writes: LatencyHistogram,
}

impl RunResult {
    fn ops_per_sec(&self) -> f64 {
        self.ops_total as f64 / self.duration.as_secs_f64()
    }
    fn gb_per_sec(&self) -> f64 {
        (self.bytes_moved as f64 / 1e9) / self.duration.as_secs_f64()
    }
    fn to_csv_row(&self, args: &Args, mix: Mix) -> Vec<String> {
        vec![
            self.backend_id.clone(),
            build_variant_label(&self.backend_id).to_string(),
            ttl_mode_label(args.ttl_ms).to_string(),
            routing_mode_label(&self.backend_id).to_string(),
            args.eviction_policy.label().to_string(),
            args.cache_capacity_keys
                .map_or_else(String::new, |value| value.to_string()),
            args.cache_memory_bytes
                .map_or_else(String::new, |value| value.to_string()),
            args.read_mode.label().to_string(),
            args.value_size.to_string(),
            mix.label(),
            args.vcpu_budget.to_string(),
            args.clients.to_string(),
            args.pipeline_depth.to_string(),
            format!("{:.3}", self.duration.as_secs_f64()),
            self.ops_total.to_string(),
            format!("{:.0}", self.ops_per_sec()),
            format!("{:.3}", self.gb_per_sec()),
            format!("{:.3}", self.vcpu_consumed),
            self.p50.to_string(),
            self.p99.to_string(),
            self.p999.to_string(),
            self.read_ops.to_string(),
            self.read_p99.to_string(),
            self.read_p999.to_string(),
            self.write_ops.to_string(),
            self.write_p99.to_string(),
            self.write_p999.to_string(),
            self.errors.to_string(),
            args.latency_sample_rate.to_string(),
            args.ttl_ms
                .map_or_else(String::new, |ttl_ms| ttl_ms.to_string()),
            args.key_pattern.clone(),
            args.key_distribution.clone(),
        ]
    }
}

impl BenchEvictionPolicy {
    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lru => "lru",
            #[cfg(feature = "prefix-eviction")]
            Self::Prefix => "prefix",
        }
    }
}

fn ttl_mode_label(ttl_ms: Option<u64>) -> &'static str {
    match ttl_ms {
        Some(_) => "active",
        None => "none",
    }
}

fn build_variant_label(backend_id: &str) -> &'static str {
    if backend_id.starts_with("fc-") {
        if cfg!(feature = "unsafe") {
            "unsafe"
        } else {
            "safe"
        }
    } else {
        "competitor"
    }
}

fn routing_mode_label(backend_id: &str) -> &'static str {
    match backend_id {
        id if id.starts_with("fc-embed") => "embedded-direct",
        id if id.starts_with("fc-shared") => "shared-handle",
        "fc-server-scnp-direct" => "tcp-shard-direct",
        "fc-server-scnp" | "fc-server-resp" => "tcp-fanout",
        "redis" | "valkey" | "dragonfly" => "tcp-baseline",
        _ => "competitor-shared",
    }
}

fn run_one(
    args: &Args,
    backend: &dyn Backend,
    workload: Arc<Workload>,
    mix: Mix,
) -> Result<RunResult, BoxError> {
    let pipeline_depth = args.pipeline_depth.max(1);
    if pipeline_depth > 1 {
        if !backend.supports_pipelining() {
            return Err(format!("{} does not support --pipeline-depth > 1", backend.id()).into());
        }
        if args.ttl_ms.is_some() {
            return Err("--pipeline-depth > 1 does not support TTL workloads yet".into());
        }
    }

    if let Some(ttl_ms) = args.ttl_ms {
        backend.warmup_ttl(workload.keys(), workload.value(), ttl_ms)?;
    } else {
        backend.warmup(workload.keys(), workload.value())?;
    }

    let worker_key_indices = (0..args.clients)
        .map(|worker_idx| backend.worker_key_indices(workload.keys(), worker_idx, args.clients))
        .collect::<Result<Vec<_>, _>>()?;
    for (worker_idx, indices) in worker_key_indices.iter().enumerate() {
        let worker_key_count = indices.as_ref().map_or(args.key_count, Vec::len);
        if worker_key_count == 0 {
            return Err(format!(
                "{} worker {worker_idx} has no local keys; increase --key-count or reduce --clients",
                backend.id()
            )
            .into());
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let warmup_done = Arc::new(AtomicBool::new(false));
    let ops_counter = Arc::new(AtomicU64::new(0));
    let read_counter = Arc::new(AtomicU64::new(0));
    let write_counter = Arc::new(AtomicU64::new(0));
    let bytes_counter = Arc::new(AtomicU64::new(0));
    let err_counter = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(args.clients);
    for (worker_idx, key_indices) in worker_key_indices.into_iter().enumerate() {
        let mut worker = backend.new_worker_for(worker_idx, args.clients)?;
        let indexed_keys = key_indices.is_some() && worker.supports_indexed_keys();
        let stop = Arc::clone(&stop);
        let warmup_done = Arc::clone(&warmup_done);
        let ops = Arc::clone(&ops_counter);
        let read_ops = Arc::clone(&read_counter);
        let write_ops = Arc::clone(&write_counter);
        let bytes = Arc::clone(&bytes_counter);
        let errs = Arc::clone(&err_counter);
        let wl = Arc::clone(&workload);
        let value_size = args.value_size;
        let key_count = key_indices.as_ref().map_or(args.key_count, Vec::len);
        let seed = (0xDEADBEEF_u64).wrapping_add(worker_idx as u64);
        let latency_sample_rate = args.latency_sample_rate;
        let ttl_ms = args.ttl_ms;
        let key_distribution = wl.key_distribution();

        let h = thread::spawn(move || {
            let mut hist = LatencyHistogram::new();
            let mut read_hist = LatencyHistogram::new();
            let mut write_hist = LatencyHistogram::new();
            let latency_clock = FastClock::new();
            let mut scratch: Vec<u8> = Vec::with_capacity(value_size);
            let mut stream = OpStream::new(seed, key_count, mix, key_distribution);
            let keys = wl.keys();
            let value = wl.value();
            let mut local_ops = 0u64;
            let mut local_reads = 0u64;
            let mut local_writes = 0u64;
            let mut local_bytes = 0u64;
            let mut local_errs = 0u64;
            let mut pipeline_ops = Vec::with_capacity(pipeline_depth);

            if pipeline_depth == 1 {
                while !stop.load(Ordering::Relaxed) {
                    let (op, local_idx) = stream.next_op();
                    let measured = warmup_done.load(Ordering::Relaxed);
                    let sample_latency = measured
                        && latency_sample_rate != 0
                        && local_ops.is_multiple_of(latency_sample_rate);
                    let start = sample_latency.then(|| latency_clock.now());
                    let res = if indexed_keys {
                        match op {
                            Op::Get => worker.get_index(local_idx, &mut scratch).map(|_| ()),
                            Op::Set => match ttl_ms {
                                Some(ttl_ms) => worker.set_index_ttl(local_idx, value, ttl_ms),
                                None => worker.set_index(local_idx, value),
                            },
                        }
                    } else {
                        let idx = key_indices
                            .as_ref()
                            .map_or(local_idx, |indices| indices[local_idx]);
                        let key = keys[idx].as_slice();
                        match op {
                            Op::Get => worker.get(key, &mut scratch).map(|_| ()),
                            Op::Set => match ttl_ms {
                                Some(ttl_ms) => worker.set_ttl(key, value, ttl_ms),
                                None => worker.set(key, value),
                            },
                        }
                    };
                    match res {
                        Ok(()) => {
                            if measured {
                                if let Some(start) = start {
                                    let elapsed = latency_clock.elapsed_ns(start);
                                    hist.record(elapsed);
                                    match op {
                                        Op::Get => read_hist.record(elapsed),
                                        Op::Set => write_hist.record(elapsed),
                                    }
                                }
                                local_ops += 1;
                                match op {
                                    Op::Get => local_reads += 1,
                                    Op::Set => local_writes += 1,
                                }
                                local_bytes += value_size as u64;
                            }
                        }
                        Err(_) => {
                            local_errs += 1;
                        }
                    }
                }
            } else {
                while !stop.load(Ordering::Relaxed) {
                    pipeline_ops.clear();
                    let measured = warmup_done.load(Ordering::Relaxed);
                    let sample_latency = measured
                        && latency_sample_rate != 0
                        && local_ops.is_multiple_of(latency_sample_rate);
                    let start = sample_latency.then(|| latency_clock.now());

                    for _ in 0..pipeline_depth {
                        let (op, local_idx) = stream.next_op();
                        let res = if indexed_keys {
                            match op {
                                Op::Get => worker.begin_pipeline_get_index(local_idx),
                                Op::Set => worker.begin_pipeline_set_index(local_idx, value),
                            }
                        } else {
                            let idx = key_indices
                                .as_ref()
                                .map_or(local_idx, |indices| indices[local_idx]);
                            let key = keys[idx].as_slice();
                            match op {
                                Op::Get => worker.begin_pipeline_get(key),
                                Op::Set => worker.begin_pipeline_set(key, value),
                            }
                        };
                        match res {
                            Ok(()) => pipeline_ops.push(op),
                            Err(_) => {
                                local_errs += 1;
                                break;
                            }
                        }
                    }

                    if pipeline_ops.is_empty() {
                        continue;
                    }

                    if worker.flush_pipeline().is_err() {
                        local_errs += pipeline_ops.len() as u64;
                        continue;
                    }

                    let mut batch_reads = 0u64;
                    let mut batch_writes = 0u64;
                    for op in pipeline_ops.iter().copied() {
                        let res = match op {
                            Op::Get => worker.finish_pipeline_get(&mut scratch).map(|_| ()),
                            Op::Set => worker.finish_pipeline_set(),
                        };
                        match res {
                            Ok(()) => match op {
                                Op::Get => batch_reads += 1,
                                Op::Set => batch_writes += 1,
                            },
                            Err(_) => local_errs += 1,
                        }
                    }

                    if measured {
                        let batch_ops = batch_reads + batch_writes;
                        if batch_ops == 0 {
                            continue;
                        }
                        if let Some(start) = start {
                            let elapsed = latency_clock.elapsed_ns(start) / batch_ops;
                            for _ in 0..batch_ops {
                                hist.record(elapsed);
                            }
                            for _ in 0..batch_reads {
                                read_hist.record(elapsed);
                            }
                            for _ in 0..batch_writes {
                                write_hist.record(elapsed);
                            }
                        }
                        local_ops += batch_ops;
                        local_reads += batch_reads;
                        local_writes += batch_writes;
                        local_bytes += batch_ops * value_size as u64;
                    }
                }
            }
            ops.fetch_add(local_ops, Ordering::Relaxed);
            read_ops.fetch_add(local_reads, Ordering::Relaxed);
            write_ops.fetch_add(local_writes, Ordering::Relaxed);
            bytes.fetch_add(local_bytes, Ordering::Relaxed);
            errs.fetch_add(local_errs, Ordering::Relaxed);
            WorkerResult {
                all: hist,
                reads: read_hist,
                writes: write_hist,
            }
        });
        handles.push(h);
    }

    thread::sleep(Duration::from_secs(args.warmup));
    let pre_self = process_cpu_time();
    let pre_ext = args.server_pid.and_then(external_cpu_time);
    let measure_start = Instant::now();
    warmup_done.store(true, Ordering::Relaxed);

    thread::sleep(Duration::from_secs(args.duration));
    stop.store(true, Ordering::Relaxed);

    let mut combined = LatencyHistogram::new();
    let mut read_combined = LatencyHistogram::new();
    let mut write_combined = LatencyHistogram::new();
    for h in handles {
        let hist = h.join().expect("worker panic");
        combined.merge(&hist.all);
        read_combined.merge(&hist.reads);
        write_combined.merge(&hist.writes);
    }

    let wall = measure_start.elapsed();
    let cpu_used = match (pre_ext, backend.class()) {
        (Some(pre), BackendClass::Networked) => {
            let now = external_cpu_time(args.server_pid.unwrap()).unwrap_or(pre);
            vcpu(now.saturating_sub(pre), wall)
        }
        _ => vcpu(process_cpu_time() - pre_self, wall),
    };

    Ok(RunResult {
        backend_id: backend.id().to_string(),
        ops_total: ops_counter.load(Ordering::Relaxed),
        duration: wall,
        bytes_moved: bytes_counter.load(Ordering::Relaxed),
        vcpu_consumed: cpu_used,
        p50: combined.p50_ns(),
        p99: combined.p99_ns(),
        p999: combined.p999_ns(),
        read_ops: read_counter.load(Ordering::Relaxed),
        read_p99: read_combined.p99_ns(),
        read_p999: read_combined.p999_ns(),
        write_ops: write_counter.load(Ordering::Relaxed),
        write_p99: write_combined.p99_ns(),
        write_p999: write_combined.p999_ns(),
        errors: err_counter.load(Ordering::Relaxed),
    })
}

fn print_header() {
    println!(
        "| {:<20} | {:>14} | {:>12} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>8} |",
        "backend",
        "ops/sec",
        "logical GB/s",
        "vCPU",
        "p50",
        "p99",
        "p999",
        "r-p999",
        "w-p999",
        "errors"
    );
    println!(
        "| {:<20} | {:>14} | {:>12} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>8} |",
        "-".repeat(20),
        "-".repeat(14),
        "-".repeat(12),
        "-".repeat(10),
        "-".repeat(10),
        "-".repeat(10),
        "-".repeat(10),
        "-".repeat(10),
        "-".repeat(10),
        "-".repeat(8),
    );
}

fn print_row(r: &RunResult) {
    println!(
        "| {:<20} | {:>14.0} | {:>12.3} | {:>10.3} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10} | {:>8} |",
        r.backend_id,
        r.ops_per_sec(),
        r.gb_per_sec(),
        r.vcpu_consumed,
        format_ns(r.p50),
        format_ns(r.p99),
        format_ns(r.p999),
        format_ns(r.read_p999),
        format_ns(r.write_p999),
        r.errors
    );
}
