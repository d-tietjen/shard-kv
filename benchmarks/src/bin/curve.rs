//! Open-loop CPU-curve driver.
//!
//! For each requested backend, at each requested target rate, pace
//! requests with per-worker local pacing and measure:
//!
//!   achieved ops/sec, CPU consumed (vCPU), p50/p99/p99.9
//!
//! Output is one row per (backend, target_rate) and a per-backend
//! markdown summary. A cell where achieved/target < 0.95 is flagged.

use std::error::Error;

type BoxError = Box<dyn Error + Send + Sync>;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use fast_cache_benchmarks::backend::{Backend, BackendClass, Op};
use fast_cache_benchmarks::backends::{BACKEND_IDS, BenchmarkCacheConfig, make};
use fast_cache_benchmarks::clock::FastClock;
use fast_cache_benchmarks::cpu::{external_cpu_time, process_cpu_time, vcpu};
use fast_cache_benchmarks::csv::CsvWriter;
use fast_cache_benchmarks::histogram::{LatencyHistogram, format_ns};
use fast_cache_benchmarks::workload::{
    KeyDistribution, KeyPattern, Mix, OpStream, Workload, WorkloadSpec,
};

#[derive(Parser, Debug, Clone)]
#[command(about = "Open-loop CPU-vs-load curve benchmark")]
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

    /// Server shard count (advisory).
    #[arg(long, default_value_t = 4)]
    vcpu_budget: usize,

    /// Concurrent submitter threads. Driver size, not workload axis.
    #[arg(long, default_value_t = 16)]
    submitters: usize,

    /// Requests to write before flushing and reading responses.
    ///
    /// A depth of 1 is strict request/response. Values greater than 1 require
    /// a network backend with explicit pipelining support.
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

    /// Comma-separated target rates. Supports suffixes K, M (e.g., 500K, 2M).
    #[arg(long, default_value = "100K,250K,500K,1M,2M,4M,8M")]
    target_rates: String,

    /// Per-rate measurement duration in seconds (after warmup).
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Per-rate warmup duration in seconds.
    #[arg(long, default_value_t = 2)]
    warmup: u64,

    /// Record one latency sample every N successful measured operations.
    /// Use 0 to disable latency timing when measuring driver overhead.
    #[arg(long, default_value_t = 1)]
    latency_sample_rate: u64,

    /// Optional CSV path to append results.
    #[arg(long)]
    csv: Option<String>,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();

    let backend_ids: Vec<String> = match &args.backends {
        Some(s) => s.split(',').map(|s| s.trim().to_string()).collect(),
        None => BACKEND_IDS.iter().map(|s| s.to_string()).collect(),
    };

    let target_rates = parse_rates(&args.target_rates)?;
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
        "curve: value_size={}B mix={} key_pattern={} key_distribution={} vcpu_budget={} submitters={} pipeline_depth={} keys={} duration={}s/cell",
        args.value_size,
        mix.label(),
        key_pattern.label(),
        key_distribution.label(),
        args.vcpu_budget,
        args.submitters,
        args.pipeline_depth,
        args.key_count,
        args.duration
    );

    let csv_header = vec![
        "backend",
        "value_size",
        "mix",
        "vcpu_budget",
        "submitters",
        "pipeline_depth",
        "target_rate",
        "achieved_rate",
        "achieved_pct",
        "vcpu_consumed",
        "p50_ns",
        "p99_ns",
        "p999_ns",
        "errors",
        "duration_s",
        "latency_sample_rate",
        "key_pattern",
        "key_distribution",
    ];
    let mut csv = CsvWriter::new(args.csv.as_ref(), csv_header);

    for id in &backend_ids {
        let backend = match make(
            id,
            args.vcpu_budget,
            args.submitters,
            args.addr.as_deref(),
            args.key_count,
            BenchmarkCacheConfig::default(),
        ) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("skipping {id}: {e}");
                continue;
            }
        };
        backend.warmup(workload.keys(), workload.value())?;

        println!();
        println!("## {}", backend.id());
        print_header();

        for &rate in &target_rates {
            let result = run_one(&args, backend.as_ref(), workload.clone(), mix, rate)?;
            print_row(rate, &result);
            csv.write_row(&result.to_csv_row(backend.id(), &args, mix, rate))?;
            if result.achieved_pct() < 0.95 {
                println!(
                    "  saturated at {} ops/sec",
                    fmt_rate(result.achieved_rate())
                );
                break;
            }
        }
    }

    Ok(())
}

fn parse_rates(s: &str) -> Result<Vec<u64>, BoxError> {
    s.split(',')
        .map(|tok| {
            let tok = tok.trim();
            let (digits, mul) = if let Some(d) = tok.strip_suffix(['K', 'k']) {
                (d, 1_000u64)
            } else if let Some(d) = tok.strip_suffix(['M', 'm']) {
                (d, 1_000_000u64)
            } else {
                (tok, 1u64)
            };
            let n: f64 = digits.parse().map_err(|e| format!("rate `{tok}`: {e}"))?;
            Ok((n * mul as f64) as u64)
        })
        .collect()
}

fn fmt_rate(r: f64) -> String {
    if r >= 1_000_000.0 {
        format!("{:.2}M", r / 1_000_000.0)
    } else if r >= 1_000.0 {
        format!("{:.1}K", r / 1_000.0)
    } else {
        format!("{r:.0}")
    }
}

fn split_rate_for_worker(target_rate: u64, worker_idx: usize, worker_count: usize) -> u64 {
    if worker_count == 0 {
        return 0;
    }
    let base = target_rate / worker_count as u64;
    let remainder = target_rate % worker_count as u64;
    match (worker_idx as u64) < remainder {
        true => base + 1,
        false => base,
    }
}

fn batch_interval(worker_rate: u64, batch_size: usize) -> Duration {
    match worker_rate {
        0 => Duration::MAX,
        rate => Duration::from_secs_f64(batch_size as f64 / rate as f64),
    }
}

fn wait_for_next_batch(stop: &AtomicBool, next_batch: &mut Instant, interval: Duration) {
    loop {
        let now = Instant::now();
        if now >= *next_batch || stop.load(Ordering::Relaxed) {
            break;
        }
        let sleep_for = *next_batch - now;
        if sleep_for > Duration::from_micros(100) {
            thread::sleep(sleep_for / 2);
        } else {
            std::hint::spin_loop();
        }
    }

    *next_batch += interval;
    let now = Instant::now();
    if now.saturating_duration_since(*next_batch) > interval.mul_f64(128.0) {
        *next_batch = now;
    }
}

struct RunResult {
    target_rate: u64,
    ops_total: u64,
    duration: Duration,
    vcpu_consumed: f64,
    p50: u64,
    p99: u64,
    p999: u64,
    errors: u64,
}

impl RunResult {
    fn achieved_rate(&self) -> f64 {
        self.ops_total as f64 / self.duration.as_secs_f64()
    }
    fn achieved_pct(&self) -> f64 {
        if self.target_rate == 0 {
            return 1.0;
        }
        self.achieved_rate() / self.target_rate as f64
    }
    fn to_csv_row(&self, backend_id: &str, args: &Args, mix: Mix, target_rate: u64) -> Vec<String> {
        vec![
            backend_id.to_string(),
            args.value_size.to_string(),
            mix.label(),
            args.vcpu_budget.to_string(),
            args.submitters.to_string(),
            args.pipeline_depth.to_string(),
            target_rate.to_string(),
            format!("{:.0}", self.achieved_rate()),
            format!("{:.3}", self.achieved_pct()),
            format!("{:.3}", self.vcpu_consumed),
            self.p50.to_string(),
            self.p99.to_string(),
            self.p999.to_string(),
            self.errors.to_string(),
            format!("{:.3}", self.duration.as_secs_f64()),
            args.latency_sample_rate.to_string(),
            args.key_pattern.clone(),
            args.key_distribution.clone(),
        ]
    }
}

fn run_one(
    args: &Args,
    backend: &dyn Backend,
    workload: Arc<Workload>,
    mix: Mix,
    target_rate: u64,
) -> Result<RunResult, BoxError> {
    let pipeline_depth = args.pipeline_depth.max(1);
    if pipeline_depth > 1 && !backend.supports_pipelining() {
        return Err(format!("{} does not support --pipeline-depth > 1", backend.id()).into());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let warmup_done = Arc::new(AtomicBool::new(false));
    let ops_counter = Arc::new(AtomicU64::new(0));
    let err_counter = Arc::new(AtomicU64::new(0));
    let worker_key_indices = (0..args.submitters)
        .map(|worker_idx| backend.worker_key_indices(workload.keys(), worker_idx, args.submitters))
        .collect::<Result<Vec<_>, _>>()?;
    for (worker_idx, indices) in worker_key_indices.iter().enumerate() {
        let worker_key_count = indices.as_ref().map_or(args.key_count, Vec::len);
        if worker_key_count == 0 {
            return Err(format!(
                "{} worker {worker_idx} has no local keys; increase --key-count or reduce --submitters",
                backend.id()
            )
            .into());
        }
    }

    let mut handles = Vec::with_capacity(args.submitters);
    for (worker_idx, key_indices) in worker_key_indices.into_iter().enumerate() {
        let mut worker = backend.new_worker_for(worker_idx, args.submitters)?;
        let indexed_keys = key_indices.is_some() && worker.supports_indexed_keys();
        let stop = Arc::clone(&stop);
        let warmup_done = Arc::clone(&warmup_done);
        let ops = Arc::clone(&ops_counter);
        let errs = Arc::clone(&err_counter);
        let wl = Arc::clone(&workload);
        let value_size = args.value_size;
        let key_count = key_indices.as_ref().map_or(args.key_count, Vec::len);
        let seed = (0xC0FFEE_u64).wrapping_add(worker_idx as u64);
        let latency_sample_rate = args.latency_sample_rate;
        let key_distribution = wl.key_distribution();
        let worker_rate = split_rate_for_worker(target_rate, worker_idx, args.submitters);
        let batch_size = pipeline_depth;

        let h = thread::spawn(move || {
            let mut hist = LatencyHistogram::new();
            let latency_clock = FastClock::new();
            let mut scratch: Vec<u8> = Vec::with_capacity(value_size);
            let mut stream = OpStream::new(seed, key_count, mix, key_distribution);
            let keys = wl.keys();
            let value = wl.value();
            let mut local_ops = 0u64;
            let mut local_errs = 0u64;
            let mut pipeline_ops = Vec::with_capacity(pipeline_depth);
            let mut next_batch = Instant::now();
            let batch_interval = batch_interval(worker_rate, batch_size);

            while !stop.load(Ordering::Relaxed) && worker_rate != 0 {
                wait_for_next_batch(&stop, &mut next_batch, batch_interval);
                if stop.load(Ordering::Relaxed) {
                    break;
                }

                if pipeline_depth == 1 {
                    let (op, local_idx) = stream.next_op();
                    let measured = warmup_done.load(Ordering::Relaxed);
                    let sample_latency = measured
                        && latency_sample_rate != 0
                        && local_ops.is_multiple_of(latency_sample_rate);
                    let start = sample_latency.then(|| latency_clock.now());
                    let res = if indexed_keys {
                        match op {
                            Op::Get => worker.get_index(local_idx, &mut scratch).map(|_| ()),
                            Op::Set => worker.set_index(local_idx, value),
                        }
                    } else {
                        let idx = key_indices
                            .as_ref()
                            .map_or(local_idx, |indices| indices[local_idx]);
                        let key = keys[idx].as_slice();
                        match op {
                            Op::Get => worker.get(key, &mut scratch).map(|_| ()),
                            Op::Set => worker.set(key, value),
                        }
                    };
                    match res {
                        Ok(()) => {
                            if measured {
                                if let Some(start) = start {
                                    hist.record(latency_clock.elapsed_ns(start));
                                }
                                local_ops += 1;
                            }
                        }
                        Err(_) => {
                            local_errs += 1;
                        }
                    }
                } else {
                    pipeline_ops.clear();
                    let measured = warmup_done.load(Ordering::Relaxed);
                    let sample_latency = measured
                        && latency_sample_rate != 0
                        && local_ops.is_multiple_of(latency_sample_rate);
                    let start = sample_latency.then(|| latency_clock.now());

                    for _ in 0..pipeline_depth {
                        let (op, local_idx) = stream.next_op();
                        let idx = key_indices
                            .as_ref()
                            .map_or(local_idx, |indices| indices[local_idx]);
                        let key = keys[idx].as_slice();
                        let res = match op {
                            Op::Get => worker.begin_pipeline_get(key),
                            Op::Set => worker.begin_pipeline_set(key, value),
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
                        std::hint::spin_loop();
                        continue;
                    }

                    if worker.flush_pipeline().is_err() {
                        local_errs += pipeline_ops.len() as u64;
                        continue;
                    }

                    let mut batch_ops = 0u64;
                    for op in pipeline_ops.iter().copied() {
                        let res = match op {
                            Op::Get => worker.finish_pipeline_get(&mut scratch).map(|_| ()),
                            Op::Set => worker.finish_pipeline_set(),
                        };
                        match res {
                            Ok(()) => batch_ops += 1,
                            Err(_) => local_errs += 1,
                        }
                    }

                    if measured && batch_ops > 0 {
                        if let Some(start) = start {
                            let elapsed = latency_clock.elapsed_ns(start) / batch_ops;
                            for _ in 0..batch_ops {
                                hist.record(elapsed);
                            }
                        }
                        local_ops += batch_ops;
                    }
                }
            }
            ops.fetch_add(local_ops, Ordering::Relaxed);
            errs.fetch_add(local_errs, Ordering::Relaxed);
            hist
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
    for h in handles {
        let hist = h.join().expect("worker panic");
        combined.merge(&hist);
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
        target_rate,
        ops_total: ops_counter.load(Ordering::Relaxed),
        duration: wall,
        vcpu_consumed: cpu_used,
        p50: combined.p50_ns(),
        p99: combined.p99_ns(),
        p999: combined.p999_ns(),
        errors: err_counter.load(Ordering::Relaxed),
    })
}

fn print_header() {
    println!(
        "| {:>12} | {:>12} | {:>8} | {:>10} | {:>10} | {:>10} | {:>10} | {:>8} |",
        "target", "achieved", "pct", "vCPU", "p50", "p99", "p999", "errors"
    );
    println!(
        "| {:>12} | {:>12} | {:>8} | {:>10} | {:>10} | {:>10} | {:>10} | {:>8} |",
        "-".repeat(12),
        "-".repeat(12),
        "-".repeat(8),
        "-".repeat(10),
        "-".repeat(10),
        "-".repeat(10),
        "-".repeat(10),
        "-".repeat(8),
    );
}

fn print_row(target_rate: u64, r: &RunResult) {
    println!(
        "| {:>12} | {:>12} | {:>7.1}% | {:>10.3} | {:>10} | {:>10} | {:>10} | {:>8} |",
        fmt_rate(target_rate as f64),
        fmt_rate(r.achieved_rate()),
        r.achieved_pct() * 100.0,
        r.vcpu_consumed,
        format_ns(r.p50),
        format_ns(r.p99),
        format_ns(r.p999),
        r.errors
    );
}
