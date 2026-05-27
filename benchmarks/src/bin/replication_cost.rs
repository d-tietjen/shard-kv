//! Native replication cost benchmark.
//!
//! This intentionally compares the same embedded write workload with
//! replication disabled, immediate one-record batches, and normal batching.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use crossbeam_channel::RecvTimeoutError;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use shardcache_benchmarks::backend::Op;
use shardcache_benchmarks::clock::FastClock;
use shardcache_benchmarks::cpu::{process_cpu_time, vcpu};
use shardcache_benchmarks::histogram::{LatencyHistogram, format_ns};
use shardcache_benchmarks::workload::{
    KeyDistribution, KeyPattern, Mix, OpStream, Workload, WorkloadSpec,
};
use shardmap::config::{ReplicationCompression, ReplicationConfig, ReplicationSendPolicy};
use shardmap::replication::{ReplicatedEmbeddedStore, ReplicationReplica};
use shardmap::storage::EmbeddedStore;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Measure native replication write-path cost")]
struct Args {
    #[arg(long, default_value = "64,512,4096,16384")]
    value_sizes: String,

    #[arg(long, default_value = "set,80-20")]
    mixes: String,

    #[arg(long, default_value = "baseline,immediate-none,batch-none")]
    modes: String,

    #[arg(long, default_value_t = 16)]
    shards: usize,

    #[arg(long, default_value_t = 16)]
    clients: usize,

    #[arg(long, default_value_t = 100_000)]
    key_count: usize,

    #[arg(long, default_value_t = 5)]
    duration: u64,

    #[arg(long, default_value_t = 2)]
    warmup: u64,

    #[arg(long, default_value_t = 512)]
    batch_max_records: usize,

    #[arg(long, default_value_t = 1024 * 1024)]
    batch_max_bytes: usize,

    #[arg(long, default_value_t = 750)]
    batch_max_delay_us: u64,

    #[arg(long, default_value_t = 16_384)]
    queue_capacity: usize,

    #[arg(long, default_value_t = 65_536)]
    subscriber_channel_capacity: usize,

    #[arg(long, default_value = "local")]
    replica_mode: String,

    #[arg(long, default_value = "semi-random")]
    value_pattern: String,

    #[arg(long, default_value_t = 4_096)]
    value_pool_count: usize,

    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    value_pool_max_bytes: usize,

    /// Record one latency sample every N measured operations. Use 0 to disable.
    #[arg(long, default_value_t = 10_000)]
    latency_sample_rate: u64,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let value_sizes = parse_usize_list(&args.value_sizes)?;
    let mixes = args
        .mixes
        .split(',')
        .map(|mix| Mix::parse(mix.trim()).map_err(|error| -> BoxError { error.into() }))
        .collect::<Result<Vec<_>, _>>()?;
    let modes = args
        .modes
        .split(',')
        .map(Mode::parse)
        .collect::<Result<Vec<_>, _>>()?;
    let value_pattern = ValuePattern::parse(&args.value_pattern)?;
    let replica_mode = ReplicaMode::parse(&args.replica_mode)?;

    println!(
        "value_pattern={} value_pool_count={} value_pool_max_bytes={} replica_mode={}",
        value_pattern.label(),
        args.value_pool_count,
        args.value_pool_max_bytes,
        replica_mode.label(),
    );

    println!(
        "| mode | value | pool | mix | ops/s | vCPU | ns/op | p50 | p99 | p999 | emitted | batches | raw MiB/s | wire MiB/s | ratio | drops | backpressure | queue hi | replica apply/s |"
    );
    println!(
        "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );

    for value_size in value_sizes {
        let values = Arc::new(ValueCorpus::build(
            value_pattern,
            value_size,
            args.key_count,
            args.value_pool_count,
            args.value_pool_max_bytes,
        ));
        for mix in mixes.iter().copied() {
            let workload = Arc::new(Workload::build(&WorkloadSpec {
                key_count: args.key_count,
                value_size,
                mix,
                key_pattern: KeyPattern::Point,
                key_distribution: KeyDistribution::Uniform,
            }));
            for mode in &modes {
                let result = run_mode(
                    &args,
                    replica_mode,
                    *mode,
                    workload.clone(),
                    values.clone(),
                    value_size,
                    mix,
                )?;
                println!(
                    "| {} | {} | {} | {} | {:.2} | {:.3} | {:.1} | {} | {} | {} | {} | {} | {:.2} | {:.2} | {:.3} | {} | {} | {} | {:.2} |",
                    mode.label(),
                    value_size,
                    values.len(),
                    mix.label(),
                    result.ops_per_sec,
                    result.vcpu,
                    result.ns_per_op,
                    format_ns(result.p50_ns),
                    format_ns(result.p99_ns),
                    format_ns(result.p999_ns),
                    result.emitted,
                    result.batches,
                    result.raw_mib_per_sec,
                    result.wire_mib_per_sec,
                    result.compression_ratio,
                    result.drops,
                    result.backpressure_events,
                    result.queue_high_watermark,
                    result.replica_apply_per_sec,
                );
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ValuePattern {
    Repeat,
    SemiRandom,
}

impl ValuePattern {
    fn parse(value: &str) -> Result<Self, BoxError> {
        Ok(match value.trim() {
            "repeat" => Self::Repeat,
            "semi-random" | "semirandom" => Self::SemiRandom,
            other => {
                return Err(
                    format!("unknown value pattern `{other}`; use repeat or semi-random").into(),
                );
            }
        })
    }

    fn label(self) -> &'static str {
        match self {
            Self::Repeat => "repeat",
            Self::SemiRandom => "semi-random",
        }
    }
}

struct ValueCorpus {
    values: Vec<Vec<u8>>,
}

impl ValueCorpus {
    fn build(
        pattern: ValuePattern,
        value_size: usize,
        key_count: usize,
        requested_pool_count: usize,
        max_pool_bytes: usize,
    ) -> Self {
        let pool_count = match pattern {
            ValuePattern::Repeat => 1,
            ValuePattern::SemiRandom => {
                let byte_limited = max_pool_bytes
                    .checked_div(value_size)
                    .unwrap_or(requested_pool_count);
                requested_pool_count
                    .min(key_count)
                    .min(byte_limited.max(1))
                    .max(1)
            }
        };
        let mut values = Vec::with_capacity(pool_count);
        for idx in 0..pool_count {
            values.push(match pattern {
                ValuePattern::Repeat => repeat_value(value_size),
                ValuePattern::SemiRandom => semi_random_value(value_size, idx as u64),
            });
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

fn repeat_value(value_size: usize) -> Vec<u8> {
    let mut value = vec![0u8; value_size];
    for (idx, byte) in value.iter_mut().enumerate() {
        *byte = (idx & 0xff) as u8;
    }
    value
}

fn semi_random_value(value_size: usize, seed: u64) -> Vec<u8> {
    let mut rng = SmallRng::seed_from_u64(0xFCA5_2026 ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut value = vec![0u8; value_size];
    rng.fill_bytes(&mut value);

    // Leave a little structure so this represents application payloads better
    // than fully random bytes, while avoiding repeated all-value batches.
    for (block_idx, block) in value.chunks_mut(64).enumerate() {
        let marker = seed.wrapping_add(block_idx as u64).to_le_bytes();
        for (idx, byte) in marker.iter().copied().enumerate().take(block.len().min(8)) {
            block[idx] = byte;
        }
    }
    value
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Baseline,
    ImmediateNone,
    ImmediateZstd,
    BatchNone,
    BatchZstd,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, BoxError> {
        Ok(match value.trim() {
            "baseline" => Self::Baseline,
            "immediate-none" => Self::ImmediateNone,
            "immediate-zstd" => Self::ImmediateZstd,
            "batch-none" => Self::BatchNone,
            "batch-zstd" => Self::BatchZstd,
            other => return Err(format!("unknown replication benchmark mode: {other}").into()),
        })
    }

    fn label(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::ImmediateNone => "immediate-none",
            Self::ImmediateZstd => "immediate-zstd",
            Self::BatchNone => "batch-none",
            Self::BatchZstd => "batch-zstd",
        }
    }

    fn config(self, args: &Args) -> Option<ReplicationConfig> {
        let (send_policy, compression) = match self {
            Self::Baseline => return None,
            Self::ImmediateNone => (
                ReplicationSendPolicy::Immediate,
                ReplicationCompression::None,
            ),
            Self::ImmediateZstd => (
                ReplicationSendPolicy::Immediate,
                ReplicationCompression::Zstd,
            ),
            Self::BatchNone => (ReplicationSendPolicy::Batch, ReplicationCompression::None),
            Self::BatchZstd => (ReplicationSendPolicy::Batch, ReplicationCompression::Zstd),
        };
        Some(ReplicationConfig {
            enabled: true,
            send_policy,
            compression,
            batch_max_records: args.batch_max_records,
            batch_max_bytes: args.batch_max_bytes,
            batch_max_delay_us: args.batch_max_delay_us,
            queue_capacity: args.queue_capacity,
            subscriber_channel_capacity: args.subscriber_channel_capacity,
            ..ReplicationConfig::default()
        })
    }
}

struct RunResult {
    ops_per_sec: f64,
    vcpu: f64,
    ns_per_op: f64,
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    emitted: u64,
    batches: u64,
    raw_mib_per_sec: f64,
    wire_mib_per_sec: f64,
    compression_ratio: f64,
    drops: u64,
    backpressure_events: u64,
    queue_high_watermark: usize,
    replica_apply_per_sec: f64,
}

#[derive(Debug, Clone, Copy)]
enum ReplicaMode {
    Local,
    None,
}

impl ReplicaMode {
    fn parse(value: &str) -> Result<Self, BoxError> {
        Ok(match value.trim() {
            "local" => Self::Local,
            "none" => Self::None,
            other => return Err(format!("unknown replica mode: {other}").into()),
        })
    }

    fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::None => "none",
        }
    }
}

fn run_mode(
    args: &Args,
    replica_mode: ReplicaMode,
    mode: Mode,
    workload: Arc<Workload>,
    values: Arc<ValueCorpus>,
    value_size: usize,
    mix: Mix,
) -> Result<RunResult, BoxError> {
    match mode.config(args) {
        Some(config) => run_replicated(
            args,
            replica_mode,
            config,
            workload,
            values,
            value_size,
            mix,
        ),
        None => run_baseline(args, workload, values, value_size, mix),
    }
}

fn run_baseline(
    args: &Args,
    workload: Arc<Workload>,
    values: Arc<ValueCorpus>,
    _value_size: usize,
    mix: Mix,
) -> Result<RunResult, BoxError> {
    let store = Arc::new(EmbeddedStore::new(args.shards.next_power_of_two()));
    for (key_index, key) in workload
        .keys()
        .iter()
        .enumerate()
        .take(args.key_count.min(10_000))
    {
        store.set(key.clone(), values.value_for(key_index).to_vec(), None);
    }
    let ops = run_workers(
        args,
        workload,
        values,
        mix,
        {
            let store = Arc::clone(&store);
            move |key, scratch| {
                scratch.clear();
                if let Some(value) = store.get(key) {
                    scratch.extend_from_slice(&value);
                    true
                } else {
                    false
                }
            }
        },
        move |key, value| {
            store.set(key.to_vec(), value.to_vec(), None);
        },
    )?;
    Ok(RunResult {
        ops_per_sec: ops.ops_per_sec,
        vcpu: ops.vcpu,
        ns_per_op: ops.ns_per_op,
        p50_ns: ops.p50_ns,
        p99_ns: ops.p99_ns,
        p999_ns: ops.p999_ns,
        emitted: 0,
        batches: 0,
        raw_mib_per_sec: 0.0,
        wire_mib_per_sec: 0.0,
        compression_ratio: 1.0,
        drops: 0,
        backpressure_events: 0,
        queue_high_watermark: 0,
        replica_apply_per_sec: 0.0,
    })
}

fn run_replicated(
    args: &Args,
    replica_mode: ReplicaMode,
    config: ReplicationConfig,
    workload: Arc<Workload>,
    values: Arc<ValueCorpus>,
    _value_size: usize,
    mix: Mix,
) -> Result<RunResult, BoxError> {
    let store = Arc::new(ReplicatedEmbeddedStore::new(
        args.shards.next_power_of_two(),
        config,
    )?);
    for (key_index, key) in workload
        .keys()
        .iter()
        .enumerate()
        .take(args.key_count.min(10_000))
    {
        store.set(key.clone(), values.value_for(key_index).to_vec(), None);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let applied = Arc::new(AtomicU64::new(0));
    let replica_handle = match replica_mode {
        ReplicaMode::Local => Some(spawn_replica(
            Arc::clone(&store),
            args.subscriber_channel_capacity,
            Arc::clone(&stop),
            Arc::clone(&applied),
        )),
        ReplicaMode::None => None,
    };

    let ops = run_workers(
        args,
        workload,
        values,
        mix,
        {
            let store = Arc::clone(&store);
            move |key, scratch| {
                scratch.clear();
                if let Some(value) = store.get(key) {
                    scratch.extend_from_slice(&value);
                    true
                } else {
                    false
                }
            }
        },
        {
            let store = Arc::clone(&store);
            move |key, value| {
                store.set(key.to_vec(), value.to_vec(), None);
            }
        },
    )?;
    stop.store(true, Ordering::Relaxed);
    if let Some(replica_handle) = replica_handle {
        let _ = replica_handle.join();
    }

    let metrics = store.metrics_snapshot();
    let raw_mib_per_sec = metrics.sent_bytes_uncompressed as f64 / 1024.0 / 1024.0 / ops.duration;
    let wire_mib_per_sec = metrics.sent_bytes_compressed as f64 / 1024.0 / 1024.0 / ops.duration;
    Ok(RunResult {
        ops_per_sec: ops.ops_per_sec,
        vcpu: ops.vcpu,
        ns_per_op: ops.ns_per_op,
        p50_ns: ops.p50_ns,
        p99_ns: ops.p99_ns,
        p999_ns: ops.p999_ns,
        emitted: metrics.emitted_mutations,
        batches: metrics.sent_batches,
        raw_mib_per_sec,
        wire_mib_per_sec,
        compression_ratio: if metrics.sent_bytes_uncompressed == 0 {
            1.0
        } else {
            metrics.sent_bytes_compressed as f64 / metrics.sent_bytes_uncompressed as f64
        },
        drops: metrics.drops,
        backpressure_events: metrics.backpressure_events,
        queue_high_watermark: metrics.queue_high_watermark,
        replica_apply_per_sec: applied.load(Ordering::Relaxed) as f64 / ops.duration,
    })
}

fn spawn_replica(
    store: Arc<ReplicatedEmbeddedStore>,
    subscriber_channel_capacity: usize,
    stop: Arc<AtomicBool>,
    applied: Arc<AtomicU64>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut replica = ReplicationReplica::new(store.inner().shard_count());
        let mut rx = store.primary().subscribe(subscriber_channel_capacity);
        while !stop.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(10)) {
                Ok(frame) => {
                    let _ = replica.apply_frame(frame);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    let _ = store.catch_up_replica(&mut replica);
                    rx = store.primary().subscribe(subscriber_channel_capacity);
                }
            }
        }
        while let Ok(frame) = rx.try_recv() {
            let _ = replica.apply_frame(frame);
        }
        let _ = store.catch_up_replica(&mut replica);
        applied.store(
            replica.metrics_snapshot().replica_applied,
            Ordering::Relaxed,
        );
    })
}

struct OpsResult {
    ops_per_sec: f64,
    vcpu: f64,
    ns_per_op: f64,
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    duration: f64,
}

fn run_workers(
    args: &Args,
    workload: Arc<Workload>,
    values: Arc<ValueCorpus>,
    mix: Mix,
    get: impl Fn(&[u8], &mut Vec<u8>) -> bool + Send + Sync + 'static,
    set: impl Fn(&[u8], &[u8]) + Send + Sync + 'static,
) -> Result<OpsResult, BoxError> {
    let get = Arc::new(get);
    let set = Arc::new(set);
    let stop = Arc::new(AtomicBool::new(false));
    let measured = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for worker in 0..args.clients {
        let set = Arc::clone(&set);
        let get = Arc::clone(&get);
        let workload = Arc::clone(&workload);
        let values = Arc::clone(&values);
        let stop = Arc::clone(&stop);
        let measured = Arc::clone(&measured);
        let ops = Arc::clone(&ops);
        let latency_sample_rate = args.latency_sample_rate;
        handles.push(thread::spawn(move || {
            let latency_clock = FastClock::new();
            let mut histogram = LatencyHistogram::new();
            let mut stream = OpStream::new(
                worker as u64 + 1,
                workload.keys().len(),
                mix,
                workload.key_distribution(),
            );
            let mut scratch = Vec::new();
            let mut local_ops = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                let (op, key_index) = stream.next_op();
                let key = &workload.keys()[key_index];
                let measured_now = measured.load(Ordering::Relaxed);
                let sample_latency = measured_now
                    && latency_sample_rate != 0
                    && local_ops.is_multiple_of(latency_sample_rate);
                let latency_start = sample_latency.then(|| latency_clock.now());
                match op {
                    Op::Get => {
                        let _ = get(key, &mut scratch);
                    }
                    Op::Set => set(key, values.value_for(key_index)),
                }
                if measured_now {
                    if let Some(latency_start) = latency_start {
                        histogram.record(latency_clock.elapsed_ns(latency_start));
                    }
                    local_ops += 1;
                }
            }
            ops.fetch_add(local_ops, Ordering::Relaxed);
            histogram
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
    let mut histogram = LatencyHistogram::new();
    for handle in handles {
        let worker_histogram = handle
            .join()
            .map_err(|_| "replication bench worker panicked")?;
        histogram.merge(&worker_histogram);
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
        p50_ns: histogram.p50_ns(),
        p99_ns: histogram.p99_ns(),
        p999_ns: histogram.p999_ns(),
        duration: secs,
    })
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
