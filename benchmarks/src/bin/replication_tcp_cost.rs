//! Native FCRP TCP replication transport benchmark.
//!
//! This complements `replication_cost`, which measures the embedded
//! shard-local replication pipeline without TCP.

use std::error::Error;
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use fast_cache::config::{
    ReplicationCompression, ReplicationConfig, ReplicationRole, ReplicationSendPolicy,
};
use fast_cache::replication::{
    ReplicatedEmbeddedStore, ReplicationPrimaryServer, ReplicationReplicaClient, SnapshotProvider,
};
use fast_cache::storage::EmbeddedStore;
use fast_cache_benchmarks::backend::Op;
use fast_cache_benchmarks::cpu::{process_cpu_time, vcpu};
use fast_cache_benchmarks::workload::{
    KeyDistribution, KeyPattern, Mix, OpStream, Workload, WorkloadSpec,
};
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Measure native FCRP TCP replication transport cost")]
struct Args {
    #[arg(long, default_value = "64,4096")]
    value_sizes: String,

    #[arg(long, default_value = "set,80-20")]
    mixes: String,

    #[arg(long, default_value = "baseline,batch-none")]
    modes: String,

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

    #[arg(long, default_value_t = 64)]
    batch_max_records: usize,

    #[arg(long, default_value_t = 256 * 1024)]
    batch_max_bytes: usize,

    #[arg(long, default_value_t = 250)]
    batch_max_delay_us: u64,

    #[arg(long, default_value_t = 65_536)]
    queue_capacity: usize,

    #[arg(long, default_value_t = 65_536)]
    subscriber_channel_capacity: usize,

    #[arg(long, default_value_t = 4_096)]
    value_pool_count: usize,

    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    value_pool_max_bytes: usize,
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

    println!(
        "transport={} shards={} clients={} duration={} warmup={}",
        runtime_label(),
        args.shards,
        args.clients,
        args.duration,
        args.warmup
    );
    println!(
        "| mode | value | pool | mix | ops/s | vCPU | ns/op | emitted | batches | raw MiB/s | wire MiB/s | drops | backpressure | queue hi | replica apply/s |"
    );
    println!(
        "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );

    for value_size in value_sizes {
        let values = Arc::new(ValueCorpus::build(
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
                    *mode,
                    Arc::clone(&workload),
                    Arc::clone(&values),
                    mix,
                )?;
                println!(
                    "| {} | {} | {} | {} | {:.2} | {:.3} | {:.1} | {} | {} | {:.2} | {:.2} | {} | {} | {} | {:.2} |",
                    mode.label(),
                    value_size,
                    values.len(),
                    mix.label(),
                    result.ops_per_sec,
                    result.vcpu,
                    result.ns_per_op,
                    result.emitted,
                    result.batches,
                    result.raw_mib_per_sec,
                    result.wire_mib_per_sec,
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
enum Mode {
    Baseline,
    ImmediateNone,
    BatchNone,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, BoxError> {
        Ok(match value.trim() {
            "baseline" => Self::Baseline,
            "immediate-none" => Self::ImmediateNone,
            "batch-none" => Self::BatchNone,
            other => return Err(format!("unknown mode: {other}").into()),
        })
    }

    fn label(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::ImmediateNone => "immediate-none",
            Self::BatchNone => "batch-none",
        }
    }

    fn config(self, args: &Args, bind_addr: String) -> Option<ReplicationConfig> {
        let send_policy = match self {
            Self::Baseline => return None,
            Self::ImmediateNone => ReplicationSendPolicy::Immediate,
            Self::BatchNone => ReplicationSendPolicy::Batch,
        };
        Some(ReplicationConfig {
            enabled: true,
            role: ReplicationRole::Primary,
            bind_addr,
            compression: ReplicationCompression::None,
            send_policy,
            batch_max_records: args.batch_max_records,
            batch_max_bytes: args.batch_max_bytes,
            batch_max_delay_us: args.batch_max_delay_us,
            queue_capacity: args.queue_capacity,
            subscriber_channel_capacity: args.subscriber_channel_capacity,
            ..ReplicationConfig::default()
        })
    }
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
    emitted: u64,
    batches: u64,
    raw_mib_per_sec: f64,
    wire_mib_per_sec: f64,
    drops: u64,
    backpressure_events: u64,
    queue_high_watermark: usize,
    replica_apply_per_sec: f64,
}

fn run_mode(
    args: &Args,
    mode: Mode,
    workload: Arc<Workload>,
    values: Arc<ValueCorpus>,
    mix: Mix,
) -> Result<RunResult, BoxError> {
    match mode.config(args, free_local_addr()?) {
        Some(config) => run_replicated_tcp(args, config, workload, values, mix),
        None => run_baseline(args, workload, values, mix),
    }
}

fn run_baseline(
    args: &Args,
    workload: Arc<Workload>,
    values: Arc<ValueCorpus>,
    mix: Mix,
) -> Result<RunResult, BoxError> {
    let store = Arc::new(EmbeddedStore::new(args.shards.next_power_of_two()));
    prepopulate_store(&store, &workload, &values, args.key_count);
    let ops = run_workers(
        args,
        workload,
        values,
        mix,
        {
            let store = Arc::clone(&store);
            move |key, scratch| {
                scratch.clear();
                match store.get(key) {
                    Some(value) => {
                        scratch.extend_from_slice(&value);
                        true
                    }
                    None => false,
                }
            }
        },
        move |key, value| store.set(key.to_vec(), value.to_vec(), None),
    )?;
    Ok(RunResult {
        ops_per_sec: ops.ops_per_sec,
        vcpu: ops.vcpu,
        ns_per_op: ops.ns_per_op,
        emitted: 0,
        batches: 0,
        raw_mib_per_sec: 0.0,
        wire_mib_per_sec: 0.0,
        drops: 0,
        backpressure_events: 0,
        queue_high_watermark: 0,
        replica_apply_per_sec: 0.0,
    })
}

fn run_replicated_tcp(
    args: &Args,
    config: ReplicationConfig,
    workload: Arc<Workload>,
    values: Arc<ValueCorpus>,
    mix: Mix,
) -> Result<RunResult, BoxError> {
    let bind_addr = config.bind_addr.clone();
    let store = Arc::new(ReplicatedEmbeddedStore::new(
        args.shards.next_power_of_two(),
        config.clone(),
    )?);
    prepopulate_replicated_store(&store, &workload, &values, args.key_count);
    let server = ReplicationPrimaryServer::start(
        config,
        store.primary(),
        Arc::clone(&store) as Arc<dyn SnapshotProvider>,
    )?;
    let client = ReplicationReplicaClient::start(ReplicationConfig {
        enabled: true,
        role: ReplicationRole::Replica,
        bind_addr: String::new(),
        replica_of: Some(bind_addr),
        compression: ReplicationCompression::None,
        ..ReplicationConfig::default()
    })?;
    thread::sleep(Duration::from_millis(250));

    let ops = run_workers(
        args,
        workload,
        values,
        mix,
        {
            let store = Arc::clone(&store);
            move |key, scratch| {
                scratch.clear();
                match store.get(key) {
                    Some(value) => {
                        scratch.extend_from_slice(&value);
                        true
                    }
                    None => false,
                }
            }
        },
        {
            let store = Arc::clone(&store);
            move |key, value| store.set(key.to_vec(), value.to_vec(), None)
        },
    )?;
    thread::sleep(Duration::from_millis(250));

    let metrics = store.metrics_snapshot();
    let replica_applied = client.replica().lock().metrics_snapshot().replica_applied;
    client.shutdown().ok();
    server.shutdown().ok();

    Ok(RunResult {
        ops_per_sec: ops.ops_per_sec,
        vcpu: ops.vcpu,
        ns_per_op: ops.ns_per_op,
        emitted: metrics.emitted_mutations,
        batches: metrics.sent_batches,
        raw_mib_per_sec: metrics.sent_bytes_uncompressed as f64 / 1024.0 / 1024.0 / ops.duration,
        wire_mib_per_sec: metrics.sent_bytes_compressed as f64 / 1024.0 / 1024.0 / ops.duration,
        drops: metrics.drops,
        backpressure_events: metrics.backpressure_events,
        queue_high_watermark: metrics.queue_high_watermark,
        replica_apply_per_sec: replica_applied as f64 / ops.duration,
    })
}

struct OpsResult {
    ops_per_sec: f64,
    vcpu: f64,
    ns_per_op: f64,
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
    let mut handles = Vec::with_capacity(args.clients);
    for worker in 0..args.clients {
        let get = Arc::clone(&get);
        let set = Arc::clone(&set);
        let workload = Arc::clone(&workload);
        let values = Arc::clone(&values);
        let stop = Arc::clone(&stop);
        let measured = Arc::clone(&measured);
        let ops = Arc::clone(&ops);
        handles.push(thread::spawn(move || {
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
                match op {
                    Op::Get => {
                        let _ = get(key, &mut scratch);
                    }
                    Op::Set => set(key, values.value_for(key_index)),
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
        handle.join().map_err(|_| "bench worker panicked")?;
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

fn prepopulate_store(
    store: &EmbeddedStore,
    workload: &Workload,
    values: &ValueCorpus,
    key_count: usize,
) {
    for (key_index, key) in workload
        .keys()
        .iter()
        .enumerate()
        .take(key_count.min(10_000))
    {
        store.set(key.clone(), values.value_for(key_index).to_vec(), None);
    }
}

fn prepopulate_replicated_store(
    store: &ReplicatedEmbeddedStore,
    workload: &Workload,
    values: &ValueCorpus,
    key_count: usize,
) {
    for (key_index, key) in workload
        .keys()
        .iter()
        .enumerate()
        .take(key_count.min(10_000))
    {
        store.set(key.clone(), values.value_for(key_index).to_vec(), None);
    }
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

fn free_local_addr() -> Result<String, BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(addr.to_string())
}

fn runtime_label() -> &'static str {
    match std::env::var("FAST_CACHE_REPLICATION_USE_MONOIO").is_ok_and(|value| value != "0") {
        true => "monoio",
        false => "std",
    }
}
