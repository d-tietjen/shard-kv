use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use shardcache_benchmarks::histogram::LatencyHistogram;
use shardmap::config::{
    EvictionPolicy, KvOverflowBackend, KvOverflowConfig, KvOverflowReplica, KvOverflowTransport,
};
use shardmap::storage::{
    DEFAULT_KV_OVERFLOW_SLOT_COUNT, EmbeddedStore, KvOverflowCluster, KvOverflowNode,
    KvOverflowOptions, KvOverflowStore, KvOverflowValue,
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Parser)]
#[command(about = "Primary write overhead of asynchronous key-value overflow")]
struct Args {
    #[arg(long, default_value_t = 200_000)]
    iterations: usize,
    #[arg(long, default_value_t = 16_384)]
    keys: usize,
    #[arg(long, default_value_t = 1_024)]
    value_size: usize,
    /// Number of primary writer threads sharing the store.
    #[arg(long, default_value_t = 1)]
    producers: usize,
    /// Total queued plus active mutations. Defaults to twice `iterations` to
    /// leave headroom for per-shard distribution in blocked mode.
    #[arg(long)]
    queue_capacity: Option<usize>,
    /// Overflow endpoint used by workers.
    #[arg(long, value_enum, default_value_t = BenchmarkBackend::Noop)]
    backend: BenchmarkBackend,
    /// SCNP or Redis URL. Required for non-noop backends.
    #[arg(long)]
    endpoint: Vec<String>,
    /// SCNP transport. Direct mode requires one endpoint plus replica metadata.
    #[arg(long, value_enum, default_value_t = BenchmarkTransport::Fanout)]
    transport: BenchmarkTransport,
    #[arg(long, default_value = "benchmark-replica")]
    scnp_replica_id: String,
    #[arg(long, default_value_t = 16)]
    scnp_shard_count: usize,
    /// Zero uses the port advertised by SCNP.TOPOLOGY.
    #[arg(long, default_value_t = 0)]
    scnp_direct_base_port: u16,
    #[arg(long, default_value_t = 64)]
    pipeline_max_items: usize,
    #[arg(long, default_value_t = 256 * 1024)]
    pipeline_max_bytes: usize,
    #[arg(long, default_value_t = 1)]
    max_inflight_per_target: usize,
    /// Number of in-process logical replicas for topology-scaling tests.
    #[arg(long, default_value_t = 1)]
    noop_replicas: usize,
    /// Remote shard targets exposed by each in-process logical replica.
    #[arg(long, default_value_t = 1)]
    noop_shards_per_replica: usize,
    /// Record one operation latency for every N writes on each producer.
    #[arg(long, default_value_t = 64)]
    latency_sample_every: usize,
    /// Read operations measured after every write is visible remotely.
    #[arg(long, default_value_t = 0)]
    read_iterations: usize,
    /// Whether the in-process no-op endpoint drains during primary writes.
    #[arg(long, value_enum, default_value_t = DrainMode::Blocked)]
    drain_mode: DrainMode,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum BenchmarkBackend {
    Noop,
    Scnp,
    Redis,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum BenchmarkTransport {
    Fanout,
    Direct,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum DrainMode {
    Blocked,
    Concurrent,
}

#[derive(Debug)]
struct DrainGate {
    open: AtomicBool,
    mutex: Mutex<()>,
    wake: Condvar,
}

impl DrainGate {
    fn new(open: bool) -> Self {
        Self {
            open: AtomicBool::new(open),
            mutex: Mutex::new(()),
            wake: Condvar::new(),
        }
    }

    fn wait(&self) {
        if self.open.load(Ordering::Acquire) {
            return;
        }
        let mut guard = self.mutex.lock().expect("benchmark drain gate");
        while !self.open.load(Ordering::Acquire) {
            guard = self.wake.wait(guard).expect("benchmark drain gate wait");
        }
    }

    fn open(&self) {
        self.open.store(true, Ordering::Release);
        self.wake.notify_all();
    }
}

#[derive(Debug)]
struct NoopNode {
    id: String,
    replica_id: String,
    remote_shard: usize,
    puts: Arc<AtomicU64>,
    drain_gate: Arc<DrainGate>,
}

impl KvOverflowNode for NoopNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn replica_id(&self) -> &str {
        &self.replica_id
    }

    fn remote_shard(&self) -> usize {
        self.remote_shard
    }

    fn put(&self, _key: &[u8], value: &[u8], _ttl_ms: Option<u64>) -> shardmap::Result<()> {
        self.drain_gate.wait();
        self.puts
            .fetch_add(black_box(value.len() as u64), Ordering::Relaxed);
        Ok(())
    }

    fn get(&self, _key: &[u8]) -> shardmap::Result<Option<KvOverflowValue>> {
        Ok(None)
    }

    fn delete(&self, _key: &[u8]) -> shardmap::Result<()> {
        Ok(())
    }
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    if args.iterations == 0
        || args.keys == 0
        || args.producers == 0
        || args.pipeline_max_items == 0
        || args.pipeline_max_bytes == 0
        || args.max_inflight_per_target == 0
        || args.noop_replicas == 0
        || args.noop_shards_per_replica == 0
        || args.latency_sample_every == 0
        || args.producers > args.iterations
    {
        return Err(
            "iterations, keys, producers, and no-op topology dimensions must be greater than zero, and producers cannot exceed iterations"
                .into(),
        );
    }
    let queue_capacity = args
        .queue_capacity
        .unwrap_or_else(|| args.iterations.saturating_mul(2));
    if queue_capacity == 0 {
        return Err("queue-capacity must be greater than zero".into());
    }
    if args.backend == BenchmarkBackend::Noop
        && args.drain_mode == DrainMode::Blocked
        && queue_capacity < args.iterations
    {
        return Err("blocked drain mode requires queue-capacity >= iterations".into());
    }
    if args.backend != BenchmarkBackend::Noop && args.endpoint.is_empty() {
        return Err("at least one --endpoint is required for SCNP and Redis backends".into());
    }
    if args.backend == BenchmarkBackend::Scnp
        && args.transport == BenchmarkTransport::Direct
        && (args.endpoint.len() != 1
            || args.scnp_shard_count == 0
            || !args.scnp_shard_count.is_power_of_two())
    {
        return Err(
            "direct SCNP requires exactly one --endpoint and a power-of-two --scnp-shard-count"
                .into(),
        );
    }

    let value: Arc<[u8]> = vec![0x5a; args.value_size].into();
    let baseline_duration = run_baseline(
        Arc::clone(&value),
        args.iterations,
        args.keys,
        args.producers,
        args.latency_sample_every,
    );

    let drain_gate = Arc::new(DrainGate::new(
        args.drain_mode == DrainMode::Concurrent || args.backend != BenchmarkBackend::Noop,
    ));
    let noop_puts = Arc::new(AtomicU64::new(0));
    let overflow = Arc::new(make_overflow_store(
        &args,
        queue_capacity,
        Arc::clone(&drain_gate),
        Arc::clone(&noop_puts),
    )?);
    let enqueue_result = run_overflow(
        Arc::clone(&overflow),
        Arc::clone(&value),
        args.iterations,
        args.keys,
        args.producers,
        args.latency_sample_every,
    );
    drain_gate.open();
    let enqueue = enqueue_result?;
    let flush_started = Instant::now();
    overflow.flush_remote()?;
    let flush_duration = flush_started.elapsed();
    let health = overflow.health_snapshot();

    println!(
        "kv-overflow-primary-cost: iterations={} keys={} value_size={} workers={} drains_per_shard={} primary_shards={} producers={} queue_capacity={} backend={:?} transport={:?} endpoints={} noop_replicas={} noop_shards_per_replica={} pipeline_max_items={} pipeline_max_bytes={} max_inflight_per_target={} latency_sample_every={} drain_mode={:?}",
        args.iterations,
        args.keys,
        args.value_size,
        health.drains_per_shard * health.primary_shard_count,
        health.drains_per_shard,
        health.primary_shard_count,
        args.producers,
        queue_capacity,
        args.backend,
        args.transport,
        args.endpoint.len(),
        args.noop_replicas,
        args.noop_shards_per_replica,
        args.pipeline_max_items,
        args.pipeline_max_bytes,
        args.max_inflight_per_target,
        args.latency_sample_every,
        args.drain_mode,
    );
    println!("| case | ns/op | ops/sec |");
    println!("| --- | ---: | ---: |");
    print_row("embedded-set", baseline_duration.duration, args.iterations);
    print_row("kv-overflow-enqueue", enqueue.duration, args.iterations);
    println!("| case | samples | p50 | p95 | p99 |");
    println!("| --- | ---: | ---: | ---: | ---: |");
    print_latency_row("embedded-set", &baseline_duration.latency);
    print_latency_row("kv-overflow-enqueue", &enqueue.latency);
    println!(
        "enqueue overhead: {:.2}x; sampled p99 overhead: {:.2}x; drain after producer: {:.3} ms; end-to-end: {:.0} ops/sec; replicated puts: {}; checksum: {}",
        enqueue.duration.as_secs_f64() / baseline_duration.duration.as_secs_f64(),
        enqueue.latency.p99_ns() as f64 / baseline_duration.latency.p99_ns() as f64,
        flush_duration.as_secs_f64() * 1_000.0,
        args.iterations as f64 / (enqueue.duration + flush_duration).as_secs_f64(),
        health.replicated_puts,
        noop_puts.load(Ordering::Relaxed)
    );
    if args.read_iterations > 0 {
        let local_reads = run_reads(
            Arc::clone(&overflow),
            args.read_iterations,
            args.keys,
            args.producers,
            args.latency_sample_every,
            false,
        )?;
        let remote_reads = run_reads(
            Arc::clone(&overflow),
            args.read_iterations,
            args.keys,
            args.producers,
            args.latency_sample_every,
            true,
        )?;
        println!("| read case | ns/op | ops/sec | samples | p50 | p95 | p99 |");
        println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
        print_read_row("embedded-get", &local_reads, args.read_iterations);
        print_read_row("overflow-remote-get", &remote_reads, args.read_iterations);
    }
    Ok(())
}

struct RunMeasurement {
    duration: Duration,
    latency: LatencyHistogram,
}

fn make_overflow_store(
    args: &Args,
    queue_capacity: usize,
    drain_gate: Arc<DrainGate>,
    noop_puts: Arc<AtomicU64>,
) -> shardmap::Result<KvOverflowStore> {
    match args.backend {
        BenchmarkBackend::Noop => {
            let mut nodes = Vec::<Arc<dyn KvOverflowNode>>::with_capacity(
                args.noop_replicas
                    .saturating_mul(args.noop_shards_per_replica),
            );
            for replica in 0..args.noop_replicas {
                let replica_id = format!("benchmark-noop-{replica:04}");
                for remote_shard in 0..args.noop_shards_per_replica {
                    nodes.push(Arc::new(NoopNode {
                        id: format!("{replica_id}#{remote_shard}"),
                        replica_id: replica_id.clone(),
                        remote_shard,
                        puts: Arc::clone(&noop_puts),
                        drain_gate: Arc::clone(&drain_gate),
                    }));
                }
            }
            let cluster = Arc::new(KvOverflowCluster::with_previous_for_primary_shards(
                nodes,
                Vec::new(),
                DEFAULT_KV_OVERFLOW_SLOT_COUNT,
                16,
            )?);
            KvOverflowStore::new(
                EmbeddedStore::new(16),
                cluster,
                KvOverflowOptions {
                    max_memory_bytes: usize::MAX,
                    eviction_policy: EvictionPolicy::Lru,
                    fetch_on_miss: true,
                    cleanup_interval: Duration::from_secs(60),
                    queue_capacity: queue_capacity.div_ceil(16),
                    pipeline_max_items: args.pipeline_max_items,
                    pipeline_max_bytes: args.pipeline_max_bytes,
                    pipeline_flush: Duration::from_micros(200),
                    max_inflight_per_target: args.max_inflight_per_target,
                },
            )
        }
        BenchmarkBackend::Scnp | BenchmarkBackend::Redis => {
            let direct = args.backend == BenchmarkBackend::Scnp
                && args.transport == BenchmarkTransport::Direct;
            let config = KvOverflowConfig {
                enabled: true,
                backend: match args.backend {
                    BenchmarkBackend::Scnp => KvOverflowBackend::Scnp,
                    BenchmarkBackend::Redis => KvOverflowBackend::Redis,
                    BenchmarkBackend::Noop => unreachable!(),
                },
                endpoints: if direct {
                    Vec::new()
                } else {
                    args.endpoint.clone()
                },
                replicas: if direct {
                    vec![KvOverflowReplica {
                        id: args.scnp_replica_id.clone(),
                        addresses: args.endpoint.clone(),
                        shard_count: args.scnp_shard_count,
                        direct_shard_base_port: args.scnp_direct_base_port,
                    }]
                } else {
                    Vec::new()
                },
                transport: match args.transport {
                    BenchmarkTransport::Fanout => KvOverflowTransport::Fanout,
                    BenchmarkTransport::Direct => KvOverflowTransport::DirectShard,
                },
                max_memory_bytes: u64::MAX,
                queue_capacity,
                queue_capacity_per_shard: queue_capacity.div_ceil(16),
                pipeline_max_items: args.pipeline_max_items,
                pipeline_max_bytes: args.pipeline_max_bytes,
                max_inflight_per_target: args.max_inflight_per_target,
                ..KvOverflowConfig::default()
            };
            KvOverflowStore::from_config(EmbeddedStore::new(16), &config)
        }
    }
}

fn run_baseline(
    value: Arc<[u8]>,
    iterations: usize,
    keys: usize,
    producers: usize,
    latency_sample_every: usize,
) -> RunMeasurement {
    let store = Arc::new(EmbeddedStore::new(16));
    let start = Arc::new(Barrier::new(producers + 1));
    let mut joins = Vec::with_capacity(producers);
    for producer in 0..producers {
        let store = Arc::clone(&store);
        let value = Arc::clone(&value);
        let start = Arc::clone(&start);
        joins.push(std::thread::spawn(move || {
            let mut latency = LatencyHistogram::new();
            let range = producer_range(iterations, producers, producer);
            start.wait();
            for iteration in range {
                let key = key_for(iteration % keys);
                if iteration % latency_sample_every == 0 {
                    let started = Instant::now();
                    store.set(key, value.as_ref().to_vec(), None);
                    latency.record(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                } else {
                    store.set(key, value.as_ref().to_vec(), None);
                }
            }
            latency
        }));
    }
    let started = Instant::now();
    start.wait();
    let mut latency = LatencyHistogram::new();
    for join in joins {
        latency.merge(&join.join().expect("baseline producer"));
    }
    RunMeasurement {
        duration: started.elapsed(),
        latency,
    }
}

fn run_overflow(
    store: Arc<KvOverflowStore>,
    value: Arc<[u8]>,
    iterations: usize,
    keys: usize,
    producers: usize,
    latency_sample_every: usize,
) -> shardmap::Result<RunMeasurement> {
    let start = Arc::new(Barrier::new(producers + 1));
    let mut joins = Vec::with_capacity(producers);
    for producer in 0..producers {
        let store = Arc::clone(&store);
        let value = Arc::clone(&value);
        let start = Arc::clone(&start);
        joins.push(std::thread::spawn(move || {
            let mut latency = LatencyHistogram::new();
            let range = producer_range(iterations, producers, producer);
            start.wait();
            for iteration in range {
                let key = key_for(iteration % keys);
                if iteration % latency_sample_every == 0 {
                    let started = Instant::now();
                    store.set(key, value.as_ref().to_vec(), None)?;
                    latency.record(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                } else {
                    store.set(key, value.as_ref().to_vec(), None)?;
                }
            }
            shardmap::Result::Ok(latency)
        }));
    }
    let started = Instant::now();
    start.wait();
    let mut latency = LatencyHistogram::new();
    for join in joins {
        latency.merge(&join.join().expect("overflow producer")?);
    }
    Ok(RunMeasurement {
        duration: started.elapsed(),
        latency,
    })
}

fn run_reads(
    store: Arc<KvOverflowStore>,
    iterations: usize,
    keys: usize,
    producers: usize,
    latency_sample_every: usize,
    remote: bool,
) -> shardmap::Result<RunMeasurement> {
    let start = Arc::new(Barrier::new(producers + 1));
    let mut joins = Vec::with_capacity(producers);
    for producer in 0..producers {
        let store = Arc::clone(&store);
        let start = Arc::clone(&start);
        joins.push(std::thread::spawn(move || {
            let mut latency = LatencyHistogram::new();
            let range = producer_range(iterations, producers, producer);
            start.wait();
            for iteration in range {
                let key = key_for(iteration % keys);
                let sampled = iteration % latency_sample_every == 0;
                let started = sampled.then(Instant::now);
                let found = if remote {
                    store.get_remote(&key)?.is_some()
                } else {
                    store.inner().get(&key).is_some()
                };
                if !found {
                    return Err(shardmap::ShardCacheError::Protocol(
                        "benchmark read missed a replicated key".into(),
                    ));
                }
                if let Some(started) = started {
                    latency.record(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                }
            }
            shardmap::Result::Ok(latency)
        }));
    }
    let started = Instant::now();
    start.wait();
    let mut latency = LatencyHistogram::new();
    for join in joins {
        latency.merge(&join.join().expect("read producer")?);
    }
    Ok(RunMeasurement {
        duration: started.elapsed(),
        latency,
    })
}

fn producer_range(iterations: usize, producers: usize, producer: usize) -> std::ops::Range<usize> {
    let start = iterations * producer / producers;
    let end = iterations * (producer + 1) / producers;
    start..end
}

fn key_for(index: usize) -> Vec<u8> {
    format!("bench-key-{index:08}").into_bytes()
}

fn print_row(label: &str, duration: Duration, iterations: usize) {
    let ns_per_op = duration.as_nanos() as f64 / iterations as f64;
    let ops_per_second = iterations as f64 / duration.as_secs_f64();
    println!("| {label} | {ns_per_op:.1} | {ops_per_second:.0} |");
}

fn print_latency_row(label: &str, latency: &LatencyHistogram) {
    println!(
        "| {label} | {} | {} ns | {} ns | {} ns |",
        latency.count(),
        latency.p50_ns(),
        latency.p95_ns(),
        latency.p99_ns()
    );
}

fn print_read_row(label: &str, measurement: &RunMeasurement, iterations: usize) {
    println!(
        "| {label} | {:.1} | {:.0} | {} | {} ns | {} ns | {} ns |",
        measurement.duration.as_nanos() as f64 / iterations as f64,
        iterations as f64 / measurement.duration.as_secs_f64(),
        measurement.latency.count(),
        measurement.latency.p50_ns(),
        measurement.latency.p95_ns(),
        measurement.latency.p99_ns()
    );
}
