use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use shardmap::config::{EvictionPolicy, KvOverflowBackend, KvOverflowConfig};
use shardmap::storage::{
    EmbeddedStore, KvOverflowCluster, KvOverflowNode, KvOverflowOptions, KvOverflowStore,
    KvOverflowValue,
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
    #[arg(long, default_value_t = 2)]
    worker_threads: usize,
    /// Number of primary writer threads sharing the store.
    #[arg(long, default_value_t = 1)]
    producers: usize,
    /// Total queued plus active mutations. Defaults to `iterations`.
    #[arg(long)]
    queue_capacity: Option<usize>,
    /// Overflow endpoint used by workers.
    #[arg(long, value_enum, default_value_t = BenchmarkBackend::Noop)]
    backend: BenchmarkBackend,
    /// SCNP or Redis URL. Required for non-noop backends.
    #[arg(long)]
    endpoint: Vec<String>,
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
    puts: AtomicU64,
    drain_gate: Arc<DrainGate>,
}

impl KvOverflowNode for NoopNode {
    fn id(&self) -> &str {
        "benchmark-noop"
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
        || args.worker_threads == 0
        || args.producers == 0
        || args.producers > args.iterations
    {
        return Err(
            "iterations, keys, worker-threads, and producers must be greater than zero, and producers cannot exceed iterations"
                .into(),
        );
    }
    let queue_capacity = args.queue_capacity.unwrap_or(args.iterations);
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

    let value: Arc<[u8]> = vec![0x5a; args.value_size].into();
    let baseline_duration = run_baseline(
        Arc::clone(&value),
        args.iterations,
        args.keys,
        args.producers,
    );

    let drain_gate = Arc::new(DrainGate::new(
        args.drain_mode == DrainMode::Concurrent || args.backend != BenchmarkBackend::Noop,
    ));
    let node = Arc::new(NoopNode {
        puts: AtomicU64::new(0),
        drain_gate: Arc::clone(&drain_gate),
    });
    let overflow = Arc::new(make_overflow_store(
        &args,
        queue_capacity,
        Arc::clone(&node),
    )?);
    let enqueue_duration = run_overflow(
        Arc::clone(&overflow),
        Arc::clone(&value),
        args.iterations,
        args.keys,
        args.producers,
    )?;
    drain_gate.open();
    let flush_started = Instant::now();
    overflow.flush_remote()?;
    let flush_duration = flush_started.elapsed();
    let health = overflow.health_snapshot();

    println!(
        "kv-overflow-primary-cost: iterations={} keys={} value_size={} workers={} producers={} queue_capacity={} backend={:?} endpoints={} drain_mode={:?}",
        args.iterations,
        args.keys,
        args.value_size,
        args.worker_threads,
        args.producers,
        queue_capacity,
        args.backend,
        args.endpoint.len(),
        args.drain_mode,
    );
    println!("| case | ns/op | ops/sec |");
    println!("| --- | ---: | ---: |");
    print_row("embedded-set", baseline_duration, args.iterations);
    print_row("kv-overflow-enqueue", enqueue_duration, args.iterations);
    println!(
        "enqueue overhead: {:.2}x; drain after producer: {:.3} ms; end-to-end: {:.0} ops/sec; replicated puts: {}; checksum: {}",
        enqueue_duration.as_secs_f64() / baseline_duration.as_secs_f64(),
        flush_duration.as_secs_f64() * 1_000.0,
        args.iterations as f64 / (enqueue_duration + flush_duration).as_secs_f64(),
        health.replicated_puts,
        node.puts.load(Ordering::Relaxed)
    );
    Ok(())
}

fn make_overflow_store(
    args: &Args,
    queue_capacity: usize,
    node: Arc<NoopNode>,
) -> shardmap::Result<KvOverflowStore> {
    match args.backend {
        BenchmarkBackend::Noop => {
            let cluster = Arc::new(KvOverflowCluster::new(vec![node])?);
            KvOverflowStore::new(
                EmbeddedStore::new(16),
                cluster,
                KvOverflowOptions {
                    max_memory_bytes: usize::MAX,
                    eviction_policy: EvictionPolicy::Lru,
                    fetch_on_miss: true,
                    cleanup_interval: Duration::from_secs(60),
                    worker_threads: args.worker_threads,
                    queue_capacity,
                },
            )
        }
        BenchmarkBackend::Scnp | BenchmarkBackend::Redis => {
            let config = KvOverflowConfig {
                enabled: true,
                backend: match args.backend {
                    BenchmarkBackend::Scnp => KvOverflowBackend::Scnp,
                    BenchmarkBackend::Redis => KvOverflowBackend::Redis,
                    BenchmarkBackend::Noop => unreachable!(),
                },
                endpoints: args.endpoint.clone(),
                max_memory_bytes: u64::MAX,
                connections_per_endpoint: args.worker_threads,
                worker_threads: args.worker_threads,
                queue_capacity,
                ..KvOverflowConfig::default()
            };
            KvOverflowStore::from_config(EmbeddedStore::new(16), &config)
        }
    }
}

fn run_baseline(value: Arc<[u8]>, iterations: usize, keys: usize, producers: usize) -> Duration {
    let store = Arc::new(EmbeddedStore::new(16));
    let start = Arc::new(Barrier::new(producers + 1));
    let mut joins = Vec::with_capacity(producers);
    for producer in 0..producers {
        let store = Arc::clone(&store);
        let value = Arc::clone(&value);
        let start = Arc::clone(&start);
        joins.push(std::thread::spawn(move || {
            let range = producer_range(iterations, producers, producer);
            start.wait();
            for iteration in range {
                let key = key_for(iteration % keys);
                store.set(key, value.as_ref().to_vec(), None);
            }
        }));
    }
    let started = Instant::now();
    start.wait();
    for join in joins {
        join.join().expect("baseline producer");
    }
    started.elapsed()
}

fn run_overflow(
    store: Arc<KvOverflowStore>,
    value: Arc<[u8]>,
    iterations: usize,
    keys: usize,
    producers: usize,
) -> shardmap::Result<Duration> {
    let start = Arc::new(Barrier::new(producers + 1));
    let mut joins = Vec::with_capacity(producers);
    for producer in 0..producers {
        let store = Arc::clone(&store);
        let value = Arc::clone(&value);
        let start = Arc::clone(&start);
        joins.push(std::thread::spawn(move || {
            let range = producer_range(iterations, producers, producer);
            start.wait();
            for iteration in range {
                let key = key_for(iteration % keys);
                store.set(key, value.as_ref().to_vec(), None)?;
            }
            shardmap::Result::Ok(())
        }));
    }
    let started = Instant::now();
    start.wait();
    for join in joins {
        join.join().expect("overflow producer")?;
    }
    Ok(started.elapsed())
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
