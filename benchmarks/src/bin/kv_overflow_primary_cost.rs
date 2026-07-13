use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use shardmap::config::EvictionPolicy;
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
}

#[derive(Debug)]
struct NoopNode {
    puts: AtomicU64,
    drain_gate: Arc<(Mutex<bool>, Condvar)>,
}

impl KvOverflowNode for NoopNode {
    fn id(&self) -> &str {
        "benchmark-noop"
    }

    fn put(&self, _key: &[u8], value: &[u8], _ttl_ms: Option<u64>) -> shardmap::Result<()> {
        let (open, wake) = self.drain_gate.as_ref();
        let mut is_open = open.lock().expect("benchmark drain gate");
        while !*is_open {
            is_open = wake.wait(is_open).expect("benchmark drain gate wait");
        }
        drop(is_open);
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
    if args.iterations == 0 || args.keys == 0 || args.worker_threads == 0 {
        return Err("iterations, keys, and worker-threads must be greater than zero".into());
    }

    let value = vec![0x5a; args.value_size];
    let baseline = EmbeddedStore::new(16);
    let baseline_duration = run_baseline(&baseline, &value, args.iterations, args.keys);

    let drain_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let node = Arc::new(NoopNode {
        puts: AtomicU64::new(0),
        drain_gate: Arc::clone(&drain_gate),
    });
    let cluster = Arc::new(KvOverflowCluster::new(vec![node.clone()]).expect("benchmark cluster"));
    let overflow = KvOverflowStore::new(
        EmbeddedStore::new(16),
        cluster,
        KvOverflowOptions {
            max_memory_bytes: usize::MAX,
            eviction_policy: EvictionPolicy::Lru,
            fetch_on_miss: true,
            cleanup_interval: Duration::from_secs(60),
            worker_threads: args.worker_threads,
            queue_capacity: args.iterations,
        },
    )?;
    let enqueue_started = Instant::now();
    for iteration in 0..args.iterations {
        let key = key_for(iteration % args.keys);
        overflow.set(key, value.clone(), None)?;
    }
    let enqueue_duration = enqueue_started.elapsed();
    let (open, wake) = drain_gate.as_ref();
    *open.lock().expect("benchmark drain gate") = true;
    wake.notify_all();
    let flush_started = Instant::now();
    overflow.flush_remote()?;
    let flush_duration = flush_started.elapsed();
    let health = overflow.health_snapshot();

    println!(
        "kv-overflow-primary-cost: iterations={} keys={} value_size={} workers={}",
        args.iterations, args.keys, args.value_size, args.worker_threads
    );
    println!("| case | ns/op | ops/sec |");
    println!("| --- | ---: | ---: |");
    print_row("embedded-set", baseline_duration, args.iterations);
    print_row("kv-overflow-enqueue", enqueue_duration, args.iterations);
    println!(
        "enqueue overhead: {:.2}x; drain after producer: {:.3} ms; replicated puts: {}; checksum: {}",
        enqueue_duration.as_secs_f64() / baseline_duration.as_secs_f64(),
        flush_duration.as_secs_f64() * 1_000.0,
        health.replicated_puts,
        node.puts.load(Ordering::Relaxed)
    );
    Ok(())
}

fn run_baseline(store: &EmbeddedStore, value: &[u8], iterations: usize, keys: usize) -> Duration {
    let started = Instant::now();
    for iteration in 0..iterations {
        let key = key_for(iteration % keys);
        store.set(key, value.to_vec(), None);
    }
    started.elapsed()
}

fn key_for(index: usize) -> Vec<u8> {
    format!("bench-key-{index:08}").into_bytes()
}

fn print_row(label: &str, duration: Duration, iterations: usize) {
    let ns_per_op = duration.as_nanos() as f64 / iterations as f64;
    let ops_per_second = iterations as f64 / duration.as_secs_f64();
    println!("| {label} | {ns_per_op:.1} | {ops_per_second:.0} |");
}
