//! Exposes a caller-owned embedded store while simultaneously driving local
//! embedded traffic against the same store.
//!
//! Pair this with `saturation --backends fc-server-resp` to measure third-party
//! client performance while the embedding process is also using the database.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use shardcache_benchmarks::backend::Op;
use shardcache_benchmarks::clock::FastClock;
use shardcache_benchmarks::cpu::{process_cpu_time, vcpu};
use shardcache_benchmarks::csv::CsvWriter;
use shardcache_benchmarks::histogram::{LatencyHistogram, format_ns};
use shardcache_benchmarks::workload::{
    KeyDistribution, KeyPattern, Mix, OpStream, Workload, WorkloadSpec,
};
use shardmap::config::{EvictionPolicy, ShardCacheConfig};
use shardmap::server::{ServerRuntime, ShardCacheServer};
use shardmap::storage::{
    EmbeddedKeyRoute, EmbeddedRouteMode, EmbeddedStore, ShardArcEmbeddedStore,
    take_local_embedded_store, with_local_embedded_store,
};

#[derive(Debug, Parser)]
#[command(about = "Mixed embedded + third-party server benchmark harness")]
struct Args {
    /// Server bind address, e.g. 127.0.0.1:6383.
    #[arg(long, default_value = "127.0.0.1:6383")]
    bind_addr: String,

    /// Number of embedded storage shards.
    #[arg(long, default_value_t = 16)]
    shard_count: usize,

    /// Tokio runtime worker threads for the server.
    #[arg(long)]
    runtime_threads: Option<usize>,

    /// Maximum accepted client connections.
    #[arg(long, default_value_t = 4096)]
    max_connections: usize,

    /// Total memory budget in bytes. 0 disables memory-limit eviction.
    #[arg(long, default_value_t = 0)]
    max_memory_bytes: u64,

    /// Route s:<session>:c:<chunk> keys by session prefix.
    #[arg(long)]
    session_prefix_routing: bool,

    /// Number of local embedded worker threads.
    #[arg(long, default_value_t = 4)]
    internal_workers: usize,

    /// Local embedded value size in bytes.
    #[arg(long, default_value_t = 1024)]
    value_size: usize,

    /// Local embedded operation mix: get, set, 80-20, or <get_pct>-<set_pct>.
    #[arg(long, default_value = "get")]
    internal_mix: String,

    /// Key set cardinality shared by local and remote benchmark clients.
    #[arg(long, default_value_t = 100_000)]
    key_count: usize,

    /// Local embedded key pattern: point or session.
    #[arg(long, default_value = "point")]
    key_pattern: String,

    /// Local embedded key distribution: uniform, zipf[:theta], or hot:<keys>[:pct].
    #[arg(long, default_value = "uniform")]
    key_distribution: String,

    /// Warmup seconds before local embedded stats are recorded.
    #[arg(long, default_value_t = 2)]
    internal_warmup: u64,

    /// Record one local embedded latency sample every N successful measured ops.
    /// Use 0 to disable local latency timing.
    #[arg(long, default_value_t = 64)]
    internal_latency_sample_rate: u64,

    /// Optional CSV path for the local embedded side of the mixed run.
    #[arg(long)]
    internal_csv: Option<String>,

    /// Run the server and embedded workload on one owner-local store.
    ///
    /// This measures the topology where third-party clients and embedded
    /// callers share the same shard-owned memory instead of going through the
    /// shared Arc<EmbeddedStore> lock path.
    #[arg(long)]
    owner_local: bool,

    /// Benchmark-only topology probe: shard-level Arc storage.
    #[arg(long)]
    shard_arc: bool,

    /// Owner-local operations to run before yielding to the TCP server.
    #[arg(long, default_value_t = 1024)]
    owner_yield_ops: u64,
}

#[derive(Clone)]
struct PreparedKey {
    key: Vec<u8>,
    route: EmbeddedKeyRoute,
}

struct InternalRunState {
    stop: Arc<AtomicBool>,
    measure: Arc<AtomicBool>,
    handles: Vec<thread::JoinHandle<InternalWorkerResult>>,
}

struct InternalWorkerResult {
    ops: u64,
    reads: u64,
    writes: u64,
    bytes: u64,
    errors: u64,
    all: LatencyHistogram,
    reads_hist: LatencyHistogram,
    writes_hist: LatencyHistogram,
}

struct InternalRunResult {
    ops: u64,
    reads: u64,
    writes: u64,
    bytes: u64,
    errors: u64,
    wall: Duration,
    vcpu_consumed: f64,
    all: LatencyHistogram,
    reads_hist: LatencyHistogram,
    writes_hist: LatencyHistogram,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    ServerRuntime::initialize_tracing();
    let args = Args::parse();
    if args.internal_workers == 0 {
        return Err("--internal-workers must be greater than zero".into());
    }
    if args.key_count == 0 {
        return Err("--key-count must be greater than zero".into());
    }
    if args.owner_local && args.shard_arc {
        return Err("--owner-local and --shard-arc are mutually exclusive".into());
    }

    let mix = Mix::parse(&args.internal_mix)?;
    let key_pattern = KeyPattern::parse(&args.key_pattern)?;
    let key_distribution = KeyDistribution::parse(&args.key_distribution)?;
    let route_mode = match args.session_prefix_routing {
        true => EmbeddedRouteMode::SessionPrefix,
        false => EmbeddedRouteMode::FullKey,
    };

    if args.owner_local {
        return run_owner_local(args, mix, key_pattern, key_distribution, route_mode);
    }
    if args.shard_arc {
        return run_shard_arc(args, mix, key_pattern, key_distribution, route_mode);
    }

    let store = Arc::new(EmbeddedStore::with_route_mode(args.shard_count, route_mode));
    if args.max_memory_bytes > 0 {
        let per_shard = args
            .max_memory_bytes
            .checked_div(args.shard_count as u64)
            .and_then(|bytes| usize::try_from(bytes).ok());
        store.configure_memory_policy(per_shard, EvictionPolicy::Lru);
    }

    let workload = Workload::build(&WorkloadSpec {
        key_count: args.key_count,
        value_size: args.value_size,
        mix,
        key_pattern,
        key_distribution,
    });
    let keys = prepare_keys(&store, workload.keys());
    warmup_store(&store, &keys, workload.value());

    let internal = spawn_internal_load(
        Arc::clone(&store),
        Arc::new(keys),
        Arc::new(workload.value().to_vec()),
        mix,
        key_distribution,
        args.internal_workers,
        args.internal_latency_sample_rate,
    );

    let mut config = ShardCacheConfig::default();
    config.bind_addr = args.bind_addr.clone();
    config.shard_count = args.shard_count;
    config.max_connections = args.max_connections;
    config.max_memory_bytes = args.max_memory_bytes;
    config.eviction_policy = if args.max_memory_bytes > 0 {
        EvictionPolicy::Lru
    } else {
        EvictionPolicy::None
    };
    config.persistence.enabled = false;
    config.validate()?;

    let runtime_threads = if args.owner_local {
        1
    } else {
        args.runtime_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1)
                .clamp(2, 8)
        })
    };
    println!(
        "embedded_mixed_harness: pid={} bind={} shards={} runtime_threads={} internal_workers={} value_size={} mix={} keys={} warmup={}s",
        std::process::id(),
        args.bind_addr,
        args.shard_count,
        runtime_threads,
        args.internal_workers,
        args.value_size,
        mix.label(),
        args.key_count,
        args.internal_warmup
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_threads)
        .enable_all()
        .build()?;

    let measure = Arc::clone(&internal.measure);
    let cpu_start = std::sync::Arc::new(std::sync::Mutex::new(None));
    let cpu_start_for_runtime = Arc::clone(&cpu_start);
    let server = ShardCacheServer::from_embedded_store(config, store);

    let server_result: Result<(), Box<dyn std::error::Error>> = runtime.block_on(async move {
        let server_task = tokio::spawn(async move {
            server
                .run_with_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await
        });

        tokio::time::sleep(Duration::from_secs(args.internal_warmup)).await;
        {
            let mut guard = cpu_start_for_runtime
                .lock()
                .expect("cpu start mutex poisoned");
            *guard = Some((Instant::now(), process_cpu_time()));
        }
        measure.store(true, Ordering::Release);
        println!("embedded_mixed_harness: local embedded measurement active");

        match server_task.await {
            Ok(result) => result.map_err(|error| Box::new(error) as Box<dyn std::error::Error>),
            Err(error) => Err(Box::new(error) as Box<dyn std::error::Error>),
        }
    });

    internal.stop.store(true, Ordering::Release);
    let Some((started_at, cpu_before)) = cpu_start.lock().expect("cpu start mutex poisoned").take()
    else {
        return Err("mixed harness stopped before local measurement started".into());
    };
    let result = join_internal_load(
        internal,
        started_at.elapsed(),
        process_cpu_time() - cpu_before,
    );
    print_internal_result(&result);
    write_internal_csv(&args, &result)?;

    server_result?;
    Ok(())
}

fn run_shard_arc(
    args: Args,
    mix: Mix,
    key_pattern: KeyPattern,
    key_distribution: KeyDistribution,
    route_mode: EmbeddedRouteMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(ShardArcEmbeddedStore::with_route_mode(
        args.shard_count,
        route_mode,
    ));
    if args.max_memory_bytes > 0 {
        let per_shard = args
            .max_memory_bytes
            .checked_div(args.shard_count as u64)
            .and_then(|bytes| usize::try_from(bytes).ok());
        store.configure_memory_policy(per_shard, EvictionPolicy::Lru);
    }

    let workload = Workload::build(&WorkloadSpec {
        key_count: args.key_count,
        value_size: args.value_size,
        mix,
        key_pattern,
        key_distribution,
    });
    let keys = prepare_shard_arc_keys(&store, workload.keys());
    warmup_shard_arc_store(&store, &keys, workload.value());

    let internal = spawn_shard_arc_internal_load(
        Arc::clone(&store),
        Arc::new(keys),
        Arc::new(workload.value().to_vec()),
        mix,
        key_distribution,
        args.internal_workers,
        args.internal_latency_sample_rate,
    );

    let mut config = ShardCacheConfig::default();
    config.bind_addr = args.bind_addr.clone();
    config.shard_count = args.shard_count;
    config.max_connections = args.max_connections;
    config.max_memory_bytes = args.max_memory_bytes;
    config.eviction_policy = if args.max_memory_bytes > 0 {
        EvictionPolicy::Lru
    } else {
        EvictionPolicy::None
    };
    config.persistence.enabled = false;
    config.validate()?;

    let runtime_threads = args.runtime_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1)
            .clamp(2, 8)
    });
    println!(
        "embedded_mixed_harness: pid={} bind={} shards={} runtime_threads={} internal_workers={} value_size={} mix={} keys={} warmup={}s shard_arc=true",
        std::process::id(),
        args.bind_addr,
        args.shard_count,
        runtime_threads,
        args.internal_workers,
        args.value_size,
        mix.label(),
        args.key_count,
        args.internal_warmup
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_threads)
        .enable_all()
        .build()?;

    let measure = Arc::clone(&internal.measure);
    let cpu_start = Arc::new(std::sync::Mutex::new(None));
    let cpu_start_for_runtime = Arc::clone(&cpu_start);
    let server = ShardCacheServer::from_benchmark_shard_arc_embedded_store(config, store);

    let server_result: Result<(), Box<dyn std::error::Error>> = runtime.block_on(async move {
        let server_task = tokio::spawn(async move {
            server
                .run_with_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await
        });

        tokio::time::sleep(Duration::from_secs(args.internal_warmup)).await;
        {
            let mut guard = cpu_start_for_runtime
                .lock()
                .expect("cpu start mutex poisoned");
            *guard = Some((Instant::now(), process_cpu_time()));
        }
        measure.store(true, Ordering::Release);
        println!("embedded_mixed_harness: shard-arc embedded measurement active");

        match server_task.await {
            Ok(result) => result.map_err(|error| Box::new(error) as Box<dyn std::error::Error>),
            Err(error) => Err(Box::new(error) as Box<dyn std::error::Error>),
        }
    });

    internal.stop.store(true, Ordering::Release);
    let Some((started_at, cpu_before)) = cpu_start.lock().expect("cpu start mutex poisoned").take()
    else {
        return Err("shard-arc harness stopped before local measurement started".into());
    };
    let result = join_internal_load(
        internal,
        started_at.elapsed(),
        process_cpu_time() - cpu_before,
    );
    print_internal_result(&result);
    write_internal_csv(&args, &result)?;

    server_result?;
    Ok(())
}

fn run_owner_local(
    args: Args,
    mix: Mix,
    key_pattern: KeyPattern,
    key_distribution: KeyDistribution,
    route_mode: EmbeddedRouteMode,
) -> Result<(), Box<dyn std::error::Error>> {
    if args.shard_count != 1 {
        return Err("--owner-local currently requires --shard-count 1".into());
    }
    if args.internal_workers != 1 {
        return Err("--owner-local runs one owner thread; set --internal-workers 1".into());
    }
    if args.owner_yield_ops == 0 {
        return Err("--owner-yield-ops must be greater than zero".into());
    }

    let store = EmbeddedStore::with_route_mode(args.shard_count, route_mode);
    if args.max_memory_bytes > 0 {
        let per_shard = args
            .max_memory_bytes
            .checked_div(args.shard_count as u64)
            .and_then(|bytes| usize::try_from(bytes).ok());
        store.configure_memory_policy(per_shard, EvictionPolicy::Lru);
    }

    let workload = Workload::build(&WorkloadSpec {
        key_count: args.key_count,
        value_size: args.value_size,
        mix,
        key_pattern,
        key_distribution,
    });
    let keys = prepare_keys(&store, workload.keys());
    warmup_store(&store, &keys, workload.value());
    let local_store = store
        .into_local_stores(1)
        .into_iter()
        .next()
        .expect("single-shard owner-local harness must create one local store");

    let mut config = ShardCacheConfig::default();
    config.bind_addr = args.bind_addr.clone();
    config.shard_count = args.shard_count;
    config.max_connections = args.max_connections;
    config.max_memory_bytes = args.max_memory_bytes;
    config.eviction_policy = if args.max_memory_bytes > 0 {
        EvictionPolicy::Lru
    } else {
        EvictionPolicy::None
    };
    config.persistence.enabled = false;
    config.validate()?;

    println!(
        "embedded_mixed_harness: pid={} bind={} shards={} runtime_threads=1 internal_workers=1 value_size={} mix={} keys={} warmup={}s owner_local=true owner_yield_ops={}",
        std::process::id(),
        args.bind_addr,
        args.shard_count,
        args.value_size,
        mix.label(),
        args.key_count,
        args.internal_warmup,
        args.owner_yield_ops
    );

    let stop = Arc::new(AtomicBool::new(false));
    let measure = Arc::new(AtomicBool::new(false));
    let keys = Arc::new(keys);
    let value = Arc::new(workload.value().to_vec());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    let cpu_start = Arc::new(std::sync::Mutex::new(None));
    let cpu_start_for_runtime = Arc::clone(&cpu_start);
    let stop_for_runtime = Arc::clone(&stop);
    let measure_for_runtime = Arc::clone(&measure);
    let internal_latency_sample_rate = args.internal_latency_sample_rate;
    let internal_warmup = args.internal_warmup;
    let owner_yield_ops = args.owner_yield_ops;

    let (worker, server_result): (InternalWorkerResult, Result<(), shardmap::ShardCacheError>) =
        runtime.block_on(local.run_until(async move {
            local_store.install_local().map_err(|error| {
                shardmap::ShardCacheError::Config(format!(
                    "failed to install owner-local embedded store: {error}"
                ))
            })?;

            let local_task = tokio::task::spawn_local(run_owner_local_internal_worker(
                Arc::clone(&keys),
                Arc::clone(&value),
                mix,
                key_distribution,
                Arc::clone(&stop_for_runtime),
                Arc::clone(&measure_for_runtime),
                internal_latency_sample_rate,
                owner_yield_ops,
            ));

            let server = ShardCacheServer::from_thread_local_embedded_store(config);
            let server_task = tokio::task::spawn_local(async move {
                server
                    .run_thread_local_with_shutdown(async {
                        let _ = tokio::signal::ctrl_c().await;
                    })
                    .await
            });

            tokio::time::sleep(Duration::from_secs(internal_warmup)).await;
            {
                let mut guard = cpu_start_for_runtime
                    .lock()
                    .expect("cpu start mutex poisoned");
                *guard = Some((Instant::now(), process_cpu_time()));
            }
            measure_for_runtime.store(true, Ordering::Release);
            println!("embedded_mixed_harness: owner-local embedded measurement active");

            let server_result = match server_task.await {
                Ok(result) => result,
                Err(error) => Err(shardmap::ShardCacheError::Config(format!(
                    "owner-local server task failed: {error}"
                ))),
            };
            stop_for_runtime.store(true, Ordering::Release);
            let worker = local_task
                .await
                .expect("owner-local internal embedded worker panicked");
            let _ = take_local_embedded_store();
            Ok::<_, shardmap::ShardCacheError>((worker, server_result))
        }))?;

    let Some((started_at, cpu_before)) = cpu_start.lock().expect("cpu start mutex poisoned").take()
    else {
        return Err("owner-local harness stopped before local measurement started".into());
    };
    let result = InternalRunResult {
        ops: worker.ops,
        reads: worker.reads,
        writes: worker.writes,
        bytes: worker.bytes,
        errors: worker.errors,
        wall: started_at.elapsed(),
        vcpu_consumed: vcpu(process_cpu_time() - cpu_before, started_at.elapsed()),
        all: worker.all,
        reads_hist: worker.reads_hist,
        writes_hist: worker.writes_hist,
    };
    print_internal_result(&result);
    write_internal_csv(&args, &result)?;
    server_result?;
    Ok(())
}

async fn run_owner_local_internal_worker(
    keys: Arc<Vec<PreparedKey>>,
    value: Arc<Vec<u8>>,
    mix: Mix,
    key_distribution: KeyDistribution,
    stop: Arc<AtomicBool>,
    measure: Arc<AtomicBool>,
    latency_sample_rate: u64,
    yield_ops: u64,
) -> InternalWorkerResult {
    let mut stream = OpStream::new(0xA11C_010C_A1_u64, keys.len(), mix, key_distribution);
    let latency_clock = FastClock::new();
    let mut all = LatencyHistogram::new();
    let mut reads_hist = LatencyHistogram::new();
    let mut writes_hist = LatencyHistogram::new();
    let mut ops = 0u64;
    let mut reads = 0u64;
    let mut writes = 0u64;
    let mut bytes = 0u64;
    let mut errors = 0u64;

    while !stop.load(Ordering::Relaxed) {
        for _ in 0..yield_ops {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let (op, key_idx) = stream.next_op();
            let prepared = &keys[key_idx];
            let measured = measure.load(Ordering::Acquire);
            let sample_latency =
                measured && latency_sample_rate != 0 && ops.is_multiple_of(latency_sample_rate);
            let started = sample_latency.then(|| latency_clock.now());
            let ok = with_local_embedded_store(|store| match op {
                Op::Get => store
                    .get_ref_routed_no_ttl_local(prepared.route, &prepared.key)
                    .is_some(),
                Op::Set => {
                    store.set_slice_routed_no_ttl_local(
                        prepared.route,
                        &prepared.key,
                        value.as_slice(),
                    );
                    true
                }
            })
            .expect("owner-local embedded store should stay installed");

            if !measured {
                continue;
            }
            if !ok {
                errors += 1;
                continue;
            }
            if let Some(started) = started {
                let elapsed = latency_clock.elapsed_ns(started);
                all.record(elapsed);
                match op {
                    Op::Get => reads_hist.record(elapsed),
                    Op::Set => writes_hist.record(elapsed),
                }
            }
            ops += 1;
            bytes += value.len() as u64;
            match op {
                Op::Get => reads += 1,
                Op::Set => writes += 1,
            }
        }
        tokio::task::yield_now().await;
    }

    InternalWorkerResult {
        ops,
        reads,
        writes,
        bytes,
        errors,
        all,
        reads_hist,
        writes_hist,
    }
}

fn prepare_keys(store: &EmbeddedStore, keys: &[Vec<u8>]) -> Vec<PreparedKey> {
    keys.iter()
        .map(|key| PreparedKey {
            route: store.route_key(key),
            key: key.clone(),
        })
        .collect()
}

fn warmup_store(store: &EmbeddedStore, keys: &[PreparedKey], value: &[u8]) {
    for prepared in keys {
        store.set_slice_routed_no_ttl(prepared.route, &prepared.key, value);
    }
}

fn prepare_shard_arc_keys(store: &ShardArcEmbeddedStore, keys: &[Vec<u8>]) -> Vec<PreparedKey> {
    keys.iter()
        .map(|key| PreparedKey {
            route: store.route_key(key),
            key: key.clone(),
        })
        .collect()
}

fn warmup_shard_arc_store(store: &ShardArcEmbeddedStore, keys: &[PreparedKey], value: &[u8]) {
    for prepared in keys {
        store.set_slice_routed_no_ttl(prepared.route, &prepared.key, value);
    }
}

fn spawn_shard_arc_internal_load(
    store: Arc<ShardArcEmbeddedStore>,
    keys: Arc<Vec<PreparedKey>>,
    value: Arc<Vec<u8>>,
    mix: Mix,
    key_distribution: KeyDistribution,
    worker_count: usize,
    latency_sample_rate: u64,
) -> InternalRunState {
    let stop = Arc::new(AtomicBool::new(false));
    let measure = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(worker_count);

    for worker_idx in 0..worker_count {
        let store = Arc::clone(&store);
        let keys = Arc::clone(&keys);
        let value = Arc::clone(&value);
        let stop = Arc::clone(&stop);
        let measure = Arc::clone(&measure);
        handles.push(thread::spawn(move || {
            run_shard_arc_internal_worker(
                store,
                keys,
                value,
                worker_idx,
                mix,
                key_distribution,
                stop,
                measure,
                latency_sample_rate,
            )
        }));
    }

    InternalRunState {
        stop,
        measure,
        handles,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_shard_arc_internal_worker(
    store: Arc<ShardArcEmbeddedStore>,
    keys: Arc<Vec<PreparedKey>>,
    value: Arc<Vec<u8>>,
    worker_idx: usize,
    mix: Mix,
    key_distribution: KeyDistribution,
    stop: Arc<AtomicBool>,
    measure: Arc<AtomicBool>,
    latency_sample_rate: u64,
) -> InternalWorkerResult {
    let mut stream = OpStream::new(
        0xA12C_EDED_u64.wrapping_add(worker_idx as u64),
        keys.len(),
        mix,
        key_distribution,
    );
    let latency_clock = FastClock::new();
    let mut all = LatencyHistogram::new();
    let mut reads_hist = LatencyHistogram::new();
    let mut writes_hist = LatencyHistogram::new();
    let mut ops = 0u64;
    let mut reads = 0u64;
    let mut writes = 0u64;
    let mut bytes = 0u64;
    let mut errors = 0u64;

    while !stop.load(Ordering::Relaxed) {
        let (op, key_idx) = stream.next_op();
        let prepared = &keys[key_idx];
        let measured = measure.load(Ordering::Acquire);
        let sample_latency =
            measured && latency_sample_rate != 0 && ops.is_multiple_of(latency_sample_rate);
        let started = sample_latency.then(|| latency_clock.now());
        let ok = match op {
            Op::Get => store.contains_routed_no_ttl(prepared.route, &prepared.key),
            Op::Set => {
                store.set_slice_routed_no_ttl(prepared.route, &prepared.key, value.as_slice());
                true
            }
        };

        if !measured {
            continue;
        }
        if !ok {
            errors += 1;
            continue;
        }
        if let Some(started) = started {
            let elapsed = latency_clock.elapsed_ns(started);
            all.record(elapsed);
            match op {
                Op::Get => reads_hist.record(elapsed),
                Op::Set => writes_hist.record(elapsed),
            }
        }
        ops += 1;
        bytes += value.len() as u64;
        match op {
            Op::Get => reads += 1,
            Op::Set => writes += 1,
        }
    }

    InternalWorkerResult {
        ops,
        reads,
        writes,
        bytes,
        errors,
        all,
        reads_hist,
        writes_hist,
    }
}

fn spawn_internal_load(
    store: Arc<EmbeddedStore>,
    keys: Arc<Vec<PreparedKey>>,
    value: Arc<Vec<u8>>,
    mix: Mix,
    key_distribution: KeyDistribution,
    worker_count: usize,
    latency_sample_rate: u64,
) -> InternalRunState {
    let stop = Arc::new(AtomicBool::new(false));
    let measure = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(worker_count);

    for worker_idx in 0..worker_count {
        let store = Arc::clone(&store);
        let keys = Arc::clone(&keys);
        let value = Arc::clone(&value);
        let stop = Arc::clone(&stop);
        let measure = Arc::clone(&measure);
        handles.push(thread::spawn(move || {
            run_internal_worker(
                store,
                keys,
                value,
                worker_idx,
                mix,
                key_distribution,
                stop,
                measure,
                latency_sample_rate,
            )
        }));
    }

    InternalRunState {
        stop,
        measure,
        handles,
    }
}

#[allow(clippy::too_many_arguments)]
fn run_internal_worker(
    store: Arc<EmbeddedStore>,
    keys: Arc<Vec<PreparedKey>>,
    value: Arc<Vec<u8>>,
    worker_idx: usize,
    mix: Mix,
    key_distribution: KeyDistribution,
    stop: Arc<AtomicBool>,
    measure: Arc<AtomicBool>,
    latency_sample_rate: u64,
) -> InternalWorkerResult {
    let mut stream = OpStream::new(
        0xA11C_EDED_u64.wrapping_add(worker_idx as u64),
        keys.len(),
        mix,
        key_distribution,
    );
    let latency_clock = FastClock::new();
    let mut all = LatencyHistogram::new();
    let mut reads_hist = LatencyHistogram::new();
    let mut writes_hist = LatencyHistogram::new();
    let mut ops = 0u64;
    let mut reads = 0u64;
    let mut writes = 0u64;
    let mut bytes = 0u64;
    let mut errors = 0u64;

    while !stop.load(Ordering::Relaxed) {
        let (op, key_idx) = stream.next_op();
        let prepared = &keys[key_idx];
        let measured = measure.load(Ordering::Acquire);
        let sample_latency =
            measured && latency_sample_rate != 0 && ops.is_multiple_of(latency_sample_rate);
        let started = sample_latency.then(|| latency_clock.now());
        let ok = match op {
            Op::Get => store.get_ref(&prepared.key).is_some(),
            Op::Set => {
                store.set_slice_routed_no_ttl(prepared.route, &prepared.key, &value);
                true
            }
        };

        if !measured {
            continue;
        }
        if !ok {
            errors += 1;
            continue;
        }
        if let Some(started) = started {
            let elapsed = latency_clock.elapsed_ns(started);
            all.record(elapsed);
            match op {
                Op::Get => reads_hist.record(elapsed),
                Op::Set => writes_hist.record(elapsed),
            }
        }
        ops += 1;
        bytes += value.len() as u64;
        match op {
            Op::Get => reads += 1,
            Op::Set => writes += 1,
        }
    }

    InternalWorkerResult {
        ops,
        reads,
        writes,
        bytes,
        errors,
        all,
        reads_hist,
        writes_hist,
    }
}

fn join_internal_load(
    internal: InternalRunState,
    wall: Duration,
    cpu_used: Duration,
) -> InternalRunResult {
    let mut result = InternalRunResult {
        ops: 0,
        reads: 0,
        writes: 0,
        bytes: 0,
        errors: 0,
        wall,
        vcpu_consumed: vcpu(cpu_used, wall),
        all: LatencyHistogram::new(),
        reads_hist: LatencyHistogram::new(),
        writes_hist: LatencyHistogram::new(),
    };

    for handle in internal.handles {
        let worker = handle.join().expect("internal embedded worker panicked");
        result.ops += worker.ops;
        result.reads += worker.reads;
        result.writes += worker.writes;
        result.bytes += worker.bytes;
        result.errors += worker.errors;
        result.all.merge(&worker.all);
        result.reads_hist.merge(&worker.reads_hist);
        result.writes_hist.merge(&worker.writes_hist);
    }
    result
}

fn print_internal_result(result: &InternalRunResult) {
    println!(
        "embedded_mixed_harness: internal ops/sec={:.0} logical_gb/s={:.3} process_vcpu={:.3} p50={} p99={} p999={} reads={} writes={} errors={}",
        result.ops_per_sec(),
        result.gb_per_sec(),
        result.vcpu_consumed,
        format_ns(result.all.p50_ns()),
        format_ns(result.all.p99_ns()),
        format_ns(result.all.p999_ns()),
        result.reads,
        result.writes,
        result.errors
    );
}

fn write_internal_csv(
    args: &Args,
    result: &InternalRunResult,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut csv = CsvWriter::new(
        args.internal_csv.as_ref(),
        vec![
            "mode",
            "bind_addr",
            "shard_count",
            "runtime_threads",
            "internal_workers",
            "value_size",
            "mix",
            "key_count",
            "duration_s",
            "ops_total",
            "ops_per_sec",
            "logical_payload_gb_per_sec",
            "process_vcpu",
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
            "latency_sample_count",
        ],
    );
    let runtime_threads = if args.owner_local {
        1
    } else {
        args.runtime_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(1)
                .clamp(2, 8)
        })
    };
    let mode = if args.owner_local {
        "owner-local-embedded"
    } else if args.shard_arc {
        "shard-arc-embedded"
    } else {
        "mixed-local-embedded"
    };
    csv.write_row(&[
        mode.to_string(),
        args.bind_addr.clone(),
        args.shard_count.to_string(),
        runtime_threads.to_string(),
        args.internal_workers.to_string(),
        args.value_size.to_string(),
        Mix::parse(&args.internal_mix)?.label(),
        args.key_count.to_string(),
        format!("{:.3}", result.wall.as_secs_f64()),
        result.ops.to_string(),
        format!("{:.0}", result.ops_per_sec()),
        format!("{:.3}", result.gb_per_sec()),
        format!("{:.3}", result.vcpu_consumed),
        result.all.p50_ns().to_string(),
        result.all.p99_ns().to_string(),
        result.all.p999_ns().to_string(),
        result.reads.to_string(),
        result.reads_hist.p99_ns().to_string(),
        result.reads_hist.p999_ns().to_string(),
        result.writes.to_string(),
        result.writes_hist.p99_ns().to_string(),
        result.writes_hist.p999_ns().to_string(),
        result.errors.to_string(),
        result.all.count().to_string(),
    ])?;
    Ok(())
}

impl InternalRunResult {
    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.wall.as_secs_f64()
    }

    fn gb_per_sec(&self) -> f64 {
        (self.bytes as f64 / 1e9) / self.wall.as_secs_f64()
    }
}
