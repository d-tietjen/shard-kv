//! Semantic cache benchmark harness.
//!
//! This mirrors the public BetterDB-vs-RedisVL benchmark shape: pairwise
//! quality sweeps across cosine-distance thresholds plus unique/cycling lookup
//! latency. Embeddings are supplied or generated ahead of time so this binary
//! measures shardmap's native semantic cache path, not a Python embedding model.

use std::error::Error;
use std::fs::File;
use std::hint::black_box;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use shardcache_benchmarks::cpu::{process_cpu_time, vcpu};
use shardcache_benchmarks::csv::CsvWriter;
use shardcache_benchmarks::histogram::LatencyHistogram;
use shardmap::ShardMapWithShards;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

const DEFAULT_THRESHOLDS: &str = "0.05,0.10,0.15,0.20,0.25,0.30,0.35,0.40,0.45";

#[derive(Debug, Parser)]
#[command(about = "Semantic cache quality and latency benchmark")]
struct Args {
    /// Optional precomputed pair CSV.
    ///
    /// Supported columns are either
    /// `dataset,pair_id,label,cache_embedding,query_embedding` or
    /// `pair_id,label,cache_embedding,query_embedding`. Embedding components
    /// are separated by whitespace, `;`, or `|`.
    #[arg(long)]
    pairs_csv: Option<String>,

    /// Optional path to write the loaded/generated pair fixture as CSV.
    #[arg(long)]
    pairs_output_csv: Option<String>,

    /// Dataset label used for generated synthetic pairs or CSV rows without a dataset column.
    #[arg(long, default_value = "synthetic")]
    dataset: String,

    /// Number of synthetic pairs to generate when --pairs-csv is omitted.
    #[arg(long, default_value_t = 5_000)]
    pairs: usize,

    /// Synthetic embedding dimensionality.
    #[arg(long, default_value_t = 384)]
    dims: usize,

    /// Number of entries in the latency benchmark index.
    #[arg(long, default_value_t = 5_000)]
    index_entries: usize,

    /// Number of cache stripes to use for shardcache runs.
    #[arg(long, default_value_t = 64)]
    cache_shards: usize,

    /// Number of measured latency lookups.
    #[arg(long, default_value_t = 200)]
    measured_queries: usize,

    /// Warmup lookups before latency measurement.
    #[arg(long, default_value_t = 50)]
    warmup_queries: usize,

    /// Reused query vector pool size for cycling latency.
    #[arg(long, default_value_t = 64)]
    cycling_queries: usize,

    /// Cosine-distance thresholds to sweep.
    #[arg(long, default_value = DEFAULT_THRESHOLDS)]
    thresholds: String,

    /// Cosine-distance threshold used for latency runs.
    #[arg(long, default_value_t = 0.35)]
    latency_threshold: f32,

    /// Stored value size in bytes.
    #[arg(long, default_value_t = 64)]
    value_size: usize,

    /// Random seed used for synthetic pairs and latency query generation.
    #[arg(long, default_value_t = 0x5eed)]
    seed: u64,

    /// Which benchmark sections to run.
    #[arg(long, default_value = "all")]
    mode: RunMode,

    /// Optional quality CSV output path.
    #[arg(long)]
    quality_csv: Option<String>,

    /// Optional latency CSV output path.
    #[arg(long)]
    latency_csv: Option<String>,

    /// Worker threads for hot-query load mode. Zero disables the load run.
    #[arg(long, default_value_t = 0)]
    load_workers: usize,

    /// Duration for hot-query load mode.
    #[arg(long, default_value_t = 5)]
    load_seconds: u64,

    /// Reused query vector pool size for hot-query load mode.
    #[arg(long, default_value_t = 64)]
    load_query_pool: usize,

    /// Warmup lookups before hot-query load mode.
    #[arg(long, default_value_t = 64)]
    load_warmup_queries: usize,

    /// Consume load queries from one shared cursor so each query is used at most once.
    #[arg(long)]
    load_unique_queries: bool,

    /// Use stored entry embeddings as load queries so the load run is all hits.
    #[arg(long)]
    load_exact_hits: bool,

    /// Use independent random unit vectors as load queries so the load run is cold misses.
    #[arg(long)]
    load_miss_random: bool,

    /// Disable shardcache's exact semantic query result cache.
    #[arg(long)]
    disable_semantic_query_cache: bool,

    /// Optional hot-query load CSV output path.
    #[arg(long)]
    load_csv: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum RunMode {
    All,
    Quality,
    Latency,
    Load,
}

#[derive(Debug, Clone)]
struct Pair {
    dataset: String,
    pair_id: String,
    label: bool,
    cache_embedding: Vec<f32>,
    query_embedding: Vec<f32>,
}

#[derive(Debug, Clone)]
struct QualityRow {
    dataset: String,
    threshold_distance: f32,
    min_score: f32,
    pairs: usize,
    tp: u64,
    fp: u64,
    tn: u64,
    fn_: u64,
}

impl QualityRow {
    fn precision(&self) -> f64 {
        ratio(self.tp, self.tp.saturating_add(self.fp))
    }

    fn recall(&self) -> f64 {
        ratio(self.tp, self.tp.saturating_add(self.fn_))
    }

    fn f1(&self) -> f64 {
        let precision = self.precision();
        let recall = self.recall();
        if precision + recall == 0.0 {
            0.0
        } else {
            2.0 * precision * recall / (precision + recall)
        }
    }

    fn hit_rate(&self) -> f64 {
        ratio(self.tp.saturating_add(self.fp), self.pairs as u64)
    }

    fn fpr(&self) -> f64 {
        ratio(self.fp, self.fp.saturating_add(self.tn))
    }
}

#[derive(Debug, Clone)]
struct LatencyRow {
    dataset: String,
    mode: &'static str,
    index_entries: usize,
    dims: usize,
    threshold_distance: f32,
    queries: usize,
    warmup: usize,
    hits: u64,
    total_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Debug, Clone)]
struct LoadRow {
    dataset: String,
    mode: &'static str,
    workers: usize,
    index_entries: usize,
    dims: usize,
    threshold_distance: f32,
    duration_ms: f64,
    queries: u64,
    hits: u64,
    ops_per_sec: f64,
    ops_per_cpu: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    process_cpu_seconds: f64,
    process_vcpu: f64,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    if args.pairs == 0 {
        return Err("--pairs must be greater than zero".into());
    }
    if args.dims == 0 {
        return Err("--dims must be greater than zero".into());
    }
    if args.index_entries == 0 {
        return Err("--index-entries must be greater than zero".into());
    }
    if args.measured_queries == 0 {
        return Err("--measured-queries must be greater than zero".into());
    }
    if args.cycling_queries == 0 {
        return Err("--cycling-queries must be greater than zero".into());
    }
    if args.load_workers > 0 && args.load_query_pool == 0 {
        return Err("--load-query-pool must be greater than zero when load mode is enabled".into());
    }
    if args.load_workers > 0 && args.load_seconds == 0 {
        return Err("--load-seconds must be greater than zero when load mode is enabled".into());
    }
    if args.load_exact_hits && args.load_miss_random {
        return Err("--load-exact-hits and --load-miss-random are mutually exclusive".into());
    }

    let thresholds = parse_thresholds(&args.thresholds)?;
    let pairs = match &args.pairs_csv {
        Some(path) => load_pairs_csv(path, &args.dataset)?,
        None => generate_pairs(args.pairs, args.dims, args.seed, &args.dataset),
    };
    if let Some(path) = &args.pairs_output_csv {
        write_pairs_csv(path, &pairs)?;
    }
    let dims = pairs
        .first()
        .map(|pair| pair.cache_embedding.len())
        .unwrap_or(args.dims);

    println!(
        "semantic-cache-matrix: dataset={} pairs={} dims={} thresholds={} mode={:?}",
        args.dataset,
        pairs.len(),
        dims,
        args.thresholds,
        args.mode
    );

    match args.cache_shards {
        1 => run_selected::<1>(&pairs, &thresholds, &args, dims),
        2 => run_selected::<2>(&pairs, &thresholds, &args, dims),
        4 => run_selected::<4>(&pairs, &thresholds, &args, dims),
        8 => run_selected::<8>(&pairs, &thresholds, &args, dims),
        16 => run_selected::<16>(&pairs, &thresholds, &args, dims),
        32 => run_selected::<32>(&pairs, &thresholds, &args, dims),
        64 => run_selected::<64>(&pairs, &thresholds, &args, dims),
        128 => run_selected::<128>(&pairs, &thresholds, &args, dims),
        _ => Err("--cache-shards must be one of 1,2,4,8,16,32,64,128".into()),
    }
}

fn run_selected<const SHARDS: usize>(
    pairs: &[Pair],
    thresholds: &[f32],
    args: &Args,
    dims: usize,
) -> Result<(), BoxError> {
    if matches!(args.mode, RunMode::All | RunMode::Quality) {
        run_quality::<SHARDS>(pairs, thresholds, args.quality_csv.as_deref())?;
    }
    if matches!(args.mode, RunMode::All | RunMode::Latency) {
        run_latency::<SHARDS>(pairs, args, dims, args.latency_csv.as_deref())?;
    }
    if matches!(args.mode, RunMode::Load) || args.load_workers > 0 {
        run_load::<SHARDS>(pairs, args, dims, args.load_csv.as_deref())?;
    }

    Ok(())
}

fn run_quality<const SHARDS: usize>(
    pairs: &[Pair],
    thresholds: &[f32],
    csv_path: Option<&str>,
) -> Result<(), BoxError> {
    let mut csv = CsvWriter::new(
        csv_path,
        vec![
            "dataset",
            "adapter",
            "mode",
            "pairs",
            "threshold_distance",
            "min_score",
            "tp",
            "fp",
            "tn",
            "fn",
            "precision",
            "recall",
            "f1",
            "hit_rate",
            "fpr",
        ],
    );

    println!("quality:");
    println!(
        "| {:<20} | {:>9} | {:>7} | {:>7} | {:>7} | {:>7} | {:>8} | {:>8} | {:>8} |",
        "dataset", "distance", "F1", "prec", "recall", "hit", "FPR", "TP", "FP"
    );
    println!(
        "| {:-<20} | {:-<9} | {:-<7} | {:-<7} | {:-<7} | {:-<7} | {:-<8} | {:-<8} | {:-<8} |",
        "", "", "", "", "", "", "", "", ""
    );

    for threshold in thresholds.iter().copied() {
        let min_score = cosine_distance_to_min_score(threshold)?;
        let mut row = QualityRow {
            dataset: pairs
                .first()
                .map(|pair| pair.dataset.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            threshold_distance: threshold,
            min_score,
            pairs: pairs.len(),
            tp: 0,
            fp: 0,
            tn: 0,
            fn_: 0,
        };

        for pair in pairs {
            let cache = ShardMapWithShards::<SHARDS>::new();
            cache.insert_semantic_slice(
                pair.pair_id.as_bytes(),
                b"Answer: synthetic",
                &pair.cache_embedding,
            )?;
            let hit = cache
                .semantic_search(&pair.query_embedding, min_score)?
                .is_some();
            match (pair.label, hit) {
                (true, true) => row.tp = row.tp.saturating_add(1),
                (false, true) => row.fp = row.fp.saturating_add(1),
                (false, false) => row.tn = row.tn.saturating_add(1),
                (true, false) => row.fn_ = row.fn_.saturating_add(1),
            }
        }

        println!(
            "| {:<20} | {:>9.2} | {:>7.4} | {:>7.4} | {:>7.4} | {:>7.4} | {:>8.4} | {:>8} | {:>8} |",
            row.dataset,
            row.threshold_distance,
            row.f1(),
            row.precision(),
            row.recall(),
            row.hit_rate(),
            row.fpr(),
            row.tp,
            row.fp
        );
        csv.write_row(&[
            row.dataset.clone(),
            "shardmap".to_string(),
            "bare".to_string(),
            row.pairs.to_string(),
            format!("{:.6}", row.threshold_distance),
            format!("{:.6}", row.min_score),
            row.tp.to_string(),
            row.fp.to_string(),
            row.tn.to_string(),
            row.fn_.to_string(),
            format!("{:.6}", row.precision()),
            format!("{:.6}", row.recall()),
            format!("{:.6}", row.f1()),
            format!("{:.6}", row.hit_rate()),
            format!("{:.6}", row.fpr()),
        ])?;
    }

    Ok(())
}

fn run_latency<const SHARDS: usize>(
    pairs: &[Pair],
    args: &Args,
    dims: usize,
    csv_path: Option<&str>,
) -> Result<(), BoxError> {
    let min_score = cosine_distance_to_min_score(args.latency_threshold)?;
    let mut csv = CsvWriter::new(
        csv_path,
        vec![
            "dataset",
            "adapter",
            "mode",
            "index_entries",
            "dims",
            "threshold_distance",
            "queries",
            "warmup",
            "hits",
            "total_ms",
            "p50_ms",
            "p95_ms",
            "p99_ms",
        ],
    );

    let cache = build_latency_cache::<SHARDS>(pairs, args.index_entries, args.value_size)?;
    if args.disable_semantic_query_cache {
        cache.disable_semantic_query_cache();
    }
    let unique = build_unique_queries(pairs, args.measured_queries + args.warmup_queries, dims);
    let cycling = build_cycling_queries(
        pairs,
        args.measured_queries + args.warmup_queries,
        args.cycling_queries,
        dims,
    );

    println!("latency:");
    println!(
        "| {:<8} | {:>7} | {:>7} | {:>7} | {:>9} | {:>9} | {:>9} | {:>8} |",
        "mode", "entries", "dims", "queries", "p50 ms", "p95 ms", "p99 ms", "hits"
    );
    println!(
        "| {:-<8} | {:-<7} | {:-<7} | {:-<7} | {:-<9} | {:-<9} | {:-<9} | {:-<8} |",
        "", "", "", "", "", "", "", ""
    );

    let dataset = pairs
        .first()
        .map(|pair| pair.dataset.as_str())
        .unwrap_or(args.dataset.as_str());
    for (mode, queries) in [
        ("unique", unique.as_slice()),
        ("cycling", cycling.as_slice()),
    ] {
        let row = measure_latency(
            mode,
            &cache,
            queries,
            args.warmup_queries,
            min_score,
            dataset,
            args,
            dims,
        )?;
        println!(
            "| {:<8} | {:>7} | {:>7} | {:>7} | {:>9.4} | {:>9.4} | {:>9.4} | {:>8} |",
            row.mode,
            row.index_entries,
            row.dims,
            row.queries,
            row.p50_ms,
            row.p95_ms,
            row.p99_ms,
            row.hits
        );
        csv.write_row(&[
            row.dataset,
            "shardmap".to_string(),
            row.mode.to_string(),
            row.index_entries.to_string(),
            row.dims.to_string(),
            format!("{:.6}", row.threshold_distance),
            row.queries.to_string(),
            row.warmup.to_string(),
            row.hits.to_string(),
            format!("{:.6}", row.total_ms),
            format!("{:.6}", row.p50_ms),
            format!("{:.6}", row.p95_ms),
            format!("{:.6}", row.p99_ms),
        ])?;
    }

    Ok(())
}

fn run_load<const SHARDS: usize>(
    pairs: &[Pair],
    args: &Args,
    dims: usize,
    csv_path: Option<&str>,
) -> Result<(), BoxError> {
    if args.load_workers == 0 {
        return Ok(());
    }

    let min_score = cosine_distance_to_min_score(args.latency_threshold)?;
    let mut csv = CsvWriter::new(
        csv_path,
        vec![
            "dataset",
            "adapter",
            "mode",
            "workers",
            "index_entries",
            "dims",
            "threshold_distance",
            "duration_ms",
            "queries",
            "hits",
            "ops_per_sec",
            "ops_per_cpu",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "process_cpu_seconds",
            "process_vcpu",
            "sut_cpu_seconds",
            "sut_vcpu",
            "client_cpu_seconds",
            "client_vcpu",
            "ops_per_sut_cpu",
            "ops_per_total_cpu",
        ],
    );

    let cache = build_latency_cache::<SHARDS>(pairs, args.index_entries, args.value_size)?;
    if args.disable_semantic_query_cache {
        cache.disable_semantic_query_cache();
    }
    let query_pool = Arc::new(if args.load_miss_random {
        build_miss_queries(args.load_query_pool, dims, args.seed ^ 0xbad5eed)
    } else if args.load_exact_hits {
        build_exact_queries(pairs, args.load_query_pool, dims)
    } else {
        build_cycling_queries(pairs, args.load_query_pool, args.load_query_pool, dims)
    });
    for query in query_pool.iter().take(args.load_warmup_queries) {
        black_box(cache.semantic_search(query, min_score)?);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let shared_query_cursor = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(args.load_workers);
    let cpu_start = process_cpu_time();
    let start = Instant::now();
    for worker_id in 0..args.load_workers {
        let cache = cache.clone();
        let query_pool = Arc::clone(&query_pool);
        let stop = Arc::clone(&stop);
        let shared_query_cursor = Arc::clone(&shared_query_cursor);
        let unique_queries = args.load_unique_queries;
        handles.push(thread::spawn(
            move || -> Result<(u64, u64, LatencyHistogram), BoxError> {
                let mut histogram = LatencyHistogram::new();
                let mut queries = 0u64;
                let mut hits = 0u64;
                let mut index = worker_id % query_pool.len();
                while !stop.load(Ordering::Relaxed) {
                    let query = if unique_queries {
                        let next = shared_query_cursor.fetch_add(1, Ordering::Relaxed);
                        let Some(query) = query_pool.get(next) else {
                            break;
                        };
                        query
                    } else {
                        let query = &query_pool[index];
                        index += 1;
                        if index == query_pool.len() {
                            index = 0;
                        }
                        query
                    };

                    let start = Instant::now();
                    let hit = cache.semantic_search(query, min_score)?;
                    histogram.record(start.elapsed().as_nanos() as u64);
                    queries = queries.saturating_add(1);
                    if hit.is_some() {
                        hits = hits.saturating_add(1);
                    }
                    black_box(hit);
                }
                Ok((queries, hits, histogram))
            },
        ));
    }

    let load_duration = Duration::from_secs(args.load_seconds);
    if args.load_unique_queries {
        while start.elapsed() < load_duration
            && shared_query_cursor.load(Ordering::Relaxed) < query_pool.len()
        {
            thread::sleep(Duration::from_millis(1));
        }
    } else {
        thread::sleep(load_duration);
    }
    stop.store(true, Ordering::Relaxed);

    let mut total_queries = 0u64;
    let mut total_hits = 0u64;
    let mut histogram = LatencyHistogram::new();
    for handle in handles {
        let (queries, hits, worker_histogram) =
            handle.join().map_err(|_| "load worker panicked")??;
        total_queries = total_queries.saturating_add(queries);
        total_hits = total_hits.saturating_add(hits);
        histogram.merge(&worker_histogram);
    }
    let duration = start.elapsed();
    let process_cpu = process_cpu_time().saturating_sub(cpu_start);
    let ops_per_sec = total_queries as f64 / duration.as_secs_f64();
    let process_vcpu = vcpu(process_cpu, duration);
    let dataset = pairs
        .first()
        .map(|pair| pair.dataset.clone())
        .unwrap_or_else(|| args.dataset.clone());
    let row = LoadRow {
        dataset,
        mode: if args.load_miss_random {
            "miss-random"
        } else if args.load_unique_queries {
            "unique-stream"
        } else {
            "hot-cycling"
        },
        workers: args.load_workers,
        index_entries: args.index_entries,
        dims,
        threshold_distance: args.latency_threshold,
        duration_ms: duration.as_secs_f64() * 1_000.0,
        queries: total_queries,
        hits: total_hits,
        ops_per_sec,
        ops_per_cpu: if process_vcpu > 0.0 {
            ops_per_sec / process_vcpu
        } else {
            0.0
        },
        p50_ms: histogram.p50_ns() as f64 / 1_000_000.0,
        p95_ms: histogram.p95_ns() as f64 / 1_000_000.0,
        p99_ms: histogram.p99_ns() as f64 / 1_000_000.0,
        process_cpu_seconds: process_cpu.as_secs_f64(),
        process_vcpu,
    };

    println!("load:");
    println!(
        "| {:<11} | {:>7} | {:>7} | {:>10} | {:>10} | {:>9} | {:>9} | {:>9} | {:>8} | {:>10} |",
        "mode",
        "workers",
        "entries",
        "ops/sec",
        "ops/sut-cpu",
        "p50 ms",
        "p95 ms",
        "p99 ms",
        "vCPU",
        "hits"
    );
    println!(
        "| {:-<11} | {:-<7} | {:-<7} | {:-<10} | {:-<10} | {:-<9} | {:-<9} | {:-<9} | {:-<8} | {:-<10} |",
        "", "", "", "", "", "", "", "", "", ""
    );
    println!(
        "| {:<11} | {:>7} | {:>7} | {:>10.0} | {:>10.0} | {:>9.4} | {:>9.4} | {:>9.4} | {:>8.2} | {:>10} |",
        row.mode,
        row.workers,
        row.index_entries,
        row.ops_per_sec,
        row.ops_per_cpu,
        row.p50_ms,
        row.p95_ms,
        row.p99_ms,
        row.process_vcpu,
        row.hits
    );
    csv.write_row(&[
        row.dataset,
        "shardmap".to_string(),
        row.mode.to_string(),
        row.workers.to_string(),
        row.index_entries.to_string(),
        row.dims.to_string(),
        format!("{:.6}", row.threshold_distance),
        format!("{:.6}", row.duration_ms),
        row.queries.to_string(),
        row.hits.to_string(),
        format!("{:.6}", row.ops_per_sec),
        format!("{:.6}", row.ops_per_cpu),
        format!("{:.6}", row.p50_ms),
        format!("{:.6}", row.p95_ms),
        format!("{:.6}", row.p99_ms),
        format!("{:.6}", row.process_cpu_seconds),
        format!("{:.6}", row.process_vcpu),
        format!("{:.6}", row.process_cpu_seconds),
        format!("{:.6}", row.process_vcpu),
        "0.000000".to_string(),
        "0.000000".to_string(),
        format!("{:.6}", row.ops_per_cpu),
        format!("{:.6}", row.ops_per_cpu),
    ])?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn measure_latency<const SHARDS: usize>(
    mode: &'static str,
    cache: &ShardMapWithShards<SHARDS>,
    queries: &[Vec<f32>],
    warmup: usize,
    min_score: f32,
    dataset: &str,
    args: &Args,
    dims: usize,
) -> Result<LatencyRow, BoxError> {
    for query in queries.iter().take(warmup) {
        black_box(cache.semantic_search(query, min_score)?);
    }

    let mut hist = LatencyHistogram::new();
    let mut hits = 0u64;
    let start_total = Instant::now();
    for query in queries.iter().skip(warmup) {
        let start = Instant::now();
        let hit = cache.semantic_search(query, min_score)?;
        let elapsed = start.elapsed();
        hist.record(elapsed.as_nanos() as u64);
        if hit.is_some() {
            hits = hits.saturating_add(1);
        }
        black_box(hit);
    }
    let total = start_total.elapsed();

    Ok(LatencyRow {
        dataset: dataset.to_string(),
        mode,
        index_entries: args.index_entries,
        dims,
        threshold_distance: args.latency_threshold,
        queries: args.measured_queries,
        warmup,
        hits,
        total_ms: total.as_secs_f64() * 1_000.0,
        p50_ms: hist.p50_ns() as f64 / 1_000_000.0,
        p95_ms: hist.p95_ns() as f64 / 1_000_000.0,
        p99_ms: hist.p99_ns() as f64 / 1_000_000.0,
    })
}

fn build_latency_cache<const SHARDS: usize>(
    pairs: &[Pair],
    index_entries: usize,
    value_size: usize,
) -> Result<ShardMapWithShards<SHARDS>, BoxError> {
    let cache = ShardMapWithShards::<SHARDS>::with_capacity(index_entries);
    let value = vec![b'x'; value_size];
    for index in 0..index_entries {
        let pair = &pairs[index % pairs.len()];
        let key = format!("semantic:{index:016x}");
        cache.insert_semantic_slice(key.as_bytes(), &value, &pair.cache_embedding)?;
    }
    Ok(cache)
}

fn build_unique_queries(pairs: &[Pair], count: usize, dims: usize) -> Vec<Vec<f32>> {
    let mut queries = Vec::with_capacity(count);
    for index in 0..count {
        if let Some(pair) = pairs.get(index % pairs.len()) {
            queries.push(pair.query_embedding.clone());
        }
    }
    if queries.is_empty() {
        queries.push(vec![1.0; dims]);
    }
    queries
}

fn build_exact_queries(pairs: &[Pair], count: usize, dims: usize) -> Vec<Vec<f32>> {
    let mut queries = Vec::with_capacity(count);
    for index in 0..count {
        if let Some(pair) = pairs.get(index % pairs.len()) {
            queries.push(pair.cache_embedding.clone());
        }
    }
    if queries.is_empty() {
        queries.push(vec![1.0; dims]);
    }
    queries
}

fn build_miss_queries(count: usize, dims: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|_| random_unit_vector(dims, &mut rng))
        .collect()
}

fn build_cycling_queries(
    pairs: &[Pair],
    count: usize,
    cycling_queries: usize,
    dims: usize,
) -> Vec<Vec<f32>> {
    let unique = build_unique_queries(pairs, cycling_queries, dims);
    (0..count)
        .map(|index| unique[index % unique.len()].clone())
        .collect()
}

fn load_pairs_csv(path: &str, default_dataset: &str) -> Result<Vec<Pair>, BoxError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut pairs = Vec::new();

    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
        if pairs.is_empty()
            && fields
                .iter()
                .any(|field| field.eq_ignore_ascii_case("label"))
        {
            continue;
        }
        let (dataset, pair_id, label, cache_embedding, query_embedding) = match fields.as_slice() {
            [
                dataset,
                pair_id,
                label,
                cache_embedding,
                query_embedding,
                ..,
            ] => (
                (*dataset).to_string(),
                (*pair_id).to_string(),
                parse_label(label)?,
                parse_embedding(cache_embedding)?,
                parse_embedding(query_embedding)?,
            ),
            [pair_id, label, cache_embedding, query_embedding] => (
                default_dataset.to_string(),
                (*pair_id).to_string(),
                parse_label(label)?,
                parse_embedding(cache_embedding)?,
                parse_embedding(query_embedding)?,
            ),
            _ => {
                return Err(format!(
                    "{path}:{} expected 4 or 5 CSV fields, got {}",
                    line_no + 1,
                    fields.len()
                )
                .into());
            }
        };
        if cache_embedding.len() != query_embedding.len() {
            return Err(format!(
                "{path}:{} embedding dimension mismatch: {} vs {}",
                line_no + 1,
                cache_embedding.len(),
                query_embedding.len()
            )
            .into());
        }
        pairs.push(Pair {
            dataset,
            pair_id,
            label,
            cache_embedding,
            query_embedding,
        });
    }

    if pairs.is_empty() {
        return Err(format!("{path} did not contain any pairs").into());
    }

    Ok(pairs)
}

fn write_pairs_csv(path: &str, pairs: &[Pair]) -> Result<(), BoxError> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(
        writer,
        "dataset,pair_id,label,cache_embedding,query_embedding"
    )?;
    for pair in pairs {
        write!(
            writer,
            "{},{},{},{},",
            pair.dataset,
            pair.pair_id,
            if pair.label { "1" } else { "0" },
            format_embedding(&pair.cache_embedding)
        )?;
        writeln!(writer, "{}", format_embedding(&pair.query_embedding))?;
    }
    Ok(())
}

fn format_embedding(values: &[f32]) -> String {
    let mut out = String::with_capacity(values.len().saturating_mul(12));
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(';');
        }
        out.push_str(&format!("{value:.8}"));
    }
    out
}

fn generate_pairs(count: usize, dims: usize, seed: u64, dataset: &str) -> Vec<Pair> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut pairs = Vec::with_capacity(count);
    for index in 0..count {
        let label = index % 2 == 0;
        let cache_embedding = random_unit_vector(dims, &mut rng);
        let target_score = match label {
            true => rng.gen_range(0.62..0.98),
            false => rng.gen_range(0.42..0.82),
        };
        let query_embedding = vector_with_cosine(&cache_embedding, target_score, &mut rng);
        pairs.push(Pair {
            dataset: dataset.to_string(),
            pair_id: format!("pair:{index:016x}"),
            label,
            cache_embedding,
            query_embedding,
        });
    }
    pairs
}

fn random_unit_vector(dims: usize, rng: &mut SmallRng) -> Vec<f32> {
    let mut values = (0..dims)
        .map(|_| rng.gen_range(-1.0f32..1.0f32))
        .collect::<Vec<_>>();
    normalize(&mut values);
    values
}

fn vector_with_cosine(base: &[f32], score: f32, rng: &mut SmallRng) -> Vec<f32> {
    let mut orth = random_unit_vector(base.len(), rng);
    let dot = dot(base, &orth);
    for (component, base_component) in orth.iter_mut().zip(base.iter()) {
        *component -= dot * base_component;
    }
    normalize(&mut orth);

    let orth_scale = (1.0 - score * score).max(0.0).sqrt();
    let mut query = base
        .iter()
        .zip(orth.iter())
        .map(|(base_component, orth_component)| {
            score * base_component + orth_scale * orth_component
        })
        .collect::<Vec<_>>();
    normalize(&mut query);
    query
}

fn normalize(values: &mut [f32]) {
    let norm = values
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if norm == 0.0 {
        return;
    }
    for value in values {
        *value = (f64::from(*value) / norm) as f32;
    }
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum()
}

fn parse_thresholds(value: &str) -> Result<Vec<f32>, BoxError> {
    let mut thresholds = Vec::new();
    for part in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let threshold = part
            .parse::<f32>()
            .map_err(|error| format!("invalid threshold `{part}`: {error}"))?;
        cosine_distance_to_min_score(threshold)?;
        thresholds.push(threshold);
    }
    if thresholds.is_empty() {
        return Err("at least one threshold is required".into());
    }
    Ok(thresholds)
}

fn cosine_distance_to_min_score(distance: f32) -> Result<f32, BoxError> {
    if !distance.is_finite() || !(0.0..=2.0).contains(&distance) {
        return Err(
            format!("cosine distance threshold must be finite and in [0,2]: {distance}").into(),
        );
    }
    Ok(1.0 - distance)
}

fn parse_label(value: &str) -> Result<bool, BoxError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "positive" | "pos" | "match" => Ok(true),
        "0" | "false" | "no" | "negative" | "neg" | "miss" => Ok(false),
        other => Err(format!("invalid label `{other}`").into()),
    }
}

fn parse_embedding(value: &str) -> Result<Vec<f32>, BoxError> {
    let trimmed = value.trim().trim_start_matches('[').trim_end_matches(']');
    let embedding = trimmed
        .split(|ch: char| ch.is_ascii_whitespace() || ch == ';' || ch == '|')
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<f32>()
                .map_err(|error| format!("invalid embedding component `{part}`: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if embedding.is_empty() {
        return Err("embedding cannot be empty".into());
    }
    Ok(embedding)
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_pair_cosine_tracks_target_score() {
        let mut rng = SmallRng::seed_from_u64(7);
        let base = random_unit_vector(128, &mut rng);
        let query = vector_with_cosine(&base, 0.75, &mut rng);
        assert!((dot(&base, &query) - 0.75).abs() < 0.001);
    }

    #[test]
    fn parses_embedding_components() {
        assert_eq!(
            parse_embedding("[1 2;3|4]").unwrap(),
            vec![1.0, 2.0, 3.0, 4.0]
        );
    }
}
