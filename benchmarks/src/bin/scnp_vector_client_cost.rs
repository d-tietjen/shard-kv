//! End-to-end cost of the typed SCNP vector client used by Object RAG.

use std::error::Error;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use shardcache_benchmarks::cpu::{process_cpu_time, vcpu};
use shardcache_benchmarks::csv::CsvWriter;
use shardcache_benchmarks::histogram::LatencyHistogram;
use shardcache_client_rs::{
    ShardCacheClient, ShardCacheDirectRouter, ShardCacheDirectShardClient, VAddOptions, VSimOptions,
};

type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, Parser)]
#[command(about = "Benchmark typed SCNP VADD/VSIM/VREM client operations")]
struct Args {
    /// Fanout listener or first direct-shard address.
    #[arg(long, default_value = "127.0.0.1:6380")]
    addr: String,

    #[arg(long, default_value = "fanout")]
    transport: Transport,

    /// Required server shard count for direct-shard transport.
    #[arg(long, default_value_t = 1)]
    shard_count: usize,

    #[arg(long, default_value_t = 1)]
    workers: usize,

    #[arg(long, default_value_t = 1_024)]
    entries: usize,

    #[arg(long, alias = "dimensions", default_value_t = 16)]
    dims: usize,

    #[arg(long, default_value_t = 64)]
    query_pool: usize,

    #[arg(long, default_value_t = 10)]
    count: usize,

    #[arg(long, default_value_t = 64)]
    ef_search: usize,

    #[arg(long, alias = "warmup", default_value_t = 1)]
    warmup_seconds: u64,

    #[arg(long, alias = "duration", default_value_t = 5)]
    duration_seconds: u64,

    /// Resolve the authentication token from this environment variable.
    #[arg(long)]
    auth_token_env: Option<String>,

    #[arg(long)]
    csv: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Transport {
    Fanout,
    DirectShard,
}

impl Transport {
    fn label(self) -> &'static str {
        match self {
            Self::Fanout => "typed-scnp-fanout",
            Self::DirectShard => "typed-scnp-direct",
        }
    }
}

enum VectorClient {
    Fanout(ShardCacheClient),
    Direct(ShardCacheDirectShardClient),
}

impl VectorClient {
    fn connect(
        transport: Transport,
        addr: &str,
        shard_count: usize,
        auth_token: Option<&[u8]>,
    ) -> Result<Self, BoxError> {
        let timeout = Duration::from_secs(5);
        match transport {
            Transport::Fanout => Ok(Self::Fanout(
                ShardCacheClient::connect_with_timeouts_and_auth(
                    addr, timeout, timeout, auth_token,
                )?,
            )),
            Transport::DirectShard => {
                let router = ShardCacheDirectRouter::new(addr, shard_count)?;
                Ok(Self::Direct(router.connect_shard_with_timeouts_and_auth(
                    0, timeout, timeout, auth_token,
                )?))
            }
        }
    }

    fn vadd(
        &mut self,
        key: &[u8],
        element: &[u8],
        vector: &[f32],
        attributes: &[u8],
    ) -> Result<bool, BoxError> {
        let options = VAddOptions::new().attributes(attributes);
        Ok(match self {
            Self::Fanout(client) => client.vadd(key, element, vector, options)?,
            Self::Direct(client) => client.vadd(key, element, vector, options)?,
        })
    }

    fn vsim(
        &mut self,
        key: &[u8],
        vector: &[f32],
        count: usize,
        ef_search: usize,
    ) -> Result<usize, BoxError> {
        let options = VSimOptions::new().count(count).ef_search(ef_search);
        Ok(match self {
            Self::Fanout(client) => client.vsim(key, vector, options)?.len(),
            Self::Direct(client) => client.vsim(key, vector, options)?.len(),
        })
    }
}

struct WorkerResult {
    operations: u64,
    hits: u64,
    latency: LatencyHistogram,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    validate(&args)?;
    let auth_token = args
        .auth_token_env
        .as_deref()
        .map(std::env::var)
        .transpose()?
        .map(String::into_bytes)
        .map(Arc::<[u8]>::from);
    let stop = Arc::new(AtomicBool::new(false));
    let run_nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let measured = Arc::new(Barrier::new(args.workers + 1));
    let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();
    let mut handles = Vec::with_capacity(args.workers);

    for worker_id in 0..args.workers {
        let addr = args.addr.clone();
        let stop = Arc::clone(&stop);
        let measured = Arc::clone(&measured);
        let ready_tx = ready_tx.clone();
        let auth_token = auth_token.clone();
        let transport = args.transport;
        let shard_count = args.shard_count;
        let entries = args.entries;
        let dims = args.dims;
        let query_pool = args.query_pool;
        let count = args.count;
        let ef_search = args.ef_search;
        let warmup = Duration::from_secs(args.warmup_seconds);
        handles.push(thread::spawn(move || -> Result<WorkerResult, BoxError> {
            let prepared = (|| -> Result<_, BoxError> {
                let mut client =
                    VectorClient::connect(transport, &addr, shard_count, auth_token.as_deref())?;
                let key = format!("typed-object-rag:{run_nonce}:{worker_id}").into_bytes();
                let vectors = (0..entries)
                    .map(|index| deterministic_vector(index, dims))
                    .collect::<Vec<_>>();
                for (index, vector) in vectors.iter().enumerate() {
                    let element = format!("doc:{index:08}");
                    let attributes = format!(r#"{{"group":{},"source":"bench"}}"#, index % 4);
                    black_box(client.vadd(
                        &key,
                        element.as_bytes(),
                        vector,
                        attributes.as_bytes(),
                    )?);
                }

                let queries = vectors.iter().take(query_pool).cloned().collect::<Vec<_>>();
                let warmup_deadline = Instant::now() + warmup;
                let mut cursor = 0usize;
                while Instant::now() < warmup_deadline {
                    black_box(client.vsim(&key, &queries[cursor], count, ef_search)?);
                    cursor = (cursor + 1) % queries.len();
                }
                Ok((client, key, queries, cursor))
            })();

            let (mut client, key, queries, mut cursor) = match prepared {
                Ok(prepared) => {
                    let _ = ready_tx.send(Ok(()));
                    prepared
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error.to_string()));
                    measured.wait();
                    return Err(error);
                }
            };

            measured.wait();
            let mut operations = 0u64;
            let mut hits = 0u64;
            let mut latency = LatencyHistogram::new();
            while !stop.load(Ordering::Relaxed) {
                let started = Instant::now();
                let matches = client.vsim(&key, &queries[cursor], count, ef_search)?;
                latency.record(started.elapsed().as_nanos() as u64);
                operations = operations.saturating_add(1);
                hits = hits.saturating_add((matches > 0) as u64);
                cursor = (cursor + 1) % queries.len();
                black_box(matches);
            }
            Ok(WorkerResult {
                operations,
                hits,
                latency,
            })
        }));
    }

    drop(ready_tx);
    let mut setup_error = None;
    for _ in 0..args.workers {
        match ready_rx.recv()? {
            Ok(()) => {}
            Err(error) => {
                setup_error.get_or_insert(error);
            }
        }
    }
    if setup_error.is_some() {
        stop.store(true, Ordering::Relaxed);
    }
    measured.wait();
    let cpu_start = process_cpu_time();
    let started = Instant::now();
    thread::sleep(Duration::from_secs(args.duration_seconds));
    stop.store(true, Ordering::Relaxed);

    let mut operations = 0u64;
    let mut hits = 0u64;
    let mut latency = LatencyHistogram::new();
    for handle in handles {
        let joined = handle.join().map_err(|_| "typed vector worker panicked")?;
        if setup_error.is_some() {
            continue;
        }
        let result = joined?;
        operations = operations.saturating_add(result.operations);
        hits = hits.saturating_add(result.hits);
        latency.merge(&result.latency);
    }
    if let Some(error) = setup_error {
        return Err(format!("typed vector worker setup failed: {error}").into());
    }
    let elapsed = started.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_start);
    let process_vcpu = vcpu(cpu, elapsed);
    let ops_per_sec = operations as f64 / elapsed.as_secs_f64();

    println!(
        "{}: workers={} entries={} dims={} count={} ops/sec={:.0} p50={:.3}us p95={:.3}us p99={:.3}us hits={}/{} client_vcpu={:.2}",
        args.transport.label(),
        args.workers,
        args.entries,
        args.dims,
        args.count,
        ops_per_sec,
        latency.p50_ns() as f64 / 1_000.0,
        latency.p95_ns() as f64 / 1_000.0,
        latency.p99_ns() as f64 / 1_000.0,
        hits,
        operations,
        process_vcpu,
    );

    let mut csv = CsvWriter::new(
        args.csv.as_deref(),
        vec![
            "transport",
            "workers",
            "entries",
            "dims",
            "query_pool",
            "count",
            "ef_search",
            "duration_s",
            "operations",
            "hits",
            "ops_per_sec",
            "p50_us",
            "p95_us",
            "p99_us",
            "client_cpu_s",
            "client_vcpu",
        ],
    );
    csv.write_row(&[
        args.transport.label().to_string(),
        args.workers.to_string(),
        args.entries.to_string(),
        args.dims.to_string(),
        args.query_pool.to_string(),
        args.count.to_string(),
        args.ef_search.to_string(),
        format!("{:.6}", elapsed.as_secs_f64()),
        operations.to_string(),
        hits.to_string(),
        format!("{ops_per_sec:.6}"),
        format!("{:.6}", latency.p50_ns() as f64 / 1_000.0),
        format!("{:.6}", latency.p95_ns() as f64 / 1_000.0),
        format!("{:.6}", latency.p99_ns() as f64 / 1_000.0),
        format!("{:.6}", cpu.as_secs_f64()),
        format!("{process_vcpu:.6}"),
    ])?;
    Ok(())
}

fn validate(args: &Args) -> Result<(), BoxError> {
    if args.workers == 0
        || args.entries == 0
        || args.dims == 0
        || args.query_pool == 0
        || args.count == 0
        || args.ef_search == 0
        || args.duration_seconds == 0
    {
        return Err(
            "workers, entries, dimensions, query pool, count, EF, and duration must be positive"
                .into(),
        );
    }
    if args.query_pool > args.entries {
        return Err("query pool cannot exceed populated entries".into());
    }
    Ok(())
}

fn deterministic_vector(seed: usize, dims: usize) -> Vec<f32> {
    let mut vector = (0..dims)
        .map(|index| (((seed + 1) * (index + 3)) % 97) as f32 / 97.0)
        .collect::<Vec<_>>();
    let norm = vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if norm == 0.0 {
        vector[0] = 1.0;
        return vector;
    }
    for value in &mut vector {
        *value = (f64::from(*value) / norm) as f32;
    }
    vector
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_vectors_are_finite_and_nonzero_for_a_full_cycle() {
        for seed in 0..194 {
            let vector = deterministic_vector(seed, 16);
            assert!(vector.iter().all(|value| value.is_finite()));
            assert!(vector.iter().any(|value| *value != 0.0));
        }
    }
}
