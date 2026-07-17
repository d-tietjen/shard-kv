//! Active-sync local metadata and background convergence cost benchmark.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use shardcache_benchmarks::histogram::{LatencyHistogram, format_ns};
use shardmap::storage::EmbeddedStore;
use shardmap::{
    ActiveShardMap, ActiveSyncConfig, ConflictClaim, ConflictDecision, ConflictOrderer,
    IncarnationId, NodeId, ShardCacheError, SyncOptions,
};

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Parser)]
#[command(about = "Measure active-sync local and background convergence cost")]
struct Args {
    #[arg(
        long,
        default_value = "baseline,causal-local,consensus-local,causal-sync,consensus-sync"
    )]
    modes: String,

    #[arg(long, default_value_t = 16)]
    shards: usize,

    #[arg(long, default_value_t = 16)]
    clients: usize,

    #[arg(long, default_value_t = 100_000)]
    key_count: usize,

    #[arg(long, default_value_t = 1024)]
    value_size: usize,

    #[arg(long, default_value_t = 80)]
    read_percent: u8,

    #[arg(long, default_value_t = 5)]
    duration: u64,

    #[arg(long, default_value_t = 2)]
    warmup: u64,

    #[arg(long, default_value_t = 1000)]
    sync_interval_ms: u64,

    /// Apply the same long-lived TTL to baseline and active-sync values.
    #[arg(long, default_value_t = 0)]
    ttl_seconds: u64,

    #[arg(long, default_value_t = 10_000)]
    latency_sample_rate: u64,
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Baseline,
    CausalLocal,
    ConsensusLocal,
    CausalSync,
    ConsensusSync,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, BoxError> {
        match value.trim() {
            "baseline" => Ok(Self::Baseline),
            "causal-local" | "active-local" => Ok(Self::CausalLocal),
            "consensus-local" | "active-orderer" => Ok(Self::ConsensusLocal),
            "causal-sync" | "active-sync" => Ok(Self::CausalSync),
            "consensus-sync" => Ok(Self::ConsensusSync),
            other => Err(format!(
                "unknown mode `{other}`; use baseline, causal-local, consensus-local, causal-sync, or consensus-sync"
            )
            .into()),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::CausalLocal => "causal-local",
            Self::ConsensusLocal => "consensus-local",
            Self::CausalSync => "causal-sync",
            Self::ConsensusSync => "consensus-sync",
        }
    }

    fn uses_consensus(self) -> bool {
        matches!(self, Self::ConsensusLocal | Self::ConsensusSync)
    }

    fn syncs_peer(self) -> bool {
        matches!(self, Self::CausalSync | Self::ConsensusSync)
    }

    fn conflict_order(self) -> &'static str {
        match self {
            Self::Baseline => "n/a",
            Self::CausalLocal | Self::CausalSync => "causal-hlc",
            Self::ConsensusLocal | Self::ConsensusSync => "consensus",
        }
    }

    fn peer_sync(self) -> &'static str {
        if self.syncs_peer() { "on" } else { "off" }
    }
}

#[derive(Clone)]
enum BenchStore {
    Baseline(Arc<EmbeddedStore>),
    Active(ActiveShardMap),
}

impl BenchStore {
    fn get(&self, key: &[u8]) {
        match self {
            Self::Baseline(store) => {
                std::hint::black_box(store.get(key));
            }
            Self::Active(store) => {
                std::hint::black_box(store.get(key));
            }
        }
    }

    fn set(&self, key: &[u8], value: &Bytes, ttl: Option<Duration>) {
        match self {
            Self::Baseline(store) => store.set_value_bytes(
                key,
                value.clone(),
                ttl.map(|ttl| benchmark_now_millis().saturating_add(duration_millis(ttl))),
            ),
            Self::Active(store) => {
                if let Some(ttl) = ttl {
                    store
                        .set_value_bytes_with_ttl(key, value.clone(), ttl)
                        .expect("active-sync benchmark TTL write");
                } else {
                    store
                        .set_value_bytes(key, value.clone())
                        .expect("active-sync benchmark write");
                }
            }
        }
    }
}

#[derive(Debug)]
struct PhaseResult {
    operations: u64,
    elapsed: Duration,
    latency: LatencyHistogram,
}

struct UnexpectedConflictOrderer;

impl ConflictOrderer for UnexpectedConflictOrderer {
    fn decide(&self, _claim: &ConflictClaim) -> shardmap::Result<ConflictDecision> {
        Err(ShardCacheError::Protocol(
            "conflict-free benchmark unexpectedly invoked the orderer".into(),
        ))
    }
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    if args.clients == 0
        || args.key_count == 0
        || args.value_size == 0
        || args.read_percent > 100
        || args.duration == 0
        || args.shards == 0
        || !args.shards.is_power_of_two()
    {
        return Err(
            "benchmark counts must be nonzero, read percent <= 100, and shards a power of two"
                .into(),
        );
    }
    let modes = args
        .modes
        .split(',')
        .map(Mode::parse)
        .collect::<Result<Vec<_>, _>>()?;
    let keys = Arc::new(
        (0..args.key_count)
            .map(|index| {
                format!("active-key-{index:016x}")
                    .into_bytes()
                    .into_boxed_slice()
            })
            .collect::<Vec<_>>(),
    );
    let value = Bytes::from(build_value(args.value_size));
    let ttl = (args.ttl_seconds > 0).then(|| Duration::from_secs(args.ttl_seconds));

    println!(
        "| mode | conflict order | peer sync | shards | clients | value | read % | ops/s | p50 | p99 | p999 | retained |"
    );
    println!("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for mode in modes {
        let (store, peer) = build_store(mode, &args, &keys, &value, ttl)?;
        let sync_stop = Arc::new(AtomicBool::new(false));
        let sync_join = peer.as_ref().map(|peer| {
            let BenchStore::Active(local) = store.clone() else {
                unreachable!();
            };
            let peer = peer.clone();
            let stop = Arc::clone(&sync_stop);
            let interval = Duration::from_millis(args.sync_interval_ms.max(1));
            thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    thread::sleep(interval);
                    local
                        .sync_with(&peer, SyncOptions::default())
                        .map_err(|error| error.to_string())?;
                }
                Ok::<(), String>(())
            })
        });

        if args.warmup > 0 {
            let _ = run_phase(
                store.clone(),
                Arc::clone(&keys),
                value.clone(),
                &args,
                Duration::from_secs(args.warmup),
                false,
            );
        }
        let result = run_phase(
            store.clone(),
            Arc::clone(&keys),
            value.clone(),
            &args,
            Duration::from_secs(args.duration),
            true,
        );
        sync_stop.store(true, Ordering::Release);
        if let Some(join) = sync_join {
            join.join()
                .map_err(|_| "active-sync benchmark thread panicked")?
                .map_err(|error| format!("active-sync background sync failed: {error}"))?;
        }
        if let (BenchStore::Active(local), Some(peer)) = (&store, &peer) {
            finish_sync_and_verify(local, peer, &keys)?;
        }
        let retained = match &store {
            BenchStore::Baseline(_) => 0,
            BenchStore::Active(store) => store.health_snapshot().retained_blocks,
        };
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {:.2} | {} | {} | {} | {} |",
            mode.label(),
            mode.conflict_order(),
            mode.peer_sync(),
            args.shards,
            args.clients,
            args.value_size,
            args.read_percent,
            result.operations as f64 / result.elapsed.as_secs_f64(),
            format_ns(result.latency.p50_ns()),
            format_ns(result.latency.p99_ns()),
            format_ns(result.latency.p999_ns()),
            retained,
        );
    }
    Ok(())
}

fn finish_sync_and_verify(
    local: &ActiveShardMap,
    peer: &ActiveShardMap,
    keys: &[Box<[u8]>],
) -> Result<(), BoxError> {
    const MAX_FINAL_SYNC_ROUNDS: usize = 4096;

    let mut reached_quiet_round = false;
    for _ in 0..MAX_FINAL_SYNC_ROUNDS {
        let report = local.sync_with(peer, SyncOptions::default())?;
        let transferred =
            report.blocks_to_local + report.blocks_to_peer + report.state_snapshot_fallbacks;
        if !report.truncated && transferred == 0 {
            reached_quiet_round = true;
            break;
        }
        if report.truncated && transferred == 0 {
            return Err("active-sync final convergence made no progress".into());
        }
    }
    if !reached_quiet_round {
        return Err("active-sync final convergence exceeded its round limit".into());
    }
    for key in keys {
        if local.get(key) != peer.get(key) {
            return Err("active-sync benchmark peers did not converge".into());
        }
    }
    Ok(())
}

fn build_store(
    mode: Mode,
    args: &Args,
    keys: &[Box<[u8]>],
    value: &Bytes,
    ttl: Option<Duration>,
) -> Result<(BenchStore, Option<ActiveShardMap>), BoxError> {
    match mode {
        Mode::Baseline => {
            let store = Arc::new(EmbeddedStore::new(args.shards));
            for key in keys {
                store.set_value_bytes(
                    key,
                    value.clone(),
                    ttl.map(|ttl| benchmark_now_millis().saturating_add(duration_millis(ttl))),
                );
            }
            Ok((BenchStore::Baseline(store), None))
        }
        Mode::CausalLocal | Mode::ConsensusLocal | Mode::CausalSync | Mode::ConsensusSync => {
            let mut local_config =
                ActiveSyncConfig::new("benchmark", NodeId::new("benchmark-local")?);
            local_config.incarnation_id = IncarnationId(1);
            let local = if mode.uses_consensus() {
                ActiveShardMap::new_consensus_ordered_eventual(
                    args.shards,
                    local_config,
                    Arc::new(UnexpectedConflictOrderer),
                )?
            } else {
                ActiveShardMap::new_causal_eventual(args.shards, local_config)?
            };
            for key in keys {
                if let Some(ttl) = ttl {
                    local.set_value_bytes_with_ttl(key, value.clone(), ttl)?;
                } else {
                    local.set_value_bytes(key, value.clone())?;
                }
            }
            local.seal_pending()?;
            let peer = if mode.syncs_peer() {
                let mut peer_config =
                    ActiveSyncConfig::new("benchmark", NodeId::new("benchmark-peer")?);
                peer_config.incarnation_id = IncarnationId(2);
                let peer = if mode.uses_consensus() {
                    ActiveShardMap::new_consensus_ordered_eventual(
                        args.shards,
                        peer_config,
                        Arc::new(UnexpectedConflictOrderer),
                    )?
                } else {
                    ActiveShardMap::new_causal_eventual(args.shards, peer_config)?
                };
                local.sync_with(&peer, SyncOptions::default())?;
                Some(peer)
            } else {
                None
            };
            Ok((BenchStore::Active(local), peer))
        }
    }
}

fn run_phase(
    store: BenchStore,
    keys: Arc<Vec<Box<[u8]>>>,
    value: Bytes,
    args: &Args,
    duration: Duration,
    measure_latency: bool,
) -> PhaseResult {
    let start = Instant::now();
    let deadline = start + duration;
    let mut results = Vec::with_capacity(args.clients);
    thread::scope(|scope| {
        let mut joins = Vec::with_capacity(args.clients);
        for client_id in 0..args.clients {
            let store = store.clone();
            let keys = Arc::clone(&keys);
            let value = value.clone();
            let read_percent = u64::from(args.read_percent);
            let sample_rate = args.latency_sample_rate;
            let ttl = (args.ttl_seconds > 0).then(|| Duration::from_secs(args.ttl_seconds));
            joins.push(scope.spawn(move || {
                let mut state = (client_id as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                let mut operations = 0u64;
                let mut latency = LatencyHistogram::new();
                while Instant::now() < deadline {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let key = &keys[state as usize % keys.len()];
                    let read = state % 100 < read_percent;
                    let sampled = measure_latency
                        && sample_rate > 0
                        && operations.is_multiple_of(sample_rate);
                    let operation_start = sampled.then(Instant::now);
                    if read {
                        store.get(key);
                    } else {
                        store.set(key, &value, ttl);
                    }
                    if let Some(operation_start) = operation_start {
                        latency.record(
                            operation_start
                                .elapsed()
                                .as_nanos()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                    }
                    operations = operations.saturating_add(1);
                }
                (operations, latency)
            }));
        }
        for join in joins {
            results.push(join.join().expect("active-sync benchmark worker"));
        }
    });
    let mut latency = LatencyHistogram::new();
    let mut operations = 0u64;
    for (thread_operations, thread_latency) in results {
        operations = operations.saturating_add(thread_operations);
        latency.merge(&thread_latency);
    }
    PhaseResult {
        operations,
        elapsed: start.elapsed(),
        latency,
    }
}

fn build_value(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(17))
        .collect()
}

fn benchmark_now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
