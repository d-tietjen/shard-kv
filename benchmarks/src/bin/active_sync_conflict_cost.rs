//! Active-sync concurrent-conflict convergence benchmark.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use shardcache_benchmarks::histogram::{LatencyHistogram, format_ns};
use shardmap::{
    ActiveShardMap, ActiveSyncConfig, ConflictClaim, ConflictDecision, ConflictOrderer,
    IncarnationId, NodeId, SyncOptions,
};

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Parser)]
#[command(about = "Measure active-sync convergence under guaranteed concurrent conflicts")]
struct Args {
    #[arg(long, default_value = "causal,consensus")]
    modes: String,

    #[arg(long, default_value_t = 8)]
    shards: usize,

    /// Keys concurrently overwritten by both nodes in every round.
    #[arg(long, default_value_t = 1024)]
    conflict_keys: usize,

    #[arg(long, default_value_t = 1024)]
    value_size: usize,

    #[arg(long, default_value_t = 50)]
    rounds: usize,

    #[arg(long, default_value_t = 5)]
    warmup_rounds: usize,

    /// Synthetic external-orderer latency per decision.
    #[arg(long, default_value_t = 0)]
    orderer_delay_micros: u64,
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Causal,
    Consensus,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, BoxError> {
        match value.trim() {
            "causal" => Ok(Self::Causal),
            "consensus" => Ok(Self::Consensus),
            other => Err(format!("unknown mode `{other}`; use causal or consensus").into()),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Causal => "causal",
            Self::Consensus => "consensus",
        }
    }
}

#[derive(Debug)]
struct DeterministicOrderer {
    delay: Duration,
    calls: AtomicU64,
}

impl DeterministicOrderer {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            calls: AtomicU64::new(0),
        }
    }

    fn reset_calls(&self) {
        self.calls.store(0, Ordering::Relaxed);
    }

    fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
}

impl ConflictOrderer for DeterministicOrderer {
    fn decide(&self, claim: &ConflictClaim) -> shardmap::Result<ConflictDecision> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
        }
        let winner = claim
            .candidates()
            .iter()
            .max()
            .expect("conflict claims always contain candidates")
            .dot()
            .clone();
        ConflictDecision::new(claim, winner)
    }
}

#[derive(Debug)]
struct BenchResult {
    conflict_pairs: u64,
    reported_conflicts: u64,
    admission_elapsed: Duration,
    convergence_elapsed: Duration,
    convergence_latency: LatencyHistogram,
    orderer_calls: u64,
}

struct BenchPair {
    left: ActiveShardMap,
    right: ActiveShardMap,
    orderer: Option<Arc<DeterministicOrderer>>,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    validate_args(&args)?;
    let modes = args
        .modes
        .split(',')
        .map(Mode::parse)
        .collect::<Result<Vec<_>, _>>()?;
    let keys = build_keys(args.conflict_keys);

    println!(
        "| mode | orderer delay | conflict pairs | admission mutations/s | convergence pairs/s | end-to-end pairs/s | sync p50 | sync p99 | reported conflicts | orderer calls/pair |"
    );
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for mode in modes {
        let result = run_benchmark(mode, &args, &keys)?;
        let admission_mutations = result.conflict_pairs.saturating_mul(2);
        let end_to_end_elapsed = result.admission_elapsed + result.convergence_elapsed;
        println!(
            "| {} | {} | {} | {:.2} | {:.2} | {:.2} | {} | {} | {} | {:.2} |",
            mode.label(),
            orderer_delay_label(mode, args.orderer_delay_micros),
            result.conflict_pairs,
            rate(admission_mutations, result.admission_elapsed),
            rate(result.conflict_pairs, result.convergence_elapsed),
            rate(result.conflict_pairs, end_to_end_elapsed),
            format_ns(result.convergence_latency.p50_ns()),
            format_ns(result.convergence_latency.p99_ns()),
            result.reported_conflicts,
            result.orderer_calls as f64 / result.conflict_pairs as f64,
        );
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), BoxError> {
    if args.shards == 0
        || !args.shards.is_power_of_two()
        || args.conflict_keys == 0
        || args.value_size == 0
        || args.rounds == 0
    {
        return Err(
            "shards must be a nonzero power of two and benchmark counts must be nonzero".into(),
        );
    }
    Ok(())
}

fn run_benchmark(mode: Mode, args: &Args, keys: &[Box<[u8]>]) -> Result<BenchResult, BoxError> {
    let pair = build_pair(mode, args, keys)?;
    for round in 0..args.warmup_rounds {
        run_round(&pair, keys, args.value_size, round as u64, false)?;
    }
    if let Some(orderer) = &pair.orderer {
        orderer.reset_calls();
    }

    let mut result = BenchResult {
        conflict_pairs: 0,
        reported_conflicts: 0,
        admission_elapsed: Duration::ZERO,
        convergence_elapsed: Duration::ZERO,
        convergence_latency: LatencyHistogram::new(),
        orderer_calls: 0,
    };
    for round in 0..args.rounds {
        let measured = run_round(
            &pair,
            keys,
            args.value_size,
            (args.warmup_rounds + round) as u64,
            true,
        )?;
        result.conflict_pairs = result.conflict_pairs.saturating_add(keys.len().try_into()?);
        result.reported_conflicts = result
            .reported_conflicts
            .saturating_add(measured.reported_conflicts);
        result.admission_elapsed += measured.admission_elapsed;
        result.convergence_elapsed += measured.convergence_elapsed;
        result
            .convergence_latency
            .record(duration_ns(measured.convergence_elapsed));
    }
    result.orderer_calls = pair.orderer.as_ref().map_or(0, |orderer| orderer.calls());
    let expected_conflict_applications = result.conflict_pairs.saturating_mul(2);
    if result.reported_conflicts != expected_conflict_applications {
        return Err(format!(
            "benchmark generated {} conflict pairs but sync reported {} conflict applications instead of {}",
            result.conflict_pairs, result.reported_conflicts, expected_conflict_applications
        )
        .into());
    }
    if matches!(mode, Mode::Consensus) && result.orderer_calls < result.conflict_pairs {
        return Err(format!(
            "consensus benchmark generated {} conflict pairs but invoked the orderer only {} times",
            result.conflict_pairs, result.orderer_calls
        )
        .into());
    }
    Ok(result)
}

#[derive(Debug)]
struct RoundResult {
    reported_conflicts: u64,
    admission_elapsed: Duration,
    convergence_elapsed: Duration,
}

fn run_round(
    pair: &BenchPair,
    keys: &[Box<[u8]>],
    value_size: usize,
    round: u64,
    measure: bool,
) -> Result<RoundResult, BoxError> {
    let left_value = round_value(value_size, round, b'L');
    let right_value = round_value(value_size, round, b'R');
    let admission_start = Instant::now();
    for key in keys {
        pair.left.set_value_bytes(key, left_value.clone())?;
        pair.right.set_value_bytes(key, right_value.clone())?;
    }
    pair.left.seal_pending()?;
    pair.right.seal_pending()?;
    let admission_elapsed = admission_start.elapsed();

    let convergence_start = Instant::now();
    let report = pair.left.sync_with(&pair.right, SyncOptions::default())?;
    let convergence_elapsed = convergence_start.elapsed();
    if report.truncated {
        return Err("conflict benchmark sync was truncated; reduce --conflict-keys".into());
    }
    for key in keys {
        if pair.left.get(key) != pair.right.get(key) {
            return Err(
                format!("active-sync peers did not converge in conflict round {round}").into(),
            );
        }
    }
    Ok(RoundResult {
        reported_conflicts: report.conflicts.try_into()?,
        admission_elapsed: if measure {
            admission_elapsed
        } else {
            Duration::ZERO
        },
        convergence_elapsed: if measure {
            convergence_elapsed
        } else {
            Duration::ZERO
        },
    })
}

fn build_pair(mode: Mode, args: &Args, keys: &[Box<[u8]>]) -> Result<BenchPair, BoxError> {
    let orderer = matches!(mode, Mode::Consensus).then(|| {
        Arc::new(DeterministicOrderer::new(Duration::from_micros(
            args.orderer_delay_micros,
        )))
    });
    let left = build_map(mode, args.shards, "conflict-left", 1, orderer.clone())?;
    let right = build_map(mode, args.shards, "conflict-right", 2, orderer.clone())?;
    let seed = round_value(args.value_size, u64::MAX, b'S');
    for key in keys {
        left.set_value_bytes(key, seed.clone())?;
    }
    let report = left.sync_with(&right, SyncOptions::default())?;
    if report.truncated {
        return Err("conflict benchmark seed sync was truncated; reduce --conflict-keys".into());
    }
    for key in keys {
        if left.get(key) != right.get(key) {
            return Err("active-sync peers did not converge during benchmark setup".into());
        }
    }
    Ok(BenchPair {
        left,
        right,
        orderer,
    })
}

fn build_map(
    mode: Mode,
    shards: usize,
    node: &str,
    incarnation: u128,
    orderer: Option<Arc<DeterministicOrderer>>,
) -> Result<ActiveShardMap, BoxError> {
    let mut config = ActiveSyncConfig::new("conflict-benchmark", NodeId::new(node)?);
    config.incarnation_id = IncarnationId(incarnation);
    match mode {
        Mode::Causal => Ok(ActiveShardMap::new_causal_eventual(shards, config)?),
        Mode::Consensus => Ok(ActiveShardMap::new_consensus_ordered_eventual(
            shards,
            config,
            orderer.expect("consensus benchmark has an orderer"),
        )?),
    }
}

fn build_keys(count: usize) -> Vec<Box<[u8]>> {
    (0..count)
        .map(|index| {
            format!("conflict-key-{index:016x}")
                .into_bytes()
                .into_boxed_slice()
        })
        .collect()
}

fn round_value(size: usize, round: u64, side: u8) -> Bytes {
    let mut value = vec![side; size];
    for (destination, source) in value.iter_mut().skip(1).zip(round.to_le_bytes()) {
        *destination = source;
    }
    Bytes::from(value)
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn rate(operations: u64, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64()
}

fn orderer_delay_label(mode: Mode, delay_micros: u64) -> String {
    match mode {
        Mode::Causal => "n/a".into(),
        Mode::Consensus => format_ns(delay_micros.saturating_mul(1000)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_args() -> Args {
        Args {
            modes: "causal,consensus".into(),
            shards: 2,
            conflict_keys: 8,
            value_size: 32,
            rounds: 2,
            warmup_rounds: 1,
            orderer_delay_micros: 0,
        }
    }

    #[test]
    fn benchmark_exercises_causal_and_consensus_conflict_paths() {
        let args = test_args();
        let keys = build_keys(args.conflict_keys);
        let causal = run_benchmark(Mode::Causal, &args, &keys).unwrap();
        let consensus = run_benchmark(Mode::Consensus, &args, &keys).unwrap();

        assert_eq!(causal.conflict_pairs, 16);
        assert_eq!(causal.reported_conflicts, 32);
        assert_eq!(causal.orderer_calls, 0);
        assert_eq!(consensus.conflict_pairs, 16);
        assert_eq!(consensus.reported_conflicts, 32);
        assert!(consensus.orderer_calls >= consensus.conflict_pairs);
    }
}
