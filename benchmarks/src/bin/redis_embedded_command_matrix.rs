//! Embedded Redis command benchmark.
//!
//! This runs the same Redis command case manifest as `redis_command_matrix`,
//! but executes commands directly through shardmap's first-party embedded Redis
//! API instead of RESP/SCNP sockets.

use std::collections::BTreeSet;
use std::error::Error;
use std::io::Write;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use serde::Serialize;
use shardcache_benchmarks::histogram::LatencyHistogram;
use shardcache_benchmarks::redis_command_cases::{
    REDIS_COMMAND_CASES, REDIS_COMMAND_DESTRUCTIVE_CASES, REDIS_COMMAND_LARGE_CASES,
    REDIS_MODULE_COMMAND_CASES, RedisCommandCase,
};
use shardmap::protocol::Frame;
use shardmap::redis_embedded::{EmbeddedRedis, EmbeddedRedisSession, PreparedEmbeddedRedisCommand};
use xxhash_rust::xxh3::xxh3_64;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Embedded shardcache Redis command benchmark")]
struct Args {
    /// Command, case, family, or comma-separated filters.
    #[arg(long, default_value = "all")]
    cases: String,

    /// Command, case, family, or comma-separated filters to exclude.
    #[arg(long, default_value = "")]
    skip_cases: String,

    /// Logical target name written into CSV.
    #[arg(long, default_value = "shardcache-embedded")]
    target: String,

    /// Fixture key suffixing mode.
    #[arg(long, default_value = "per-client")]
    fixture_scope: FixtureScope,

    /// Concurrent embedded client threads.
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Logical key lanes used to spread per-client fixtures across shards.
    #[arg(long, default_value_t = 1)]
    key_shards: usize,

    /// Embedded store shard count. Defaults to vCPUs when supplied, otherwise key-shards.
    #[arg(long)]
    store_shards: Option<usize>,

    /// Warmup seconds before recording.
    #[arg(long, default_value_t = 1)]
    warmup: u64,

    /// Measurement duration in seconds.
    #[arg(long, default_value_t = 5)]
    duration: u64,

    /// Operations to execute in a tight batch before checking the clock.
    #[arg(long, default_value_t = 1)]
    pipeline_depth: usize,

    /// Optional CSV output path.
    #[arg(long)]
    csv: Option<String>,

    /// Append CSV rows without writing a header.
    #[arg(long)]
    append_csv: bool,

    /// Stable run identifier written into CSV and plan metadata.
    #[arg(long)]
    run_id: Option<String>,

    /// Stable plan identifier shared by isolated target runs.
    #[arg(long)]
    resolved_plan_id: Option<String>,

    /// Suite label written into CSV and plan metadata.
    #[arg(long, default_value = "ad-hoc")]
    suite: String,

    /// Scenario label written into CSV and plan metadata.
    #[arg(long, default_value = "redis-embedded-command-matrix")]
    scenario: String,

    /// vCPU allocation for this isolated run.
    #[arg(long)]
    vcpus: Option<usize>,

    /// Total prepared command memory budget across all clients.
    #[arg(long, default_value_t = 256)]
    memory_budget_mib: usize,

    /// Total prepared command count budget across all clients. Zero means memory-bounded only.
    #[arg(long, default_value_t = 0)]
    command_budget: usize,

    /// Optional resolved plan JSON output path.
    #[arg(long)]
    plan_json: Option<String>,

    /// Treat Redis error frames as benchmark failures.
    #[arg(long)]
    fail_on_error: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum FixtureScope {
    PerClient,
    #[value(alias = "shared")]
    SharedKeyspace,
}

impl FixtureScope {
    fn label(self) -> &'static str {
        match self {
            Self::PerClient => "per-client",
            Self::SharedKeyspace => "shared-keyspace",
        }
    }
}

#[derive(Debug, Clone)]
struct RunConfig {
    clients: usize,
    key_shards: usize,
    fixture_scope: FixtureScope,
    pipeline_depth: usize,
    warmup: Duration,
    duration: Duration,
    memory_budget_mib: usize,
    command_budget: usize,
}

struct PlanLabels<'a> {
    run_id: &'a str,
    resolved_plan_id: &'a str,
    suite: &'a str,
    scenario: &'a str,
    target: &'a str,
    cases_filter: &'a str,
    skip_cases: &'a str,
}

#[derive(Debug, Clone, Default)]
struct CaseStats {
    ops: u64,
    errors: u64,
    expected_errors: u64,
    elapsed_ns: u128,
    latency: LatencyHistogram,
}

impl CaseStats {
    fn add(&mut self, other: &Self) {
        self.ops = self.ops.saturating_add(other.ops);
        self.errors = self.errors.saturating_add(other.errors);
        self.expected_errors = self.expected_errors.saturating_add(other.expected_errors);
        self.elapsed_ns = self.elapsed_ns.saturating_add(other.elapsed_ns);
        self.latency.merge(&other.latency);
    }

    fn ops_per_sec(&self, duration: Duration) -> f64 {
        self.ops as f64 / duration.as_secs_f64()
    }

    fn avg_us(&self) -> f64 {
        match self.ops {
            0 => 0.0,
            ops => self.elapsed_ns as f64 / ops as f64 / 1_000.0,
        }
    }

    fn p50_us(&self) -> f64 {
        self.latency.p50_ns() as f64 / 1_000.0
    }

    fn p95_us(&self) -> f64 {
        self.latency.p95_ns() as f64 / 1_000.0
    }

    fn p99_us(&self) -> f64 {
        self.latency.p99_ns() as f64 / 1_000.0
    }

    fn p999_us(&self) -> f64 {
        self.latency.p999_ns() as f64 / 1_000.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedOperation {
    case_index: usize,
    commands: Vec<PreparedEmbeddedRedisCommand>,
    encoded_bytes: usize,
}

#[derive(Debug, Clone)]
struct WorkerCommandPlan {
    operations: Vec<PlannedOperation>,
    encoded_bytes: usize,
}

impl WorkerCommandPlan {
    fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

#[derive(Debug, Serialize)]
struct ResolvedPlanMetadata {
    schema_version: u32,
    run_id: String,
    resolved_plan_id: String,
    suite: String,
    scenario: String,
    target: String,
    protocol: String,
    cases_filter: String,
    skip_cases: String,
    clients: usize,
    key_shards: usize,
    store_shards: usize,
    fixture_scope: String,
    pipeline_depth: usize,
    memory_budget_mib: usize,
    command_budget: usize,
    selected_cases: Vec<ResolvedPlanCase>,
    workers: Vec<ResolvedPlanWorker>,
    total_operations: usize,
    total_encoded_bytes: usize,
}

#[derive(Debug, Serialize)]
struct ResolvedPlanCase {
    family: String,
    category: String,
    command: String,
    case: String,
    profile: String,
    expected_error: bool,
    keyspace_wide: bool,
}

#[derive(Debug, Serialize)]
struct ResolvedPlanWorker {
    worker_id: usize,
    operations: usize,
    encoded_bytes: usize,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    if args.clients == 0 {
        return Err("--clients must be at least 1".into());
    }
    if args.key_shards == 0 {
        return Err("--key-shards must be at least 1".into());
    }
    if !args.key_shards.is_power_of_two() {
        return Err("--key-shards must be a power of two".into());
    }
    if args.pipeline_depth == 0 {
        return Err("--pipeline-depth must be at least 1".into());
    }
    if args.memory_budget_mib == 0 {
        return Err("--memory-budget-mib must be at least 1".into());
    }

    let store_shards = args
        .store_shards
        .unwrap_or_else(|| args.vcpus.unwrap_or(args.key_shards).max(args.key_shards));
    if store_shards == 0 || !store_shards.is_power_of_two() {
        return Err("--store-shards must be a non-zero power of two".into());
    }

    let cases = select_cases(&args.cases, &args.skip_cases)?;
    let duration = Duration::from_secs(args.duration);
    let warmup = Duration::from_secs(args.warmup);
    let run_config = RunConfig {
        clients: args.clients,
        key_shards: args.key_shards,
        fixture_scope: args.fixture_scope,
        pipeline_depth: args.pipeline_depth,
        warmup,
        duration,
        memory_budget_mib: args.memory_budget_mib,
        command_budget: args.command_budget,
    };
    let run_id = args.run_id.clone().unwrap_or_else(default_run_id);
    let resolved_plan_id = args
        .resolved_plan_id
        .clone()
        .unwrap_or_else(|| compute_plan_id(&args.suite, &args.scenario, &cases, &run_config));
    let labels = PlanLabels {
        run_id: &run_id,
        resolved_plan_id: &resolved_plan_id,
        suite: &args.suite,
        scenario: &args.scenario,
        target: &args.target,
        cases_filter: &args.cases,
        skip_cases: &args.skip_cases,
    };
    let plan_metadata = build_plan_metadata(&labels, store_shards, &cases, &run_config)?;
    if let Some(path) = args.plan_json.as_deref() {
        std::fs::write(path, serde_json::to_string_pretty(&plan_metadata)?)?;
    }

    println!(
        "redis-embedded-command-matrix: run_id={} resolved_plan_id={} suite={} scenario={} target={} cases={} clients={} key_shards={} store_shards={} fixture_scope={} pipeline_depth={} warmup={}s duration={}s memory_budget_mib={} command_budget={}",
        run_id,
        resolved_plan_id,
        args.suite,
        args.scenario,
        args.target,
        cases.len(),
        args.clients,
        args.key_shards,
        store_shards,
        args.fixture_scope.label(),
        args.pipeline_depth,
        args.warmup,
        args.duration,
        args.memory_budget_mib,
        args.command_budget,
    );
    println!();
    println!(
        "| target | family | command | case | ops/sec | avg us | p99 us | errors | expected errors |"
    );
    println!("| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |");

    let mut csv = match args.csv.as_deref() {
        Some(path) if args.append_csv => Some(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?,
        ),
        Some(path) => Some(std::fs::File::create(path)?),
        None => None,
    };
    if let Some(csv) = csv.as_mut()
        && !args.append_csv
    {
        writeln!(
            csv,
            "run_id,resolved_plan_id,suite,scenario,category,target,family,command,case,clients,key_shards,pipeline_depth,vcpus,duration_s,ops,ops_per_sec,avg_us,p50_us,p95_us,p99_us,p999_us,errors,expected_errors,profile,memory_budget_mib,command_budget,command_bytes,host"
        )?;
    }

    let redis = Arc::new(EmbeddedRedis::new(store_shards));
    let stats = run_target(redis, &cases, run_config)?;
    for (case, stats) in cases.iter().zip(stats.iter()) {
        if args.fail_on_error && stats.errors > 0 {
            return Err(format!(
                "{} {} produced {} Redis errors",
                args.target, case.case_name, stats.errors
            )
            .into());
        }

        println!(
            "| {} | {} | {} | {} | {:.0} | {:.2} | {:.2} | {} | {} |",
            args.target,
            case.family.label(),
            case.command_name,
            case.case_name,
            stats.ops_per_sec(duration),
            stats.avg_us(),
            stats.p99_us(),
            stats.errors,
            stats.expected_errors
        );
        if let Some(csv) = csv.as_mut() {
            writeln!(
                csv,
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},{},{},{},{},{},{}",
                run_id,
                resolved_plan_id,
                args.suite,
                args.scenario,
                case_category(case),
                args.target,
                case.family.label(),
                case.command_name,
                case.case_name,
                args.clients,
                args.key_shards,
                args.pipeline_depth,
                args.vcpus.unwrap_or(0),
                args.duration,
                stats.ops,
                stats.ops_per_sec(duration),
                stats.avg_us(),
                stats.p50_us(),
                stats.p95_us(),
                stats.p99_us(),
                stats.p999_us(),
                stats.errors,
                stats.expected_errors,
                case.profile.label(),
                args.memory_budget_mib,
                args.command_budget,
                plan_metadata.total_encoded_bytes,
                host_label(),
            )?;
        }
    }

    Ok(())
}

fn default_run_id() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    format!("embedded-run-{secs}")
}

fn compute_plan_id(
    suite: &str,
    scenario: &str,
    cases: &[RedisCommandCase],
    config: &RunConfig,
) -> String {
    let mut text = format!(
        "{suite}|{scenario}|{clients}|{key_shards}|{}|{pipeline_depth}|{memory_budget_mib}|{command_budget}",
        config.fixture_scope.label(),
        clients = config.clients,
        key_shards = config.key_shards,
        pipeline_depth = config.pipeline_depth,
        memory_budget_mib = config.memory_budget_mib,
        command_budget = config.command_budget,
    );
    for case in cases {
        text.push('|');
        text.push_str(case.command_name);
        text.push(':');
        text.push_str(case.case_name);
    }
    format!("plan-{:016x}", xxh3_64(text.as_bytes()))
}

fn case_category(case: &RedisCommandCase) -> String {
    if case.profile.label() == "destructive" {
        return "destructive".to_string();
    }
    if case.family.label() == "module" {
        return format!("module:{}", module_prefix(case.command_name));
    }
    if case.is_keyspace_wide() {
        return "keyspace".to_string();
    }
    case.family.label().to_string()
}

fn module_prefix(command_name: &str) -> String {
    command_name
        .split_once('.')
        .map(|(prefix, _)| prefix)
        .unwrap_or(command_name)
        .to_ascii_lowercase()
}

fn host_label() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

fn all_non_destructive_cases() -> impl Iterator<Item = RedisCommandCase> {
    REDIS_COMMAND_CASES
        .iter()
        .chain(REDIS_COMMAND_LARGE_CASES.iter())
        .copied()
}

fn all_cases() -> impl Iterator<Item = RedisCommandCase> {
    all_non_destructive_cases().chain(REDIS_COMMAND_DESTRUCTIVE_CASES.iter().copied())
}

fn select_cases(raw: &str, skip_raw: &str) -> Result<Vec<RedisCommandCase>, BoxError> {
    let filters = parse_filters(raw);
    if filters.is_empty() {
        return Err("--cases must not be empty".into());
    }
    let mut cases = Vec::new();
    let mut seen = BTreeSet::new();
    for filter in filters {
        let matches = matching_cases(filter);
        for case in matches {
            if seen.insert(case.case_name) {
                cases.push(case);
            }
        }
    }
    if cases.is_empty() {
        return Err(format!("no Redis command benchmark cases matched `{raw}`").into());
    }

    let skip_filters = parse_filters(skip_raw);
    if !skip_filters.is_empty() {
        let mut skipped = BTreeSet::new();
        for filter in skip_filters {
            let matches = matching_cases(filter);
            if matches.is_empty() {
                return Err(
                    format!("no Redis command benchmark cases matched skip `{filter}`").into(),
                );
            }
            skipped.extend(matches.into_iter().map(|case| case.case_name));
        }
        cases.retain(|case| !skipped.contains(case.case_name));
    }
    if cases.is_empty() {
        return Err("--skip-cases excluded every selected Redis command benchmark case".into());
    }
    Ok(cases)
}

fn parse_filters(raw: &str) -> Vec<&str> {
    raw.split(',')
        .map(str::trim)
        .filter(|filter| !filter.is_empty())
        .collect()
}

fn matching_cases(filter: &str) -> Vec<RedisCommandCase> {
    if filter.eq_ignore_ascii_case("all") {
        return REDIS_COMMAND_CASES.to_vec();
    }
    if filter.eq_ignore_ascii_case("large") || filter.eq_ignore_ascii_case("profile:large") {
        return REDIS_COMMAND_LARGE_CASES.to_vec();
    }
    if filter.eq_ignore_ascii_case("small") || filter.eq_ignore_ascii_case("profile:small") {
        return REDIS_COMMAND_CASES.to_vec();
    }
    if filter.eq_ignore_ascii_case("extended") {
        return all_non_destructive_cases().collect();
    }
    if filter.eq_ignore_ascii_case("destructive")
        || filter.eq_ignore_ascii_case("profile:destructive")
    {
        return REDIS_COMMAND_DESTRUCTIVE_CASES.to_vec();
    }
    if filter.eq_ignore_ascii_case("extended-with-destructive") {
        return all_cases().collect();
    }
    if filter.eq_ignore_ascii_case("modules")
        || filter.eq_ignore_ascii_case("module")
        || filter.eq_ignore_ascii_case("profile:module")
        || filter.eq_ignore_ascii_case("profile:modules")
    {
        return REDIS_MODULE_COMMAND_CASES.to_vec();
    }
    if let Some(module) = filter.strip_prefix("module:") {
        return REDIS_MODULE_COMMAND_CASES
            .iter()
            .copied()
            .filter(|case| {
                let prefix_len = case
                    .command_name
                    .find('.')
                    .unwrap_or(case.command_name.len());
                module.eq_ignore_ascii_case(&case.command_name[..prefix_len])
            })
            .collect();
    }
    if filter.eq_ignore_ascii_case("extended-no-keyspace")
        || filter.eq_ignore_ascii_case("no-keyspace")
        || filter.eq_ignore_ascii_case("profile:no-keyspace")
        || filter.eq_ignore_ascii_case("profile:hot")
    {
        return all_non_destructive_cases()
            .filter(|case| !case.is_keyspace_wide())
            .collect();
    }
    if filter.eq_ignore_ascii_case("keyspace")
        || filter.eq_ignore_ascii_case("profile:keyspace")
        || filter.eq_ignore_ascii_case("workload:keyspace")
    {
        return all_non_destructive_cases()
            .filter(|case| case.is_keyspace_wide())
            .collect();
    }

    if let Some(family) = filter.strip_prefix("family:") {
        return all_cases()
            .filter(|case| family.eq_ignore_ascii_case(case.family.label()))
            .collect();
    }

    let command_matches = all_cases()
        .chain(REDIS_MODULE_COMMAND_CASES.iter().copied())
        .filter(|case| filter.eq_ignore_ascii_case(case.command_name))
        .collect::<Vec<_>>();
    if !command_matches.is_empty() {
        return command_matches;
    }

    let family_matches = all_cases()
        .chain(REDIS_MODULE_COMMAND_CASES.iter().copied())
        .filter(|case| filter.eq_ignore_ascii_case(case.family.label()))
        .collect::<Vec<_>>();
    if !family_matches.is_empty() {
        return family_matches;
    }

    all_cases()
        .chain(REDIS_MODULE_COMMAND_CASES.iter().copied())
        .filter(|case| filter.eq_ignore_ascii_case(case.case_name))
        .collect()
}

fn build_plan_metadata(
    labels: &PlanLabels<'_>,
    store_shards: usize,
    cases: &[RedisCommandCase],
    config: &RunConfig,
) -> Result<ResolvedPlanMetadata, BoxError> {
    let mut workers = Vec::with_capacity(config.clients);
    let mut total_operations = 0_usize;
    let mut total_encoded_bytes = 0_usize;
    for worker_id in 0..config.clients {
        let suffixes = WorkerSuffixes::new(worker_id, config.key_shards);
        let plan = build_worker_command_plan(cases, &suffixes, config)?;
        total_operations = total_operations.saturating_add(plan.operation_count());
        total_encoded_bytes = total_encoded_bytes.saturating_add(plan.encoded_bytes);
        workers.push(ResolvedPlanWorker {
            worker_id,
            operations: plan.operation_count(),
            encoded_bytes: plan.encoded_bytes,
        });
    }

    Ok(ResolvedPlanMetadata {
        schema_version: 1,
        run_id: labels.run_id.to_string(),
        resolved_plan_id: labels.resolved_plan_id.to_string(),
        suite: labels.suite.to_string(),
        scenario: labels.scenario.to_string(),
        target: labels.target.to_string(),
        protocol: "embedded".to_string(),
        cases_filter: labels.cases_filter.to_string(),
        skip_cases: labels.skip_cases.to_string(),
        clients: config.clients,
        key_shards: config.key_shards,
        store_shards,
        fixture_scope: config.fixture_scope.label().to_string(),
        pipeline_depth: config.pipeline_depth,
        memory_budget_mib: config.memory_budget_mib,
        command_budget: config.command_budget,
        selected_cases: cases
            .iter()
            .map(|case| ResolvedPlanCase {
                family: case.family.label().to_string(),
                category: case_category(case),
                command: case.command_name.to_string(),
                case: case.case_name.to_string(),
                profile: case.profile.label().to_string(),
                expected_error: case.expect_error,
                keyspace_wide: case.is_keyspace_wide(),
            })
            .collect(),
        workers,
        total_operations,
        total_encoded_bytes,
    })
}

fn build_worker_command_plan(
    cases: &[RedisCommandCase],
    suffixes: &WorkerSuffixes,
    config: &RunConfig,
) -> Result<WorkerCommandPlan, BoxError> {
    let base = cases
        .iter()
        .enumerate()
        .map(|(case_index, case)| {
            let namespace = suffixes.for_case(case, config.fixture_scope);
            let commands = prepare_case(case, namespace)?;
            let encoded_bytes = commands
                .iter()
                .map(PreparedEmbeddedRedisCommand::encoded_bytes)
                .sum::<usize>();
            Ok(PlannedOperation {
                case_index,
                commands,
                encoded_bytes,
            })
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    if base.is_empty() {
        return Err("resolved command plan has no commands".into());
    }

    let base_bytes = operation_bytes(&base);
    let memory_budget_bytes = config
        .memory_budget_mib
        .checked_mul(1024 * 1024)
        .ok_or("--memory-budget-mib is too large")?;
    let worker_memory_budget = memory_budget_bytes / config.clients.max(1);
    if base_bytes > worker_memory_budget {
        return Err(format!(
            "memory budget is too small for one embedded command pass: worker needs at least {} bytes, budget allows {} bytes",
            base_bytes, worker_memory_budget
        )
        .into());
    }

    let worker_command_budget = if config.command_budget == 0 {
        usize::MAX
    } else {
        let per_worker = config.command_budget / config.clients.max(1);
        if per_worker < base.len() {
            return Err(format!(
                "command budget is too small for one embedded command pass: worker needs at least {} commands, budget allows {} commands",
                base.len(),
                per_worker
            )
            .into());
        }
        per_worker
    };

    let mut operations = base.clone();
    let mut bytes = base_bytes;
    while operations.len().saturating_add(base.len()) <= worker_command_budget
        && bytes.saturating_add(base_bytes) <= worker_memory_budget
    {
        operations.extend(base.iter().cloned());
        bytes = bytes.saturating_add(base_bytes);
    }

    Ok(WorkerCommandPlan {
        operations,
        encoded_bytes: bytes,
    })
}

fn operation_bytes(operations: &[PlannedOperation]) -> usize {
    operations
        .iter()
        .map(|operation| operation.encoded_bytes)
        .sum::<usize>()
}

fn prepare_case(
    case: &RedisCommandCase,
    namespace: &KeyNamespace,
) -> Result<Vec<PreparedEmbeddedRedisCommand>, BoxError> {
    let mut commands = Vec::new();
    if case.script.is_empty() {
        commands.push(prepare_command(case.parts, namespace)?);
    } else {
        for parts in case.script {
            commands.push(prepare_command(parts, namespace)?);
        }
    }
    Ok(commands)
}

fn prepare_command(
    parts: &[&str],
    namespace: &KeyNamespace,
) -> Result<PreparedEmbeddedRedisCommand, BoxError> {
    let mut rewritten = Vec::with_capacity(parts.len());
    for part in parts {
        append_rewritten_parts(part, namespace, &mut rewritten)?;
    }
    Ok(PreparedEmbeddedRedisCommand::new(&rewritten)?)
}

fn run_target(
    redis: Arc<EmbeddedRedis>,
    cases: &[RedisCommandCase],
    config: RunConfig,
) -> Result<Vec<CaseStats>, BoxError> {
    let cases = Arc::new(cases.to_vec());
    let mut handles = Vec::with_capacity(config.clients);
    for worker_id in 0..config.clients {
        let redis = Arc::clone(&redis);
        let cases = Arc::clone(&cases);
        let config = config.clone();
        handles.push(thread::spawn(move || {
            run_worker(worker_id, redis, &cases, config)
        }));
    }

    let mut totals = vec![CaseStats::default(); cases.len()];
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| "embedded Redis command benchmark worker panicked")??;
        for (total, stats) in totals.iter_mut().zip(worker_stats.iter()) {
            total.add(stats);
        }
    }
    Ok(totals)
}

fn run_worker(
    worker_id: usize,
    redis: Arc<EmbeddedRedis>,
    cases: &[RedisCommandCase],
    config: RunConfig,
) -> Result<Vec<CaseStats>, BoxError> {
    let suffixes = WorkerSuffixes::new(worker_id, config.key_shards);
    let plan = build_worker_command_plan(cases, &suffixes, &config)?;
    run_setup(&redis, cases, &suffixes, config.fixture_scope)?;
    if !config.warmup.is_zero() {
        let mut session = redis.session();
        run_plan(
            &mut session,
            cases,
            &plan,
            config.pipeline_depth,
            Instant::now() + config.warmup,
            None,
        );
        run_setup(&redis, cases, &suffixes, config.fixture_scope)?;
    }

    let deadline = Instant::now() + config.duration;
    let mut stats = vec![CaseStats::default(); cases.len()];
    let mut session = redis.session();
    run_plan(
        &mut session,
        cases,
        &plan,
        config.pipeline_depth,
        deadline,
        Some(&mut stats),
    );
    Ok(stats)
}

struct WorkerSuffixes {
    worker: KeyNamespace,
    shared_keyspace: KeyNamespace,
}

impl WorkerSuffixes {
    fn new(worker_id: usize, key_shards: usize) -> Self {
        let shard_lane = worker_id % key_shards;
        Self {
            worker: KeyNamespace::new(
                format!("shard:{shard_lane}:worker:{worker_id}"),
                shard_lane,
                key_shards,
            ),
            shared_keyspace: KeyNamespace::unsharded("shared-keyspace"),
        }
    }

    fn for_case(&self, case: &RedisCommandCase, fixture_scope: FixtureScope) -> &KeyNamespace {
        match (fixture_scope, case.is_keyspace_wide()) {
            (FixtureScope::SharedKeyspace, true) => &self.shared_keyspace,
            _ => &self.worker,
        }
    }
}

struct KeyNamespace {
    label: String,
    shard_lane: usize,
    key_shards: usize,
}

impl KeyNamespace {
    fn new(label: String, shard_lane: usize, key_shards: usize) -> Self {
        Self {
            label,
            shard_lane,
            key_shards,
        }
    }

    fn unsharded(label: &str) -> Self {
        Self {
            label: label.to_string(),
            shard_lane: 0,
            key_shards: 1,
        }
    }

    fn label(&self) -> &str {
        &self.label
    }
}

fn run_setup(
    redis: &EmbeddedRedis,
    cases: &[RedisCommandCase],
    suffixes: &WorkerSuffixes,
    fixture_scope: FixtureScope,
) -> Result<(), BoxError> {
    for case in cases {
        let namespace = suffixes.for_case(case, fixture_scope);
        for parts in case.setup {
            let error = if let Some(directive) = parts
                .first()
                .and_then(|part| part.strip_prefix("$vector-fixture:"))
            {
                run_vector_fixture(redis, directive, namespace)?
            } else {
                execute_setup_command(redis, parts, namespace)?
            };
            if error && !case.ignore_setup_error {
                return Err(
                    format!("setup for `{}` produced a Redis error", case.case_name).into(),
                );
            }
        }
    }
    Ok(())
}

fn execute_setup_command(
    redis: &EmbeddedRedis,
    parts: &[&str],
    namespace: &KeyNamespace,
) -> Result<bool, BoxError> {
    let command = prepare_command(parts, namespace)?;
    Ok(frame_contains_error(&redis.execute_prepared(&command)))
}

fn run_vector_fixture(
    redis: &EmbeddedRedis,
    directive: &str,
    namespace: &KeyNamespace,
) -> Result<bool, BoxError> {
    let mut parts = directive.split(':');
    let key = parts.next().ok_or("vector fixture is missing key")?;
    let count = parse_required_count("vector fixture count", parts.next())?;
    let dim = parse_required_count("vector fixture dim", parts.next())?;
    if parts.next().is_some() {
        return Err(format!("invalid vector fixture directive: {directive}").into());
    }

    let mut saw_error = execute_setup_command(redis, &["DEL", &format!("$key:{key}")], namespace)?;
    for index in 0..count {
        let command = vector_add_command(key, index, dim);
        let refs = command.iter().map(String::as_str).collect::<Vec<_>>();
        saw_error |= execute_setup_command(redis, &refs, namespace)?;
    }
    Ok(saw_error)
}

fn parse_required_count(label: &str, raw: Option<&str>) -> Result<usize, BoxError> {
    let raw = raw.ok_or_else(|| format!("{label} is missing"))?;
    parse_token_count(label, raw)
}

fn vector_add_command(key: &str, index: usize, dim: usize) -> Vec<String> {
    let mut parts = Vec::with_capacity(dim + 7);
    parts.push("VADD".to_string());
    parts.push(format!("$key:{key}"));
    push_vector_values(&mut parts, dim, index);
    parts.push(format!("elem:{index:06}"));
    parts.push("SETATTR".to_string());
    parts.push(format!(
        "{{\"group\":{},\"keep\":{}}}",
        index % 4,
        index.is_multiple_of(2)
    ));
    parts
}

fn run_plan(
    session: &mut EmbeddedRedisSession<'_>,
    cases: &[RedisCommandCase],
    plan: &WorkerCommandPlan,
    pipeline_depth: usize,
    deadline: Instant,
    mut stats: Option<&mut [CaseStats]>,
) {
    let mut cursor = 0;
    while Instant::now() < deadline {
        for _ in 0..pipeline_depth {
            if Instant::now() >= deadline {
                break;
            }
            let operation = &plan.operations[cursor];
            cursor += 1;
            if cursor == plan.operations.len() {
                cursor = 0;
            }

            let started = Instant::now();
            let error = execute_operation(session, operation);
            if let Some(stats) = stats.as_deref_mut() {
                let elapsed_ns = started.elapsed().as_nanos();
                let case_stats = &mut stats[operation.case_index];
                case_stats.ops = case_stats.ops.saturating_add(1);
                case_stats.elapsed_ns = case_stats.elapsed_ns.saturating_add(elapsed_ns);
                case_stats
                    .latency
                    .record(elapsed_ns.min(u128::from(u64::MAX)) as u64);
                if error {
                    if cases[operation.case_index].expect_error {
                        case_stats.expected_errors = case_stats.expected_errors.saturating_add(1);
                    } else {
                        case_stats.errors = case_stats.errors.saturating_add(1);
                    }
                }
            }
        }
    }
}

fn execute_operation(session: &mut EmbeddedRedisSession<'_>, operation: &PlannedOperation) -> bool {
    let mut saw_error = false;
    for command in &operation.commands {
        let frame = session.execute_prepared(command);
        saw_error |= frame_contains_error(&frame);
    }
    saw_error
}

fn frame_contains_error(frame: &Frame) -> bool {
    match frame {
        Frame::Error(_) => true,
        Frame::Array(items) | Frame::Set(items) | Frame::Push(items) => {
            items.iter().any(frame_contains_error)
        }
        Frame::Map(items) => items
            .iter()
            .any(|(key, value)| frame_contains_error(key) || frame_contains_error(value)),
        Frame::Attribute { attributes, data } => {
            attributes
                .iter()
                .any(|(key, value)| frame_contains_error(key) || frame_contains_error(value))
                || frame_contains_error(data)
        }
        Frame::SimpleString(_)
        | Frame::BlobString(_)
        | Frame::Integer(_)
        | Frame::Null
        | Frame::Boolean(_)
        | Frame::Double(_)
        | Frame::BigNumber(_)
        | Frame::VerbatimString { .. } => false,
    }
}

fn append_rewritten_parts(
    part: &str,
    namespace: &KeyNamespace,
    out: &mut Vec<Vec<u8>>,
) -> Result<(), BoxError> {
    if let Some(key) = part.strip_prefix("$key:") {
        out.push(rewrite_key(key, namespace)?);
        return Ok(());
    }
    if let Some(size) = part.strip_prefix("$value:") {
        out.push(make_value(parse_token_count("$value", size)?));
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$members:") {
        for index in 0..parse_token_count("$members", count)? {
            out.push(format!("m:{index:06}").into_bytes());
        }
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$list-values:") {
        for index in 0..parse_token_count("$list-values", count)? {
            out.push(format!("v:{index:06}").into_bytes());
        }
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$hash-fields:") {
        for index in 0..parse_token_count("$hash-fields", count)? {
            out.push(format!("f:{index:06}").into_bytes());
            out.push(format!("v:{index:06}").into_bytes());
        }
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$zitems:") {
        for index in 0..parse_token_count("$zitems", count)? {
            out.push(index.to_string().into_bytes());
            out.push(format!("m:{index:06}").into_bytes());
        }
        return Ok(());
    }
    if let Some(spec) = part.strip_prefix("$vector-values:") {
        let (dim, seed) = parse_vector_values_spec(spec)?;
        let mut values = Vec::with_capacity(dim + 2);
        push_vector_values(&mut values, dim, seed);
        out.extend(values.into_iter().map(String::into_bytes));
        return Ok(());
    }
    if let Some(spec) = part.strip_prefix("$vector-fp32:") {
        let (dim, seed) = parse_vector_values_spec(spec)?;
        out.push(b"FP32".to_vec());
        out.push(vector_fp32(dim, seed));
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$kvpairs:") {
        for index in 0..parse_token_count("$kvpairs", count)? {
            out.push(rewrite_key(&format!("ks:{index:06}"), namespace)?);
            out.push(format!("v:{index:06}").into_bytes());
        }
        return Ok(());
    }
    if let Some(value) = part.strip_prefix("$dump:string:") {
        out.push(make_string_dump_payload(value.as_bytes()));
        return Ok(());
    }

    out.push(rewrite_part(part, namespace)?);
    Ok(())
}

fn parse_vector_values_spec(spec: &str) -> Result<(usize, usize), BoxError> {
    let Some((dim, seed)) = spec.split_once(':') else {
        return Err(format!("$vector-values spec `{spec}` must use dim:seed").into());
    };
    Ok((
        parse_token_count("$vector-values dim", dim)?,
        seed.parse::<usize>()
            .map_err(|error| format!("$vector-values seed `{seed}` is invalid: {error}"))?,
    ))
}

fn push_vector_values(parts: &mut Vec<String>, dim: usize, seed: usize) {
    parts.push("VALUES".to_string());
    parts.push(dim.to_string());
    for component in 0..dim {
        parts.push(format!("{:.6}", vector_component(seed, component)));
    }
}

fn vector_fp32(dim: usize, seed: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(dim.saturating_mul(4));
    for index in 0..dim {
        let value = vector_component(seed, index) as f32;
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

fn vector_component(seed: usize, component: usize) -> f64 {
    let raw = ((seed.wrapping_mul(31) + component.wrapping_mul(17) + 13) % 201) as f64;
    (raw - 100.0) / 100.0
}

fn parse_token_count(token: &str, raw: &str) -> Result<usize, BoxError> {
    let count = raw
        .parse::<usize>()
        .map_err(|err| format!("{token} count `{raw}` is invalid: {err}"))?;
    if count == 0 {
        return Err(format!("{token} count must be greater than zero").into());
    }
    Ok(count)
}

fn make_value(size: usize) -> Vec<u8> {
    const PATTERN: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._";
    let mut value = Vec::with_capacity(size);
    for index in 0..size {
        value.push(PATTERN[index % PATTERN.len()]);
    }
    value
}

fn rewrite_part(part: &str, namespace: &KeyNamespace) -> Result<Vec<u8>, BoxError> {
    match is_probable_key(part) {
        true => rewrite_key(part, namespace),
        false => Ok(part.as_bytes().to_vec()),
    }
}

fn rewrite_key(key: &str, namespace: &KeyNamespace) -> Result<Vec<u8>, BoxError> {
    if namespace.key_shards <= 1 {
        return Ok(format!("{key}:{{{}}}", namespace.label()).into_bytes());
    }

    let shift = shift_for(namespace.key_shards);
    for nonce in 0..10_000 {
        let candidate = format!("{key}:{{{}:n:{nonce}}}", namespace.label());
        if stripe_index(xxh3_64(candidate.as_bytes()), shift) == namespace.shard_lane {
            return Ok(candidate.into_bytes());
        }
    }
    Err(format!(
        "could not route benchmark key `{key}` to logical shard {} of {}",
        namespace.shard_lane, namespace.key_shards
    )
    .into())
}

fn stripe_index(hash: u64, shift: u32) -> usize {
    if shift == usize::BITS {
        0
    } else {
        ((hash as usize) << 7) >> shift
    }
}

fn shift_for(shard_count: usize) -> u32 {
    usize::BITS - shard_count.trailing_zeros()
}

fn make_string_dump_payload(value: &[u8]) -> Vec<u8> {
    const RDB_VERSION: u16 = 9;
    let mut out = Vec::with_capacity(value.len() + 16);
    out.push(0);
    write_rdb_len(&mut out, value.len() as u64);
    out.extend_from_slice(value);
    out.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let checksum = crc64_jones(0, &out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

fn write_rdb_len(out: &mut Vec<u8>, len: u64) {
    match len {
        len if len < (1 << 6) => out.push(len as u8),
        len if len < (1 << 14) => {
            out.push(((len >> 8) as u8) | 0x40);
            out.push((len & 0xff) as u8);
        }
        len if u32::try_from(len).is_ok() => {
            out.push(0x80);
            out.extend_from_slice(&(len as u32).to_be_bytes());
        }
        len => {
            out.push(0x81);
            out.extend_from_slice(&len.to_be_bytes());
        }
    }
}

fn crc64_jones(mut crc: u64, bytes: &[u8]) -> u64 {
    const TABLE: [u64; 256] = make_crc64_jones_table();
    for byte in bytes {
        crc = TABLE[((crc as u8) ^ *byte) as usize] ^ (crc >> 8);
    }
    crc
}

const fn make_crc64_jones_table() -> [u64; 256] {
    const POLY: u64 = 0x95ac_9329_ac4b_c9b5;
    let mut table = [0_u64; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ POLY
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

fn is_probable_key(part: &str) -> bool {
    matches!(
        part,
        "s" | "s-nx"
            | "s-del"
            | "exp"
            | "expireat-bench"
            | "pexpireat-bench"
            | "memory-bench"
            | "object-bench"
            | "touch-a"
            | "touch-b"
            | "randomkey-bench"
            | "copy-src"
            | "copy-dst"
            | "setnx-bench"
            | "bitstr"
            | "bit-a"
            | "bit-b"
            | "bit-out"
            | "n"
            | "nf"
            | "rename-a"
            | "rename-b"
            | "renamenx-src"
            | "renamenx-dst"
            | "txn"
            | "txn-discard"
            | "ma"
            | "mb"
            | "mc"
            | "h"
            | "hflds"
            | "hm"
            | "l"
            | "lmove-bench"
            | "lmpop-bench"
            | "blmpop-bench"
            | "bl"
            | "br"
            | "bm"
            | "bmd"
            | "missing-list"
            | "set-a"
            | "set-b"
            | "set-u"
            | "set-i"
            | "set-d"
            | "z"
            | "z2"
            | "zlex"
            | "zstored"
            | "zu1"
            | "zu2"
            | "zout"
            | "zi"
            | "zd"
            | "bzmin"
            | "bzmax"
            | "zmpop-bench"
            | "bzmpop-bench"
            | "wrong"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(command_budget: usize) -> RunConfig {
        RunConfig {
            clients: 1,
            key_shards: 1,
            fixture_scope: FixtureScope::PerClient,
            pipeline_depth: 1,
            warmup: Duration::ZERO,
            duration: Duration::from_secs(1),
            memory_budget_mib: 8,
            command_budget,
        }
    }

    #[test]
    fn module_filter_selects_namespace() {
        let cases = select_cases("module:topk", "").unwrap();

        assert!(!cases.is_empty());
        assert!(
            cases
                .iter()
                .all(|case| case.command_name.starts_with("TOPK."))
        );
    }

    #[test]
    fn precomposed_plan_is_deterministic() {
        let cases = select_cases("SET,GET,HSET,HGET", "").unwrap();
        let suffixes = WorkerSuffixes::new(0, 1);
        let config = test_config(0);

        let first = build_worker_command_plan(&cases, &suffixes, &config).unwrap();
        let second = build_worker_command_plan(&cases, &suffixes, &config).unwrap();

        assert_eq!(first.operation_count(), second.operation_count());
        assert_eq!(first.encoded_bytes, second.encoded_bytes);
        assert_eq!(first.operations, second.operations);
    }

    #[test]
    fn precomposed_plan_rejects_too_small_command_budget() {
        let cases = select_cases("SET,GET", "").unwrap();
        let suffixes = WorkerSuffixes::new(0, 1);

        let err = build_worker_command_plan(&cases, &suffixes, &test_config(1)).unwrap_err();

        assert!(
            err.to_string()
                .contains("command budget is too small for one embedded command pass")
        );
    }

    #[test]
    fn transaction_operations_execute_through_session() {
        let redis = EmbeddedRedis::new(1);
        let mut session = redis.session();
        let namespace = KeyNamespace::unsharded("txn-test");
        let commands = [
            ["MULTI"].as_slice(),
            ["SET", "txn", "v"].as_slice(),
            ["GET", "txn"].as_slice(),
            ["EXEC"].as_slice(),
        ]
        .iter()
        .map(|parts| prepare_command(parts, &namespace).unwrap())
        .collect::<Vec<_>>();
        let operation = PlannedOperation {
            case_index: 0,
            commands,
            encoded_bytes: 0,
        };

        assert!(!execute_operation(&mut session, &operation));
        assert_eq!(
            redis.execute(&[b"GET".as_slice(), b"txn:{txn-test}"]),
            Frame::BlobString(b"v".to_vec())
        );
    }

    #[test]
    fn all_core_command_cases_smoke_through_embedded_api() {
        let cases = select_cases("extended-with-destructive", "").unwrap();
        assert_embedded_cases_smoke(&cases, "core");
    }

    #[test]
    fn all_module_command_cases_smoke_through_embedded_api() {
        let cases = select_cases("modules", "").unwrap();
        assert_embedded_cases_smoke(&cases, "module");
    }

    fn assert_embedded_cases_smoke(cases: &[RedisCommandCase], label: &str) {
        let unexpected_errors = cases
            .iter()
            .filter_map(|case| {
                let redis = EmbeddedRedis::new(1);
                let suffixes = WorkerSuffixes::new(0, 1);
                let mut config = test_config(1);
                config.memory_budget_mib = 64;
                let single_case = [*case];
                let plan = build_worker_command_plan(&single_case, &suffixes, &config).unwrap();
                assert_eq!(plan.operation_count(), 1);
                run_setup(&redis, &single_case, &suffixes, config.fixture_scope).unwrap();

                let mut session = redis.session();
                let saw_error = execute_operation(&mut session, &plan.operations[0]);
                (saw_error && !case.expect_error).then_some(case.case_name)
            })
            .collect::<Vec<_>>();
        assert!(
            unexpected_errors.is_empty(),
            "embedded {label} smoke produced unexpected errors for cases: {unexpected_errors:?}"
        );
    }
}
