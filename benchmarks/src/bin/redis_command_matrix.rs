//! RESP command-script benchmark for Redis-compatible servers.
//!
//! This intentionally lives in the benchmark harness instead of Criterion so
//! the same executable can compare shardcache, Redis, and Valkey over TCP.

use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use shardcache_benchmarks::redis_command_cases::{
    REDIS_COMMAND_CASES, REDIS_COMMAND_DESTRUCTIVE_CASES, REDIS_COMMAND_LARGE_CASES,
    REDIS_MODULE_COMMAND_CASES, RedisCommandCase,
};
use shardmap::protocol::{
    FAST_FLAG_REDIS_COMMAND_ARGS, FAST_PROTOCOL_VERSION, FAST_REQUEST_MAGIC, FAST_RESPONSE_MAGIC,
    FastCodec, FastCommand, FastCommandKind, FastRedisRouteKeys, FastRequest,
};
use xxhash_rust::xxh3::xxh3_64;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Head-to-head RESP benchmark for Redis-compatible commands")]
struct Args {
    /// Comma-separated targets in name=host:port form.
    #[arg(long)]
    targets: String,

    /// Command, case, family, or comma-separated filters.
    ///
    /// Use `all` for every small case, `extended` for small plus large cases,
    /// `extended-no-keyspace` to omit full keyspace walks, `profile:keyspace`
    /// for only keyspace-wide cases, or `profile:destructive` for FLUSH* cases.
    #[arg(long, default_value = "all")]
    cases: String,

    /// Command, case, family, or comma-separated filters to exclude.
    #[arg(long, default_value = "")]
    skip_cases: String,

    /// Fixture key suffixing mode.
    ///
    /// `per-client` isolates all fixtures by client. `shared-keyspace` keeps
    /// mutating command fixtures per-client but shares keyspace-wide fixtures,
    /// avoiding hidden keyspace growth when CLIENTS increases.
    #[arg(long, default_value = "per-client")]
    fixture_scope: FixtureScope,

    /// Concurrent client connections per target.
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Logical key lanes used to spread per-client fixtures across server shards.
    ///
    /// Use this with CLIENTS > 1, typically matching shardcache's SHARD_COUNT.
    #[arg(long, default_value_t = 1)]
    key_shards: usize,

    /// Warmup seconds before recording.
    #[arg(long, default_value_t = 1)]
    warmup: u64,

    /// Measurement duration in seconds.
    #[arg(long, default_value_t = 5)]
    duration: u64,

    /// Commands to keep in flight per client before reading replies.
    #[arg(long, default_value_t = 1)]
    pipeline_depth: usize,

    /// Optional CSV output path.
    #[arg(long)]
    csv: Option<String>,

    /// Treat RESP error replies as benchmark failures.
    #[arg(long)]
    fail_on_error: bool,
}

#[derive(Debug, Clone)]
struct Target {
    name: String,
    protocol: TargetProtocol,
    addr: TargetAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetProtocol {
    Resp,
    Scnp,
}

impl TargetProtocol {
    fn label(self) -> &'static str {
        match self {
            Self::Resp => "resp",
            Self::Scnp => "scnp",
        }
    }
}

#[derive(Debug, Clone)]
enum TargetAddr {
    Single(String),
    DirectShards {
        host: String,
        base_port: u16,
        shard_count: usize,
    },
}

impl TargetAddr {
    fn label(&self) -> String {
        match self {
            Self::Single(addr) => addr.clone(),
            Self::DirectShards {
                host,
                base_port,
                shard_count,
            } => {
                format!("{host}:{base_port}+{shard_count}")
            }
        }
    }

    fn probe_addr(&self) -> String {
        self.addr_for_lane(0).expect("lane 0 is always valid")
    }

    fn addr_for_lane(&self, shard_lane: usize) -> Result<String, BoxError> {
        match self {
            Self::Single(addr) => Ok(addr.clone()),
            Self::DirectShards {
                host,
                base_port,
                shard_count,
            } => {
                if shard_lane >= *shard_count {
                    return Err(format!(
                        "direct-shard target has {shard_count} shard ports but key lane {shard_lane} was requested"
                    )
                    .into());
                }
                let offset = u16::try_from(shard_lane)
                    .map_err(|_| format!("key lane {shard_lane} does not fit in a TCP port"))?;
                let port = base_port.checked_add(offset).ok_or_else(|| {
                    format!("direct-shard target port range overflows from {base_port}")
                })?;
                Ok(format!("{host}:{port}"))
            }
        }
    }

    fn shard_count(&self) -> Option<usize> {
        match self {
            Self::Single(_) => None,
            Self::DirectShards { shard_count, .. } => Some(*shard_count),
        }
    }
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

#[derive(Debug, Clone, Copy)]
struct RunConfig {
    clients: usize,
    key_shards: usize,
    fixture_scope: FixtureScope,
    pipeline_depth: usize,
    warmup: Duration,
    duration: Duration,
}

#[derive(Debug, Clone, Default)]
struct CaseStats {
    ops: u64,
    errors: u64,
    expected_errors: u64,
    elapsed_ns: u128,
}

impl CaseStats {
    fn add(&mut self, other: &Self) {
        self.ops = self.ops.saturating_add(other.ops);
        self.errors = self.errors.saturating_add(other.errors);
        self.expected_errors = self.expected_errors.saturating_add(other.expected_errors);
        self.elapsed_ns = self.elapsed_ns.saturating_add(other.elapsed_ns);
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

    let targets = parse_targets(&args.targets)?;
    let cases = select_cases(&args.cases, &args.skip_cases)?;
    let duration = Duration::from_secs(args.duration);
    let warmup = Duration::from_secs(args.warmup);

    println!(
        "redis-command-matrix: targets={} cases={} clients={} key_shards={} fixture_scope={} pipeline_depth={} warmup={}s duration={}s",
        targets
            .iter()
            .map(|target| format!(
                "{}={}:{}",
                target.name,
                target.protocol.label(),
                target.addr.label()
            ))
            .collect::<Vec<_>>()
            .join(","),
        cases.len(),
        args.clients,
        args.key_shards,
        args.fixture_scope.label(),
        args.pipeline_depth,
        args.warmup,
        args.duration,
    );
    println!();
    println!("| target | family | command | case | ops/sec | avg us | errors | expected errors |");
    println!("| --- | --- | --- | --- | ---: | ---: | ---: | ---: |");

    let mut csv = match args.csv.as_deref() {
        Some(path) => Some(std::fs::File::create(path)?),
        None => None,
    };
    if let Some(csv) = csv.as_mut() {
        writeln!(
            csv,
            "target,family,command,case,clients,key_shards,pipeline_depth,duration_s,ops,ops_per_sec,avg_us,errors,expected_errors,profile"
        )?;
    }

    for target in targets {
        let stats = run_target(
            &target,
            &cases,
            RunConfig {
                clients: args.clients,
                key_shards: args.key_shards,
                fixture_scope: args.fixture_scope,
                pipeline_depth: args.pipeline_depth,
                warmup,
                duration,
            },
        )?;
        for (case, stats) in cases.iter().zip(stats.iter()) {
            if args.fail_on_error && stats.errors > 0 {
                return Err(format!(
                    "{} {} produced {} RESP errors",
                    target.name, case.case_name, stats.errors
                )
                .into());
            }

            println!(
                "| {} | {} | {} | {} | {:.0} | {:.2} | {} | {} |",
                target.name,
                case.family.label(),
                case.command_name,
                case.case_name,
                stats.ops_per_sec(duration),
                stats.avg_us(),
                stats.errors,
                stats.expected_errors
            );
            if let Some(csv) = csv.as_mut() {
                writeln!(
                    csv,
                    "{},{},{},{},{},{},{},{},{},{:.3},{:.3},{},{},{}",
                    target.name,
                    case.family.label(),
                    case.command_name,
                    case.case_name,
                    args.clients,
                    args.key_shards,
                    args.pipeline_depth,
                    args.duration,
                    stats.ops,
                    stats.ops_per_sec(duration),
                    stats.avg_us(),
                    stats.errors,
                    stats.expected_errors,
                    case.profile.label()
                )?;
            }
        }
    }

    Ok(())
}

fn parse_targets(raw: &str) -> Result<Vec<Target>, BoxError> {
    let targets = raw
        .split(',')
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            let (name, addr) = part
                .split_once('=')
                .or_else(|| part.split_once('@'))
                .ok_or_else(|| format!("target `{part}` must use name=host:port"))?;
            let (protocol, addr) = parse_target_protocol(addr.trim())?;
            Ok(Target {
                name: name.trim().to_string(),
                protocol,
                addr: parse_target_addr(addr)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if targets.is_empty() {
        return Err("at least one --targets entry is required".into());
    }
    Ok(targets)
}

fn parse_target_addr(raw: &str) -> Result<TargetAddr, String> {
    let (_protocol, raw) = parse_target_protocol(raw)?;
    let Some((addr, shard_count)) = raw.rsplit_once('+') else {
        return Ok(TargetAddr::Single(raw.to_string()));
    };
    let shard_count = shard_count
        .parse::<usize>()
        .map_err(|error| format!("direct-shard target `{raw}` has invalid shard count: {error}"))?;
    if shard_count == 0 {
        return Err(format!(
            "direct-shard target `{raw}` must have at least one shard"
        ));
    }
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| format!("direct-shard target `{raw}` must use host:base_port+shards"))?;
    let base_port = port
        .parse::<u16>()
        .map_err(|error| format!("direct-shard target `{raw}` has invalid base port: {error}"))?;
    Ok(TargetAddr::DirectShards {
        host: host.to_string(),
        base_port,
        shard_count,
    })
}

fn parse_target_protocol(raw: &str) -> Result<(TargetProtocol, &str), String> {
    if let Some(addr) = raw.strip_prefix("scnp:") {
        if addr.is_empty() {
            return Err("SCNP target address must not be empty".to_string());
        }
        return Ok((TargetProtocol::Scnp, addr));
    }
    if let Some(addr) = raw.strip_prefix("resp:") {
        if addr.is_empty() {
            return Err("RESP target address must not be empty".to_string());
        }
        return Ok((TargetProtocol::Resp, addr));
    }
    Ok((TargetProtocol::Resp, raw))
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

fn all_non_destructive_cases() -> impl Iterator<Item = RedisCommandCase> {
    REDIS_COMMAND_CASES
        .iter()
        .chain(REDIS_COMMAND_LARGE_CASES.iter())
        .copied()
}

fn all_cases() -> impl Iterator<Item = RedisCommandCase> {
    all_non_destructive_cases().chain(REDIS_COMMAND_DESTRUCTIVE_CASES.iter().copied())
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

fn run_target(
    target: &Target,
    cases: &[RedisCommandCase],
    config: RunConfig,
) -> Result<Vec<CaseStats>, BoxError> {
    if let Some(shard_count) = target.addr.shard_count()
        && config.key_shards > shard_count
    {
        return Err(format!(
            "target `{}` exposes {shard_count} direct shard ports but --key-shards is {}",
            target.name, config.key_shards
        )
        .into());
    }
    BenchConn::connect(target.protocol, &target.addr.probe_addr())?;

    let cases = Arc::new(cases.to_vec());
    let mut handles = Vec::with_capacity(config.clients);
    for worker_id in 0..config.clients {
        let target = target.clone();
        let cases = Arc::clone(&cases);
        handles.push(thread::spawn(move || {
            run_worker(worker_id, &target, &cases, config)
        }));
    }

    let mut totals = vec![CaseStats::default(); cases.len()];
    for handle in handles {
        let worker_stats = handle
            .join()
            .map_err(|_| "Redis command benchmark worker panicked")??;
        for (total, stats) in totals.iter_mut().zip(worker_stats.iter()) {
            total.add(stats);
        }
    }
    Ok(totals)
}

fn run_worker(
    worker_id: usize,
    target: &Target,
    cases: &[RedisCommandCase],
    config: RunConfig,
) -> Result<Vec<CaseStats>, BoxError> {
    let suffixes = WorkerSuffixes::new(worker_id, config.key_shards);
    let addr = target.addr.addr_for_lane(suffixes.worker.shard_lane)?;
    let mut conn = BenchConn::connect(target.protocol, &addr)?;
    run_setup(&mut conn, cases, &suffixes, config.fixture_scope)?;
    if !config.warmup.is_zero() {
        run_script(
            &mut conn,
            cases,
            &suffixes,
            config.fixture_scope,
            config.pipeline_depth,
            Instant::now() + config.warmup,
            None,
        )?;
        // Warmup can stop between stateful command pairs, so reconnect before
        // timing to clear connection-local state such as Pub/Sub subscriptions.
        conn = BenchConn::connect(target.protocol, &addr)?;
        run_setup(&mut conn, cases, &suffixes, config.fixture_scope)?;
    }

    let deadline = Instant::now() + config.duration;
    let mut stats = vec![CaseStats::default(); cases.len()];
    run_script(
        &mut conn,
        cases,
        &suffixes,
        config.fixture_scope,
        config.pipeline_depth,
        deadline,
        Some(&mut stats),
    )?;
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
    conn: &mut BenchConn,
    cases: &[RedisCommandCase],
    suffixes: &WorkerSuffixes,
    fixture_scope: FixtureScope,
) -> Result<(), BoxError> {
    for case in cases {
        let namespace = suffixes.for_case(case, fixture_scope);
        for parts in case.setup {
            if conn.execute(parts, namespace)? && !case.ignore_setup_error {
                return Err(format!("setup for `{}` produced a RESP error", case.case_name).into());
            }
        }
    }
    Ok(())
}

fn run_script(
    conn: &mut BenchConn,
    cases: &[RedisCommandCase],
    suffixes: &WorkerSuffixes,
    fixture_scope: FixtureScope,
    pipeline_depth: usize,
    deadline: Instant,
    mut stats: Option<&mut [CaseStats]>,
) -> Result<(), BoxError> {
    let mut pending = Vec::with_capacity(pipeline_depth);
    while Instant::now() < deadline {
        for (index, case) in cases.iter().enumerate() {
            if Instant::now() >= deadline {
                break;
            }
            let namespace = suffixes.for_case(case, fixture_scope);
            let started = Instant::now();
            conn.write_case(case, namespace)?;
            pending.push((index, started));
            if pending.len() >= pipeline_depth {
                drain_pipeline(conn, cases, &mut pending, stats.as_deref_mut())?;
            }
        }
        if !pending.is_empty() {
            drain_pipeline(conn, cases, &mut pending, stats.as_deref_mut())?;
        }
    }
    Ok(())
}

fn drain_pipeline(
    conn: &mut BenchConn,
    cases: &[RedisCommandCase],
    pending: &mut Vec<(usize, Instant)>,
    mut stats: Option<&mut [CaseStats]>,
) -> Result<(), BoxError> {
    conn.flush()?;
    for (index, started) in pending.drain(..) {
        let error = conn.read_case(&cases[index])?;
        if let Some(stats) = stats.as_deref_mut() {
            let case_stats = &mut stats[index];
            case_stats.ops = case_stats.ops.saturating_add(1);
            case_stats.elapsed_ns = case_stats
                .elapsed_ns
                .saturating_add(started.elapsed().as_nanos());
            if error {
                if cases[index].expect_error {
                    case_stats.expected_errors = case_stats.expected_errors.saturating_add(1);
                } else {
                    case_stats.errors = case_stats.errors.saturating_add(1);
                }
            }
        }
    }
    Ok(())
}

enum BenchConn {
    Resp(RespConn),
    Scnp(ScnpConn),
}

impl BenchConn {
    fn connect(protocol: TargetProtocol, addr: &str) -> Result<Self, BoxError> {
        match protocol {
            TargetProtocol::Resp => Ok(Self::Resp(RespConn::connect(addr)?)),
            TargetProtocol::Scnp => Ok(Self::Scnp(ScnpConn::connect(addr)?)),
        }
    }

    fn execute(&mut self, parts: &[&str], namespace: &KeyNamespace) -> Result<bool, BoxError> {
        match self {
            Self::Resp(conn) => conn.execute(parts, namespace),
            Self::Scnp(conn) => conn.execute(parts, namespace),
        }
    }

    fn write_case(
        &mut self,
        case: &RedisCommandCase,
        namespace: &KeyNamespace,
    ) -> Result<(), BoxError> {
        match self {
            Self::Resp(conn) => conn.write_case(case, namespace),
            Self::Scnp(conn) => conn.write_case(case, namespace),
        }
    }

    fn flush(&mut self) -> Result<(), BoxError> {
        match self {
            Self::Resp(conn) => conn.flush(),
            Self::Scnp(conn) => conn.flush(),
        }
    }

    fn read_case(&mut self, case: &RedisCommandCase) -> Result<bool, BoxError> {
        match self {
            Self::Resp(conn) => conn.read_case(case),
            Self::Scnp(conn) => conn.read_case(case),
        }
    }
}

struct RespConn {
    reader: BufReader<TcpStream>,
    line: Vec<u8>,
    command_cache: HashMap<CommandCacheKey, Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CommandCacheKey {
    ptr: usize,
    len: usize,
    suffix_ptr: usize,
    suffix_len: usize,
}

impl RespConn {
    fn connect(addr: &str) -> Result<Self, BoxError> {
        let stream = TcpStream::connect(addr)?;
        let _ = stream.set_nodelay(true);
        let reader = BufReader::with_capacity(64 * 1024, stream);
        Ok(Self {
            reader,
            line: Vec::with_capacity(128),
            command_cache: HashMap::new(),
        })
    }

    fn execute(&mut self, parts: &[&str], namespace: &KeyNamespace) -> Result<bool, BoxError> {
        self.write_command(parts, namespace)?;
        self.reader.get_mut().flush()?;
        self.read_frame()
    }

    fn write_case(
        &mut self,
        case: &RedisCommandCase,
        namespace: &KeyNamespace,
    ) -> Result<(), BoxError> {
        if case.script.is_empty() {
            return self.write_command(case.parts, namespace);
        }
        for parts in case.script {
            self.write_command(parts, namespace)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), BoxError> {
        self.reader.get_mut().flush()?;
        Ok(())
    }

    fn read_case(&mut self, case: &RedisCommandCase) -> Result<bool, BoxError> {
        let responses = case.script.len().max(1);
        let mut saw_error = false;
        for _ in 0..responses {
            saw_error |= self.read_frame()?;
        }
        Ok(saw_error)
    }

    fn write_command(&mut self, parts: &[&str], namespace: &KeyNamespace) -> Result<(), BoxError> {
        let cache_label = namespace.label();
        let cache_key = CommandCacheKey {
            ptr: parts.as_ptr() as usize,
            len: parts.len(),
            suffix_ptr: cache_label.as_ptr() as usize,
            suffix_len: cache_label.len(),
        };
        let encoded = match self.command_cache.entry(cache_key) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(encode_command(parts, namespace)?)
            }
        };
        self.reader.get_mut().write_all(encoded)?;
        Ok(())
    }

    fn read_frame(&mut self) -> Result<bool, BoxError> {
        let mut prefix = [0_u8; 1];
        self.reader.read_exact(&mut prefix)?;
        match prefix[0] {
            b'+' | b':' | b',' | b'#' | b'_' => {
                self.read_line()?;
                Ok(false)
            }
            b'-' => {
                self.read_line()?;
                Ok(true)
            }
            b'$' => {
                let len = self.read_len_line()?;
                if len >= 0 {
                    self.skip_exact(len as usize + 2)?;
                }
                Ok(false)
            }
            b'*' | b'~' => {
                let len = self.read_len_line()?;
                if len > 0 {
                    let mut saw_error = false;
                    for _ in 0..len {
                        saw_error |= self.read_frame()?;
                    }
                    Ok(saw_error)
                } else {
                    Ok(false)
                }
            }
            b'%' => {
                let len = self.read_len_line()?;
                if len > 0 {
                    let mut saw_error = false;
                    for _ in 0..len * 2 {
                        saw_error |= self.read_frame()?;
                    }
                    Ok(saw_error)
                } else {
                    Ok(false)
                }
            }
            other => Err(format!("unsupported RESP response prefix: {other:#x}").into()),
        }
    }

    fn read_len_line(&mut self) -> Result<i64, BoxError> {
        self.read_line()?;
        let text = std::str::from_utf8(&self.line)?;
        Ok(text.parse::<i64>()?)
    }

    fn read_line(&mut self) -> Result<(), BoxError> {
        self.line.clear();
        if self.reader.read_until(b'\n', &mut self.line)? == 0 {
            return Err("RESP stream closed while reading line".into());
        }
        if !self.line.ends_with(b"\r\n") {
            return Err("RESP line ended without CRLF".into());
        }
        let len = self.line.len();
        self.line.truncate(len - 2);
        Ok(())
    }

    fn skip_exact(&mut self, len: usize) -> Result<(), BoxError> {
        let mut remaining = len;
        let mut buf = [0_u8; 8192];
        while remaining > 0 {
            let take = remaining.min(buf.len());
            self.reader.read_exact(&mut buf[..take])?;
            remaining -= take;
        }
        Ok(())
    }
}

struct ScnpConn {
    reader: BufReader<TcpStream>,
    body: Vec<u8>,
    command_cache: HashMap<CommandCacheKey, Vec<u8>>,
}

impl ScnpConn {
    const STATUS_OK: u8 = 0;
    const STATUS_NULL: u8 = 1;
    const STATUS_ERROR: u8 = 2;
    const STATUS_INTEGER: u8 = 3;
    const STATUS_VALUE: u8 = 4;
    const STATUS_BOOLEAN: u8 = 5;
    const STATUS_ARRAY: u8 = 6;
    const STATUS_FLOAT: u8 = 7;

    fn connect(addr: &str) -> Result<Self, BoxError> {
        let stream = TcpStream::connect(addr)?;
        let _ = stream.set_nodelay(true);
        let reader = BufReader::with_capacity(64 * 1024, stream);
        Ok(Self {
            reader,
            body: Vec::with_capacity(1024),
            command_cache: HashMap::new(),
        })
    }

    fn execute(&mut self, parts: &[&str], namespace: &KeyNamespace) -> Result<bool, BoxError> {
        self.write_command(parts, namespace)?;
        self.reader.get_mut().flush()?;
        self.read_frame()
    }

    fn write_case(
        &mut self,
        case: &RedisCommandCase,
        namespace: &KeyNamespace,
    ) -> Result<(), BoxError> {
        if case.script.is_empty() {
            return self.write_command(case.parts, namespace);
        }
        for parts in case.script {
            self.write_command(parts, namespace)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), BoxError> {
        self.reader.get_mut().flush()?;
        Ok(())
    }

    fn read_case(&mut self, case: &RedisCommandCase) -> Result<bool, BoxError> {
        let responses = case.script.len().max(1);
        let mut saw_error = false;
        for _ in 0..responses {
            saw_error |= self.read_frame()?;
        }
        Ok(saw_error)
    }

    fn write_command(&mut self, parts: &[&str], namespace: &KeyNamespace) -> Result<(), BoxError> {
        let cache_label = namespace.label();
        let cache_key = CommandCacheKey {
            ptr: parts.as_ptr() as usize,
            len: parts.len(),
            suffix_ptr: cache_label.as_ptr() as usize,
            suffix_len: cache_label.len(),
        };
        let encoded = match self.command_cache.entry(cache_key) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(encode_scnp_command(parts, namespace)?)
            }
        };
        self.reader.get_mut().write_all(encoded)?;
        Ok(())
    }

    fn read_frame(&mut self) -> Result<bool, BoxError> {
        let mut header = [0_u8; 8];
        self.reader.read_exact(&mut header)?;
        if header[0] != FAST_RESPONSE_MAGIC {
            return Err(format!("invalid SCNP response magic byte: {:#04x}", header[0]).into());
        }
        if header[1] != FAST_PROTOCOL_VERSION {
            return Err(format!("unsupported SCNP response version: {}", header[1]).into());
        }

        let status = header[2];
        let body_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        self.body.resize(body_len, 0);
        self.reader.read_exact(&mut self.body)?;

        match status {
            Self::STATUS_OK
            | Self::STATUS_NULL
            | Self::STATUS_INTEGER
            | Self::STATUS_BOOLEAN
            | Self::STATUS_ARRAY
            | Self::STATUS_FLOAT => Ok(false),
            Self::STATUS_ERROR => Ok(true),
            Self::STATUS_VALUE => Ok(self.body.first().copied() == Some(b'-')),
            other => Err(format!("unsupported SCNP response status: {other:#x}").into()),
        }
    }
}

fn encode_command(parts: &[&str], namespace: &KeyNamespace) -> Result<Vec<u8>, BoxError> {
    let mut rewritten_parts = Vec::with_capacity(parts.len());
    for part in parts {
        append_rewritten_parts(part, namespace, &mut rewritten_parts)?;
    }

    let bytes = rewritten_parts
        .iter()
        .map(Vec::len)
        .sum::<usize>()
        .saturating_add(rewritten_parts.len().saturating_mul(16))
        .saturating_add(32);
    let mut encoded = Vec::with_capacity(bytes);
    write!(encoded, "*{}\r\n", rewritten_parts.len())?;
    for part in &rewritten_parts {
        write!(encoded, "${}\r\n", part.len())?;
        encoded.write_all(part)?;
        encoded.write_all(b"\r\n")?;
    }
    Ok(encoded)
}

fn encode_scnp_command(parts: &[&str], namespace: &KeyNamespace) -> Result<Vec<u8>, BoxError> {
    let mut rewritten_parts = Vec::with_capacity(parts.len());
    for part in parts {
        append_rewritten_parts(part, namespace, &mut rewritten_parts)?;
    }
    let Some((command, args)) = rewritten_parts.split_first() else {
        return Err("cannot encode empty SCNP Redis command".into());
    };
    let Some(kind) = FastCommandKind::from_redis_name(command) else {
        let parts = rewritten_parts
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        let mut encoded = Vec::new();
        FastCodec::encode_request(
            &FastRequest {
                key_hash: None,
                route_shard: None,
                key_tag: None,
                command: FastCommand::RespCommand { parts },
            },
            &mut encoded,
        );
        return Ok(encoded);
    };

    let route = scnp_route_metadata(kind, args, namespace);
    let route_prefix_len = route.map_or(0, |_| 8 + 4 + 8);
    let body_len = compact_list_len(args)?
        .checked_add(route_prefix_len)
        .ok_or("SCNP Redis command body length overflow")?;
    let mut encoded = Vec::with_capacity(8 + body_len);
    encoded.push(FAST_REQUEST_MAGIC);
    encoded.push(FAST_PROTOCOL_VERSION);
    encoded.push(kind as u8);
    encoded.push(match route {
        Some(_) => {
            FAST_FLAG_REDIS_COMMAND_ARGS
                | shardmap::protocol::FAST_FLAG_KEY_HASH
                | shardmap::protocol::FAST_FLAG_ROUTE_SHARD
                | shardmap::protocol::FAST_FLAG_KEY_TAG
        }
        None => FAST_FLAG_REDIS_COMMAND_ARGS,
    });
    encoded.extend_from_slice(&(body_len as u32).to_le_bytes());
    if let Some(route) = route {
        encoded.extend_from_slice(&route.key_hash.to_le_bytes());
        encoded.extend_from_slice(&route.shard_lane.to_le_bytes());
        encoded.extend_from_slice(&route.key_tag.to_le_bytes());
    }
    write_compact_len_prefixed_list(args, &mut encoded)?;
    Ok(encoded)
}

#[derive(Clone, Copy)]
struct ShardCacheRouteMetadata {
    key_hash: u64,
    shard_lane: u32,
    key_tag: u64,
}

fn scnp_route_metadata(
    kind: FastCommandKind,
    args: &[Vec<u8>],
    namespace: &KeyNamespace,
) -> Option<ShardCacheRouteMetadata> {
    if namespace.key_shards == 0 || !namespace.key_shards.is_power_of_two() {
        return None;
    }
    let borrowed = args.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let route_keys = match kind.redis_route_keys(&borrowed) {
        FastRedisRouteKeys::Keys(keys) if !keys.is_empty() => keys,
        FastRedisRouteKeys::None | FastRedisRouteKeys::AllShards | FastRedisRouteKeys::Keys(_) => {
            return None;
        }
    };

    let first_hash = xxh3_64(route_keys[0]);
    let shift = shift_for(namespace.key_shards);
    let shard_lane = stripe_index(first_hash, shift);
    if route_keys
        .iter()
        .skip(1)
        .any(|key| stripe_index(xxh3_64(key), shift) != shard_lane)
    {
        return None;
    }

    Some(ShardCacheRouteMetadata {
        key_hash: first_hash,
        shard_lane: u32::try_from(shard_lane).ok()?,
        key_tag: first_hash >> 56,
    })
}

fn compact_list_len(parts: &[Vec<u8>]) -> Result<usize, BoxError> {
    let count = u32::try_from(parts.len())
        .map_err(|_| format!("SCNP Redis command has too many arguments: {}", parts.len()))?;
    let mut len = compact_u32_len(count);
    for part in parts {
        let part_len = u32::try_from(part.len()).map_err(|_| {
            format!(
                "SCNP Redis command argument is too large: {} bytes",
                part.len()
            )
        })?;
        len = len
            .checked_add(compact_u32_len(part_len))
            .and_then(|len| len.checked_add(part.len()))
            .ok_or("SCNP Redis command body length overflow")?;
    }
    Ok(len)
}

fn write_compact_len_prefixed_list(parts: &[Vec<u8>], out: &mut Vec<u8>) -> Result<(), BoxError> {
    write_compact_u32(
        u32::try_from(parts.len())
            .map_err(|_| format!("SCNP Redis command has too many arguments: {}", parts.len()))?,
        out,
    );
    for part in parts {
        write_compact_u32(
            u32::try_from(part.len()).map_err(|_| {
                format!(
                    "SCNP Redis command argument is too large: {} bytes",
                    part.len()
                )
            })?,
            out,
        );
        out.extend_from_slice(part);
    }
    Ok(())
}

fn write_compact_u32(mut value: u32, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn compact_u32_len(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
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
    if len < (1 << 6) {
        out.push(len as u8);
    } else if len < (1 << 14) {
        out.push(((len >> 8) as u8) | 0x40);
        out.push((len & 0xff) as u8);
    } else if u32::try_from(len).is_ok() {
        out.push(0x80);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(0x81);
        out.extend_from_slice(&len.to_be_bytes());
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
    use super::{
        FixtureScope, REDIS_COMMAND_CASES, REDIS_COMMAND_DESTRUCTIVE_CASES,
        REDIS_MODULE_COMMAND_CASES, WorkerSuffixes, all_non_destructive_cases, parse_target_addr,
        rewrite_key, select_cases, shift_for, stripe_index,
    };

    #[test]
    fn skip_cases_excludes_command_filters() {
        let cases = select_cases("all", "OBJECT,COPY,ZDIFFSTORE").unwrap();

        assert_eq!(cases.len(), REDIS_COMMAND_CASES.len() - 3);
        assert!(!cases.iter().any(|case| case.command_name == "OBJECT"));
        assert!(!cases.iter().any(|case| case.command_name == "COPY"));
        assert!(!cases.iter().any(|case| case.command_name == "ZDIFFSTORE"));
    }

    #[test]
    fn skip_cases_excludes_large_command_filters() {
        let cases = select_cases("extended", "KEYS,SCAN").unwrap();
        let expected = all_non_destructive_cases()
            .filter(|case| case.command_name != "KEYS" && case.command_name != "SCAN")
            .count();

        assert_eq!(cases.len(), expected);
        assert!(!cases.iter().any(|case| case.command_name == "KEYS"));
        assert!(!cases.iter().any(|case| case.command_name == "SCAN"));
    }

    #[test]
    fn extended_no_keyspace_excludes_keyspace_wide_cases() {
        let cases = select_cases("extended-no-keyspace", "").unwrap();
        let expected = all_non_destructive_cases()
            .filter(|case| !case.is_keyspace_wide())
            .count();

        assert_eq!(cases.len(), expected);
        assert!(!cases.iter().any(|case| case.is_keyspace_wide()));
    }

    #[test]
    fn keyspace_filter_selects_only_keyspace_wide_cases() {
        let cases = select_cases("profile:keyspace", "").unwrap();
        let expected = all_non_destructive_cases()
            .filter(|case| case.is_keyspace_wide())
            .count();

        assert_eq!(cases.len(), expected);
        assert!(cases.iter().all(|case| case.is_keyspace_wide()));
        assert!(
            !cases
                .iter()
                .any(|case| case.profile.label() == "destructive")
        );
    }

    #[test]
    fn destructive_filter_is_opt_in() {
        let default_cases = select_cases("all", "").unwrap();
        assert!(
            !default_cases
                .iter()
                .any(|case| case.profile.label() == "destructive")
        );

        let destructive_cases = select_cases("profile:destructive", "").unwrap();
        assert_eq!(
            destructive_cases.len(),
            REDIS_COMMAND_DESTRUCTIVE_CASES.len()
        );
        assert!(
            destructive_cases
                .iter()
                .all(|case| case.profile.label() == "destructive")
        );
    }

    #[test]
    fn module_filter_is_opt_in() {
        let default_cases = select_cases("all", "").unwrap();
        assert!(
            !default_cases
                .iter()
                .any(|case| case.profile.label() == "module")
        );

        let module_cases = select_cases("modules", "").unwrap();
        assert_eq!(module_cases.len(), REDIS_MODULE_COMMAND_CASES.len());
        assert!(
            module_cases
                .iter()
                .all(|case| case.profile.label() == "module")
        );
        assert!(
            module_cases
                .iter()
                .all(|case| case.family.label() == "module")
        );
    }

    #[test]
    fn module_prefix_filter_selects_namespace() {
        let json_cases = select_cases("module:json", "").unwrap();
        assert!(!json_cases.is_empty());
        assert!(
            json_cases
                .iter()
                .all(|case| case.command_name.starts_with("JSON."))
        );
    }

    #[test]
    fn expected_error_cases_remain_in_default_matrix() {
        let cases = select_cases("all", "").unwrap();
        let expected_errors = cases
            .iter()
            .filter(|case| case.expect_error)
            .map(|case| case.command_name)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            expected_errors,
            [
                "CLUSTER", "FAILOVER", "FCALL", "FCALL_RO", "HOST:", "MIGRATE", "MONITOR", "MOVE",
                "POST", "PSYNC", "SHUTDOWN", "SYNC",
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn skip_cases_rejects_unknown_filters() {
        let err = select_cases("all", "not-a-command").unwrap_err();
        assert!(err.to_string().contains("not-a-command"));
    }

    #[test]
    fn worker_suffixes_include_logical_key_shard_lane() {
        let suffixes = WorkerSuffixes::new(5, 4);
        let case = REDIS_COMMAND_CASES
            .iter()
            .find(|case| !case.is_keyspace_wide())
            .unwrap();

        assert_eq!(
            suffixes.for_case(case, FixtureScope::PerClient).label(),
            "shard:1:worker:5"
        );
    }

    #[test]
    fn sharded_worker_suffix_routes_rewritten_keys_to_lane() {
        let suffixes = WorkerSuffixes::new(5, 4);
        let namespace = suffixes.for_case(&REDIS_COMMAND_CASES[0], FixtureScope::PerClient);
        let key = rewrite_key("dump-bench", namespace).unwrap();

        assert_eq!(stripe_index(super::xxh3_64(&key), shift_for(4)), 1);
    }

    #[test]
    fn direct_shard_target_maps_lanes_to_shard_ports() {
        let target = parse_target_addr("127.0.0.1:6384+4").unwrap();

        assert_eq!(target.addr_for_lane(0).unwrap(), "127.0.0.1:6384");
        assert_eq!(target.addr_for_lane(1).unwrap(), "127.0.0.1:6385");
        assert_eq!(target.addr_for_lane(2).unwrap(), "127.0.0.1:6386");
        assert_eq!(target.addr_for_lane(3).unwrap(), "127.0.0.1:6387");
        assert!(target.addr_for_lane(4).is_err());
    }

    #[test]
    fn dump_restore_workers_route_to_matching_direct_shard_ports() {
        let target = parse_target_addr("127.0.0.1:6384+4").unwrap();
        let dump_case = REDIS_COMMAND_CASES
            .iter()
            .find(|case| case.command_name == "DUMP")
            .unwrap();
        let restore_case = REDIS_COMMAND_CASES
            .iter()
            .find(|case| case.command_name == "RESTORE")
            .unwrap();

        for worker_id in 0..8 {
            let suffixes = WorkerSuffixes::new(worker_id, 4);
            let lane = worker_id % 4;
            let addr = target.addr_for_lane(lane).unwrap();
            let dump_key = rewrite_key(
                "dump-bench",
                suffixes.for_case(dump_case, FixtureScope::PerClient),
            )
            .unwrap();
            let restore_key = rewrite_key(
                "restore-bench",
                suffixes.for_case(restore_case, FixtureScope::PerClient),
            )
            .unwrap();

            assert_eq!(addr, format!("127.0.0.1:{}", 6384 + lane));
            assert_eq!(stripe_index(super::xxh3_64(&dump_key), shift_for(4)), lane);
            assert_eq!(
                stripe_index(super::xxh3_64(&restore_key), shift_for(4)),
                lane
            );
        }
    }

    #[test]
    fn shared_keyspace_scope_still_uses_shared_suffix_for_keyspace_cases() {
        let suffixes = WorkerSuffixes::new(5, 4);
        let case = all_non_destructive_cases()
            .find(|case| case.is_keyspace_wide())
            .unwrap();

        assert_eq!(
            suffixes
                .for_case(&case, FixtureScope::SharedKeyspace)
                .label(),
            "shared-keyspace"
        );
    }
}
