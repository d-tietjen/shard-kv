//! RESP command-script benchmark for Redis-compatible servers.
//!
//! This intentionally lives in the benchmark harness instead of Criterion so
//! the same executable can compare fast-cache, Redis, and Valkey over TCP.

use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use fast_cache_benchmarks::redis_command_cases::{
    REDIS_COMMAND_CASES, REDIS_COMMAND_LARGE_CASES, RedisCommandCase,
};

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Head-to-head RESP benchmark for Redis-compatible commands")]
struct Args {
    /// Comma-separated targets in name=host:port form.
    #[arg(long)]
    targets: String,

    /// Command, case, family, or comma-separated filters. Use `all` for every case.
    #[arg(long, default_value = "all")]
    cases: String,

    /// Concurrent client connections per target.
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Warmup seconds before recording.
    #[arg(long, default_value_t = 1)]
    warmup: u64,

    /// Measurement duration in seconds.
    #[arg(long, default_value_t = 5)]
    duration: u64,

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
    addr: String,
}

#[derive(Debug, Clone, Default)]
struct CaseStats {
    ops: u64,
    errors: u64,
    elapsed_ns: u128,
}

impl CaseStats {
    fn add(&mut self, other: &Self) {
        self.ops = self.ops.saturating_add(other.ops);
        self.errors = self.errors.saturating_add(other.errors);
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

    let targets = parse_targets(&args.targets)?;
    let cases = select_cases(&args.cases)?;
    let duration = Duration::from_secs(args.duration);
    let warmup = Duration::from_secs(args.warmup);

    println!(
        "redis-command-matrix: targets={} cases={} clients={} warmup={}s duration={}s",
        targets
            .iter()
            .map(|target| format!("{}={}", target.name, target.addr))
            .collect::<Vec<_>>()
            .join(","),
        cases.len(),
        args.clients,
        args.warmup,
        args.duration,
    );
    println!();
    println!("| target | family | command | case | ops/sec | avg us | errors |");
    println!("| --- | --- | --- | --- | ---: | ---: | ---: |");

    let mut csv = match args.csv.as_deref() {
        Some(path) => Some(std::fs::File::create(path)?),
        None => None,
    };
    if let Some(csv) = csv.as_mut() {
        writeln!(
            csv,
            "target,family,command,case,clients,duration_s,ops,ops_per_sec,avg_us,errors,profile"
        )?;
    }

    for target in targets {
        let stats = run_target(&target, &cases, args.clients, warmup, duration)?;
        for (case, stats) in cases.iter().zip(stats.iter()) {
            if args.fail_on_error && stats.errors > 0 {
                return Err(format!(
                    "{} {} produced {} RESP errors",
                    target.name, case.case_name, stats.errors
                )
                .into());
            }

            println!(
                "| {} | {} | {} | {} | {:.0} | {:.2} | {} |",
                target.name,
                case.family.label(),
                case.command_name,
                case.case_name,
                stats.ops_per_sec(duration),
                stats.avg_us(),
                stats.errors
            );
            if let Some(csv) = csv.as_mut() {
                writeln!(
                    csv,
                    "{},{},{},{},{},{},{},{:.3},{:.3},{},{}",
                    target.name,
                    case.family.label(),
                    case.command_name,
                    case.case_name,
                    args.clients,
                    args.duration,
                    stats.ops,
                    stats.ops_per_sec(duration),
                    stats.avg_us(),
                    stats.errors,
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
            Ok(Target {
                name: name.trim().to_string(),
                addr: addr.trim().to_string(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    if targets.is_empty() {
        return Err("at least one --targets entry is required".into());
    }
    Ok(targets)
}

fn select_cases(raw: &str) -> Result<Vec<RedisCommandCase>, BoxError> {
    let filters = raw
        .split(',')
        .map(str::trim)
        .filter(|filter| !filter.is_empty())
        .collect::<Vec<_>>();
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
    Ok(cases)
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
        return REDIS_COMMAND_CASES
            .iter()
            .chain(REDIS_COMMAND_LARGE_CASES.iter())
            .copied()
            .collect();
    }

    if let Some(family) = filter.strip_prefix("family:") {
        return REDIS_COMMAND_CASES
            .iter()
            .copied()
            .filter(|case| family.eq_ignore_ascii_case(case.family.label()))
            .collect();
    }

    let command_matches = REDIS_COMMAND_CASES
        .iter()
        .copied()
        .filter(|case| filter.eq_ignore_ascii_case(case.command_name))
        .collect::<Vec<_>>();
    if !command_matches.is_empty() {
        return command_matches;
    }

    let family_matches = REDIS_COMMAND_CASES
        .iter()
        .copied()
        .filter(|case| filter.eq_ignore_ascii_case(case.family.label()))
        .collect::<Vec<_>>();
    if !family_matches.is_empty() {
        return family_matches;
    }

    REDIS_COMMAND_CASES
        .iter()
        .copied()
        .filter(|case| filter.eq_ignore_ascii_case(case.case_name))
        .chain(
            REDIS_COMMAND_LARGE_CASES
                .iter()
                .copied()
                .filter(|case| filter.eq_ignore_ascii_case(case.case_name)),
        )
        .collect()
}

fn run_target(
    target: &Target,
    cases: &[RedisCommandCase],
    clients: usize,
    warmup: Duration,
    duration: Duration,
) -> Result<Vec<CaseStats>, BoxError> {
    RespConn::connect(&target.addr)?;

    let cases = Arc::new(cases.to_vec());
    let mut handles = Vec::with_capacity(clients);
    for worker_id in 0..clients {
        let addr = target.addr.clone();
        let cases = Arc::clone(&cases);
        handles.push(thread::spawn(move || {
            run_worker(worker_id, &addr, &cases, warmup, duration)
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
    addr: &str,
    cases: &[RedisCommandCase],
    warmup: Duration,
    duration: Duration,
) -> Result<Vec<CaseStats>, BoxError> {
    let mut conn = RespConn::connect(addr)?;
    let suffix = format!("worker:{worker_id}");
    run_setup(&mut conn, cases, &suffix)?;
    if !warmup.is_zero() {
        run_script(&mut conn, cases, &suffix, Instant::now() + warmup, None)?;
    }

    let deadline = Instant::now() + duration;
    let mut stats = vec![CaseStats::default(); cases.len()];
    run_script(&mut conn, cases, &suffix, deadline, Some(&mut stats))?;
    Ok(stats)
}

fn run_setup(
    conn: &mut RespConn,
    cases: &[RedisCommandCase],
    suffix: &str,
) -> Result<(), BoxError> {
    for case in cases {
        for parts in case.setup {
            if conn.execute(parts, suffix)? {
                return Err(format!("setup for `{}` produced a RESP error", case.case_name).into());
            }
        }
    }
    Ok(())
}

fn run_script(
    conn: &mut RespConn,
    cases: &[RedisCommandCase],
    suffix: &str,
    deadline: Instant,
    mut stats: Option<&mut [CaseStats]>,
) -> Result<(), BoxError> {
    while Instant::now() < deadline {
        for (index, case) in cases.iter().enumerate() {
            if Instant::now() >= deadline {
                break;
            }
            let started = Instant::now();
            let error = conn.execute(case.parts, suffix)?;
            let elapsed = started.elapsed();
            if let Some(stats) = stats.as_deref_mut() {
                let case_stats = &mut stats[index];
                case_stats.ops = case_stats.ops.saturating_add(1);
                case_stats.elapsed_ns = case_stats.elapsed_ns.saturating_add(elapsed.as_nanos());
                if error {
                    case_stats.errors = case_stats.errors.saturating_add(1);
                }
            }
        }
    }
    Ok(())
}

struct RespConn {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    line: Vec<u8>,
    command_cache: HashMap<CommandCacheKey, Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CommandCacheKey {
    ptr: usize,
    len: usize,
}

impl RespConn {
    fn connect(addr: &str) -> Result<Self, BoxError> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let reader = BufReader::with_capacity(64 * 1024, stream.try_clone()?);
        let writer = BufWriter::with_capacity(64 * 1024, stream);
        Ok(Self {
            reader,
            writer,
            line: Vec::with_capacity(128),
            command_cache: HashMap::new(),
        })
    }

    fn execute(&mut self, parts: &[&str], suffix: &str) -> Result<bool, BoxError> {
        self.write_command(parts, suffix)?;
        self.writer.flush()?;
        self.read_frame()
    }

    fn write_command(&mut self, parts: &[&str], suffix: &str) -> Result<(), BoxError> {
        let cache_key = CommandCacheKey {
            ptr: parts.as_ptr() as usize,
            len: parts.len(),
        };
        if !self.command_cache.contains_key(&cache_key) {
            let encoded = encode_command(parts, suffix)?;
            self.command_cache.insert(cache_key, encoded);
        }
        let encoded = self
            .command_cache
            .get(&cache_key)
            .expect("encoded command was just cached");
        self.writer.write_all(encoded)?;
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
        loop {
            let mut byte = [0_u8; 1];
            self.reader.read_exact(&mut byte)?;
            match byte[0] {
                b'\r' => {
                    self.reader.read_exact(&mut byte)?;
                    if byte[0] != b'\n' {
                        return Err("RESP line ended with CR without LF".into());
                    }
                    return Ok(());
                }
                other => self.line.push(other),
            }
        }
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

fn encode_command(parts: &[&str], suffix: &str) -> Result<Vec<u8>, BoxError> {
    let mut rewritten_parts = Vec::with_capacity(parts.len());
    for part in parts {
        append_rewritten_parts(part, suffix, &mut rewritten_parts)?;
    }

    let bytes = rewritten_parts
        .iter()
        .map(String::len)
        .sum::<usize>()
        .saturating_add(rewritten_parts.len().saturating_mul(16))
        .saturating_add(32);
    let mut encoded = Vec::with_capacity(bytes);
    write!(encoded, "*{}\r\n", rewritten_parts.len())?;
    for part in &rewritten_parts {
        write!(encoded, "${}\r\n", part.len())?;
        encoded.write_all(part.as_bytes())?;
        encoded.write_all(b"\r\n")?;
    }
    Ok(encoded)
}

fn append_rewritten_parts(part: &str, suffix: &str, out: &mut Vec<String>) -> Result<(), BoxError> {
    if let Some(key) = part.strip_prefix("$key:") {
        out.push(rewrite_key(key, suffix));
        return Ok(());
    }
    if let Some(size) = part.strip_prefix("$value:") {
        out.push(make_value(parse_token_count("$value", size)?));
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$members:") {
        for index in 0..parse_token_count("$members", count)? {
            out.push(format!("m:{index:06}"));
        }
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$list-values:") {
        for index in 0..parse_token_count("$list-values", count)? {
            out.push(format!("v:{index:06}"));
        }
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$hash-fields:") {
        for index in 0..parse_token_count("$hash-fields", count)? {
            out.push(format!("f:{index:06}"));
            out.push(format!("v:{index:06}"));
        }
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$zitems:") {
        for index in 0..parse_token_count("$zitems", count)? {
            out.push(index.to_string());
            out.push(format!("m:{index:06}"));
        }
        return Ok(());
    }
    if let Some(count) = part.strip_prefix("$kvpairs:") {
        for index in 0..parse_token_count("$kvpairs", count)? {
            out.push(rewrite_key(&format!("ks:{index:06}"), suffix));
            out.push(format!("v:{index:06}"));
        }
        return Ok(());
    }

    out.push(rewrite_part(part, suffix));
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

fn make_value(size: usize) -> String {
    const PATTERN: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._";
    let mut value = Vec::with_capacity(size);
    for index in 0..size {
        value.push(PATTERN[index % PATTERN.len()]);
    }
    String::from_utf8(value).expect("benchmark value pattern is ASCII")
}

fn rewrite_part(part: &str, suffix: &str) -> String {
    match is_probable_key(part) {
        true => rewrite_key(part, suffix),
        false => part.to_string(),
    }
}

fn rewrite_key(key: &str, suffix: &str) -> String {
    format!("{key}:{{{suffix}}}")
}

fn is_probable_key(part: &str) -> bool {
    matches!(
        part,
        "s" | "s-nx"
            | "s-del"
            | "exp"
            | "n"
            | "nf"
            | "ma"
            | "mb"
            | "mc"
            | "h"
            | "l"
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
            | "wrong"
    )
}
