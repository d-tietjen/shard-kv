//! Rust load generator for ShardCache semantic RESP server benchmarks.

use std::error::Error;
use std::fs::File;
use std::hint::black_box;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use shardcache_benchmarks::cpu::{external_cpu_time, process_cpu_time, vcpu};
use shardcache_benchmarks::csv::CsvWriter;
use shardcache_benchmarks::histogram::LatencyHistogram;

type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, Parser)]
#[command(about = "ShardCache semantic RESP server load benchmark")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:6390")]
    addr: String,
    #[arg(long)]
    pairs_csv: String,
    #[arg(long, default_value = "semantic-fixture")]
    dataset: String,
    #[arg(long, default_value = "miss-cold")]
    scenario: String,
    #[arg(long, default_value_t = 100_000)]
    entries: usize,
    #[arg(long, default_value_t = 384)]
    dims: usize,
    #[arg(long, default_value_t = 0.35)]
    threshold: f32,
    #[arg(long, default_value = "fixture")]
    query_source: String,
    #[arg(long, default_value_t = 100_000)]
    query_pool: usize,
    #[arg(long, default_value_t = 0)]
    warmup_queries: usize,
    #[arg(long)]
    unique_queries: bool,
    #[arg(long, default_value_t = 16)]
    workers: usize,
    #[arg(long, default_value_t = 10.0)]
    seconds: f64,
    #[arg(long, default_value_t = 64)]
    pipeline: usize,
    #[arg(long, default_value_t = 0x5eed)]
    seed: u64,
    #[arg(long, default_value = "")]
    process_cpuset: String,
    #[arg(long, default_value = "")]
    external_pids: String,
    #[arg(long)]
    output: String,
    #[arg(long, default_value_t = 10_000)]
    progress_every: usize,
}

#[derive(Debug, Clone)]
struct Pair {
    cache_embedding: Vec<f32>,
    query_embedding: Vec<f32>,
    label: bool,
}

#[derive(Debug)]
struct Fixture {
    cache_vectors: Vec<Vec<f32>>,
    query_vectors: Vec<Vec<f32>>,
}

#[derive(Debug, Clone, Copy)]
struct CpuSnapshot {
    process: Duration,
    external: Duration,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let fixture = load_fixture(&args)?;
    populate(&args, &fixture)?;
    run_load(&args, &fixture)?;
    Ok(())
}

fn populate(args: &Args, fixture: &Fixture) -> Result<(), BoxError> {
    let mut conn = RespConn::connect(&args.addr)?;
    let pipeline = args.pipeline.max(1);
    let mut pending = 0usize;
    for index in 0..args.entries {
        if args.progress_every > 0 && index > 0 && index % args.progress_every == 0 {
            println!("shardcache-server queued {index}/{}", args.entries);
        }
        let key = format!("entry:{index}");
        let value = format!("value:{index}");
        let embedding = f32le_bytes(&fixture.cache_vectors[index % fixture.cache_vectors.len()]);
        conn.write_semantic_set(key.as_bytes(), value.as_bytes(), &embedding)?;
        pending += 1;
        if pending >= pipeline {
            conn.flush()?;
            for _ in 0..pending {
                conn.read_ok()?;
            }
            pending = 0;
        }
    }
    if pending > 0 {
        conn.flush()?;
        for _ in 0..pending {
            conn.read_ok()?;
        }
    }

    let min_score = min_score_bytes(args.threshold)?;
    let warmup = args.warmup_queries.min(fixture.query_vectors.len());
    for query in fixture.query_vectors.iter().take(warmup) {
        conn.write_semantic_search(&f32le_bytes(query), &min_score)?;
    }
    conn.flush()?;
    for _ in 0..warmup {
        black_box(conn.read_search_hit()?);
    }
    Ok(())
}

fn run_load(args: &Args, fixture: &Fixture) -> Result<(), BoxError> {
    let query_commands = Arc::new(
        fixture
            .query_vectors
            .iter()
            .map(|query| {
                let mut out = Vec::with_capacity(1600);
                write_semantic_search_frame(
                    &mut out,
                    &f32le_bytes(query),
                    &min_score_bytes(args.threshold).expect("threshold validated"),
                )
                .expect("vec write cannot fail");
                out
            })
            .collect::<Vec<_>>(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let shared_query_cursor = Arc::new(AtomicUsize::new(0));
    let pipeline = args.pipeline.max(1);
    let mut handles = Vec::with_capacity(args.workers);
    let cpu_start = cpu_snapshot(args);
    let start = Instant::now();

    for worker_id in 0..args.workers {
        let addr = args.addr.clone();
        let query_commands = Arc::clone(&query_commands);
        let stop = Arc::clone(&stop);
        let shared_query_cursor = Arc::clone(&shared_query_cursor);
        let unique_queries = args.unique_queries;
        handles.push(thread::spawn(
            move || -> Result<(u64, u64, LatencyHistogram), BoxError> {
                let mut conn = RespConn::connect(&addr)?;
                let mut histogram = LatencyHistogram::new();
                let mut queries = 0u64;
                let mut hits = 0u64;
                let mut index = worker_id % query_commands.len();
                while !stop.load(Ordering::Relaxed) {
                    let mut batch = Vec::with_capacity(pipeline);
                    for _ in 0..pipeline {
                        let next = if unique_queries {
                            let next = shared_query_cursor.fetch_add(1, Ordering::Relaxed);
                            if next >= query_commands.len() {
                                break;
                            }
                            next
                        } else {
                            let next = index;
                            index += 1;
                            if index == query_commands.len() {
                                index = 0;
                            }
                            next
                        };
                        batch.push(next);
                    }
                    if batch.is_empty() {
                        break;
                    }
                    let started = Instant::now();
                    for query_index in &batch {
                        conn.write_raw(&query_commands[*query_index])?;
                    }
                    conn.flush()?;
                    let mut batch_hits = 0u64;
                    for _ in &batch {
                        if conn.read_search_hit()? {
                            batch_hits = batch_hits.saturating_add(1);
                        }
                    }
                    let per_op_ns = (started.elapsed().as_nanos() / batch.len() as u128) as u64;
                    for _ in &batch {
                        histogram.record(per_op_ns);
                    }
                    queries = queries.saturating_add(batch.len() as u64);
                    hits = hits.saturating_add(batch_hits);
                }
                Ok((queries, hits, histogram))
            },
        ));
    }

    let load_duration = Duration::from_secs_f64(args.seconds);
    if args.unique_queries {
        while start.elapsed() < load_duration
            && shared_query_cursor.load(Ordering::Relaxed) < query_commands.len()
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

    let elapsed = start.elapsed();
    let cpu_end = cpu_snapshot(args);
    let process_cpu = cpu_end.process.saturating_sub(cpu_start.process);
    let external_cpu = cpu_end.external.saturating_sub(cpu_start.external);
    let total_cpu = process_cpu + external_cpu;
    let process_vcpu = vcpu(process_cpu, elapsed);
    let external_vcpu = vcpu(external_cpu, elapsed);
    let total_vcpu = vcpu(total_cpu, elapsed);
    let ops = total_queries as f64 / elapsed.as_secs_f64();
    let ops_per_sut_cpu = if external_vcpu > 0.0 {
        ops / external_vcpu
    } else {
        0.0
    };
    let ops_per_total_cpu = if total_vcpu > 0.0 {
        ops / total_vcpu
    } else {
        0.0
    };

    println!(
        "{}/shardcache-server-rust: ops/sec={:.0} ops/sut-cpu={:.0} p50={:.4}ms p95={:.4}ms p99={:.4}ms hits={}/{} total_vcpu={:.2} sut_vcpu={:.2} client_vcpu={:.2}",
        args.scenario,
        ops,
        ops_per_sut_cpu,
        histogram.p50_ns() as f64 / 1_000_000.0,
        histogram.p95_ns() as f64 / 1_000_000.0,
        histogram.p99_ns() as f64 / 1_000_000.0,
        total_hits,
        total_queries,
        total_vcpu,
        external_vcpu,
        process_vcpu,
    );

    let mut csv = CsvWriter::new(
        Some(&args.output),
        vec![
            "scenario",
            "adapter",
            "workers",
            "entries",
            "dims",
            "query_pool",
            "seconds",
            "queries",
            "hits",
            "ops_per_sec",
            "ops_per_cpu",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "process_cpu_seconds",
            "process_vcpu",
            "external_cpu_seconds",
            "external_vcpu",
            "total_cpu_seconds",
            "total_vcpu",
            "sut_cpu_seconds",
            "sut_vcpu",
            "client_cpu_seconds",
            "client_vcpu",
            "ops_per_sut_cpu",
            "ops_per_total_cpu",
            "process_cpuset",
            "external_pids",
        ],
    );
    csv.write_row(&[
        args.scenario.clone(),
        "shardcache-server".to_string(),
        args.workers.to_string(),
        args.entries.to_string(),
        args.dims.to_string(),
        args.query_pool.to_string(),
        format!("{:.6}", elapsed.as_secs_f64()),
        total_queries.to_string(),
        total_hits.to_string(),
        format!("{ops:.6}"),
        format!("{ops_per_total_cpu:.6}"),
        format!("{:.6}", histogram.p50_ns() as f64 / 1_000_000.0),
        format!("{:.6}", histogram.p95_ns() as f64 / 1_000_000.0),
        format!("{:.6}", histogram.p99_ns() as f64 / 1_000_000.0),
        format!("{:.6}", process_cpu.as_secs_f64()),
        format!("{process_vcpu:.6}"),
        format!("{:.6}", external_cpu.as_secs_f64()),
        format!("{external_vcpu:.6}"),
        format!("{:.6}", total_cpu.as_secs_f64()),
        format!("{total_vcpu:.6}"),
        format!("{:.6}", external_cpu.as_secs_f64()),
        format!("{external_vcpu:.6}"),
        format!("{:.6}", process_cpu.as_secs_f64()),
        format!("{process_vcpu:.6}"),
        format!("{ops_per_sut_cpu:.6}"),
        format!("{ops_per_total_cpu:.6}"),
        args.process_cpuset.clone(),
        args.external_pids.clone(),
    ])?;
    Ok(())
}

fn cpu_snapshot(args: &Args) -> CpuSnapshot {
    CpuSnapshot {
        process: process_cpu_time(),
        external: parse_external_pids(&args.external_pids)
            .into_iter()
            .filter_map(external_cpu_time)
            .sum(),
    }
}

fn load_fixture(args: &Args) -> Result<Fixture, BoxError> {
    let pairs = load_pairs_csv(&args.pairs_csv, &args.dataset)?;
    let dims = pairs
        .first()
        .map(|pair| pair.cache_embedding.len())
        .unwrap_or(args.dims);
    let cache_vectors = (0..args.entries)
        .map(|index| pairs[index % pairs.len()].cache_embedding.clone())
        .collect::<Vec<_>>();
    let query_vectors = match args.query_source.as_str() {
        "exact" => (0..args.query_pool)
            .map(|index| cache_vectors[index % cache_vectors.len()].clone())
            .collect(),
        "miss-random" => {
            let mut rng = SmallRng::seed_from_u64(args.seed ^ 0xbad5eed);
            (0..args.query_pool)
                .map(|_| random_unit_vector(dims, &mut rng))
                .collect()
        }
        "fixture" => (0..args.query_pool)
            .map(|index| pairs[index % pairs.len()].query_embedding.clone())
            .collect(),
        "fixture-positive" => pairs
            .iter()
            .filter(|pair| pair.label)
            .cycle()
            .take(args.query_pool)
            .map(|pair| pair.query_embedding.clone())
            .collect(),
        "fixture-negative" => pairs
            .iter()
            .filter(|pair| !pair.label)
            .cycle()
            .take(args.query_pool)
            .map(|pair| pair.query_embedding.clone())
            .collect(),
        other => return Err(format!("unsupported query source: {other}").into()),
    };
    Ok(Fixture {
        cache_vectors,
        query_vectors,
    })
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
        let (_dataset, _pair_id, label, cache_embedding, query_embedding) = match fields.as_slice()
        {
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
        pairs.push(Pair {
            cache_embedding,
            query_embedding,
            label,
        });
    }
    if pairs.is_empty() {
        return Err(format!("{path} did not contain any pairs").into());
    }
    Ok(pairs)
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

fn random_unit_vector(dims: usize, rng: &mut SmallRng) -> Vec<f32> {
    let mut values = (0..dims)
        .map(|_| rng.gen_range(-1.0f32..1.0f32))
        .collect::<Vec<_>>();
    normalize(&mut values);
    values
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

fn min_score_bytes(threshold: f32) -> Result<Vec<u8>, BoxError> {
    if !threshold.is_finite() || !(0.0..=2.0).contains(&threshold) {
        return Err(format!("cosine distance threshold must be in [0,2]: {threshold}").into());
    }
    Ok(format!("{:.8}", 1.0 - threshold).into_bytes())
}

fn f32le_bytes(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len().saturating_mul(4));
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

fn parse_external_pids(raw: &str) -> Vec<u32> {
    raw.split(',')
        .filter_map(|part| part.trim().parse::<u32>().ok())
        .collect()
}

struct RespConn {
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
    line: Vec<u8>,
}

impl RespConn {
    fn connect(addr: &str) -> Result<Self, BoxError> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let reader = stream.try_clone()?;
        Ok(Self {
            r: BufReader::with_capacity(64 * 1024, reader),
            w: BufWriter::with_capacity(64 * 1024, stream),
            line: Vec::with_capacity(64),
        })
    }

    fn write_raw(&mut self, bytes: &[u8]) -> Result<(), BoxError> {
        self.w.write_all(bytes)?;
        Ok(())
    }

    fn write_semantic_set(
        &mut self,
        key: &[u8],
        value: &[u8],
        embedding: &[u8],
    ) -> Result<(), BoxError> {
        write_array_header(&mut self.w, 4)?;
        write_bulk(&mut self.w, b"SEMANTIC.SET")?;
        write_bulk(&mut self.w, key)?;
        write_bulk(&mut self.w, value)?;
        write_bulk(&mut self.w, embedding)?;
        Ok(())
    }

    fn write_semantic_search(
        &mut self,
        embedding: &[u8],
        min_score: &[u8],
    ) -> Result<(), BoxError> {
        write_semantic_search_frame(&mut self.w, embedding, min_score)
    }

    fn flush(&mut self) -> Result<(), BoxError> {
        self.w.flush()?;
        Ok(())
    }

    fn read_ok(&mut self) -> Result<(), BoxError> {
        read_line(&mut self.r, &mut self.line)?;
        match self.line.first().copied() {
            Some(b'+') => Ok(()),
            Some(b'-') => {
                Err(format!("RESP error: {}", String::from_utf8_lossy(&self.line)).into())
            }
            _ => Err(format!(
                "unexpected response: {}",
                String::from_utf8_lossy(&self.line)
            )
            .into()),
        }
    }

    fn read_search_hit(&mut self) -> Result<bool, BoxError> {
        read_frame_hit(&mut self.r, &mut self.line)
    }
}

fn write_semantic_search_frame<W: Write>(
    w: &mut W,
    embedding: &[u8],
    min_score: &[u8],
) -> Result<(), BoxError> {
    write_array_header(w, 3)?;
    write_bulk(w, b"SEMANTIC.SEARCH")?;
    write_bulk(w, embedding)?;
    write_bulk(w, min_score)?;
    Ok(())
}

fn write_array_header<W: Write>(w: &mut W, n: usize) -> Result<(), BoxError> {
    w.write_all(b"*")?;
    write_len(w, n as i64)?;
    Ok(())
}

fn write_bulk<W: Write>(w: &mut W, value: &[u8]) -> Result<(), BoxError> {
    w.write_all(b"$")?;
    write_len(w, value.len() as i64)?;
    w.write_all(value)?;
    w.write_all(b"\r\n")?;
    Ok(())
}

fn write_len<W: Write>(w: &mut W, n: i64) -> Result<(), BoxError> {
    w.write_all(n.to_string().as_bytes())?;
    w.write_all(b"\r\n")?;
    Ok(())
}

fn read_frame_hit<R: Read>(r: &mut BufReader<R>, line: &mut Vec<u8>) -> Result<bool, BoxError> {
    read_line(r, line)?;
    match line.first().copied() {
        Some(b'$') => {
            let len = parse_len(&line[1..])?;
            if len < 0 {
                return Ok(false);
            }
            skip_bytes(r, len as usize + 2)?;
            Ok(true)
        }
        Some(b'*') => {
            let len = parse_len(&line[1..])?;
            if len < 0 {
                return Ok(false);
            }
            for _ in 0..len {
                skip_frame(r, line)?;
            }
            Ok(true)
        }
        Some(b'-') => Err(format!("RESP error: {}", String::from_utf8_lossy(line)).into()),
        Some(b'+') | Some(b':') => Ok(true),
        _ => Err(format!("unexpected RESP frame: {}", String::from_utf8_lossy(line)).into()),
    }
}

fn skip_frame<R: Read>(r: &mut BufReader<R>, line: &mut Vec<u8>) -> Result<(), BoxError> {
    read_line(r, line)?;
    match line.first().copied() {
        Some(b'$') => {
            let len = parse_len(&line[1..])?;
            if len >= 0 {
                skip_bytes(r, len as usize + 2)?;
            }
        }
        Some(b'*') => {
            let len = parse_len(&line[1..])?;
            if len >= 0 {
                for _ in 0..len {
                    skip_frame(r, line)?;
                }
            }
        }
        Some(b'+') | Some(b':') => {}
        Some(b'-') => return Err(format!("RESP error: {}", String::from_utf8_lossy(line)).into()),
        _ => {
            return Err(format!("unexpected RESP frame: {}", String::from_utf8_lossy(line)).into());
        }
    }
    Ok(())
}

fn skip_bytes<R: Read>(r: &mut R, mut len: usize) -> Result<(), BoxError> {
    let mut buf = [0u8; 8192];
    while len > 0 {
        let take = len.min(buf.len());
        r.read_exact(&mut buf[..take])?;
        len -= take;
    }
    Ok(())
}

fn read_line<R: Read>(r: &mut BufReader<R>, line: &mut Vec<u8>) -> Result<(), BoxError> {
    line.clear();
    let read = r.read_until(b'\n', line)?;
    if read == 0 {
        return Err("RESP connection closed".into());
    }
    if line.ends_with(b"\r\n") {
        line.truncate(line.len() - 2);
        return Ok(());
    }
    Err("RESP framing: expected CRLF".into())
}

fn parse_len(raw: &[u8]) -> Result<i64, BoxError> {
    std::str::from_utf8(raw)?
        .parse::<i64>()
        .map_err(|error| format!("invalid RESP length: {error}").into())
}
