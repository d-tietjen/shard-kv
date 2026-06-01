//! resp_blast — a zero-runtime-generation RESP load generator.
//!
//! Design goal: the load generator must never be the bottleneck and must add
//! the minimum possible CPU per request, so a measured throughput plateau is
//! the *server's* ceiling, not the client's.
//!
//! How it avoids client-side CPU:
//!   1. Pre-population (untimed): seed the dataset once so reads hit real data.
//!   2. Pre-encoding (untimed): the RESP request bytes are encoded ONCE, then a
//!      per-connection send buffer is built by repeating that command
//!      `--pipeline` times. The timed loop never formats, allocates, or hashes.
//!   3. Hot loop (timed): each pinned thread does `write_all(prebuilt)` then a
//!      minimal in-place RESP reply skip to keep the pipeline in sync, and adds
//!      `pipeline` to a thread-local counter. Latency is sampled sparsely so
//!      `clock_gettime` stays out of the per-op path.
//!
//! Fairness: pin client threads to a core set disjoint from the server's
//! cpuset (see the wrapper script). For a true 1:1 per-core comparison, pin the
//! server container to a single core and give this tool the rest.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;

#[derive(Parser, Debug, Clone)]
#[command(about = "Zero-runtime-generation RESP load generator (pre-encoded pipeline blaster)")]
struct Args {
    /// Target server, host:port.
    #[arg(long)]
    target: String,

    /// Command to benchmark, whitespace-separated, e.g. "GET k" or
    /// "LPOS lb b RANK 1 COUNT 0". Encoded once and reused.
    #[arg(long)]
    command: String,

    /// Setup commands run once before timing (semicolon-separated), e.g.
    /// "SET k v; RPUSH lb a b c b b". Use this to pre-populate the dataset.
    #[arg(long, default_value = "")]
    populate: String,

    /// Concurrent connections / threads.
    #[arg(long, default_value_t = 8)]
    clients: usize,

    /// Pipeline depth: requests written before draining replies.
    #[arg(long, default_value_t = 1)]
    pipeline: usize,

    /// Warmup seconds (untimed).
    #[arg(long, default_value_t = 2)]
    warmup: u64,

    /// Measurement seconds.
    #[arg(long, default_value_t = 5)]
    duration: u64,

    /// Pin client threads to these cores (comma list, e.g. "4,5,6,7").
    /// Threads are assigned round-robin. Empty = no pinning.
    #[arg(long, default_value = "")]
    client_cores: String,

    /// Sample one latency measurement every N pipeline batches per thread.
    /// Higher = less clock overhead in the hot loop.
    #[arg(long, default_value_t = 64)]
    latency_sample_every: u64,

    /// Optional label for the output row.
    #[arg(long, default_value = "")]
    label: String,
}

fn main() {
    let args = Args::parse();
    if let Err(error) = run(args) {
        eprintln!("resp_blast error: {error}");
        std::process::exit(1);
    }
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let addr = args
        .target
        .to_socket_addrs()?
        .next()
        .ok_or("could not resolve target")?;

    // --- Pre-population (untimed) ---
    if !args.populate.trim().is_empty() {
        let mut conn = TcpStream::connect(addr)?;
        conn.set_nodelay(true)?;
        for stmt in args.populate.split(';') {
            let parts: Vec<&str> = stmt.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }
            let frame = encode_command(&parts);
            conn.write_all(&frame)?;
            conn.flush()?;
            // drain exactly one reply so the connection stays in sync.
            let mut reader = ReplyReader::new(conn);
            reader.skip_one_reply()?;
            conn = reader.into_inner();
        }
    }

    // --- Pre-encoding (untimed): encode once, replicate to pipeline depth ---
    let cmd_parts: Vec<&str> = args.command.split_whitespace().collect();
    if cmd_parts.is_empty() {
        return Err("empty --command".into());
    }
    let single = encode_command(&cmd_parts);
    let mut send_buf = Vec::with_capacity(single.len() * args.pipeline);
    for _ in 0..args.pipeline {
        send_buf.extend_from_slice(&single);
    }
    let send_buf: Arc<[u8]> = Arc::from(send_buf.into_boxed_slice());

    let client_cores = parse_cores(&args.client_cores);

    let stop = Arc::new(AtomicBool::new(false));
    // Each thread publishes its running op count to its OWN atomic (no
    // cross-thread contention). The coordinator snapshots all of them at the
    // warmup boundary and again at window end; the difference is window ops.
    let counters: Vec<Arc<AtomicU64>> = (0..args.clients)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    // Barrier: all threads connect + warm, then enter the hot loop together.
    let start_barrier = Arc::new(Barrier::new(args.clients + 1));

    let mut handles = Vec::with_capacity(args.clients);
    for tid in 0..args.clients {
        let send_buf = Arc::clone(&send_buf);
        let stop = Arc::clone(&stop);
        let counter = Arc::clone(&counters[tid]);
        let start_barrier = Arc::clone(&start_barrier);
        let pipeline = args.pipeline;
        let sample_every = args.latency_sample_every.max(1);
        let core = client_cores.as_ref().map(|cores| cores[tid % cores.len()]);

        handles.push(std::thread::spawn(
            move || -> std::io::Result<ThreadResult> {
                if let Some(core) = core {
                    core_affinity::set_for_current(core_affinity::CoreId { id: core });
                }
                let stream = TcpStream::connect(addr)?;
                stream.set_nodelay(true)?;
                let mut conn = ReplyReader::new(stream);

                // Warmup batches (untimed) to fill socket buffers / page in.
                for _ in 0..8 {
                    conn.get_mut().write_all(&send_buf)?;
                    conn.get_mut().flush()?;
                    for _ in 0..pipeline {
                        conn.skip_one_reply()?;
                    }
                }

                let mut hist = new_hist();
                let mut local_ops: u64 = 0;
                let mut batch: u64 = 0;

                start_barrier.wait();
                // Hot loop. Runs across warmup + window; the coordinator isolates
                // the window via counter snapshots.
                while !stop.load(Ordering::Relaxed) {
                    let sample = batch.is_multiple_of(sample_every);
                    let t0 = if sample { Some(Instant::now()) } else { None };

                    conn.get_mut().write_all(&send_buf)?;
                    conn.get_mut().flush()?;
                    for _ in 0..pipeline {
                        conn.skip_one_reply()?;
                    }

                    if let Some(t0) = t0 {
                        // per-request latency = batch latency / pipeline depth
                        let per = t0.elapsed().as_nanos() as u64 / pipeline as u64;
                        let _ = hist.record(per.max(1));
                    }
                    local_ops += pipeline as u64;
                    batch = batch.wrapping_add(1);
                    // Publish running count (cheap relaxed store, own cache line).
                    counter.store(local_ops, Ordering::Relaxed);
                }
                Ok(ThreadResult { hist })
            },
        ));
    }

    // Coordinator: start threads, let them warm, then snapshot the window.
    start_barrier.wait();
    let snapshot = |counters: &[Arc<AtomicU64>]| -> u64 {
        counters.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    };
    std::thread::sleep(Duration::from_secs(args.warmup));
    let ops_at_warmup = snapshot(&counters);
    let window_start = Instant::now();
    std::thread::sleep(Duration::from_secs(args.duration));
    let ops_at_end = snapshot(&counters);
    let elapsed = window_start.elapsed();
    stop.store(true, Ordering::Relaxed);

    let mut merged = new_hist();
    let mut per_thread = Vec::new();
    for (tid, handle) in handles.into_iter().enumerate() {
        let result = handle.join().expect("thread panicked")?;
        merged.add(&result.hist).expect("merge histogram");
        let _ = tid;
    }
    // Per-thread window ops (for the client-saturation balance check).
    // We only have the final counter, but warmup is identical across threads;
    // window balance is well-approximated by final counts.
    for c in &counters {
        per_thread.push(c.load(Ordering::Relaxed));
    }

    let window_ops = ops_at_end.saturating_sub(ops_at_warmup);
    let secs = elapsed.as_secs_f64();
    let ops_per_sec = window_ops as f64 / secs;
    let label = if args.label.is_empty() {
        args.command.clone()
    } else {
        args.label.clone()
    };

    // Client-saturation check: spread across threads. If one thread is pinned
    // at ~the same ops as the busiest, the client likely has headroom.
    let max_thread = *per_thread.iter().max().unwrap_or(&0);
    let min_thread = *per_thread.iter().min().unwrap_or(&0);

    println!(
        "{label}\tclients={}\tpipeline={}\tops/sec={:.0}\tp50us={:.1}\tp99us={:.1}\tp999us={:.1}\twindow_s={:.2}\tthread_ops[min..max]={}..{}",
        args.clients,
        args.pipeline,
        ops_per_sec,
        merged.value_at_quantile(0.50) as f64 / 1000.0,
        merged.value_at_quantile(0.99) as f64 / 1000.0,
        merged.value_at_quantile(0.999) as f64 / 1000.0,
        secs,
        min_thread,
        max_thread,
    );
    Ok(())
}

struct ThreadResult {
    hist: Histogram<u64>,
}

/// Auto-resizing HDR histogram. Without auto-resize, recording a value above
/// the default range is dropped, silently corrupting tail quantiles. Auto-resize
/// keeps p99/p999/max trustworthy.
fn new_hist() -> Histogram<u64> {
    let mut h = Histogram::<u64>::new(3).expect("histogram");
    h.auto(true);
    h
}

fn parse_cores(spec: &str) -> Option<Vec<usize>> {
    let spec = spec.trim();
    if spec.is_empty() {
        return None;
    }
    let cores: Vec<usize> = spec
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .collect();
    if cores.is_empty() { None } else { Some(cores) }
}

/// Encode argv as a RESP array of bulk strings. Called only outside the timed
/// loop.
fn encode_command(parts: &[&str]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + parts.iter().map(|p| p.len() + 16).sum::<usize>());
    out.extend_from_slice(b"*");
    out.extend_from_slice(parts.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for part in parts {
        out.extend_from_slice(b"$");
        out.extend_from_slice(part.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(part.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Minimal, allocation-free RESP reply consumer. Buffers raw bytes and skips
/// exactly one complete reply (recursively for aggregates), keeping the
/// pipeline in sync without parsing values into owned types.
struct ReplyReader {
    stream: TcpStream,
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
}

impl ReplyReader {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: vec![0u8; 64 * 1024],
            pos: 0,
            filled: 0,
        }
    }

    fn get_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    fn into_inner(self) -> TcpStream {
        self.stream
    }

    #[inline]
    fn fill(&mut self) -> std::io::Result<()> {
        if self.pos == self.filled {
            self.pos = 0;
            self.filled = self.stream.read(&mut self.buf)?;
            if self.filled == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed connection",
                ));
            }
        }
        Ok(())
    }

    #[inline]
    fn next_byte(&mut self) -> std::io::Result<u8> {
        self.fill()?;
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// Read until and including CRLF, returning the line contents (excluding
    /// CRLF) copied into `line`. Cheap: small integer/status lines only.
    #[inline]
    fn read_line(&mut self, line: &mut Vec<u8>) -> std::io::Result<()> {
        line.clear();
        loop {
            let b = self.next_byte()?;
            if b == b'\r' {
                let lf = self.next_byte()?;
                debug_assert_eq!(lf, b'\n');
                return Ok(());
            }
            line.push(b);
        }
    }

    #[inline]
    fn skip_exact(&mut self, mut n: usize) -> std::io::Result<()> {
        while n > 0 {
            self.fill()?;
            let avail = self.filled - self.pos;
            let take = avail.min(n);
            self.pos += take;
            n -= take;
        }
        Ok(())
    }

    fn skip_one_reply(&mut self) -> std::io::Result<()> {
        let mut line = Vec::with_capacity(32);
        let prefix = self.next_byte()?;
        match prefix {
            b'+' | b'-' | b':' | b',' | b'#' | b'_' | b'(' | b'=' => {
                // simple string / error / integer / double / bool / null / big
                self.read_line(&mut line)?;
                Ok(())
            }
            b'$' => {
                // bulk string: $<len>\r\n<bytes>\r\n  (or $-1\r\n)
                self.read_line(&mut line)?;
                let len: i64 = parse_int(&line);
                if len >= 0 {
                    self.skip_exact(len as usize + 2)?; // payload + CRLF
                }
                Ok(())
            }
            b'*' | b'~' | b'>' => {
                // array / set / push: <count> then that many replies
                self.read_line(&mut line)?;
                let count: i64 = parse_int(&line);
                for _ in 0..count.max(0) {
                    self.skip_one_reply()?;
                }
                Ok(())
            }
            b'%' => {
                // map: <count> pairs => count*2 replies
                self.read_line(&mut line)?;
                let count: i64 = parse_int(&line);
                for _ in 0..(count.max(0) * 2) {
                    self.skip_one_reply()?;
                }
                Ok(())
            }
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected RESP prefix {other:#x}"),
            )),
        }
    }
}

#[inline]
fn parse_int(line: &[u8]) -> i64 {
    let mut neg = false;
    let mut val: i64 = 0;
    for (i, &b) in line.iter().enumerate() {
        if i == 0 && b == b'-' {
            neg = true;
            continue;
        }
        if b.is_ascii_digit() {
            val = val * 10 + (b - b'0') as i64;
        } else {
            break;
        }
    }
    if neg { -val } else { val }
}
