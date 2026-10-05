//! Finite, source-neutral RESP diagnostics. No product storage API is linked here.
//! Timing includes request construction and complete validated wire operations.
//! Worker ranges and trace digests are canonical; concurrent wire interleaving is not.
use clap::Parser;
use hdrhistogram::Histogram;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const SEED: u64 = 0x4544_454e_2266_0001;
const CLIENTS: usize = 16;
const TTL_MS: i64 = 7_200_000;
const DEADLINE_SECS: u64 = 900;
const IDS: [&str; 24] = [
    "key63",
    "key64",
    "key65",
    "value0",
    "value15",
    "value17",
    "value255",
    "value257",
    "cardinality1k",
    "cardinality1m",
    "mixed-compressible",
    "mixed-entropy",
    "classes-compressible",
    "classes-entropy",
    "overwrite-compressible",
    "overwrite-entropy",
    "resize-compressible",
    "resize-entropy",
    "half-compressible",
    "half-entropy",
    "groups-compressible",
    "groups-entropy",
    "metadata",
    "types",
];

#[derive(Parser)]
struct Args {
    #[arg(long)]
    scenario: String,
    /// Only loopback endpoints are accepted; no arbitrary target host.
    #[arg(long)]
    addr: Option<SocketAddr>,
    /// Print the pure finite contract without connecting or executing commands.
    #[arg(long)]
    describe: bool,
    /// Verify an existing saturation keyspace instead of mutating it.
    #[arg(long)]
    verify_saturation: bool,
}

#[derive(Clone, Debug, Serialize)]
struct Scenario {
    id: String,
    keys: usize,
    key_length: usize,
    value_length: usize,
    pattern: String,
    family: String,
    saturation: bool,
}
impl Scenario {
    fn parse(id: &str, saturation: bool) -> Result<Self> {
        let mut s = Self {
            id: id.into(),
            keys: 100_000,
            key_length: 18,
            value_length: 16,
            pattern: "high-entropy".into(),
            family: "fixed".into(),
            saturation,
        };
        if saturation && id.starts_with("core-v") {
            let rest = id.strip_prefix("core-v").unwrap();
            let (size, pattern) = rest
                .split_once('-')
                .ok_or("invalid core verification selector")?;
            s.value_length = size.parse()?;
            if ![16, 64, 256, 1024, 4096].contains(&s.value_length)
                || !["compressible", "high-entropy"].contains(&pattern)
            {
                return Err("unknown core verification selector".into());
            }
            s.pattern = pattern.into();
            s.family = "access".into();
            return Ok(s);
        }
        if saturation {
            let parts: Vec<_> = id.split('-').collect();
            if parts.len() != 2
                || !["v16", "v64"].contains(&parts[0])
                || !["get", "set", "hot", "pipeline"].contains(&parts[1])
            {
                return Err("unknown access scenario".into());
            }
            if parts[0] == "v64" {
                s.value_length = 64;
                s.pattern = "compressible".into();
            }
            s.family = "access".into();
            return Ok(s);
        }
        if !IDS.contains(&id) {
            return Err("unknown stateful scenario".into());
        }
        match id {
            "key63" => s.key_length = 63,
            "key64" => s.key_length = 64,
            "key65" => s.key_length = 65,
            "value0" => s.value_length = 0,
            "value15" => s.value_length = 15,
            "value17" => s.value_length = 17,
            "value255" => s.value_length = 255,
            "value257" => s.value_length = 257,
            "cardinality1k" => s.keys = 1_000,
            "cardinality1m" => s.keys = 1_000_000,
            "metadata" | "types" => s.family = id.into(),
            _ => {
                s.family = id.split('-').next().unwrap().into();
                if id.ends_with("compressible") {
                    s.pattern = "compressible".into();
                }
                if s.family == "overwrite" {
                    s.value_length = 64;
                }
            }
        }
        Ok(s)
    }
    fn phases(&self) -> Vec<Phase> {
        let mut p = vec![Phase {
            id: "load".into(),
            step: 0,
        }];
        match self.family.as_str() {
            "overwrite" => {
                for n in 1..=10 {
                    p.push(Phase {
                        id: format!("overwrite-{n}"),
                        step: n,
                    });
                }
            }
            "resize" => {
                for n in 1..=12 {
                    p.push(Phase {
                        id: format!("resize-{n}"),
                        step: n,
                    });
                }
            }
            "half" | "groups" => {
                for n in 1..=6 {
                    p.push(Phase {
                        id: format!("{}-{n}", self.family),
                        step: n,
                    });
                }
            }
            "metadata" => {
                for (n, id) in ["expire", "persist", "delete", "reinsert"]
                    .iter()
                    .enumerate()
                {
                    p.push(Phase {
                        id: (*id).into(),
                        step: n + 1,
                    });
                }
            }
            "types" => {
                for (n, id) in ["typed", "delete", "reinsert"].iter().enumerate() {
                    p.push(Phase {
                        id: (*id).into(),
                        step: n + 1,
                    });
                }
            }
            _ => {}
        }
        if self.saturation {
            p = vec![Phase {
                id: "verify".into(),
                step: 0,
            }];
        }
        p
    }
    fn record(&self, index: usize, phase: &Phase) -> Record {
        let mut r = Record {
            key: fixed_key(index, self.key_length),
            value_length: self.value_length,
            generation: 0,
            kind: Kind::String,
            expiring: false,
        };
        if self.saturation {
            r.key = format!("k:{index:016x}").into_bytes();
        }
        match self.family.as_str() {
            "mixed" => {
                let band = index % 10;
                r.key = fixed_key(index, [8, 18, 63, 64][index % 4]);
                r.value_length = match band {
                    0..=2 => 16,
                    3..=5 => 64,
                    6..=7 => 256,
                    8 => 1024,
                    _ => 4096,
                };
            }
            "classes" => {
                let class = if index < 9 {
                    index + 1
                } else {
                    10 + (index - 9) % 151
                };
                let stride = class * 2;
                let k = if class < 10 {
                    class - 1
                } else {
                    (stride.saturating_sub(256)).max(18)
                };
                r.key = class_key(index, class, k);
                r.value_length = stride - k;
            }
            "overwrite" => r.generation = phase.step,
            "resize" => {
                r.value_length = [16, 64, 255, 257][phase.step % 4];
                r.generation = phase.step;
            }
            "half" => {
                if index % 2 == 0 {
                    if phase.step % 2 == 1 {
                        r.kind = Kind::Absent;
                    }
                    r.generation = phase.step / 2;
                }
            }
            "groups" => {
                if index % 4 != 3 {
                    if phase.step % 2 == 1 {
                        r.kind = Kind::Absent;
                    }
                    r.value_length = [16, 64, 255, 17][phase.step / 2];
                    r.generation = phase.step / 2;
                }
            }
            "metadata" => {
                if index % 10 == 0 {
                    r.expiring = phase.step == 1;
                    if phase.step == 3 {
                        r.kind = Kind::Absent;
                    }
                    if phase.step == 4 {
                        r.generation = 1;
                    }
                }
            }
            "types" => {
                if index % 10 == 0 {
                    r.kind = match phase.step {
                        1 => {
                            if index % 20 == 0 {
                                Kind::Hash
                            } else {
                                Kind::List
                            }
                        }
                        2 => Kind::Absent,
                        _ => Kind::String,
                    };
                    if phase.step == 3 {
                        r.generation = 1;
                    }
                }
            }
            _ => {}
        }
        r
    }
}
#[derive(Clone, Debug, Serialize)]
struct Phase {
    id: String,
    step: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
enum Kind {
    Absent,
    String,
    Hash,
    List,
}
struct Record {
    key: Vec<u8>,
    value_length: usize,
    generation: usize,
    kind: Kind,
    expiring: bool,
}
fn fixed_key(index: usize, length: usize) -> Vec<u8> {
    assert!(length >= 8);
    let mut key = vec![b'k'; length];
    key[length - 8..].copy_from_slice(&(index as u64).to_be_bytes());
    key
}
fn class_key(index: usize, class: usize, length: usize) -> Vec<u8> {
    if length < 18 {
        return vec![class as u8; length];
    }
    let mut key = fixed_key(index, length);
    key[..2].copy_from_slice(&(class as u16).to_be_bytes());
    key
}
fn payload(size: usize, pattern: &str, generation: usize) -> Vec<u8> {
    if pattern == "compressible" {
        return vec![b'x' + (generation % 26) as u8; size];
    }
    let mut state = SEED.wrapping_add((generation as u64).wrapping_mul(0x1000_0000));
    let mut value = Vec::with_capacity(size);
    while value.len() < size {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut word = state;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        word ^= word >> 31;
        value.extend_from_slice(&word.to_le_bytes()[..(size - value.len()).min(8)]);
    }
    value
}
fn frame(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}
fn hex(hash: Sha256) -> String {
    format!("{:x}", hash.finalize())
}
fn state_frame(hash: &mut Sha256, r: &Record, value: &[u8]) {
    frame(hash, &r.key);
    frame(hash, &[r.kind as u8]);
    frame(hash, &[u8::from(r.expiring)]);
    frame(hash, if r.kind == Kind::Absent { &[] } else { value });
}
#[derive(Debug, PartialEq)]
enum Reply {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
}
fn line<R: BufRead>(r: &mut R) -> Result<Vec<u8>> {
    let mut line = Vec::new();
    let mut limited = r.take(8193);
    let n = limited.read_until(b'\n', &mut line)?;
    if n > 8192 || !line.ends_with(b"\r\n") {
        return Err("missing/oversized RESP line".into());
    }
    line.truncate(line.len() - 2);
    Ok(line)
}
fn number(bytes: &[u8]) -> Result<i64> {
    let s = std::str::from_utf8(bytes)?;
    let n = s.parse::<i64>()?;
    if n.to_string() != s {
        return Err("noncanonical RESP integer".into());
    }
    Ok(n)
}
fn reply<R: BufRead>(r: &mut R, depth: usize) -> Result<Reply> {
    if depth > 4 {
        return Err("RESP nesting bound".into());
    }
    let l = line(r)?;
    let (&tag, rest) = l.split_first().ok_or("empty RESP line")?;
    Ok(match tag {
        b'+' => Reply::Simple(rest.to_vec()),
        b'-' => Reply::Error(rest.to_vec()),
        b':' => Reply::Integer(number(rest)?),
        b'$' => {
            let n = number(rest)?;
            if n == -1 {
                Reply::Bulk(None)
            } else {
                if !(0..=8192).contains(&n) {
                    return Err("RESP bulk bound".into());
                }
                let mut data = vec![0; n as usize];
                r.read_exact(&mut data)?;
                let mut crlf = [0; 2];
                r.read_exact(&mut crlf)?;
                if crlf != *b"\r\n" {
                    return Err("RESP bulk terminator".into());
                }
                Reply::Bulk(Some(data))
            }
        }
        b'*' => {
            let n = number(rest)?;
            if !(0..=16).contains(&n) {
                return Err("RESP array bound".into());
            }
            let mut a = Vec::new();
            for _ in 0..n {
                a.push(reply(r, depth + 1)?);
            }
            Reply::Array(a)
        }
        _ => return Err("unsupported RESP reply tag".into()),
    })
}
struct Conn {
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
    trace: Sha256,
    command_counts: std::collections::BTreeMap<String, u64>,
}
impl Conn {
    fn connect(addr: SocketAddr) -> Result<Self> {
        let s = TcpStream::connect_timeout(&addr, Duration::from_secs(10))?;
        s.set_nodelay(true)?;
        s.set_read_timeout(Some(Duration::from_secs(10)))?;
        s.set_write_timeout(Some(Duration::from_secs(10)))?;
        Ok(Self {
            r: BufReader::new(s.try_clone()?),
            w: BufWriter::new(s),
            trace: Sha256::new(),
            command_counts: std::collections::BTreeMap::new(),
        })
    }
    fn command(&mut self, parts: &[&[u8]]) -> Result<Reply> {
        *self
            .command_counts
            .entry(std::str::from_utf8(parts[0])?.to_owned())
            .or_default() += 1;
        self.trace.update((parts.len() as u64).to_be_bytes());
        write!(self.w, "*{}\r\n", parts.len())?;
        for part in parts {
            frame(&mut self.trace, part);
            write!(self.w, "${}\r\n", part.len())?;
            self.w.write_all(part)?;
            self.w.write_all(b"\r\n")?;
        }
        self.w.flush()?;
        reply(&mut self.r, 0)
    }
}
trait Wire {
    fn command(&mut self, parts: &[&[u8]]) -> Result<Reply>;
}
impl Wire for Conn {
    fn command(&mut self, parts: &[&[u8]]) -> Result<Reply> {
        Conn::command(self, parts)
    }
}

fn expect(actual: Reply, expected: Reply) -> Result<()> {
    if actual != expected {
        return Err(format!("RESP mismatch: got {actual:?}, expected {expected:?}").into());
    }
    Ok(())
}
fn ok(c: &mut impl Wire, parts: &[&[u8]]) -> Result<()> {
    expect(c.command(parts)?, Reply::Simple(b"OK".to_vec()))
}
fn integer(c: &mut impl Wire, parts: &[&[u8]], value: i64) -> Result<()> {
    expect(c.command(parts)?, Reply::Integer(value))
}
fn mutate(c: &mut impl Wire, s: &Scenario, p: &Phase, i: usize) -> Result<u64> {
    let r = s.record(i, p);
    let value = payload(r.value_length, &s.pattern, r.generation);
    if p.step == 0 {
        ok(c, &[b"SET", &r.key, &value])?;
        return Ok(1);
    }
    let affected = match s.family.as_str() {
        "half" => i % 2 == 0,
        "groups" => i % 4 != 3,
        "metadata" | "types" => i % 10 == 0,
        _ => true,
    };
    if !affected {
        return Ok(0);
    }
    if r.kind == Kind::Absent {
        integer(c, &[b"DEL", &r.key], 1)?;
        return Ok(1);
    }
    match s.family.as_str() {
        "metadata" if p.step == 1 => {
            integer(c, &[b"PEXPIRE", &r.key, TTL_MS.to_string().as_bytes()], 1)?;
        }
        "metadata" if p.step == 2 => {
            integer(c, &[b"PERSIST", &r.key], 1)?;
        }
        "types" if p.step == 1 => {
            integer(c, &[b"DEL", &r.key], 1)?;
            if r.kind == Kind::Hash {
                integer(c, &[b"HSET", &r.key, b"f", &value], 1)?;
            } else {
                integer(c, &[b"RPUSH", &r.key, &value], 1)?;
            }
            return Ok(2);
        }
        _ => ok(c, &[b"SET", &r.key, &value])?,
    }
    Ok(1)
}
fn verify(c: &mut impl Wire, s: &Scenario, p: &Phase, i: usize, hash: &mut Sha256) -> Result<u64> {
    let r = s.record(i, p);
    let value = payload(r.value_length, &s.pattern, r.generation);
    let actual = c.command(&[b"GET", &r.key])?;
    let mut ops = 1;
    match r.kind {
        Kind::Absent => expect(actual, Reply::Bulk(None))?,
        Kind::String => {
            expect(actual, Reply::Bulk(Some(value.clone())))?;
            let ttl = c.command(&[b"PTTL", &r.key])?;
            ops += 1;
            if r.expiring {
                if !matches!(ttl, Reply::Integer(v) if (TTL_MS - 900_000..=TTL_MS).contains(&v)) {
                    return Err("expiration bound/identity mismatch".into());
                }
            } else {
                expect(ttl, Reply::Integer(-1))?;
            }
        }
        Kind::Hash | Kind::List => {
            if !matches!(actual, Reply::Error(ref e) if e.starts_with(b"WRONGTYPE ")) {
                return Err("expected typed WRONGTYPE".into());
            }
            let type_name = if r.kind == Kind::Hash {
                b"hash".as_slice()
            } else {
                b"list".as_slice()
            };
            expect(
                c.command(&[b"TYPE", &r.key])?,
                Reply::Simple(type_name.to_vec()),
            )?;
            ops += 1;
            if r.kind == Kind::Hash {
                expect(
                    c.command(&[b"HGETALL", &r.key])?,
                    Reply::Array(vec![
                        Reply::Bulk(Some(b"f".to_vec())),
                        Reply::Bulk(Some(value.clone())),
                    ]),
                )?;
            } else {
                expect(
                    c.command(&[b"LRANGE", &r.key, b"0", b"-1"])?,
                    Reply::Array(vec![Reply::Bulk(Some(value.clone()))]),
                )?;
            }
            expect(c.command(&[b"PTTL", &r.key])?, Reply::Integer(-1))?;
            ops += 2;
        }
    }
    // A frame is accepted only after the actual reply equals this exact expected state.
    state_frame(hash, &r, &value);
    Ok(ops)
}
#[derive(Serialize)]
struct Timing {
    wire_commands: u64,
    completed_records: u64,
    elapsed_ns: u64,
    wire_commands_per_sec: f64,
    completed_records_per_sec: f64,
    latency_unit: &'static str,
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    maximum_ns: u64,
}
#[derive(Serialize)]
struct PhaseEvent {
    schema: u32,
    event: &'static str,
    pid: u32,
    scenario: String,
    phase: String,
    control_commands: std::collections::BTreeMap<String, u64>,
    live_keys: usize,
    absent_keys: usize,
    logical_bytes: usize,
    state_sha256: String,
    mutation_trace_sha256: String,
    verification_trace_sha256: String,
    mutation: Timing,
    verification: Timing,
    mutation_command_counts: std::collections::BTreeMap<String, u64>,
    verification_command_counts: std::collections::BTreeMap<String, u64>,
}
fn timed(
    s: &Scenario,
    p: &Phase,
    addr: SocketAddr,
    mutation: bool,
    started: Instant,
) -> Result<(
    Timing,
    String,
    String,
    std::collections::BTreeMap<String, u64>,
)> {
    let begin = Instant::now();
    let results = thread::scope(|scope| -> Result<Vec<_>> {
        let mut handles = Vec::new();
        for worker in 0..CLIENTS {
            handles.push(scope.spawn(move || -> Result<_> {
                let mut c = Conn::connect(addr)?;
                let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
                let mut operations = 0;
                let mut state = Sha256::new();
                for i in s.keys * worker / CLIENTS..s.keys * (worker + 1) / CLIENTS {
                    if started.elapsed().as_secs() >= DEADLINE_SECS {
                        return Err("native runtime deadline".into());
                    }
                    let t = Instant::now();
                    let n = if mutation {
                        mutate(&mut c, s, p, i)?
                    } else {
                        verify(&mut c, s, p, i, &mut state)?
                    };
                    if n > 0 {
                        histogram.record(t.elapsed().as_nanos().max(1).try_into()?)?;
                    }
                    operations += n;
                }
                Ok((
                    operations,
                    histogram,
                    hex(c.trace),
                    hex(state),
                    c.command_counts,
                ))
            }));
        }
        handles
            .into_iter()
            .map(|h| match h.join() {
                Ok(v) => v,
                Err(_) => Err("native worker panicked".into()),
            })
            .collect()
    })?;
    let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    let mut trace = Sha256::new();
    let mut state = Sha256::new();
    let mut operations = 0;
    let mut command_counts = std::collections::BTreeMap::new();
    for (n, h, tr, st, counts) in results {
        for (command, n) in counts {
            *command_counts.entry(command).or_insert(0u64) += n;
        }
        operations += n;
        histogram.add(&h)?;
        frame(&mut trace, tr.as_bytes());
        frame(&mut state, st.as_bytes());
    }
    let elapsed_ns: u64 = begin.elapsed().as_nanos().try_into()?;
    Ok((
        Timing {
            wire_commands: operations,
            completed_records: histogram.len(),
            elapsed_ns,
            wire_commands_per_sec: operations as f64 * 1e9 / elapsed_ns.max(1) as f64,
            completed_records_per_sec: histogram.len() as f64 * 1e9 / elapsed_ns.max(1) as f64,
            latency_unit: if mutation {
                "logical-record-transition"
            } else {
                "full-record-verification"
            },
            p50_ns: histogram.value_at_quantile(0.5),
            p99_ns: histogram.value_at_quantile(0.99),
            p999_ns: histogram.value_at_quantile(0.999),
            maximum_ns: histogram.max(),
        },
        hex(trace),
        hex(state),
        command_counts,
    ))
}
fn expected(s: &Scenario, p: &Phase) -> (usize, usize, String) {
    let mut live = 0;
    let mut logical = 0;
    let mut outer = Sha256::new();
    for worker in 0..CLIENTS {
        let mut inner = Sha256::new();
        for i in s.keys * worker / CLIENTS..s.keys * (worker + 1) / CLIENTS {
            let r = s.record(i, p);
            let value = payload(r.value_length, &s.pattern, r.generation);
            if r.kind != Kind::Absent {
                live += 1;
                logical += r.key.len() + value.len() + usize::from(r.kind == Kind::Hash);
            }
            state_frame(&mut inner, &r, &value);
        }
        frame(&mut outer, hex(inner).as_bytes());
    }
    (live, logical, hex(outer))
}
fn emit(value: &impl Serialize) -> Result<()> {
    let stdout = io::stdout();
    let mut out = stdout.lock();
    serde_json::to_writer(&mut out, value)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}
fn acknowledge() -> Result<()> {
    let mut s = String::new();
    let n = io::stdin().lock().take(32).read_line(&mut s)?;
    if n == 0 || s != "continue\n" {
        return Err("missing/exact phase acknowledgment".into());
    }
    Ok(())
}
fn main() -> Result<()> {
    let args = Args::parse();
    if args.scenario == HWM_ID || args.scenario == HWM_STEADY_ID {
        return hwm_main(args);
    }
    let s = Scenario::parse(&args.scenario, args.verify_saturation)?;
    let phases = s.phases();
    let mut distribution = std::collections::BTreeMap::new();
    for i in 0..s.keys {
        let r = s.record(i, &phases[0]);
        *distribution
            .entry((r.key.len(), r.value_length))
            .or_insert(0usize) += 1;
    }
    let distribution: Vec<_> = distribution.into_iter().map(|((k,v),n)| serde_json::json!({"key_bytes":k,"value_bytes":v,"records":n,"logical_bytes":n*(k+v),"compact_class":if k <=64 && v<=256 {Some((k+v).max(1).div_ceil(2))} else {None}})).collect();
    let contract = serde_json::json!({"schema": 1, "event": "ready", "pid": std::process::id(),
        "scenario": s, "seed": SEED, "clients": CLIENTS, "pipeline": 1, "deadline_seconds": DEADLINE_SECS,
        "phases": phases, "distribution":distribution, "initial_dbsize_checks":usize::from(!s.saturation), "timing_scope": "mutation: native request construction through complete transition replies; verification commands have a separate interval; percentile unit is one completed record transaction, which may contain multiple wire commands", "trace_order": "sixteen canonical contiguous worker ranges; actual concurrent interleaving is not hashed"});
    if args.describe {
        if args.addr.is_some() {
            return Err("describe cannot accept endpoint".into());
        }
        emit(&contract)?;
        return Ok(());
    }
    let addr = args.addr.ok_or("missing endpoint")?;
    if !addr.ip().is_loopback() || addr.port() == 0 {
        return Err("endpoint must be loopback and nonzero".into());
    }
    let started = Instant::now();
    let mut control = Conn::connect(addr)?;
    if !args.verify_saturation {
        integer(&mut control, &[b"DBSIZE"], 0)?;
    }
    emit(&contract)?;
    acknowledge()?;
    for p in &phases {
        let empty = || Timing {
            wire_commands: 0,
            completed_records: 0,
            elapsed_ns: 0,
            wire_commands_per_sec: 0.0,
            completed_records_per_sec: 0.0,
            latency_unit: "absent",
            p50_ns: 0,
            p99_ns: 0,
            p999_ns: 0,
            maximum_ns: 0,
        };
        let (mutation, trace, _, mutation_command_counts) = if args.verify_saturation {
            (
                empty(),
                hex(Sha256::new()),
                String::new(),
                std::collections::BTreeMap::new(),
            )
        } else {
            timed(&s, p, addr, true, started)?
        };
        let (verification, verification_trace, state, verification_command_counts) =
            timed(&s, p, addr, false, started)?;
        let (live, logical, expected_state) = expected(&s, p);
        if state != expected_state {
            return Err("full verified state digest differs".into());
        }
        integer(&mut control, &[b"DBSIZE"], live as i64)?;
        emit(&PhaseEvent {
            schema: 1,
            event: "phase",
            pid: std::process::id(),
            scenario: s.id.clone(),
            phase: p.id.clone(),
            control_commands: [("DBSIZE".into(), 1)].into_iter().collect(),
            live_keys: live,
            absent_keys: s.keys - live,
            logical_bytes: logical,
            state_sha256: state,
            mutation_trace_sha256: trace,
            verification_trace_sha256: verification_trace,
            mutation,
            verification,
            mutation_command_counts,
            verification_command_counts,
        })?;
        acknowledge()?;
        if started.elapsed().as_secs() >= DEADLINE_SECS {
            return Err("native runtime deadline".into());
        }
    }
    emit(
        &serde_json::json!({"schema":1,"event":"complete","pid":std::process::id(),"scenario":s.id,"phases":phases.len(),"errors":0}),
    )?;
    Ok(())
}

// The original catalog, seed, payloads and event schema above stay unchanged.
// This finite selector is a new driver contract, independent of server source.
const HWM_ID: &str = "storage-hwm-str-v1";
const HWM_STEADY_ID: &str = "storage-hwm-str-steady-v1";
const HWM_KEYS: usize = 1_000_000;
const HWM_SEED: u64 = 0xC0FFEE;
const HWM_DEADLINE_SECS: u64 = 840;

fn hwm_phases() -> Vec<Phase> {
    let mut phases = vec![Phase {
        id: "load".into(),
        step: 0,
    }];
    for cycle in 1..=3 {
        for (offset, name) in ["delete", "idle", "refill"].into_iter().enumerate() {
            phases.push(Phase {
                id: format!("{name}-{cycle}"),
                step: (cycle - 1) * 3 + offset + 1,
            });
        }
    }
    phases
}

fn hwm_survivor(index: usize) -> bool {
    // 104729 is coprime to 1M: this permutation retains exactly 50000 keys.
    (index * 104_729 + 12_345) % HWM_KEYS < HWM_KEYS / 20
}

fn hwm_record(index: usize, phase: &Phase) -> Record {
    let survivor = hwm_survivor(index);
    let absent = phase.step > 0 && phase.step % 3 != 0 && !survivor;
    let generation = if survivor || phase.step == 0 {
        0
    } else {
        (phase.step - 1) / 3 + usize::from(phase.step % 3 == 0)
    };
    Record {
        key: fixed_key(index, 18),
        value_length: 64,
        generation,
        kind: if absent { Kind::Absent } else { Kind::String },
        expiring: false,
    }
}

fn hwm_mix(mut word: u64) -> u64 {
    word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    word ^ (word >> 31)
}

fn hwm_payload(index: usize, generation: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(64);
    // The first word is bijective in index; later words change with generation.
    value.extend_from_slice(&hwm_mix(HWM_SEED ^ index as u64).to_le_bytes());
    for word in 1u64..8 {
        let input = HWM_SEED
            ^ (index as u64).rotate_left(17)
            ^ (generation as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
            ^ word.wrapping_mul(0xd134_2543_de82_ef95);
        value.extend_from_slice(&hwm_mix(input).to_le_bytes());
    }
    value
}

fn hwm_mutate(c: &mut impl Wire, phase: &Phase, index: usize) -> Result<u64> {
    if phase.step > 0 && (phase.step % 3 == 2 || hwm_survivor(index)) {
        return Ok(0);
    }
    let r = hwm_record(index, phase);
    if r.kind == Kind::Absent {
        integer(c, &[b"DEL", &r.key], 1)?;
    } else {
        let value = hwm_payload(index, r.generation);
        ok(c, &[b"SET", &r.key, &value])?;
    }
    Ok(1)
}

fn hwm_verify(c: &mut impl Wire, phase: &Phase, index: usize, state: &mut Sha256) -> Result<u64> {
    let r = hwm_record(index, phase);
    let value = hwm_payload(index, r.generation);
    let live = r.kind != Kind::Absent;
    expect(
        c.command(&[b"GET", &r.key])?,
        Reply::Bulk(live.then(|| value.clone())),
    )?;
    expect(
        c.command(&[b"TYPE", &r.key])?,
        Reply::Simple(if live {
            b"string".to_vec()
        } else {
            b"none".to_vec()
        }),
    )?;
    integer(c, &[b"PTTL", &r.key], if live { -1 } else { -2 })?;
    state_frame(state, &r, &value);
    Ok(3)
}

struct HwmTimed {
    timing: Timing,
    trace: String,
    state: String,
    commands: std::collections::BTreeMap<String, u64>,
    workers: Vec<serde_json::Value>,
}

fn hwm_timed(
    phase: &Phase,
    addr: SocketAddr,
    mutation: bool,
    started: Instant,
    scenario: &str,
) -> Result<HwmTimed> {
    let begin = Instant::now();
    let idle = mutation && phase.step % 3 == 2;
    let empty_histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    let results = thread::scope(|scope| {
        let handles: Vec<_> = (0..CLIENTS).map(|worker| {
            let local_histogram = empty_histogram.clone();
            scope.spawn(move || {
            let start = HWM_KEYS * worker / CLIENTS;
            let end = HWM_KEYS * (worker + 1) / CLIENTS;
            let mut h = local_histogram;
            let mut state = Sha256::new();
            let mut conn = Conn::connect(addr);
            let mut error = None;
            if let Ok(c) = &mut conn {
                for i in start..if idle { start } else { end } {
                    let t = Instant::now();
                    let operation = if started.elapsed().as_secs() >= HWM_DEADLINE_SECS {
                        Err("native runtime deadline".into())
                    } else if mutation {
                        hwm_mutate(c, phase, i)
                    } else {
                        hwm_verify(c, phase, i, &mut state)
                    };
                    match operation {
                        Ok(n) if n > 0 => {
                            if let Err(e) = t.elapsed().as_nanos().max(1).try_into().map_err(|_| "latency overflow").and_then(|ns| h.record(ns).map_err(|_| "histogram bound")) {
                                error = Some(e.to_string()); break;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => { error = Some(e.to_string()); break; }
                    }
                }
            } else if let Err(e) = &conn {
                error = Some(e.to_string());
            }
            let (trace, counts) = match conn {
                Ok(c) => (hex(c.trace), c.command_counts),
                Err(_) => (hex(Sha256::new()), std::collections::BTreeMap::new()),
            };
            let wire: u64 = counts.values().sum();
            let receipt = serde_json::json!({"worker":worker,"range_start":start,"range_end":end,
                "rust_thread_id":format!("{:?}",thread::current().id()),
                "wire_commands":wire,"completed_records":h.len(),"histogram_samples":h.len(),
                "trace_sha256":trace,"state_sha256":hex(state),"command_counts":counts,
                "errors":u64::from(error.is_some()),"error":error});
            (receipt, h)
        })}).collect();
        handles.into_iter().enumerate().map(|(worker, handle)| match handle.join() {
            Ok(value) => value,
            Err(_) => (serde_json::json!({"worker":worker,"range_start":HWM_KEYS*worker/CLIENTS,
                "range_end":HWM_KEYS*(worker+1)/CLIENTS,"wire_commands":null,"completed_records":null,
                "histogram_samples":null,"trace_sha256":null,"state_sha256":null,"command_counts":null,
                "rust_thread_id":null,
                "errors":1,"error":"worker panicked; partial counters unobserved"}),
                empty_histogram.clone()),
        }).collect::<Vec<_>>()
    });
    let workers: Vec<_> = results.iter().map(|(r, _)| r.clone()).collect();
    if workers.iter().any(|r| r["errors"] != 0) {
        emit(
            &serde_json::json!({"schema":2,"event":"worker-failure","pid":std::process::id(),
            "scenario":scenario,"phase":phase.id,"mutation":mutation,"workers":workers}),
        )?;
        return Err("HWM worker failure; all sixteen observations retained".into());
    }
    let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    let mut trace = Sha256::new();
    let mut state = Sha256::new();
    let mut commands = std::collections::BTreeMap::new();
    for (receipt, local) in results {
        h.add(&local)?;
        frame(
            &mut trace,
            receipt["trace_sha256"]
                .as_str()
                .ok_or("worker trace absent")?
                .as_bytes(),
        );
        frame(
            &mut state,
            receipt["state_sha256"]
                .as_str()
                .ok_or("worker state absent")?
                .as_bytes(),
        );
        let counts: std::collections::BTreeMap<String, u64> =
            serde_json::from_value(receipt["command_counts"].clone())?;
        for (name, n) in counts {
            *commands.entry(name).or_insert(0u64) += n;
        }
    }
    let wire: u64 = commands.values().sum();
    let elapsed_ns: u64 = if wire == 0 {
        0
    } else {
        begin.elapsed().as_nanos().try_into()?
    };
    let timing = Timing {
        wire_commands: wire,
        completed_records: h.len(),
        elapsed_ns,
        wire_commands_per_sec: if wire == 0 {
            0.0
        } else {
            wire as f64 * 1e9 / elapsed_ns.max(1) as f64
        },
        completed_records_per_sec: if wire == 0 {
            0.0
        } else {
            h.len() as f64 * 1e9 / elapsed_ns.max(1) as f64
        },
        latency_unit: if wire == 0 {
            "absent"
        } else if mutation {
            "logical-record-transition"
        } else {
            "full-record-verification"
        },
        p50_ns: h.value_at_quantile(0.5),
        p99_ns: h.value_at_quantile(0.99),
        p999_ns: h.value_at_quantile(0.999),
        maximum_ns: h.max(),
    };
    Ok(HwmTimed {
        timing,
        trace: hex(trace),
        state: hex(state),
        commands,
        workers,
    })
}

fn hwm_live_indices(phase: &Phase) -> Vec<usize> {
    (0..HWM_KEYS)
        .filter(|&i| phase.step % 3 == 0 || hwm_survivor(i))
        .collect()
}

fn hwm_uniform_rank(worker: usize, sequence: u64, cardinality: usize) -> Result<usize> {
    // Rejection removes modulo bias. Each candidate mixer is bijective; the
    // retry domain is separate from the worker/sequence domain.
    let input = HWM_SEED ^ (worker as u64).rotate_left(32) ^ sequence;
    let divisor = cardinality as u64;
    if divisor == 0 {
        return Err("empty steady live domain".into());
    }
    let threshold = divisor.wrapping_neg() % divisor;
    // Odd addition and the mixer are bijective; at most `threshold` distinct
    // rejected words exist. No unbounded random retry loop is needed.
    for retry in 0..=threshold {
        let word = hwm_mix(input.wrapping_add(retry.wrapping_mul(0x9e37_79b9_7f4a_7c15)));
        if word >= threshold {
            return Ok((word % divisor) as usize);
        }
    }
    Err("uniform rejection bound violated".into())
}

fn hwm_steady_request(c: &mut impl Wire, phase: &Phase, index: usize, set: bool) -> Result<()> {
    let r = hwm_record(index, phase);
    if r.kind != Kind::String {
        return Err("steady key selection is not live".into());
    }
    let value = hwm_payload(index, r.generation);
    if set {
        ok(c, &[b"SET", &r.key, &value, b"XX"])?;
    } else {
        expect(c.command(&[b"GET", &r.key])?, Reply::Bulk(Some(value)))?;
    }
    Ok(())
}

fn hwm_steady(phase: &Phase, addr: SocketAddr, started: Instant) -> Result<serde_json::Value> {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    let indices = Arc::new(hwm_live_indices(phase));
    let latch = Arc::new((Mutex::new(None::<Instant>), Condvar::new()));
    let ready = Arc::new(AtomicUsize::new(0));
    let cancel = Arc::new(AtomicBool::new(false));
    let histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
    let mut spawn_error = None;
    let (results, epoch) = thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..CLIENTS {
            let keys = indices.clone();
            let worker_latch = latch.clone();
            let worker_ready = ready.clone();
            let worker_cancel = cancel.clone();
            let mut hist = histogram.clone();
            let result = thread::Builder::new().name(format!("hwm-steady-{worker}")).spawn_scoped(scope,move || {
                let mut connection = Conn::connect(addr);
                let mut error = connection.as_ref().err().map(|e| e.to_string());
                if error.is_some() { worker_cancel.store(true,Ordering::Release); }
                worker_ready.fetch_add(1,Ordering::AcqRel);
                let mut lock = worker_latch.0.lock().unwrap_or_else(|e| e.into_inner());
                while lock.is_none() { lock = worker_latch.1.wait(lock).unwrap_or_else(|e| e.into_inner()); }
                let epoch = lock.ok_or("window epoch missing").map_err(|e| e.to_string());
                drop(lock);
                let mut attempts = 0u64;
                let mut replies = 0u64;
                let mut warmup = 0u64;
                let mut straddled = 0u64;
                let mut late = 0u64;
                let mut measured = std::collections::BTreeMap::<String,u64>::new();
                let mut finish_ns = 0u64;
                if let (Ok(c),Ok(epoch)) = (&mut connection,epoch) {
                    while !worker_cancel.load(Ordering::Acquire) && epoch.elapsed() < Duration::from_secs(23) {
                        if started.elapsed().as_secs() >= HWM_DEADLINE_SECS {
                            error = Some("native runtime deadline".into()); worker_cancel.store(true,Ordering::Release); break;
                        }
                        let rank = match hwm_uniform_rank(worker,attempts,keys.len()) {
                            Ok(rank) => rank,
                            Err(e) => { error=Some(e.to_string()); worker_cancel.store(true,Ordering::Release); break; }
                        };
                        let index = keys[rank];
                        let set = attempts % 5 == 4;
                        let began = epoch.elapsed();
                        attempts += 1;
                        if let Err(e) = hwm_steady_request(c,phase,index,set) {
                            error = Some(e.to_string()); worker_cancel.store(true,Ordering::Release); break;
                        }
                        let finished = epoch.elapsed();
                        finish_ns = finished.as_nanos().min(u64::MAX as u128) as u64;
                        replies += 1;
                        if began < Duration::from_secs(3) {
                            warmup += 1;
                            if finished >= Duration::from_secs(3) { straddled += 1; }
                        } else if finished <= Duration::from_secs(23) {
                            let ns = (finished-began).as_nanos().max(1).min(u64::MAX as u128) as u64;
                            if let Err(e) = hist.record(ns) {
                                error = Some(e.to_string()); worker_cancel.store(true,Ordering::Release); break;
                            }
                            *measured.entry(if set { "SET" } else { "GET" }.into()).or_default() += 1;
                        } else { late += 1; }
                    }
                }
                let (trace,commands) = match connection {
                    Ok(c) => (hex(c.trace),c.command_counts),
                    Err(_) => (hex(Sha256::new()),std::collections::BTreeMap::new()),
                };
                (serde_json::json!({"worker":worker,"rust_thread_id":format!("{:?}",thread::current().id()),
                    "sequence_start":0,"sequence_end":attempts,"wire_commands":commands,"trace_sha256":trace,
                    "successful_replies":replies,"warmup_completions":warmup,"warmup_straddled":straddled,
                    "late_completions_excluded":late,"measured_command_counts":measured,"histogram_samples":hist.len(),
                    "last_completion_ns_from_epoch":finish_ns,"errors":u64::from(error.is_some()),"error":error}),hist)
            });
            match result {
                Ok(handle) => handles.push((worker, handle)),
                Err(e) => {
                    spawn_error = Some(e.to_string());
                    cancel.store(true, Ordering::Release);
                    break;
                }
            }
        }
        let setup = Instant::now();
        while ready.load(Ordering::Acquire) < handles.len()
            && setup.elapsed() < Duration::from_secs(12)
            && started.elapsed().as_secs() < HWM_DEADLINE_SECS
        {
            thread::sleep(Duration::from_millis(1));
        }
        if ready.load(Ordering::Acquire) != CLIENTS {
            cancel.store(true, Ordering::Release);
        }
        let epoch = Instant::now();
        *latch.0.lock().unwrap_or_else(|e| e.into_inner()) = Some(epoch);
        latch.1.notify_all();
        let mut results: Vec<_> = handles
            .into_iter()
            .map(|(worker, handle)| match handle.join() {
                Ok(result) => result,
                Err(_) => (
                    serde_json::json!({"worker":worker,"rust_thread_id":null,"errors":1,
                "error":"steady worker panic; counters unobserved"}),
                    histogram.clone(),
                ),
            })
            .collect();
        for worker in results.len()..CLIENTS {
            results.push((
                serde_json::json!({"worker":worker,"rust_thread_id":null,"errors":1,
                "error":"steady worker not spawned; counters unobserved"}),
                histogram.clone(),
            ));
        }
        (results, epoch)
    });
    let mut all = histogram.clone();
    let mut outer = Sha256::new();
    let mut measured = std::collections::BTreeMap::<String, u64>::new();
    let workers: Vec<_> = results.iter().map(|(r, _)| r.clone()).collect();
    if spawn_error.is_some()
        || ready.load(Ordering::Acquire) != CLIENTS
        || workers.iter().any(|w| w["errors"] != 0)
    {
        emit(
            &serde_json::json!({"schema":2,"event":"worker-failure","pid":std::process::id(),
            "scenario":HWM_STEADY_ID,"phase":phase.id,"steady":true,"workers":workers,"spawn_error":spawn_error}),
        )?;
        return Err("steady worker failure; all sixteen observations retained".into());
    }
    for (r, h) in results {
        all.add(&h)?;
        frame(
            &mut outer,
            r["trace_sha256"]
                .as_str()
                .ok_or("steady trace absent")?
                .as_bytes(),
        );
        let counts: std::collections::BTreeMap<String, u64> =
            serde_json::from_value(r["measured_command_counts"].clone())?;
        for (k, n) in counts {
            *measured.entry(k).or_default() += n;
        }
    }
    Ok(
        serde_json::json!({"schema":2,"event":"steady-complete","pid":std::process::id(),"scenario":HWM_STEADY_ID,
        "phase":phase.id,"pipeline":1,"clients":CLIENTS,"warmup_seconds":3,"measured_seconds":20,
        "live_keys":indices.len(),"access_policy":"uniform-live-key-rejection-v1","mix":"4GET-1SET-XX",
        "trace_sha256":hex(outer),"workers":workers,"errors":0,"measured_command_counts":measured,
        "completed_requests":all.len(),"histogram_samples":all.len(),"ops_per_sec":all.len() as f64/20.0,
        "latency_unit":"checked-P1-request-completion","histogram_scope":"fully measured requests only; warmup straddles and late completions excluded",
        "elapsed_ns_from_epoch":epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        "p50_ns":all.value_at_quantile(0.5),"p99_ns":all.value_at_quantile(0.99),"p999_ns":all.value_at_quantile(0.999),"maximum_ns":all.max()}),
    )
}

fn hwm_main(args: Args) -> Result<()> {
    if args.verify_saturation {
        return Err("HWM cannot verify a saturation keyspace".into());
    }
    let phases = hwm_phases();
    let steady = args.scenario == HWM_STEADY_ID;
    let id = args.scenario.clone();
    let spec = Scenario {
        id: id.clone(),
        keys: HWM_KEYS,
        key_length: 18,
        value_length: 64,
        pattern: "per-key-unique-high-entropy-v1".into(),
        family: "hwm".into(),
        saturation: false,
    };
    let mut contract = serde_json::json!({"schema":2,"event":"ready","pid":std::process::id(),
        "scenario":spec,"seed":HWM_SEED,"clients":CLIENTS,"pipeline":1,"deadline_seconds":HWM_DEADLINE_SECS,
        "phases":phases,"initial_dbsize_checks":1,"survivor_rule":{"multiplier":104729,"offset":12345,"modulus":HWM_KEYS,"below":50000},
        "idle_seconds":10,"classification":if steady { "high-water-steady-lifecycle-diagnostic" } else { "high-water-lifecycle-subset" },"steady_get_set_tested":steady,
        "payload_witnesses":([[0,0],[0,1],[17,0],[500001,2],[999999,3]].map(|[index,generation]| {
            let value=hwm_payload(index,generation);
            serde_json::json!({"index":index,"generation":generation,"sha256":format!("{:x}",Sha256::digest(value))})
        })),
        "distribution":[{"key_bytes":18,"value_bytes":64,"records":HWM_KEYS,"logical_bytes":HWM_KEYS*82,"compact_class":41}],
        "timing_scope":"mutation: record transition and checked replies; verification: GET/TYPE/PTTL per record, separately timed; no per-wire-command percentile inferred",
        "trace_order":"sixteen canonical contiguous worker ranges; concurrent wire interleaving is not hashed"});
    if steady {
        contract["steady_contract"] = serde_json::json!({"warmup_seconds":3,"measured_seconds":20,"pipeline":1,
        "clients":CLIENTS,"phases":phases.iter().filter(|p| p.step%3!=2).map(|p| p.id.clone()).collect::<Vec<_>>(),
        "access_policy":"uniform-live-key-rejection-v1","mix":"4GET-1SET-XX","verification":"exhaustive after every window"});
    }
    if args.describe {
        if args.addr.is_some() {
            return Err("describe cannot accept endpoint".into());
        }
        return emit(&contract);
    }
    let addr = args.addr.ok_or("missing endpoint")?;
    if !addr.ip().is_loopback() || addr.port() == 0 {
        return Err("endpoint must be loopback and nonzero".into());
    }
    let started = Instant::now();
    let mut control = Conn::connect(addr)?;
    integer(&mut control, &[b"DBSIZE"], 0)?;
    emit(&contract)?;
    acknowledge()?;
    for phase in &phases {
        let idle_start = Instant::now();
        let idle_elapsed_ns: u64 = if phase.step % 3 == 2 {
            if started.elapsed().as_secs() + 10 >= HWM_DEADLINE_SECS {
                return Err("idle would exceed HWM native deadline".into());
            }
            thread::sleep(std::time::Duration::from_secs(10));
            idle_start.elapsed().as_nanos().try_into()?
        } else {
            0
        };
        if phase.step % 3 == 2 {
            // Let the controller sample the genuinely quiet server before the
            // exhaustive reads below. The next acknowledgement permits reads.
            emit(
                &serde_json::json!({"schema":2,"event":"idle-boundary","pid":std::process::id(),
                "scenario":id,"phase":phase.id,"idle_elapsed_ns":idle_elapsed_ns}),
            )?;
            acknowledge()?;
        }
        let mutation = hwm_timed(phase, addr, true, started, &id)?;
        if steady && phase.step % 3 != 2 {
            emit(
                &serde_json::json!({"schema":2,"event":"steady-ready","pid":std::process::id(),"scenario":id,
                "phase":phase.id,"warmup_seconds":3,"measured_seconds":20,"clients":CLIENTS,"pipeline":1}),
            )?;
            acknowledge()?;
            emit(&hwm_steady(phase, addr, started)?)?;
            acknowledge()?;
        }
        let verification = hwm_timed(phase, addr, false, started, &id)?;
        let live = if phase.step > 0 && phase.step % 3 != 0 {
            50000
        } else {
            HWM_KEYS
        };
        integer(&mut control, &[b"DBSIZE"], live as i64)?;
        emit(
            &serde_json::json!({"schema":2,"event":"phase","pid":std::process::id(),"scenario":id,"phase":phase.id,
            "control_commands":{"DBSIZE":1},"live_keys":live,"absent_keys":HWM_KEYS-live,"logical_bytes":live*82,
            "state_sha256":verification.state,"mutation_trace_sha256":mutation.trace,"verification_trace_sha256":verification.trace,
            "mutation":mutation.timing,"verification":verification.timing,"mutation_command_counts":mutation.commands,
            "verification_command_counts":verification.commands,"mutation_workers":mutation.workers,"verification_workers":verification.workers,
            "idle_elapsed_ns":idle_elapsed_ns}),
        )?;
        acknowledge()?;
        if started.elapsed().as_secs() >= HWM_DEADLINE_SECS {
            return Err("native runtime deadline".into());
        }
    }
    emit(
        &serde_json::json!({"schema":2,"event":"complete","pid":std::process::id(),"scenario":id,"phases":phases.len(),"errors":0}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    #[test]
    fn finite_catalog_and_access_reject_unknown() {
        assert_eq!(IDS.len(), 24);
        assert_eq!(IDS.iter().collect::<HashSet<_>>().len(), 24);
        assert!(Scenario::parse("unknown", false).is_err());
        assert!(Scenario::parse("v16-get-extra", true).is_err());
    }
    #[test]
    fn boundary_keys_are_unique_and_exact_length() {
        for len in [8, 18, 63, 64, 65] {
            let keys: HashSet<_> = (0..100_000).map(|i| fixed_key(i, len)).collect();
            assert_eq!(keys.len(), 100_000);
            assert!(keys.iter().all(|k| k.len() == len));
        }
    }
    #[test]
    fn class_dataset_covers_all160_and_unique_keys() {
        let s = Scenario::parse("classes-entropy", false).unwrap();
        let p = &s.phases()[0];
        let mut keys = HashSet::new();
        let mut classes = HashSet::new();
        for i in 0..s.keys {
            let r = s.record(i, p);
            assert!(r.key.len() <= 64 && r.value_length <= 256);
            assert!(keys.insert(r.key.clone()));
            classes.insert((r.key.len() + r.value_length).div_ceil(2));
        }
        assert_eq!(classes, (1..=160).collect());
    }
    #[test]
    fn mixed_weights_are_exact() {
        let s = Scenario::parse("mixed-entropy", false).unwrap();
        let p = &s.phases()[0];
        assert_eq!(
            (0..s.keys)
                .filter(|&i| s.record(i, p).value_length <= 256)
                .count(),
            80_000
        );
    }
    #[test]
    fn overwrite_changes_old_value_without_length_change() {
        let s = Scenario::parse("overwrite-entropy", false).unwrap();
        let p = s.phases();
        let a = s.record(0, &p[0]);
        let b = s.record(0, &p[1]);
        assert_eq!(a.value_length, b.value_length);
        assert_ne!(
            payload(a.value_length, &s.pattern, a.generation),
            payload(b.value_length, &s.pattern, b.generation)
        );
    }
    #[test]
    fn resize_crosses_inclusive_eligibility_and_returns() {
        let s = Scenario::parse("resize-entropy", false).unwrap();
        let p = s.phases();
        assert_eq!(p.len(), 13);
        assert_eq!(
            p.iter()
                .take(5)
                .map(|p| s.record(0, p).value_length)
                .collect::<Vec<_>>(),
            vec![16, 64, 255, 257, 16]
        );
    }
    #[test]
    fn half_delete_reinsert_has_exact_cardinality_and_absences() {
        let s = Scenario::parse("half-entropy", false).unwrap();
        let p = s.phases();
        assert_eq!(expected(&s, &p[1]).0, 50_000);
        assert_eq!(expected(&s, &p[2]).0, 100_000);
    }
    #[test]
    fn group_trace_retains_sentinels_and_changes_classes() {
        let s = Scenario::parse("groups-entropy", false).unwrap();
        let p = s.phases();
        assert_eq!(s.record(3, &p[1]).kind, Kind::String);
        assert_eq!(s.record(0, &p[1]).kind, Kind::Absent);
        assert_eq!(s.record(0, &p[2]).value_length, 64);
    }
    #[test]
    fn metadata_has_expiry_persist_absence_reinsert() {
        let s = Scenario::parse("metadata", false).unwrap();
        let p = s.phases();
        assert!(s.record(0, &p[1]).expiring);
        assert!(!s.record(0, &p[2]).expiring);
        assert_eq!(s.record(0, &p[3]).kind, Kind::Absent);
        assert_eq!(s.record(0, &p[4]).generation, 1);
    }
    #[test]
    fn type_trace_splits_hash_list_and_clears() {
        let s = Scenario::parse("types", false).unwrap();
        let p = s.phases();
        assert_eq!(s.record(0, &p[1]).kind, Kind::Hash);
        assert_eq!(s.record(10, &p[1]).kind, Kind::List);
        assert_eq!(s.record(0, &p[2]).kind, Kind::Absent);
        assert_eq!(s.record(0, &p[3]).kind, Kind::String);
    }
    #[test]
    fn state_digest_changes_on_generation_and_typed_state() {
        let s = Scenario::parse("overwrite-entropy", false).unwrap();
        let p = s.phases();
        assert_ne!(expected(&s, &p[0]).2, expected(&s, &p[1]).2);
    }
    #[test]
    fn hash_framing_prevents_concatenation_alias() {
        let mut a = Sha256::new();
        frame(&mut a, b"a");
        frame(&mut a, b"bc");
        let mut b = Sha256::new();
        frame(&mut b, b"ab");
        frame(&mut b, b"c");
        assert_ne!(hex(a), hex(b));
    }
    #[test]
    fn entropy_payload_matches_original_splitmix_seed() {
        assert_eq!(
            &payload(16, "high-entropy", 0),
            &[
                0x2b, 0x3f, 0xfe, 0xfc, 0x73, 0xd7, 0xdf, 0x74, 0x69, 0xf3, 0x85, 0x4a, 0xcf, 0x6a,
                0x59, 0xcf
            ]
        );
    }
    #[test]
    fn parser_handles_binary_bulk_and_exact_null() {
        let mut r = io::Cursor::new(b"$3\r\na\0b\r\n$-1\r\n".to_vec());
        assert_eq!(
            reply(&mut r, 0).unwrap(),
            Reply::Bulk(Some(b"a\0b".to_vec()))
        );
        assert_eq!(reply(&mut r, 0).unwrap(), Reply::Bulk(None));
    }
    #[test]
    fn parser_rejects_truncated_oversized_and_noncanonical() {
        for raw in [
            b"$3\r\nx".as_slice(),
            b"$8193\r\n",
            b":01\r\n",
            b"*17\r\n",
            b"$-2\r\n",
            b"?x\r\n",
        ] {
            assert!(reply(&mut io::Cursor::new(raw), 0).is_err());
        }
    }
    #[test]
    fn verification_rejects_one_key_corruption_and_stale_value() {
        assert!(expect(Reply::Bulk(Some(vec![1])), Reply::Bulk(Some(vec![2]))).is_err());
        assert!(expect(Reply::Bulk(Some(vec![1])), Reply::Bulk(None)).is_err());
    }
    struct Transcript {
        replies: std::collections::VecDeque<(Vec<Vec<u8>>, Reply)>,
    }
    impl Wire for Transcript {
        fn command(&mut self, parts: &[&[u8]]) -> Result<Reply> {
            let (expected, reply) = self.replies.pop_front().ok_or("unexpected extra command")?;
            if expected != parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>() {
                return Err("transcript command differs".into());
            }
            Ok(reply)
        }
    }
    fn response(parts: &[&[u8]], reply: Reply) -> (Vec<Vec<u8>>, Reply) {
        (parts.iter().map(|p| p.to_vec()).collect(), reply)
    }
    #[test]
    fn exhaustive_verifier_checks_every_key_and_ttl() {
        let mut s = Scenario::parse("value17", false).unwrap();
        s.keys = 32;
        let p = &s.phases()[0];
        let mut transcript = Transcript {
            replies: std::collections::VecDeque::new(),
        };
        for i in 0..s.keys {
            let r = s.record(i, p);
            let value = payload(r.value_length, &s.pattern, 0);
            transcript
                .replies
                .push_back(response(&[b"GET", &r.key], Reply::Bulk(Some(value))));
            transcript
                .replies
                .push_back(response(&[b"PTTL", &r.key], Reply::Integer(-1)));
        }
        let mut outer = Sha256::new();
        let mut operations = 0;
        for worker in 0..CLIENTS {
            let mut inner = Sha256::new();
            for i in s.keys * worker / CLIENTS..s.keys * (worker + 1) / CLIENTS {
                operations += verify(&mut transcript, &s, p, i, &mut inner).unwrap();
            }
            frame(&mut outer, hex(inner).as_bytes());
        }
        assert!(transcript.replies.is_empty());
        assert_eq!(operations, 64);
        assert_eq!(hex(outer), expected(&s, p).2);
    }
    #[test]
    fn exhaustive_verifier_rejects_corrupted_last_key_and_deleted_resurrection() {
        let s = Scenario::parse("value17", false).unwrap();
        let p = &s.phases()[0];
        let r = s.record(s.keys - 1, p);
        let mut c = Transcript {
            replies: [response(&[b"GET", &r.key], Reply::Bulk(Some(vec![0; 17])))].into(),
        };
        assert!(verify(&mut c, &s, p, s.keys - 1, &mut Sha256::new()).is_err());
        let s = Scenario::parse("half-entropy", false).unwrap();
        let p = &s.phases()[1];
        let r = s.record(0, p);
        let mut c = Transcript {
            replies: [response(&[b"GET", &r.key], Reply::Bulk(Some(vec![1])))].into(),
        };
        assert!(verify(&mut c, &s, p, 0, &mut Sha256::new()).is_err());
    }
    #[test]
    fn typed_verification_requires_wrongtype_exact_type_contents_and_no_ttl() {
        let s = Scenario::parse("types", false).unwrap();
        let p = &s.phases()[1];
        let r = s.record(0, p);
        let v = payload(16, &s.pattern, 0);
        let mut c = Transcript {
            replies: [
                response(
                    &[b"GET", &r.key],
                    Reply::Error(b"WRONGTYPE Operation".to_vec()),
                ),
                response(&[b"TYPE", &r.key], Reply::Simple(b"hash".to_vec())),
                response(
                    &[b"HGETALL", &r.key],
                    Reply::Array(vec![Reply::Bulk(Some(b"f".to_vec())), Reply::Bulk(Some(v))]),
                ),
                response(&[b"PTTL", &r.key], Reply::Integer(-1)),
            ]
            .into(),
        };
        assert_eq!(verify(&mut c, &s, p, 0, &mut Sha256::new()).unwrap(), 4);
        assert!(c.replies.is_empty());
        let mut c = Transcript {
            replies: [response(&[b"GET", &r.key], Reply::Bulk(None))].into(),
        };
        assert!(verify(&mut c, &s, p, 0, &mut Sha256::new()).is_err());
    }
    #[test]
    fn expiration_verification_rejects_missing_expired_or_unbounded_live_ttl() {
        let s = Scenario::parse("metadata", false).unwrap();
        let p = &s.phases()[1];
        let r = s.record(0, p);
        let v = payload(16, &s.pattern, 0);
        for ttl in [-2, -1, 0, 6_299_999, 7_200_001] {
            let mut c = Transcript {
                replies: [
                    response(&[b"GET", &r.key], Reply::Bulk(Some(v.clone()))),
                    response(&[b"PTTL", &r.key], Reply::Integer(ttl)),
                ]
                .into(),
            };
            assert!(verify(&mut c, &s, p, 0, &mut Sha256::new()).is_err());
        }
    }
    #[test]
    fn core_verification_selectors_are_finite_and_preserve_original_payload_shapes() {
        for size in [16, 64, 256, 1024, 4096] {
            for pattern in ["compressible", "high-entropy"] {
                let s = Scenario::parse(&format!("core-v{size}-{pattern}"), true).unwrap();
                assert!(s.saturation);
                assert_eq!(s.value_length, size);
                assert_eq!(s.record(99, &s.phases()[0]).key, b"k:0000000000000063");
            }
        }
        assert!(Scenario::parse("core-v17-high-entropy", true).is_err());
    }
    #[test]
    fn hwm_catalog_and_survivor_permutation() {
        let phases = hwm_phases();
        assert_eq!(phases.len(), 10);
        assert_eq!(
            phases.iter().map(|p| p.step).collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
        assert_eq!(phases[9].id, "refill-3");
        assert_eq!((0..HWM_KEYS).filter(|&i| hwm_survivor(i)).count(), 50_000);
        assert_eq!(
            (0..HWM_KEYS)
                .map(|i| (i * 104_729 + 12_345) % HWM_KEYS)
                .collect::<HashSet<_>>()
                .len(),
            HWM_KEYS
        );
        assert_eq!(IDS.len(), 24);
        assert!(!IDS.contains(&HWM_ID));
    }
    #[test]
    fn hwm_payload_mixed_generations_are_unique() {
        // Includes the formerly aliasing (index=0,g=0), (index=1,g=1) pair.
        let values = (0..HWM_KEYS)
            .map(|i| {
                let generation = if hwm_survivor(i) { 0 } else { i % 3 + 1 };
                let value = hwm_payload(i, generation);
                assert_eq!(value.len(), 64);
                assert_eq!(&value[..8], &hwm_payload(i, 0)[..8]);
                value[..8].to_vec()
            })
            .collect::<HashSet<_>>();
        assert_eq!(values.len(), HWM_KEYS);
        assert_ne!(hwm_payload(0, 0), hwm_payload(1, 1));
        assert_ne!(&hwm_payload(17, 0)[8..], &hwm_payload(17, 1)[8..]);
        assert_eq!(fixed_key(999_999, 18).len(), 18);
    }
    #[test]
    fn hwm_survivors_keep_generation_zero() {
        let survivor = (0..HWM_KEYS).find(|&i| hwm_survivor(i)).unwrap();
        let deleted = (0..HWM_KEYS).find(|&i| !hwm_survivor(i)).unwrap();
        for phase in hwm_phases() {
            let a = hwm_record(survivor, &phase);
            assert_eq!(a.generation, 0);
            assert_eq!(a.kind, Kind::String);
            assert!(!a.expiring);
            let b = hwm_record(deleted, &phase);
            if phase.step % 3 == 0 {
                assert_eq!(b.generation, phase.step / 3);
                assert_eq!(b.kind, Kind::String);
            } else {
                assert_eq!(b.kind, Kind::Absent);
            }
        }
    }
    #[test]
    fn hwm_mutation_preserves_survivors_and_idle() {
        let phases = hwm_phases();
        let survivor = (0..HWM_KEYS).find(|&i| hwm_survivor(i)).unwrap();
        let deleted = (0..HWM_KEYS).find(|&i| !hwm_survivor(i)).unwrap();
        let mut none = Transcript { replies: [].into() };
        assert_eq!(hwm_mutate(&mut none, &phases[1], survivor).unwrap(), 0);
        assert_eq!(hwm_mutate(&mut none, &phases[2], deleted).unwrap(), 0);
        let key = fixed_key(deleted, 18);
        let mut delete = Transcript {
            replies: [response(&[b"DEL", &key], Reply::Integer(1))].into(),
        };
        assert_eq!(hwm_mutate(&mut delete, &phases[1], deleted).unwrap(), 1);
        let value = hwm_payload(deleted, 1);
        let mut refill = Transcript {
            replies: [response(
                &[b"SET", &key, &value],
                Reply::Simple(b"OK".to_vec()),
            )]
            .into(),
        };
        assert_eq!(hwm_mutate(&mut refill, &phases[3], deleted).unwrap(), 1);
        assert!(refill.replies.is_empty() && delete.replies.is_empty());
    }
    #[test]
    fn hwm_verifier_requires_bytes_type_and_absence() {
        let phases = hwm_phases();
        let index = (0..HWM_KEYS).find(|&i| !hwm_survivor(i)).unwrap();
        let key = fixed_key(index, 18);
        for phase in [&phases[1], &phases[3]] {
            let live = phase.step % 3 == 0;
            let value = hwm_payload(index, 1);
            let mut wire = Transcript {
                replies: [
                    response(&[b"GET", &key], Reply::Bulk(live.then_some(value))),
                    response(
                        &[b"TYPE", &key],
                        Reply::Simple(if live {
                            b"string".to_vec()
                        } else {
                            b"none".to_vec()
                        }),
                    ),
                    response(&[b"PTTL", &key], Reply::Integer(if live { -1 } else { -2 })),
                ]
                .into(),
            };
            assert_eq!(
                hwm_verify(&mut wire, phase, index, &mut Sha256::new()).unwrap(),
                3
            );
            assert!(wire.replies.is_empty());
        }
        let mut resurrected = Transcript {
            replies: [response(&[b"GET", &key], Reply::Bulk(Some(vec![0; 64])))].into(),
        };
        assert!(hwm_verify(&mut resurrected, &phases[1], index, &mut Sha256::new()).is_err());
        let mut missing = Transcript {
            replies: [response(&[b"GET", &key], Reply::Bulk(None))].into(),
        };
        assert!(hwm_verify(&mut missing, &phases[3], index, &mut Sha256::new()).is_err());
        let value = hwm_payload(index, 1);
        let mut wrong_type = Transcript {
            replies: [
                response(&[b"GET", &key], Reply::Bulk(Some(value))),
                response(&[b"TYPE", &key], Reply::Simple(b"hash".to_vec())),
            ]
            .into(),
        };
        assert!(hwm_verify(&mut wrong_type, &phases[3], index, &mut Sha256::new()).is_err());
    }
    #[test]
    fn hwm_verifier_rejects_stale_generation() {
        let phase = &hwm_phases()[6];
        let index = (0..HWM_KEYS).find(|&i| !hwm_survivor(i)).unwrap();
        for mut value in [hwm_payload(index, 1), hwm_payload(index, 2)] {
            if value == hwm_payload(index, 2) {
                value[63] ^= 1;
            }
            let key = fixed_key(index, 18);
            let mut wire = Transcript {
                replies: [response(&[b"GET", &key], Reply::Bulk(Some(value)))].into(),
            };
            assert!(hwm_verify(&mut wire, phase, index, &mut Sha256::new()).is_err());
        }
    }
    #[test]
    fn hwm_state_digest_binds_generation_and_absence() {
        let index = (0..HWM_KEYS).find(|&i| !hwm_survivor(i)).unwrap();
        let states = [0, 1, 3, 6, 9].map(|step| {
            let r = hwm_record(index, &hwm_phases()[step]);
            let mut hash = Sha256::new();
            state_frame(&mut hash, &r, &hwm_payload(index, r.generation));
            hex(hash)
        });
        assert_eq!(states.iter().collect::<HashSet<_>>().len(), states.len());
    }
    #[test]
    fn hwm_steady_selects_live_uniform_keys() {
        for step in [0, 1, 3, 4, 6, 7, 9] {
            let phase = &hwm_phases()[step];
            let keys = hwm_live_indices(phase);
            assert_eq!(keys.len(), if step % 3 == 0 { HWM_KEYS } else { 50000 });
            for worker in 0..CLIENTS {
                for sequence in 0..128 {
                    let rank = hwm_uniform_rank(worker, sequence, keys.len()).unwrap();
                    let index = keys[rank];
                    assert_eq!(hwm_record(index, phase).kind, Kind::String);
                    assert_eq!(
                        rank,
                        hwm_uniform_rank(worker, sequence, keys.len()).unwrap()
                    );
                }
            }
        }
        assert!(hwm_uniform_rank(0, 0, 0).is_err());
        assert_eq!((0..10).filter(|&step| step % 3 != 2).count(), 7);
    }
    #[test]
    fn hwm_steady_set_xx_preserves_value_and_requires_exists() {
        let survivor = (0..HWM_KEYS).find(|&i| hwm_survivor(i)).unwrap();
        let refilled = (0..HWM_KEYS).find(|&i| !hwm_survivor(i)).unwrap();
        let phase = &hwm_phases()[6];
        for index in [survivor, refilled] {
            let r = hwm_record(index, phase);
            let value = hwm_payload(index, r.generation);
            let mut wire = Transcript {
                replies: [
                    response(&[b"GET", &r.key], Reply::Bulk(Some(value.clone()))),
                    response(
                        &[b"SET", &r.key, &value, b"XX"],
                        Reply::Simple(b"OK".to_vec()),
                    ),
                ]
                .into(),
            };
            hwm_steady_request(&mut wire, phase, index, false).unwrap();
            hwm_steady_request(&mut wire, phase, index, true).unwrap();
            assert!(wire.replies.is_empty());
            let mut absent = Transcript {
                replies: [response(
                    &[b"SET", &r.key, &value, b"XX"],
                    Reply::Bulk(None),
                )]
                .into(),
            };
            assert!(hwm_steady_request(&mut absent, phase, index, true).is_err());
        }
        let mut none = Transcript { replies: [].into() };
        assert!(hwm_steady_request(&mut none, &hwm_phases()[1], refilled, true).is_err());
    }
    #[test]
    fn hwm_steady_checked_prefix_counts_trace() {
        let phase = &hwm_phases()[3];
        let keys = hwm_live_indices(phase);
        let mut expected_trace = Sha256::new();
        let mut transcript = Transcript { replies: [].into() };
        for sequence in 0..37 {
            let index = keys[hwm_uniform_rank(15, sequence, keys.len()).unwrap()];
            let r = hwm_record(index, phase);
            let value = hwm_payload(index, r.generation);
            let parts: Vec<&[u8]> = if sequence % 5 == 4 {
                vec![b"SET", &r.key, &value, b"XX"]
            } else {
                vec![b"GET", &r.key]
            };
            expected_trace.update((parts.len() as u64).to_be_bytes());
            for part in &parts {
                frame(&mut expected_trace, part);
            }
            transcript.replies.push_back(response(
                &parts,
                if sequence % 5 == 4 {
                    Reply::Simple(b"OK".to_vec())
                } else {
                    Reply::Bulk(Some(value.clone()))
                },
            ));
        }
        // The native trace is checked against Python's independent prefix oracle
        // at actual window admission. This transcript exercises the exact argv
        // and generation-correct values for the same deterministic sequence.
        for sequence in 0..37 {
            let index = keys[hwm_uniform_rank(15, sequence, keys.len()).unwrap()];
            hwm_steady_request(&mut transcript, phase, index, sequence % 5 == 4).unwrap();
        }
        assert!(transcript.replies.is_empty());
        assert_eq!((0..37).filter(|sequence| sequence % 5 == 4).count(), 7);
        assert_ne!(hex(expected_trace), hex(Sha256::new()));
    }

    // These tests use real loopback sockets and the production Conn parser.
    // They qualify checked failure paths, not a benchmark row or a server image.
    fn hwm_release_fault_peer(parts: &[&[u8]], response: &[u8]) -> (Conn, thread::JoinHandle<()>) {
        use std::net::{Shutdown, TcpListener};
        let mut expected = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            expected.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            expected.extend_from_slice(part);
            expected.extend_from_slice(b"\r\n");
        }
        let response = response.to_vec();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let peer = thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let until = Instant::now() + Duration::from_secs(3);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < until, "fault peer accept deadline");
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("fault peer accept: {error}"),
                }
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut observed = vec![0; expected.len()];
            stream.read_exact(&mut observed).unwrap();
            assert_eq!(
                observed, expected,
                "fault missed the intended checked request"
            );
            stream.write_all(&response).unwrap();
            stream.shutdown(Shutdown::Write).unwrap();
        });
        (Conn::connect(address).unwrap(), peer)
    }

    #[test]
    fn hwm_release_mutation_transport_faults() {
        let phases = hwm_phases();
        let index = (0..HWM_KEYS).find(|&i| !hwm_survivor(i)).unwrap();
        let key = fixed_key(index, 18);
        let value = hwm_payload(index, 0);
        for response in [
            b"+OK\r".as_slice(),
            b"-WRONGTYPE injected\r\n",
            b"$-1\r\n",
            b":1\r\n",
        ] {
            let (mut connection, peer) = hwm_release_fault_peer(&[b"SET", &key, &value], response);
            let result = hwm_mutate(&mut connection, &phases[0], index);
            peer.join().unwrap();
            assert!(result.is_err(), "unchecked mutation response was accepted");
            assert_eq!(connection.command_counts.get("SET"), Some(&1));
        }
        let (mut connection, peer) = hwm_release_fault_peer(&[b"DEL", &key], b":0\r\n");
        let result = hwm_mutate(&mut connection, &phases[1], index);
        peer.join().unwrap();
        assert!(result.is_err(), "missing deletion was accepted");
    }

    #[test]
    fn hwm_release_verification_transport_faults() {
        let phase = &hwm_phases()[6];
        let index = (0..HWM_KEYS).find(|&i| !hwm_survivor(i)).unwrap();
        let key = fixed_key(index, 18);
        let mut stale = b"$64\r\n".to_vec();
        stale.extend_from_slice(&hwm_payload(index, 1));
        stale.extend_from_slice(b"\r\n");
        let responses = [
            b"$64\r\ntruncated".to_vec(),
            b"-WRONGTYPE injected\r\n".to_vec(),
            b"$-1\r\n".to_vec(),
            b"$8193\r\n".to_vec(),
            stale,
        ];
        for response in responses {
            let (mut connection, peer) = hwm_release_fault_peer(&[b"GET", &key], &response);
            let mut state = Sha256::new();
            let result = hwm_verify(&mut connection, phase, index, &mut state);
            peer.join().unwrap();
            assert!(result.is_err(), "invalid checked value was accepted");
            assert_eq!(
                hex(state),
                hex(Sha256::new()),
                "failed record entered the state digest"
            );
            assert_eq!(connection.command_counts.get("GET"), Some(&1));
            assert!(!connection.command_counts.contains_key("TYPE"));
        }
    }

    #[test]
    fn hwm_release_steady_transport_faults() {
        let phase = &hwm_phases()[6];
        let index = (0..HWM_KEYS).find(|&i| !hwm_survivor(i)).unwrap();
        let key = fixed_key(index, 18);
        let value = hwm_payload(index, 2);
        for response in [b"$-1\r\n".as_slice(), b"-WRONGTYPE injected\r\n", b"+OK\r"] {
            let (mut connection, peer) =
                hwm_release_fault_peer(&[b"SET", &key, &value, b"XX"], response);
            let result = hwm_steady_request(&mut connection, phase, index, true);
            peer.join().unwrap();
            assert!(
                result.is_err(),
                "failed SET XX was counted as a checked request"
            );
            assert_eq!(connection.command_counts.get("SET"), Some(&1));
        }
        let (mut connection, peer) = hwm_release_fault_peer(&[b"GET", &key], b"$64\r\ntruncated");
        let result = hwm_steady_request(&mut connection, phase, index, false);
        peer.join().unwrap();
        assert!(result.is_err(), "truncated steady GET was accepted");
        assert_eq!(connection.command_counts.get("GET"), Some(&1));
    }

    #[test]
    fn hwm_release_steady_workers_fail_closed_on_wire_error() {
        use std::net::{Shutdown, TcpListener};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        let peer = thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let until = Instant::now() + Duration::from_secs(5);
            let mut connections = Vec::new();
            while connections.len() < CLIENTS {
                match listener.accept() {
                    Ok((stream, _)) => connections.push(stream),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < until, "steady fault accept deadline");
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("steady fault accept: {error}"),
                }
            }
            let handlers: Vec<_> = connections
                .into_iter()
                .map(|mut stream| {
                    let observed = observed.clone();
                    thread::spawn(move || {
                        stream
                            .set_read_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        stream
                            .set_write_timeout(Some(Duration::from_secs(5)))
                            .unwrap();
                        let prefix = b"*2\r\n$3\r\nGET\r\n$18\r\n";
                        let mut first = [0];
                        if stream.read(&mut first).unwrap() == 0 {
                            return; // Another real worker's checked failure cancelled this worker.
                        }
                        let mut tail = vec![0; prefix.len() + 20 - 1];
                        stream.read_exact(&mut tail).unwrap();
                        let mut request = first.to_vec();
                        request.extend_from_slice(&tail);
                        assert_eq!(&request[..prefix.len()], prefix);
                        assert_eq!(&request[prefix.len()..prefix.len() + 10], &[b'k'; 10]);
                        assert_eq!(&request[request.len() - 2..], b"\r\n");
                        observed.fetch_add(1, Ordering::SeqCst);
                        stream.write_all(b"$-1\r\n").unwrap();
                        stream.shutdown(Shutdown::Write).unwrap();
                    })
                })
                .collect();
            for handler in handlers {
                handler.join().unwrap();
            }
            CLIENTS
        });
        // Call the actual steady function directly to reach checked failure and
        // cancellation without pretending a small fixture completed a 1M fill.
        // Its actual worker-failure event is retained in the --nocapture log.
        let result = hwm_steady(&hwm_phases()[0], address, Instant::now());
        assert_eq!(peer.join().unwrap(), CLIENTS);
        assert!(
            result.is_err(),
            "faulty steady workers published a completion"
        );
        assert!((1..=CLIENTS).contains(&requests.load(Ordering::SeqCst)));
    }
}
