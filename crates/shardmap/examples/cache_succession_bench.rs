//! Matched public-API throughput probe for the actual succession implementation.
//! Aggregate wall ns/op is a throughput reciprocal, not request/tail latency.
use serde_json::json;
use shardmap::succession::{
    Cache, Operation, Options, Peer, Staged, authenticate_peer, protect_current_process,
};
use shardmap::{CacheOptions, SharedCache};
use std::error::Error;
use std::hint::black_box;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

// The benchmark runner replaces this block only when building its predecessor driver.
// BEGIN CANDIDATE ENGINE API
use shardmap::succession::{EngineBuildDigest, QualifiedBuildPair};

/// Report the engine identity compiled into this driver.
fn engine_digest() -> Result<serde_json::Value, Box<dyn Error>> {
    Ok(serde_json::to_value(EngineBuildDigest::current())?)
}

/// Report the running receiver's build for the timed row.
fn target_digest(_source: [u8; 32]) -> serde_json::Value {
    json!(EngineBuildDigest::current())
}

/// Receive from the exact-build default or the runner's explicit directional pair.
fn receive_cache(stream: &mut UnixStream, peer: Peer) -> Result<Staged, Box<dyn Error>> {
    let source = std::env::var_os("SHARDMAP_BENCH_SOURCE_DIGEST");
    let target = std::env::var_os("SHARDMAP_BENCH_TARGET_DIGEST");
    match (source, target) {
        (Some(source), Some(target)) => {
            let pair = QualifiedBuildPair::new(
                serde_json::from_str(source.to_str().ok_or("invalid source digest")?)?,
                serde_json::from_str(target.to_str().ok_or("invalid target digest")?)?,
            )?;
            // SAFETY: The fixture runner builds both engines from the selected
            // sources, owns both launches and supplies the exact directional pair.
            // A has consumed all active handles and the Unix peer is checked.
            Ok(unsafe { Staged::receive_qualified(stream, peer, pair) }?)
        }
        (None, None) => {
            // SAFETY: This mode uses the exact same engine for both owners.
            Ok(unsafe { Staged::receive(stream, peer) }?)
        }
        _ => Err("incomplete qualified engine pair".into()),
    }
}
// END CANDIDATE ENGINE API

const CAPACITY: usize = 14_336;
trait Points: Send + Sync + 'static {
    fn read(&self, key: &[u8]) -> bool;
    fn put(&self, key: &[u8], ttl: bool);
}
impl Points for Cache {
    #[inline(always)]
    fn read(&self, key: &[u8]) -> bool {
        match self.get(key).expect("bounded public read") {
            Some(value) => {
                black_box(&*value);
                true
            }
            None => false,
        }
    }
    #[inline(always)]
    fn put(&self, key: &[u8], ttl: bool) {
        self.insert(key, &[42; 64], ttl.then_some(Duration::from_secs(3600)))
            .expect("bounded public write");
    }
}
impl Points for SharedCache<64> {
    #[inline(always)]
    fn read(&self, key: &[u8]) -> bool {
        match self.get(key) {
            Some(value) => {
                black_box(&*value);
                true
            }
            None => false,
        }
    }
    #[inline(always)]
    fn put(&self, key: &[u8], ttl: bool) {
        self.insert_slice_with_ttl(key, &[42; 64], ttl.then_some(3_600_000));
    }
}
fn keys(count: usize) -> Vec<[u8; 8]> {
    if count != CAPACITY {
        return (0..count as u64).map(u64::to_le_bytes).collect();
    }
    let mut used = [0; 64];
    let mut keys = Vec::with_capacity(count);
    for n in 0..u64::MAX {
        let key = n.to_le_bytes();
        let shard = shardmap::storage::stripe_index(
            shardmap::storage::hash_key(&key),
            shardmap::storage::shift_for(64),
        );
        if used[shard] < CAPACITY / 64 {
            used[shard] += 1;
            keys.push(key);
        }
        if keys.len() == count {
            break;
        }
    }
    keys
}
fn run<T: Points>(
    cache: T,
    mode: &str,
    threads: usize,
    count: usize,
    iterations: usize,
    prefilled: bool,
) -> serde_json::Value {
    let cache = Arc::new(cache);
    let keys = Arc::new(keys(count));
    if !prefilled {
        for key in keys.iter() {
            cache.put(key, mode == "ttl");
        }
    }
    for key in keys.iter() {
        assert!(cache.read(key));
    }
    let ready = Arc::new(Barrier::new(threads + 1));
    let done = Arc::new(Barrier::new(threads + 1));
    let mut workers = Vec::new();
    let miss = mode == "miss";
    let writes = mode == "write";
    let mixed = mode == "mixed";
    for worker in 0..threads {
        let c = cache.clone();
        let k = keys.clone();
        let r = ready.clone();
        let d = done.clone();
        workers.push(std::thread::spawn(move || {
            r.wait();
            let mut state = worker as u64 + 12345;
            let mut hits = 0;
            for i in 0..iterations {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let absent = (state | (1 << 63)).to_le_bytes();
                let key = black_box(if miss {
                    &absent
                } else {
                    &k[state as usize % k.len()]
                });
                if writes || (mixed && i % 20 == 0) {
                    c.put(key, false);
                } else {
                    hits += usize::from(c.read(key));
                }
            }
            d.wait();
            hits
        }));
    }
    let start = Instant::now();
    ready.wait();
    done.wait();
    let ns = start.elapsed().as_nanos();
    let hits: usize = workers.into_iter().map(|w| w.join().expect("worker")).sum();
    let expected = if miss || writes {
        0
    } else {
        iterations * threads
            - if mixed {
                iterations.div_ceil(20) * threads
            } else {
                0
            }
    };
    assert_eq!(hits, expected);
    json!({"mode": mode, "threads": threads, "key_count": count, "operations": iterations * threads,
        "wall_ns": ns, "ns_per_op": ns as f64 / (iterations * threads) as f64, "checked_hits": hits})
}
fn profile() -> Options {
    Options {
        capacity: CAPACITY as u32,
        max_key_bytes: 8,
        max_value_bytes: 64,
        default_ttl_ms: None,
    }
}
fn resource() -> serde_json::Value {
    // SAFETY: initialized writable rusage, read-only current-process query.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return json!(null);
    }
    json!({"max_rss_kib": usage.ru_maxrss, "minor_faults": usage.ru_minflt, "cpu_us":
        (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1_000_000 + usage.ru_utime.tv_usec + usage.ru_stime.tv_usec})
}

/// Return Linux's process-wide page-table footprint in KiB.
fn page_table_kib() -> Result<u64, Box<dyn Error>> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix("VmPTE:"))
        .ok_or("missing VmPTE")?;
    Ok(value
        .split_whitespace()
        .next()
        .ok_or("missing VmPTE value")?
        .parse()?)
}

/// Emit one row. Only an adopted row has a handoff measurement.
fn report(
    method: &str,
    measurement: serde_json::Value,
    backing: Option<u64>,
    adoption: Option<serde_json::Value>,
) {
    println!(
        "{}",
        json!({"method": method, "measurement": measurement,
        "native_slot_budget": CAPACITY, "native_table_reserve_per_shard": CAPACITY / 64 * 2,
        "backing_bytes": backing, "old_owner_exited": adoption.is_some(),
        "adoption": adoption, "process_resource": resource()})
    );
}
fn adopted_receiver(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() != 9 {
        return Err("invalid receiver arguments".into());
    }
    protect_current_process()?;
    let mut stream = UnixStream::connect(&args[2])?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let old = Peer {
        pid: args[3].parse()?,
        uid: Peer::current().uid,
    };
    let pte_before_attach = page_table_kib()?;
    // A sends this marker after preparing, so receive timing excludes A's
    // drain and authorization setup. The memfd protocol follows unchanged.
    let mut prepared = [0];
    stream.read_exact(&mut prepared)?;
    if prepared != [1] {
        return Err("owner did not prepare".into());
    }
    let attach_started = Instant::now();
    let staged = receive_cache(&mut stream, old)?;
    let attach_ns = attach_started.elapsed().as_nanos();
    let pte_after_attach = page_table_kib()?;
    let source_engine_digest = staged.readiness().engine_abi;
    let method = &args[8];
    let mut prefault_ns = 0;
    if method == "native-adopted-prefault" {
        let start = Instant::now();
        staged.prefault()?;
        prefault_ns = start.elapsed().as_nanos();
    }
    let pte_after_prefault = page_table_kib()?;
    stream.write_all(&[1])?;
    let mut decision = [0];
    stream.read_exact(&mut decision)?;
    if decision != [1] {
        return Err("uncommitted handoff".into());
    }
    let cache = staged.activate(Operation([11; 16]))?;
    stream.write_all(&[2])?;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        // SAFETY: signal zero only observes the known old owner; no mutation.
        if unsafe { libc::kill(old.pid as i32, 0) } < 0
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            break;
        }
        if Instant::now() >= deadline {
            return Err("old owner did not exit/reap".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let backing = cache.identity()?.bytes;
    let pte_before_workload = page_table_kib()?;
    let measurement = if method == "native-adopted-cold" || method == "native-adopted-prefault" {
        // No digest, warmup or cache access in B before this first batch.
        let keys = keys(args[6].parse()?);
        let before = resource();
        let start = Instant::now();
        for key in &keys {
            assert!(cache.read(key));
        }
        let elapsed = start.elapsed().as_nanos();
        let after = resource();
        json!({"mode":"cold", "threads":1,"key_count":keys.len(),"operations":keys.len(),
            "wall_ns":elapsed,"ns_per_op":elapsed as f64 / keys.len() as f64,
            "prefault_ns":prefault_ns,"first_batch_minor_faults":
                after["minor_faults"].as_i64().expect("faults") - before["minor_faults"].as_i64().expect("faults")})
    } else {
        run(
            cache.clone(),
            &args[4],
            args[5].parse()?,
            args[6].parse()?,
            args[7].parse()?,
            true,
        )
    };
    let pte_after_workload = page_table_kib()?;
    let adoption = json!({
        "source_engine_digest": source_engine_digest,
        "target_engine_digest": target_digest(source_engine_digest),
        "attach_ns": attach_ns,
        "prefault_ns": prefault_ns,
        "vm_pte_kib": {
            "before_attach": pte_before_attach, "after_attach": pte_after_attach,
            "after_prefault": pte_after_prefault, "before_workload": pte_before_workload,
            "after_workload": pte_after_workload,
        },
    });
    report(method, measurement, Some(backing), Some(adoption));
    Ok(())
}
fn handoff(
    method: &str,
    mode: &str,
    threads: usize,
    count: usize,
    iterations: usize,
) -> Result<(), Box<dyn Error>> {
    protect_current_process()?;
    let cache = Cache::create(profile())?;
    for key in keys(count) {
        cache.put(&key, mode == "ttl");
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("control.sock");
    let listener = UnixListener::bind(&path)?;
    let successor = match std::env::var_os("SHARDMAP_BENCH_SUCCESSOR") {
        Some(path) => std::path::PathBuf::from(path),
        None => std::env::current_exe()?,
    };
    let mut child = Command::new(successor)
        .arg("__receive")
        .arg(&path)
        .arg(std::process::id().to_string())
        .arg(mode)
        .arg(threads.to_string())
        .arg(count.to_string())
        .arg(iterations.to_string())
        .arg(method)
        .spawn()?;
    let result = (|| -> Result<(), Box<dyn Error>> {
        let peer = Peer {
            pid: child.id(),
            uid: Peer::current().uid,
        };
        let (mut stream, _) = listener.accept()?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        authenticate_peer(&stream, peer)?;
        let prepare_started = Instant::now();
        let mut prepared = cache.prepare(Operation([11; 16]), peer, Duration::from_secs(1))?;
        let prepare_ns = prepare_started.elapsed().as_nanos();
        let source_engine_digest = prepared.readiness().engine_abi;
        stream.write_all(&[1])?;
        prepared.send(&mut stream)?;
        let mut ready = [0];
        stream.read_exact(&mut ready)?;
        assert_eq!(ready, [1]);
        let ready_ns = prepare_started.elapsed().as_nanos();
        stream.write_all(&[1])?;
        stream.read_exact(&mut ready)?;
        assert_eq!(ready, [2]);
        drop(prepared);
        eprintln!(
            "owner-metrics {}",
            json!({"source_engine_digest": source_engine_digest, "prepare_ns": prepare_ns,
                "ready_ns": ready_ns, "commit_ns": prepare_started.elapsed().as_nanos()})
        );
        Ok(()) // A exits without waiting for B; B proves exit before timing.
    })();
    if result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    result
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).is_some_and(|arg| arg == "__engine_digest") {
        println!("{}", engine_digest()?);
        return Ok(());
    }
    if args.get(1).is_some_and(|arg| arg == "__receive") {
        return adopted_receiver(&args);
    }
    if args.len() != 6 {
        return Err(
            "usage: cache_succession_bench heap-default|heap-matched|native|native-adopted read|ttl|mixed|write|miss threads keys iterations".into(),
        );
    }
    let method = &args[1];
    let mode = &args[2];
    let threads: usize = args[3].parse()?;
    let count: usize = args[4].parse()?;
    let iterations: usize = args[5].parse()?;
    if threads == 0
        || threads > 64
        || count == 0
        || count > CAPACITY
        || iterations == 0
        || !["read", "ttl", "mixed", "write", "miss"].contains(&mode.as_str())
    {
        return Err("invalid benchmark bounds".into());
    }
    if [
        "native-adopted",
        "native-adopted-cold",
        "native-adopted-prefault",
    ]
    .contains(&method.as_str())
    {
        return handoff(method, mode, threads, count, iterations);
    }
    let measurement;
    let mut backing_bytes = None;
    if method == "native" {
        protect_current_process()?;
        let cache = Cache::create(Options {
            capacity: CAPACITY as u32,
            max_key_bytes: 8,
            max_value_bytes: 64,
            default_ttl_ms: None,
        })?;
        backing_bytes = Some(cache.identity()?.bytes);
        measurement = run(cache, mode, threads, count, iterations, false);
    } else {
        // Two controls expose the cost of extra native-table churn headroom:
        // ordinary 8192-entry hint, and the same bucket reservation as native.
        let hint = match method.as_str() {
            "heap-default" => 8192,
            "heap-matched" => CAPACITY * 2,
            _ => return Err("unknown method".into()),
        };
        let cache = SharedCache::<64>::with_options(CacheOptions {
            capacity_hint: Some(hint),
            ..CacheOptions::default()
        });
        measurement = run(cache, mode, threads, count, iterations, false);
    }
    report(method, measurement, backing_bytes, None);
    Ok(())
}
