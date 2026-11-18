//! Component proof driven by scripts/prove-cache-succession.py. Neither this
//! fixture nor a cache receipt establishes real TLS/SQL service atomicity.
use std::error::Error;
use std::hint::black_box;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shardmap::SharedCache;
use shardmap::succession::{
    Cache, EngineBuildDigest, Error as CacheError, MemoryPolicy, Operation, Options, Peer,
    QualifiedBuildPair, Staged, authenticate_peer,
};

const INTERFACE: &str = match option_env!("SHARDMAP_DEMO_INTERFACE") {
    Some(s) => s,
    None => "development",
};
const OPERATION: Operation = Operation([7; 16]);
const N: usize = 100_000;
type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn bench(cache: &Cache) -> Result<Value> {
    let start_cpu = cpu_us()?;
    let start = Instant::now();
    for _ in 0..N {
        let v = cache.get(black_box(b"key"))?.ok_or("missing key")?;
        black_box(&*v);
    }
    let elapsed = start.elapsed();
    let reads =
        json!({"operations": N, "wall_ns": elapsed.as_nanos(), "cpu_us": cpu_us()? - start_cpu});
    let start_cpu = cpu_us()?;
    let start = Instant::now();
    for _ in 0..N {
        cache.insert(black_box(b"bench"), black_box(&[9; 64]), None)?;
    }
    let writes = json!({"operations": N, "wall_ns": start.elapsed().as_nanos(), "cpu_us": cpu_us()? - start_cpu});
    Ok(json!({"reads": reads, "writes": writes}))
}
fn baseline() -> Value {
    let cache = SharedCache::<64>::new();
    cache.insert(&b"key"[..], &[42u8; 64][..]);
    let start = Instant::now();
    for _ in 0..N {
        black_box(cache.get(black_box(b"key")));
    }
    json!({"operations": N, "wall_ns": start.elapsed().as_nanos(), "profile": "ordinary process-local SharedCache borrowed read, no TTL"})
}
fn cpu_us() -> Result<u64> {
    // SAFETY: rusage is writable and RUSAGE_SELF has no process mutation effect.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(
        (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as u64 * 1_000_000
            + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as u64,
    )
}
fn rss_kib() -> Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    Ok(status
        .lines()
        .find(|s| s.starts_with("VmRSS:"))
        .ok_or("RSS missing")?
        .split_whitespace()
        .nth(1)
        .ok_or("RSS value missing")?
        .parse()?)
}
fn arena_hash(base: usize, bytes: u64, offset: u64) -> String {
    // SAFETY: proof calls this only on a live mapping while A is quiescent and
    // B has no entry APIs. Exact geometry is from the validated cache receipt.
    let arena = unsafe {
        std::slice::from_raw_parts(
            (base + offset as usize) as *const u8,
            (bytes - offset) as usize,
        )
    };
    format!("{:x}", Sha256::digest(arena))
}
fn socket(stream: &UnixStream) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    Ok(())
}

// Reject unregistered connectors before pausing A. A production supervisor owns
// the private listener and deadline; no registration request supplies peer IDs.
fn accept_peer(listener: &UnixListener, expected: Peer) -> Result<(UnixStream, usize)> {
    listener.set_nonblocking(true)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut rejected = 0;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("successor connection timed out before prepare".into());
        }
        match listener.accept() {
            Ok((stream, _)) => match authenticate_peer(&stream, expected) {
                Ok(()) => return Ok((stream, rejected)),
                Err(CacheError::Unauthorized) => {
                    rejected += 1;
                }
                Err(error) => return Err(error.into()),
            },
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                let mut fd = libc::pollfd {
                    fd: listener.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: live writable pollfd; bounded timeout, no cache access.
                let ready = unsafe {
                    libc::poll(&mut fd, 1, remaining.as_millis().clamp(1, 10_000) as i32)
                };
                if ready < 0
                    && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
                {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}
fn owner(dir: &Path, mode: &str) -> Result<()> {
    let listener = UnixListener::bind(dir.join("control.sock"))?;
    let memory_policy = if matches!(mode, "locked" | "lock-denied") {
        MemoryPolicy::Locked
    } else {
        MemoryPolicy::Pageable
    };
    let cache = Cache::create_with_memory_policy(
        Options {
            capacity: 4096,
            max_key_bytes: 32,
            max_value_bytes: 128,
            default_ttl_ms: None,
        },
        memory_policy,
    )?;
    cache.insert(b"key", &[42; 64], None)?;
    cache.insert(b"expires", b"ttl", Some(Duration::from_secs(1)))?;
    let timings = bench(&cache)?;
    let heap_baseline = baseline();
    let value = cache.get(b"key")?.ok_or("value")?;
    let (slot, payload) = cache.value_offsets(&value)?;
    assert_eq!(
        value.as_ptr() as usize,
        cache.mapping_address() + payload as usize
    );
    drop(value);
    let backing = cache.identity()?;
    std::fs::write(
        dir.join("owner-ready.json"),
        serde_json::to_vec(
            &json!({"peer": Peer::current(), "mapping_base": cache.mapping_address(), "bytes": backing.bytes}),
        )?,
    )?;
    // Supervisor supplies the actual child PID; do not trust a peer registration.
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let recipient: Peer = serde_json::from_str(&line)?;
    let (mut stream, rejected) = accept_peer(&listener, recipient)?;
    assert_eq!(
        &*cache.get(b"key")?.ok_or("still active before prepare")?,
        &[42; 64]
    );
    if mode == "stray" {
        assert!(rejected > 0);
    }
    socket(&stream)?;
    let mapping_base = cache.mapping_address();
    let start = Instant::now();
    let mut prepared = cache.prepare(OPERATION, recipient, Duration::from_secs(2))?;
    let prepare_ns = start.elapsed().as_nanos();
    let hash = arena_hash(mapping_base, backing.bytes, backing.arena_offset);
    prepared.send(&mut stream)?;
    let mut ready = [0];
    stream.read_exact(&mut ready)?;
    assert_eq!(
        ready,
        [if matches!(mode, "collision" | "abi" | "lock-denied") {
            4
        } else {
            1
        }]
    );
    let receipt = prepared.readiness();
    let prepared_ns = start.elapsed().as_nanos();
    match mode {
        "commit" | "stray" | "locked" => {
            // Component fixture's recorded decision. Production must use the one
            // durable cache+TLS/socket+SQL decision, under common admission.
            stream.write_all(&[1])?;
            stream.read_exact(&mut ready)?;
            assert_eq!(ready, [2]);
            assert!(matches!(prepared.abort(), Err(CacheError::Inactive)));
        }
        "abort" => {
            // Represents another participant rejecting readiness before COMMIT.
            let cache = prepared.abort()?;
            stream.write_all(&[0])?;
            assert_eq!(&*cache.get(b"key")?.ok_or("value after abort")?, &[42; 64]);
            cache.insert(b"after-abort", b"still A", None)?;
        }
        "collision" | "abi" | "lock-denied" => {
            let cache = prepared.abort()?;
            assert_eq!(
                &*cache.get(b"key")?.ok_or("retained after import refusal")?,
                &[42; 64]
            );
        }
        "precommit-crash" => {
            assert_eq!(stream.read(&mut ready)?, 0);
            let cache = prepared.abort()?;
            assert_eq!(
                &*cache.get(b"key")?.ok_or("retained after crash")?,
                &[42; 64]
            );
        }
        "postcommit-crash" => {
            stream.write_all(&[1])?;
            assert_eq!(stream.read(&mut ready)?, 0);
            assert!(matches!(prepared.abort(), Err(CacheError::Inactive)));
        }
        "ambiguous" => {
            drop(prepared);
            stream.write_all(&[3])?;
        }
        _ => return Err("unknown decision".into()),
    }
    std::fs::write(
        dir.join("owner.json"),
        serde_json::to_vec_pretty(&json!({
            "interface": INTERFACE, "peer": Peer::current(), "backing": backing, "receipt": receipt,
            "mapping_base": mapping_base, "slot_offset": slot, "value_offset": payload,
            "arena_sha256_before_attach": hash, "prepare_ns": prepare_ns, "ready_pause_ns": prepared_ns,
            "decision_pause_ns": start.elapsed().as_nanos(), "operations": timings, "heap_baseline": heap_baseline,
            "rss_kib": rss_kib()?, "mode": mode, "old_access_closed": mode != "abort"
            , "unregistered_peers_rejected_before_prepare": rejected
        }))?,
    )?;
    // A exits completely. No cache relay, worker, helper, or runtime remains.
    Ok(())
}
fn successor(dir: &Path, old: Peer, mode: &str, pair: Option<QualifiedBuildPair>) -> Result<()> {
    if mode == "lock-denied" {
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: isolated test process only; never changes the old owner's allowance.
        if unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &limit) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    let mut stream = UnixStream::connect(dir.join("control.sock"))?;
    socket(&stream)?;
    let old_mapping: Value = serde_json::from_slice(&std::fs::read(dir.join("owner-ready.json"))?)?;
    let address = old_mapping["mapping_base"]
        .as_u64()
        .ok_or("old mapping base")? as usize;
    let bytes = old_mapping["bytes"].as_u64().ok_or("mapping size")? as usize;
    let collision = if mode == "collision" {
        // SAFETY: NOREPLACE never overwrites an existing mapping. This optional
        // reservation makes the intended address unavailable to the importer.
        let ptr = unsafe {
            libc::mmap(
                address as *mut _,
                bytes,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED_NOREPLACE,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED
            && std::io::Error::last_os_error().raw_os_error() != Some(libc::EEXIST)
        {
            return Err(std::io::Error::last_os_error().into());
        }
        ptr
    } else {
        libc::MAP_FAILED
    };
    let start = Instant::now();
    // SAFETY: the fixture supervisor pins both artifacts and verifies its
    // separately built engines retain Format 4. A is parked on a private
    // authenticated channel with no raw aliases or competing writers.
    let received = unsafe {
        match pair {
            Some(pair) => Staged::receive_qualified(&mut stream, old, pair),
            None => Staged::receive(&mut stream, old),
        }
    };
    if matches!(mode, "collision" | "abi" | "lock-denied") {
        match (&received, mode) {
            (Err(CacheError::Profile), "abi") => {}
            (Err(CacheError::MemoryLock(_)), "lock-denied") => {}
            (Err(CacheError::Io(error)), "collision")
                if error.raw_os_error() == Some(libc::EEXIST) => {}
            _ => return Err("import failed with unexpected outcome".into()),
        }
        assert!(!std::fs::read_to_string("/proc/self/maps")?.contains("memfd:shardmap-succession"));
        assert!(
            std::fs::read_dir("/proc/self/fd")?
                .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
                .all(|p| !p.to_string_lossy().contains("memfd:shardmap-succession"))
        );
        if collision != libc::MAP_FAILED {
            // SAFETY: only releases the reservation created by this fixture.
            unsafe {
                libc::munmap(collision, bytes);
            }
        }
        stream.write_all(&[4])?;
        std::fs::write(
            dir.join("successor.json"),
            serde_json::to_vec_pretty(&json!({"interface": INTERFACE, "refused": mode}))?,
        )?;
        return Ok(());
    }
    let staged = received?;
    let attach_ns = start.elapsed().as_nanos();
    assert_eq!(staged.mapping_address(), address);
    let receipt = staged.readiness();
    let base = staged.mapping_address();
    let hash = arena_hash(base, receipt.backing.bytes, receipt.backing.arena_offset);
    let overlap_rss = rss_kib()?;
    stream.write_all(&[1])?;
    if mode == "precommit-crash" {
        std::fs::write(dir.join("successor.json"), b"{\"exit_before_commit\":true}")?;
        // SAFETY: deliberate process-exit injection, no caller resumes this process.
        unsafe {
            libc::_exit(77);
        }
    }
    let mut decision = [0];
    stream.read_exact(&mut decision)?;
    let mut extra = json!({});
    match decision[0] {
        1 => {
            let cache = staged.activate(OPERATION)?;
            if mode == "postcommit-crash" {
                cache.insert(b"key", b"advanced before acknowledgement", None)?;
                std::fs::write(
                    dir.join("successor.json"),
                    b"{\"exit_after_advance_before_ack\":true}",
                )?;
                // SAFETY: deliberate exit after mutation and before the acknowledgement.
                unsafe {
                    libc::_exit(78);
                }
            }
            stream.write_all(&[2])?;
            // The supervisor reaps A while waiting for us. Wait for actual exit,
            // not just EOF or a logical epoch change.
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                // SAFETY: signal zero checks existence only; supervisor owns PID.
                if unsafe { libc::kill(old.pid as i32, 0) } == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    break;
                }
                if Instant::now() > deadline {
                    return Err("A did not fully exit".into());
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            let v = cache.get(b"key")?.ok_or("retained value")?;
            assert_eq!(&*v, &[42; 64]);
            let (slot, payload) = cache.value_offsets(&v)?;
            assert_eq!(v.as_ptr() as usize, base + payload as usize);
            drop(v);
            cache.insert(b"key", &[43; 64], None)?;
            let updated = cache.get(b"key")?.ok_or("updated inherited value")?;
            assert_eq!(&*updated, &[43; 64]);
            assert_eq!(cache.value_offsets(&updated)?, (slot, payload));
            assert_eq!(updated.as_ptr() as usize, base + payload as usize);
            drop(updated);
            assert!(cache.remove(b"key")?);
            assert!(cache.get(b"key")?.is_none());
            assert!(!cache.remove(b"key")?);
            cache.insert(b"key", &[42; 64], None)?;
            assert_eq!(&*cache.get(b"key")?.ok_or("reinserted key")?, &[42; 64]);
            cache.insert(b"new-interface", INTERFACE.as_bytes(), None)?;
            assert_eq!(
                &*cache.get(b"new-interface")?.ok_or("new value")?,
                INTERFACE.as_bytes()
            );
            cache.insert(b"new-expiry", b"ttl", Some(Duration::from_secs(1)))?;
            assert_eq!(&*cache.get(b"new-expiry")?.ok_or("new TTL")?, b"ttl");
            std::thread::sleep(Duration::from_millis(1100));
            assert!(cache.get(b"expires")?.is_none());
            assert!(cache.get(b"new-expiry")?.is_none());
            let cache = if pair.is_some() {
                let worker = cache.clone();
                let refusal = cache
                    .prepare(OPERATION, old, Duration::ZERO)
                    .expect_err("foreign backing must fail before parking");
                assert!(matches!(refusal.error, CacheError::Profile));
                assert_eq!(refusal.cache.identity()?, receipt.backing);
                assert_eq!(
                    &*worker.get(b"key")?.ok_or("retained after refusal")?,
                    &[42; 64]
                );
                drop(worker);
                refusal.cache
            } else {
                cache
            };
            extra = json!({"old_process_exited": true, "slot_offset": slot, "value_offset": payload,
                "new_value": INTERFACE, "ttl_preserved": true, "crud_after_A_exit": true,
                "foreign_reprepare_refused": pair.is_some(), "operations_after_A_exit": bench(&cache)?});
        }
        0 => {
            assert!(matches!(
                staged.activate(OPERATION),
                Err(CacheError::Inactive)
            ));
        }
        3 => {
            drop(staged);
        }
        _ => return Err("invalid coordinator decision".into()),
    }
    std::fs::write(
        dir.join("successor.json"),
        serde_json::to_vec_pretty(&json!({
            "interface": INTERFACE, "peer": Peer::current(), "receipt": receipt, "mapping_base": base,
            "arena_sha256_after_attach": hash, "attach_ns": attach_ns, "overlap_rss_kib": overlap_rss,
            "engine_digest": EngineBuildDigest::current(), "decision": decision[0], "result": extra
        }))?,
    )?;
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).is_some_and(|arg| arg == "engine-digest") {
        println!("{}", serde_json::to_string(&EngineBuildDigest::current())?);
        return Ok(());
    }
    if args.get(1).is_some_and(|arg| arg == "unprotected") {
        assert!(matches!(
            Cache::create(Options {
                capacity: 1,
                max_key_bytes: 8,
                max_value_bytes: 64,
                default_ttl_ms: None
            }),
            Err(CacheError::ProcessProtection)
        ));
        return Ok(());
    }
    shardmap::succession::protect_current_process()?;
    match args.get(1).map(String::as_str) {
        Some("owner") => owner(
            Path::new(args.get(2).ok_or("directory")?),
            args.get(3).ok_or("mode")?,
        ),
        Some("successor") => successor(
            Path::new(args.get(2).ok_or("directory")?),
            Peer {
                pid: args.get(3).ok_or("owner PID")?.parse()?,
                uid: Peer::current().uid,
            },
            args.get(4).ok_or("mode")?,
            args.get(5)
                .map(|pins| -> Result<_> {
                    let [source, target] = serde_json::from_str(pins)?;
                    Ok(QualifiedBuildPair::new(source, target)?)
                })
                .transpose()?,
        ),
        _ => Err("use scripts/prove-cache-succession.py".into()),
    }
}
