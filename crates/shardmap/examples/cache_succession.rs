//! Component proof driven by scripts/prove-cache-succession.py. Neither this
//! fixture nor a cache receipt establishes real TLS/SQL service atomicity.
use std::error::Error;
use std::hint::black_box;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use shardmap::SharedCache;
use shardmap::succession::{Cache, Error as CacheError, Operation, Options, Peer, Staged};

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
fn owner(dir: &Path, mode: &str) -> Result<()> {
    let listener = UnixListener::bind(dir.join("control.sock"))?;
    let cache = Cache::create(Options {
        capacity: 4096,
        max_key_bytes: 32,
        max_value_bytes: 128,
        default_ttl_ms: None,
    })?;
    cache.insert(b"key", &[42; 64], None)?;
    cache.insert(b"expires", b"ttl", Some(Duration::from_secs(1)))?;
    let timings = bench(&cache)?;
    let heap_baseline = baseline();
    let value = cache.get(b"key")?.ok_or("value")?;
    let (slot, payload) = (value.slot_offset, value.value_offset);
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
    let (mut stream, _) = listener.accept()?;
    socket(&stream)?;
    let start = Instant::now();
    let mut prepared = cache.prepare(OPERATION, recipient, Duration::from_secs(2))?;
    let prepare_ns = start.elapsed().as_nanos();
    let hash = arena_hash(cache.mapping_address(), backing.bytes, backing.arena_offset);
    prepared.send(&mut stream)?;
    let mut ready = [0];
    stream.read_exact(&mut ready)?;
    assert_eq!(ready, [1]);
    let receipt = prepared.readiness();
    let prepared_ns = start.elapsed().as_nanos();
    match mode {
        "commit" => {
            // Component fixture's recorded decision. Production must use the one
            // durable cache+TLS/socket+SQL decision, under common admission.
            stream.write_all(&[1])?;
            stream.read_exact(&mut ready)?;
            assert_eq!(ready, [2]);
            assert!(matches!(prepared.abort(), Err(CacheError::Inactive)));
            assert!(matches!(cache.get(b"key"), Err(CacheError::Inactive)));
        }
        "abort" => {
            // Represents another participant rejecting readiness before COMMIT.
            prepared.abort()?;
            stream.write_all(&[0])?;
            assert_eq!(&*cache.get(b"key")?.ok_or("value after abort")?, &[42; 64]);
            cache.insert(b"after-abort", b"still A", None)?;
        }
        "ambiguous" => {
            drop(prepared);
            stream.write_all(&[3])?;
            assert!(matches!(cache.get(b"key"), Err(CacheError::Inactive)));
            assert!(matches!(
                cache.insert(b"bad", b"write", None),
                Err(CacheError::Inactive)
            ));
        }
        _ => return Err("unknown decision".into()),
    }
    std::fs::write(
        dir.join("owner.json"),
        serde_json::to_vec_pretty(&json!({
            "interface": INTERFACE, "peer": Peer::current(), "backing": backing, "receipt": receipt,
            "mapping_base": cache.mapping_address(), "slot_offset": slot, "value_offset": payload,
            "arena_sha256_before_attach": hash, "prepare_ns": prepare_ns, "ready_pause_ns": prepared_ns,
            "decision_pause_ns": start.elapsed().as_nanos(), "operations": timings, "heap_baseline": heap_baseline,
            "rss_kib": rss_kib()?, "mode": mode, "old_access_closed": mode != "abort"
        }))?,
    )?;
    // A exits completely. No cache relay, worker, helper, or runtime remains.
    Ok(())
}
fn successor(dir: &Path, old: Peer) -> Result<()> {
    let mut stream = UnixStream::connect(dir.join("control.sock"))?;
    socket(&stream)?;
    // Reserve A's virtual range so the retained payload must resolve at a
    // different address in this independently executed process.
    let old_mapping: Value = serde_json::from_slice(&std::fs::read(dir.join("owner-ready.json"))?)?;
    let address = old_mapping["mapping_base"]
        .as_u64()
        .ok_or("old mapping base")? as usize;
    let bytes = old_mapping["bytes"].as_u64().ok_or("old mapping size")? as usize;
    // SAFETY: NOREPLACE never overwrites existing mappings; anonymous PROT_NONE
    // reservation is released after the new mapping has been established.
    let reservation = unsafe {
        libc::mmap(
            address as *mut _,
            bytes,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED_NOREPLACE,
            -1,
            0,
        )
    };
    if reservation == libc::MAP_FAILED
        && std::io::Error::last_os_error().raw_os_error() != Some(libc::EEXIST)
    {
        return Err(std::io::Error::last_os_error().into());
    }
    let start = Instant::now();
    // SAFETY: supervisor owns both exact A/B artifacts and the private 0700
    // directory/channel. A has drained, and no raw writes/other owners exist.
    let staged = unsafe { Staged::receive(&mut stream, old) }?;
    let attach_ns = start.elapsed().as_nanos();
    if reservation != libc::MAP_FAILED {
        // SAFETY: the reservation is owned by this proof and has no references.
        unsafe {
            libc::munmap(reservation, bytes);
        }
    }
    assert_ne!(staged.mapping_address(), address);
    let receipt = staged.readiness();
    let base = staged.mapping_address();
    let hash = arena_hash(base, receipt.backing.bytes, receipt.backing.arena_offset);
    let overlap_rss = rss_kib()?;
    stream.write_all(&[1])?;
    let mut decision = [0];
    stream.read_exact(&mut decision)?;
    let mut extra = json!({});
    match decision[0] {
        1 => {
            let cache = staged.activate(OPERATION)?;
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
            assert_eq!(v.as_ptr() as usize, base + v.value_offset as usize);
            let slot = v.slot_offset;
            let payload = v.value_offset;
            drop(v);
            cache.insert(b"new-interface", INTERFACE.as_bytes(), None)?;
            assert_eq!(
                &*cache.get(b"new-interface")?.ok_or("new value")?,
                INTERFACE.as_bytes()
            );
            std::thread::sleep(Duration::from_millis(1100));
            assert!(cache.get(b"expires")?.is_none());
            extra = json!({"old_process_exited": true, "slot_offset": slot, "value_offset": payload,
                "new_value": INTERFACE, "ttl_preserved": true, "operations_after_A_exit": bench(&cache)?});
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
            "decision": decision[0], "result": extra
        }))?,
    )?;
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
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
        ),
        _ => Err("use scripts/prove-cache-succession.py".into()),
    }
}
