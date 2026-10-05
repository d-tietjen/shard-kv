//! RF-GENERAL-DELIVERY, RF-GENERAL-RESTART and RF-COMPACT-RESTART.
//!
//! The parent owns the independent command/state ledger. The child runs the
//! real caller-owned EmbeddedStore server and reports backend reachability.
//! An unread response is delivery-unknown, even if it reached the TCP buffer;
//! observing the mutation on another connection precedes reconnect/retry.
//! There is no exactly-once or global retry-order claim.
//!
//! EmbeddedStore is volatile. Recovery here is explicitly an embedding's
//! completed SnapshotStore checkpoint, not automatic EmbeddedStore WAL replay.
//! Only string records (including TTL/governance) are checkpointed; Redis
//! aggregate persistence is outside that API and outside these scenarios.

use super::*;
use crate::persistence::{SnapshotCompression, SnapshotRepository, SnapshotStore};
use crate::storage::StoredEntry;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream as BlockingStream};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

const CHILD_TEST: &str = "server::tests::storage_faults::storage_fault_child";
const EVENT_PREFIX: &str = "EDEN_STORAGE_FAULT:";
const IO_LIMIT: Duration = Duration::from_secs(5);
const MAX_LINE: usize = 64 * 1024;
// First seed is the fixed regression; the second changes all payload bytes.
const SEEDS: [u64; 2] = [0xed22_6601, 0x9e37_79b9_7f4a_7c15];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct EntryState {
    key: Vec<u8>,
    value: Vec<u8>,
    expire_at_ms: Option<u64>,
    governance: Option<Vec<u8>>,
}

impl From<StoredEntry> for EntryState {
    fn from(entry: StoredEntry) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
            expire_at_ms: entry.expire_at_ms,
            governance: entry.governance,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Observation {
    pid: u32,
    compact_enabled: bool,
    general: usize,
    compact: usize,
    keys: Vec<Vec<u8>>,
    stored_bytes: usize,
    entries: Vec<EntryState>,
}

type Ledger = BTreeMap<Vec<u8>, EntryState>;

fn observation(store: &EmbeddedStore) -> Value {
    store.process_maintenance();
    let (general, compact) = store.storage_backend_counts();
    let mut entries = store
        .try_entry_snapshot()
        .expect("fallible string snapshot")
        .into_iter()
        .map(EntryState::from)
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    json!({
        "pid": std::process::id(),
        "compact_enabled": cfg!(feature = "experimental-compact-point-storage"),
        "general": general, "compact": compact,
        "keys": store.key_snapshot(), "stored_bytes": store.stored_bytes(),
        "entries": entries,
    })
}

fn emit(event: Value) {
    let bytes = serde_json::to_vec(&event).expect("serialize private test event");
    assert!(bytes.len() < MAX_LINE);
    let mut output = std::io::stdout().lock();
    writeln!(
        output,
        "{EVENT_PREFIX}{}",
        String::from_utf8(bytes).unwrap()
    )
    .unwrap();
    output.flush().unwrap();
}

fn request(reader: &mut impl BufRead) -> Value {
    let mut bytes = Vec::new();
    reader
        .take((MAX_LINE + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .expect("private control request");
    assert!(!bytes.is_empty() && bytes.len() <= MAX_LINE);
    assert_eq!(bytes.last(), Some(&b'\n'));
    serde_json::from_slice(&bytes).expect("control JSON")
}

// Explicitly ignored in ordinary libtest runs. Parent launches exactly this
// test with a private directory/endpoint; an unconfigured explicit run fails.
#[test]
#[ignore = "private subprocess fixture, invoked only by the three parent scenarios"]
fn storage_fault_child() {
    let data_dir = std::env::var_os("EDEN_STORAGE_FAULT_DIR").expect("parent fixture directory");
    let addr = std::env::var("EDEN_STORAGE_FAULT_ADDR").expect("parent fixture endpoint");
    assert!(addr.starts_with("127.0.0.1:"));
    let restore_after = std::env::var("EDEN_STORAGE_FAULT_RESTORE_AFTER")
        .ok()
        .map(|value| value.parse::<usize>().expect("restore placement"));
    let store = Arc::new(EmbeddedStore::new(1));
    let repository = SnapshotStore::new(&data_dir);
    let mut control = BufReader::new(std::io::stdin());
    if std::env::var_os("EDEN_STORAGE_FAULT_RESTORE").is_some() {
        let mut restored = 0;
        repository
            .visit_latest_snapshot(|entry| {
                store.restore_entries([entry]);
                restored += 1;
                if restore_after == Some(restored) {
                    emit(json!({"event": "restore-prefix", "restored": restored,
                        "state": observation(&store)}));
                    assert_eq!(request(&mut control)["op"], "resume");
                }
                Ok(())
            })
            .expect("decode authoritative checkpoint")
            .expect("checkpoint exists");
        if let Some(placement) = restore_after {
            assert!(restored >= placement, "restore placement was never reached");
        }
    }
    // Read-only protocol identity proof before the parent sends any mutation.
    // A raced ephemeral-port collision cannot turn another service's PONG into
    // permission to populate it. The marker is removed before state admission.
    let ready_key = format!("__storage_fault_ready:{}", std::process::id()).into_bytes();
    let ready_value = format!("{}:{data_dir:?}", std::process::id()).into_bytes();
    store.set_slice(&ready_key, &ready_value, None);
    let control_store = store.clone();
    thread::spawn(move || {
        emit(
            json!({"event": "ready", "state": observation(&control_store),
            "ready_key": ready_key, "ready_value": ready_value}),
        );
        loop {
            let command = request(&mut control);
            match command["op"].as_str().expect("control operation") {
                "state" => emit(json!({"event": "state", "state": observation(&control_store)})),
                "checkpoint" | "checkpoint-interrupt" => {
                    let entries = control_store
                        .try_entry_snapshot()
                        .expect("checkpoint input");
                    let count = entries.len();
                    let interrupt = command["op"] == "checkpoint-interrupt";
                    let after = command["after"].as_u64().unwrap_or(0) as usize;
                    if interrupt {
                        assert!(after > 0 && after < count);
                    }
                    let mut written = 0;
                    let path = repository
                        .write_snapshot_streaming(
                            now_millis(),
                            SnapshotCompression::None,
                            |write| {
                                for entry in entries {
                                    write(entry)?;
                                    written += 1;
                                    if interrupt && written == after {
                                        emit(
                                            json!({"event": "checkpoint-prefix", "written": written,
                                            "state": observation(&control_store)}),
                                        );
                                        assert_eq!(request(&mut control)["op"], "resume");
                                    }
                                }
                                Ok(())
                            },
                        )
                        .expect("atomic checkpoint and directory fsync");
                    emit(json!({"event": "checkpoint", "count": count, "path": path,
                        "state": observation(&control_store)}));
                }
                "govern" => {
                    let entry: EntryState =
                        serde_json::from_value(command["entry"].clone()).unwrap();
                    assert!(entry.governance.is_some());
                    control_store.set_value_bytes_routed_expire_at_with_governance(
                        control_store.route_key(&entry.key),
                        &entry.key,
                        bytes::Bytes::from(entry.value),
                        entry.governance.map(bytes::Bytes::from),
                        entry.expire_at_ms,
                        now_millis(),
                    );
                    emit(json!({"event": "governed", "state": observation(&control_store)}));
                }
                "authorized" => {
                    let key: Vec<u8> = serde_json::from_value(command["key"].clone()).unwrap();
                    let metadata: Vec<u8> =
                        serde_json::from_value(command["metadata"].clone()).unwrap();
                    let value = control_store
                        .try_get_value_bytes_with_governance_filter(&key, |actual| {
                            actual == Some(metadata.as_slice())
                        })
                        .expect("authorized lookup")
                        .map(|value| value.to_vec());
                    emit(json!({"event": "authorized", "value": value}));
                }
                other => panic!("unknown private test operation: {other}"),
            }
        }
    });
    let mut config = ShardCacheConfig::default();
    config.bind_addr = addr;
    config.shard_count = 1;
    config.max_connections = 16;
    config.persistence.enabled = false;
    config.ttl_sweep_interval_ms = 10;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(ShardCacheServer::from_embedded_store(config, store).run())
        .expect("caller-owned embedded server");
    panic!("child returned before parent-owned SIGKILL");
}

struct FaultChild {
    child: Child,
    input: ChildStdin,
    events: Receiver<Value>,
    reader: Option<thread::JoinHandle<()>>,
    addr: String,
    reaped: bool,
}

impl FaultChild {
    fn spawn(data_dir: &std::path::Path, restore: bool, placement: Option<usize>) -> Self {
        let reserved = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reserved.local_addr().unwrap().to_string();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                CHILD_TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("EDEN_STORAGE_FAULT_DIR", data_dir)
            .env("EDEN_STORAGE_FAULT_ADDR", &addr)
            .env("SHARDCACHE_WORKER_COUNT", "1")
            .env("SHARDCACHE_USE_MONOIO", "0")
            .env("SHARDCACHE_DIRECT_SHARD_PORTS", "0")
            .env_remove("EDEN_STORAGE_FAULT_RESTORE")
            .env_remove("EDEN_STORAGE_FAULT_RESTORE_AFTER")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if restore {
            command.env("EDEN_STORAGE_FAULT_RESTORE", "1");
        }
        if let Some(count) = placement {
            command.env("EDEN_STORAGE_FAULT_RESTORE_AFTER", count.to_string());
        }
        // Keep the ephemeral reservation until immediately before the owned
        // child's bind. A collision is a startup failure, never a reuse claim.
        drop(reserved);
        let mut child = command.spawn().expect("spawn exact libtest fixture");
        let input = child.stdin.take().unwrap();
        let output = child.stdout.take().unwrap();
        let (sender, events) = mpsc::channel();
        // Install ownership cleanup before the fallible reader-thread spawn.
        let mut owned = Self {
            child,
            input,
            events,
            reader: None,
            addr,
            reaped: false,
        };
        let reader = thread::Builder::new()
            .name("storage-fault-events".into())
            .spawn(move || {
                let mut output = BufReader::new(output);
                loop {
                    let mut line = Vec::new();
                    let read = output
                        .by_ref()
                        .take((MAX_LINE + 1) as u64)
                        .read_until(b'\n', &mut line)
                        .unwrap();
                    if read == 0 {
                        break;
                    }
                    assert!(line.len() <= MAX_LINE, "unbounded child event");
                    let line = String::from_utf8(line).unwrap();
                    if let Some((_, payload)) = line.split_once(EVENT_PREFIX) {
                        eprintln!("{EVENT_PREFIX}{}", payload.trim());
                        if sender
                            .send(serde_json::from_str(payload.trim()).unwrap())
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            })
            .expect("owned fixture event reader");
        owned.reader = Some(reader);
        owned
    }

    fn event(&self, name: &str) -> Value {
        let event = self
            .events
            .recv_timeout(IO_LIMIT)
            .expect("bounded child event");
        assert_eq!(event["event"], name, "unexpected child event: {event}");
        event
    }

    fn control(&mut self, command: Value, event: &str) -> Value {
        let bytes = serde_json::to_vec(&command).unwrap();
        assert!(bytes.len() < MAX_LINE);
        self.input.write_all(&bytes).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
        self.event(event)
    }

    fn ready(&mut self) {
        let event = self.event("ready");
        assert_eq!(event["state"]["pid"], self.child.id());
        let ready_key: Vec<u8> = serde_json::from_value(event["ready_key"].clone()).unwrap();
        let ready_value: Vec<u8> = serde_json::from_value(event["ready_value"].clone()).unwrap();
        let deadline = Instant::now() + IO_LIMIT;
        loop {
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "fixture exited before listening"
            );
            if let Ok(mut stream) = BlockingStream::connect(&self.addr) {
                configure_stream(&stream);
                assert_eq!(
                    wire(&mut stream, &[b"PING"]),
                    Frame::SimpleString("PONG".into())
                );
                assert_eq!(
                    wire(&mut stream, &[b"GET", &ready_key]),
                    Frame::BlobString(ready_value),
                    "caller-owned protocol identity"
                );
                assert!(self.child.try_wait().unwrap().is_none());
                assert_eq!(wire(&mut stream, &[b"DEL", &ready_key]), Frame::Integer(1));
                return;
            }
            assert!(Instant::now() < deadline, "fixture did not listen");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn connect(&self) -> BlockingStream {
        let stream = BlockingStream::connect(&self.addr).unwrap();
        configure_stream(&stream);
        stream
    }

    fn state(&mut self) -> Observation {
        let event = self.control(json!({"op": "state"}), "state");
        serde_json::from_value(event["state"].clone()).unwrap()
    }

    fn checkpoint(&mut self, model: &Ledger, counts: (usize, usize), data_dir: &std::path::Path) {
        let event = self.control(json!({"op": "checkpoint"}), "checkpoint");
        assert_eq!(event["count"], model.len());
        assert_observation(
            self.child.id(),
            &serde_json::from_value(event["state"].clone()).unwrap(),
            model,
            counts,
        );
        // Compare the completed durable file with parent-owned expectations;
        // never use the file or SUT counters to construct the expected state.
        let snapshot = SnapshotStore::new(data_dir)
            .load_latest_snapshot()
            .unwrap()
            .unwrap();
        let mut entries = snapshot
            .entries
            .into_iter()
            .map(EntryState::from)
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        assert_eq!(entries, model.values().cloned().collect::<Vec<_>>());
    }

    fn crash(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "fault target already exited"
        );
        self.child.kill().expect("SIGKILL owned child");
        let deadline = Instant::now() + IO_LIMIT;
        loop {
            if self.child.try_wait().unwrap().is_some() {
                break;
            }
            assert!(Instant::now() < deadline, "owned child did not terminate");
            thread::sleep(Duration::from_millis(10));
        }
        let status = self.child.wait().expect("reap exact owned PID");
        self.reaped = true;
        assert_eq!(status.signal(), Some(9), "intended SIGKILL was not reached");
        self.reader
            .take()
            .unwrap()
            .join()
            .expect("child event reader");
    }
}

impl Drop for FaultChild {
    fn drop(&mut self) {
        if !self.reaped {
            // Cleanup is owned by this Child handle, including assertion failure.
            let _ = self.child.kill();
            let deadline = Instant::now() + IO_LIMIT;
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            if self.child.try_wait().ok().flatten().is_some() {
                let _ = self.child.wait();
                self.reaped = true;
            } else {
                eprintln!(
                    "owned storage fault child {} cleanup incomplete",
                    self.child.id()
                );
                if !thread::panicking() {
                    panic!("owned child was not reaped within cleanup bound");
                }
            }
        }
        if self.reaped
            && let Some(reader) = self.reader.take()
        {
            let _ = reader.join();
        }
    }
}

fn configure_stream(stream: &BlockingStream) {
    stream.set_read_timeout(Some(IO_LIMIT)).unwrap();
    stream.set_write_timeout(Some(IO_LIMIT)).unwrap();
    stream.set_nodelay(true).unwrap();
}

fn send_unread(stream: &mut BlockingStream, parts: &[&[u8]]) {
    let request = Frame::Array(
        parts
            .iter()
            .map(|part| Frame::BlobString(part.to_vec()))
            .collect(),
    );
    let mut bytes = Vec::new();
    RespCodec::encode(&request, &mut bytes);
    stream.write_all(&bytes).unwrap();
}

fn wire(stream: &mut BlockingStream, parts: &[&[u8]]) -> Frame {
    send_unread(stream, parts);
    let mut bytes = Vec::new();
    let deadline = Instant::now() + IO_LIMIT;
    // One byte at a time retains no extra response from the next command.
    for _ in 0..MAX_LINE {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .expect("whole reply deadline");
        stream.set_read_timeout(Some(remaining)).unwrap();
        let mut byte = [0];
        stream
            .read_exact(&mut byte)
            .expect("bounded checked RESP reply");
        bytes.push(byte[0]);
        if let Some((frame, consumed)) = RespCodec::decode(&bytes).unwrap() {
            assert_eq!(consumed, bytes.len());
            stream.set_read_timeout(Some(IO_LIMIT)).unwrap();
            return frame;
        }
    }
    panic!("reply exceeded fixture bound");
}

fn assert_observation(pid: u32, actual: &Observation, model: &Ledger, counts: (usize, usize)) {
    assert_eq!(actual.pid, pid);
    assert_eq!(
        actual.compact_enabled,
        cfg!(feature = "experimental-compact-point-storage")
    );
    assert_eq!(
        (actual.general, actual.compact),
        counts,
        "changed backend reachability"
    );
    assert_eq!(
        actual.entries,
        model.values().cloned().collect::<Vec<_>>(),
        "full key/value/TTL/governance ledger"
    );
    assert_eq!(
        actual.keys,
        model.keys().cloned().collect::<Vec<_>>(),
        "exhaustive keys and absence"
    );
    assert_eq!(
        actual.general + actual.compact,
        model.len(),
        "physical entry count"
    );
    assert_eq!(
        actual.stored_bytes,
        model
            .values()
            .map(|entry| entry.key.len()
                + entry.value.len()
                + entry.governance.as_ref().map_or(0, Vec::len))
            .sum::<usize>(),
        "logical stored-byte accounting"
    );
}

fn check_state(child: &mut FaultChild, model: &Ledger, counts: (usize, usize), absent: &[Vec<u8>]) {
    assert_observation(child.child.id(), &child.state(), model, counts);
    let mut stream = child.connect();
    assert_eq!(
        wire(&mut stream, &[b"DBSIZE"]),
        Frame::Integer(model.len() as i64)
    );
    for entry in model.values() {
        let expected = if entry.governance.is_some() {
            Frame::Null
        } else {
            Frame::BlobString(entry.value.clone())
        };
        assert_eq!(
            wire(&mut stream, &[b"GET", &entry.key]),
            expected,
            "GET full binary key"
        );
        if let Some(metadata) = &entry.governance {
            let event = child.control(
                json!({"op": "authorized", "key": entry.key, "metadata": metadata}),
                "authorized",
            );
            assert_eq!(event["value"], json!(entry.value));
            let denied = child.control(
                json!({"op": "authorized", "key": entry.key, "metadata": b"wrong-policy"}),
                "authorized",
            );
            assert!(denied["value"].is_null());
        } else {
            assert_eq!(
                wire(&mut stream, &[b"TYPE", &entry.key]),
                Frame::SimpleString("string".into())
            );
            let ttl = wire(&mut stream, &[b"PTTL", &entry.key]);
            match entry.expire_at_ms {
                None => assert_eq!(ttl, Frame::Integer(-1)),
                Some(deadline) => {
                    assert!(now_millis() < deadline, "fixture TTL expired before oracle");
                    assert!(matches!(ttl, Frame::Integer(value) if value > 0 && value <= 120_000));
                }
            }
        }
    }
    for key in absent {
        assert!(!model.contains_key(key));
        assert_eq!(wire(&mut stream, &[b"GET", key]), Frame::Null);
        assert_eq!(
            wire(&mut stream, &[b"TYPE", key]),
            Frame::SimpleString("none".into())
        );
        assert_eq!(wire(&mut stream, &[b"PTTL", key]), Frame::Integer(-2));
    }
}

fn keys() -> Vec<Vec<u8>> {
    vec![
        vec![],
        vec![0, 255],
        vec![0, 255, 0],
        vec![255; 64],
        vec![255; 65],
    ]
}

fn value(seed: u64, index: usize, len: usize) -> Vec<u8> {
    let mut state = seed ^ index as u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn fill(child: &mut FaultChild, seed: u64, compact: bool) -> Ledger {
    let mut model = Ledger::new();
    let mut stream = child.connect();
    for (index, key) in keys().into_iter().enumerate() {
        let value = value(seed, index, if compact { 16 } else { 257 });
        assert_eq!(
            wire(&mut stream, &[b"SET", &key, &value]),
            Frame::SimpleString("OK".into())
        );
        model.insert(
            key.clone(),
            EntryState {
                key,
                value,
                expire_at_ms: None,
                governance: None,
            },
        );
    }
    model
}

fn crash_after_unread_insert(
    child: &mut FaultChild,
    model: &mut Ledger,
    seed: u64,
    compact: bool,
) -> Vec<u8> {
    let key = b"crash-insert:\0\xff".to_vec();
    let value = value(seed, 20, if compact { 16 } else { 257 });
    let mut unknown = child.connect();
    send_unread(&mut unknown, &[b"SET", &key, &value]);
    let mut observer = child.connect();
    let deadline = Instant::now() + IO_LIMIT;
    loop {
        if wire(&mut observer, &[b"GET", &key]) == Frame::BlobString(value.clone()) {
            break;
        }
        assert!(Instant::now() < deadline, "unread insert was never applied");
    }
    model.insert(
        key.clone(),
        EntryState {
            key: key.clone(),
            value,
            expire_at_ms: None,
            governance: None,
        },
    );
    check_state(child, model, if compact { (1, 5) } else { (6, 0) }, &[]);
    // The process crashes while the inserting client has never consumed a
    // reply. This is not a claim that the server had not written to TCP.
    child.crash();
    drop(unknown);
    key
}

fn ttl_and_govern(child: &mut FaultChild, model: &mut Ledger) {
    let cohort = keys();
    let deadline = now_millis() + 120_000;
    let mut stream = child.connect();
    assert_eq!(
        wire(
            &mut stream,
            &[b"PEXPIREAT", &cohort[1], deadline.to_string().as_bytes()]
        ),
        Frame::Integer(1)
    );
    model.get_mut(&cohort[1]).unwrap().expire_at_ms = Some(deadline);
    let governed = model.get_mut(&cohort[2]).unwrap();
    governed.governance = Some(b"fixture:tenant-A\0\xff".to_vec());
    child.control(json!({"op": "govern", "entry": governed}), "governed");
}

fn replay_with_crash(
    data_dir: &std::path::Path,
    model: &Ledger,
    counts: (usize, usize),
    absent: &[Vec<u8>],
    placement: usize,
) -> FaultChild {
    assert!(placement > 0 && placement < model.len());
    let mut interrupted = FaultChild::spawn(data_dir, true, Some(placement));
    let prefix = interrupted.event("restore-prefix");
    assert_eq!(prefix["restored"], placement);
    let actual: Observation = serde_json::from_value(prefix["state"].clone()).unwrap();
    assert_eq!(actual.pid, interrupted.child.id());
    assert_eq!(
        actual.entries.len(),
        placement,
        "restore progressed before its crash"
    );
    assert!(
        BlockingStream::connect(&interrupted.addr).is_err(),
        "partial recovery must not listen"
    );
    for entry in &actual.entries {
        assert_eq!(
            model.get(&entry.key),
            Some(entry),
            "partial recovery lineage"
        );
    }
    interrupted.crash();
    let mut recovered = FaultChild::spawn(data_dir, true, None);
    recovered.ready();
    check_state(&mut recovered, model, counts, absent);
    recovered
}

#[test]
fn general_delivery_unknown_reconnect_retries_preserve_state() {
    for seed in SEEDS {
        eprintln!("BEGIN RF-GENERAL-DELIVERY seed={seed:x}");
        let directory = tempfile::tempdir().unwrap();
        let mut child = FaultChild::spawn(directory.path(), false, None);
        child.ready();
        let mut model = fill(&mut child, seed, false);
        let mut ordinary = child.connect();
        let deadline = now_millis() + 120_000;
        assert_eq!(
            wire(
                &mut ordinary,
                &[b"PEXPIREAT", &keys()[0], deadline.to_string().as_bytes()]
            ),
            Frame::Integer(1)
        );
        model.get_mut(&keys()[0]).unwrap().expire_at_ms = Some(deadline);
        for (index, key) in keys().into_iter().enumerate() {
            eprintln!("BEGIN RF-GENERAL-DELIVERY seed={seed:x} key_index={index} SETXX then DEL");
            let replacement = value(seed ^ 0xa5a5, index, 258);
            let mut unknown = child.connect();
            send_unread(
                &mut unknown,
                &[b"SET", &key, &replacement, b"XX", b"KEEPTTL"],
            );
            // Observe the exact mutation through a distinct protocol connection.
            let limit = Instant::now() + IO_LIMIT;
            loop {
                if wire(&mut ordinary, &[b"GET", &key]) == Frame::BlobString(replacement.clone()) {
                    break;
                }
                assert!(
                    Instant::now() < limit,
                    "SET XX application seam not reached"
                );
            }
            model.get_mut(&key).unwrap().value = replacement.clone();
            check_state(&mut child, &model, (model.len(), 0), &[]);
            unknown.shutdown(Shutdown::Both).unwrap();
            drop(unknown); // Never consumed the first SET reply.
            let mut retry = child.connect();
            assert_eq!(
                wire(&mut retry, &[b"SET", &key, &replacement, b"XX", b"KEEPTTL"]),
                Frame::SimpleString("OK".into())
            );
            check_state(&mut child, &model, (model.len(), 0), &[]);
            let mut unknown = child.connect();
            send_unread(&mut unknown, &[b"DEL", &key]);
            let limit = Instant::now() + IO_LIMIT;
            loop {
                if wire(&mut ordinary, &[b"GET", &key]) == Frame::Null {
                    break;
                }
                assert!(Instant::now() < limit, "DEL application seam not reached");
            }
            model.remove(&key).unwrap();
            let absent = keys()
                .into_iter()
                .filter(|key| !model.contains_key(key))
                .collect::<Vec<_>>();
            check_state(&mut child, &model, (model.len(), 0), &absent);
            unknown.shutdown(Shutdown::Both).unwrap();
            drop(unknown); // First DEL reply unknown; applied ledger says present -> absent.
            let mut retry = child.connect();
            assert_eq!(wire(&mut retry, &[b"DEL", &key]), Frame::Integer(0));
            assert_eq!(
                wire(&mut retry, &[b"SET", &key, &replacement, b"XX"]),
                Frame::Null
            );
            check_state(&mut child, &model, (model.len(), 0), &absent);
            eprintln!(
                "RF-GENERAL-DELIVERY seed={seed:x} key_index={index} SETXX/DEL observed-applied unread/retry checked"
            );
        }
        child.crash();
    }
}

#[test]
fn general_crash_restart_checkpoint_and_disposable_reset() {
    for seed in SEEDS {
        eprintln!("BEGIN RF-GENERAL-RESTART seed={seed:x}");
        let directory = tempfile::tempdir().unwrap();
        let mut child = FaultChild::spawn(directory.path(), false, None);
        child.ready();
        let mut model = fill(&mut child, seed, false);
        check_state(&mut child, &model, (5, 0), &[]);
        let inserted = crash_after_unread_insert(&mut child, &mut model, seed, false);
        // No checkpoint was admitted: volatile memory must start empty.
        let mut child = FaultChild::spawn(directory.path(), false, None);
        child.ready();
        let mut reset_absent = keys();
        reset_absent.push(inserted);
        check_state(&mut child, &Ledger::new(), (0, 0), &reset_absent);
        let mut model = fill(&mut child, seed, false);
        ttl_and_govern(&mut child, &mut model);
        let mut absent = Vec::new();
        for placement in 0..3 {
            eprintln!("BEGIN RF-GENERAL-RESTART seed={seed:x} placement={placement}");
            if placement == 1 {
                let replacement = value(seed, 9, 259);
                let mut stream = child.connect();
                assert_eq!(
                    wire(&mut stream, &[b"SET", &keys()[3], &replacement, b"XX"]),
                    Frame::SimpleString("OK".into())
                );
                model.get_mut(&keys()[3]).unwrap().value = replacement;
            } else if placement == 2 {
                let key = keys()[4].clone();
                assert_eq!(
                    wire(&mut child.connect(), &[b"DEL", &key]),
                    Frame::Integer(1)
                );
                model.remove(&key).unwrap();
                absent.push(key);
            }
            check_state(&mut child, &model, (model.len(), 0), &absent);
            child.checkpoint(&model, (model.len(), 0), directory.path());
            child.crash();
            child = replay_with_crash(
                directory.path(),
                &model,
                (model.len(), 0),
                &absent,
                if placement % 2 == 0 {
                    1
                } else {
                    model.len() - 1
                },
            );
            eprintln!(
                "RF-GENERAL-RESTART seed={seed:x} placement={placement} completed-checkpoint/SIGKILL/partial-restore/SIGKILL/replay exact"
            );
        }
        child.crash();
    }
}

#[cfg(feature = "experimental-compact-point-storage")]
#[test]
fn compact_crash_restart_migrated_metadata_and_repeated_restore() {
    for seed in SEEDS {
        eprintln!("BEGIN RF-COMPACT-RESTART seed={seed:x}");
        let directory = tempfile::tempdir().unwrap();
        let mut child = FaultChild::spawn(directory.path(), false, None);
        child.ready();
        let mut model = fill(&mut child, seed, true);
        // 65-byte key is general; the four <=64-byte keys are actually compact.
        check_state(&mut child, &model, (1, 4), &[]);
        let inserted = crash_after_unread_insert(&mut child, &mut model, seed, true);
        let mut child = FaultChild::spawn(directory.path(), false, None);
        child.ready();
        let mut reset_absent = keys();
        reset_absent.push(inserted);
        check_state(&mut child, &Ledger::new(), (0, 0), &reset_absent);
        let mut model = fill(&mut child, seed, true);
        check_state(&mut child, &model, (1, 4), &[]);
        ttl_and_govern(&mut child, &mut model);
        // Exactly the TTL and governance targets moved; two plain compact keys survive.
        check_state(&mut child, &model, (3, 2), &[]);
        for placement in [1, model.len() - 1] {
            eprintln!("BEGIN RF-COMPACT-RESTART seed={seed:x} restore_after={placement}");
            child.checkpoint(&model, (3, 2), directory.path());
            child.crash();
            child = replay_with_crash(directory.path(), &model, (3, 2), &[], placement);
            eprintln!(
                "RF-COMPACT-RESTART seed={seed:x} restore_after={placement} mixed actual-backend checkpoint/repeated-SIGKILL exact"
            );
        }
        // A later volatile mutation is not durable until a complete checkpoint
        // is acknowledged. Kill inside the streaming producer, before its
        // flush/rename/fsync; restart must select the previous completed file.
        let replacement = value(seed ^ 0x55, 30, 17);
        assert_eq!(
            wire(
                &mut child.connect(),
                &[b"SET", &keys()[3], &replacement, b"XX"]
            ),
            Frame::SimpleString("OK".into())
        );
        let mut candidate = model.clone();
        candidate.get_mut(&keys()[3]).unwrap().value = replacement;
        check_state(&mut child, &candidate, (3, 2), &[]);
        let prefix = child.control(json!({"op": "checkpoint-interrupt", "after": if seed == SEEDS[0] { 1 } else { candidate.len() - 1 }}), "checkpoint-prefix");
        assert!(prefix["written"].as_u64().unwrap() > 0);
        assert_observation(
            child.child.id(),
            &serde_json::from_value(prefix["state"].clone()).unwrap(),
            &candidate,
            (3, 2),
        );
        child.crash();
        let mut recovered = FaultChild::spawn(directory.path(), true, None);
        recovered.ready();
        check_state(&mut recovered, &model, (3, 2), &[]);
        recovered.crash();
        eprintln!(
            "RF-COMPACT-RESTART seed={seed:x} uncommitted-streaming-write discarded; previous checkpoint exact"
        );
    }
}
