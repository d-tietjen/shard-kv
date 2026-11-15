use super::*;
use std::sync::mpsc;
use std::thread;

fn options(capacity: u32) -> Options {
    Options {
        capacity,
        max_key_bytes: 32,
        max_value_bytes: 128,
        default_ttl_ms: None,
    }
}
fn prepare(cache: &Cache) -> Prepared {
    cache
        .prepare(
            Operation([3; 16]),
            Peer {
                pid: std::process::id() + 1,
                uid: Peer::current().uid,
            },
            Duration::from_secs(1),
        )
        .expect("prepare")
}
// Unit-only simulation of an authenticated recipient in this process. Production
// receive uses SO_PEERCRED and the actual recipient PID; the exec example covers it.
fn candidate(p: &Prepared) -> (OwnedFd, Readiness) {
    let mut r = p.receipt;
    r.recipient = Peer::current();
    let hash = authorization(&p.credential, &r).expect("digest");
    for (word, bytes) in p
        .inner
        .mapping
        .header()
        .authorization
        .iter()
        .zip(hash.chunks_exact(8))
    {
        word.store(
            u64::from_le_bytes(bytes.try_into().expect("word")),
            Ordering::Relaxed,
        );
    }
    (p.inner.mapping.file.try_clone().expect("fd").into(), r)
}
fn staged(p: &Prepared) -> Staged {
    let (fd, r) = candidate(p);
    Staged::attach(fd, r, &p.credential).expect("stage")
}

#[test]
fn preserves_slots_borrowed_payload_and_latest_writes_across_successions() {
    let a = Cache::create(options(8)).expect("create");
    a.insert(b"key", b"old", None).expect("write");
    let identity = a.identity().expect("identity");
    let v = a.get(b"key").expect("get").expect("value");
    let slot = v.slot_offset;
    let payload = v.value_offset;
    assert_eq!(v.as_ptr() as usize, a.mapping_address() + payload as usize);
    drop(v);
    let p = prepare(&a);
    let b = staged(&p);
    assert_ne!(b.mapping_address(), a.mapping_address());
    assert!(matches!(
        a.insert(b"key", b"bad", None),
        Err(Error::Inactive)
    ));
    let b = b
        .activate(p.receipt.operation)
        .expect("global commit cache participant");
    assert!(matches!(p.abort(), Err(Error::Inactive)));
    assert!(matches!(a.get(b"key"), Err(Error::Inactive)));
    drop(a);
    let v = b.get(b"key").expect("get").expect("value");
    assert_eq!(&*v, b"old");
    assert_eq!((v.slot_offset, v.value_offset), (slot, payload));
    assert_eq!(v.as_ptr() as usize, b.mapping_address() + payload as usize);
    drop(v);
    assert_eq!(b.identity().expect("identity"), identity);
    b.insert(b"key", b"latest", None).expect("new write");
    let p = prepare(&b);
    let c = staged(&p)
        .activate(p.receipt.operation)
        .expect("reverse/new handoff");
    drop(p);
    drop(b);
    assert_eq!(&*c.get(b"key").expect("get").expect("value"), b"latest");
    assert_eq!(c.identity().expect("identity"), identity);
}

#[test]
fn failed_other_participant_aborts_without_rewinding_or_rebuilding() {
    let a = Cache::create(options(8)).expect("create");
    a.insert(b"key", b"committed", None).expect("write");
    let slot = a.get(b"key").expect("get").expect("value").slot_offset;
    let p = prepare(&a);
    let b = staged(&p);
    p.abort()
        .expect("SQL/TLS fixture refused readiness before global commit");
    assert!(matches!(
        b.activate(Operation([3; 16])),
        Err(Error::Inactive)
    ));
    let v = a.get(b"key").expect("get").expect("value");
    assert_eq!(&*v, b"committed");
    assert_eq!(v.slot_offset, slot);
    drop(v);
    let p = prepare(&a);
    let _b = staged(&p);
    drop(p); // Ambiguous coordinator outcome must not implicitly reopen A.
    assert!(matches!(a.get(b"key"), Err(Error::Inactive)));
}

#[test]
fn guard_and_inflight_mutation_drain_are_bounded() {
    let a = Cache::create(options(8)).expect("create");
    a.insert(b"key", b"value", None).expect("write");
    let value = a.get(b"key").expect("get").expect("value");
    let err = a.prepare(
        Operation([4; 16]),
        Peer {
            pid: std::process::id() + 1,
            uid: Peer::current().uid,
        },
        Duration::from_millis(10),
    );
    assert!(matches!(err, Err(Error::DrainTimeout)));
    assert_eq!(&*value, b"value");
    drop(value);
    a.insert(b"key", b"after-timeout", None)
        .expect("admission restored");
    // Hold the exact mutation gate to exercise prepare while a mutation is in flight.
    let gate = a.inner.gate.write();
    let clone = a.clone();
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = clone.prepare(
            Operation([5; 16]),
            Peer {
                pid: std::process::id() + 1,
                uid: Peer::current().uid,
            },
            Duration::from_secs(1),
        );
        tx.send(result).expect("send");
    });
    while a.inner.accepting.load(Ordering::Acquire) {
        thread::yield_now();
    }
    assert!(rx.try_recv().is_err());
    drop(gate);
    let p = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("drained")
        .expect("prepare");
    worker.join().expect("thread");
    let b = staged(&p).activate(p.receipt.operation).expect("commit");
    assert_eq!(
        &*b.get(b"key").expect("get").expect("value"),
        b"after-timeout"
    );
}

#[test]
fn capacity_ttl_limits_and_expired_slot_reuse_survive_attach() {
    let a = Cache::create(options(2)).expect("create");
    a.insert(b"live", b"one", None).expect("write");
    a.insert(b"expires", b"two", Some(Duration::from_millis(15)))
        .expect("write");
    assert!(matches!(
        a.insert(b"full", b"three", None),
        Err(Error::Capacity)
    ));
    let p = prepare(&a);
    let b = staged(&p).activate(p.receipt.operation).expect("commit");
    thread::sleep(Duration::from_millis(25));
    assert!(b.get(b"expires").expect("get").is_none());
    b.insert(b"new", b"three", None)
        .expect("reclaimed expired slot");
    assert_eq!(&*b.get(b"live").expect("get").expect("value"), b"one");
    assert!(matches!(b.insert(&[0; 33], b"v", None), Err(Error::Limit)));
    assert!(matches!(b.insert(b"v", &[0; 129], None), Err(Error::Limit)));
    assert!(b.remove(b"live").expect("delete"));
    assert!(!b.remove(b"live").expect("delete"));
    b.insert(b"", b"", None).expect("empty key and value");
    assert!(b.get(b"").expect("get").is_some());
}

#[test]
fn rejects_wrong_credential_recipient_profile_stale_and_replayed_transfer() {
    let a = Cache::create(options(4)).expect("create");
    let p = prepare(&a);
    let (fd, r) = candidate(&p);
    assert!(matches!(
        Staged::attach(fd, r, &[0; 32]),
        Err(Error::Unauthorized)
    ));
    let (fd, mut r) = candidate(&p);
    r.recipient.pid += 1;
    assert!(matches!(
        Staged::attach(fd, r, &p.credential),
        Err(Error::Unauthorized)
    ));
    let (fd, mut r) = candidate(&p);
    r.options.capacity += 1;
    assert!(matches!(
        Staged::attach(fd, r, &p.credential),
        Err(Error::Profile)
    ));
    let first = staged(&p);
    assert!(matches!(
        first.activate(Operation([9; 16])),
        Err(Error::Unauthorized)
    ));
    let stale = staged(&p);
    let b = staged(&p).activate(p.receipt.operation).expect("commit");
    assert!(matches!(
        stale.activate(p.receipt.operation),
        Err(Error::Inactive)
    ));
    let (fd, r) = candidate(&p);
    assert!(matches!(
        Staged::attach(fd, r, &p.credential),
        Err(Error::Inactive)
    ));
    assert!(matches!(p.abort(), Err(Error::Inactive)));
    b.insert(b"still-active", b"ok", None).expect("active");
}

#[test]
fn rejects_unsealed_malformed_backing_and_wrong_kernel_peer() {
    let a = Cache::create(options(2)).expect("create");
    let p = prepare(&a);
    let (_, r) = candidate(&p);
    let file = tempfile::tempfile().expect("file");
    file.set_len(r.backing.bytes).expect("size");
    assert!(matches!(
        Staged::attach(file.into(), r, &p.credential),
        Err(Error::Profile)
    ));
    let (left, mut right) = UnixStream::pair().expect("socket");
    assert!(matches!(
        transport::authenticate(
            &left,
            Peer {
                pid: Peer::current().pid + 1,
                uid: Peer::current().uid
            }
        ),
        Err(Error::Unauthorized)
    ));
    let (fd, r) = candidate(&p);
    let mut bad = r;
    bad.backing.store[0] ^= 1;
    transport::send(&mut right, fd.as_raw_fd(), &bad, &p.credential).expect("send");
    let mut left = left;
    // SAFETY: unit fixture owns both endpoints/backing and obeys the exclusive protocol.
    assert!(matches!(
        unsafe { Staged::receive(&mut left, Peer::current()) },
        Err(Error::Profile)
    ));
}

#[test]
fn activation_abort_race_has_exactly_one_winner() {
    for _ in 0..40 {
        let a = Cache::create(options(2)).expect("create");
        a.insert(b"key", b"value", None).expect("write");
        let p = prepare(&a);
        let b = staged(&p);
        let activate = thread::spawn(move || b.activate(Operation([3; 16])));
        let aborted = p.abort().is_ok();
        let activated = activate.join().expect("thread");
        assert_ne!(aborted, activated.is_ok());
        assert_eq!(a.get(b"key").is_ok(), aborted);
    }
}

#[test]
fn concurrent_public_writes_drain_without_losing_last_acknowledged_value() {
    let a = Cache::create(options(2)).expect("create");
    a.insert(b"sequence", &0u64.to_le_bytes(), None)
        .expect("initial value");
    let acknowledged = Arc::new(AtomicU64::new(0));
    let worker_cache = a.clone();
    let worker_ack = acknowledged.clone();
    let writer = thread::spawn(move || {
        for sequence in 1u64..=1_000_000 {
            match worker_cache.insert(b"sequence", &sequence.to_le_bytes(), None) {
                Ok(()) => worker_ack.store(sequence, Ordering::Release),
                Err(Error::Inactive) => return,
                Err(error) => panic!("unexpected write failure: {error}"),
            }
        }
    });
    while acknowledged.load(Ordering::Acquire) == 0 {
        thread::yield_now();
    }
    let p = prepare(&a);
    writer.join().expect("old writer stopped");
    let last = acknowledged.load(Ordering::Acquire);
    let b = staged(&p).activate(p.receipt.operation).expect("commit");
    let value = b
        .get(b"sequence")
        .expect("get")
        .expect("last acknowledged write");
    assert_eq!(&*value, &last.to_le_bytes());
    assert!(matches!(
        a.insert(b"sequence", &0u64.to_le_bytes(), None),
        Err(Error::Inactive)
    ));
}
