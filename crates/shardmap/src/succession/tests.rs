use super::*;
use std::thread;

fn options(capacity: u32) -> Options {
    protect_current_process().expect("protect test process");
    Options {
        capacity,
        max_key_bytes: 32,
        max_value_bytes: 128,
        default_ttl_ms: None,
    }
}
fn prepare(cache: Cache) -> Prepared {
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
// Unit-only recipient simulation reuses this process's mapping. Independent
// mmap/exec custody, collisions and old process exit are proved by the A/B runner.
fn candidate(p: &Prepared) -> Readiness {
    let mut receipt = p.receipt;
    receipt.recipient = Peer::current();
    let hash = authorization(&p.credential, &receipt).expect("digest");
    for (word, bytes) in p
        .mapping
        .header()
        .authorization
        .iter()
        .zip(hash.as_chunks::<8>().0)
    {
        word.store(u64::from_le_bytes(*bytes), Ordering::Relaxed);
    }
    receipt
}
fn staged(p: &Prepared) -> Staged {
    let receipt = candidate(p);
    Staged::validate(p.mapping.clone(), receipt, &p.credential).expect("stage exact engine")
}

#[test]
fn retains_table_slots_payload_and_latest_writes_across_successions() {
    let a = Cache::create(options(8)).expect("create");
    a.insert(b"key", b"old", None).expect("write");
    let identity = a.identity().expect("identity");
    let base = a.mapping_address();
    let value = a.get(b"key").expect("get").expect("value");
    let offsets = a.value_offsets(&value).expect("offset diagnostics");
    let pointer = value.as_ptr() as usize;
    drop(value);
    let p = prepare(a);
    let b = staged(&p).activate(p.receipt.operation).expect("activate");
    assert!(matches!(p.abort(), Err(Error::Inactive)));
    let value = b.get(b"key").expect("get").expect("retained");
    assert_eq!(&*value, b"old");
    assert_eq!(
        b.value_offsets(&value).expect("offset diagnostics"),
        offsets
    );
    assert_eq!(value.as_ptr() as usize, pointer);
    assert_eq!(b.mapping_address(), base);
    drop(value);
    b.insert(b"key", b"latest", None).expect("update");
    let p = prepare(b);
    let c = staged(&p)
        .activate(p.receipt.operation)
        .expect("second handoff");
    drop(p);
    assert_eq!(&*c.get(b"key").expect("get").expect("value"), b"latest");
    assert_eq!(c.identity().expect("identity"), identity);
}

#[test]
fn offset_diagnostics_are_explicit_and_refuse_another_cache() {
    let a = Cache::create(options(2)).expect("create");
    let other = Cache::create(options(2)).expect("create another");
    a.insert(b"key", b"value", None).expect("write");
    let value = a.get(b"key").expect("get").expect("value");
    let (slot, payload) = a.value_offsets(&value).expect("diagnostic");
    assert_eq!(
        value.as_ptr() as usize,
        a.mapping_address() + payload as usize
    );
    assert!(slot < payload);
    assert!(matches!(other.value_offsets(&value), Err(Error::Profile)));
}

#[test]
fn abort_restores_original_handle_and_refuses_stale_activation() {
    let a = Cache::create(options(8)).expect("create");
    a.insert(b"key", b"committed", None).expect("write");
    let pointer = a.get(b"key").expect("get").expect("value").as_ptr() as usize;
    let p = prepare(a);
    let b = staged(&p);
    let a = p.abort().expect("other participant failed before commit");
    assert!(matches!(
        b.activate(Operation([3; 16])),
        Err(Error::Inactive)
    ));
    let value = a.get(b"key").expect("get").expect("value");
    assert_eq!(&*value, b"committed");
    assert_eq!(value.as_ptr() as usize, pointer);
    drop(value);
    a.insert(b"key", b"after-abort", None)
        .expect("resumed write");
    let p = prepare(a);
    let b = staged(&p);
    drop(p); // Uncertain outcome must not implicitly abort.
    let b = b
        .activate(Operation([3; 16]))
        .expect("recorded global commit");
    assert_eq!(
        &*b.get(b"key").expect("get").expect("value"),
        b"after-abort"
    );
}

#[test]
fn live_worker_borrow_refuses_prepare_and_timeout_returns_active_cache() {
    let a = Cache::create(options(8)).expect("create");
    a.insert(b"key", b"value", None).expect("write");
    let worker = a.clone();
    let value = worker.get(b"key").expect("get").expect("value");
    let failed = a
        .prepare(
            Operation([4; 16]),
            Peer {
                pid: std::process::id() + 1,
                uid: Peer::current().uid,
            },
            Duration::from_millis(5),
        )
        .expect_err("live worker must drain");
    assert!(matches!(failed.error, Error::DrainTimeout));
    assert_eq!(&*value, b"value");
    drop(value);
    drop(worker);
    let a = failed.cache;
    a.insert(b"key", b"after-timeout", None)
        .expect("still active");
    let p = prepare(a);
    let a = p.abort().expect("abort");
    assert_eq!(
        &*a.get(b"key").expect("get").expect("value"),
        b"after-timeout"
    );
}

#[test]
fn preparation_waits_for_last_acknowledged_worker_write() {
    let a = Cache::create(options(8)).expect("create");
    let worker = a.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel();
    let writer = thread::spawn(move || {
        worker
            .insert(b"sequence", &1u64.to_le_bytes(), None)
            .expect("write");
        ready_tx.send(()).expect("ready");
        stop_rx.recv().expect("close application admission");
        worker
            .insert(b"sequence", &2u64.to_le_bytes(), None)
            .expect("last admitted write");
    });
    ready_rx.recv().expect("ready");
    let waiter = thread::spawn(move || prepare(a));
    stop_tx.send(()).expect("stop");
    writer.join().expect("writer");
    let p = waiter.join().expect("drained");
    let b = staged(&p).activate(p.receipt.operation).expect("activate");
    assert_eq!(
        &*b.get(b"sequence").expect("get").expect("value"),
        &2u64.to_le_bytes()
    );
}

#[test]
fn capacity_ttl_defaults_limits_and_expired_slot_reuse_survive_attach() {
    let a = Cache::create(options(2)).expect("create");
    a.insert(b"live", b"one", None).expect("write");
    a.insert(b"expires", b"two", Some(Duration::from_millis(15)))
        .expect("write");
    assert!(matches!(
        a.insert(b"full", b"three", None),
        Err(Error::Capacity)
    ));
    let p = prepare(a);
    let b = staged(&p).activate(p.receipt.operation).expect("activate");
    thread::sleep(Duration::from_millis(25));
    assert!(b.get(b"expires").expect("get").is_none());
    b.insert(b"new", b"three", None).expect("reclaim expired");
    assert_eq!(&*b.get(b"live").expect("get").expect("value"), b"one");
    assert!(matches!(b.insert(&[0; 33], b"v", None), Err(Error::Limit)));
    assert!(matches!(b.insert(b"v", &[0; 129], None), Err(Error::Limit)));
    assert!(matches!(b.get(&[0; 33]), Err(Error::Limit)));
    assert!(b.remove(b"live").expect("remove"));
    assert!(!b.remove(b"live").expect("remove"));
    b.insert(b"", b"", None).expect("empty key/value");
    assert!(b.get(b"").expect("get").is_some());
    let a = Cache::create(Options {
        default_ttl_ms: Some(0),
        ..options(2)
    })
    .expect("default ttl");
    a.insert(b"key", b"value", None).expect("write");
    assert!(a.get(b"key").expect("get").is_none());
    assert!(!a.remove(b"key").expect("remove expired"));
}

#[test]
fn fixed_shard_quota_refuses_skew_without_spilling_or_evicting() {
    let a = Cache::create(options(256)).expect("create");
    assert_eq!(a.shard_count(), 64);
    assert_eq!(a.shard_capacity(0), Some(4));
    assert_eq!(a.shard_capacity(64), None);
    let keys: Vec<_> = (0..10000u64)
        .map(u64::to_le_bytes)
        .filter(|key| {
            crate::storage::stripe_index(
                crate::storage::hash_key(key),
                crate::storage::shift_for(64),
            ) == 0
        })
        .take(5)
        .collect();
    assert_eq!(keys.len(), 5);
    for key in &keys[..4] {
        a.insert(key, b"value", None).expect("within quota");
    }
    assert!(matches!(
        a.insert(&keys[4], b"value", None),
        Err(Error::Capacity)
    ));
    assert!(a.remove(&keys[0]).expect("remove"));
    a.insert(&keys[4], b"replacement", None)
        .expect("reuse same shard");
    for key in &keys[1..4] {
        assert_eq!(&*a.get(key).expect("get").expect("value"), b"value");
    }
}

#[test]
fn deletion_churn_never_requires_another_table_allocation() {
    let a = Cache::create(options(32)).expect("create");
    for i in 0..32u64 {
        a.insert(&i.to_le_bytes(), b"value", None).expect("fill");
    }
    for i in 32..20000u64 {
        assert!(a.remove(&(i - 32).to_le_bytes()).expect("remove"));
        a.insert(&i.to_le_bytes(), b"new", None)
            .expect("no allocation growth");
    }
    let p = prepare(a);
    let b = staged(&p)
        .activate(p.receipt.operation)
        .expect("attach after churn");
    for i in 19968..20000u64 {
        assert_eq!(
            &*b.get(&i.to_le_bytes()).expect("get").expect("value"),
            b"new"
        );
    }
}

#[test]
fn credentials_recipient_operation_engine_profile_and_replay_are_checked() {
    let a = Cache::create(options(4)).expect("create");
    let p = prepare(a);
    let receipt = candidate(&p);
    assert!(matches!(
        Staged::validate(p.mapping.clone(), receipt, &[0; 32]),
        Err(Error::Unauthorized)
    ));
    let fd = || p.mapping.file.try_clone().expect("fd").into();
    let mut wrong = receipt;
    wrong.recipient.pid += 1;
    assert!(matches!(
        Staged::attach(fd(), wrong, &p.credential),
        Err(Error::Unauthorized)
    ));
    let mut wrong = receipt;
    wrong.engine_abi[0] ^= 1;
    assert!(matches!(
        Staged::attach(fd(), wrong, &p.credential),
        Err(Error::Profile)
    ));
    let mut wrong = receipt;
    wrong.options.capacity += 1;
    assert!(matches!(
        Staged::validate(p.mapping.clone(), wrong, &p.credential),
        Err(Error::Profile)
    ));
    assert!(matches!(
        staged(&p).activate(Operation([9; 16])),
        Err(Error::Unauthorized)
    ));
    let stale = staged(&p);
    let b = staged(&p).activate(p.receipt.operation).expect("activate");
    assert!(matches!(
        stale.activate(p.receipt.operation),
        Err(Error::Inactive)
    ));
    assert!(matches!(
        Staged::validate(p.mapping.clone(), receipt, &p.credential),
        Err(Error::Inactive)
    ));
    drop(b);
}

#[test]
fn occupied_virtual_range_is_refused_without_overwriting_it() {
    let a = Cache::create(options(4)).expect("create");
    a.insert(b"key", b"value", None).expect("write");
    let p = prepare(a);
    let receipt = candidate(&p);
    let fd = p.mapping.file.try_clone().expect("fd").into();
    assert!(
        matches!(Staged::attach(fd, receipt, &p.credential), Err(Error::Io(error)) if error.raw_os_error() == Some(libc::EEXIST))
    );
    let a = p.abort().expect("original mapping unchanged");
    assert_eq!(&*a.get(b"key").expect("get").expect("value"), b"value");
}

#[test]
fn format_and_descriptor_policy_refuse_import() {
    let a = Cache::create(options(4)).expect("create");
    let p = prepare(a);
    let receipt = candidate(&p);
    // SAFETY: parked cache, prefix only, no outstanding reference during write.
    unsafe {
        std::ptr::addr_of_mut!((*p.mapping.ptr.as_ptr().cast::<Header>()).layout).write(2);
    }
    assert!(matches!(
        Staged::validate(p.mapping.clone(), receipt, &p.credential),
        Err(Error::Profile)
    ));
    use std::os::unix::fs::PermissionsExt;
    let file = tempfile::tempfile().expect("file");
    file.set_permissions(std::fs::Permissions::from_mode(0o644))
        .expect("mode");
    assert!(matches!(
        Staged::attach(file.into(), receipt, &p.credential),
        Err(Error::Profile)
    ));
}

#[test]
fn abort_racing_activation_never_exposes_two_active_caches() {
    for _ in 0..8 {
        let a = Cache::create(options(4)).expect("create");
        a.insert(b"key", b"value", None).expect("write");
        let p = prepare(a);
        let b = staged(&p);
        let ready = Arc::new(std::sync::Barrier::new(2));
        let other = ready.clone();
        let worker = thread::spawn(move || {
            other.wait();
            p.abort()
        });
        ready.wait();
        let successor = b.activate(Operation([3; 16]));
        let old = worker.join().expect("race");
        assert_ne!(old.is_ok(), successor.is_ok());
        let active = old.or(successor).expect("one owner");
        assert_eq!(&*active.get(b"key").expect("get").expect("value"), b"value");
    }
}

#[test]
fn malformed_control_frames_close_every_received_descriptor() {
    use std::io::Write;
    use std::net::Shutdown;
    use std::os::unix::fs::MetadataExt;
    protect_current_process().expect("protect");
    let file = tempfile::tempfile().expect("file");
    let identity = file.metadata().expect("metadata");
    let references = || {
        std::fs::read_dir("/proc/self/fd")
            .expect("fds")
            .filter_map(|p| {
                let metadata = std::fs::metadata(p.ok()?.path()).ok()?;
                Some(usize::from(
                    metadata.dev() == identity.dev() && metadata.ino() == identity.ino(),
                ))
            })
            .sum::<usize>()
    };
    let before = references();
    let a = Cache::create(options(2)).expect("create");
    let prepared = prepare(a);
    let bytes = serde_json::to_vec(&prepared.receipt).expect("receipt");
    let mut short = (bytes.len() as u32).to_le_bytes().to_vec();
    short.extend(bytes);
    short.extend([0; 7]);
    let mut invalid = 3u32.to_le_bytes().to_vec();
    invalid.extend(b"{!}");
    for (count, payload) in [
        (0, vec![]),
        (16, vec![]),
        (1, 2050u32.to_le_bytes().to_vec()),
        (1, invalid),
        (1, short),
    ] {
        let (mut left, mut right) = UnixStream::pair().expect("pair");
        let mut tag = [1u8];
        let mut iov = libc::iovec {
            iov_base: tag.as_mut_ptr().cast(),
            iov_len: 1,
        };
        let mut control = [0u64; 16];
        // SAFETY: initialized with live aligned buffers; test deliberately sends
        // bounded invalid payloads/descriptor counts, not forged kernel cmsgs.
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        if count != 0 {
            msg.msg_control = control.as_mut_ptr().cast();
            // SAFETY: at most 16 valid descriptors fit this aligned buffer.
            unsafe {
                msg.msg_controllen = libc::CMSG_SPACE(count * 4) as usize;
                let c = libc::CMSG_FIRSTHDR(&msg);
                (*c).cmsg_level = libc::SOL_SOCKET;
                (*c).cmsg_type = libc::SCM_RIGHTS;
                (*c).cmsg_len = libc::CMSG_LEN(count * 4) as usize;
                for i in 0..count as usize {
                    libc::CMSG_DATA(c)
                        .cast::<i32>()
                        .add(i)
                        .write(file.as_raw_fd());
                }
            }
        }
        // SAFETY: synchronous call; borrowed test buffers and descriptor are live.
        assert_eq!(
            unsafe { libc::sendmsg(right.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) },
            1
        );
        right.write_all(&payload).expect("payload");
        right.shutdown(Shutdown::Write).expect("eof");
        assert!(transport::receive(&mut left).is_err());
        assert_eq!(
            references(),
            before,
            "received descriptor leaked on rejected frame"
        );
    }
}
