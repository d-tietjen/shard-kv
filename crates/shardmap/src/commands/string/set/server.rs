use crate::commands::CommandSpec;
use crate::protocol::{FastCommand, FastRequest, FastResponse};
use crate::server::commands::{
    DirectCommandContext, DirectFastCommand, FastCommandContext, FastDirectCommand,
    RawCommandContext, RawDirectCommand,
};
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, hash_key};

use super::Set;
use super::options::{SetCondition, SetOptions};
use super::storage::EmbeddedStringWrite;

#[cfg(feature = "server")]
impl RawDirectCommand for Set {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        let RawCommandContext {
            store,
            args,
            out,
            resp_protocol,
            ..
        } = ctx;
        match SetRawArgs::from_args(store, args.as_slice()) {
            ready @ (SetRawArgs::Ready { .. } | SetRawArgs::KeepTtl { .. }) => {
                if !ready.apply_ready(store) {
                    ServerWire::write_resp_error(
                        out,
                        "ERR mutation rejected by an installed storage extension",
                    );
                    return;
                }
                out.extend_from_slice(b"+OK\r\n");
            }
            SetRawArgs::Null => ServerWire::write_resp_null(out, resp_protocol),
            SetRawArgs::WrongArity => ServerWire::write_resp_error(
                out,
                &format!(
                    "ERR wrong number of arguments for '{}' command",
                    <Self as CommandSpec>::NAME
                ),
            ),
            SetRawArgs::Syntax => ServerWire::write_resp_error(out, "ERR syntax error"),
        }
    }
}

#[cfg(feature = "server")]
enum SetRawArgs<'a> {
    Ready {
        key: &'a [u8],
        value: &'a [u8],
        ttl_ms: Option<u64>,
    },
    KeepTtl {
        key: &'a [u8],
        value: &'a [u8],
    },
    Null,
    WrongArity,
    Syntax,
}

#[cfg(feature = "server")]
impl<'a> SetRawArgs<'a> {
    fn from_args(store: &EmbeddedStore, args: &'a [&'a [u8]]) -> Self {
        match args {
            [key, value] => Self::Ready {
                key,
                value,
                ttl_ms: None,
            },
            [key, value, rest @ ..] => match SetOptions::parse(rest) {
                Some(options) => Self::from_options(store, key, value, options),
                None => Self::Syntax,
            },
            _ => Self::WrongArity,
        }
    }

    fn from_options(
        store: &EmbeddedStore,
        key: &'a [u8],
        value: &'a [u8],
        options: SetOptions,
    ) -> Self {
        let allowed = match options.condition {
            SetCondition::Always => true,
            SetCondition::Nx => !store.exists(key),
            SetCondition::Xx => store.exists(key),
        };
        match allowed {
            true if options.keep_ttl() => Self::KeepTtl { key, value },
            true => Self::Ready {
                key,
                value,
                ttl_ms: options.ttl_ms(),
            },
            false => Self::Null,
        }
    }

    #[inline]
    fn apply_ready(self, store: &EmbeddedStore) -> bool {
        match self {
            Self::Ready { key, value, ttl_ms } => {
                if !store.point_mutation_is_accepted(key, value.len(), None) {
                    return false;
                }
                store.set_slice_prehashed(hash_key(key), key, value, ttl_ms);
                true
            }
            Self::KeepTtl { key, value } => {
                if !store.point_mutation_is_accepted(key, value.len(), None) {
                    return false;
                }
                store.set_slice_keep_ttl(key, value, false);
                true
            }
            _ => false,
        }
    }
}

#[cfg(feature = "server")]
impl EmbeddedStringWrite for Set {}

#[cfg(feature = "server")]
impl DirectFastCommand for Set {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        match request.command {
            FastCommand::Set { key, value } => {
                ctx.set_owned(key.to_vec(), value.to_vec(), None);
                FastResponse::Ok
            }
            _ => FastResponse::Error(b"ERR unsupported command".to_vec()),
        }
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for Set {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match command {
            FastCommand::Set { key, value } => {
                if !<Self as EmbeddedStringWrite>::set_decoded(
                    ctx.store,
                    ctx.key_hash,
                    key,
                    value,
                    None,
                    ctx.single_threaded,
                ) {
                    ServerWire::write_fast_error(
                        ctx.out,
                        "ERR mutation rejected by an installed storage extension",
                    );
                } else {
                    ServerWire::write_fast_ok(ctx.out);
                }
            }
            _ => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}

#[cfg(all(test, feature = "server", not(feature = "no-ttl")))]
mod keepttl_deadline_tests {
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::SetRawArgs;
    use crate::storage::EmbeddedStore;

    const PARSE_APPLY_DELAY: Duration = Duration::from_millis(20);

    fn entry(store: &EmbeddedStore, key: &[u8]) -> Option<(Vec<u8>, Option<u64>)> {
        store
            .try_entry_snapshot()
            .expect("string snapshot must succeed")
            .into_iter()
            .find(|entry| entry.key.as_slice() == key)
            .map(|entry| (entry.value, entry.expire_at_ms))
    }

    fn wall_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test wall clock must follow the Unix epoch")
            .as_millis()
            .try_into()
            .expect("test wall clock must fit u64")
    }

    fn apply_ready(store: &EmbeddedStore, parsed: SetRawArgs<'_>) {
        assert!(
            parsed.apply_ready(store),
            "production SET application rejected"
        );
    }

    fn apply_after_delay(store: &EmbeddedStore, parsed: SetRawArgs<'_>) -> (u64, u64) {
        // The single fixed sleep separates actual parsing from production
        // application, retaining the original delay and exact expiry oracle.
        let parsed_at_ms = wall_millis();
        let delay_started = Instant::now();
        std::thread::sleep(PARSE_APPLY_DELAY);
        let apply_at_ms = wall_millis();
        let elapsed = delay_started.elapsed();
        assert!(elapsed >= PARSE_APPLY_DELAY);
        assert!(
            elapsed < Duration::from_secs(5),
            "test delay was overscheduled"
        );
        assert!(apply_at_ms > parsed_at_ms, "wall clock did not advance");
        apply_ready(store, parsed);
        eprintln!(
            "KEEPTTL_DELAY requested_ms=20 actual_ns={} parsed_after_ms={} apply_before_ms={}",
            elapsed.as_nanos(),
            parsed_at_ms,
            apply_at_ms
        );
        (parsed_at_ms, apply_at_ms)
    }

    #[test]
    fn keepttl_delayed_parse_apply_preserves_absolute_deadline() {
        for initial_ttl_ms in [60_000, 90_000, 120_000] {
            for condition in [None, Some(b"XX".as_slice())] {
                let store = EmbeddedStore::new(1);
                let key = b"keepttl-delayed-existing".as_slice();
                store.set(key.to_vec(), b"original".to_vec(), Some(initial_ttl_ms));
                let before = entry(&store, key).expect("seeded TTL entry");
                let original_deadline = before.1.expect("seeded absolute deadline");
                let mut args = vec![key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
                if let Some(condition) = condition {
                    args.push(condition);
                }
                let parsed = SetRawArgs::from_args(&store, &args);
                let (parsed_at_ms, apply_at_ms) = apply_after_delay(&store, parsed);
                assert!(
                    original_deadline > apply_at_ms,
                    "seed TTL expired before apply"
                );
                let after = entry(&store, key).expect("updated entry remains live");
                assert_eq!(after.0.as_slice(), b"updated".as_slice());
                assert_eq!(
                    after.1,
                    Some(original_deadline),
                    "KEEPTTL changed the original absolute expiry: initial_ttl_ms={initial_ttl_ms} \
                     condition={condition:?} parsed_after_ms={parsed_at_ms} apply_before_ms={apply_at_ms}"
                );
            }
        }
    }

    #[test]
    fn keepttl_persistent_entries_stay_persistent_after_delay() {
        for condition in [None, Some(b"XX".as_slice())] {
            let store = EmbeddedStore::new(1);
            let key = b"keepttl-persistent-existing".as_slice();
            store.set(key.to_vec(), b"original".to_vec(), None);
            assert_eq!(entry(&store, key), Some((b"original".to_vec(), None)));
            let mut args = vec![key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
            if let Some(condition) = condition {
                args.push(condition);
            }
            let parsed = SetRawArgs::from_args(&store, &args);
            apply_after_delay(&store, parsed);
            assert_eq!(entry(&store, key), Some((b"updated".to_vec(), None)));
        }
    }

    #[test]
    fn keepttl_missing_entries_are_created_persistent_after_delay() {
        for condition in [None, Some(b"NX".as_slice())] {
            let store = EmbeddedStore::new(1);
            let key = b"keepttl-missing-create".as_slice();
            assert_eq!(entry(&store, key), None);
            let mut args = vec![key, b"created".as_slice(), b"KEEPTTL".as_slice()];
            if let Some(condition) = condition {
                args.push(condition);
            }
            let parsed = SetRawArgs::from_args(&store, &args);
            apply_after_delay(&store, parsed);
            assert_eq!(entry(&store, key), Some((b"created".to_vec(), None)));
        }
    }

    #[test]
    fn keepttl_nx_rejection_preserves_value_and_absolute_expiry() {
        for initial_ttl_ms in [None, Some(60_000)] {
            let store = EmbeddedStore::new(1);
            let key = b"keepttl-nx-existing".as_slice();
            store.set(key.to_vec(), b"original".to_vec(), initial_ttl_ms);
            let before = entry(&store, key).expect("seeded NX entry");
            let args = [
                key,
                b"rejected".as_slice(),
                b"KEEPTTL".as_slice(),
                b"NX".as_slice(),
            ];
            assert!(matches!(
                SetRawArgs::from_args(&store, &args),
                SetRawArgs::Null
            ));
            assert_eq!(entry(&store, key), Some(before));
        }
    }

    #[test]
    fn keepttl_xx_rejection_does_not_create_missing_entry() {
        let store = EmbeddedStore::new(1);
        let key = b"keepttl-xx-missing".as_slice();
        let args = [
            key,
            b"rejected".as_slice(),
            b"KEEPTTL".as_slice(),
            b"XX".as_slice(),
        ];
        assert!(matches!(
            SetRawArgs::from_args(&store, &args),
            SetRawArgs::Null
        ));
        assert_eq!(entry(&store, key), None);
    }

    #[test]
    fn plain_set_after_parse_clears_existing_absolute_expiry() {
        let store = EmbeddedStore::new(1);
        let key = b"plain-set-expiry-control".as_slice();
        store.set(key.to_vec(), b"original".to_vec(), Some(60_000));
        assert!(entry(&store, key).expect("seeded TTL entry").1.is_some());
        let args = [key, b"updated".as_slice()];
        let parsed = SetRawArgs::from_args(&store, &args);
        assert!(matches!(&parsed, SetRawArgs::Ready { ttl_ms: None, .. }));
        apply_after_delay(&store, parsed);
        assert_eq!(entry(&store, key), Some((b"updated".to_vec(), None)));
    }

    #[test]
    fn keepttl_conflicting_expiry_or_conditions_remain_syntax_errors() {
        let store = EmbeddedStore::new(1);
        let key = b"keepttl-invalid-options".as_slice();
        store.set(key.to_vec(), b"original".to_vec(), Some(60_000));
        let before = entry(&store, key).expect("seeded syntax control entry");
        let invalid: [&[&[u8]]; 4] = [
            &[b"KEEPTTL", b"EX", b"60"],
            &[b"KEEPTTL", b"PX", b"60"],
            &[b"KEEPTTL", b"NX", b"XX"],
            &[b"NX", b"XX", b"KEEPTTL"],
        ];
        for options in invalid {
            let mut args = vec![key, b"rejected".as_slice()];
            args.extend_from_slice(options);
            assert!(matches!(
                SetRawArgs::from_args(&store, &args),
                SetRawArgs::Syntax
            ));
            assert_eq!(entry(&store, key), Some(before.clone()));
        }
    }

    #[test]
    fn keepttl_uses_the_intervening_write_expiry() {
        for replacement_ttl in [None, Some(120_000)] {
            let store = EmbeddedStore::new(1);
            let key = b"keepttl-intervening-write".as_slice();
            store.set(key.to_vec(), b"original".to_vec(), Some(60_000));
            let args = [key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
            let parsed = SetRawArgs::from_args(&store, &args);
            store.set(key.to_vec(), b"intervening".to_vec(), replacement_ttl);
            let expected_expiry = entry(&store, key).expect("intervening entry").1;
            apply_after_delay(&store, parsed);
            assert_eq!(
                entry(&store, key),
                Some((b"updated".to_vec(), expected_expiry))
            );
        }
    }

    #[test]
    fn keepttl_expired_between_parse_and_apply_creates_persistent_value() {
        let store = EmbeddedStore::new(1);
        let key = b"keepttl-expired-before-apply".as_slice();
        store.set(key.to_vec(), b"original".to_vec(), Some(60_000));
        let args = [key, b"created".as_slice(), b"KEEPTTL".as_slice()];
        let parsed = SetRawArgs::from_args(&store, &args);
        assert!(store.expire(key, wall_millis().saturating_sub(1)));
        apply_after_delay(&store, parsed);
        assert_eq!(entry(&store, key), Some((b"created".to_vec(), None)));
    }

    #[test]
    fn keepttl_session_route_replaces_the_current_session_value() {
        use crate::storage::EmbeddedRouteMode;

        let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::SessionPrefix);
        let key = b"s:keep-session:c:1".as_slice();
        store.batch_set_session_slices_no_ttl(b"s:keep-session", [(key, b"original")]);
        let args = [key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
        apply_after_delay(&store, SetRawArgs::from_args(&store, &args));
        assert_eq!(store.get(key), Some(b"updated".to_vec()));
        assert_eq!(entry(&store, key), Some((b"updated".to_vec(), None)));
        store.set(key.to_vec(), b"ttl-value".to_vec(), Some(60_000));
        let deadline = entry(&store, key).expect("routed TTL entry").1;
        apply_after_delay(&store, SetRawArgs::from_args(&store, &args));
        assert_eq!(entry(&store, key), Some((b"updated".to_vec(), deadline)));
    }

    #[cfg(feature = "redis")]
    #[test]
    fn keepttl_object_overwrite_retains_the_object_deadline() {
        let store = EmbeddedStore::new(4);
        let key = b"keepttl-object".as_slice();
        let _ = store.hset(key, b"field", b"original");
        assert!(store.has_redis_objects());
        let deadline = wall_millis().saturating_add(60_000);
        assert!(store.expire(key, deadline));
        let args = [key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
        apply_after_delay(&store, SetRawArgs::from_args(&store, &args));
        assert_eq!(
            entry(&store, key),
            Some((b"updated".to_vec(), Some(deadline)))
        );
        assert_eq!(store.len(), 1);
        assert_eq!(store.redis_type(key), "string");
        assert_eq!(
            store.key_snapshot(),
            vec![bytes::Bytes::copy_from_slice(key)]
        );
        // The exact key snapshot refreshes the cached object-presence hint via
        // object_count(); deletion alone intentionally leaves that hint stale.
        assert!(!store.has_redis_objects());
    }

    #[test]
    fn keepttl_observer_receives_the_exact_written_expiry_once() {
        use crate::storage::PointMutationKind;
        use std::sync::{Arc, Mutex};

        let store = EmbeddedStore::new(1);
        let key = b"keepttl-observer".as_slice();
        store.set(key.to_vec(), b"original".to_vec(), Some(60_000));
        let deadline = entry(&store, key).expect("seeded observer entry").1;
        let observed = Arc::new(Mutex::new(Vec::new()));
        let sink = observed.clone();
        store.configure_point_mutation_observer(Some(Arc::new(
            move |kind, key, value, expiry, governance| {
                assert_eq!(kind, PointMutationKind::Set);
                assert!(governance.is_none());
                sink.lock().expect("observer lock").push((
                    key.to_vec(),
                    value.expect("set value").to_vec(),
                    expiry,
                ));
            },
        )));
        let args = [key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
        apply_after_delay(&store, SetRawArgs::from_args(&store, &args));
        assert_eq!(
            *observed.lock().expect("observer assertions"),
            vec![(key.to_vec(), b"updated".to_vec(), deadline)]
        );
    }

    #[test]
    fn keepttl_validator_rejection_preserves_value_expiry_and_observer() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let store = EmbeddedStore::new(1);
        let key = b"keepttl-validator".as_slice();
        store.set(key.to_vec(), b"original".to_vec(), Some(60_000));
        let before = entry(&store, key);
        let calls = Arc::new(AtomicUsize::new(0));
        let sink = calls.clone();
        store.configure_point_mutation_observer(Some(Arc::new(move |_, _, _, _, _| {
            sink.fetch_add(1, Ordering::Relaxed);
        })));
        store.configure_point_mutation_validator(Some(Arc::new(|_, _, _| false)));
        let args = [key, b"rejected".as_slice(), b"KEEPTTL".as_slice()];
        assert!(!SetRawArgs::from_args(&store, &args).apply_ready(&store));
        assert_eq!(entry(&store, key), before);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn keepttl_raw_prehashed_replacement_clears_governance() {
        let key = b"keepttl-governed-raw".as_slice();
        for keep in [false, true] {
            let store = EmbeddedStore::new(4);
            store.set_value_bytes_with_governance(
                key,
                bytes::Bytes::from_static(b"original"),
                Some(60_000),
                bytes::Bytes::from_static(b"policy"),
            );
            let seeded = store.try_entry_snapshot().expect("governed seed snapshot");
            let seeded = seeded
                .iter()
                .find(|entry| entry.key.as_slice() == key)
                .expect("seeded governed entry");
            assert_eq!(seeded.governance.as_deref(), Some(b"policy".as_slice()));
            assert_eq!(store.get(key), None);
            let deadline = entry(&store, key).expect("governed seed").1;
            if keep {
                let args = [key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
                apply_after_delay(&store, SetRawArgs::from_args(&store, &args));
            } else {
                store.set_slice_prehashed(
                    crate::storage::hash_key(key),
                    key,
                    b"updated",
                    Some(60_000),
                );
            }
            let snapshot = store.try_entry_snapshot().expect("governed snapshot");
            let updated = snapshot
                .iter()
                .find(|entry| entry.key.as_slice() == key)
                .expect("updated governed entry");
            assert_eq!(updated.value.as_slice(), b"updated");
            assert_eq!(updated.governance, None);
            assert_eq!(store.get(key), Some(b"updated".to_vec()));
            if keep {
                assert_eq!(updated.expire_at_ms, deadline);
            }
        }
    }

    #[test]
    fn keepttl_raw_route_fallback_clears_governance() {
        use crate::storage::EmbeddedRouteMode;

        let key = b"s:keepttl-governed-route:c:1".as_slice();
        for mode in [
            EmbeddedRouteMode::SessionPrefix,
            EmbeddedRouteMode::OverflowSlot,
        ] {
            for keep in [false, true] {
                let store = EmbeddedStore::with_route_mode(4, mode);
                store.set_value_bytes_with_governance(
                    key,
                    bytes::Bytes::from_static(b"original"),
                    Some(60_000),
                    bytes::Bytes::from_static(b"policy"),
                );
                let deadline = entry(&store, key).expect("routed governed seed").1;
                if keep {
                    let args = [key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
                    apply_after_delay(&store, SetRawArgs::from_args(&store, &args));
                } else {
                    store.set_slice_prehashed(
                        crate::storage::hash_key(key),
                        key,
                        b"updated",
                        Some(60_000),
                    );
                }
                let snapshot = store.try_entry_snapshot().expect("routed snapshot");
                let updated = snapshot
                    .iter()
                    .find(|entry| entry.key.as_slice() == key)
                    .expect("updated routed entry");
                assert_eq!(updated.governance, None);
                assert_eq!(store.get(key), Some(b"updated".to_vec()));
                if keep {
                    assert_eq!(updated.expire_at_ms, deadline);
                }
            }
        }
    }

    #[cfg(all(feature = "kv-overflow", feature = "object-overflow"))]
    #[derive(Default)]
    struct NoPayloadReads {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[cfg(all(feature = "kv-overflow", feature = "object-overflow"))]
    impl crate::storage::ObjectOverflowStore for NoPayloadReads {
        fn put_value(&self, _object_key: &str, _value: &[u8]) -> crate::Result<()> {
            Ok(())
        }

        fn get_value(&self, _object_key: &str) -> crate::Result<bytes::Bytes> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(crate::ShardCacheError::ObjectIntegrity(
                "KEEPTTL must not materialize the cold payload".into(),
            ))
        }

        fn delete_value(&self, _object_key: &str) -> crate::Result<()> {
            Ok(())
        }
    }

    #[cfg(all(feature = "kv-overflow", feature = "object-overflow"))]
    fn cold_fixture(
        key: &[u8],
        ttl_ms: Option<u64>,
    ) -> (EmbeddedStore, std::sync::Arc<NoPayloadReads>, Option<u64>) {
        use crate::config::{
            EvictionPolicy, ObjectOverflowBackend, ObjectOverflowCompression,
            ObjectOverflowFailurePolicy,
        };
        use crate::storage::{ObjectOverflowRuntime, ObjectOverflowRuntimeOptions};
        use std::sync::Arc;

        let backend = Arc::new(NoPayloadReads::default());
        let runtime = ObjectOverflowRuntime::new(
            backend.clone(),
            ObjectOverflowRuntimeOptions {
                backend: ObjectOverflowBackend::File,
                min_value_bytes: 4,
                offload_min_idle_ticks: 0,
                offload_max_frequency: u32::MAX,
                compression: ObjectOverflowCompression::Zstd,
                zstd_level: 3,
                failure_policy: ObjectOverflowFailurePolicy::RetainResident,
                max_retries: 0,
                retry_backoff: Duration::from_millis(1),
                operation_timeout: Duration::from_millis(1_000),
                worker_threads: 1,
                queue_capacity: 16,
                degraded_failure_threshold: 8,
                degraded_cooldown: Duration::from_millis(100),
                fetch_on_get: true,
                delete_on_overwrite: true,
                prefix: "test-overflow".to_string(),
                node_id: "keepttl-test".to_string(),
                generation_id: "keepttl-generation".to_string(),
                cleanup_on_start: false,
                cleanup_grace: Duration::from_secs(60),
            },
        )
        .expect("bounded mock overflow runtime");
        let store = EmbeddedStore::new(1);
        store
            .configure_object_overflow(Some(runtime))
            .expect("overflow configuration");
        store.set(key.to_vec(), vec![42; 64 * 1024], ttl_ms);
        let deadline = entry(&store, key).expect("resident seed before offload").1;
        store.configure_memory_policy(Some(8), EvictionPolicy::Lru);
        let started = Instant::now();
        let mut observed = false;
        for _ in 0..500 {
            store.process_maintenance();
            if store.shard_stats_snapshot()[0]
                .object_overflow
                .remote_entries
                == 1
            {
                observed = true;
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(5));
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(observed, "cold entry never became remote");
        store.configure_memory_policy(None, EvictionPolicy::Lru);
        assert_eq!(backend.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
        (store, backend, deadline)
    }

    #[cfg(all(feature = "kv-overflow", feature = "object-overflow"))]
    #[test]
    fn keepttl_cold_point_retains_scalar_absolute_deadline_without_materialization() {
        let key = b"keepttl-live-cold".as_slice();
        let (store, backend, deadline) = cold_fixture(key, Some(60_000));
        assert!(deadline.is_some());
        let args = [key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
        apply_after_delay(&store, SetRawArgs::from_args(&store, &args));
        assert_eq!(entry(&store, key), Some((b"updated".to_vec(), deadline)));
        assert_eq!(
            store.shard_stats_snapshot()[0]
                .object_overflow
                .remote_entries,
            0
        );
        assert_eq!(backend.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[cfg(all(feature = "kv-overflow", feature = "object-overflow"))]
    #[test]
    fn keepttl_persistent_cold_point_remains_persistent_without_materialization() {
        let key = b"keepttl-persistent-cold".as_slice();
        let (store, backend, deadline) = cold_fixture(key, None);
        assert_eq!(deadline, None);
        let args = [key, b"updated".as_slice(), b"KEEPTTL".as_slice()];
        apply_after_delay(&store, SetRawArgs::from_args(&store, &args));
        assert_eq!(entry(&store, key), Some((b"updated".to_vec(), None)));
        assert_eq!(backend.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[cfg(all(feature = "kv-overflow", feature = "object-overflow"))]
    #[test]
    fn keepttl_expired_cold_point_creates_persistent_replacement_without_materialization() {
        let key = b"keepttl-expired-cold".as_slice();
        let (store, backend, deadline) = cold_fixture(key, Some(5_000));
        let deadline = deadline.expect("short cold fixture deadline");
        let now = wall_millis();
        assert!(deadline > now, "cold fixture expired before the boundary");
        let until_expired = deadline - now + 1;
        assert!(until_expired <= 5_001);
        std::thread::sleep(Duration::from_millis(until_expired));
        assert!(wall_millis() > deadline, "fixture deadline did not pass");
        let args = [key, b"created".as_slice(), b"KEEPTTL".as_slice()];
        apply_after_delay(&store, SetRawArgs::from_args(&store, &args));
        assert_eq!(entry(&store, key), Some((b"created".to_vec(), None)));
        assert_eq!(backend.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}
