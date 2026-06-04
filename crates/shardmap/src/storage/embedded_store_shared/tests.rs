use std::collections::HashMap;
use std::thread;

use super::*;
use crate::storage::{hash_key, stripe_index};

#[test]
fn shared_store_matches_hash_map_for_point_keys() {
    let store = SharedEmbeddedStore::<16>::new(SharedEmbeddedConfig::default());
    let mut oracle = HashMap::new();

    for index in 0..1_000usize {
        let key = SharedBytes::from(format!("key-{index}"));
        let value = SharedBytes::from(format!("value-{index}"));
        store.insert(key.clone(), value.clone());
        oracle.insert(key, value);
    }

    for (key, value) in oracle {
        let stored = store.get(key.as_ref()).expect("value exists");
        assert_eq!(stored.value(), value.as_ref());
    }
}

#[test]
fn route_key_uses_shift_based_striping() {
    let store = SharedEmbeddedStore::<16>::new(SharedEmbeddedConfig::default());
    let key = b"route-me";
    let route = store.route_key(key);
    assert_eq!(route.shard_id, stripe_index(hash_key(key), shift_for(16)));
}

#[test]
fn cloned_handles_work_from_multiple_threads() {
    let store = SharedEmbeddedStore::<16>::new(SharedEmbeddedConfig::default());
    let mut workers = Vec::new();

    for worker_id in 0..8usize {
        let store = store.clone();
        workers.push(thread::spawn(move || {
            for index in 0..1_250usize {
                let key = SharedBytes::from(format!("worker-{worker_id}-key-{index}"));
                let value = SharedBytes::from(format!("worker-{worker_id}-value-{index}"));
                store.insert(key.clone(), value.clone());
                assert_eq!(store.get(key.as_ref()).unwrap().value(), value.as_ref());
            }
        }));
    }

    for worker in workers {
        worker.join().expect("worker");
    }

    for worker_id in 0..8usize {
        for index in 0..1_250usize {
            let key = format!("worker-{worker_id}-key-{index}");
            let value = format!("worker-{worker_id}-value-{index}");
            assert_eq!(store.get(key.as_bytes()).unwrap().value(), value.as_bytes());
        }
    }
}

#[test]
fn entry_and_mutation_guards_update_values() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    let key = SharedBytes::from_static(b"entry-key");
    let value = SharedBytes::from_static(b"first");
    store.entry(key.clone()).or_insert(value);
    assert_eq!(store.get(key.as_ref()).unwrap().value(), b"first");

    let mut value = store.get_mut(key.as_ref()).expect("entry exists");
    value.set(SharedBytes::from_static(b"second"));
    assert_eq!(value.value(), Some(b"second".as_slice()));
    drop(value);

    assert_eq!(
        store.remove(key.as_ref()).expect("removed").as_ref(),
        b"second"
    );
    assert!(!store.contains_key(key.as_ref()));
}

#[test]
fn value_bytes_clone_outlives_shared_lock_and_updates() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    store.insert_slice(b"alpha", b"first");

    let value = store
        .get_value_bytes(b"alpha")
        .expect("stored bytes should clone");
    store.insert_slice(b"alpha", b"second");

    assert_eq!(value.as_ref(), b"first");
    assert_eq!(store.get(b"alpha").unwrap().value(), b"second");
}

#[test]
fn prepared_point_keys_read_and_write_shared_values() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    let prepared = store.prepare_point_key(b"prepared-key");

    store.insert_prepared_slice(&prepared, b"first");
    assert_eq!(
        store
            .get_prepared_ref(&prepared)
            .expect("prepared value")
            .value(),
        b"first"
    );

    store.insert_prepared_slice(&prepared, b"second");
    assert_eq!(
        store
            .get_prepared_value_bytes(&prepared)
            .expect("prepared bytes")
            .as_ref(),
        b"second"
    );
}

#[test]
fn point_writes_do_not_activate_semantic_generation() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());

    store.insert_slice(b"alpha", b"one");
    store.insert_slice(b"alpha", b"two");

    assert!(!store.inner.semantic_data_active.load(Ordering::Acquire));
    assert_eq!(store.semantic_generation(), 0);
}

#[test]
fn point_overwrite_invalidates_active_semantic_entry() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    let key = (0..1024usize)
        .map(|index| format!("semantic-key-{index}"))
        .find(|key| store.route_key(key.as_bytes()).shard_id != store.semantic_shard_id())
        .expect("key outside semantic shard");
    let embedding = [1.0, 0.0];

    store
        .insert_semantic_slice(key.as_bytes(), b"semantic", &embedding)
        .unwrap();
    assert!(store.semantic_search(&embedding, 0.99).unwrap().is_some());

    store.insert_slice(key.as_bytes(), b"plain");

    assert!(store.inner.semantic_data_active.load(Ordering::Acquire));
    assert!(store.semantic_search(&embedding, 0.99).unwrap().is_none());
}

#[cfg(feature = "no-ttl")]
#[test]
#[should_panic(expected = "shardcache/no-ttl builds do not support shared-store TTL writes")]
fn no_ttl_feature_rejects_shared_ttl_writes() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    store.insert_slice_with_ttl(b"ttl-key", b"value", Some(1000));
}

#[test]
fn fair_lock_policy_roundtrips() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig {
        lock_policy: SharedEmbeddedLockPolicy::Fair,
        ..SharedEmbeddedConfig::default()
    });
    store.insert_slice(b"fair-key", b"fair-value");
    assert_eq!(store.get(b"fair-key").unwrap().value(), b"fair-value");
    store.insert_slice(b"fair-key", b"updated");
    assert_eq!(store.get(b"fair-key").unwrap().value(), b"updated");
}

#[cfg(not(feature = "no-ttl"))]
#[test]
fn ttl_values_are_filtered_from_shared_reads() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    store.insert_slice_with_ttl(b"ttl-key", b"value", Some(25));
    assert_eq!(store.get(b"ttl-key").unwrap().value(), b"value");

    std::thread::sleep(std::time::Duration::from_millis(50));

    assert!(store.get(b"ttl-key").is_none());
    assert!(!store.contains_key(b"ttl-key"));
    assert!(store.get_mut(b"ttl-key").is_none());
}

#[cfg(not(feature = "no-ttl"))]
#[test]
fn shared_get_mut_preserves_existing_ttl() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    store.insert_slice_with_ttl(b"ttl-key", b"value", Some(5));

    store
        .get_mut(b"ttl-key")
        .expect("ttl-key exists")
        .set_slice(b"updated");
    assert_eq!(store.get(b"ttl-key").unwrap().value(), b"updated");

    std::thread::sleep(std::time::Duration::from_millis(15));
    assert!(store.get(b"ttl-key").is_none());
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn shared_get_mut_value_mut_no_ttl_updates_bytes_in_place() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    store.insert_slice(b"alpha", b"one");

    {
        let mut entry = store.get_mut(b"alpha").expect("alpha exists");
        let value = entry.value_mut_no_ttl().expect("unique no-TTL value");
        value.copy_from_slice(b"two");
    }

    assert_eq!(store.get(b"alpha").unwrap().value(), b"two");
}

#[cfg(all(feature = "mutable-value-slices", not(feature = "no-ttl")))]
#[test]
fn shared_get_mut_value_mut_no_ttl_rejects_ttl_values() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    store.insert_slice_with_ttl(b"ttl-key", b"value", Some(1000));

    let mut entry = store.get_mut(b"ttl-key").expect("ttl-key exists");
    assert!(entry.value_mut_no_ttl().is_none());
    entry.set_slice(b"after");
    drop(entry);

    assert_eq!(store.get(b"ttl-key").unwrap().value(), b"after");
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn shared_get_mut_value_mut_no_ttl_rejects_shared_value_bytes() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    let key = SharedBytes::from_static(b"alpha");
    let shared_value = SharedBytes::copy_from_slice(b"one");
    store.insert(key.clone(), shared_value.clone());

    {
        let mut entry = store.get_mut(key.as_ref()).expect("alpha exists");
        assert!(
            entry.value_mut_no_ttl().is_none(),
            "raw mutation must reject aliased bytes buffers"
        );
    }
    assert_eq!(store.get(key.as_ref()).unwrap().value(), b"one");

    drop(shared_value);
    {
        let mut entry = store.get_mut(key.as_ref()).expect("alpha exists");
        let value = entry.value_mut_no_ttl().expect("buffer is unique again");
        value.copy_from_slice(b"two");
    }
    assert_eq!(store.get(key.as_ref()).unwrap().value(), b"two");
}

#[cfg(all(feature = "mutable-value-slices", not(feature = "no-ttl")))]
#[test]
fn shared_get_mut_value_mut_no_ttl_does_not_resurrect_expired_guard() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    store.insert_slice_with_ttl(b"ttl-key", b"value", Some(25));

    let mut entry = store
        .get_mut(b"ttl-key")
        .expect("ttl-key exists before expiry");
    std::thread::sleep(std::time::Duration::from_millis(50));

    assert_eq!(entry.value(), None);
    assert!(entry.value_mut_no_ttl().is_none());
    entry.set_slice(b"after");
    drop(entry);

    assert!(store.get(b"ttl-key").is_none());
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn shared_get_mut_value_mut_no_ttl_rejects_missing_values() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());

    assert!(store.get_mut(b"missing").is_none());
}

#[test]
fn session_api_roundtrips() {
    let store = SharedEmbeddedStore::<8>::new(SharedEmbeddedConfig {
        route_mode: EmbeddedRouteMode::SessionPrefix,
        ..SharedEmbeddedConfig::default()
    });
    store.set_session(b"session-a", b"chunk-1", b"value-1");
    assert_eq!(
        store
            .get_session(b"session-a", b"chunk-1")
            .expect("session value")
            .value(),
        b"value-1"
    );
    assert!(store.get_session(b"session-b", b"chunk-1").is_none());
}

#[test]
fn per_stripe_memory_limit_evicts_independently() {
    let limit = 64usize;
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig {
        total_memory_bytes: Some(limit * 4),
        eviction_policy: EvictionPolicy::Lru,
        ..SharedEmbeddedConfig::default()
    });

    for shard_id in 0..4usize {
        for index in 0..64usize {
            let key = format!("stripe-{shard_id}-{index}");
            let route = store.route_key(key.as_bytes());
            if route.shard_id == shard_id {
                store.insert_slice(key.as_bytes(), b"0123456789abcdef");
            }
        }
    }

    for shard in &store.inner.shards {
        assert!(shard.read().stored_bytes() <= limit);
    }
}

#[test]
fn semantic_shard_uses_total_memory_budget() {
    let per_stripe_limit = 64usize;
    let total_limit = per_stripe_limit * 4;
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig {
        total_memory_bytes: Some(total_limit),
        eviction_policy: EvictionPolicy::Lru,
        ..SharedEmbeddedConfig::default()
    });

    let mut inserted = 0usize;
    for index in 0..4096usize {
        let key = format!("semantic-budget-{index}");
        if store.route_key(key.as_bytes()).shard_id != 0 {
            continue;
        }
        let embedding = [1.0, index as f32 + 1.0];
        store
            .insert_semantic_slice(key.as_bytes(), b"0123456789abcdef", &embedding)
            .unwrap();
        inserted += 1;
        if inserted >= 8 {
            break;
        }
    }
    assert_eq!(inserted, 8);

    let semantic_bytes = store.stripe(0).read().stored_bytes();
    assert!(
        semantic_bytes > per_stripe_limit,
        "semantic shard should exceed one per-stripe slice of the total budget"
    );
    assert!(
        semantic_bytes <= total_limit,
        "semantic shard should still obey the total semantic budget"
    );
}
