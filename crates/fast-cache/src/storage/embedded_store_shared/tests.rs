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

#[test]
fn ttl_values_are_filtered_from_shared_reads() {
    let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
    store.insert_slice_with_ttl(b"ttl-key", b"value", Some(2));
    assert_eq!(store.get(b"ttl-key").unwrap().value(), b"value");

    std::thread::sleep(std::time::Duration::from_millis(5));

    assert!(store.get(b"ttl-key").is_none());
    assert!(!store.contains_key(b"ttl-key"));
    assert!(store.get_mut(b"ttl-key").is_none());
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
