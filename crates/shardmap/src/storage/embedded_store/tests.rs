use super::{EmbeddedRouteMode, EmbeddedStore, PackedSessionWrite, ShardArcEmbeddedStore};
use crate::config::EvictionPolicy;
#[cfg(feature = "telemetry")]
use crate::storage::CacheTelemetry;
#[cfg(feature = "redis")]
use crate::storage::RedisZSetStore;
use crate::storage::hash_key;
#[cfg(feature = "redis")]
use crate::storage::{RedisObjectResult, RedisStringLookup};
use std::collections::BTreeMap;
#[cfg(feature = "telemetry")]
use std::sync::Arc;

#[test]
fn shard_arc_store_routes_and_serves_blob_reads() {
    let store = ShardArcEmbeddedStore::new(4);
    let key = b"shard-arc-key";
    let route = store.route_key(key);
    store.set_slice_routed_no_ttl(route, key, b"value");

    assert!(store.contains_routed_no_ttl(route, key));

    let mut out = bytes::BytesMut::new();
    assert!(store.get_blob_string_hashed_into(hash_key(key), key, &mut out));
    assert_eq!(out.as_ref(), b"$5\r\nvalue\r\n");
}

#[test]
fn visit_string_keys_and_entries_do_not_require_snapshots() {
    let store = EmbeddedStore::new(4);
    store.set(b"a".to_vec(), b"one".to_vec(), None);
    store.set(b"b".to_vec(), b"two".to_vec(), Some(60_000));

    let mut keys = Vec::new();
    store.visit_string_keys(|key| {
        keys.push(key.to_vec());
        true
    });
    keys.sort();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

    let mut entries = BTreeMap::new();
    store.visit_string_entries(|key, value, expire_at_ms| {
        entries.insert(key.to_vec(), (value.to_vec(), expire_at_ms.is_some()));
        true
    });
    assert_eq!(
        entries.get(b"a".as_slice()),
        Some(&(b"one".to_vec(), false))
    );
    assert_eq!(entries.get(b"b".as_slice()), Some(&(b"two".to_vec(), true)));

    let mut first_key_only = Vec::new();
    store.visit_string_keys(|key| {
        first_key_only.push(key.to_vec());
        false
    });
    assert_eq!(first_key_only.len(), 1);
}

#[cfg(feature = "redis")]
#[test]
fn redis_objects_enforce_wrongtype_and_set_overwrite() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        store.hset(b"user:1", b"name", b"ada"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.hget(b"user:1", b"name"),
        RedisObjectResult::Bulk(Some(b"ada".to_vec()))
    );
    assert!(store.exists(b"user:1"));

    let mut encoded = Vec::new();
    assert_eq!(
        store.get_string_value_into(b"user:1", |value| encoded.extend_from_slice(value)),
        RedisStringLookup::WrongType
    );

    store.set(b"user:1".to_vec(), b"string".to_vec(), None);
    assert_eq!(store.hget(b"user:1", b"name"), RedisObjectResult::WrongType);
    assert_eq!(store.get(b"user:1"), Some(b"string".to_vec()));

    assert!(store.delete(b"user:1"));
    assert_eq!(store.get(b"user:1"), None);
    assert!(!store.exists(b"user:1"));
}

#[cfg(feature = "redis")]
#[test]
fn redis_list_set_and_zset_semantics() {
    let store = EmbeddedStore::new(4);

    assert_eq!(
        store.lpush(b"list", &[b"a".as_slice(), b"b".as_slice()]),
        RedisObjectResult::Integer(2)
    );
    assert_eq!(
        store.rpush(b"list", &[b"c".as_slice()]),
        RedisObjectResult::Integer(3)
    );
    assert_eq!(
        store.lrange(b"list", 0, -1),
        RedisObjectResult::Array(vec![
            Some(b"b".to_vec()),
            Some(b"a".to_vec()),
            Some(b"c".to_vec())
        ])
    );
    assert_eq!(
        store.lpop(b"list"),
        RedisObjectResult::Bulk(Some(b"b".to_vec()))
    );

    assert_eq!(
        store.sadd(b"set", &[b"x".as_slice(), b"x".as_slice(), b"y".as_slice()]),
        RedisObjectResult::Integer(2)
    );
    assert_eq!(store.sismember(b"set", b"x"), RedisObjectResult::Integer(1));
    assert_eq!(
        store.srem(b"set", &[b"x".as_slice()]),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(store.scard(b"set"), RedisObjectResult::Integer(1));

    assert_eq!(store.zadd(b"z", 2.0, b"b"), RedisObjectResult::Integer(1));
    assert_eq!(store.zadd(b"z", 1.0, b"a"), RedisObjectResult::Integer(1));
    assert_eq!(
        store.zrange(b"z", 0, -1),
        RedisObjectResult::Array(vec![Some(b"a".to_vec()), Some(b"b".to_vec())])
    );
    assert_eq!(
        store.zscore(b"z", b"b"),
        RedisObjectResult::Bulk(Some(b"2".to_vec()))
    );

    for (score, member) in [
        (1.0, b"c".as_slice()),
        (1.0, b"a".as_slice()),
        (1.0, b"b".as_slice()),
        (2.0, b"e".as_slice()),
        (0.0, b"d".as_slice()),
    ] {
        assert_eq!(
            store.zadd(b"z-indexed", score, member),
            RedisObjectResult::Integer(1)
        );
    }
    assert_eq!(
        store.zrange(b"z-indexed", 0, -1),
        RedisObjectResult::Array(vec![
            Some(b"d".to_vec()),
            Some(b"a".to_vec()),
            Some(b"b".to_vec()),
            Some(b"c".to_vec()),
            Some(b"e".to_vec())
        ])
    );
    assert_eq!(
        store.zrank(b"z-indexed", b"b", false),
        RedisObjectResult::Integer(2)
    );
    assert_eq!(
        store.zrank(b"z-indexed", b"b", true),
        RedisObjectResult::Integer(2)
    );
    assert_eq!(
        store
            .zcount_range(b"z-indexed", 1.0, true, 2.0, true)
            .unwrap(),
        4
    );
    assert_eq!(
        store
            .zcount_range(b"z-indexed", 1.0, false, 2.0, true)
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .zcount_range(b"z-indexed", 1.0, true, 2.0, false)
            .unwrap(),
        3
    );
    assert_eq!(
        store.zadd(b"z-indexed", 3.0, b"d"),
        RedisObjectResult::Integer(0)
    );
    assert_eq!(
        store.zrange(b"z-indexed", 0, -1),
        RedisObjectResult::Array(vec![
            Some(b"a".to_vec()),
            Some(b"b".to_vec()),
            Some(b"c".to_vec()),
            Some(b"e".to_vec()),
            Some(b"d".to_vec())
        ])
    );
    assert_eq!(
        store.zrank(b"z-indexed", b"d", false),
        RedisObjectResult::Integer(4)
    );
    assert_eq!(
        store.zrank(b"z-indexed", b"d", true),
        RedisObjectResult::Integer(0)
    );
}

#[cfg(feature = "redis")]
#[test]
fn redis_segmented_list_semantics_cross_inline_boundary() {
    let store = EmbeddedStore::new(4);
    let values = (0..20)
        .map(|index| format!("v{index:02}").into_bytes())
        .collect::<Vec<_>>();
    let slices = values.iter().map(Vec::as_slice).collect::<Vec<_>>();

    assert_eq!(
        store.rpush(b"seg-list", &slices),
        RedisObjectResult::Integer(20)
    );
    assert_eq!(
        store.lindex(b"seg-list", 0),
        RedisObjectResult::Bulk(Some(b"v00".to_vec()))
    );
    assert_eq!(
        store.lindex(b"seg-list", -1),
        RedisObjectResult::Bulk(Some(b"v19".to_vec()))
    );
    assert_eq!(
        store.lrange(b"seg-list", 8, 11),
        RedisObjectResult::Array(vec![
            Some(b"v08".to_vec()),
            Some(b"v09".to_vec()),
            Some(b"v10".to_vec()),
            Some(b"v11".to_vec()),
        ])
    );

    assert_eq!(
        store.lrem(b"seg-list", 0, b"v10"),
        RedisObjectResult::Integer(1)
    );
    assert_eq!(
        store.lindex(b"seg-list", 10),
        RedisObjectResult::Bulk(Some(b"v11".to_vec()))
    );
    assert_eq!(
        store.ltrim(b"seg-list", 3, 12),
        RedisObjectResult::Simple("OK")
    );
    assert_eq!(store.llen(b"seg-list"), RedisObjectResult::Integer(10));
    assert_eq!(
        store.lpop(b"seg-list"),
        RedisObjectResult::Bulk(Some(b"v03".to_vec()))
    );
    assert_eq!(
        store.rpop(b"seg-list"),
        RedisObjectResult::Bulk(Some(b"v13".to_vec()))
    );
}

#[test]
fn batch_get_preserves_input_order() {
    let store = EmbeddedStore::new(4);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);
    store.set(b"beta".to_vec(), b"two".to_vec(), None);
    store.set(b"gamma".to_vec(), b"three".to_vec(), None);

    let values = store.batch_get(vec![
        b"gamma".to_vec(),
        b"missing".to_vec(),
        b"alpha".to_vec(),
        b"beta".to_vec(),
    ]);

    assert_eq!(
        values,
        vec![
            Some(b"three".to_vec()),
            None,
            Some(b"one".to_vec()),
            Some(b"two".to_vec())
        ]
    );
}

#[test]
fn ttl_expires_with_lazy_reads() {
    let store = EmbeddedStore::new(2);
    store.set(b"alpha".to_vec(), b"one".to_vec(), Some(1));
    std::thread::sleep(std::time::Duration::from_millis(5));

    assert_eq!(store.get(b"alpha"), None);
    assert!(!store.exists(b"alpha"));
}

#[test]
fn get_mut_replaces_and_removes_point_values() {
    let store = EmbeddedStore::new(2);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);

    {
        let mut entry = store.get_mut(b"alpha").expect("alpha exists");
        assert_eq!(entry.value(), Some(b"one".as_slice()));
        entry.set_slice(b"two");
        assert_eq!(entry.value(), Some(b"two".as_slice()));
    }
    assert_eq!(store.get(b"alpha"), Some(b"two".to_vec()));

    let removed = store.get_mut(b"alpha").expect("alpha exists").remove();
    assert_eq!(removed.as_deref(), Some(b"two".as_slice()));
    assert_eq!(store.get(b"alpha"), None);
}

#[test]
fn get_mut_preserves_existing_ttl() {
    let store = EmbeddedStore::new(2);
    store.set(b"alpha".to_vec(), b"one".to_vec(), Some(5));

    store
        .get_mut(b"alpha")
        .expect("alpha exists")
        .set_slice(b"two");
    assert_eq!(store.get(b"alpha"), Some(b"two".to_vec()));

    std::thread::sleep(std::time::Duration::from_millis(15));
    assert_eq!(store.get(b"alpha"), None);
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn get_mut_value_mut_no_ttl_updates_bytes_in_place() {
    let store = EmbeddedStore::new(2);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);

    {
        let mut entry = store.get_mut(b"alpha").expect("alpha exists");
        let value = entry.value_mut_no_ttl().expect("unique no-TTL value");
        value.copy_from_slice(b"two");
    }

    assert_eq!(store.get(b"alpha"), Some(b"two".to_vec()));
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn get_mut_value_mut_no_ttl_rejects_ttl_values() {
    let store = EmbeddedStore::new(2);
    store.set(b"alpha".to_vec(), b"one".to_vec(), Some(1000));

    let mut entry = store.get_mut(b"alpha").expect("alpha exists");
    assert!(entry.value_mut_no_ttl().is_none());
    entry.set_slice(b"two");
    drop(entry);

    assert_eq!(store.get(b"alpha"), Some(b"two".to_vec()));
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn get_mut_value_mut_no_ttl_rejects_shared_value_bytes() {
    let store = EmbeddedStore::new(2);
    let shared_value = bytes::Bytes::copy_from_slice(b"one");
    store.set_value_bytes(b"alpha", shared_value.clone(), None);

    {
        let mut entry = store.get_mut(b"alpha").expect("alpha exists");
        assert!(
            entry.value_mut_no_ttl().is_none(),
            "raw mutation must reject aliased bytes buffers"
        );
    }
    assert_eq!(store.get(b"alpha"), Some(b"one".to_vec()));

    drop(shared_value);
    {
        let mut entry = store.get_mut(b"alpha").expect("alpha exists");
        let value = entry.value_mut_no_ttl().expect("buffer is unique again");
        value.copy_from_slice(b"two");
    }
    assert_eq!(store.get(b"alpha"), Some(b"two".to_vec()));
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn get_mut_value_mut_no_ttl_does_not_resurrect_expired_guard() {
    let store = EmbeddedStore::new(2);
    store.set(b"alpha".to_vec(), b"one".to_vec(), Some(25));

    let mut entry = store.get_mut(b"alpha").expect("alpha exists before expiry");
    std::thread::sleep(std::time::Duration::from_millis(50));

    assert_eq!(entry.value(), None);
    assert!(entry.value_mut_no_ttl().is_none());
    entry.set_slice(b"two");
    drop(entry);

    assert_eq!(store.get(b"alpha"), None);
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn get_mut_value_mut_no_ttl_rejects_missing_values() {
    let store = EmbeddedStore::new(2);

    assert!(store.get_mut(b"missing").is_none());
}

#[test]
fn session_prefix_routing_colocates_chunks() {
    let store = EmbeddedStore::with_route_mode(8, EmbeddedRouteMode::SessionPrefix);

    assert_eq!(
        store.shard_for_key(b"s:42:c:0"),
        store.shard_for_key(b"s:42:c:7")
    );
    assert_eq!(
        store.shard_for_key(b"s:7:c:1"),
        store.shard_for_key(b"s:7:c:99")
    );
    assert_eq!(
        store.route_session(b"s:42").shard_id,
        store.route_key(b"s:42:c:0").shard_id
    );
}

#[test]
fn session_view_survives_update_and_delete() {
    let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
    store.set(b"s:9:c:0".to_vec(), b"alpha".to_vec(), None);
    store.set(b"s:9:c:1".to_vec(), b"beta".to_vec(), None);

    let keys = vec![b"s:9:c:0".to_vec(), b"s:9:c:1".to_vec()];
    let view = store.batch_get_session_view(b"s:9", &keys);
    assert_eq!(view.slice(0), Some(&b"alpha"[..]));
    assert_eq!(view.slice(1), Some(&b"beta"[..]));

    store.set(b"s:9:c:0".to_vec(), b"gamma".to_vec(), None);
    assert!(store.delete(b"s:9:c:1"));

    assert_eq!(view.slice(0), Some(&b"alpha"[..]));
    assert_eq!(view.slice(1), Some(&b"beta"[..]));
    drop(view);

    assert_eq!(store.get(b"s:9:c:0"), Some(b"gamma".to_vec()));
    assert_eq!(store.get(b"s:9:c:1"), None);
}

#[test]
fn single_view_survives_update_and_delete() {
    let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::FullKey);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);

    let view = store.get_view(b"alpha");
    assert_eq!(view.slice(), Some(&b"one"[..]));

    store.set(b"alpha".to_vec(), b"two".to_vec(), None);
    assert!(store.delete(b"alpha"));

    assert_eq!(view.slice(), Some(&b"one"[..]));
    drop(view);
}

#[test]
fn read_slice_meta_survives_view_and_store_drop() {
    let slice = {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::FullKey);
        store.set(b"alpha".to_vec(), b"one".to_vec(), None);
        let view = store.get_view(b"alpha");
        view.slice_meta().expect("alpha should exist")
    };

    assert_eq!(slice.as_slice(), b"one");
}

#[test]
fn batch_slice_meta_survives_view_and_store_drop() {
    let slice = {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::FullKey);
        store.set(b"alpha".to_vec(), b"one".to_vec(), None);
        let view = store.batch_get_view(&[b"alpha".to_vec()]);
        view.slice_meta(0).expect("alpha should exist")
    };

    assert_eq!(slice.as_slice(), b"one");
}

#[test]
fn batch_view_preserves_order_across_shards() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);
    store.set(b"beta".to_vec(), b"two".to_vec(), None);

    let view = store.batch_get_view(&[b"beta".to_vec(), b"missing".to_vec(), b"alpha".to_vec()]);
    assert_eq!(view.slice(0), Some(&b"two"[..]));
    assert_eq!(view.slice(1), None);
    assert_eq!(view.slice(2), Some(&b"one"[..]));
}

#[test]
fn routed_session_view_matches_prefix_view() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::SessionPrefix);
    store.batch_set_session_slices_no_ttl(
        b"s:9",
        [
            (&b"s:9:c:0"[..], &b"alpha"[..]),
            (&b"s:9:c:1"[..], &b"beta"[..]),
        ],
    );

    let keys = vec![b"s:9:c:0".to_vec(), b"s:9:c:1".to_vec()];
    let routed = store.batch_get_session_view_routed(store.route_session(b"s:9"), &keys);
    let plain = store.batch_get_session_view(b"s:9", &keys);

    assert_eq!(routed.slice(0), plain.slice(0));
    assert_eq!(routed.slice(1), plain.slice(1));
}

#[test]
fn packed_session_publish_swaps_generation_without_invalidating_readers() {
    let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
    store.batch_set_session_packed_no_ttl(PackedSessionWrite::from_owned_items(
        b"s:5".to_vec(),
        vec![
            (b"s:5:c:0".to_vec(), b"alpha".to_vec()),
            (b"s:5:c:1".to_vec(), b"beta".to_vec()),
        ],
    ));

    let keys = vec![b"s:5:c:0".to_vec(), b"s:5:c:1".to_vec()];
    let view = store.batch_get_session_view(b"s:5", &keys);
    assert_eq!(view.slice(0), Some(&b"alpha"[..]));
    assert_eq!(view.slice(1), Some(&b"beta"[..]));

    store.batch_set_session_packed_no_ttl(PackedSessionWrite::from_owned_items(
        b"s:5".to_vec(),
        vec![(b"s:5:c:0".to_vec(), b"gamma".to_vec())],
    ));

    assert_eq!(view.slice(0), Some(&b"alpha"[..]));
    assert_eq!(view.slice(1), Some(&b"beta"[..]));
    drop(view);

    assert_eq!(store.get(b"s:5:c:0"), Some(b"gamma".to_vec()));
    assert_eq!(store.get(b"s:5:c:1"), None);
}

#[test]
fn point_write_overrides_explicit_session_slot_entry() {
    let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
    store.batch_set_session_slices_no_ttl(b"s:5", [(b"s:5:c:0".as_slice(), b"alpha".as_slice())]);

    store.set(b"s:5:c:0".to_vec(), b"gamma".to_vec(), None);

    assert_eq!(store.get(b"s:5:c:0"), Some(b"gamma".to_vec()));
    let batch = store.batch_get_session_view(b"s:5", &[b"s:5:c:0".to_vec()]);
    assert_eq!(batch.slice(0), Some(&b"gamma"[..]));
}

#[test]
fn owned_workers_cover_every_shard_once() {
    let store = EmbeddedStore::with_route_mode(8, EmbeddedRouteMode::SessionPrefix);
    let workers = store.into_owned_workers(3);
    let mut seen = Vec::new();

    for worker in &workers {
        for shard_id in 0..8 {
            if worker.owns_shard(shard_id) {
                seen.push(shard_id);
            }
        }
    }

    seen.sort_unstable();
    assert_eq!(seen, (0..8).collect::<Vec<_>>());
}

#[test]
fn owned_worker_point_view_reads_from_owned_shard() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);
    store.set(b"beta".to_vec(), b"two".to_vec(), None);

    let mut workers = store.into_owned_workers(2);
    let route = workers[0].route_key(b"alpha");
    let worker = workers
        .iter_mut()
        .find(|worker| worker.owns_shard(route.shard_id))
        .expect("worker should own routed shard");

    let view = worker.get_view_routed_no_ttl(route, b"alpha");
    assert!(view.is_hit());
    assert_eq!(view.slice(), Some(&b"one"[..]));
    assert_eq!(view.len(), 3);
}

#[test]
fn owned_worker_get_mut_replaces_point_value() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);

    let mut workers = store.into_owned_workers(2);
    let route = workers[0].route_key(b"alpha");
    let worker = workers
        .iter_mut()
        .find(|worker| worker.owns_shard(route.shard_id))
        .expect("worker should own routed shard");

    {
        let mut entry = worker.get_mut(b"alpha").expect("alpha exists");
        assert_eq!(entry.value(), Some(b"one".as_slice()));
        entry.set_slice(b"two");
        assert_eq!(entry.value(), Some(b"two".as_slice()));
    }
    assert_eq!(worker.get(b"alpha"), Some(b"two".to_vec()));
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn owned_worker_get_mut_value_mut_no_ttl_updates_bytes_in_place() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);

    let mut workers = store.into_owned_workers(2);
    let route = workers[0].route_key(b"alpha");
    let worker = workers
        .iter_mut()
        .find(|worker| worker.owns_shard(route.shard_id))
        .expect("worker should own routed shard");

    {
        let mut entry = worker.get_mut(b"alpha").expect("alpha exists");
        let value = entry.value_mut_no_ttl().expect("unique no-TTL value");
        value.copy_from_slice(b"two");
    }

    assert_eq!(worker.get(b"alpha"), Some(b"two".to_vec()));
}

#[cfg(feature = "mutable-value-slices")]
#[test]
fn owned_worker_get_mut_value_mut_no_ttl_rejects_shared_value_bytes() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    store.set(b"alpha".to_vec(), b"seed".to_vec(), None);

    let mut workers = store.into_owned_workers(2);
    let route = workers[0].route_key(b"alpha");
    let worker = workers
        .iter_mut()
        .find(|worker| worker.owns_shard(route.shard_id))
        .expect("worker should own routed shard");
    let shared_value = bytes::Bytes::copy_from_slice(b"one!");

    {
        let mut entry = worker.get_mut(b"alpha").expect("alpha exists");
        entry.set(shared_value.clone());
    }
    {
        let mut entry = worker.get_mut(b"alpha").expect("alpha exists");
        assert!(
            entry.value_mut_no_ttl().is_none(),
            "raw mutation must reject aliased bytes buffers"
        );
    }
    assert_eq!(worker.get(b"alpha"), Some(b"one!".to_vec()));

    drop(shared_value);
    {
        let mut entry = worker.get_mut(b"alpha").expect("alpha exists");
        let value = entry.value_mut_no_ttl().expect("buffer is unique again");
        value.copy_from_slice(b"two!");
    }
    assert_eq!(worker.get(b"alpha"), Some(b"two!".to_vec()));
}

#[test]
fn prepared_point_key_view_reads_value() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);

    let prepared = store.prepare_point_key(b"alpha");
    let view = store.get_prepared_view_no_ttl(&prepared);

    assert!(view.is_hit());
    assert_eq!(view.slice(), Some(&b"one"[..]));
}

#[test]
fn owned_worker_prepared_point_key_reads_value() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    store.set(b"alpha".to_vec(), b"one".to_vec(), None);

    let mut workers = store.into_owned_workers(2);
    let prepared = workers[0].prepare_point_key(b"alpha");
    let worker = workers
        .iter_mut()
        .find(|worker| worker.owns_shard(prepared.route().shard_id))
        .expect("worker should own routed shard");

    let value = worker.get_prepared_ref_no_ttl(&prepared);
    assert_eq!(value, Some(&b"one"[..]));
}

#[test]
fn owned_worker_prehashed_session_view_matches_routed_view() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::SessionPrefix);
    store.batch_set_session_slices_no_ttl(
        b"s:7",
        [
            (&b"s:7:c:0"[..], &b"alpha"[..]),
            (&b"s:7:c:1"[..], &b"beta"[..]),
        ],
    );

    let mut workers = store.into_owned_workers(2);
    let route = workers[0].route_session(b"s:7");
    let worker = workers
        .iter_mut()
        .find(|worker| worker.owns_shard(route.shard_id))
        .expect("worker should own routed shard");

    let keys = vec![
        b"s:7:c:0".to_vec(),
        b"s:7:c:missing".to_vec(),
        b"s:7:c:1".to_vec(),
    ];
    let plain = worker.batch_get_session_view_routed_no_ttl(route, &keys);
    let plain_values = (0..plain.item_count())
        .map(|index| plain.slice(index).map(|value| value.to_vec()))
        .collect::<Vec<_>>();
    let plain_hit_count = plain.hit_count();
    let plain_total_bytes = plain.total_bytes();
    drop(plain);

    let key_hashes = keys.iter().map(|key| hash_key(key)).collect::<Vec<_>>();
    let prehashed =
        worker.batch_get_session_view_prehashed_routed_no_ttl(route, &keys, &key_hashes);

    assert_eq!(prehashed.hit_count(), plain_hit_count);
    assert_eq!(prehashed.total_bytes(), plain_total_bytes);
    assert_eq!(prehashed.item_count(), plain_values.len());
    for (index, expected) in plain_values.iter().enumerate() {
        assert_eq!(
            prehashed.slice(index).map(|value| value.to_vec()),
            *expected,
            "mismatch at index {index}"
        );
    }
}

#[test]
fn owned_worker_session_packed_matches_batch_view() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::SessionPrefix);
    store.batch_set_session_slices_no_ttl(
        b"s:4",
        [
            (&b"s:4:c:0"[..], &b"left"[..]),
            (&b"s:4:c:1"[..], &b"right"[..]),
        ],
    );

    let mut workers = store.into_owned_workers(2);
    let route = workers[0].route_session(b"s:4");
    let worker = workers
        .iter_mut()
        .find(|worker| worker.owns_shard(route.shard_id))
        .expect("worker should own routed shard");

    let keys = vec![
        b"s:4:c:0".to_vec(),
        b"s:4:c:missing".to_vec(),
        b"s:4:c:1".to_vec(),
    ];
    let view = worker.batch_get_session_view_routed_no_ttl(route, &keys);
    let expected = (0..view.item_count())
        .map(|index| view.slice(index).map(|value| value.to_vec()))
        .collect::<Vec<_>>();
    drop(view);

    let packed = worker.batch_get_session_packed_routed_no_ttl(route, &keys);
    let unpacked_values = packed
        .offsets
        .iter()
        .zip(packed.lengths.iter())
        .map(|(&offset, &length)| {
            if offset == usize::MAX {
                None
            } else {
                Some(packed.buffer[offset..offset + length].to_vec())
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(unpacked_values, expected);
}

#[test]
fn memory_cap_lru_evicts_colder_generic_entry_before_session_slot() {
    let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
    store.configure_memory_policy(Some(8), EvictionPolicy::Lru);

    store.set(b"a".to_vec(), b"1".to_vec(), None);
    assert_eq!(store.stored_bytes(), 2);

    store.batch_set_session_slices_no_ttl(b"s:1", [(&b"s:1:c:0"[..], &b"x"[..])]);

    assert_eq!(store.get(b"a"), None);
    assert_eq!(store.get(b"s:1:c:0"), Some(b"x".to_vec()));
    assert_eq!(store.len(), 1);
    assert!(store.stored_bytes() <= 8);
}

#[cfg(feature = "redis")]
#[test]
fn vector_shard_can_use_total_memory_budget() {
    let store = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let per_shard_limit = 64;
    let total_limit = 1024;
    store.configure_memory_policy(Some(per_shard_limit), EvictionPolicy::Lru);
    store.configure_vector_memory_policy(Some(total_limit), EvictionPolicy::Lru);

    let key = (0..10_000)
        .map(|index| format!("vector-budget-{index}").into_bytes())
        .find(|candidate| store.route_key(candidate).shard_id != store.vector_shard_id())
        .expect("key routed away from vector shard");
    let route = store.route_vector_key(&key);
    let mut value = crate::storage::VECTOR_SET_PREFIX.to_vec();
    value.resize(512, b'v');

    store.set_value_bytes_routed_no_ttl_then(route, &key, bytes::Bytes::from(value), || {});

    let mut stored_len = 0;
    assert!(
        store.with_shared_value_bytes_routed(route, &key, &mut |bytes| {
            stored_len = bytes.len();
        })
    );
    assert!(stored_len > per_shard_limit);
    assert!(stored_len < total_limit);
}

#[test]
fn memory_cap_lfu_preserves_hot_generic_entry_over_session_slot() {
    let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
    store.configure_memory_policy(Some(8), EvictionPolicy::Lfu);

    store.set(b"a".to_vec(), b"1".to_vec(), None);
    store.set(b"a".to_vec(), b"1".to_vec(), None);
    store.set(b"a".to_vec(), b"1".to_vec(), None);

    store.batch_set_session_slices_no_ttl(b"s:1", [(&b"s:1:c:0"[..], &b"x"[..])]);

    assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
    assert_eq!(store.get(b"s:1:c:0"), None);
    assert_eq!(store.len(), 1);
    assert!(store.stored_bytes() <= 8);
}

#[cfg(feature = "prefix-eviction")]
#[test]
fn memory_cap_prefix_eviction_drops_cold_session_group() {
    let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
    store.configure_memory_policy(Some(12), EvictionPolicy::Prefix);

    store.batch_set_session_slices_no_ttl(b"s:cold", [(&b"s:cold:c:0"[..], &b"x"[..])]);
    store.batch_set_session_slices_no_ttl(b"s:hot", [(&b"s:hot:c:0"[..], &b"y"[..])]);

    assert_eq!(store.get(b"s:cold:c:0"), None);
    assert_eq!(store.get(b"s:hot:c:0"), Some(b"y".to_vec()));
    assert_eq!(store.len(), 1);
    assert!(store.stored_bytes() <= 12);
}

#[cfg(feature = "telemetry")]
#[test]
fn telemetry_snapshot_tracks_basic_ops() {
    let metrics = CacheTelemetry::new(2);
    let store = EmbeddedStore::with_route_mode_and_metrics(
        2,
        EmbeddedRouteMode::SessionPrefix,
        Some(Arc::clone(&metrics)),
    );

    store.set(b"s:1:c:0".to_vec(), b"alpha".to_vec(), None);
    assert_eq!(store.get(b"s:1:c:0"), Some(b"alpha".to_vec()));
    assert_eq!(store.get(b"s:1:c:9"), None);
    let _ = store.batch_get_session_packed(b"s:1", &[b"s:1:c:0".to_vec()]);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.sets, 1);
    assert_eq!(snapshot.gets, 3);
    assert_eq!(snapshot.batch_gets, 1);
    assert_eq!(snapshot.hits, 2);
    assert_eq!(snapshot.misses, 1);
    assert_eq!(snapshot.keys_total, 1);
}
