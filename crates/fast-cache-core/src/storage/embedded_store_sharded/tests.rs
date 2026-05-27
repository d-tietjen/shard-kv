use std::ops::ControlFlow;

use super::{
    LocalRouteError, WorkerLocalEmbeddedStore, WorkerLocalEmbeddedStoreBootstrap,
    take_thread_local_embedded_store, with_thread_local_embedded_store,
};
use crate::cuda::{CudaChunkTransferDescriptor, CudaSessionChunkEvent, CudaSessionTransferRequest};
use crate::storage::{EmbeddedRouteMode, EmbeddedStore};

fn find_key(store: &WorkerLocalEmbeddedStore, want_local: bool) -> Vec<u8> {
    for index in 0..10_000usize {
        let key = format!("k-{index}").into_bytes();
        if store.key_is_local(&key) == want_local {
            return key;
        }
    }
    panic!("unable to find a test key for locality={want_local}");
}

fn find_session(store: &WorkerLocalEmbeddedStore, want_local: bool) -> Vec<u8> {
    for index in 0..10_000usize {
        let session = format!("s:{index}").into_bytes();
        if store.session_is_local(&session) == want_local {
            return session;
        }
    }
    panic!("unable to find a test session for locality={want_local}");
}

#[test]
fn worker_local_store_round_trip() {
    let store = EmbeddedStore::with_route_mode(2, EmbeddedRouteMode::SessionPrefix);
    let bootstrap = WorkerLocalEmbeddedStoreBootstrap::from_embedded(store, 2);
    let mut stores = bootstrap.into_stores();
    let local = stores.pop().expect("expected one local store");
    let local_key = find_key(&local, true);
    local
        .install_local()
        .expect("install local worker-local embedded store");

    with_thread_local_embedded_store(|store| {
        store.set(local_key.clone(), b"alpha".to_vec(), None);
        assert_eq!(store.get(&local_key), Some(b"alpha".to_vec()));
    })
    .expect("worker-local embedded store should be installed");

    let recovered = take_thread_local_embedded_store();
    assert!(recovered.is_some());
}

#[test]
fn worker_local_store_rejects_non_local_keys() {
    let store = EmbeddedStore::with_route_mode(2, EmbeddedRouteMode::FullKey);
    let bootstrap = WorkerLocalEmbeddedStoreBootstrap::from_embedded(store, 2);
    let mut stores = bootstrap.into_stores();
    let mut local = stores.pop().expect("expected one local store");

    let local_key = find_key(&local, true);
    let remote_key = find_key(&local, false);

    local
        .set_if_local(local_key.clone(), b"alpha".to_vec(), None)
        .expect("local key should be accepted");
    assert_eq!(
        local
            .get_if_local(&local_key)
            .expect("local read should work"),
        Some(b"alpha".to_vec())
    );
    assert!(matches!(
        local.get_if_local(&remote_key),
        Err(LocalRouteError::KeyNotLocal { .. })
    ));
}

#[test]
fn worker_local_zero_copy_view_is_local_only() {
    let store = EmbeddedStore::with_route_mode(2, EmbeddedRouteMode::FullKey);
    let bootstrap = WorkerLocalEmbeddedStoreBootstrap::from_embedded(store, 2);
    let mut stores = bootstrap.into_stores();
    let mut local = stores.pop().expect("expected one local store");

    let local_key = find_key(&local, true);
    local
        .set_if_local(local_key.clone(), b"view-bytes".to_vec(), None)
        .expect("local key should be accepted");

    let view = local
        .get_view_if_local(&local_key)
        .expect("view should stay on the owning thread");
    assert_eq!(view.slice(), Some(b"view-bytes".as_slice()));
    assert!(view.is_hit());
}

#[test]
fn worker_local_get_mut_is_local_only() {
    let store = EmbeddedStore::with_route_mode(2, EmbeddedRouteMode::FullKey);
    let bootstrap = WorkerLocalEmbeddedStoreBootstrap::from_embedded(store, 2);
    let mut stores = bootstrap.into_stores();
    let mut local = stores.pop().expect("expected one local store");

    let local_key = find_key(&local, true);
    let remote_key = find_key(&local, false);
    local
        .set_if_local(local_key.clone(), b"one".to_vec(), None)
        .expect("local key should be accepted");

    {
        let mut entry = local
            .get_mut_if_local(&local_key)
            .expect("local get_mut should route")
            .expect("local key should exist");
        assert_eq!(entry.value(), Some(b"one".as_slice()));
        entry.set_slice(b"two");
    }

    assert_eq!(local.get(&local_key), Some(b"two".to_vec()));
    assert!(matches!(
        local.get_mut_if_local(&remote_key),
        Err(LocalRouteError::KeyNotLocal { .. })
    ));
}

#[test]
fn worker_local_prepared_point_key_is_exact_and_local() {
    let store = EmbeddedStore::with_route_mode(2, EmbeddedRouteMode::FullKey);
    let bootstrap = WorkerLocalEmbeddedStoreBootstrap::from_embedded(store, 2);
    let mut stores = bootstrap.into_stores();
    let mut local = stores.pop().expect("expected one local store");

    let local_key = find_key(&local, true);
    let remote_key = find_key(&local, false);
    local
        .set_if_local(local_key.clone(), b"prepared-bytes".to_vec(), None)
        .expect("local key should be accepted");

    let prepared = local
        .prepare_point_key_if_local(&local_key)
        .expect("prepared key should stay on the owning thread");
    assert_eq!(
        local.get_prepared_point_ref_no_ttl_local(&prepared),
        Some(b"prepared-bytes".as_slice())
    );

    let prepared_view = local.get_prepared_point_view_no_ttl_local(&prepared);
    assert_eq!(prepared_view.slice(), Some(b"prepared-bytes".as_slice()));
    assert!(matches!(
        local.prepare_point_key_if_local(&remote_key),
        Err(LocalRouteError::KeyNotLocal { .. })
    ));
}

#[test]
fn worker_local_session_batch_view_is_route_checked() {
    let store = EmbeddedStore::with_route_mode(2, EmbeddedRouteMode::SessionPrefix);
    let bootstrap = WorkerLocalEmbeddedStoreBootstrap::from_embedded(store, 2);
    let mut stores = bootstrap.into_stores();
    let mut local = stores.pop().expect("expected one local store");

    let local_session = find_session(&local, true);
    let remote_session = find_session(&local, false);
    let keys = vec![
        b"s:local:c:0".to_vec(),
        b"s:local:c:1".to_vec(),
        b"s:local:c:2".to_vec(),
    ];
    let items = keys
        .iter()
        .enumerate()
        .map(|(index, key)| (key.clone(), format!("chunk-{index}").into_bytes()))
        .collect::<Vec<_>>();
    local
        .batch_set_session_owned_no_ttl_if_local(local_session.clone(), items)
        .expect("local session should be accepted");

    let view = local
        .batch_get_session_view_if_local(&local_session, &keys)
        .expect("local session batch view should work");
    assert_eq!(view.hit_count(), 3);
    assert_eq!(view.slice(1), Some(b"chunk-1".as_slice()));
    assert!(matches!(
        local.batch_get_session_view_if_local(&remote_session, &keys),
        Err(LocalRouteError::SessionNotLocal { .. })
    ));
}

#[test]
fn worker_local_session_packed_view_is_zero_copy_eligible() {
    let store = EmbeddedStore::with_route_mode(2, EmbeddedRouteMode::SessionPrefix);
    let bootstrap = WorkerLocalEmbeddedStoreBootstrap::from_embedded(store, 2);
    let mut stores = bootstrap.into_stores();
    let mut local = stores.pop().expect("expected one local store");

    let local_session = find_session(&local, true);
    let remote_session = find_session(&local, false);
    let keys = vec![
        b"s:packed:c:0".to_vec(),
        b"s:packed:c:missing".to_vec(),
        b"s:packed:c:1".to_vec(),
    ];
    local
        .batch_set_session_owned_no_ttl_if_local(
            local_session.clone(),
            vec![
                (b"s:packed:c:0".to_vec(), b"alpha".to_vec()),
                (b"s:packed:c:1".to_vec(), b"beta".to_vec()),
            ],
        )
        .expect("local session should be accepted");

    let packed = local
        .batch_get_session_packed_view_if_local(&local_session, &keys)
        .expect("local session packed view should work")
        .expect("local session should be stored as a packed slab");
    assert_eq!(packed.hit_count(), 2);
    assert_eq!(packed.total_bytes(), 9);
    assert_eq!(
        &packed.buffer()[packed.offsets()[0]..packed.offsets()[0] + packed.lengths()[0]],
        b"alpha"
    );
    assert_eq!(packed.offsets()[1], usize::MAX);
    assert_eq!(
        &packed.buffer()[packed.offsets()[2]..packed.offsets()[2] + packed.lengths()[2]],
        b"beta"
    );
    drop(packed);

    assert!(matches!(
        local.batch_get_session_packed_view_if_local(&remote_session, &keys),
        Err(LocalRouteError::SessionNotLocal { .. })
    ));
}

#[test]
fn worker_local_stream_session_transfer_tracks_hits_and_misses() {
    let store = EmbeddedStore::with_route_mode(2, EmbeddedRouteMode::SessionPrefix);
    let bootstrap = WorkerLocalEmbeddedStoreBootstrap::from_embedded(store, 2);
    let mut stores = bootstrap.into_stores();
    let mut local = stores.pop().expect("expected one local store");
    let session = find_session(&local, true);

    local
        .batch_set_session_owned_no_ttl_if_local(
            session.clone(),
            vec![
                (b"s:gpu:l:0".to_vec(), b"layer-0".to_vec()),
                (b"s:gpu:l:1".to_vec(), b"layer-1".to_vec()),
            ],
        )
        .expect("local session should be accepted");

    let request = CudaSessionTransferRequest::new(
        session.clone(),
        vec![
            CudaChunkTransferDescriptor::new(b"s:gpu:l:0".to_vec(), 0, 0),
            CudaChunkTransferDescriptor::new(b"s:gpu:l:2".to_vec(), 2, 128),
            CudaChunkTransferDescriptor::new(b"s:gpu:l:1".to_vec(), 1, 256),
        ],
    );

    let mut observed = Vec::new();
    let result = local
        .stream_session_transfer_if_local(&request, |event| {
            match event {
                CudaSessionChunkEvent::Hit(hit) => observed.push((
                    hit.descriptor().layer_index(),
                    Some(hit.as_slice().to_vec()),
                    hit.descriptor().dst_offset_bytes(),
                )),
                CudaSessionChunkEvent::Miss(descriptor) => observed.push((
                    descriptor.layer_index(),
                    None,
                    descriptor.dst_offset_bytes(),
                )),
            }
            ControlFlow::<()>::Continue(())
        })
        .expect("local transfer should work");

    let stats = match result {
        ControlFlow::Continue(stats) => stats,
        ControlFlow::Break(()) => panic!("stream should not break"),
    };
    assert_eq!(stats.requested_chunks, 3);
    assert_eq!(stats.hit_chunks, 2);
    assert_eq!(stats.missed_chunks, 1);
    assert_eq!(stats.transferred_bytes, b"layer-0".len() + b"layer-1".len());
    assert_eq!(
        observed,
        vec![
            (0, Some(b"layer-0".to_vec()), 0),
            (2, None, 128),
            (1, Some(b"layer-1".to_vec()), 256),
        ]
    );
}
