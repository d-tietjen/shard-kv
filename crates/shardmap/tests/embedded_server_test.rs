mod common;

use std::sync::Arc;
use std::time::Duration;

use shardcache_client_rs::{ShardCacheClient, ShardCacheDirectRouter, VAddOptions, VSimOptions};
use shardmap::config::ServerEndpointMode;
use shardmap::protocol::{FastCodec, FastCommand, FastRequest, FastResponse, Frame};
use shardmap::storage::{
    EmbeddedStore, hash_key, take_local_embedded_store, with_local_embedded_store,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use common::{send_command, test_config};

static SERVER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn fast_request<'a>(command: FastCommand<'a>, key: Option<&'a [u8]>) -> FastRequest<'a> {
    FastRequest {
        key_hash: key.map(hash_key),
        route_shard: None,
        key_tag: None,
        command,
    }
}

async fn send_fast_command(
    stream: &mut tokio::net::TcpStream,
    request: FastRequest<'_>,
) -> FastResponse {
    let mut bytes = Vec::new();
    FastCodec::encode_request(&request, &mut bytes);
    stream.write_all(&bytes).await.expect("write fast command");

    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk))
            .await
            .expect("timeout")
            .expect("read");
        assert!(read > 0, "server closed connection");
        buffer.extend_from_slice(&chunk[..read]);
        if let Some((response, _)) =
            FastCodec::decode_response(&buffer).expect("decode fast response")
        {
            return response;
        }
    }
}

fn key_for_shard(store: &EmbeddedStore, shard_id: usize) -> Vec<u8> {
    for index in 0..10_000 {
        let key = format!("embedded-server-key-{shard_id}-{index}").into_bytes();
        if store.route_key(&key).shard_id == shard_id {
            return key;
        }
    }
    panic!("unable to find key for shard {shard_id}");
}

fn reserve_consecutive_ports() -> (std::net::TcpListener, std::net::TcpListener) {
    for _ in 0..100 {
        let first = std::net::TcpListener::bind("127.0.0.1:0").expect("bind first port");
        let first_port = first.local_addr().expect("first addr").port();
        let Some(second_port) = first_port.checked_add(1) else {
            continue;
        };
        let second_addr = format!("127.0.0.1:{second_port}");
        if let Ok(second) = std::net::TcpListener::bind(second_addr) {
            return (first, second);
        }
    }
    panic!("unable to find consecutive free ports");
}

fn free_consecutive_ports() -> (u16, u16) {
    let (first, second) = reserve_consecutive_ports();
    let ports = (
        first.local_addr().expect("first addr").port(),
        second.local_addr().expect("second addr").port(),
    );
    drop(second);
    drop(first);
    ports
}

fn free_consecutive_ports_for_shards(shard_count: usize) -> (u16, u16) {
    for _ in 0..100 {
        let first = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fanout port");
        let first_port = first.local_addr().expect("fanout addr").port();
        let mut listeners = vec![first];
        let mut complete = true;
        for offset in 1..=shard_count {
            let Ok(offset) = u16::try_from(offset) else {
                complete = false;
                break;
            };
            let Some(port) = first_port.checked_add(offset) else {
                complete = false;
                break;
            };
            match std::net::TcpListener::bind(("127.0.0.1", port)) {
                Ok(listener) => listeners.push(listener),
                Err(_) => {
                    complete = false;
                    break;
                }
            }
        }
        if complete {
            let direct_port = first_port + 1;
            drop(listeners);
            return (first_port, direct_port);
        }
    }
    panic!("unable to find fanout and direct shard port range");
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_store_can_be_exposed_as_tcp_server() {
    let _test_guard = SERVER_TEST_LOCK.lock().await;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut config = test_config(temp_dir.path().join("embedded-server-data"), false);
            config.bind_addr = format!("127.0.0.1:{}", common::free_port());
            // Deliberately mismatch the config and store shard counts. A
            // caller-owned embedded server must serve the caller's store rather
            // than constructing storage from the server config.
            config.shard_count = 1;
            config.persistence.enabled = false;

            let store = Arc::new(EmbeddedStore::new(4));
            store.set(b"local-key".to_vec(), b"local-value".to_vec(), None);
            let routed_key = key_for_shard(&store, 3);
            store.set(routed_key.clone(), b"routed-value".to_vec(), None);

            let server = shardmap::server::ShardCacheServer::from_embedded_store(
                config.clone(),
                store.clone(),
            );
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let join = tokio::task::spawn_local(async move {
                server
                    .run_with_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if join.is_finished() {
                    let result = join.await.expect("server task join");
                    panic!("embedded server exited before listening: {result:?}");
                }
                if tokio::net::TcpStream::connect(&config.bind_addr)
                    .await
                    .is_ok()
                {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("embedded server did not start listening in time");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            let mut stream = tokio::net::TcpStream::connect(&config.bind_addr)
                .await
                .unwrap();

            let get = send_command(&mut stream, &[b"GET", b"local-key"]).await;
            assert_eq!(get, Frame::BlobString(b"local-value".to_vec()));

            let fast_get = send_fast_command(
                &mut stream,
                fast_request(
                    FastCommand::Get {
                        key: routed_key.as_slice(),
                    },
                    Some(routed_key.as_slice()),
                ),
            )
            .await;
            assert_eq!(fast_get, FastResponse::Value(b"routed-value".to_vec()));

            let set = send_command(&mut stream, &[b"SET", b"remote-key", b"remote-value"]).await;
            assert_eq!(set, Frame::SimpleString("OK".into()));
            assert_eq!(store.get(b"remote-key"), Some(b"remote-value".to_vec()));

            shutdown_tx.send(()).unwrap();
            join.await.unwrap().unwrap();
        })
        .await;
}

fn exercise_reused_direct_routes(
    address: &str,
    route_mode: shardcache_client_rs::ShardCacheRouteMode,
    key: &[u8],
) {
    use shardcache_client_rs::{ShardCacheClientError, ShardCacheDirectClient, ShardCacheRoute};

    fn config_error<T>(result: shardcache_client_rs::Result<T>) {
        let error = result.err().expect("invalid route must be rejected");
        assert!(
            matches!(&error, ShardCacheClientError::Config(_)),
            "{error}"
        );
    }

    let timeout = Duration::from_secs(2);
    let router = ShardCacheDirectRouter::new(address, 4)
        .unwrap()
        .with_route_mode(route_mode);
    let mut client =
        ShardCacheDirectClient::connect_with_route_mode(address, 4, route_mode).unwrap();
    let route = client.route_key(key);
    assert_eq!(route.shard_id, 1);
    let invalid = ShardCacheRoute {
        shard_id: usize::MAX,
        ..route
    };
    let value = b"value\0\xff";
    let mut out = Vec::new();
    config_error(client.get_routed_into(invalid, key, &mut out));
    config_error(client.set_routed(invalid, key, value));
    config_error(client.set_ex_routed(invalid, key, value, 60_000));
    config_error(client.get_ex_routed_into(invalid, key, 60_000, &mut out));
    config_error(client.del_routed(invalid, key));
    config_error(client.exists_routed(invalid, key));
    config_error(client.ttl_routed(invalid, key));
    config_error(client.expire_routed(invalid, key, 60_000));

    client.set_routed(route, key, value).unwrap();
    assert!(client.get_routed_into(route, key, &mut out).unwrap());
    assert_eq!(out, value);
    assert!(client.exists_routed(route, key).unwrap());
    assert_eq!(client.ttl_routed(route, key).unwrap(), -1);
    assert!(client.expire_routed(route, key, 60_000).unwrap());
    assert!(client.ttl_routed(route, key).unwrap() >= 0);
    assert!(
        client
            .get_ex_routed_into(route, key, 60_000, &mut out)
            .unwrap()
    );
    assert_eq!(out, value);
    client.set_ex_routed(route, key, value, 60_000).unwrap();
    assert!(client.del_routed(route, key).unwrap());
    assert!(!client.exists_routed(route, key).unwrap());
    assert_eq!(client.ttl_routed(route, key).unwrap(), -2);

    client.set(key, value).unwrap();
    let mut pinned = router
        .connect_shard_with_timeouts(1, timeout, timeout)
        .unwrap();
    assert_eq!(pinned.route_key(key).unwrap(), route);
    let wrong_shard = ShardCacheRoute {
        shard_id: 0,
        ..route
    };
    config_error(pinned.get_routed_into(wrong_shard, key, &mut out));
    config_error(pinned.set_routed(wrong_shard, key, value));
    config_error(pinned.set_ex_routed(wrong_shard, key, value, 60_000));
    config_error(pinned.get_ex_routed_into(wrong_shard, key, 60_000, &mut out));
    config_error(pinned.del_routed(wrong_shard, key));
    config_error(pinned.exists_routed(wrong_shard, key));
    config_error(pinned.ttl_routed(wrong_shard, key));
    config_error(pinned.expire_routed(wrong_shard, key, 60_000));
    assert!(pinned.get_routed_into(route, key, &mut out).unwrap());
    assert_eq!(out, value);
    pinned.set_routed(route, key, b"pinned").unwrap();
    assert!(pinned.exists_routed(route, key).unwrap());
    assert_eq!(pinned.ttl_routed(route, key).unwrap(), -1);
    assert!(pinned.expire_routed(route, key, 60_000).unwrap());
    assert!(
        pinned
            .get_ex_routed_into(route, key, 60_000, &mut out)
            .unwrap()
    );
    assert_eq!(out, b"pinned");
    pinned.set_ex_routed(route, key, value, 60_000).unwrap();
    assert!(pinned.del_routed(route, key).unwrap());

    config_error(pinned.begin_pipeline_get_routed(invalid, key));
    config_error(pinned.begin_pipeline_set_routed(invalid, key, value));
    config_error(pinned.begin_pipeline_set_ex_routed(invalid, key, value, 60_000));
    config_error(pinned.begin_pipeline_get_ex_routed(invalid, key, 60_000));
    config_error(pinned.begin_pipeline_del_routed(invalid, key));
    config_error(pinned.begin_pipeline_exists_routed(invalid, key));
    config_error(pinned.begin_pipeline_ttl_routed(invalid, key));
    config_error(pinned.begin_pipeline_expire_routed(invalid, key, 60_000));
    pinned.begin_pipeline_set_routed(route, key, value).unwrap();
    pinned.begin_pipeline_get_routed(route, key).unwrap();
    pinned.begin_pipeline_exists_routed(route, key).unwrap();
    pinned.begin_pipeline_ttl_routed(route, key).unwrap();
    pinned
        .begin_pipeline_set_ex_routed(route, key, b"ttl", 60_000)
        .unwrap();
    pinned
        .begin_pipeline_get_ex_routed(route, key, 60_000)
        .unwrap();
    pinned
        .begin_pipeline_expire_routed(route, key, 60_000)
        .unwrap();
    pinned.begin_pipeline_del_routed(route, key).unwrap();
    pinned.flush_pipeline().unwrap();
    pinned.finish_pipeline_set().unwrap();
    assert!(pinned.finish_pipeline_get_into(&mut out).unwrap());
    assert_eq!(out, value);
    assert!(pinned.finish_pipeline_exists().unwrap());
    assert_eq!(pinned.finish_pipeline_ttl().unwrap(), -1);
    pinned.finish_pipeline_set_ex().unwrap();
    assert!(pinned.finish_pipeline_get_ex_into(&mut out).unwrap());
    assert_eq!(out, b"ttl");
    assert!(pinned.finish_pipeline_expire().unwrap());
    assert!(pinned.finish_pipeline_del().unwrap());
    assert!(!client.get_into(key, &mut out).unwrap());
}

fn exercise_reused_session_routes(address: &str, keys: &[Vec<u8>]) {
    use shardcache_client_rs::{ShardCacheDirectClient, ShardCacheRouteMode};

    let mode = ShardCacheRouteMode::SessionPrefix;
    let router = ShardCacheDirectRouter::new(address, 4)
        .unwrap()
        .with_route_mode(mode);
    let mut client = ShardCacheDirectClient::connect_with_route_mode(address, 4, mode).unwrap();
    let first = client.route_key(&keys[0]);
    let second = client.route_key(&keys[1]);
    assert_eq!(first.shard_id, 1);
    assert_eq!(second.shard_id, first.shard_id);
    assert_ne!(first.key_hash, second.key_hash);
    client.set_routed(first, &keys[0], b"first").unwrap();
    client.set(&keys[1], b"second").unwrap();
    let mut pinned = router.connect_shard(1).unwrap();
    let mut out = Vec::new();
    assert!(pinned.get_routed_into(first, &keys[0], &mut out).unwrap());
    assert_eq!(out, b"first");
    assert!(pinned.get_routed_into(second, &keys[1], &mut out).unwrap());
    assert_eq!(out, b"second");
    assert_eq!(pinned.ttl_routed(first, &keys[0]).unwrap(), -1);
    assert!(!pinned.expire_routed(first, &keys[0], 60_000).unwrap());
    assert!(!client.expire(&keys[1], 60_000).unwrap());
    assert_eq!(client.ttl_routed(second, &keys[1]).unwrap(), -1);
    assert!(pinned.get_routed_into(first, &keys[0], &mut out).unwrap());
    assert_eq!(out, b"first");
    assert!(client.del_routed(first, &keys[0]).unwrap());
    assert!(client.del(&keys[1]).unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn reused_direct_routes_preserve_operations_and_pipeline_order() {
    use shardcache_client_rs::ShardCacheRouteMode;
    use shardmap::storage::EmbeddedRouteMode;

    let _test_guard = SERVER_TEST_LOCK.lock().await;
    for (client_mode, store_mode) in [
        (ShardCacheRouteMode::FullKey, EmbeddedRouteMode::FullKey),
        (
            ShardCacheRouteMode::SessionPrefix,
            EmbeddedRouteMode::SessionPrefix,
        ),
        (
            ShardCacheRouteMode::OverflowSlot,
            EmbeddedRouteMode::OverflowSlot,
        ),
    ] {
        tokio::task::LocalSet::new()
            .run_until(async {
                let temp = tempfile::TempDir::new().unwrap();
                let (fanout_port, direct_port) = free_consecutive_ports_for_shards(4);
                let mut config = test_config(temp.path().join("reused-routes"), false);
                config.bind_addr = format!("127.0.0.1:{fanout_port}");
                config.server_endpoint_mode = ServerEndpointMode::DirectShard;
                let store = Arc::new(EmbeddedStore::with_route_mode(4, store_mode));
                let key = if client_mode == ShardCacheRouteMode::OverflowSlot {
                    let mut key = b"SCKVKEY1".to_vec();
                    key.extend_from_slice(&1u32.to_le_bytes());
                    key.extend_from_slice(b"\0\0\0\0\0\0binary\xff");
                    key
                } else {
                    (0..10_000)
                        .find_map(|index| {
                            let key = format!("reused-{index}\0").into_bytes();
                            (store.route_key(&key).shard_id == 1).then_some(key)
                        })
                        .expect("key for shard one")
                };
                assert_eq!(store.route_key(&key).shard_id, 1);
                let session_keys = (client_mode == ShardCacheRouteMode::SessionPrefix).then(|| {
                    (0..10_000)
                        .find_map(|index| {
                            let first = format!("s:reused-{index}:c:0").into_bytes();
                            (store.route_key(&first).shard_id == 1)
                                .then(|| vec![first, format!("s:reused-{index}:c:1").into_bytes()])
                        })
                        .expect("session keys for shard one")
                });
                let check_key = key.clone();
                let server =
                    shardmap::server::ShardCacheServer::from_embedded_store(config, store.clone());
                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
                let server_task = tokio::task::spawn_local(async move {
                    server
                        .run_with_shutdown(async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                });
                let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                loop {
                    if tokio::net::TcpStream::connect(("127.0.0.1", fanout_port))
                        .await
                        .is_ok()
                    {
                        break;
                    }
                    assert!(
                        !server_task.is_finished(),
                        "server stopped before listening"
                    );
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "server did not start"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let client_task = tokio::task::spawn_blocking(move || {
                    exercise_reused_direct_routes(
                        &format!("127.0.0.1:{direct_port}"),
                        client_mode,
                        &key,
                    );
                    if let Some(keys) = session_keys {
                        exercise_reused_session_routes(&format!("127.0.0.1:{direct_port}"), &keys);
                    }
                });
                let result = tokio::time::timeout(Duration::from_secs(10), client_task).await;
                let _ = shutdown_tx.send(());
                server_task.await.unwrap().unwrap();
                result.unwrap().unwrap();
                assert!(store.get(&check_key).is_none());
            })
            .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_server_direct_shard_endpoint_shares_store_memory() {
    let _test_guard = SERVER_TEST_LOCK.lock().await;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let (fanout_port, direct_port) = free_consecutive_ports();
            let mut config = test_config(temp_dir.path().join("embedded-direct-shard-data"), false);
            config.bind_addr = format!("127.0.0.1:{fanout_port}");
            config.shard_count = 1;
            config.persistence.enabled = false;
            config.server_endpoint_mode = ServerEndpointMode::DirectShard;

            let store = Arc::new(EmbeddedStore::new(1));
            store.set(b"local-key".to_vec(), b"local-value".to_vec(), None);

            let server = shardmap::server::ShardCacheServer::from_embedded_store(
                config.clone(),
                store.clone(),
            );
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let join = tokio::task::spawn_local(async move {
                server
                    .run_with_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });

            let fanout_addr = config.bind_addr.clone();
            let direct_addr = format!("127.0.0.1:{direct_port}");
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                let fanout_ready = tokio::net::TcpStream::connect(&fanout_addr).await.is_ok();
                let direct_ready = tokio::net::TcpStream::connect(&direct_addr).await.is_ok();
                if fanout_ready && direct_ready {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("embedded direct-shard server did not start listening in time");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            let mut direct_stream = tokio::net::TcpStream::connect(&direct_addr).await.unwrap();
            let get = send_command(&mut direct_stream, &[b"GET", b"local-key"]).await;
            assert_eq!(get, Frame::BlobString(b"local-value".to_vec()));

            let set = send_command(
                &mut direct_stream,
                &[b"SET", b"remote-key", b"remote-value"],
            )
            .await;
            assert_eq!(set, Frame::SimpleString("OK".into()));
            assert_eq!(store.get(b"remote-key"), Some(b"remote-value".to_vec()));

            let mut fanout_stream = tokio::net::TcpStream::connect(&fanout_addr).await.unwrap();
            let fanout_get = send_command(&mut fanout_stream, &[b"GET", b"remote-key"]).await;
            assert_eq!(fanout_get, Frame::BlobString(b"remote-value".to_vec()));

            shutdown_tx.send(()).unwrap();
            join.await.unwrap().unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn typed_scnp_vector_client_round_trips_fanout_and_direct_shard() {
    let _test_guard = SERVER_TEST_LOCK.lock().await;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let shard_count = 4;
            let (fanout_port, direct_port) = free_consecutive_ports_for_shards(shard_count);
            let mut config = test_config(temp_dir.path().join("typed-vector-client"), false);
            config.bind_addr = format!("127.0.0.1:{fanout_port}");
            config.shard_count = shard_count;
            config.persistence.enabled = false;
            config.server_endpoint_mode = ServerEndpointMode::DirectShard;

            let store = Arc::new(EmbeddedStore::new(shard_count));
            let vector_key = key_for_shard(&store, 2);
            let server =
                shardmap::server::ShardCacheServer::from_embedded_store(config.clone(), store);
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let join = tokio::task::spawn_local(async move {
                server
                    .run_with_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });

            let fanout_addr = config.bind_addr.clone();
            let direct_addr = format!("127.0.0.1:{direct_port}");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                let fanout_ready = tokio::net::TcpStream::connect(&fanout_addr).await.is_ok();
                let direct_ready = tokio::net::TcpStream::connect(&direct_addr).await.is_ok();
                if fanout_ready && direct_ready {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "typed vector server did not start in time"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            tokio::task::spawn_blocking(move || {
                let timeout = Duration::from_secs(2);
                let mut client = ShardCacheClient::connect_with_timeouts_and_auth(
                    &fanout_addr,
                    timeout,
                    timeout,
                    None,
                )
                .unwrap();
                assert_eq!(client.ping().unwrap(), b"PONG");
                assert!(
                    client
                        .vadd(
                            &vector_key,
                            b"doc-a",
                            &[1.0, 0.0],
                            VAddOptions::new()
                                .attributes(br#"{"kind":"a"}"#)
                                .governance_metadata(b"tenant=acme"),
                        )
                        .unwrap()
                );
                assert!(
                    client
                        .vadd(
                            &vector_key,
                            b"doc-b",
                            &[0.0, 1.0],
                            VAddOptions::new().attributes(br#"{"kind":"b"}"#),
                        )
                        .unwrap()
                );
                let matches = client
                    .vsim(
                        &vector_key,
                        &[1.0, 0.0],
                        VSimOptions::new()
                            .count(2)
                            .exact(true)
                            .allow_governance(b"tenant=acme")
                            .with_governance(true),
                    )
                    .unwrap();
                assert_eq!(matches.len(), 2);
                assert_eq!(matches[0].element, b"doc-a");
                assert!((matches[0].score - 1.0).abs() < 1e-6);
                assert_eq!(
                    matches[0].attributes.as_deref(),
                    Some(br#"{"kind":"a"}"#.as_slice())
                );
                assert_eq!(
                    matches[0].governance.as_deref(),
                    Some(b"tenant=acme".as_slice())
                );
                assert!(matches[1].governance.is_none());

                let router = ShardCacheDirectRouter::new(&direct_addr, shard_count).unwrap();
                let mut direct = router
                    .connect_shard_with_timeouts_and_auth(0, timeout, timeout, None)
                    .unwrap();
                assert_eq!(direct.ping().unwrap(), b"PONG");
                let direct_matches = direct
                    .vsim(
                        &vector_key,
                        &[0.0, 1.0],
                        VSimOptions::new().count(1).exact(true),
                    )
                    .unwrap();
                assert_eq!(direct_matches[0].element, b"doc-b");
                assert!(!direct.vrem(&vector_key, b"doc-a").unwrap());
                assert!(
                    direct
                        .vrem_governed(&vector_key, b"doc-a", b"tenant=acme")
                        .unwrap()
                );
                assert!(!direct.vrem(&vector_key, b"doc-a").unwrap());
            })
            .await
            .unwrap();

            shutdown_tx.send(()).unwrap();
            join.await.unwrap().unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_server_reports_direct_shard_bind_failure() {
    let _test_guard = SERVER_TEST_LOCK.lock().await;
    let temp_dir = tempfile::TempDir::new().unwrap();
    let (fanout_reservation, direct_reservation) = reserve_consecutive_ports();
    let fanout_port = fanout_reservation.local_addr().unwrap().port();
    let direct_port = direct_reservation.local_addr().unwrap().port();
    assert_eq!(direct_port, fanout_port + 1);
    drop(fanout_reservation);

    let mut config = test_config(temp_dir.path().join("embedded-direct-bind-failure"), false);
    config.bind_addr = format!("127.0.0.1:{fanout_port}");
    config.shard_count = 1;
    config.persistence.enabled = false;
    config.server_endpoint_mode = ServerEndpointMode::DirectShard;

    let server = shardmap::server::ShardCacheServer::from_embedded_store(
        config,
        Arc::new(EmbeddedStore::new(1)),
    );
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        server.run_with_shutdown(std::future::pending()),
    )
    .await
    .expect("direct-shard bind failure should return immediately");
    let error = result.expect_err("occupied direct-shard port must fail startup");
    assert!(matches!(
        error,
        shardmap::ShardCacheError::Io(ref source)
            if source.kind() == std::io::ErrorKind::AddrInUse
    ));

    drop(direct_reservation);
}

#[tokio::test(flavor = "current_thread")]
async fn thread_local_embedded_store_can_be_exposed_as_tcp_server() {
    let _test_guard = SERVER_TEST_LOCK.lock().await;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut config = test_config(temp_dir.path().join("thread-local-server-data"), false);
            config.bind_addr = format!("127.0.0.1:{}", common::free_port());
            config.shard_count = 1;
            config.persistence.enabled = false;

            let store = EmbeddedStore::new(1);
            store.set(b"local-key".to_vec(), b"local-value".to_vec(), None);
            let local_store = store
                .into_local_stores(1)
                .into_iter()
                .next()
                .expect("local store");
            local_store.install_local().expect("install local store");

            let server = shardmap::server::ShardCacheServer::from_thread_local_embedded_store(
                config.clone(),
            );
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let join = tokio::task::spawn_local(async move {
                server
                    .run_thread_local_with_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if join.is_finished() {
                    let result = join.await.expect("server task join");
                    panic!("thread-local embedded server exited before listening: {result:?}");
                }
                if tokio::net::TcpStream::connect(&config.bind_addr)
                    .await
                    .is_ok()
                {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("thread-local embedded server did not start listening in time");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            let mut stream = tokio::net::TcpStream::connect(&config.bind_addr)
                .await
                .unwrap();

            let get = send_command(&mut stream, &[b"GET", b"local-key"]).await;
            assert_eq!(get, Frame::BlobString(b"local-value".to_vec()));

            let set = send_command(&mut stream, &[b"SET", b"remote-key", b"remote-value"]).await;
            assert_eq!(set, Frame::SimpleString("OK".into()));
            with_local_embedded_store(|store| {
                assert_eq!(store.get(b"remote-key"), Some(b"remote-value".to_vec()));
                store.set(b"embedded-key".to_vec(), b"embedded-value".to_vec(), None);
            })
            .expect("local store remains installed");

            let get_embedded = send_command(&mut stream, &[b"GET", b"embedded-key"]).await;
            assert_eq!(get_embedded, Frame::BlobString(b"embedded-value".to_vec()));

            shutdown_tx.send(()).unwrap();
            join.await.unwrap().unwrap();

            let mut local_store =
                take_local_embedded_store().expect("server leaves caller local store installed");
            assert_eq!(
                local_store.get(b"remote-key"),
                Some(b"remote-value".to_vec())
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn embedded_public_server_rejects_cross_shard_commands() {
    let _test_guard = SERVER_TEST_LOCK.lock().await;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut config = test_config(temp_dir.path().join("embedded-routed-data"), false);
            config.bind_addr = format!("127.0.0.1:{}", common::free_port());
            config.persistence.enabled = false;

            let store = Arc::new(EmbeddedStore::new(4));
            let key_a = key_for_shard(&store, 0);
            let key_b = key_for_shard(&store, 1);
            store.set(key_a.clone(), b"a".to_vec(), None);
            store.set(key_b.clone(), b"b".to_vec(), None);

            let server = shardmap::server::ShardCacheServer::from_embedded_store(
                config.clone(),
                store.clone(),
            );
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let join = tokio::task::spawn_local(async move {
                server
                    .run_with_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if tokio::net::TcpStream::connect(&config.bind_addr)
                    .await
                    .is_ok()
                {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("embedded server did not start listening in time");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            let mut stream = tokio::net::TcpStream::connect(&config.bind_addr)
                .await
                .unwrap();

            let cross_shard =
                send_command(&mut stream, &[b"MGET", key_a.as_slice(), key_b.as_slice()]).await;
            assert_eq!(
                cross_shard,
                Frame::Error(
                    "ERR routed public embedded server only accepts single-shard commands".into()
                )
            );

            let get_a = send_command(&mut stream, &[b"GET", key_a.as_slice()]).await;
            assert_eq!(get_a, Frame::BlobString(b"a".to_vec()));

            let get_b = send_command(&mut stream, &[b"GET", key_b.as_slice()]).await;
            assert_eq!(get_b, Frame::BlobString(b"b".to_vec()));

            shutdown_tx.send(()).unwrap();
            join.await.unwrap().unwrap();
        })
        .await;
}
