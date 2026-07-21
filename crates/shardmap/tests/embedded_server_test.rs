mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use shardcache_client_rs::{ShardCacheClient, ShardCacheDirectRouter, VAddOptions, VSimOptions};
use shardmap::config::{
    ReplicationCompression, ReplicationConfig, ReplicationRole, ReplicationSendPolicy,
    ServerEndpointMode,
};
use shardmap::protocol::{FastCodec, FastCommand, FastRequest, FastResponse, Frame};
use shardmap::replication::{ReplicatedEmbeddedStore, ReplicationReplicaClient};
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

#[tokio::test(flavor = "current_thread")]
async fn replicated_embedded_store_can_serve_read_replica() {
    let _test_guard = SERVER_TEST_LOCK.lock().await;
    let addr = format!("127.0.0.1:{}", common::free_port());
    let primary_config = ReplicationConfig {
        enabled: true,
        role: ReplicationRole::Primary,
        bind_addr: addr.clone(),
        compression: ReplicationCompression::None,
        send_policy: ReplicationSendPolicy::Immediate,
        batch_max_records: 1,
        batch_max_delay_us: 1_000,
        snapshot_chunk_bytes: 4 * 1024,
        ..ReplicationConfig::default()
    };
    let primary = Arc::new(
        ReplicatedEmbeddedStore::new(4, primary_config.clone()).expect("replicated primary"),
    );
    primary
        .set(b"before-connect".to_vec(), b"snapshot-value".to_vec(), None)
        .unwrap();

    let server = primary
        .serve_replicas(primary_config)
        .expect("replication listener");
    let replica = ReplicationReplicaClient::start(ReplicationConfig {
        enabled: true,
        role: ReplicationRole::Replica,
        replica_of: Some(addr),
        compression: ReplicationCompression::None,
        ..ReplicationConfig::default()
    })
    .expect("replica client");

    assert_eq!(
        await_replica_value(&replica, b"before-connect", Duration::from_secs(3)),
        Some(b"snapshot-value".to_vec())
    );

    primary
        .set(b"after-connect".to_vec(), b"streamed-value".to_vec(), None)
        .unwrap();
    assert_eq!(
        await_replica_value(&replica, b"after-connect", Duration::from_secs(3)),
        Some(b"streamed-value".to_vec())
    );

    replica.shutdown().ok();
    server.shutdown().ok();
}

fn await_replica_value(
    client: &ReplicationReplicaClient,
    key: &[u8],
    timeout: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(value) = client.replica().lock().get(key) {
            return Some(value);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}
