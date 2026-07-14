use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use shardmap::config::{
    EvictionPolicy, KvOverflowConfig, KvOverflowReplica, KvOverflowReplicaServerConfig,
    PersistenceConfig, ServerEndpointMode, ShardCacheConfig,
};
#[cfg(feature = "object-overflow")]
use shardmap::config::{
    ObjectOverflowBackend, ObjectOverflowCompression, ObjectOverflowConfig,
    ObjectOverflowFailurePolicy,
};
#[cfg(feature = "scnp-tls")]
use shardmap::config::{ScnpTlsClientConfig, ScnpTlsServerConfig};
use shardmap::server::ShardCacheServer;
use shardmap::storage::{EmbeddedRouteMode, EmbeddedStore, KvOverflowStore};

static LIVE_SERVER_TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_live_server_test() -> std::sync::MutexGuard<'static, ()> {
    LIVE_SERVER_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct TestServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<JoinHandle<shardmap::Result<()>>>,
}

impl TestServer {
    fn start(addr: String, store: Arc<EmbeddedStore>) -> Self {
        Self::start_with_config(addr, store, ShardCacheConfig::default())
    }

    fn start_with_config(
        addr: String,
        store: Arc<EmbeddedStore>,
        mut config: ShardCacheConfig,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join = std::thread::spawn(move || {
            config.bind_addr = addr;
            config.shard_count = store.shard_count();
            config.persistence = PersistenceConfig {
                enabled: false,
                ..PersistenceConfig::default()
            };
            let server = ShardCacheServer::from_embedded_store(config, store);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(shardmap::ShardCacheError::Io)?;
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                server
                    .run_with_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
            })
        });
        Self {
            shutdown: Some(shutdown_tx),
            join: Some(join),
        }
    }
}

#[cfg(feature = "object-overflow")]
#[test]
fn replica_lru_cascades_cold_values_to_filesystem_object_overflow() {
    let _test_guard = lock_live_server_test();
    let temp = tempfile::tempdir().expect("tempdir");
    let addr = free_addr();
    let replica = Arc::new(EmbeddedStore::new(1));
    replica.configure_memory_policy(Some(9 * 1024), EvictionPolicy::Lru);
    let server_config = ShardCacheConfig {
        object_overflow: ObjectOverflowConfig {
            enabled: true,
            backend: ObjectOverflowBackend::File,
            endpoint: temp.path().display().to_string(),
            bucket: "kv-replica".into(),
            prefix: "overflow".into(),
            node_id: Some("replica-a".into()),
            min_value_bytes: 4,
            offload_min_idle_ticks: 0,
            compression: ObjectOverflowCompression::Zstd,
            zstd_level: 1,
            failure_policy: ObjectOverflowFailurePolicy::RetainResident,
            max_retries: 0,
            retry_backoff_ms: 1,
            operation_timeout_ms: 1_000,
            worker_threads: 1,
            queue_capacity: 32,
            ..ObjectOverflowConfig::default()
        },
        ..ShardCacheConfig::default()
    };
    let _server = TestServer::start_with_config(addr.clone(), Arc::clone(&replica), server_config);
    wait_for_server(&addr);

    let primary_config = KvOverflowConfig {
        enabled: true,
        endpoints: vec![addr],
        max_memory_bytes: 1,
        worker_threads: 1,
        queue_capacity: 32,
        ..KvOverflowConfig::default()
    };
    let primary = KvOverflowStore::from_config(EmbeddedStore::new(1), &primary_config).unwrap();
    primary
        .set(b"cold".to_vec(), vec![1; 4 * 1024], None)
        .unwrap();
    primary
        .set(b"hot".to_vec(), vec![2; 4 * 1024], None)
        .unwrap();
    primary
        .flush_remote()
        .unwrap_or_else(|error| panic!("{error}; health={:?}", primary.health_snapshot()));
    for _ in 0..128 {
        assert!(primary.cluster().get(b"hot").unwrap().is_some());
    }
    primary
        .set(b"new".to_vec(), vec![3; 4 * 1024], None)
        .unwrap();
    primary.flush_remote().unwrap();

    let overflow = &replica.shard_stats_snapshot()[0].object_overflow;
    assert!(overflow.remote_entries >= 1);
    assert!(overflow.offload_successes >= 1);
    assert_eq!(
        primary.cluster().get(b"cold").unwrap().unwrap().value.len(),
        4 * 1024
    );
    assert!(
        replica.shard_stats_snapshot()[0]
            .object_overflow
            .fault_successes
            >= 1
    );
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(join) = self.join.take() {
            let result = join.join().expect("overflow server thread");
            result.expect("overflow server shutdown");
        }
    }
}

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    let addr = listener.local_addr().expect("free address");
    drop(listener);
    addr.to_string()
}

fn wait_for_server(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "server {addr} did not start");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn shard_owned_direct_replica_validates_topology_and_uses_every_remote_shard() {
    let _test_guard = lock_live_server_test();
    let addr = free_addr();
    let socket: std::net::SocketAddr = addr.parse().unwrap();
    let direct_base_port = socket.port() + 1;
    let replica = Arc::new(EmbeddedStore::with_route_mode(
        4,
        EmbeddedRouteMode::OverflowSlot,
    ));
    let server_config = ShardCacheConfig {
        shard_count: 4,
        server_endpoint_mode: ServerEndpointMode::DirectShard,
        kv_overflow_replica: KvOverflowReplicaServerConfig {
            enabled: true,
            node_id: "replica-a".into(),
            encrypted_persistence: true,
            ..KvOverflowReplicaServerConfig::default()
        },
        ..ShardCacheConfig::default()
    };
    let _server = TestServer::start_with_config(addr.clone(), Arc::clone(&replica), server_config);
    wait_for_server(&addr);
    for shard in 0..4 {
        wait_for_server(&format!("127.0.0.1:{}", direct_base_port + shard));
    }

    let primary_config = KvOverflowConfig {
        enabled: true,
        replicas: vec![KvOverflowReplica {
            id: "replica-a".into(),
            addresses: vec![addr],
            shard_count: 4,
            direct_shard_base_port: 0,
            tls_server_name: None,
        }],
        cluster_id: "direct-integration".into(),
        max_memory_bytes: 128,
        queue_capacity_per_shard: 512,
        operation_timeout_ms: 5_000,
        ..KvOverflowConfig::default()
    };
    let primary = KvOverflowStore::from_config(EmbeddedStore::new(4), &primary_config).unwrap();
    assert_eq!(
        primary.health_snapshot().shard_queue_capacities,
        vec![512; 4]
    );
    for index in 0..256 {
        primary
            .set(
                format!("direct-key-{index}").into_bytes(),
                vec![index as u8; 32],
                None,
            )
            .unwrap();
    }
    primary
        .flush_remote()
        .unwrap_or_else(|error| panic!("{error}; health={:?}", primary.health_snapshot()));

    let deadline = Instant::now() + Duration::from_secs(2);
    while replica.len() != 256 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(replica.len(), 256, "health={:?}", primary.health_snapshot());
    let occupied = replica
        .shard_stats_snapshot()
        .into_iter()
        .filter(|stats| stats.key_count > 0)
        .count();
    assert_eq!(occupied, 4);
    assert_eq!(
        primary
            .cluster()
            .get(b"direct-key-42")
            .unwrap()
            .unwrap()
            .value
            .len(),
        32
    );
}

#[test]
fn shard_owned_replica_rejects_invalid_scnp_authentication() {
    let _test_guard = lock_live_server_test();
    const TOKEN_ENV: &str = "SHARDCACHE_TEST_KV_OVERFLOW_AUTH";
    unsafe { std::env::set_var(TOKEN_ENV, "correct-token") };
    let addr = free_addr();
    let replica = Arc::new(EmbeddedStore::with_route_mode(
        1,
        EmbeddedRouteMode::OverflowSlot,
    ));
    let server_config = ShardCacheConfig {
        shard_count: 1,
        server_endpoint_mode: ServerEndpointMode::DirectShard,
        kv_overflow_replica: KvOverflowReplicaServerConfig {
            enabled: true,
            node_id: "authenticated-replica".into(),
            auth_token_env: Some(TOKEN_ENV.into()),
            encrypted_persistence: true,
            ..KvOverflowReplicaServerConfig::default()
        },
        ..ShardCacheConfig::default()
    };
    let _server = TestServer::start_with_config(addr.clone(), replica, server_config);
    wait_for_server(&addr);

    let config = KvOverflowConfig {
        enabled: true,
        replicas: vec![KvOverflowReplica {
            id: "authenticated-replica".into(),
            addresses: vec![addr],
            shard_count: 1,
            direct_shard_base_port: 0,
            tls_server_name: None,
        }],
        scnp_auth_token_env: Some(TOKEN_ENV.into()),
        max_memory_bytes: 1,
        ..KvOverflowConfig::default()
    };
    unsafe { std::env::set_var(TOKEN_ENV, "wrong-token") };
    assert!(KvOverflowStore::from_config(EmbeddedStore::new(1), &config).is_err());

    unsafe { std::env::set_var(TOKEN_ENV, "correct-token") };
    let primary = KvOverflowStore::from_config(EmbeddedStore::new(1), &config).unwrap();
    primary
        .set(b"key".to_vec(), b"value".to_vec(), None)
        .unwrap();
    primary.flush_remote().unwrap();
    assert_eq!(primary.get(b"key").unwrap(), Some(b"value".to_vec()));
    unsafe { std::env::remove_var(TOKEN_ENV) };
}

#[cfg(feature = "scnp-tls")]
#[test]
fn shard_owned_replica_encrypts_topology_mutation_and_read_connections() {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose,
    };
    use sha2::{Digest, Sha256};

    let _test_guard = lock_live_server_test();
    let directory = tempfile::tempdir().unwrap();
    let ca_path = directory.path().join("ca.pem");
    let server_cert_path = directory.path().join("server-cert.pem");
    let server_key_path = directory.path().join("server-key.pem");
    let client_cert_path = directory.path().join("client-cert.pem");
    let client_key_path = directory.path().join("client-key.pem");
    let unauthorized_cert_path = directory.path().join("unauthorized-cert.pem");
    let unauthorized_key_path = directory.path().join("unauthorized-key.pem");

    let mut ca_params = CertificateParams::new(vec!["overflow-test-ca".into()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();

    let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &ca).unwrap();

    let mut client_params = CertificateParams::new(vec!["overflow-primary".into()]).unwrap();
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_key = KeyPair::generate().unwrap();
    let client_cert = client_params.signed_by(&client_key, &ca).unwrap();
    let client_fingerprint = Sha256::digest(client_cert.der())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut unauthorized_params = CertificateParams::new(vec!["other-service".into()]).unwrap();
    unauthorized_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let unauthorized_key = KeyPair::generate().unwrap();
    let unauthorized_cert = unauthorized_params
        .signed_by(&unauthorized_key, &ca)
        .unwrap();

    let mut rotated_ca_params = CertificateParams::new(vec!["rotated-overflow-ca".into()]).unwrap();
    rotated_ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    rotated_ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::CrlSign,
    ];
    let rotated_ca =
        CertifiedIssuer::self_signed(rotated_ca_params, KeyPair::generate().unwrap()).unwrap();
    let mut rotated_server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    rotated_server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let rotated_server_key = KeyPair::generate().unwrap();
    let rotated_server_cert = rotated_server_params
        .signed_by(&rotated_server_key, &rotated_ca)
        .unwrap();
    let mut rotated_client_params =
        CertificateParams::new(vec!["rotated-overflow-primary".into()]).unwrap();
    rotated_client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let rotated_client_key = KeyPair::generate().unwrap();
    let rotated_client_cert = rotated_client_params
        .signed_by(&rotated_client_key, &rotated_ca)
        .unwrap();
    let rotated_client_fingerprint = Sha256::digest(rotated_client_cert.der())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    std::fs::write(&ca_path, ca.pem()).unwrap();
    std::fs::write(&server_cert_path, server_cert.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();
    std::fs::write(&client_cert_path, client_cert.pem()).unwrap();
    std::fs::write(&client_key_path, client_key.serialize_pem()).unwrap();
    std::fs::write(&unauthorized_cert_path, unauthorized_cert.pem()).unwrap();
    std::fs::write(&unauthorized_key_path, unauthorized_key.serialize_pem()).unwrap();

    let addr = free_addr();
    let direct_addr = {
        let mut address = addr.parse::<std::net::SocketAddr>().unwrap();
        address.set_port(address.port() + 1);
        address.to_string()
    };
    let replica = Arc::new(EmbeddedStore::with_route_mode(
        1,
        EmbeddedRouteMode::OverflowSlot,
    ));
    let server_config = ShardCacheConfig {
        shard_count: 1,
        server_endpoint_mode: ServerEndpointMode::DirectShard,
        kv_overflow_replica: KvOverflowReplicaServerConfig {
            enabled: true,
            node_id: "tls-replica".into(),
            encrypted_persistence: true,
            tls: ScnpTlsServerConfig {
                enabled: true,
                cert_path: server_cert_path.clone(),
                key_path: server_key_path.clone(),
                client_ca_path: Some(ca_path.clone()),
                client_cert_sha256: vec![client_fingerprint, rotated_client_fingerprint],
                reload_interval_ms: 1,
                ..ScnpTlsServerConfig::default()
            },
            ..KvOverflowReplicaServerConfig::default()
        },
        ..ShardCacheConfig::default()
    };
    let _server = TestServer::start_with_config(addr.clone(), replica, server_config);
    wait_for_server(&addr);
    wait_for_server(&direct_addr);

    let mut config = KvOverflowConfig {
        enabled: true,
        replicas: vec![KvOverflowReplica {
            id: "tls-replica".into(),
            addresses: vec![addr],
            shard_count: 1,
            direct_shard_base_port: 0,
            tls_server_name: Some("localhost".into()),
        }],
        scnp_tls: ScnpTlsClientConfig {
            enabled: true,
            ca_path: ca_path.clone(),
            ..ScnpTlsClientConfig::default()
        },
        max_memory_bytes: 1,
        ..KvOverflowConfig::default()
    };
    assert!(KvOverflowStore::from_config(EmbeddedStore::new(1), &config).is_err());
    config.scnp_tls.client_cert_path = Some(unauthorized_cert_path);
    config.scnp_tls.client_key_path = Some(unauthorized_key_path);
    assert!(KvOverflowStore::from_config(EmbeddedStore::new(1), &config).is_err());
    config.scnp_tls.client_cert_path = Some(client_cert_path.clone());
    config.scnp_tls.client_key_path = Some(client_key_path.clone());
    let primary = KvOverflowStore::from_config(EmbeddedStore::new(1), &config).unwrap();
    primary
        .set(b"tls-key".to_vec(), b"tls-value".to_vec(), None)
        .unwrap();
    primary.flush_remote().unwrap();
    assert_eq!(
        primary.get(b"tls-key").unwrap(),
        Some(b"tls-value".to_vec())
    );

    std::fs::write(&ca_path, rotated_ca.pem()).unwrap();
    std::fs::write(&server_cert_path, rotated_server_cert.pem()).unwrap();
    std::fs::write(&server_key_path, rotated_server_key.serialize_pem()).unwrap();
    std::fs::write(&client_cert_path, rotated_client_cert.pem()).unwrap();
    std::fs::write(&client_key_path, rotated_client_key.serialize_pem()).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let rotated_primary = KvOverflowStore::from_config(EmbeddedStore::new(1), &config).unwrap();
    rotated_primary
        .set(b"rotated-key".to_vec(), b"rotated-value".to_vec(), None)
        .unwrap();
    rotated_primary.flush_remote().unwrap();
    assert_eq!(
        rotated_primary.get(b"rotated-key").unwrap(),
        Some(b"rotated-value".to_vec())
    );
}

#[test]
fn live_scnp_nodes_form_disjoint_overflow_tier() {
    let _test_guard = lock_live_server_test();
    let first_addr = free_addr();
    let second_addr = free_addr();
    let first_store = Arc::new(EmbeddedStore::new(1));
    let second_store = Arc::new(EmbeddedStore::new(1));
    let _first_server = TestServer::start(first_addr.clone(), Arc::clone(&first_store));
    let _second_server = TestServer::start(second_addr.clone(), Arc::clone(&second_store));
    wait_for_server(&first_addr);
    wait_for_server(&second_addr);

    let config = KvOverflowConfig {
        enabled: true,
        backend: shardmap::config::KvOverflowBackend::Scnp,
        endpoints: vec![first_addr.clone(), second_addr],
        previous_endpoints: Vec::new(),
        slot_count: 16_384,
        redis_key_prefix: "shardcache:overflow:".into(),
        redis_username_env: None,
        redis_password_env: None,
        max_memory_bytes: 96,
        eviction_policy: EvictionPolicy::Lfu,
        connections_per_endpoint: 2,
        connect_timeout_ms: 500,
        operation_timeout_ms: 1_000,
        max_retries: 2,
        retry_backoff_ms: 10,
        cleanup_interval_ms: 25,
        fetch_on_miss: true,
        worker_threads: 2,
        queue_capacity: 128,
        ..KvOverflowConfig::default()
    };
    let primary = KvOverflowStore::from_config(EmbeddedStore::new(2), &config).unwrap();
    for index in 0..64 {
        primary
            .set(
                format!("key-{index}").into_bytes(),
                vec![index as u8; 32],
                None,
            )
            .unwrap();
    }
    primary.flush_remote().unwrap();

    let cluster = primary.cluster();
    assert!(!first_store.is_empty());
    assert!(!second_store.is_empty());
    assert_eq!(first_store.len() + second_store.len(), 64);
    for index in 0..64 {
        let key = format!("key-{index}");
        let expected = vec![index as u8; 32];
        assert_eq!(
            cluster.get(key.as_bytes()).unwrap().unwrap().value.as_ref(),
            expected
        );
    }

    let stats = primary.health_snapshot();
    assert!(stats.offloads > 0);
    assert!(stats.resident_keys < stats.remote_keys);
    let remote_only = (0..64)
        .map(|index| format!("key-{index}"))
        .find(|key| !primary.inner().exists(key.as_bytes()))
        .expect("memory target should create remote-only values");
    assert!(primary.get(remote_only.as_bytes()).unwrap().is_some());
    assert!(primary.health_snapshot().fault_ins > 0);

    assert!(primary.delete(b"key-0").unwrap());
    assert!(cluster.get(b"key-0").unwrap().is_none());

    primary
        .set(b"ttl-key".to_vec(), b"ttl-value".to_vec(), Some(100))
        .unwrap();
    primary.flush_remote().unwrap();
    let ttl_value = cluster.get(b"ttl-key").unwrap().unwrap();
    assert_eq!(ttl_value.value.as_ref(), b"ttl-value");
    assert!(ttl_value.ttl_ms.is_some_and(|ttl| ttl <= 100));
    std::thread::sleep(Duration::from_millis(150));
    assert!(cluster.get(b"ttl-key").unwrap().is_none());
}

#[test]
fn live_scnp_expansion_preserves_slots_and_handoffs_on_ordered_write() {
    let _test_guard = lock_live_server_test();
    let first_addr = free_addr();
    let second_addr = free_addr();
    let added_addr = free_addr();
    let first_store = Arc::new(EmbeddedStore::new(1));
    let second_store = Arc::new(EmbeddedStore::new(1));
    let added_store = Arc::new(EmbeddedStore::new(1));
    let _first_server = TestServer::start(first_addr.clone(), Arc::clone(&first_store));
    let _second_server = TestServer::start(second_addr.clone(), Arc::clone(&second_store));
    let _added_server = TestServer::start(added_addr.clone(), Arc::clone(&added_store));
    wait_for_server(&first_addr);
    wait_for_server(&second_addr);
    wait_for_server(&added_addr);

    let previous_endpoints = vec![first_addr.clone(), second_addr.clone()];
    let old_config = KvOverflowConfig {
        enabled: true,
        endpoints: previous_endpoints.clone(),
        max_memory_bytes: 1024,
        ..KvOverflowConfig::default()
    };
    let old_cluster = shardmap::storage::KvOverflowCluster::from_config(&old_config).unwrap();
    let expanded_config = KvOverflowConfig {
        endpoints: vec![first_addr.clone(), second_addr.clone(), added_addr.clone()],
        previous_endpoints,
        ..old_config.clone()
    };
    let expanded = KvOverflowStore::from_config(EmbeddedStore::new(1), &expanded_config).unwrap();
    let key = (0..100_000)
        .map(|index| format!("moving-key-{index}"))
        .find(|key| {
            old_cluster.owner_id(key.as_bytes()) != expanded.cluster().owner_id(key.as_bytes())
        })
        .expect("exact rebalance must move at least one logical slot");
    let slot = old_cluster.slot_for_key(key.as_bytes());
    assert_eq!(expanded.cluster().slot_for_key(key.as_bytes()), slot);
    old_cluster
        .put(key.as_bytes(), b"migrated-value", None)
        .unwrap();
    assert_eq!(
        old_cluster
            .get(key.as_bytes())
            .unwrap()
            .unwrap()
            .value
            .as_ref(),
        b"migrated-value"
    );
    let lengths_before_fallback = (first_store.len(), second_store.len(), added_store.len());

    assert_eq!(
        expanded
            .get_remote(key.as_bytes())
            .unwrap()
            .unwrap()
            .value
            .as_ref(),
        b"migrated-value"
    );
    assert_eq!(
        (first_store.len(), second_store.len(), added_store.len()),
        lengths_before_fallback,
        "fallback reads must not migrate remote data"
    );
    let health = expanded.health_snapshot();
    assert_eq!(health.slot_count, 16_384);
    assert_eq!(health.previous_node_count, 2);
    assert_eq!(health.handoff_reads, 1);
    assert_eq!(health.handoff_hits, 1);
    assert_eq!(health.handoff_failures, 0);

    expanded
        .set(key.as_bytes().to_vec(), b"current-value".to_vec(), None)
        .unwrap();
    expanded.flush_remote().unwrap();
    assert_eq!(
        expanded
            .get_remote(key.as_bytes())
            .unwrap()
            .unwrap()
            .value
            .as_ref(),
        b"current-value"
    );
    assert!(old_cluster.get(key.as_bytes()).unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn standalone_primary_rejects_embedded_overflow_config() {
    let config = ShardCacheConfig {
        kv_overflow: KvOverflowConfig {
            enabled: true,
            endpoints: vec!["127.0.0.1:6381".into()],
            max_memory_bytes: 1024,
            ..KvOverflowConfig::default()
        },
        persistence: PersistenceConfig {
            enabled: false,
            ..PersistenceConfig::default()
        },
        ..ShardCacheConfig::default()
    };
    let server = ShardCacheServer::from_embedded_store(config, Arc::new(EmbeddedStore::new(1)));

    let error = server
        .run_with_shutdown(async {})
        .await
        .expect_err("standalone primary must reject embedded wrapper config");
    assert!(error.to_string().contains("embedded KvOverflowStore"));
}
