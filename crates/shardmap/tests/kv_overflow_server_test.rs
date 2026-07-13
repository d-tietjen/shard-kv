use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use shardmap::config::{EvictionPolicy, KvOverflowConfig, PersistenceConfig, ShardCacheConfig};
#[cfg(feature = "object-overflow")]
use shardmap::config::{
    ObjectOverflowBackend, ObjectOverflowCompression, ObjectOverflowConfig,
    ObjectOverflowFailurePolicy,
};
use shardmap::server::ShardCacheServer;
use shardmap::storage::{EmbeddedStore, KvOverflowStore};

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
    primary.flush_remote().unwrap();
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
    assert!(
        replica.get_ref(b"hot").is_some(),
        "hot replica value stays in RAM"
    );
    assert!(
        replica.get_ref(b"cold").is_none(),
        "LRU cold replica value moves to object storage"
    );
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
fn live_scnp_nodes_form_disjoint_overflow_tier() {
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
        let on_first = first_store.exists(key.as_bytes());
        let on_second = second_store.exists(key.as_bytes());
        assert_ne!(on_first, on_second, "key must have exactly one owner");
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

    let corrupt_key = (0..1_000)
        .map(|index| format!("corrupt-{index}"))
        .find(|key| cluster.owner_id(key.as_bytes()) == first_addr)
        .expect("key owned by first node");
    first_store.set(
        corrupt_key.as_bytes().to_vec(),
        b"not-an-overflow-envelope".to_vec(),
        None,
    );
    assert!(cluster.get(corrupt_key.as_bytes()).is_err());
}

#[test]
fn live_scnp_expansion_preserves_slots_and_handoffs_on_ordered_write() {
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
        .expect("third node must acquire at least one logical slot");
    let slot = old_cluster.slot_for_key(key.as_bytes());
    let previous_owner = old_cluster.owner_id(key.as_bytes()).to_owned();

    assert_eq!(expanded.cluster().slot_for_key(key.as_bytes()), slot);
    assert_eq!(expanded.cluster().owner_id(key.as_bytes()), added_addr);
    old_cluster
        .put(key.as_bytes(), b"migrated-value", None)
        .unwrap();
    assert!(if previous_owner == first_addr {
        first_store.exists(key.as_bytes())
    } else {
        second_store.exists(key.as_bytes())
    });

    assert_eq!(
        expanded
            .get_remote(key.as_bytes())
            .unwrap()
            .unwrap()
            .value
            .as_ref(),
        b"migrated-value"
    );
    assert!(!added_store.exists(key.as_bytes()));
    assert!(if previous_owner == first_addr {
        first_store.exists(key.as_bytes())
    } else {
        second_store.exists(key.as_bytes())
    });
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
    assert!(added_store.exists(key.as_bytes()));
    assert!(!first_store.exists(key.as_bytes()));
    assert!(!second_store.exists(key.as_bytes()));
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
