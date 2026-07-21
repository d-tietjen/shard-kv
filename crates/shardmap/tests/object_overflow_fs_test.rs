use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use shardmap::config::{
    EvictionPolicy, ObjectOverflowBackend, ObjectOverflowCompression, ObjectOverflowConfig,
    ObjectOverflowFailurePolicy,
};
use shardmap::storage::{
    EmbeddedStore, FileObjectOverflowStore, ObjectOverflowRuntime, ObjectOverflowStore,
};

fn file_overflow_config(root: &Path, bucket: &str, node_id: &str) -> ObjectOverflowConfig {
    ObjectOverflowConfig {
        enabled: true,
        backend: ObjectOverflowBackend::File,
        endpoint: root.display().to_string(),
        bucket: bucket.to_string(),
        prefix: "overflow".to_string(),
        node_id: Some(node_id.to_string()),
        min_value_bytes: 4,
        offload_min_idle_ticks: 0,
        compression: ObjectOverflowCompression::Zstd,
        zstd_level: 1,
        failure_policy: ObjectOverflowFailurePolicy::RetainResident,
        max_retries: 0,
        retry_backoff_ms: 1,
        operation_timeout_ms: 1_000,
        worker_threads: 2,
        queue_capacity: 64,
        cleanup_on_start: false,
        cleanup_interval_seconds: 0,
        cleanup_grace_seconds: 1,
        ..ObjectOverflowConfig::default()
    }
}

fn collect_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_files_into(root, &mut files);
    files.sort();
    files
}

fn collect_files_into(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files_into(&path, files);
        } else if path.is_file() {
            files.push(path);
        }
    }
}

fn overflow_bin_files(root: &Path) -> Vec<PathBuf> {
    collect_files(root)
        .into_iter()
        .filter(|path| path.extension().is_some_and(|extension| extension == "bin"))
        .collect()
}

fn wait_for_file_count(store: &EmbeddedStore, root: &Path, expected: usize) {
    for _ in 0..500 {
        store.process_maintenance();
        if overflow_bin_files(root).len() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    panic!("object payload file count did not reach {expected}");
}

fn wait_for_remote_entries(store: &EmbeddedStore, expected: usize) {
    for _ in 0..500 {
        store.process_maintenance();
        if store.shard_stats_snapshot()[0]
            .object_overflow
            .remote_entries
            == expected
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("object overflow remote entry count did not reach {expected}");
}

#[test]
fn filesystem_backend_offloads_materializes_snapshots_and_deletes_remote_files() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config = file_overflow_config(temp.path(), "bucket", "node-a");
    let runtime = ObjectOverflowRuntime::from_config(&config)
        .expect("runtime")
        .expect("enabled runtime");
    let store = EmbeddedStore::new(1);
    store.configure_memory_policy(Some(8), EvictionPolicy::Lru);
    store.configure_object_overflow(Some(runtime)).unwrap();

    let value = vec![42u8; 64 * 1024];
    store.set(b"alpha".to_vec(), value.clone(), None);
    wait_for_file_count(&store, temp.path(), 1);

    let files = collect_files(temp.path());
    assert!(
        files.iter().any(|path| path.ends_with("_generation.json")),
        "generation marker should be written: {files:?}"
    );
    let remote_files = overflow_bin_files(temp.path());
    assert_eq!(remote_files.len(), 1, "expected one remote payload file");

    let snapshot = store.try_entry_snapshot().expect("snapshot materializes");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].key, b"alpha");
    assert_eq!(snapshot[0].value, value);

    assert!(store.delete(b"alpha"));
    wait_for_file_count(&store, temp.path(), 0);
    assert!(
        overflow_bin_files(temp.path()).is_empty(),
        "delete should remove the remote payload"
    );
}

#[test]
fn filesystem_backend_preserves_governance_across_offload_and_fault_in() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config = file_overflow_config(temp.path(), "governed-bucket", "governed-node");
    let runtime = ObjectOverflowRuntime::from_config(&config)
        .expect("runtime")
        .expect("enabled runtime");
    let store = EmbeddedStore::new(1);
    store.configure_memory_policy(Some(8), EvictionPolicy::Lru);
    store.configure_object_overflow(Some(runtime)).unwrap();
    let value = bytes::Bytes::from(vec![7u8; 64 * 1024]);

    store.set_value_bytes_with_governance(
        b"private",
        value.clone(),
        None,
        bytes::Bytes::from_static(b"tenant-a/repo-private"),
    );
    wait_for_file_count(&store, temp.path(), 1);
    assert_eq!(store.get(b"private"), None);
    assert_eq!(overflow_bin_files(temp.path()).len(), 1);

    let snapshot = store.try_entry_snapshot().expect("materialize snapshot");
    assert_eq!(snapshot[0].value, value.as_ref());
    assert_eq!(
        snapshot[0].governance.as_deref(),
        Some(b"tenant-a/repo-private".as_slice())
    );
    assert_eq!(
        store
            .get_value_bytes_with_governance_filter(b"private", |metadata| {
                metadata == Some(b"tenant-a/repo-private".as_slice())
            })
            .as_deref(),
        Some(value.as_ref())
    );
    assert_eq!(store.get(b"private"), None);
}

#[test]
fn filesystem_backend_snapshot_fails_when_remote_payload_is_missing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config = file_overflow_config(temp.path(), "bucket", "node-b");
    let runtime = ObjectOverflowRuntime::from_config(&config)
        .expect("runtime")
        .expect("enabled runtime");
    let store = EmbeddedStore::new(1);
    store.configure_memory_policy(Some(8), EvictionPolicy::Lru);
    store.configure_object_overflow(Some(runtime)).unwrap();

    store.set(b"alpha".to_vec(), vec![7u8; 32 * 1024], None);
    wait_for_file_count(&store, temp.path(), 1);
    wait_for_remote_entries(&store, 1);
    let remote_files = overflow_bin_files(temp.path());
    assert_eq!(remote_files.len(), 1, "expected one remote payload file");
    fs::remove_file(&remote_files[0]).expect("remove remote payload");

    assert!(store.try_get_value_bytes(b"alpha").is_err());
    assert!(store.try_entry_snapshot().is_err());
    assert!(store.visit_string_entries(|_, _, _| true).is_err());
}

#[test]
fn filesystem_listing_pages_only_the_requested_prefix() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config = file_overflow_config(temp.path(), "page-bucket", "page-node");
    let adapter = FileObjectOverflowStore::from_config(&config).expect("filesystem adapter");
    for index in 0..5 {
        adapter
            .put_value(
                &format!("overflow/page-node/generation/shard-0/{index}.bin"),
                b"x",
            )
            .unwrap();
    }
    for index in 0..5 {
        adapter
            .put_value(&format!("unrelated/noise/{index}.bin"), b"x")
            .unwrap();
    }

    let first = adapter
        .list_keys_page_bounded_with_timeout(
            "overflow/page-node/generation/",
            None,
            2,
            16 * 1024,
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(first.keys.len(), 2);
    assert!(first.keys.iter().all(|key| key.starts_with("overflow/")));
    let second = adapter
        .list_keys_page_bounded_with_timeout(
            "overflow/page-node/generation/",
            first.next_after.as_deref(),
            2,
            16 * 1024,
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(second.keys.len(), 2);
    assert!(second.keys[0] > first.keys[1]);
}

#[test]
fn filesystem_bounded_read_rejects_oversized_payloads() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config = file_overflow_config(temp.path(), "bounded-bucket", "bounded-node");
    let adapter = FileObjectOverflowStore::from_config(&config).expect("filesystem adapter");
    adapter.put_value("bounded/value.bin", &[7; 1024]).unwrap();
    assert!(
        adapter
            .get_value_bounded_with_timeout("bounded/value.bin", 8, Duration::from_secs(1))
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn filesystem_backend_atomically_replaces_existing_objects() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config = file_overflow_config(temp.path(), "atomic-bucket", "atomic-node");
    let adapter = FileObjectOverflowStore::from_config(&config).expect("filesystem adapter");
    let key = "overflow/atomic-node/generation/_generation.json";

    adapter.put_value(key, b"old marker").unwrap();
    adapter.put_value(key, b"new marker").unwrap();

    assert_eq!(adapter.get_value(key).unwrap().as_ref(), b"new marker");
    assert!(collect_files(temp.path()).iter().all(|path| {
        !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(".shardcache-overflow-"))
    }));
}

#[cfg(unix)]
#[test]
fn filesystem_backend_never_follows_symlink_components() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().expect("tempdir");
    let outside = tempfile::tempdir().expect("outside tempdir");
    let config = file_overflow_config(temp.path(), "symlink-bucket", "symlink-node");
    let adapter = FileObjectOverflowStore::from_config(&config).expect("filesystem adapter");
    let bucket = temp.path().join("symlink-bucket");
    symlink(outside.path(), bucket.join("escape")).expect("create symlink");

    assert!(adapter.put_value("escape/value.bin", b"payload").is_err());
    assert!(!outside.path().join("value.bin").exists());
    assert!(adapter.get_value("escape/value.bin").is_err());
}

#[test]
fn filesystem_backend_retains_recent_hot_values() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut config = file_overflow_config(temp.path(), "bucket-hot", "node-hot");
    config.offload_min_idle_ticks = 1024;
    let runtime = ObjectOverflowRuntime::from_config(&config)
        .expect("runtime")
        .expect("enabled runtime");
    let store = EmbeddedStore::new(1);
    store.configure_memory_policy(Some(8), EvictionPolicy::Lru);
    store.configure_object_overflow(Some(runtime)).unwrap();

    store.set(b"alpha".to_vec(), vec![9u8; 32 * 1024], None);

    assert!(store.get_ref(b"alpha").is_some());
    assert!(
        overflow_bin_files(temp.path()).is_empty(),
        "recent hot value must stay resident"
    );
    let stats = store.shard_stats_snapshot();
    let overflow = &stats[0].object_overflow;
    assert_eq!(overflow.remote_entries, 0);
    assert_eq!(overflow.offload_attempts, 0);
    assert!(overflow.offload_hot_skips >= 1);
}

#[test]
fn filesystem_cleanup_removes_only_stale_generations_for_same_node() {
    let temp = tempfile::tempdir().expect("tempdir");
    let bucket_root = temp.path().join("bucket");
    let stale = bucket_root.join("overflow/node-c/stale-generation");
    let other_node = bucket_root.join("overflow/node-d/stale-generation");
    fs::create_dir_all(stale.join("shard-0")).expect("stale generation dir");
    fs::create_dir_all(other_node.join("shard-0")).expect("other node dir");
    fs::write(
        stale.join("_generation.json"),
        br#"{"node_id":"node-c","generation_id":"stale-generation","created_ms":1,"heartbeat_ms":1}"#,
    )
    .expect("stale marker");
    fs::write(stale.join("shard-0/dead.bin"), b"stale payload").expect("stale payload");
    fs::write(
        other_node.join("_generation.json"),
        br#"{"node_id":"node-d","generation_id":"stale-generation","created_ms":1,"heartbeat_ms":1}"#,
    )
    .expect("other marker");
    fs::write(other_node.join("shard-0/live.bin"), b"other payload").expect("other payload");

    let mut config = file_overflow_config(temp.path(), "bucket", "node-c");
    config.cleanup_on_start = true;
    let _runtime = ObjectOverflowRuntime::from_config(&config)
        .expect("runtime")
        .expect("enabled runtime");

    assert!(!stale.join("_generation.json").exists());
    assert!(!stale.join("shard-0/dead.bin").exists());
    assert!(other_node.join("_generation.json").exists());
    assert!(other_node.join("shard-0/live.bin").exists());
    assert!(
        collect_files(&bucket_root.join("overflow/node-c"))
            .iter()
            .any(|path| path.ends_with("_generation.json")),
        "current generation marker should remain"
    );
}

#[cfg(unix)]
#[test]
fn filesystem_cleanup_rejects_symbolic_links() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().expect("tempdir");
    let outside = tempfile::tempdir().expect("outside tempdir");
    fs::write(outside.path().join("secret"), b"must not be listed").expect("outside file");
    let config = file_overflow_config(temp.path(), "symlink-bucket", "node-link");
    let store = FileObjectOverflowStore::from_config(&config).expect("filesystem store");
    let bucket = temp.path().join("symlink-bucket");
    symlink(outside.path(), bucket.join("escape")).expect("create symlink");

    let error = store
        .list_keys("")
        .expect_err("cleanup listing must reject symlinks");
    assert!(error.to_string().contains("symbolic link"));
    assert!(outside.path().join("secret").exists());
}

#[cfg(feature = "object-overflow-s3")]
#[test]
#[ignore = "requires SHARDKV_OBJECT_OVERFLOW_S3_ENDPOINT and SHARDKV_OBJECT_OVERFLOW_S3_BUCKET"]
fn s3_backend_smoke_offloads_faults_and_deletes_remote_values() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let Some(endpoint) = std::env::var("SHARDKV_OBJECT_OVERFLOW_S3_ENDPOINT").ok() else {
        eprintln!("skipping: SHARDKV_OBJECT_OVERFLOW_S3_ENDPOINT is not set");
        return;
    };
    let Some(bucket) = std::env::var("SHARDKV_OBJECT_OVERFLOW_S3_BUCKET").ok() else {
        eprintln!("skipping: SHARDKV_OBJECT_OVERFLOW_S3_BUCKET is not set");
        return;
    };
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    if std::env::var_os("SHARDKV_OBJECT_OVERFLOW_S3_ACCESS_KEY").is_none()
        || std::env::var_os("SHARDKV_OBJECT_OVERFLOW_S3_SECRET_KEY").is_none()
    {
        eprintln!("skipping: S3 access and secret key variables are not set");
        return;
    }
    let config = ObjectOverflowConfig {
        enabled: true,
        backend: ObjectOverflowBackend::S3,
        endpoint,
        bucket,
        prefix: format!("shardcache-smoke/{unique}"),
        node_id: Some(format!("smoke-node-{unique}")),
        region: std::env::var("SHARDKV_OBJECT_OVERFLOW_S3_REGION")
            .unwrap_or_else(|_| "us-east-1".to_string()),
        force_path_style: env_flag("SHARDKV_OBJECT_OVERFLOW_S3_FORCE_PATH_STYLE", true),
        allow_http: env_flag("SHARDKV_OBJECT_OVERFLOW_S3_ALLOW_HTTP", false),
        tls_verify: env_flag("SHARDKV_OBJECT_OVERFLOW_S3_TLS_VERIFY", true),
        tls_ca_path: std::env::var_os("SHARDKV_OBJECT_OVERFLOW_S3_TLS_CA_PATH").map(PathBuf::from),
        server_side_encryption: std::env::var("SHARDKV_OBJECT_OVERFLOW_S3_SSE").ok(),
        access_key_env: Some("SHARDKV_OBJECT_OVERFLOW_S3_ACCESS_KEY".to_string()),
        secret_key_env: Some("SHARDKV_OBJECT_OVERFLOW_S3_SECRET_KEY".to_string()),
        min_value_bytes: 4,
        offload_min_idle_ticks: 0,
        compression: ObjectOverflowCompression::Zstd,
        zstd_level: 1,
        failure_policy: ObjectOverflowFailurePolicy::RetainResident,
        max_retries: 1,
        retry_backoff_ms: 10,
        operation_timeout_ms: 5_000,
        worker_threads: 2,
        queue_capacity: 64,
        cleanup_on_start: true,
        cleanup_interval_seconds: 0,
        cleanup_grace_seconds: 60,
        ..ObjectOverflowConfig::default()
    };
    let runtime = ObjectOverflowRuntime::from_config(&config)
        .expect("runtime")
        .expect("enabled runtime");
    let store = EmbeddedStore::new(1);
    store.configure_memory_policy(Some(8), EvictionPolicy::Lru);
    store.configure_object_overflow(Some(runtime)).unwrap();

    const VALUE_COUNT: usize = 32;
    let value = vec![3u8; 16 * 1024];
    let governance = b"tenant-a/repo-private";
    for index in 0..VALUE_COUNT {
        let key = format!("s3-alpha-{index}");
        store.set_value_bytes_with_governance(
            key.as_bytes(),
            value.clone().into(),
            None,
            governance.as_slice().into(),
        );
    }

    wait_for_remote_entries(&store, VALUE_COUNT);
    let stats = store.shard_stats_snapshot();
    assert_eq!(stats[0].object_overflow.remote_entries, VALUE_COUNT);
    assert_eq!(
        stats[0].object_overflow.offload_successes,
        VALUE_COUNT as u64
    );
    assert_eq!(store.get(b"s3-alpha-0"), None);
    assert_eq!(
        store.get_value_bytes_with_governance_filter(b"s3-alpha-0", |_| false),
        None
    );
    assert_eq!(
        store
            .get_value_bytes_with_governance_filter(b"s3-alpha-0", |metadata| {
                metadata == Some(governance.as_slice())
            })
            .as_deref(),
        Some(value.as_slice())
    );
    assert_eq!(store.get(b"s3-alpha-0"), None);

    for index in 0..VALUE_COUNT {
        assert!(store.delete(format!("s3-alpha-{index}").as_bytes()));
    }
    wait_for_remote_entries(&store, 0);
    let stats = store.shard_stats_snapshot();
    assert_eq!(stats[0].object_overflow.remote_delete_failures, 0);
}

#[cfg(feature = "object-overflow-s3")]
#[test]
#[ignore = "requires a live S3/RustFS endpoint and deliberately invalid credentials"]
fn s3_backend_classifies_invalid_credentials_as_auth_configuration() {
    let Some(endpoint) = std::env::var("SHARDKV_OBJECT_OVERFLOW_S3_ENDPOINT").ok() else {
        eprintln!("skipping: SHARDKV_OBJECT_OVERFLOW_S3_ENDPOINT is not set");
        return;
    };
    let Some(bucket) = std::env::var("SHARDKV_OBJECT_OVERFLOW_S3_BUCKET").ok() else {
        eprintln!("skipping: SHARDKV_OBJECT_OVERFLOW_S3_BUCKET is not set");
        return;
    };
    if std::env::var_os("SHARDKV_OBJECT_OVERFLOW_S3_BAD_ACCESS_KEY").is_none()
        || std::env::var_os("SHARDKV_OBJECT_OVERFLOW_S3_BAD_SECRET_KEY").is_none()
    {
        eprintln!("skipping: deliberately invalid S3 credentials are not set");
        return;
    }
    let config = ObjectOverflowConfig {
        enabled: true,
        backend: ObjectOverflowBackend::S3,
        endpoint,
        bucket,
        prefix: "shardcache-invalid-auth".to_string(),
        node_id: Some("invalid-auth-node".to_string()),
        region: std::env::var("SHARDKV_OBJECT_OVERFLOW_S3_REGION")
            .unwrap_or_else(|_| "us-east-1".to_string()),
        force_path_style: env_flag("SHARDKV_OBJECT_OVERFLOW_S3_FORCE_PATH_STYLE", true),
        allow_http: env_flag("SHARDKV_OBJECT_OVERFLOW_S3_ALLOW_HTTP", false),
        tls_verify: true,
        tls_ca_path: std::env::var_os("SHARDKV_OBJECT_OVERFLOW_S3_TLS_CA_PATH").map(PathBuf::from),
        access_key_env: Some("SHARDKV_OBJECT_OVERFLOW_S3_BAD_ACCESS_KEY".to_string()),
        secret_key_env: Some("SHARDKV_OBJECT_OVERFLOW_S3_BAD_SECRET_KEY".to_string()),
        max_retries: 3,
        retry_backoff_ms: 10,
        operation_timeout_ms: 5_000,
        worker_threads: 1,
        queue_capacity: 8,
        ..ObjectOverflowConfig::default()
    };

    let error = ObjectOverflowRuntime::from_config(&config)
        .expect_err("invalid S3 credentials must reject startup");
    assert!(matches!(error, shardmap::ShardCacheError::Config(_)));
}

#[cfg(feature = "object-overflow-s3")]
fn env_flag(name: &str, default: bool) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}
