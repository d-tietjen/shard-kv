use std::fs;
use std::path::{Path, PathBuf};

use shardmap::config::{
    EvictionPolicy, ObjectOverflowBackend, ObjectOverflowCompression, ObjectOverflowConfig,
    ObjectOverflowFailurePolicy,
};
use shardmap::storage::{EmbeddedStore, ObjectOverflowRuntime};

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

#[test]
fn filesystem_backend_offloads_materializes_snapshots_and_deletes_remote_files() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config = file_overflow_config(temp.path(), "bucket", "node-a");
    let runtime = ObjectOverflowRuntime::from_config(&config)
        .expect("runtime")
        .expect("enabled runtime");
    let store = EmbeddedStore::new(1);
    store.configure_memory_policy(Some(8), EvictionPolicy::Lru);
    store.configure_object_overflow(Some(runtime));

    let value = vec![42u8; 64 * 1024];
    store.set(b"alpha".to_vec(), value.clone(), None);

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
    assert!(
        overflow_bin_files(temp.path()).is_empty(),
        "delete should remove the remote payload"
    );
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
    store.configure_object_overflow(Some(runtime));

    store.set(b"alpha".to_vec(), vec![7u8; 32 * 1024], None);
    let remote_files = overflow_bin_files(temp.path());
    assert_eq!(remote_files.len(), 1, "expected one remote payload file");
    fs::remove_file(&remote_files[0]).expect("remove remote payload");

    assert!(store.try_entry_snapshot().is_err());
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
    store.configure_object_overflow(Some(runtime));

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
    let access_key_env = std::env::var_os("SHARDKV_OBJECT_OVERFLOW_S3_ACCESS_KEY")
        .is_some()
        .then(|| "SHARDKV_OBJECT_OVERFLOW_S3_ACCESS_KEY".to_string());
    let secret_key_env = std::env::var_os("SHARDKV_OBJECT_OVERFLOW_S3_SECRET_KEY")
        .is_some()
        .then(|| "SHARDKV_OBJECT_OVERFLOW_S3_SECRET_KEY".to_string());
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
        access_key_env,
        secret_key_env,
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
        cleanup_on_start: false,
        cleanup_interval_seconds: 0,
        cleanup_grace_seconds: 60,
        ..ObjectOverflowConfig::default()
    };
    let runtime = ObjectOverflowRuntime::from_config(&config)
        .expect("runtime")
        .expect("enabled runtime");
    let store = EmbeddedStore::new(1);
    store.configure_memory_policy(Some(8), EvictionPolicy::Lru);
    store.configure_object_overflow(Some(runtime));

    let value = vec![3u8; 16 * 1024];
    store.set(b"s3-alpha".to_vec(), value.clone(), None);

    let stats = store.shard_stats_snapshot();
    assert_eq!(stats[0].object_overflow.remote_entries, 1);
    assert_eq!(stats[0].object_overflow.offload_successes, 1);
    assert_eq!(store.get(b"s3-alpha"), Some(value));

    assert!(store.delete(b"s3-alpha"));
    let stats = store.shard_stats_snapshot();
    assert_eq!(stats[0].object_overflow.remote_delete_failures, 0);
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
