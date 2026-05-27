mod common;

use shardmap::protocol::Frame;
use shardmap::storage::EngineHandle;

use common::{command, test_config};

#[tokio::test(flavor = "multi_thread")]
async fn recovers_from_wal_after_restart() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let config = test_config(temp_dir.path().join("wal-data"), true);

    let engine = EngineHandle::open(config.clone()).unwrap();
    engine
        .execute(command(&[b"SET", b"alpha", b"one"]))
        .await
        .unwrap();
    engine
        .execute(command(&[b"SET", b"beta", b"two"]))
        .await
        .unwrap();
    engine.shutdown().await.unwrap();

    let recovered = EngineHandle::open(config.clone()).unwrap();
    let alpha = recovered
        .execute(command(&[b"GET", b"alpha"]))
        .await
        .unwrap();
    let beta = recovered
        .execute(command(&[b"GET", b"beta"]))
        .await
        .unwrap();
    assert_eq!(alpha, Frame::BlobString(b"one".to_vec()));
    assert_eq!(beta, Frame::BlobString(b"two".to_vec()));
    recovered.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_and_replay_restore_latest_state() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let config = test_config(temp_dir.path().join("snapshot-data"), true);

    let engine = EngineHandle::open(config.clone()).unwrap();
    engine
        .execute(command(&[b"SET", b"alpha", b"one"]))
        .await
        .unwrap();
    let save = engine.execute(command(&[b"SAVE"])).await.unwrap();
    match save {
        Frame::BlobString(path) => {
            let text = String::from_utf8(path).unwrap();
            assert!(text.contains("snapshot-"));
        }
        other => panic!("unexpected save response: {other:?}"),
    }
    engine
        .execute(command(&[b"SET", b"alpha", b"two"]))
        .await
        .unwrap();
    engine
        .execute(command(&[b"DEL", b"missing", b"noop"]))
        .await
        .unwrap();
    engine.shutdown().await.unwrap();

    let recovered = EngineHandle::open(config.clone()).unwrap();
    let alpha = recovered
        .execute(command(&[b"GET", b"alpha"]))
        .await
        .unwrap();
    assert_eq!(alpha, Frame::BlobString(b"two".to_vec()));
    recovered.shutdown().await.unwrap();
}
