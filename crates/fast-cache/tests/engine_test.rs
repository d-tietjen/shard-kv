mod common;

use fast_cache::protocol::Frame;

use common::{TestHarness, command, distinct_keys_for_shards};

#[tokio::test(flavor = "multi_thread")]
async fn basic_commands_work() {
    let harness = TestHarness::new(false);

    let ping = harness.engine.execute(command(&[b"PING"])).await.unwrap();
    assert_eq!(ping, Frame::SimpleString("PONG".into()));

    let set = harness
        .engine
        .execute(command(&[b"SET", b"alpha", b"one"]))
        .await
        .unwrap();
    assert_eq!(set, Frame::SimpleString("OK".into()));

    let get = harness
        .engine
        .execute(command(&[b"GET", b"alpha"]))
        .await
        .unwrap();
    assert_eq!(get, Frame::BlobString(b"one".to_vec()));

    let exists = harness
        .engine
        .execute(command(&[b"EXISTS", b"alpha", b"missing"]))
        .await
        .unwrap();
    assert_eq!(exists, Frame::Integer(1));

    let expire = harness
        .engine
        .execute(command(&[b"EXPIRE", b"alpha", b"1"]))
        .await
        .unwrap();
    assert_eq!(expire, Frame::Integer(1));

    let ttl = harness
        .engine
        .execute(command(&[b"TTL", b"alpha"]))
        .await
        .unwrap();
    match ttl {
        Frame::Integer(value) => assert!(value >= 0),
        other => panic!("unexpected ttl frame: {other:?}"),
    }

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let expired = harness
        .engine
        .execute(command(&[b"GET", b"alpha"]))
        .await
        .unwrap();
    assert_eq!(expired, Frame::Null);

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mset_and_mget_preserve_order_across_shards() {
    let harness = TestHarness::new(false);
    let keys = distinct_keys_for_shards(harness.config.shard_count);

    let mut owned_parts = vec![b"MSET".to_vec()];
    for (index, key) in keys.iter().enumerate() {
        owned_parts.push(key.clone());
        owned_parts.push(format!("value-{index}").into_bytes());
    }
    let parts = owned_parts.iter().map(Vec::as_slice).collect::<Vec<_>>();

    let set = harness.engine.execute(command(&parts)).await.unwrap();
    assert_eq!(set, Frame::SimpleString("OK".into()));

    let mut owned_get_parts = vec![b"MGET".to_vec()];
    for key in &keys {
        owned_get_parts.push(key.clone());
    }
    let get_parts = owned_get_parts
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>();

    let values = harness.engine.execute(command(&get_parts)).await.unwrap();
    match values {
        Frame::Array(items) => {
            assert_eq!(items.len(), keys.len());
            for (index, item) in items.into_iter().enumerate() {
                assert_eq!(
                    item,
                    Frame::BlobString(format!("value-{index}").into_bytes())
                );
            }
        }
        other => panic!("unexpected mget response: {other:?}"),
    }

    let stats = harness.engine.execute(command(&[b"STATS"])).await.unwrap();
    match stats {
        Frame::BlobString(body) => {
            let text = String::from_utf8(body).unwrap();
            assert!(text.contains("\"shard_count\""));
        }
        other => panic!("unexpected stats response: {other:?}"),
    }

    harness.shutdown().await;
}
