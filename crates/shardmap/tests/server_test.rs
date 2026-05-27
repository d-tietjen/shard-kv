mod common;

use shardmap::protocol::{FastCodec, FastCommand, FastRequest, FastResponse, Frame};
use shardmap::storage::hash_key;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use common::{distinct_keys_for_shards, send_command, start_server, test_config};

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
        let read = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut chunk))
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

fn fast_request<'a>(command: FastCommand<'a>, key: Option<&'a [u8]>) -> FastRequest<'a> {
    FastRequest {
        key_hash: key.map(hash_key),
        route_shard: None,
        key_tag: None,
        command,
    }
}

fn resp_map_value<'a>(items: &'a [(Frame, Frame)], key: &[u8]) -> Option<&'a Frame> {
    items.iter().find_map(|(item_key, value)| match item_key {
        Frame::BlobString(bytes) if bytes == key => Some(value),
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_server_handles_resp_commands() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let mut config = test_config(temp_dir.path().join("server-data"), true);
    config.bind_addr = format!("127.0.0.1:{}", common::free_port());

    let (shutdown_tx, join) = start_server(config.clone()).await;
    let mut stream = tokio::net::TcpStream::connect(&config.bind_addr)
        .await
        .unwrap();

    let pong = send_command(&mut stream, &[b"PING"]).await;
    assert_eq!(pong, Frame::SimpleString("PONG".into()));

    assert_eq!(
        send_command(&mut stream, &[b"AUTH", b"unused"]).await,
        Frame::SimpleString("OK".into())
    );
    assert_eq!(
        send_command(&mut stream, &[b"SELECT", b"0"]).await,
        Frame::SimpleString("OK".into())
    );
    assert_eq!(
        send_command(&mut stream, &[b"ECHO", b"hello"]).await,
        Frame::BlobString(b"hello".to_vec())
    );
    assert_eq!(
        send_command(&mut stream, &[b"COMMAND"]).await,
        Frame::Array(vec![])
    );
    assert_eq!(
        send_command(&mut stream, &[b"COMMAND", b"DOCS"]).await,
        Frame::Array(vec![])
    );
    assert_eq!(
        send_command(&mut stream, &[b"CONFIG", b"GET", b"*"]).await,
        Frame::Array(vec![])
    );
    assert_eq!(
        send_command(&mut stream, &[b"CLIENT", b"GETNAME"]).await,
        Frame::Null
    );
    assert_eq!(
        send_command(&mut stream, &[b"CLIENT", b"SETNAME", b"bench"]).await,
        Frame::SimpleString("OK".into())
    );
    assert_eq!(
        send_command(&mut stream, &[b"CLIENT", b"ID"]).await,
        Frame::Integer(0)
    );
    assert_eq!(
        send_command(&mut stream, &[b"CLIENT", b"LIST"]).await,
        Frame::BlobString(Vec::new())
    );
    assert_eq!(
        send_command(&mut stream, &[b"CLIENT", b"KILL", b"ID", b"0"]).await,
        Frame::Integer(0)
    );
    match send_command(&mut stream, &[b"HELLO", b"3"]).await {
        Frame::Map(items) => {
            assert_eq!(items.len(), 7);
            assert_eq!(resp_map_value(&items, b"proto"), Some(&Frame::Integer(3)));
        }
        other => panic!("unexpected HELLO response: {other:?}"),
    }
    match send_command(&mut stream, &[b"TIME"]).await {
        Frame::Array(items) => assert_eq!(items.len(), 2),
        other => panic!("unexpected TIME response: {other:?}"),
    }
    match send_command(&mut stream, &[b"INFO"]).await {
        Frame::BlobString(body) => {
            let text = String::from_utf8(body).unwrap();
            assert!(text.contains("# Server"));
            assert!(text.contains("# Keyspace"));
        }
        other => panic!("unexpected INFO response: {other:?}"),
    }

    let set = send_command(&mut stream, &[b"SET", b"alpha", b"one"]).await;
    assert_eq!(set, Frame::SimpleString("OK".into()));

    let get = send_command(&mut stream, &[b"GET", b"alpha"]).await;
    assert_eq!(get, Frame::BlobString(b"one".to_vec()));

    assert_eq!(
        send_command(&mut stream, &[b"DBSIZE"]).await,
        Frame::Integer(1)
    );

    let keys = distinct_keys_for_shards(config.shard_count);
    let mut mset_parts = vec![b"MSET".as_slice()];
    let value_one = b"multi-one".as_slice();
    let value_two = b"multi-two".as_slice();
    mset_parts.push(keys[0].as_slice());
    mset_parts.push(value_one);
    mset_parts.push(keys[1].as_slice());
    mset_parts.push(value_two);
    let mset = send_command(&mut stream, &mset_parts).await;
    assert_eq!(mset, Frame::SimpleString("OK".into()));

    let mget_parts = [
        b"MGET".as_slice(),
        keys[1].as_slice(),
        b"missing-multi-key".as_slice(),
        keys[0].as_slice(),
        keys[1].as_slice(),
    ];
    let mget = send_command(&mut stream, &mget_parts).await;
    assert_eq!(
        mget,
        Frame::Array(vec![
            Frame::BlobString(value_two.to_vec()),
            Frame::Null,
            Frame::BlobString(value_one.to_vec()),
            Frame::BlobString(value_two.to_vec()),
        ])
    );

    let stats = send_command(&mut stream, &[b"STATS"]).await;
    match stats {
        Frame::BlobString(body) => {
            let text = String::from_utf8(body).unwrap();
            assert!(text.contains("\"total_keys\""));
        }
        other => panic!("unexpected stats frame: {other:?}"),
    }

    shutdown_tx.send(()).unwrap();
    join.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_server_handles_spanned_multikey_commands() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let mut config = test_config(temp_dir.path().join("spanned-server-data"), false);
    config.bind_addr = format!("127.0.0.1:{}", common::free_port());
    config.persistence.enabled = false;

    let (shutdown_tx, join) = start_server(config.clone()).await;
    let mut stream = tokio::net::TcpStream::connect(&config.bind_addr)
        .await
        .unwrap();

    let key_one = b"oversized-multikey-command-key-one".to_vec();
    let key_two = b"oversized-multikey-command-key-two".to_vec();
    let missing = b"oversized-multikey-command-key-missing".to_vec();
    let large_value = vec![b'x'; 4096];
    let small_value = b"small-value".to_vec();

    let mset_parts = [
        b"MSET".as_slice(),
        key_one.as_slice(),
        large_value.as_slice(),
        key_two.as_slice(),
        small_value.as_slice(),
    ];
    let mset = send_command(&mut stream, &mset_parts).await;
    assert_eq!(mset, Frame::SimpleString("OK".into()));

    let mget_parts = [
        b"MGET".as_slice(),
        key_two.as_slice(),
        missing.as_slice(),
        key_one.as_slice(),
    ];
    let mget = send_command(&mut stream, &mget_parts).await;
    assert_eq!(
        mget,
        Frame::Array(vec![
            Frame::BlobString(small_value),
            Frame::Null,
            Frame::BlobString(large_value),
        ])
    );

    shutdown_tx.send(()).unwrap();
    join.await.unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn tcp_direct_server_handles_resp_commands() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut config = test_config(temp_dir.path().join("direct-server-data"), false);
            config.bind_addr = format!("127.0.0.1:{}", common::free_port());
            config.shard_count = 1;
            config.persistence.enabled = false;

            let server = shardmap::server::ShardCacheServer::direct(config.clone());
            let join = tokio::task::spawn_local(async move { server.run().await });

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if tokio::net::TcpStream::connect(&config.bind_addr)
                    .await
                    .is_ok()
                {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("direct server did not start listening in time");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            let mut stream = tokio::net::TcpStream::connect(&config.bind_addr)
                .await
                .unwrap();

            let pong = send_command(&mut stream, &[b"PING"]).await;
            assert_eq!(pong, Frame::SimpleString("PONG".into()));

            let set = send_command(&mut stream, &[b"SET", b"alpha", b"one"]).await;
            assert_eq!(set, Frame::SimpleString("OK".into()));

            let get = send_command(&mut stream, &[b"GET", b"alpha"]).await;
            assert_eq!(get, Frame::BlobString(b"one".to_vec()));

            let ttl = send_command(&mut stream, &[b"TTL", b"alpha"]).await;
            assert_eq!(ttl, Frame::Integer(-1));

            let stats = send_command(&mut stream, &[b"STATS"]).await;
            match stats {
                Frame::BlobString(body) => {
                    let text = String::from_utf8(body).unwrap();
                    assert!(text.contains("\"total_keys\""));
                    assert!(text.contains("\"shard_count\": 1"));
                }
                other => panic!("unexpected stats frame: {other:?}"),
            }

            join.abort();
            let _ = join.await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn tcp_direct_server_handles_redis_structure_commands() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut config = test_config(temp_dir.path().join("direct-objects-data"), false);
            config.bind_addr = format!("127.0.0.1:{}", common::free_port());
            config.shard_count = 4;
            config.persistence.enabled = false;

            let server = shardmap::server::ShardCacheServer::direct(config.clone());
            let join = tokio::task::spawn_local(async move { server.run().await });

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if tokio::net::TcpStream::connect(&config.bind_addr)
                    .await
                    .is_ok()
                {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("direct server did not start listening in time");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            let mut stream = tokio::net::TcpStream::connect(&config.bind_addr)
                .await
                .unwrap();

            assert_eq!(
                send_command(&mut stream, &[b"HSET", b"hash", b"name", b"ada"]).await,
                Frame::Integer(1)
            );
            assert_eq!(
                send_command(&mut stream, &[b"HGET", b"hash", b"name"]).await,
                Frame::BlobString(b"ada".to_vec())
            );
            assert_eq!(
                send_command(&mut stream, &[b"GET", b"hash"]).await,
                Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into()
                )
            );

            assert_eq!(
                send_command(&mut stream, &[b"LPUSH", b"list", b"a", b"b"]).await,
                Frame::Integer(2)
            );
            assert_eq!(
                send_command(&mut stream, &[b"LRANGE", b"list", b"0", b"-1"]).await,
                Frame::Array(vec![
                    Frame::BlobString(b"b".to_vec()),
                    Frame::BlobString(b"a".to_vec()),
                ])
            );

            assert_eq!(
                send_command(&mut stream, &[b"SADD", b"set", b"x", b"y"]).await,
                Frame::Integer(2)
            );
            assert_eq!(
                send_command(&mut stream, &[b"SISMEMBER", b"set", b"x"]).await,
                Frame::Integer(1)
            );

            assert_eq!(
                send_command(&mut stream, &[b"ZADD", b"zset", b"2", b"b"]).await,
                Frame::Integer(1)
            );
            assert_eq!(
                send_command(&mut stream, &[b"ZADD", b"zset", b"1", b"a"]).await,
                Frame::Integer(1)
            );
            assert_eq!(
                send_command(&mut stream, &[b"ZRANGE", b"zset", b"0", b"-1"]).await,
                Frame::Array(vec![
                    Frame::BlobString(b"a".to_vec()),
                    Frame::BlobString(b"b".to_vec()),
                ])
            );
            assert_eq!(
                send_command(&mut stream, &[b"ZADD", b"zset", b"0", b"c"]).await,
                Frame::Integer(1)
            );
            assert_eq!(
                send_command(&mut stream, &[b"ZADD", b"zset", b"3", b"d"]).await,
                Frame::Integer(1)
            );
            assert_eq!(
                send_command(&mut stream, &[b"ZADD", b"zset", b"4", b"e"]).await,
                Frame::Integer(1)
            );
            assert_eq!(
                send_command(&mut stream, &[b"ZRANGE", b"zset", b"0", b"-1"]).await,
                Frame::Array(vec![
                    Frame::BlobString(b"c".to_vec()),
                    Frame::BlobString(b"a".to_vec()),
                    Frame::BlobString(b"b".to_vec()),
                    Frame::BlobString(b"d".to_vec()),
                    Frame::BlobString(b"e".to_vec()),
                ])
            );
            assert_eq!(
                send_command(&mut stream, &[b"ZADD", b"zset", b"-1", b"e"]).await,
                Frame::Integer(0)
            );
            assert_eq!(
                send_command(&mut stream, &[b"ZRANGE", b"zset", b"0", b"-1"]).await,
                Frame::Array(vec![
                    Frame::BlobString(b"e".to_vec()),
                    Frame::BlobString(b"c".to_vec()),
                    Frame::BlobString(b"a".to_vec()),
                    Frame::BlobString(b"b".to_vec()),
                    Frame::BlobString(b"d".to_vec()),
                ])
            );

            assert_eq!(
                send_command(&mut stream, &[b"SET", b"hash", b"string"]).await,
                Frame::SimpleString("OK".into())
            );
            assert_eq!(
                send_command(&mut stream, &[b"HGET", b"hash", b"name"]).await,
                Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into()
                )
            );

            join.abort();
            let _ = join.await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn tcp_direct_server_handles_native_redis_structure_commands() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut config = test_config(temp_dir.path().join("direct-native-objects-data"), false);
            config.bind_addr = format!("127.0.0.1:{}", common::free_port());
            config.shard_count = 4;
            config.persistence.enabled = false;

            let server = shardmap::server::ShardCacheServer::direct(config.clone());
            let join = tokio::task::spawn_local(async move { server.run().await });

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            loop {
                if tokio::net::TcpStream::connect(&config.bind_addr)
                    .await
                    .is_ok()
                {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("direct server did not start listening in time");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            let mut stream = tokio::net::TcpStream::connect(&config.bind_addr)
                .await
                .unwrap();

            assert_eq!(
                send_fast_command(&mut stream, fast_request(FastCommand::Auth, None)).await,
                FastResponse::Ok
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(FastCommand::Hello { proto: Some(2) }, None),
                )
                .await,
                FastResponse::Array(vec![
                    Some(b"server".to_vec()),
                    Some(b"shardcache".to_vec()),
                    Some(b"version".to_vec()),
                    Some(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
                    Some(b"proto".to_vec()),
                    Some(b"2".to_vec()),
                    Some(b"id".to_vec()),
                    Some(b"0".to_vec()),
                    Some(b"mode".to_vec()),
                    Some(b"standalone".to_vec()),
                    Some(b"role".to_vec()),
                    Some(b"master".to_vec()),
                    Some(b"modules".to_vec()),
                    Some(b"[]".to_vec()),
                ])
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(FastCommand::Echo { payload: b"hello" }, None),
                )
                .await,
                FastResponse::Value(b"hello".to_vec())
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::ConfigGet {
                            patterns: vec![b"*".as_slice()],
                        },
                        None,
                    ),
                )
                .await,
                FastResponse::Array(vec![])
            );
            assert_eq!(
                send_fast_command(&mut stream, fast_request(FastCommand::DbSize, None)).await,
                FastResponse::Integer(0)
            );
            match send_fast_command(&mut stream, fast_request(FastCommand::Time, None)).await {
                FastResponse::Array(items) => assert_eq!(items.len(), 2),
                other => panic!("unexpected TIME response: {other:?}"),
            }

            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::MSet {
                            items: vec![(b"k1", b"one"), (b"k2", b"two")]
                        },
                        None,
                    ),
                )
                .await,
                FastResponse::Ok
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::MGet {
                            keys: vec![b"k2".as_slice(), b"missing".as_slice(), b"k1".as_slice()]
                        },
                        None,
                    ),
                )
                .await,
                FastResponse::Array(vec![Some(b"two".to_vec()), None, Some(b"one".to_vec())])
            );

            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::HSet {
                            key: b"hash",
                            field: b"name",
                            value: b"ada",
                        },
                        Some(b"hash"),
                    ),
                )
                .await,
                FastResponse::Integer(1)
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::HGet {
                            key: b"hash",
                            field: b"name",
                        },
                        Some(b"hash"),
                    ),
                )
                .await,
                FastResponse::Value(b"ada".to_vec())
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::HMGet {
                            key: b"hash",
                            fields: vec![b"name".as_slice(), b"email".as_slice()],
                        },
                        Some(b"hash"),
                    ),
                )
                .await,
                FastResponse::Array(vec![Some(b"ada".to_vec()), None])
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(FastCommand::Get { key: b"hash" }, Some(b"hash")),
                )
                .await,
                FastResponse::Error(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec()
                )
            );

            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::LPush {
                            key: b"list",
                            values: vec![b"a".as_slice(), b"b".as_slice()],
                        },
                        Some(b"list"),
                    ),
                )
                .await,
                FastResponse::Integer(2)
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::LRange {
                            key: b"list",
                            start: 0,
                            stop: -1,
                        },
                        Some(b"list"),
                    ),
                )
                .await,
                FastResponse::Array(vec![Some(b"b".to_vec()), Some(b"a".to_vec())])
            );

            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::ZAdd {
                            key: b"zset",
                            score: 2.5,
                            member: b"b",
                        },
                        Some(b"zset"),
                    ),
                )
                .await,
                FastResponse::Integer(1)
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::ZScore {
                            key: b"zset",
                            member: b"b",
                        },
                        Some(b"zset"),
                    ),
                )
                .await,
                FastResponse::Float(2.5)
            );
            for (score, member) in [
                (1.0, b"a".as_slice()),
                (0.0, b"c".as_slice()),
                (3.0, b"d".as_slice()),
                (4.0, b"e".as_slice()),
            ] {
                assert_eq!(
                    send_fast_command(
                        &mut stream,
                        fast_request(
                            FastCommand::ZAdd {
                                key: b"zset",
                                score,
                                member,
                            },
                            Some(b"zset"),
                        ),
                    )
                    .await,
                    FastResponse::Integer(1)
                );
            }
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::ZAdd {
                            key: b"zset",
                            score: -1.0,
                            member: b"e",
                        },
                        Some(b"zset"),
                    ),
                )
                .await,
                FastResponse::Integer(0)
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::ZRange {
                            key: b"zset",
                            start: 0,
                            stop: -1,
                        },
                        Some(b"zset"),
                    ),
                )
                .await,
                FastResponse::Array(vec![
                    Some(b"e".to_vec()),
                    Some(b"c".to_vec()),
                    Some(b"a".to_vec()),
                    Some(b"b".to_vec()),
                    Some(b"d".to_vec()),
                ])
            );

            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::Set {
                            key: b"hash",
                            value: b"string",
                        },
                        Some(b"hash"),
                    ),
                )
                .await,
                FastResponse::Ok
            );
            assert_eq!(
                send_fast_command(
                    &mut stream,
                    fast_request(
                        FastCommand::HGet {
                            key: b"hash",
                            field: b"name",
                        },
                        Some(b"hash"),
                    ),
                )
                .await,
                FastResponse::Error(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec()
                )
            );

            join.abort();
            let _ = join.await;
        })
        .await;
}
