#![allow(dead_code)]

use std::path::PathBuf;
use std::time::Duration;

use shardmap::config::{PersistenceConfig, ShardCacheConfig, TierConfig};
use shardmap::protocol::{Frame, RespCodec};
use shardmap::server::ShardCacheServer;
use shardmap::storage::{Command, EngineHandle, hash_key, shift_for, stripe_index};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

pub struct TestHarness {
    pub temp_dir: TempDir,
    pub config: ShardCacheConfig,
    pub engine: EngineHandle,
}

impl TestHarness {
    pub fn new(persistence: bool) -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        let mut config = test_config(temp_dir.path().to_path_buf(), persistence);
        config.bind_addr = format!("127.0.0.1:{}", free_port());
        let engine = EngineHandle::open(config.clone()).expect("engine");
        Self {
            temp_dir,
            config,
            engine,
        }
    }

    pub async fn shutdown(self) {
        self.engine.shutdown().await.expect("shutdown");
    }
}

pub fn test_config(data_dir: PathBuf, persistence: bool) -> ShardCacheConfig {
    ShardCacheConfig {
        bind_addr: "127.0.0.1:6380".into(),
        max_connections: 64,
        shard_count: 4,
        ttl_sweep_interval_ms: 10,
        stats_interval_ms: 100,
        tiers: TierConfig {
            hot_capacity: 8,
            warm_capacity: 64,
            cold_capacity: 512,
            promotion_batch: 128,
        },
        persistence: PersistenceConfig {
            enabled: persistence,
            data_dir,
            snapshot_every_seconds: 1,
            snapshot_min_writes: 1,
            segment_size_bytes: 4 * 1024,
            ..Default::default()
        },
        ..Default::default()
    }
}

pub fn command(parts: &[&[u8]]) -> Command {
    let frame = Frame::Array(
        parts
            .iter()
            .map(|part| Frame::BlobString(part.to_vec()))
            .collect(),
    );
    Command::from_frame(frame).expect("command")
}

pub async fn send_command(stream: &mut TcpStream, parts: &[&[u8]]) -> Frame {
    let frame = Frame::Array(
        parts
            .iter()
            .map(|part| Frame::BlobString(part.to_vec()))
            .collect(),
    );
    let mut bytes = Vec::new();
    RespCodec::encode(&frame, &mut bytes);
    stream.write_all(&bytes).await.expect("write command");
    read_frame(stream).await
}

pub async fn read_frame(stream: &mut TcpStream) -> Frame {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk))
            .await
            .expect("timeout")
            .expect("read");
        assert!(read > 0, "server closed connection");
        buffer.extend_from_slice(&chunk[..read]);
        if let Some((frame, _)) = RespCodec::decode(&buffer).expect("decode frame") {
            return frame;
        }
    }
}

pub fn distinct_keys_for_shards(shard_count: usize) -> Vec<Vec<u8>> {
    let shift = shift_for(shard_count);
    let mut keys = vec![Vec::new(); shard_count];
    let mut filled = 0usize;
    for index in 0..10_000usize {
        let key = format!("shard-key-{index}").into_bytes();
        let shard = stripe_index(hash_key(&key), shift);
        if keys[shard].is_empty() {
            keys[shard] = key;
            filled += 1;
            if filled == shard_count {
                break;
            }
        }
    }
    keys
}

pub async fn start_server(
    config: ShardCacheConfig,
) -> (
    oneshot::Sender<()>,
    tokio::task::JoinHandle<shardmap::Result<()>>,
) {
    let engine = EngineHandle::open(config.clone()).expect("engine");
    let server = ShardCacheServer::new(config.clone(), engine);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        server
            .run_with_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if TcpStream::connect(&config.bind_addr).await.is_ok() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("server did not start listening in time");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    (shutdown_tx, join)
}

pub fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("local addr").port()
}
