//! Exposes a caller-owned shardmap embedded store over RESP/SCNP for benchmark runs.
//!
//! This intentionally uses `ShardCacheServer::from_embedded_store` instead of
//! the source-only `shardcache` CLI so benchmark rows exercise the embedded
//! database-as-server API.

use std::sync::Arc;

use clap::Parser;
use shardmap::config::{EvictionPolicy, PersistenceConfig, ServerEndpointMode, ShardCacheConfig};
use shardmap::server::{ServerRuntime, ShardCacheServer};
use shardmap::storage::{EmbeddedRouteMode, EmbeddedStore};

#[derive(Debug, Parser)]
#[command(about = "Benchmark harness for serving a caller-owned shardmap EmbeddedStore")]
struct Args {
    /// Server bind address, e.g. 127.0.0.1:6383.
    #[arg(long, default_value = "127.0.0.1:6383")]
    bind_addr: String,

    /// Number of embedded storage shards.
    #[arg(long, default_value_t = 16)]
    shard_count: usize,

    /// Maximum accepted client connections.
    #[arg(long, default_value_t = 4096)]
    max_connections: usize,

    /// Total memory budget in bytes. 0 disables memory-limit eviction.
    #[arg(long, default_value_t = 0)]
    max_memory_bytes: u64,

    /// Route s:<session>:c:<chunk> keys by session prefix.
    #[arg(long)]
    session_prefix_routing: bool,

    /// Expose shard-owned direct ports in addition to the owner-routed fanout endpoint.
    #[arg(long)]
    direct_shard_ports: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    ServerRuntime::initialize_tracing();
    let args = Args::parse();

    let server_endpoint_mode = match args.direct_shard_ports {
        true => ServerEndpointMode::DirectShard,
        false => ServerEndpointMode::Fanout,
    };
    let config = ShardCacheConfig {
        bind_addr: args.bind_addr,
        shard_count: args.shard_count,
        max_connections: args.max_connections,
        max_memory_bytes: args.max_memory_bytes,
        eviction_policy: if args.max_memory_bytes > 0 {
            EvictionPolicy::Lru
        } else {
            EvictionPolicy::None
        },
        persistence: PersistenceConfig {
            enabled: false,
            ..PersistenceConfig::default()
        },
        server_endpoint_mode,
        ..ShardCacheConfig::default()
    };
    config.validate()?;

    let route_mode = match args.session_prefix_routing {
        true => EmbeddedRouteMode::SessionPrefix,
        false => EmbeddedRouteMode::FullKey,
    };
    let store = Arc::new(EmbeddedStore::with_route_mode(args.shard_count, route_mode));
    if args.max_memory_bytes > 0 {
        let per_shard = args
            .max_memory_bytes
            .checked_div(args.shard_count as u64)
            .and_then(|bytes| usize::try_from(bytes).ok());
        store.configure_memory_policy(per_shard, EvictionPolicy::Lru);
    }

    let runtime_threads = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        .clamp(2, 8);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_threads)
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        ShardCacheServer::from_embedded_store(config, store)
            .run()
            .await
    })?;
    Ok(())
}
