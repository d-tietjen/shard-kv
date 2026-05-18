use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use tokio::runtime::Builder;

use fast_cache::FastCacheError;
use fast_cache::config::{EvictionPolicy, FastCacheConfig};
use fast_cache::server::{FastCacheServer, ServerMode, ServerRuntime};

#[derive(Debug, Parser)]
#[command(author, version, about = "Run the fast-cache server")]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long)]
    bind_addr: Option<String>,

    #[arg(long)]
    unix_socket: Option<PathBuf>,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long)]
    shard_count: Option<usize>,

    #[arg(long)]
    max_connections: Option<usize>,

    #[arg(long)]
    max_memory_bytes: Option<u64>,

    #[arg(long, value_enum)]
    eviction_policy: Option<CliEvictionPolicy>,

    #[arg(long)]
    disable_persistence: bool,

    #[arg(long)]
    snapshot_every_seconds: Option<u64>,

    #[arg(long, value_enum, default_value_t = CliServerMode::Auto)]
    server_mode: CliServerMode,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliServerMode {
    Auto,
    Engine,
    Direct,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliEvictionPolicy {
    None,
    Lru,
    Lfu,
}

impl From<CliEvictionPolicy> for EvictionPolicy {
    fn from(value: CliEvictionPolicy) -> Self {
        match value {
            CliEvictionPolicy::None => EvictionPolicy::None,
            CliEvictionPolicy::Lru => EvictionPolicy::Lru,
            CliEvictionPolicy::Lfu => EvictionPolicy::Lfu,
        }
    }
}

impl From<CliServerMode> for ServerMode {
    fn from(value: CliServerMode) -> Self {
        match value {
            CliServerMode::Auto => ServerMode::Auto,
            CliServerMode::Engine => ServerMode::Engine,
            CliServerMode::Direct => ServerMode::Direct,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    ServerRuntime::initialize_tracing();
    let cli = Cli::parse();

    let mut config = if let Some(path) = &cli.config {
        FastCacheConfig::load_from_path(path)?
    } else {
        FastCacheConfig::default()
    };

    if let Some(bind_addr) = cli.bind_addr {
        config.bind_addr = bind_addr;
    }
    if let Some(data_dir) = cli.data_dir {
        config.persistence.data_dir = data_dir;
    }
    if let Some(shard_count) = cli.shard_count {
        config.shard_count = shard_count;
    }
    if let Some(max_connections) = cli.max_connections {
        config.max_connections = max_connections;
    }
    if let Some(max_memory_bytes) = cli.max_memory_bytes {
        config.max_memory_bytes = max_memory_bytes;
    }
    if let Some(eviction_policy) = cli.eviction_policy {
        config.eviction_policy = eviction_policy.into();
    }
    if cli.disable_persistence {
        config.persistence.enabled = false;
    }
    if let Some(snapshot_every_seconds) = cli.snapshot_every_seconds {
        config.persistence.snapshot_every_seconds = snapshot_every_seconds;
    }

    config.validate()?;

    let server_mode = ServerMode::from(cli.server_mode);
    let runtime_threads = std::thread::available_parallelism()
        .map(|count| count.get().clamp(2, 4))
        .unwrap_or(2);
    let runtime = Builder::new_multi_thread()
        .worker_threads(runtime_threads)
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        if server_mode == ServerMode::Direct {
            if config.persistence.enabled {
                return Err(FastCacheError::Config(
                    "direct server mode requires --disable_persistence".into(),
                )
                .into());
            }
            if cli.unix_socket.is_some() {
                return Err(FastCacheError::Config(
                    "direct server mode (multi-direct) does not support --unix-socket".into(),
                )
                .into());
            }
            let mut server = FastCacheServer::direct(config);
            if let Some(path) = cli.unix_socket {
                server = server.with_unix_socket(path);
            }
            server.run().await?;
            return Ok(());
        }

        let engine = fast_cache::storage::EngineHandle::open(config.clone())?;
        let mut server = FastCacheServer::with_mode(config, engine, server_mode);
        if let Some(path) = cli.unix_socket {
            server = server.with_unix_socket(path);
        }
        server.run().await?;
        Ok(())
    })
}
