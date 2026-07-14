use std::env;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use shardmap::config::{EvictionPolicy, KvOverflowBackend, KvOverflowConfig};
use shardmap::storage::{EmbeddedStore, KvOverflowStore};
use tempfile::TempDir;

struct RedisServer {
    child: Child,
    _data_dir: TempDir,
    endpoint: String,
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn redis_backend_round_trips_binary_values_ttl_integrity_and_delete() {
    let Some(mut server) = start_redis_server() else {
        eprintln!(
            "skipping Redis overflow integration test: set SHARDCACHE_COMPAT_SERVER_BIN or install valkey-server/redis-server"
        );
        return;
    };
    let config = KvOverflowConfig {
        enabled: true,
        backend: KvOverflowBackend::Redis,
        endpoints: vec![server.endpoint.clone()],
        max_memory_bytes: 1,
        eviction_policy: EvictionPolicy::Lru,
        connections_per_endpoint: 2,
        worker_threads: 2,
        queue_capacity: 64,
        ..KvOverflowConfig::default()
    };
    let store = KvOverflowStore::from_config(EmbeddedStore::new(2), &config).unwrap();
    let key = b"binary:\0\xff:key".to_vec();
    let value = (0..=255).cycle().take(1024).collect::<Vec<u8>>();

    store.set(key.clone(), value.clone(), None).unwrap();
    store.flush_remote().unwrap();
    assert!(!store.inner().exists(&key));
    assert_eq!(
        store.get_remote(&key).unwrap().unwrap().value.as_ref(),
        value
    );

    let client = redis_client::Client::open(server.endpoint.as_str()).unwrap();
    let mut connection = client.get_connection().unwrap();
    let storage_key = redis_storage_key(&mut connection, &config, &key);
    let envelope: Option<Vec<u8>> = redis_client::cmd("GET")
        .arg(&storage_key)
        .query(&mut connection)
        .unwrap();
    assert!(envelope.is_some_and(|value| value.starts_with(b"SCKVOV01")));
    let unprefixed: Option<Vec<u8>> = redis_client::cmd("GET")
        .arg(&key)
        .query(&mut connection)
        .unwrap();
    assert!(unprefixed.is_none());

    let governed_key = b"governed:\0\xff:key".to_vec();
    let governed_value = b"private-model-state".repeat(64);
    let governance = b"tenant-a/repo-private".to_vec();
    store
        .set_with_governance(
            governed_key.clone(),
            governed_value.clone(),
            None,
            governance.clone(),
        )
        .unwrap();
    store.flush_remote().unwrap();
    assert_eq!(store.get(&governed_key).unwrap(), None);
    assert_eq!(store.get_remote(&governed_key).unwrap(), None);
    assert_eq!(
        store
            .get_remote_with_governance_filter(&governed_key, |_| false)
            .unwrap(),
        None
    );
    let governed = store
        .get_remote_with_governance_filter(&governed_key, |metadata| {
            metadata == Some(governance.as_slice())
        })
        .unwrap()
        .expect("authorized Redis overflow read");
    assert_eq!(governed.value.as_ref(), governed_value);
    assert_eq!(governed.governance.as_deref(), Some(governance.as_slice()));
    assert_eq!(
        store
            .get_with_governance_filter(&governed_key, |metadata| {
                metadata == Some(governance.as_slice())
            })
            .unwrap()
            .as_deref(),
        Some(governed_value.as_slice())
    );
    assert_eq!(store.get(&governed_key).unwrap(), None);

    let governed_storage_key = redis_storage_key(&mut connection, &config, &governed_key);
    let governed_envelope: Vec<u8> = redis_client::cmd("GET")
        .arg(&governed_storage_key)
        .query(&mut connection)
        .unwrap();
    assert!(
        governed_envelope.starts_with(b"SCKVOV03") || governed_envelope.starts_with(b"SCKVOV04")
    );

    let ttl_key = b"ttl-key".to_vec();
    store
        .set(ttl_key.clone(), b"expires".to_vec(), Some(200))
        .unwrap();
    store.flush_remote().unwrap();
    let ttl_storage_key = redis_storage_key(&mut connection, &config, &ttl_key);
    let redis_ttl: i64 = redis_client::cmd("PTTL")
        .arg(&ttl_storage_key)
        .query(&mut connection)
        .unwrap();
    assert!(redis_ttl > 0 && redis_ttl <= 200);
    std::thread::sleep(Duration::from_millis(250));
    assert!(store.get_remote(&ttl_key).unwrap().is_none());
    let exists: bool = redis_client::cmd("EXISTS")
        .arg(&ttl_storage_key)
        .query(&mut connection)
        .unwrap();
    assert!(!exists, "Redis must enforce overflow TTL server-side");

    let corrupt_key = b"corrupt";
    store
        .set(
            corrupt_key.to_vec(),
            b"valid-before-corruption".to_vec(),
            None,
        )
        .unwrap();
    store.flush_remote().unwrap();
    let corrupt_storage_key = redis_storage_key(&mut connection, &config, corrupt_key);
    redis_client::cmd("SET")
        .arg(corrupt_storage_key)
        .arg(b"not-an-overflow-envelope")
        .query::<()>(&mut connection)
        .unwrap();
    assert!(store.get_remote(corrupt_key).is_err());

    assert!(store.delete(&key).unwrap());
    let exists: bool = redis_client::cmd("EXISTS")
        .arg(&storage_key)
        .query(&mut connection)
        .unwrap();
    assert!(!exists);
    assert!(store.delete(&governed_key).unwrap());
    let exists: bool = redis_client::cmd("EXISTS")
        .arg(&governed_storage_key)
        .query(&mut connection)
        .unwrap();
    assert!(!exists);

    assert!(server.child.try_wait().unwrap().is_none());
}

fn redis_storage_key(
    connection: &mut redis_client::Connection,
    config: &KvOverflowConfig,
    key: &[u8],
) -> Vec<u8> {
    let mut pattern = config.redis_key_prefix.as_bytes().to_vec();
    pattern.push(b'*');
    let keys: Vec<Vec<u8>> = redis_client::cmd("KEYS")
        .arg(pattern)
        .query(connection)
        .unwrap();
    keys.into_iter()
        .find(|candidate| candidate.ends_with(key))
        .expect("structured Redis overflow key must retain the original key suffix")
}

fn start_redis_server() -> Option<RedisServer> {
    let explicit = env::var_os("SHARDCACHE_COMPAT_SERVER_BIN").map(PathBuf::from);
    let candidates = explicit
        .clone()
        .into_iter()
        .chain(find_on_path("valkey-server"))
        .chain(find_on_path("redis-server"))
        .chain(existing_path("/opt/homebrew/bin/valkey-server"))
        .chain(existing_path("/opt/homebrew/bin/redis-server"))
        .chain(existing_path("/usr/local/bin/valkey-server"))
        .chain(existing_path("/usr/local/bin/redis-server"));

    for binary in candidates {
        match spawn_redis_server(&binary) {
            Ok(server) => return Some(server),
            Err(error) if explicit.is_some() => {
                panic!(
                    "failed to start SHARDCACHE_COMPAT_SERVER_BIN={}: {error}",
                    binary.display()
                );
            }
            Err(error) => eprintln!("skipping Redis candidate {}: {error}", binary.display()),
        }
    }
    None
}

fn spawn_redis_server(binary: &Path) -> Result<RedisServer, String> {
    let data_dir = tempfile::tempdir().map_err(|error| error.to_string())?;
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(binary)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--protected-mode")
        .arg("no")
        .arg("--save")
        .arg("")
        .arg("--appendonly")
        .arg("no")
        .arg("--dir")
        .arg(data_dir.path())
        .arg("--loglevel")
        .arg("warning")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| error.to_string())?;
    let mut server = RedisServer {
        child,
        _data_dir: data_dir,
        endpoint: format!("redis://{addr}/0"),
    };
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return Ok(server);
        }
        if server.child.try_wait().ok().flatten().is_some() || Instant::now() >= deadline {
            return Err(format!("{} did not start listening", binary.display()));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free address").port()
}

fn find_on_path(name: &str) -> impl Iterator<Item = PathBuf> + '_ {
    env::var_os("PATH")
        .into_iter()
        .flat_map(|paths| env::split_paths(&paths).collect::<Vec<_>>())
        .map(move |path| path.join(name))
        .filter(|path| path.is_file())
}

fn existing_path(path: &str) -> impl Iterator<Item = PathBuf> {
    let path = PathBuf::from(path);
    path.is_file().then_some(path).into_iter()
}
