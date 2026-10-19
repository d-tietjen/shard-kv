//! Native shardcache protocol (SCNP v2) benchmark clients.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use shardcache_client_rs::{
    ShardCacheClient, ShardCacheDirectClient, ShardCacheDirectRouter, ShardCacheDirectShardClient,
    ShardCacheRoute, ShardCacheRouteMode,
};

use crate::backend::{Backend, BackendClass, BoxError, Worker};

type PreparedScnpKeys = Arc<Vec<PreparedScnpKey>>;
type PreparedScnpWorkers = Mutex<BTreeMap<usize, PreparedScnpKeys>>;

pub struct ScnpBackend {
    id: &'static str,
    addr: String,
    mode: ScnpMode,
    precompute_routes: bool,
    prepared: Arc<PreparedScnpWorkers>,
}

#[derive(Debug, Clone, Copy)]
enum ScnpMode {
    Generic,
    DirectShards {
        router: ShardCacheDirectRouter,
        route_mode: ShardCacheRouteMode,
    },
}

impl ScnpBackend {
    pub fn new(addr: &str) -> Result<Self, BoxError> {
        let _probe = ShardCacheClient::connect(addr)?;
        Ok(Self {
            id: "fc-server-scnp",
            addr: addr.to_string(),
            mode: ScnpMode::Generic,
            precompute_routes: false,
            prepared: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn new_direct_shards(addr: &str, shard_count: usize) -> Result<Self, BoxError> {
        let route_mode = std::env::var("SCNP_DIRECT_ROUTE_MODE")
            .ok()
            .map(|value| ShardCacheRouteMode::parse(&value))
            .transpose()?
            .unwrap_or(ShardCacheRouteMode::FullKey);
        let router = ShardCacheDirectRouter::new(addr, shard_count)?.with_route_mode(route_mode);
        let _probe = router.connect_shard(0)?;
        Ok(Self {
            id: "fc-server-scnp-direct",
            addr: addr.to_string(),
            mode: ScnpMode::DirectShards { router, route_mode },
            precompute_routes: false,
            prepared: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn new_direct_shards_precomputed(addr: &str, shard_count: usize) -> Result<Self, BoxError> {
        let mut backend = Self::new_direct_shards(addr, shard_count)?;
        backend.id = "fc-server-scnp-direct-precomputed";
        backend.precompute_routes = true;
        Ok(backend)
    }
}

struct PreparedScnpKey {
    key: Vec<u8>,
    route: ShardCacheRoute,
}

impl Backend for ScnpBackend {
    fn id(&self) -> &str {
        self.id
    }

    fn class(&self) -> BackendClass {
        BackendClass::Networked
    }

    fn supports_pipelining(&self) -> bool {
        true
    }

    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        match self.mode {
            ScnpMode::Generic => {
                let mut client = ShardCacheClient::connect(self.addr.as_str())?;
                for key in keys {
                    client.set(key, value)?;
                }
            }
            ScnpMode::DirectShards { router, route_mode } => {
                let mut client = ShardCacheDirectClient::connect_with_route_mode(
                    self.addr.as_str(),
                    router.shard_count(),
                    route_mode,
                )?;
                for key in keys {
                    client.set(key, value)?;
                }
            }
        }
        Ok(())
    }

    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        self.new_worker_for(0, 1)
    }

    fn new_worker_for(
        &self,
        worker_index: usize,
        worker_count: usize,
    ) -> Result<Box<dyn Worker>, BoxError> {
        match self.mode {
            ScnpMode::Generic => Ok(Box::new(ScnpWorker::Generic {
                client: ShardCacheClient::connect(self.addr.as_str())?,
            })),
            ScnpMode::DirectShards { router, .. } => {
                let shard_id = worker_index % router.shard_count();
                let prepared = if self.precompute_routes {
                    if worker_index >= worker_count {
                        return Err("SCNP prepared worker index is outside worker count".into());
                    }
                    self.prepared
                        .lock()
                        .map_err(|_| "SCNP prepared-key cache lock poisoned")?
                        .get(&worker_index)
                        .cloned()
                        .ok_or("prepare SCNP worker key indices before constructing its worker")?
                } else {
                    empty_prepared_scnp_keys()
                };
                Ok(Box::new(ScnpWorker::DirectShard {
                    client: router.connect_shard(shard_id)?,
                    prepared,
                    indexed: self.precompute_routes,
                }))
            }
        }
    }

    fn worker_key_indices(
        &self,
        keys: &[Vec<u8>],
        worker_index: usize,
        worker_count: usize,
    ) -> Result<Option<Vec<usize>>, BoxError> {
        let ScnpMode::DirectShards { router, .. } = self.mode else {
            return Ok(None);
        };
        if self.precompute_routes && worker_index >= worker_count {
            return Err("SCNP prepared worker index is outside worker count".into());
        }
        let shard_id = worker_index % router.shard_count();
        let mut indices = Vec::new();
        let mut prepared = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let route = router.route_key(key);
            if route.shard_id == shard_id {
                indices.push(index);
                if self.precompute_routes {
                    prepared.push(PreparedScnpKey {
                        key: key.clone(),
                        route,
                    });
                }
            }
        }
        if self.precompute_routes {
            let mut slots = self
                .prepared
                .lock()
                .map_err(|_| "SCNP prepared-key cache lock poisoned")?;
            slots.insert(worker_index, Arc::new(prepared));
        }
        Ok(Some(indices))
    }
}

enum ScnpWorker {
    Generic {
        client: ShardCacheClient,
    },
    DirectShard {
        client: ShardCacheDirectShardClient,
        prepared: PreparedScnpKeys,
        indexed: bool,
    },
}

impl Worker for ScnpWorker {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        Ok(match self {
            Self::Generic { client } => client.get_into(key, scratch)?,
            Self::DirectShard { client, .. } => client.get_into(key, scratch)?,
        })
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.set(key, value)?,
            Self::DirectShard { client, .. } => client.set(key, value)?,
        }
        Ok(())
    }

    fn begin_pipeline_get(&mut self, key: &[u8]) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.begin_pipeline_get(key)?,
            Self::DirectShard { client, .. } => client.begin_pipeline_get(key)?,
        }
        Ok(())
    }

    fn begin_pipeline_set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.begin_pipeline_set(key, value)?,
            Self::DirectShard { client, .. } => client.begin_pipeline_set(key, value)?,
        }
        Ok(())
    }

    fn begin_pipeline_get_index(&mut self, local_index: usize) -> Result<(), BoxError> {
        let Self::DirectShard {
            client, prepared, ..
        } = self
        else {
            return Err("generic SCNP worker does not support indexed pipelined GET".into());
        };
        let prepared = prepared
            .get(local_index)
            .ok_or("SCNP prepared key index out of range")?;
        client.begin_pipeline_get_routed(prepared.route, &prepared.key)?;
        Ok(())
    }

    fn begin_pipeline_set_index(
        &mut self,
        local_index: usize,
        value: &[u8],
    ) -> Result<(), BoxError> {
        let Self::DirectShard {
            client, prepared, ..
        } = self
        else {
            return Err("generic SCNP worker does not support indexed pipelined SET".into());
        };
        let prepared = prepared
            .get(local_index)
            .ok_or("SCNP prepared key index out of range")?;
        client.begin_pipeline_set_routed(prepared.route, &prepared.key, value)?;
        Ok(())
    }

    fn flush_pipeline(&mut self) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.flush_pipeline()?,
            Self::DirectShard { client, .. } => client.flush_pipeline()?,
        }
        Ok(())
    }

    fn finish_pipeline_get(&mut self, scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        Ok(match self {
            Self::Generic { client } => client.finish_pipeline_get_into(scratch)?,
            Self::DirectShard { client, .. } => client.finish_pipeline_get_into(scratch)?,
        })
    }

    fn finish_pipeline_set(&mut self) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.finish_pipeline_set()?,
            Self::DirectShard { client, .. } => client.finish_pipeline_set()?,
        }
        Ok(())
    }

    fn supports_indexed_keys(&self) -> bool {
        match self {
            Self::DirectShard { indexed, .. } => *indexed,
            Self::Generic { .. } => false,
        }
    }

    fn get_index(&mut self, local_index: usize, scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        let Self::DirectShard {
            client, prepared, ..
        } = self
        else {
            return Err("generic SCNP worker does not support indexed GET".into());
        };
        let prepared = prepared
            .get(local_index)
            .ok_or("SCNP prepared key index out of range")?;
        Ok(client.get_routed_into(prepared.route, &prepared.key, scratch)?)
    }

    fn set_index(&mut self, local_index: usize, value: &[u8]) -> Result<(), BoxError> {
        let Self::DirectShard {
            client, prepared, ..
        } = self
        else {
            return Err("generic SCNP worker does not support indexed SET".into());
        };
        let prepared = prepared
            .get(local_index)
            .ok_or("SCNP prepared key index out of range")?;
        client.set_routed(prepared.route, &prepared.key, value)?;
        Ok(())
    }
}

fn empty_prepared_scnp_keys() -> Arc<Vec<PreparedScnpKey>> {
    Arc::new(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardmap::protocol::{FastCodec, FastCommand, FastResponse};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    #[test]
    fn ordinary_and_precomputed_workers_preserve_request_metadata_and_pipeline_order() {
        for precomputed in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let keys = vec![b"first\0".to_vec(), b"second\xff".to_vec()];
            let peer_keys = keys.clone();
            let peer = std::thread::spawn(move || {
                let (probe, _) = listener.accept().unwrap();
                drop(probe);
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                for step in 0..4 {
                    let mut header = [0u8; 8];
                    stream.read_exact(&mut header).unwrap();
                    let body_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
                    assert!(body_len < 4096);
                    let mut bytes = header.to_vec();
                    bytes.resize(8 + body_len, 0);
                    stream.read_exact(&mut bytes[8..]).unwrap();
                    let (request, consumed) = FastCodec::decode_request(&bytes).unwrap().unwrap();
                    assert_eq!(consumed, bytes.len());
                    let index = if step < 2 { 1 } else { 0 };
                    let expected_key = peer_keys[index].as_slice();
                    let expected_route = ShardCacheDirectRouter::new("127.0.0.1:1", 1)
                        .unwrap()
                        .route_key(expected_key);
                    assert_eq!(request.key_hash, Some(expected_route.key_hash));
                    assert_eq!(request.key_tag, Some(expected_route.key_tag));
                    assert_eq!(request.route_shard, Some(0));
                    let response = match (step % 2, request.command) {
                        (0, FastCommand::Set { key, value }) => {
                            assert_eq!(key, expected_key);
                            assert_eq!(value, b"value\0\xff");
                            FastResponse::Ok
                        }
                        (1, FastCommand::Get { key }) => {
                            assert_eq!(key, expected_key);
                            FastResponse::Value(b"value\0\xff".to_vec())
                        }
                        (_, command) => panic!("unexpected request: {command:?}"),
                    };
                    let mut response_bytes = Vec::new();
                    FastCodec::encode_response(&response, &mut response_bytes);
                    stream.write_all(&response_bytes).unwrap();
                }
            });
            let backend = if precomputed {
                ScnpBackend::new_direct_shards_precomputed(&address, 1).unwrap()
            } else {
                ScnpBackend::new_direct_shards(&address, 1).unwrap()
            };
            assert_eq!(
                backend.id(),
                if precomputed {
                    "fc-server-scnp-direct-precomputed"
                } else {
                    "fc-server-scnp-direct"
                }
            );
            if precomputed {
                assert!(backend.new_worker_for(0, 1).is_err());
                assert!(backend.worker_key_indices(&keys, usize::MAX, 1).is_err());
                assert!(backend.new_worker_for(usize::MAX, 1).is_err());
            }
            assert_eq!(
                backend.worker_key_indices(&keys, 0, 1).unwrap(),
                Some(vec![0, 1])
            );
            let mut worker = backend.new_worker_for(0, 1).unwrap();
            assert_eq!(worker.supports_indexed_keys(), precomputed);
            let value = b"value\0\xff";
            let mut out = Vec::new();
            if precomputed {
                assert!(worker.begin_pipeline_get_index(2).is_err());
                worker.begin_pipeline_set_index(1, value).unwrap();
                worker.begin_pipeline_get_index(1).unwrap();
            } else {
                worker.begin_pipeline_set(&keys[1], value).unwrap();
                worker.begin_pipeline_get(&keys[1]).unwrap();
            }
            worker.flush_pipeline().unwrap();
            worker.finish_pipeline_set().unwrap();
            assert!(worker.finish_pipeline_get(&mut out).unwrap());
            assert_eq!(out, value);
            if precomputed {
                worker.set_index(0, value).unwrap();
                assert!(worker.get_index(0, &mut out).unwrap());
            } else {
                worker.set(&keys[0], value).unwrap();
                assert!(worker.get(&keys[0], &mut out).unwrap());
            }
            assert_eq!(out, value);
            peer.join().unwrap();
        }
    }
}
