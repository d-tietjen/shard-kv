//! Native shardcache protocol (SCNP v2) benchmark clients.

use std::sync::{Arc, Mutex};

use shardcache_client_rs::{
    ShardCacheClient, ShardCacheDirectClient, ShardCacheDirectRouter, ShardCacheDirectShardClient,
    ShardCacheRoute, ShardCacheRouteMode,
};

use crate::backend::{Backend, BackendClass, BoxError, Worker};

pub struct ScnpBackend {
    id: &'static str,
    addr: String,
    mode: ScnpMode,
    prepared: Arc<Mutex<Vec<Option<Arc<Vec<PreparedScnpKey>>>>>>,
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
            prepared: Arc::new(Mutex::new(Vec::new())),
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
            prepared: Arc::new(Mutex::new(Vec::new())),
        })
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
        _worker_count: usize,
    ) -> Result<Box<dyn Worker>, BoxError> {
        match self.mode {
            ScnpMode::Generic => Ok(Box::new(ScnpWorker::Generic {
                client: ShardCacheClient::connect(self.addr.as_str())?,
            })),
            ScnpMode::DirectShards { router, .. } => {
                let shard_id = worker_index % router.shard_count();
                let prepared = self
                    .prepared
                    .lock()
                    .map_err(|_| "SCNP prepared-key cache lock poisoned")?
                    .get(worker_index)
                    .and_then(Clone::clone)
                    .unwrap_or_else(empty_prepared_scnp_keys);
                Ok(Box::new(ScnpWorker::DirectShard {
                    client: router.connect_shard(shard_id)?,
                    prepared,
                }))
            }
        }
    }

    fn worker_key_indices(
        &self,
        keys: &[Vec<u8>],
        worker_index: usize,
        _worker_count: usize,
    ) -> Result<Option<Vec<usize>>, BoxError> {
        let ScnpMode::DirectShards { router, .. } = self.mode else {
            return Ok(None);
        };
        let shard_id = worker_index % router.shard_count();
        let mut indices = Vec::new();
        let mut prepared = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let route = router.route_key(key);
            if route.shard_id == shard_id {
                indices.push(index);
                prepared.push(PreparedScnpKey {
                    key: key.clone(),
                    route,
                });
            }
        }
        let mut slots = self
            .prepared
            .lock()
            .map_err(|_| "SCNP prepared-key cache lock poisoned")?;
        if slots.len() <= worker_index {
            slots.resize_with(worker_index + 1, || None);
        }
        slots[worker_index] = Some(Arc::new(prepared));
        Ok(Some(indices))
    }
}

enum ScnpWorker {
    Generic {
        client: ShardCacheClient,
    },
    DirectShard {
        client: ShardCacheDirectShardClient,
        prepared: Arc<Vec<PreparedScnpKey>>,
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
        let Self::DirectShard { client, prepared } = self else {
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
        let Self::DirectShard { client, prepared } = self else {
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
            Self::DirectShard { prepared, .. } => !prepared.is_empty(),
            Self::Generic { .. } => false,
        }
    }

    fn get_index(&mut self, local_index: usize, scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        let Self::DirectShard { client, prepared } = self else {
            return Err("generic SCNP worker does not support indexed GET".into());
        };
        let prepared = prepared
            .get(local_index)
            .ok_or("SCNP prepared key index out of range")?;
        Ok(client.get_routed_into(prepared.route, &prepared.key, scratch)?)
    }

    fn set_index(&mut self, local_index: usize, value: &[u8]) -> Result<(), BoxError> {
        let Self::DirectShard { client, prepared } = self else {
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
