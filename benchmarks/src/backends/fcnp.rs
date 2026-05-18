//! Native fast-cache protocol (FCNP v2) benchmark clients.

use fcnp_client_rs::{
    FcnpClient, FcnpDirectClient, FcnpDirectRouter, FcnpDirectShardClient, FcnpRouteMode,
};

use crate::backend::{Backend, BackendClass, BoxError, Worker};

pub struct FcnpBackend {
    id: &'static str,
    addr: String,
    mode: FcnpMode,
}

#[derive(Debug, Clone, Copy)]
enum FcnpMode {
    Generic,
    DirectShards {
        router: FcnpDirectRouter,
        route_mode: FcnpRouteMode,
    },
}

impl FcnpBackend {
    pub fn new(addr: &str) -> Result<Self, BoxError> {
        let _probe = FcnpClient::connect(addr)?;
        Ok(Self {
            id: "fc-server-fcnp",
            addr: addr.to_string(),
            mode: FcnpMode::Generic,
        })
    }

    pub fn new_direct_shards(addr: &str, shard_count: usize) -> Result<Self, BoxError> {
        let route_mode = std::env::var("FCNP_DIRECT_ROUTE_MODE")
            .ok()
            .map(|value| FcnpRouteMode::parse(&value))
            .transpose()?
            .unwrap_or(FcnpRouteMode::FullKey);
        let router = FcnpDirectRouter::new(addr, shard_count)?.with_route_mode(route_mode);
        let _probe = router.connect_shard(0)?;
        Ok(Self {
            id: "fc-server-fcnp-direct",
            addr: addr.to_string(),
            mode: FcnpMode::DirectShards { router, route_mode },
        })
    }
}

impl Backend for FcnpBackend {
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
            FcnpMode::Generic => {
                let mut client = FcnpClient::connect(self.addr.as_str())?;
                for key in keys {
                    client.set(key, value)?;
                }
            }
            FcnpMode::DirectShards { router, route_mode } => {
                let mut client = FcnpDirectClient::connect_with_route_mode(
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
            FcnpMode::Generic => Ok(Box::new(FcnpWorker::Generic {
                client: FcnpClient::connect(self.addr.as_str())?,
            })),
            FcnpMode::DirectShards { router, .. } => {
                let shard_id = worker_index % router.shard_count();
                Ok(Box::new(FcnpWorker::DirectShard {
                    client: router.connect_shard(shard_id)?,
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
        let FcnpMode::DirectShards { router, .. } = self.mode else {
            return Ok(None);
        };
        let shard_id = worker_index % router.shard_count();
        let indices = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| {
                (router.route_key(key).shard_id == shard_id).then_some(index)
            })
            .collect::<Vec<_>>();
        Ok(Some(indices))
    }
}

enum FcnpWorker {
    Generic { client: FcnpClient },
    DirectShard { client: FcnpDirectShardClient },
}

impl Worker for FcnpWorker {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        Ok(match self {
            Self::Generic { client } => client.get_into(key, scratch)?,
            Self::DirectShard { client } => client.get_into(key, scratch)?,
        })
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.set(key, value)?,
            Self::DirectShard { client } => client.set(key, value)?,
        }
        Ok(())
    }

    fn begin_pipeline_get(&mut self, key: &[u8]) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.begin_pipeline_get(key)?,
            Self::DirectShard { client } => client.begin_pipeline_get(key)?,
        }
        Ok(())
    }

    fn begin_pipeline_set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.begin_pipeline_set(key, value)?,
            Self::DirectShard { client } => client.begin_pipeline_set(key, value)?,
        }
        Ok(())
    }

    fn flush_pipeline(&mut self) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.flush_pipeline()?,
            Self::DirectShard { client } => client.flush_pipeline()?,
        }
        Ok(())
    }

    fn finish_pipeline_get(&mut self, scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        Ok(match self {
            Self::Generic { client } => client.finish_pipeline_get_into(scratch)?,
            Self::DirectShard { client } => client.finish_pipeline_get_into(scratch)?,
        })
    }

    fn finish_pipeline_set(&mut self) -> Result<(), BoxError> {
        match self {
            Self::Generic { client } => client.finish_pipeline_set()?,
            Self::DirectShard { client } => client.finish_pipeline_set()?,
        }
        Ok(())
    }
}
