use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(feature = "telemetry")]
use shardmap::storage::{CacheTelemetry, EmbeddedRouteMode};
use shardmap::storage::{EmbeddedKeyRoute, EmbeddedStore, LocalEmbeddedStore, PreparedPointKey};

use crate::backend::{Backend, BackendClass, BoxError, ReadMode, Worker};

use super::BenchmarkCacheConfig;

pub struct FcEmbed {
    id: &'static str,
    state: Arc<Mutex<FcEmbedState>>,
    ttl_enabled: AtomicBool,
    read_mode: ReadMode,
}

// Unlike shared-reference embedded backends such as DashMap, shardcache's
// embedded fast path is thread-local: each benchmark worker owns a
// LocalEmbeddedStore and only receives keys routed to its shard set.
struct FcEmbedState {
    store: Option<EmbeddedStore>,
    locals: Vec<Option<LocalEmbeddedStore>>,
    prepared_keys: Vec<Vec<PreparedPointKey>>,
    local_worker_count: Option<usize>,
}

impl FcEmbed {
    pub fn new(shard_count: usize, cache_config: BenchmarkCacheConfig) -> Self {
        let id = if cfg!(feature = "unsafe") {
            "fc-embed-unsafe"
        } else {
            "fc-embed"
        };
        Self::new_inner(id, shard_count, cache_config, false)
    }

    #[cfg(feature = "telemetry")]
    pub fn new_telemetry(shard_count: usize, cache_config: BenchmarkCacheConfig) -> Self {
        Self::new_inner("fc-embed-telemetry", shard_count, cache_config, true)
    }

    fn new_inner(
        id: &'static str,
        shard_count: usize,
        cache_config: BenchmarkCacheConfig,
        telemetry: bool,
    ) -> Self {
        let store = embedded_store(shard_count, telemetry);
        store.configure_memory_policy(
            cache_config.per_shard_memory_bytes(shard_count),
            cache_config.eviction_policy,
        );
        Self {
            id,
            state: Arc::new(Mutex::new(FcEmbedState {
                store: Some(store),
                locals: Vec::new(),
                prepared_keys: Vec::new(),
                local_worker_count: None,
            })),
            ttl_enabled: AtomicBool::new(false),
            read_mode: cache_config.read_mode,
        }
    }
}

impl Backend for FcEmbed {
    fn id(&self) -> &str {
        self.id
    }

    fn class(&self) -> BackendClass {
        BackendClass::Embedded
    }

    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        self.ttl_enabled.store(false, Ordering::Relaxed);
        let mut state = lock_state(&self.state)?;
        for k in keys {
            let route = route_key(&state, k)?;
            if state.local_worker_count.is_some() {
                set_local(&mut state, route, k, value, None)?;
            } else {
                state
                    .store
                    .as_ref()
                    .expect("fc-embed shared store exists before local split")
                    .set_slice_routed_no_ttl(route, k, value);
            }
        }
        Ok(())
    }

    fn warmup_ttl(&self, keys: &[Vec<u8>], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        self.ttl_enabled.store(true, Ordering::Relaxed);
        let mut state = lock_state(&self.state)?;
        for k in keys {
            let route = route_key(&state, k)?;
            if state.local_worker_count.is_some() {
                set_local(&mut state, route, k, value, Some(ttl_ms))?;
            } else {
                state
                    .store
                    .as_ref()
                    .expect("fc-embed shared store exists before local split")
                    .set_slice_prehashed(route.key_hash, k, value, Some(ttl_ms));
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
        let mut state = lock_state(&self.state)?;
        ensure_local_stores(&mut state, worker_count)?;
        let store = state
            .locals
            .get_mut(worker_index)
            .and_then(Option::take)
            .ok_or_else(|| {
                format!("fc-embed local store for worker {worker_index} is unavailable")
            })?;
        let prepared_keys = state
            .prepared_keys
            .get_mut(worker_index)
            .map(std::mem::take)
            .unwrap_or_default();
        Ok(Box::new(FcEmbedWorker {
            state: Arc::clone(&self.state),
            worker_index,
            store: Some(store),
            prepared_keys,
            ttl_enabled: self.ttl_enabled.load(Ordering::Relaxed),
            read_mode: self.read_mode,
        }))
    }

    fn worker_key_indices(
        &self,
        keys: &[Vec<u8>],
        worker_index: usize,
        worker_count: usize,
    ) -> Result<Option<Vec<usize>>, BoxError> {
        let mut state = lock_state(&self.state)?;
        if let Some(local_worker_count) = state.local_worker_count
            && local_worker_count != worker_count
        {
            return Err(format!(
                "fc-embed local stores were split for {local_worker_count} workers, not {worker_count}"
            )
            .into());
        }
        if worker_count == 0 {
            return Err("fc-embed worker_count must be greater than zero".into());
        }
        if state.prepared_keys.len() != worker_count {
            state.prepared_keys = (0..worker_count).map(|_| Vec::new()).collect();
        }

        let mut indices = Vec::new();
        let mut prepared_keys = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let prepared = prepare_key(&state, key)?;
            if prepared.route().shard_id % worker_count == worker_index {
                indices.push(index);
                prepared_keys.push(prepared);
            }
        }
        state.prepared_keys[worker_index] = prepared_keys;
        Ok(Some(indices))
    }
}

fn embedded_store(shard_count: usize, telemetry: bool) -> EmbeddedStore {
    #[cfg(feature = "telemetry")]
    {
        if telemetry {
            return EmbeddedStore::with_route_mode_and_metrics(
                shard_count,
                EmbeddedRouteMode::FullKey,
                Some(CacheTelemetry::new(shard_count)),
            );
        }
    }
    let _ = telemetry;
    EmbeddedStore::new(shard_count)
}

struct FcEmbedWorker {
    state: Arc<Mutex<FcEmbedState>>,
    worker_index: usize,
    store: Option<LocalEmbeddedStore>,
    prepared_keys: Vec<PreparedPointKey>,
    ttl_enabled: bool,
    read_mode: ReadMode,
}

impl Drop for FcEmbedWorker {
    fn drop(&mut self) {
        let Some(store) = self.store.take() else {
            return;
        };
        if let Ok(mut state) = self.state.lock()
            && let Some(slot) = state.locals.get_mut(self.worker_index)
        {
            debug_assert!(slot.is_none());
            *slot = Some(store);
        }
    }
}

impl Worker for FcEmbedWorker {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        let store = self
            .store
            .as_mut()
            .ok_or("fc-embed worker local store was already returned")?;
        let route = store.route_key(key);
        let value = match self.ttl_enabled {
            true => store.get_ref_routed_local(route, key),
            false => store.get_point_ref_routed_no_ttl_local(route, key),
        };
        Ok(record_read(value, scratch, self.read_mode))
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        let store = self
            .store
            .as_mut()
            .ok_or("fc-embed worker local store was already returned")?;
        let route = store.route_key(key);
        store.set_slice_routed_no_ttl_local(route, key, value);
        Ok(())
    }

    fn set_ttl(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        let store = self
            .store
            .as_mut()
            .ok_or("fc-embed worker local store was already returned")?;
        let route = store.route_key(key);
        store.set_slice_routed_local(route, key, value, Some(ttl_ms));
        Ok(())
    }

    fn supports_indexed_keys(&self) -> bool {
        !self.prepared_keys.is_empty()
    }

    fn get_index(&mut self, local_index: usize, scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        let prepared = self
            .prepared_keys
            .get(local_index)
            .ok_or("fc-embed prepared key index out of range")?;
        let store = self
            .store
            .as_mut()
            .ok_or("fc-embed worker local store was already returned")?;
        let value = match self.ttl_enabled {
            true => store.get_ref_routed_local(prepared.route(), prepared.key()),
            false => store.get_prepared_point_ref_no_ttl_local(prepared),
        };
        Ok(record_read(value, scratch, self.read_mode))
    }

    fn set_index(&mut self, local_index: usize, value: &[u8]) -> Result<(), BoxError> {
        let prepared = self
            .prepared_keys
            .get(local_index)
            .ok_or("fc-embed prepared key index out of range")?;
        let store = self
            .store
            .as_mut()
            .ok_or("fc-embed worker local store was already returned")?;
        store.set_prepared_point_slice_no_ttl_local(prepared, value);
        Ok(())
    }

    fn set_index_ttl(
        &mut self,
        local_index: usize,
        value: &[u8],
        ttl_ms: u64,
    ) -> Result<(), BoxError> {
        let prepared = self
            .prepared_keys
            .get(local_index)
            .ok_or("fc-embed prepared key index out of range")?;
        let store = self
            .store
            .as_mut()
            .ok_or("fc-embed worker local store was already returned")?;
        store.set_slice_routed_local(prepared.route(), prepared.key(), value, Some(ttl_ms));
        Ok(())
    }
}

#[inline(always)]
fn record_read(value: Option<&[u8]>, scratch: &mut Vec<u8>, read_mode: ReadMode) -> bool {
    let Some(value) = value else {
        return false;
    };
    match read_mode {
        ReadMode::Ref => {
            black_box(value.len());
        }
        ReadMode::Copy => {
            scratch.clear();
            scratch.extend_from_slice(value);
        }
    }
    true
}

fn lock_state(
    state: &Mutex<FcEmbedState>,
) -> Result<std::sync::MutexGuard<'_, FcEmbedState>, BoxError> {
    state
        .lock()
        .map_err(|_| "fc-embed state mutex poisoned".into())
}

fn ensure_local_stores(state: &mut FcEmbedState, worker_count: usize) -> Result<(), BoxError> {
    if worker_count == 0 {
        return Err("fc-embed worker_count must be greater than zero".into());
    }
    if let Some(local_worker_count) = state.local_worker_count {
        if local_worker_count == worker_count {
            return Ok(());
        }
        return Err(format!(
            "fc-embed local stores were split for {local_worker_count} workers, not {worker_count}"
        )
        .into());
    }

    let store = state
        .store
        .take()
        .ok_or("fc-embed shared store is unavailable")?;
    state.locals = store
        .into_local_stores(worker_count)
        .into_iter()
        .map(Some)
        .collect();
    if state.prepared_keys.len() != worker_count {
        state.prepared_keys = (0..worker_count).map(|_| Vec::new()).collect();
    }
    state.local_worker_count = Some(worker_count);
    Ok(())
}

fn route_key(state: &FcEmbedState, key: &[u8]) -> Result<EmbeddedKeyRoute, BoxError> {
    if let Some(store) = state.store.as_ref() {
        return Ok(store.route_key(key));
    }
    state
        .locals
        .iter()
        .find_map(Option::as_ref)
        .map(|store| store.route_key(key))
        .ok_or_else(|| "fc-embed has no store available for routing".into())
}

fn prepare_key(state: &FcEmbedState, key: &[u8]) -> Result<PreparedPointKey, BoxError> {
    if let Some(store) = state.store.as_ref() {
        return Ok(store.prepare_point_key(key));
    }
    state
        .locals
        .iter()
        .find_map(Option::as_ref)
        .map(|store| store.prepare_point_key(key))
        .ok_or_else(|| "fc-embed has no store available for key preparation".into())
}

fn set_local(
    state: &mut FcEmbedState,
    route: EmbeddedKeyRoute,
    key: &[u8],
    value: &[u8],
    ttl_ms: Option<u64>,
) -> Result<(), BoxError> {
    let worker_count = state
        .local_worker_count
        .ok_or("fc-embed local stores have not been initialized")?;
    let worker_index = route.shard_id % worker_count;
    let store = state
        .locals
        .get_mut(worker_index)
        .and_then(Option::as_mut)
        .ok_or_else(|| format!("fc-embed local store for worker {worker_index} is checked out"))?;
    match ttl_ms {
        Some(ttl_ms) => store.set_slice_routed_local(route, key, value, Some(ttl_ms)),
        None => store.set_slice_routed_no_ttl_local(route, key, value),
    }
    Ok(())
}
