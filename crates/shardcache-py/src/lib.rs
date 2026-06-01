#![cfg(not(all(test, feature = "extension-module")))]

// PyO3's `extension-module` feature intentionally avoids linking libpython on
// Unix so the cdylib can be loaded by Python. Cargo's Rust test harness is a
// binary and does need libpython symbols, so `cargo test --all-features` skips
// this crate's in-crate tests under `extension-module`. The normal
// `cargo test --workspace` path still runs them.

use dashmap::DashMap;
extern crate shardmap as shardmap_crate;
use shardmap_crate::config::{EvictionPolicy, ShardCacheConfig};
use shardmap_crate::cuda::CudaConfig;
use shardmap_crate::persistence::{PersistenceRuntime, WalAppender, load_recovery_state};
use shardmap_crate::storage::{
    Bytes, EmbeddedBatchReadView, EmbeddedKeyRoute, EmbeddedReadSlice, EmbeddedReadView,
    EmbeddedRouteMode, EmbeddedSessionRoute, EmbeddedStore, FastHashMap, LocalEmbeddedStore,
    MutationBytes, MutationOp, MutationRecord, OwnedEmbeddedBatchReadView, OwnedEmbeddedReadView,
    PackedBatch, PackedSessionWrite, StoredEntry, now_millis, shift_for, stripe_index,
};
#[cfg(feature = "telemetry")]
use shardmap_crate::storage::{CacheMetricsSnapshot, CacheTelemetry};
extern crate shardcache_runtime as shardcache_runtime_crate;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyBufferError, PyIndexError, PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyBytes, PyDict, PyEllipsis, PyMemoryView, PyModule, PySlice};
#[cfg(feature = "gpu-direct-api")]
use shardcache_runtime_crate::GpuDirectProxy;
use shardcache_runtime_crate::{
    CpuTransferTarget, TransferBackend as RuntimeTransferBackend, VllmBlockAllocation,
    VllmConnectorLoadSpec, VllmKvConnector, VllmRequestedPage,
};
use std::cell::RefCell;
use std::fs;
use std::os::raw::{c_int, c_void};
use std::path::Path;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

mod scnp_store;

#[pyfunction(name = "hash_key")]
fn py_hash_key(key: &[u8]) -> u64 {
    shardmap_crate::storage::hash_key(key)
}

#[derive(Debug, Clone)]
struct DashEntry {
    value: Bytes,
    expire_at_ms: Option<u64>,
}

#[inline(always)]
fn push_hex_byte(buf: &mut Vec<u8>, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    buf.push(HEX[(byte >> 4) as usize]);
    buf.push(HEX[(byte & 0x0F) as usize]);
}

fn encode_vllm_page_key(layer_index: u32, block_hash: &[u8]) -> Vec<u8> {
    let prefix = format!("vllm-page:{layer_index}:");
    let mut key = Vec::with_capacity(prefix.len() + block_hash.len() * 2);
    key.extend_from_slice(prefix.as_bytes());
    for byte in block_hash {
        push_hex_byte(&mut key, *byte);
    }
    key
}

fn encode_vllm_page_keys(layer_index: u32, block_hashes: &[PyBackedBytes]) -> Vec<Vec<u8>> {
    block_hashes
        .iter()
        .map(|block_hash| encode_vllm_page_key(layer_index, block_hash.as_ref()))
        .collect()
}

struct VllmTorchRestoreSymbols {
    frombuffer_fn: Py<PyAny>,
    empty_fn: Py<PyAny>,
    uint8_dtype: Py<PyAny>,
}

static VLLM_TORCH_RESTORE_SYMBOLS: GILOnceCell<VllmTorchRestoreSymbols> = GILOnceCell::new();

fn vllm_torch_restore_symbols(py: Python<'_>) -> PyResult<&VllmTorchRestoreSymbols> {
    VLLM_TORCH_RESTORE_SYMBOLS.get_or_try_init(py, || {
        let torch_module = PyModule::import(py, "torch")?;
        Ok(VllmTorchRestoreSymbols {
            frombuffer_fn: torch_module.getattr("frombuffer")?.unbind(),
            empty_fn: torch_module.getattr("empty")?.unbind(),
            uint8_dtype: torch_module.getattr("uint8")?.unbind(),
        })
    })
}

fn vllm_page_looks_like_torch_tensor(page: &Bound<'_, PyAny>) -> bool {
    page.hasattr("numel").unwrap_or(false)
        && page.hasattr("element_size").unwrap_or(false)
        && page.hasattr("dtype").unwrap_or(false)
        && page.hasattr("shape").unwrap_or(false)
        && page.hasattr("copy_").unwrap_or(false)
}

fn extract_vllm_layer_page(
    py: Python<'_>,
    kv_layer: &Bound<'_, PyAny>,
    block_id: usize,
) -> PyResult<Py<PyAny>> {
    if let Ok(shape) = kv_layer
        .getattr("shape")
        .and_then(|value| value.extract::<Vec<isize>>())
        && shape.len() >= 2
        && shape.first().copied() == Some(2)
    {
        return kv_layer
            .get_item((PySlice::full(py), block_id, PyEllipsis::get(py)))
            .map(Bound::unbind);
    }

    kv_layer
        .get_item((block_id, PyEllipsis::get(py)))
        .map(Bound::unbind)
}

fn read_batch_memoryview_at(
    py: Python<'_>,
    batch: &Py<PyReadBatch>,
    index: usize,
) -> PyResult<Py<PyMemoryView>> {
    let chunk = Py::new(
        py,
        PyReadBatchChunkView {
            owner: batch.clone_ref(py),
            index,
        },
    )?;
    PyMemoryView::from(chunk.bind(py).as_any()).map(Bound::unbind)
}

fn copy_payload_into_torch_page(
    py: Python<'_>,
    page: &Bound<'_, PyAny>,
    payload_view: &Bound<'_, PyAny>,
    payload_len: usize,
) -> PyResult<()> {
    let expected_len = page.call_method0("numel")?.extract::<usize>()?
        * page.call_method0("element_size")?.extract::<usize>()?;
    if payload_len != expected_len {
        return Err(PyValueError::new_err(format!(
            "cached page payload length {payload_len} does not match destination page byte length {expected_len}"
        )));
    }

    let dtype = page.getattr("dtype")?;
    let symbols = vllm_torch_restore_symbols(py)?;
    let frombuffer_fn = symbols.frombuffer_fn.bind(py);
    let empty_fn = symbols.empty_fn.bind(py);
    let uint8_dtype = symbols.uint8_dtype.bind(py);

    let frombuffer_kwargs = PyDict::new(py);
    frombuffer_kwargs.set_item("dtype", dtype.clone())?;
    let mut src = match frombuffer_fn.call((payload_view,), Some(&frombuffer_kwargs)) {
        Ok(value) => value,
        Err(_) => {
            let raw_kwargs = PyDict::new(py);
            raw_kwargs.set_item("dtype", uint8_dtype)?;
            let raw = frombuffer_fn.call((payload_view,), Some(&raw_kwargs))?;

            let empty_kwargs = PyDict::new(py);
            empty_kwargs.set_item("dtype", dtype.clone())?;
            let rebuilt = empty_fn.call((page.call_method0("numel")?,), Some(&empty_kwargs))?;
            rebuilt
                .call_method1("view", (uint8_dtype,))?
                .call_method1("copy_", (raw,))?;
            rebuilt
        }
    };

    src = src.call_method1("reshape", (page.getattr("shape")?,))?;
    if let Ok(device) = page.getattr("device") {
        let is_non_cpu = device
            .getattr("type")
            .and_then(|value| value.extract::<String>())
            .is_ok_and(|device_type| device_type != "cpu");
        if is_non_cpu {
            let to_kwargs = PyDict::new(py);
            to_kwargs.set_item("device", device)?;
            src = src.call_method("to", (), Some(&to_kwargs))?;
        }
    }

    page.call_method1("copy_", (src,))?;
    Ok(())
}

fn copy_payload_into_vllm_page(
    py: Python<'_>,
    page: &Bound<'_, PyAny>,
    payload_view: &Bound<'_, PyAny>,
    payload_len: usize,
) -> PyResult<()> {
    if vllm_page_looks_like_torch_tensor(page)
        && copy_payload_into_torch_page(py, page, payload_view, payload_len).is_ok()
    {
        return Ok(());
    }

    if page.hasattr("copy_from_bytes")? {
        page.call_method1("copy_from_bytes", (payload_view,))?;
        return Ok(());
    }
    if page.hasattr("copy_")? {
        page.call_method1("copy_", (payload_view,))?;
        return Ok(());
    }

    Err(PyTypeError::new_err(
        "unable to restore cached kv page into destination tensor-like value",
    ))
}

fn extract_vllm_page_payload_bytes(py: Python<'_>, page: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let mut current = page.clone().unbind();
    for name in ["detach", "contiguous", "cpu"] {
        let bound = current.bind(py);
        if let Ok(method) = bound.getattr(name)
            && method.is_callable()
        {
            current = method.call0()?.unbind();
        }
    }
    let current = current.bind(py);

    if vllm_page_looks_like_torch_tensor(current) {
        let uint8_dtype = vllm_torch_restore_symbols(py)?.uint8_dtype.bind(py);
        if let Ok(view) = current.call_method1("view", (uint8_dtype,))
            && let Ok(array) = view.call_method0("numpy")
            && let Ok(bytes_obj) = array.call_method0("tobytes")
        {
            return bytes_obj.extract::<Vec<u8>>();
        }
    }

    if let Ok(buffer) = PyBuffer::<u8>::get(current) {
        if let Some(slice) = buffer.as_slice(py) {
            let raw =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) };
            return Ok(raw.to_vec());
        }
        return buffer.to_vec(py);
    }

    if let Ok(casted) = current.call_method1("cast", ("B",))
        && let Ok(buffer) = PyBuffer::<u8>::get(&casted)
    {
        if let Some(slice) = buffer.as_slice(py) {
            let raw =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) };
            return Ok(raw.to_vec());
        }
        return buffer.to_vec(py);
    }

    if let Ok(bytes_value) = current.extract::<Vec<u8>>() {
        return Ok(bytes_value);
    }
    if current.hasattr("tobytes")? {
        return current.call_method0("tobytes")?.extract::<Vec<u8>>();
    }

    Err(PyTypeError::new_err(
        "unable to serialize kv page payload from tensor-like value",
    ))
}

fn extract_vllm_layer_payloads(
    py: Python<'_>,
    kv_layer: &Bound<'_, PyAny>,
    block_ids: &[usize],
) -> PyResult<Vec<Vec<u8>>> {
    let mut payloads = Vec::with_capacity(block_ids.len());
    for &block_id in block_ids {
        let page = extract_vllm_layer_page(py, kv_layer, block_id)?;
        payloads.push(extract_vllm_page_payload_bytes(py, page.bind(py))?);
    }
    Ok(payloads)
}

/// Python-visible packed batch metadata for the copy-out retrieval path.
///
/// The underlying payload is still copied today, but the binding can expose one
/// packed batch object instead of creating one Python `bytes` object per chunk.
#[pyclass(name = "PackedBatchResult")]
#[derive(Debug, Clone)]
struct PyPackedBatchResult {
    inner: PackedBatch,
}

type WorkerStateJob = Box<dyn FnOnce(&mut ThreadedStoreWorkerState) + Send + 'static>;

#[derive(Debug)]
struct ThreadedStoreWorkerState {
    store: LocalEmbeddedStore,
    pending_vllm_restores: FastHashMap<u64, shardcache_runtime::VllmRestoreHandle>,
    next_pending_vllm_restore_id: u64,
    direct_vllm_connector: Option<(CudaConfig, VllmKvConnector)>,
    #[cfg(feature = "gpu-direct-api")]
    gpu_direct_proxy: Option<(CudaConfig, GpuDirectProxy)>,
}

impl ThreadedStoreWorkerState {
    fn new(store: LocalEmbeddedStore) -> Self {
        Self {
            store,
            pending_vllm_restores: FastHashMap::default(),
            next_pending_vllm_restore_id: 1,
            direct_vllm_connector: None,
            #[cfg(feature = "gpu-direct-api")]
            gpu_direct_proxy: None,
        }
    }

    fn with_direct_vllm_connector<R>(
        &mut self,
        cuda: CudaConfig,
        f: impl FnOnce(&VllmKvConnector, &mut LocalEmbeddedStore) -> R,
    ) -> R {
        let store = &mut self.store;
        let connector_slot = &mut self.direct_vllm_connector;
        let needs_refresh = connector_slot
            .as_ref()
            .is_none_or(|(current, _)| current != &cuda);
        if needs_refresh {
            *connector_slot = Some((cuda.clone(), VllmKvConnector::new(cuda)));
        }
        let connector = &connector_slot
            .as_ref()
            .expect("direct vllm connector should be initialized")
            .1;
        f(connector, store)
    }

    #[cfg(feature = "gpu-direct-api")]
    fn with_gpu_direct_proxy<R>(
        &mut self,
        cuda: CudaConfig,
        f: impl FnOnce(&GpuDirectProxy, &mut LocalEmbeddedStore) -> R,
    ) -> R {
        let store = &mut self.store;
        let proxy_slot = &mut self.gpu_direct_proxy;
        let needs_refresh = proxy_slot
            .as_ref()
            .is_none_or(|(current, _)| current != &cuda);
        if needs_refresh {
            *proxy_slot = Some((cuda.clone(), GpuDirectProxy::new(cuda)));
        }
        let proxy = &proxy_slot
            .as_ref()
            .expect("gpu direct proxy should be initialized")
            .1;
        f(proxy, store)
    }

    fn insert_pending_vllm_restore(
        &mut self,
        handle: shardcache_runtime::VllmRestoreHandle,
    ) -> u64 {
        let pending_id = self.next_pending_vllm_restore_id;
        self.next_pending_vllm_restore_id = self.next_pending_vllm_restore_id.saturating_add(1);
        self.pending_vllm_restores.insert(pending_id, handle);
        pending_id
    }

    fn take_pending_vllm_restore(
        &mut self,
        pending_id: u64,
    ) -> Option<shardcache_runtime::VllmRestoreHandle> {
        self.pending_vllm_restores.remove(&pending_id)
    }

    fn pending_vllm_restore_ready(
        &mut self,
        pending_id: u64,
    ) -> Result<bool, shardcache_runtime::RuntimeError> {
        let handle = self
            .pending_vllm_restores
            .get_mut(&pending_id)
            .ok_or_else(|| {
                shardcache_runtime::RuntimeError::Engine(format!(
                    "missing direct vLLM restore handle {pending_id}"
                ))
            })?;
        handle.is_ready()
    }

    fn pending_vllm_restore_report(
        &mut self,
        pending_id: u64,
        path_version: DirectVllmRestorePathVersion,
    ) -> Result<DirectVllmRestoreReport, shardcache_runtime::RuntimeError> {
        let handle = self
            .pending_vllm_restores
            .get_mut(&pending_id)
            .ok_or_else(|| {
                shardcache_runtime::RuntimeError::Engine(format!(
                    "missing direct vLLM restore handle {pending_id}"
                ))
            })?;
        Ok(direct_vllm_restore_report_from_runtime(
            handle.peek_report(),
            path_version,
        ))
    }

    fn pending_vllm_restore_wait_on_stream(
        &mut self,
        pending_id: u64,
        stream_ptr: u64,
    ) -> Result<bool, shardcache_runtime::RuntimeError> {
        let handle = self
            .pending_vllm_restores
            .get_mut(&pending_id)
            .ok_or_else(|| {
                shardcache_runtime::RuntimeError::Engine(format!(
                    "missing direct vLLM restore handle {pending_id}"
                ))
            })?;
        handle.wait_on_stream(stream_ptr)
    }
}

enum WorkerCommand {
    Run(WorkerStateJob),
    Stop,
}

struct ThreadedStoreWorker {
    tx: Sender<WorkerCommand>,
    join: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for ThreadedStoreWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadedStoreWorker")
            .finish_non_exhaustive()
    }
}

impl ThreadedStoreWorker {
    fn new(worker_store: LocalEmbeddedStore, placement: Option<NumaWorkerPlacement>) -> Self {
        let (tx, rx) = mpsc::channel::<WorkerCommand>();
        let join = thread::spawn(move || {
            if let Some(cpu_id) = placement.and_then(|placement| placement.cpu_id) {
                pin_current_thread_to_cpu(cpu_id);
            }
            let mut state = ThreadedStoreWorkerState::new(worker_store);
            while let Ok(command) = rx.recv() {
                match command {
                    WorkerCommand::Run(job) => job(&mut state),
                    WorkerCommand::Stop => break,
                }
            }
        });
        Self {
            tx,
            join: Some(join),
        }
    }

    fn run_async<R, F>(&self, f: F) -> Receiver<R>
    where
        R: Send + 'static,
        F: FnOnce(&mut ThreadedStoreWorkerState) -> R + Send + 'static,
    {
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        self.tx
            .send(WorkerCommand::Run(Box::new(move |store| {
                let result = f(store);
                let _ = result_tx.send(result);
            })))
            .expect("shardcache worker thread is unavailable");
        result_rx
    }

    fn run<R, F>(&self, f: F) -> R
    where
        R: Send + 'static,
        F: FnOnce(&mut ThreadedStoreWorkerState) -> R + Send + 'static,
    {
        self.run_async(f)
            .recv()
            .expect("shardcache worker thread exited before returning a result")
    }

    fn run_store_async<R, F>(&self, f: F) -> Receiver<R>
    where
        R: Send + 'static,
        F: FnOnce(&mut LocalEmbeddedStore) -> R + Send + 'static,
    {
        self.run_async(move |state| f(&mut state.store))
    }

    fn run_store<R, F>(&self, f: F) -> R
    where
        R: Send + 'static,
        F: FnOnce(&mut LocalEmbeddedStore) -> R + Send + 'static,
    {
        self.run(move |state| f(&mut state.store))
    }
}

impl Drop for ThreadedStoreWorker {
    fn drop(&mut self) {
        let _ = self.tx.send(WorkerCommand::Stop);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

struct ThreadedStoreCore {
    workers: Vec<ThreadedStoreWorker>,
    route_mode: EmbeddedRouteMode,
    prefer_session_tags: bool,
    numa: NumaTopology,
    #[cfg(feature = "telemetry")]
    metrics: Option<Arc<CacheTelemetry>>,
    _persistence_owner: Option<Arc<PersistenceRuntime>>,
    wal_appenders: Vec<WalAppender>,
    wal_sequences: Vec<AtomicU64>,
}

struct SharedStoreCore {
    store: EmbeddedStore,
    _persistence_owner: Option<Arc<PersistenceRuntime>>,
    wal_appenders: Vec<WalAppender>,
    wal_sequences: Vec<AtomicU64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumaRoutePolicy {
    Off,
    WorkerPinned,
    CallerLocal,
}

impl NumaRoutePolicy {
    fn routes_by_caller(self) -> bool {
        matches!(self, Self::CallerLocal)
    }

    fn pins_workers(self) -> bool {
        matches!(self, Self::WorkerPinned | Self::CallerLocal)
    }
}

#[derive(Debug, Clone)]
struct NumaWorkerPlacement {
    cpu_id: Option<usize>,
}

#[derive(Debug, Clone)]
struct DiscoveredNumaNode {
    cpus: Vec<usize>,
}

#[derive(Debug, Clone)]
struct NumaTopology {
    policy: NumaRoutePolicy,
    worker_count: usize,
    workers: Vec<NumaWorkerPlacement>,
    node_workers: Vec<Vec<usize>>,
    cpu_to_node: Vec<Option<usize>>,
}

impl NumaTopology {
    fn new(worker_count: usize, policy: NumaRoutePolicy) -> Self {
        let worker_count = worker_count.max(1);
        if policy == NumaRoutePolicy::Off {
            return Self::single_node(worker_count, policy);
        }

        let nodes = discover_numa_nodes().unwrap_or_else(discover_single_node);
        if nodes.is_empty() {
            return Self::single_node(worker_count, policy);
        }

        let mut workers = Vec::with_capacity(worker_count);
        let mut node_workers = vec![Vec::new(); nodes.len()];
        let mut node_worker_ordinals = vec![0usize; nodes.len()];
        for worker_id in 0..worker_count {
            let node_index = worker_id % nodes.len();
            let cpu_id = nodes[node_index]
                .cpus
                .get(node_worker_ordinals[node_index] % nodes[node_index].cpus.len().max(1))
                .copied();
            node_worker_ordinals[node_index] = node_worker_ordinals[node_index].saturating_add(1);
            workers.push(NumaWorkerPlacement { cpu_id });
            node_workers[node_index].push(worker_id);
        }

        let max_cpu = nodes
            .iter()
            .flat_map(|node| node.cpus.iter().copied())
            .max()
            .unwrap_or(0);
        let mut cpu_to_node = vec![None; max_cpu.saturating_add(1)];
        for (node_index, node) in nodes.iter().enumerate() {
            for cpu in &node.cpus {
                if let Some(slot) = cpu_to_node.get_mut(*cpu) {
                    *slot = Some(node_index);
                }
            }
        }

        Self {
            policy,
            worker_count,
            workers,
            node_workers,
            cpu_to_node,
        }
    }

    fn single_node(worker_count: usize, policy: NumaRoutePolicy) -> Self {
        let worker_count = worker_count.max(1);
        let cpus = available_cpu_ids();
        let mut workers = Vec::with_capacity(worker_count);
        let mut node_workers = vec![Vec::with_capacity(worker_count)];
        for worker_id in 0..worker_count {
            workers.push(NumaWorkerPlacement {
                cpu_id: cpus.get(worker_id % cpus.len().max(1)).copied(),
            });
            node_workers[0].push(worker_id);
        }
        let max_cpu = cpus.iter().copied().max().unwrap_or(0);
        let mut cpu_to_node = vec![None; max_cpu.saturating_add(1)];
        for cpu in cpus {
            if let Some(slot) = cpu_to_node.get_mut(cpu) {
                *slot = Some(0);
            }
        }
        Self {
            policy,
            worker_count,
            workers,
            node_workers,
            cpu_to_node,
        }
    }

    fn placement_for_worker(&self, worker_id: usize) -> Option<NumaWorkerPlacement> {
        if !self.policy.pins_workers() {
            return None;
        }
        self.workers.get(worker_id).cloned()
    }

    fn default_worker_for_hash(&self, route_hash: u64) -> usize {
        stripe_index(route_hash, shift_for(self.worker_count))
    }

    fn worker_for_route_hash(&self, route_hash: u64) -> usize {
        if !self.policy.routes_by_caller() {
            return self.default_worker_for_hash(route_hash);
        }
        let node_index = self.current_node_index();
        let workers = self
            .node_workers
            .get(node_index)
            .filter(|workers| !workers.is_empty())
            .unwrap_or(&self.node_workers[0]);
        workers[(route_hash as usize) % workers.len()]
    }

    fn current_node_index(&self) -> usize {
        let Some(cpu_id) = current_cpu_id() else {
            return 0;
        };
        self.cpu_to_node
            .get(cpu_id)
            .and_then(|node| *node)
            .unwrap_or(0)
    }
}

fn available_cpu_ids() -> Vec<usize> {
    core_affinity::get_core_ids()
        .map(|cores| cores.into_iter().map(|core| core.id).collect())
        .filter(|cpus: &Vec<_>| !cpus.is_empty())
        .unwrap_or_else(|| vec![0])
}

fn discover_single_node() -> Vec<DiscoveredNumaNode> {
    vec![DiscoveredNumaNode {
        cpus: available_cpu_ids(),
    }]
}

fn discover_numa_nodes() -> Option<Vec<DiscoveredNumaNode>> {
    let root = Path::new("/sys/devices/system/node");
    let mut nodes = Vec::new();
    for entry in fs::read_dir(root).ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("node") || name[4..].parse::<usize>().is_err() {
            continue;
        }
        let cpulist = fs::read_to_string(entry.path().join("cpulist")).ok()?;
        let cpus = parse_cpu_list(&cpulist);
        if !cpus.is_empty() {
            nodes.push(DiscoveredNumaNode { cpus });
        }
    }
    (!nodes.is_empty()).then_some(nodes)
}

fn parse_cpu_list(raw: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in raw.trim().split(',').filter(|part| !part.is_empty()) {
        match part.split_once('-') {
            Some((start, end)) => {
                if let (Ok(start), Ok(end)) = (start.parse::<usize>(), end.parse::<usize>()) {
                    cpus.extend(start..=end);
                }
            }
            None => {
                if let Ok(cpu) = part.parse::<usize>() {
                    cpus.push(cpu);
                }
            }
        }
    }
    cpus
}

fn current_cpu_id() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        let cpu = unsafe { libc::sched_getcpu() };
        (cpu >= 0).then_some(cpu as usize)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

fn pin_current_thread_to_cpu(cpu_id: usize) {
    let _ = core_affinity::set_for_current(core_affinity::CoreId { id: cpu_id });
}

impl std::fmt::Debug for ThreadedStoreCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadedStoreCore")
            .field("worker_count", &self.workers.len())
            .field("route_mode", &self.route_mode)
            .field("prefer_session_tags", &self.prefer_session_tags)
            .field("numa", &self.numa)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for SharedStoreCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedStoreCore")
            .field("shard_count", &self.store.shard_count())
            .field("route_mode", &self.store.route_mode())
            .finish_non_exhaustive()
    }
}

struct PersistenceSetup {
    runtime: Arc<PersistenceRuntime>,
    appenders: Vec<WalAppender>,
    recovered: Vec<Vec<StoredEntry>>,
}

impl ThreadedStoreCore {
    #[allow(clippy::too_many_arguments)]
    fn new(
        worker_count: usize,
        route_mode: EmbeddedRouteMode,
        prefer_session_tags: bool,
        per_worker_memory_limit_bytes: Option<usize>,
        eviction_policy: EvictionPolicy,
        persistence: Option<PersistenceSetup>,
        numa_policy: NumaRoutePolicy,
        #[cfg(feature = "telemetry")] metrics: Option<Arc<CacheTelemetry>>,
    ) -> Self {
        let worker_count = worker_count.max(1);
        let numa = NumaTopology::new(worker_count, numa_policy);
        let (persistence, wal_appenders, recovered) = if let Some(persistence) = persistence {
            (
                Some(persistence.runtime),
                persistence.appenders,
                persistence.recovered,
            )
        } else {
            (
                None,
                Vec::new(),
                (0..worker_count).map(|_| Vec::new()).collect(),
            )
        };
        #[cfg(feature = "telemetry")]
        let store =
            EmbeddedStore::with_route_mode_and_metrics(worker_count, route_mode, metrics.clone());
        #[cfg(not(feature = "telemetry"))]
        let store = EmbeddedStore::with_route_mode(worker_count, route_mode);
        store.configure_memory_policy(per_worker_memory_limit_bytes, eviction_policy);
        for entries in recovered {
            store.restore_entries(entries);
        }
        let local_workers = store.into_local_stores(worker_count);
        let workers = local_workers
            .into_iter()
            .enumerate()
            .map(|(worker_id, store)| {
                ThreadedStoreWorker::new(store, numa.placement_for_worker(worker_id))
            })
            .collect::<Vec<_>>();
        Self {
            workers,
            route_mode,
            prefer_session_tags,
            numa,
            #[cfg(feature = "telemetry")]
            metrics,
            _persistence_owner: persistence,
            wal_appenders,
            wal_sequences: (0..worker_count).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    #[inline(always)]
    fn worker_count(&self) -> usize {
        self.workers.len()
    }

    #[inline(always)]
    fn worker_for_hash(&self, hash: u64) -> usize {
        self.numa.worker_for_route_hash(hash)
    }

    #[inline(always)]
    fn uses_caller_local_routes(&self) -> bool {
        self.numa.policy.routes_by_caller()
    }

    #[inline(always)]
    fn route_hash_for_key(&self, key: &[u8]) -> u64 {
        if self.prefer_session_tags
            && let Some(session_prefix) = extract_lmcache_session_prefix(key)
        {
            return shardmap_crate::storage::hash_key(&session_prefix);
        }
        match self.route_mode {
            EmbeddedRouteMode::FullKey => shardmap_crate::storage::hash_key(key),
            EmbeddedRouteMode::SessionPrefix => {
                shardmap_crate::storage::hash_key(session_route_prefix(key))
            }
        }
    }

    #[inline(always)]
    fn route_session(&self, session_prefix: &[u8]) -> usize {
        self.worker_for_hash(shardmap_crate::storage::hash_key(session_prefix))
    }

    #[inline(always)]
    fn route_key(&self, key: &[u8]) -> usize {
        self.worker_for_hash(self.route_hash_for_key(key))
    }

    #[inline(always)]
    fn routed_key(&self, key: &[u8]) -> (usize, EmbeddedKeyRoute) {
        let worker_id = self.route_key(key);
        (
            worker_id,
            EmbeddedKeyRoute {
                shard_id: worker_id,
                key_hash: shardmap_crate::storage::hash_key(key),
            },
        )
    }

    #[inline(always)]
    fn routed_session(&self, session_prefix: &[u8]) -> (usize, EmbeddedSessionRoute) {
        let worker_id = self.route_session(session_prefix);
        (
            worker_id,
            EmbeddedSessionRoute {
                shard_id: worker_id,
            },
        )
    }

    #[cfg(feature = "telemetry")]
    fn export_metrics_prometheus(&self) -> Option<String> {
        self.metrics
            .as_ref()
            .map(|metrics| metrics.export_prometheus())
    }

    #[cfg(feature = "telemetry")]
    fn metrics_snapshot(&self) -> Option<CacheMetricsSnapshot> {
        self.metrics.as_ref().map(|metrics| metrics.snapshot())
    }

    fn append_wal<K, V>(
        &self,
        shard_id: usize,
        op: MutationOp,
        key: K,
        value: V,
        expire_at_ms: Option<u64>,
        timestamp_ms: u64,
    ) where
        K: Into<MutationBytes>,
        V: Into<MutationBytes>,
    {
        if let Some(appender) = self.wal_appenders.get(shard_id) {
            let sequence = self.wal_sequences[shard_id].fetch_add(1, Ordering::Relaxed) + 1;
            appender
                .append(MutationRecord {
                    shard_id,
                    sequence,
                    timestamp_ms,
                    op,
                    key: key.into(),
                    value: value.into(),
                    expire_at_ms,
                })
                .expect("shardcache WAL append failed");
        }
    }

    #[inline(always)]
    fn wal_enabled_for_shard(&self, shard_id: usize) -> bool {
        self.wal_appenders.get(shard_id).is_some()
    }
}

impl SharedStoreCore {
    fn append_wal<K, V>(
        &self,
        shard_id: usize,
        op: MutationOp,
        key: K,
        value: V,
        expire_at_ms: Option<u64>,
        timestamp_ms: u64,
    ) where
        K: Into<MutationBytes>,
        V: Into<MutationBytes>,
    {
        if let Some(appender) = self.wal_appenders.get(shard_id) {
            let sequence = self.wal_sequences[shard_id].fetch_add(1, Ordering::Relaxed) + 1;
            appender
                .append(MutationRecord {
                    shard_id,
                    sequence,
                    timestamp_ms,
                    op,
                    key: key.into(),
                    value: value.into(),
                    expire_at_ms,
                })
                .expect("shardcache WAL append failed");
        }
    }

    #[inline(always)]
    fn wal_enabled_for_shard(&self, shard_id: usize) -> bool {
        self.wal_appenders.get(shard_id).is_some()
    }
}

fn routed_shard_for_key(
    shard_count: usize,
    route_mode: EmbeddedRouteMode,
    prefer_session_tags: bool,
    key: &[u8],
) -> usize {
    let shard_count = shard_count.max(1);
    if prefer_session_tags && let Some(session_prefix) = extract_lmcache_session_prefix(key) {
        return stripe_index(
            shardmap_crate::storage::hash_key(&session_prefix),
            shift_for(shard_count),
        );
    }
    let route_hash = match route_mode {
        EmbeddedRouteMode::FullKey => shardmap_crate::storage::hash_key(key),
        EmbeddedRouteMode::SessionPrefix => {
            shardmap_crate::storage::hash_key(session_route_prefix(key))
        }
    };
    stripe_index(route_hash, shift_for(shard_count))
}

fn build_persistence_setup(
    shard_count: usize,
    wal_path: Option<&str>,
    compress_wal: bool,
    route_mode: EmbeddedRouteMode,
    prefer_session_tags: bool,
    #[cfg(feature = "telemetry")] metrics: Option<Arc<CacheTelemetry>>,
) -> PyResult<Option<PersistenceSetup>> {
    let Some(wal_path) = wal_path else {
        return Ok(None);
    };

    let mut config = ShardCacheConfig {
        shard_count: shard_count.max(1),
        ..ShardCacheConfig::default()
    };
    config.persistence.data_dir = wal_path.into();
    config.persistence.compress_wal = compress_wal;
    config
        .validate()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    config
        .ensure_paths()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;

    let recovery = load_recovery_state(&config.persistence)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let mut recovered = vec![Vec::<StoredEntry>::new(); config.shard_count];
    for entry in recovery.entries {
        let shard_id = routed_shard_for_key(
            config.shard_count,
            route_mode,
            prefer_session_tags,
            &entry.key,
        );
        recovered[shard_id].push(entry);
    }

    #[cfg(feature = "telemetry")]
    let runtime = Arc::new(
        PersistenceRuntime::start_with_metrics(
            config.shard_count,
            config.persistence.clone(),
            metrics,
        )
        .map_err(|error| PyValueError::new_err(error.to_string()))?,
    );
    #[cfg(not(feature = "telemetry"))]
    let runtime = Arc::new(
        PersistenceRuntime::start(config.shard_count, config.persistence.clone())
            .map_err(|error| PyValueError::new_err(error.to_string()))?,
    );

    let mut appenders = Vec::with_capacity(config.shard_count);
    for shard_id in 0..config.shard_count {
        let appender = runtime.appender(shard_id).ok_or_else(|| {
            PyValueError::new_err(format!("missing WAL appender for shard {shard_id}"))
        })?;
        appenders.push(appender);
    }

    Ok(Some(PersistenceSetup {
        runtime,
        appenders,
        recovered,
    }))
}

enum StoreCore {
    Shared(SharedStoreCore),
    Threaded(ThreadedStoreCore),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectVllmRestoreReport {
    backend: RuntimeTransferBackend,
    page_count: usize,
    hit_pages: usize,
    missed_pages: usize,
    transferred_bytes: usize,
    all_hit: bool,
    total_expected_bytes: Option<usize>,
    path_version: DirectVllmRestorePathVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectVllmRestoreTicket {
    worker_id: usize,
    pending_id: u64,
    path_version: DirectVllmRestorePathVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectVllmRestorePathVersion {
    HostDirectV1,
    #[cfg_attr(not(feature = "gpu-direct-api"), allow(dead_code))]
    GpuDirectApiV0,
}

impl DirectVllmRestorePathVersion {
    const HOST_DIRECT_V1: &'static str = "host_direct_v1";
    const GPU_DIRECT_API_V0: &'static str = "gpu_direct_api_v0";

    #[inline(always)]
    fn as_str(self) -> &'static str {
        match self {
            Self::HostDirectV1 => Self::HOST_DIRECT_V1,
            Self::GpuDirectApiV0 => Self::GPU_DIRECT_API_V0,
        }
    }

    fn parse(value: &str) -> PyResult<Self> {
        match value {
            "" | Self::HOST_DIRECT_V1 => Ok(Self::HostDirectV1),
            Self::GPU_DIRECT_API_V0 => {
                #[cfg(feature = "gpu-direct-api")]
                {
                    Ok(Self::GpuDirectApiV0)
                }
                #[cfg(not(feature = "gpu-direct-api"))]
                {
                    Err(PyValueError::new_err(
                        "path_version='gpu_direct_api_v0' requires shardcache-py built with feature 'gpu-direct-api'",
                    ))
                }
            }
            other => Err(PyValueError::new_err(format!(
                "unsupported path_version {other:?}; expected one of {:?}",
                Self::supported_names()
            ))),
        }
    }

    fn supported_names() -> Vec<&'static str> {
        #[cfg(not(feature = "gpu-direct-api"))]
        {
            vec![Self::HOST_DIRECT_V1]
        }
        #[cfg(feature = "gpu-direct-api")]
        {
            vec![Self::HOST_DIRECT_V1, Self::GPU_DIRECT_API_V0]
        }
    }
}

fn direct_vllm_restore_report_dict(
    py: Python<'_>,
    report: DirectVllmRestoreReport,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item(
        "backend",
        match report.backend {
            RuntimeTransferBackend::Cpu => "cpu",
            RuntimeTransferBackend::Gpu => "gpu",
        },
    )?;
    dict.set_item("page_count", report.page_count)?;
    dict.set_item("hit_pages", report.hit_pages)?;
    dict.set_item("missed_pages", report.missed_pages)?;
    dict.set_item("transferred_bytes", report.transferred_bytes)?;
    dict.set_item("all_hit", report.all_hit)?;
    dict.set_item("total_expected_bytes", report.total_expected_bytes)?;
    dict.set_item("path_version", report.path_version.as_str())?;
    Ok(dict.unbind())
}

fn direct_vllm_restore_report_from_runtime(
    report: shardcache_runtime::VllmRestoreReport,
    path_version: DirectVllmRestorePathVersion,
) -> DirectVllmRestoreReport {
    DirectVllmRestoreReport {
        backend: report.backend(),
        page_count: report.page_count(),
        hit_pages: report.hit_pages(),
        missed_pages: report.missed_pages(),
        transferred_bytes: report.transferred_bytes(),
        all_hit: report.all_hit(),
        total_expected_bytes: report.total_expected_bytes(),
        path_version,
    }
}

#[derive(Debug)]
enum PyReadViewInner {
    Shared(EmbeddedReadView),
    Owned(OwnedEmbeddedReadView),
}

impl PyReadViewInner {
    fn is_hit(&self) -> bool {
        match self {
            Self::Shared(view) => view.is_hit(),
            Self::Owned(view) => view.is_hit(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Shared(view) => view.len(),
            Self::Owned(view) => view.len(),
        }
    }

    fn slice(&self) -> Option<&[u8]> {
        match self {
            Self::Shared(view) => view.slice(),
            Self::Owned(view) => view.slice(),
        }
    }

    fn slice_meta(&self) -> Option<EmbeddedReadSlice> {
        match self {
            Self::Shared(view) => view.slice_meta(),
            Self::Owned(view) => view.slice_meta(),
        }
    }
}

#[derive(Debug)]
enum PyReadBatchViewInner {
    Shared(EmbeddedBatchReadView),
    Owned(OwnedEmbeddedBatchReadView),
}

impl PyReadBatchViewInner {
    fn item_count(&self) -> usize {
        match self {
            Self::Shared(batch) => batch.item_count(),
            Self::Owned(batch) => batch.item_count(),
        }
    }

    fn hit_count(&self) -> usize {
        match self {
            Self::Shared(batch) => batch.hit_count(),
            Self::Owned(batch) => batch.hit_count(),
        }
    }

    fn total_bytes(&self) -> usize {
        match self {
            Self::Shared(batch) => batch.total_bytes(),
            Self::Owned(batch) => batch.total_bytes(),
        }
    }

    fn all_hit(&self) -> bool {
        match self {
            Self::Shared(batch) => batch.all_hit(),
            Self::Owned(batch) => batch.all_hit(),
        }
    }

    fn lengths(&self) -> Vec<usize> {
        match self {
            Self::Shared(batch) => batch.lengths(),
            Self::Owned(batch) => batch.lengths(),
        }
    }

    fn slice_meta(&self, index: usize) -> Option<EmbeddedReadSlice> {
        match self {
            Self::Shared(batch) => batch.slice_meta(index),
            Self::Owned(batch) => batch.slice_meta(index),
        }
    }
}

#[derive(Debug)]
enum PyReadBatchInner {
    Single(PyReadBatchViewInner),
    Routed {
        batches: Vec<PyReadBatchViewInner>,
        entries: Vec<Option<(usize, usize)>>,
        lengths: Vec<usize>,
        hit_count: usize,
        total_bytes: usize,
    },
}

impl PyReadBatchInner {
    fn item_count(&self) -> usize {
        match self {
            Self::Single(batch) => batch.item_count(),
            Self::Routed { entries, .. } => entries.len(),
        }
    }

    fn hit_count(&self) -> usize {
        match self {
            Self::Single(batch) => batch.hit_count(),
            Self::Routed { hit_count, .. } => *hit_count,
        }
    }

    fn total_bytes(&self) -> usize {
        match self {
            Self::Single(batch) => batch.total_bytes(),
            Self::Routed { total_bytes, .. } => *total_bytes,
        }
    }

    fn all_hit(&self) -> bool {
        self.hit_count() == self.item_count()
    }

    fn lengths(&self) -> Vec<usize> {
        match self {
            Self::Single(batch) => batch.lengths(),
            Self::Routed { lengths, .. } => lengths.clone(),
        }
    }

    fn slice_meta(&self, index: usize) -> Option<EmbeddedReadSlice> {
        match self {
            Self::Single(batch) => batch.slice_meta(index),
            Self::Routed {
                batches, entries, ..
            } => entries
                .get(index)
                .copied()
                .flatten()
                .and_then(|(batch_index, local_index)| {
                    batches
                        .get(batch_index)
                        .and_then(|batch| batch.slice_meta(local_index))
                }),
        }
    }
}

impl StoreCore {
    fn normalize_client_architecture(client_architecture: &str) -> &str {
        match client_architecture {
            "local_embedded" | "local_owned" | "owned_workers" => "local_embedded",
            "shared" => "shared",
            other => other,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        cores: usize,
        wal_path: Option<&str>,
        compress_wal: bool,
        max_memory_bytes: Option<usize>,
        eviction_policy: EvictionPolicy,
        route_mode: EmbeddedRouteMode,
        enable_metrics: bool,
        client_architecture: &str,
        prefer_session_tags: bool,
        numa_policy: NumaRoutePolicy,
    ) -> PyResult<Self> {
        let cores = cores.max(1);
        #[cfg(feature = "telemetry")]
        let metrics = if enable_metrics {
            Some(CacheTelemetry::new(cores))
        } else {
            None
        };

        #[cfg(not(feature = "telemetry"))]
        if enable_metrics {
            return Err(PyValueError::new_err(
                "shardcache-py was built without the telemetry feature",
            ));
        }
        if numa_policy == NumaRoutePolicy::CallerLocal && wal_path.is_some() {
            return Err(PyValueError::new_err(
                "numa_policy='caller_local' is incompatible with WAL persistence because one logical key may have one node-local copy per NUMA node",
            ));
        }

        let normalized_architecture = Self::normalize_client_architecture(client_architecture);
        if normalized_architecture != "local_embedded" && numa_policy != NumaRoutePolicy::Off {
            return Err(PyValueError::new_err(
                "numa_policy requires client_architecture='local_embedded'",
            ));
        }

        match normalized_architecture {
            "local_embedded" => {
                let per_worker_memory_limit_bytes =
                    max_memory_bytes.map(|bytes| bytes.div_ceil(cores.max(1)));
                let persistence = build_persistence_setup(
                    cores,
                    wal_path,
                    compress_wal,
                    route_mode,
                    prefer_session_tags,
                    #[cfg(feature = "telemetry")]
                    metrics.clone(),
                )?;
                Ok(Self::Threaded(ThreadedStoreCore::new(
                    cores,
                    route_mode,
                    prefer_session_tags,
                    per_worker_memory_limit_bytes,
                    eviction_policy,
                    persistence,
                    numa_policy,
                    #[cfg(feature = "telemetry")]
                    metrics,
                )))
            }
            "shared" => {
                #[cfg(feature = "telemetry")]
                let store = if metrics.is_some() {
                    EmbeddedStore::with_route_mode_and_metrics(cores, route_mode, metrics.clone())
                } else {
                    EmbeddedStore::with_route_mode(cores, route_mode)
                };

                #[cfg(not(feature = "telemetry"))]
                let store = EmbeddedStore::with_route_mode(cores, route_mode);
                store.configure_memory_policy(
                    max_memory_bytes.map(|bytes| bytes.div_ceil(cores.max(1))),
                    eviction_policy,
                );
                let persistence = build_persistence_setup(
                    cores,
                    wal_path,
                    compress_wal,
                    route_mode,
                    false,
                    #[cfg(feature = "telemetry")]
                    metrics,
                )?;
                let (persistence, wal_appenders, recovered) = if let Some(persistence) = persistence
                {
                    (
                        Some(persistence.runtime),
                        persistence.appenders,
                        persistence.recovered,
                    )
                } else {
                    (None, Vec::new(), (0..cores).map(|_| Vec::new()).collect())
                };
                for entries in recovered {
                    store.restore_entries(entries);
                }
                Ok(Self::Shared(SharedStoreCore {
                    store,
                    _persistence_owner: persistence,
                    wal_appenders,
                    wal_sequences: (0..cores).map(|_| AtomicU64::new(0)).collect(),
                }))
            }
            other => Err(PyValueError::new_err(format!(
                "unsupported client_architecture {other:?}; expected 'local_embedded' or 'shared'"
            ))),
        }
    }

    fn shard_count(&self) -> usize {
        match self {
            Self::Shared(core) => core.store.shard_count(),
            Self::Threaded(store) => store.worker_count(),
        }
    }

    fn route_mode(&self) -> EmbeddedRouteMode {
        match self {
            Self::Shared(core) => core.store.route_mode(),
            Self::Threaded(store) => store.route_mode,
        }
    }

    fn submit_vllm_paged_restore(
        &self,
        spec: VllmConnectorLoadSpec,
        cuda: CudaConfig,
        cpu_fallback: Option<CpuTransferTarget>,
    ) -> Result<DirectVllmRestoreTicket, shardcache_runtime::RuntimeError> {
        self.submit_vllm_paged_restore_host_direct(spec, cuda, cpu_fallback)
    }

    fn submit_vllm_paged_restore_with_path(
        &self,
        spec: VllmConnectorLoadSpec,
        cuda: CudaConfig,
        cpu_fallback: Option<CpuTransferTarget>,
        path_version: DirectVllmRestorePathVersion,
    ) -> Result<DirectVllmRestoreTicket, shardcache_runtime::RuntimeError> {
        match path_version {
            DirectVllmRestorePathVersion::HostDirectV1 => {
                self.submit_vllm_paged_restore(spec, cuda, cpu_fallback)
            }
            DirectVllmRestorePathVersion::GpuDirectApiV0 => {
                #[cfg(feature = "gpu-direct-api")]
                {
                    self.submit_vllm_paged_restore_gpu_direct_api_v0(spec, cuda, cpu_fallback)
                }
                #[cfg(not(feature = "gpu-direct-api"))]
                {
                    let _ = spec;
                    let _ = cuda;
                    let _ = cpu_fallback;
                    Err(shardcache_runtime::RuntimeError::Engine(
                        "path_version='gpu_direct_api_v0' requires shardcache-py built with feature 'gpu-direct-api'".into(),
                    ))
                }
            }
        }
    }

    fn submit_vllm_paged_restore_host_direct(
        &self,
        spec: VllmConnectorLoadSpec,
        cuda: CudaConfig,
        cpu_fallback: Option<CpuTransferTarget>,
    ) -> Result<DirectVllmRestoreTicket, shardcache_runtime::RuntimeError> {
        match self {
            Self::Shared(_) => Err(shardcache_runtime::RuntimeError::Engine(
                "direct vLLM restore requires client_architecture='local_embedded'".into(),
            )),
            Self::Threaded(store) => {
                let worker_id = store.route_session(&spec.session_prefix);
                store.workers[worker_id].run(move |state| {
                    let handle = state.with_direct_vllm_connector(cuda, |connector, store| {
                        let plan = spec.plan(connector, cpu_fallback)?;
                        connector.submit_restore(store, plan)
                    })?;
                    let pending_id = state.insert_pending_vllm_restore(handle);
                    Ok(DirectVllmRestoreTicket {
                        worker_id,
                        pending_id,
                        path_version: DirectVllmRestorePathVersion::HostDirectV1,
                    })
                })
            }
        }
    }

    #[cfg(feature = "gpu-direct-api")]
    fn submit_vllm_paged_restore_gpu_direct_api_v0(
        &self,
        spec: VllmConnectorLoadSpec,
        cuda: CudaConfig,
        cpu_fallback: Option<CpuTransferTarget>,
    ) -> Result<DirectVllmRestoreTicket, shardcache_runtime::RuntimeError> {
        match self {
            Self::Shared(_) => Err(shardcache_runtime::RuntimeError::Engine(
                "direct vLLM restore requires client_architecture='local_embedded'".into(),
            )),
            Self::Threaded(store) => {
                let worker_id = store.route_session(&spec.session_prefix);
                store.workers[worker_id].run(move |state| {
                    let handle = state.with_gpu_direct_proxy(cuda, |proxy, store| {
                        proxy.submit_vllm_restore(store, spec, cpu_fallback)
                    })?;
                    let pending_id = state.insert_pending_vllm_restore(handle);
                    Ok(DirectVllmRestoreTicket {
                        worker_id,
                        pending_id,
                        path_version: DirectVllmRestorePathVersion::GpuDirectApiV0,
                    })
                })
            }
        }
    }

    fn wait_vllm_paged_restore(
        &self,
        ticket: DirectVllmRestoreTicket,
    ) -> Result<DirectVllmRestoreReport, shardcache_runtime::RuntimeError> {
        match self {
            Self::Shared(_) => Err(shardcache_runtime::RuntimeError::Engine(
                "direct vLLM restore requires client_architecture='local_embedded'".into(),
            )),
            Self::Threaded(store) => store.workers[ticket.worker_id].run(move |state| {
                let handle = state
                    .take_pending_vllm_restore(ticket.pending_id)
                    .ok_or_else(|| {
                        shardcache_runtime::RuntimeError::Engine(format!(
                            "missing direct vLLM restore handle {} for worker {}",
                            ticket.pending_id, ticket.worker_id
                        ))
                    })?;
                Ok(direct_vllm_restore_report_from_runtime(
                    handle.wait()?,
                    ticket.path_version,
                ))
            }),
        }
    }

    fn is_vllm_paged_restore_ready(
        &self,
        ticket: DirectVllmRestoreTicket,
    ) -> Result<bool, shardcache_runtime::RuntimeError> {
        match self {
            Self::Shared(_) => Err(shardcache_runtime::RuntimeError::Engine(
                "direct vLLM restore requires client_architecture='local_embedded'".into(),
            )),
            Self::Threaded(store) => store.workers[ticket.worker_id]
                .run(move |state| state.pending_vllm_restore_ready(ticket.pending_id)),
        }
    }

    fn peek_vllm_paged_restore_report(
        &self,
        ticket: DirectVllmRestoreTicket,
    ) -> Result<DirectVllmRestoreReport, shardcache_runtime::RuntimeError> {
        match self {
            Self::Shared(_) => Err(shardcache_runtime::RuntimeError::Engine(
                "direct vLLM restore requires client_architecture='local_embedded'".into(),
            )),
            Self::Threaded(store) => store.workers[ticket.worker_id].run(move |state| {
                state.pending_vllm_restore_report(ticket.pending_id, ticket.path_version)
            }),
        }
    }

    fn wait_vllm_paged_restore_on_stream(
        &self,
        ticket: DirectVllmRestoreTicket,
        stream_ptr: u64,
    ) -> Result<bool, shardcache_runtime::RuntimeError> {
        match self {
            Self::Shared(_) => Err(shardcache_runtime::RuntimeError::Engine(
                "direct vLLM restore requires client_architecture='local_embedded'".into(),
            )),
            Self::Threaded(store) => store.workers[ticket.worker_id].run(move |state| {
                state.pending_vllm_restore_wait_on_stream(ticket.pending_id, stream_ptr)
            }),
        }
    }

    fn try_wait_vllm_paged_restore(
        &self,
        ticket: DirectVllmRestoreTicket,
    ) -> Result<Option<DirectVllmRestoreReport>, shardcache_runtime::RuntimeError> {
        match self {
            Self::Shared(_) => Err(shardcache_runtime::RuntimeError::Engine(
                "direct vLLM restore requires client_architecture='local_embedded'".into(),
            )),
            Self::Threaded(store) => store.workers[ticket.worker_id].run(move |state| {
                if !state.pending_vllm_restore_ready(ticket.pending_id)? {
                    return Ok(None);
                }
                let handle = state
                    .take_pending_vllm_restore(ticket.pending_id)
                    .ok_or_else(|| {
                        shardcache_runtime::RuntimeError::Engine(format!(
                            "missing direct vLLM restore handle {} for worker {}",
                            ticket.pending_id, ticket.worker_id
                        ))
                    })?;
                Ok(Some(direct_vllm_restore_report_from_runtime(
                    handle.wait()?,
                    ticket.path_version,
                )))
            }),
        }
    }

    fn cancel_vllm_paged_restore(&self, ticket: DirectVllmRestoreTicket) -> bool {
        match self {
            Self::Shared(_) => false,
            Self::Threaded(store) => store.workers[ticket.worker_id]
                .run(move |state| state.take_pending_vllm_restore(ticket.pending_id).is_some()),
        }
    }

    fn restore_vllm_paged(
        &self,
        spec: VllmConnectorLoadSpec,
        cuda: CudaConfig,
        cpu_fallback: Option<CpuTransferTarget>,
    ) -> Result<DirectVllmRestoreReport, shardcache_runtime::RuntimeError> {
        let ticket = self.submit_vllm_paged_restore(spec, cuda, cpu_fallback)?;
        self.wait_vllm_paged_restore(ticket)
    }

    fn restore_vllm_paged_with_path(
        &self,
        spec: VllmConnectorLoadSpec,
        cuda: CudaConfig,
        cpu_fallback: Option<CpuTransferTarget>,
        path_version: DirectVllmRestorePathVersion,
    ) -> Result<DirectVllmRestoreReport, shardcache_runtime::RuntimeError> {
        match path_version {
            DirectVllmRestorePathVersion::HostDirectV1 => {
                self.restore_vllm_paged(spec, cuda, cpu_fallback)
            }
            DirectVllmRestorePathVersion::GpuDirectApiV0 => {
                let ticket = self.submit_vllm_paged_restore_with_path(
                    spec,
                    cuda,
                    cpu_fallback,
                    path_version,
                )?;
                self.wait_vllm_paged_restore(ticket)
            }
        }
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl_ms: Option<u64>) {
        match self {
            Self::Shared(core) => {
                let shard_id = routed_shard_for_key(
                    core.store.shard_count(),
                    core.store.route_mode(),
                    false,
                    &key,
                );
                let now_ms = now_millis();
                let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
                if core.wal_enabled_for_shard(shard_id) {
                    let wal_key = key.clone();
                    let wal_value = value.clone();
                    core.store.set(key, value, ttl_ms);
                    core.append_wal(
                        shard_id,
                        MutationOp::Set,
                        wal_key,
                        wal_value,
                        expire_at_ms,
                        now_ms,
                    );
                } else {
                    core.store.set(key, value, ttl_ms);
                }
            }
            Self::Threaded(store) => {
                let (worker_id, route) = store.routed_key(&key);
                let now_ms = now_millis();
                let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
                let wal_record = store
                    .wal_enabled_for_shard(worker_id)
                    .then(|| (key.clone(), value.clone()));
                match store.uses_caller_local_routes().then_some(route) {
                    Some(route) => {
                        store.workers[worker_id].run_store(move |inner| {
                            inner.set_slice_routed_local(route, &key, &value, ttl_ms)
                        });
                    }
                    None => {
                        store.workers[worker_id]
                            .run_store(move |inner| inner.set(key, value, ttl_ms));
                    }
                }
                if let Some((wal_key, wal_value)) = wal_record {
                    store.append_wal(
                        worker_id,
                        MutationOp::Set,
                        wal_key,
                        wal_value,
                        expire_at_ms,
                        now_ms,
                    );
                }
            }
        }
    }

    fn batch_set(&self, items: Vec<(Vec<u8>, Vec<u8>)>, ttl_ms: Option<u64>) {
        match self {
            Self::Shared(core) => {
                let now_ms = now_millis();
                let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
                if core.wal_appenders.is_empty() {
                    core.store.batch_set(items, ttl_ms);
                } else {
                    let records = items
                        .iter()
                        .map(|(key, value)| {
                            (
                                routed_shard_for_key(
                                    core.store.shard_count(),
                                    core.store.route_mode(),
                                    false,
                                    key,
                                ),
                                key.clone(),
                                value.clone(),
                            )
                        })
                        .collect::<Vec<_>>();
                    core.store.batch_set(items, ttl_ms);
                    for (shard_id, key, value) in records {
                        core.append_wal(
                            shard_id,
                            MutationOp::Set,
                            key,
                            value,
                            expire_at_ms,
                            now_ms,
                        );
                    }
                }
            }
            Self::Threaded(store) => {
                let now_ms = now_millis();
                let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
                if store.uses_caller_local_routes() {
                    let mut grouped = vec![
                        Vec::<(EmbeddedKeyRoute, Vec<u8>, Vec<u8>)>::new();
                        store.worker_count()
                    ];
                    for (key, value) in items {
                        let (worker_id, route) = store.routed_key(&key);
                        grouped[worker_id].push((route, key, value));
                    }
                    for (worker_id, group) in grouped.into_iter().enumerate() {
                        if group.is_empty() {
                            continue;
                        }
                        let records = if store.wal_enabled_for_shard(worker_id) {
                            group
                                .iter()
                                .map(|(_, key, value)| (key.clone(), value.clone()))
                                .collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        store.workers[worker_id].run_store(move |inner| {
                            for (route, key, value) in group {
                                inner.set_slice_routed_local(route, &key, &value, ttl_ms);
                            }
                        });
                        for (key, value) in records {
                            store.append_wal(
                                worker_id,
                                MutationOp::Set,
                                key,
                                value,
                                expire_at_ms,
                                now_ms,
                            );
                        }
                    }
                    return;
                }

                let mut grouped = vec![Vec::<(Vec<u8>, Vec<u8>)>::new(); store.worker_count()];
                for (key, value) in items {
                    let worker_id = store.route_key(&key);
                    grouped[worker_id].push((key, value));
                }
                for (worker_id, group) in grouped.into_iter().enumerate() {
                    if group.is_empty() {
                        continue;
                    }
                    if store.wal_enabled_for_shard(worker_id) {
                        let records = group
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect::<Vec<_>>();
                        store.workers[worker_id]
                            .run_store(move |inner| inner.batch_set(group, ttl_ms));
                        for (key, value) in records {
                            store.append_wal(
                                worker_id,
                                MutationOp::Set,
                                key,
                                value,
                                expire_at_ms,
                                now_ms,
                            );
                        }
                    } else {
                        store.workers[worker_id]
                            .run_store(move |inner| inner.batch_set(group, ttl_ms));
                    }
                }
            }
        }
    }

    fn batch_set_session_owned_no_ttl(
        &self,
        session_prefix: Vec<u8>,
        items: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        match self {
            Self::Shared(core) => {
                let shard_id = routed_shard_for_key(
                    core.store.shard_count(),
                    core.store.route_mode(),
                    false,
                    &session_prefix,
                );
                let now_ms = now_millis();
                if core.wal_enabled_for_shard(shard_id) {
                    let records = items
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<Vec<_>>();
                    core.store
                        .batch_set_session_owned_no_ttl(session_prefix, items);
                    for (key, value) in records {
                        core.append_wal(shard_id, MutationOp::Set, key, value, None, now_ms);
                    }
                } else {
                    core.store
                        .batch_set_session_owned_no_ttl(session_prefix, items);
                }
            }
            Self::Threaded(store) => {
                let (worker_id, route) = store.routed_session(&session_prefix);
                let now_ms = now_millis();
                let wal_records = store.wal_enabled_for_shard(worker_id).then(|| {
                    items
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect::<Vec<_>>()
                });
                match store.uses_caller_local_routes().then_some(route) {
                    Some(route) => {
                        store.workers[worker_id].run_store(move |inner| {
                            inner.batch_set_session_owned_no_ttl_routed_local(
                                route,
                                session_prefix,
                                items,
                            )
                        });
                    }
                    None => {
                        store.workers[worker_id].run_store(move |inner| {
                            inner.batch_set_session_owned_no_ttl(session_prefix, items)
                        });
                    }
                }
                if let Some(records) = wal_records {
                    for (key, value) in records {
                        store.append_wal(worker_id, MutationOp::Set, key, value, None, now_ms);
                    }
                }
            }
        }
    }

    fn batch_set_session_packed_no_ttl(&self, sessions: Vec<PackedSessionWrite>) {
        if sessions.is_empty() {
            return;
        }

        match self {
            Self::Shared(core) => {
                let now_ms = now_millis();
                for packed in sessions {
                    let shard_id = routed_shard_for_key(
                        core.store.shard_count(),
                        core.store.route_mode(),
                        false,
                        packed.session_prefix(),
                    );
                    if core.wal_enabled_for_shard(shard_id) {
                        let records = packed.cloned_records();
                        core.store.batch_set_session_packed_no_ttl(packed);
                        for (key, value) in records {
                            core.append_wal(shard_id, MutationOp::Set, key, value, None, now_ms);
                        }
                    } else {
                        core.store.batch_set_session_packed_no_ttl(packed);
                    }
                }
            }
            Self::Threaded(store) => {
                let now_ms = now_millis();
                let mut grouped = (0..store.worker_count())
                    .map(|_| Vec::<(EmbeddedSessionRoute, PackedSessionWrite)>::new())
                    .collect::<Vec<_>>();
                for packed in sessions {
                    let (worker_id, route) = store.routed_session(packed.session_prefix());
                    grouped[worker_id].push((route, packed));
                }

                let mut pending = Vec::new();
                for (worker_id, group) in grouped.into_iter().enumerate() {
                    if group.is_empty() {
                        continue;
                    }

                    let wal_records = if store.wal_enabled_for_shard(worker_id) {
                        group
                            .iter()
                            .flat_map(|(_, packed)| packed.cloned_records())
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    let caller_local_routes = store.uses_caller_local_routes();

                    let rx = store.workers[worker_id].run_store_async(move |inner| {
                        for (route, packed) in group {
                            if caller_local_routes {
                                inner.batch_set_session_packed_no_ttl_routed_local(route, packed);
                            } else {
                                inner.batch_set_session_packed_no_ttl(packed);
                            }
                        }
                    });
                    pending.push((worker_id, rx, wal_records));
                }

                for (worker_id, rx, wal_records) in pending {
                    rx.recv()
                        .expect("shardcache worker thread exited before publishing session batch");
                    for (key, value) in wal_records {
                        store.append_wal(worker_id, MutationOp::Set, key, value, None, now_ms);
                    }
                }
            }
        }
    }

    fn batch_set_session_packed_items_no_ttl(
        &self,
        session_prefix: Vec<u8>,
        items: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        self.batch_set_session_packed_no_ttl(vec![PackedSessionWrite::from_owned_items(
            session_prefix,
            items,
        )]);
    }

    fn batch_put_lmcache_encoded_batch(&self, batch: EncodedLmcachePutBatch) {
        if !batch.packed_sessions.is_empty() {
            self.batch_set_session_packed_no_ttl(batch.packed_sessions);
        }
        if !batch.generic_items.is_empty() {
            self.batch_set(batch.generic_items, None);
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::Shared(core) => core.store.get(key),
            Self::Threaded(store) => {
                let (worker_id, route) = store.routed_key(key);
                let key = key.to_vec();
                if store.uses_caller_local_routes() {
                    store.workers[worker_id].run_store(move |inner| {
                        inner.get_ref_routed_local(route, &key).map(<[u8]>::to_vec)
                    })
                } else {
                    store.workers[worker_id].run_store(move |inner| inner.get(&key))
                }
            }
        }
    }

    fn get_view(&self, key: &[u8]) -> PyReadViewInner {
        match self {
            Self::Shared(core) => PyReadViewInner::Shared(core.store.get_view(key)),
            Self::Threaded(store) => {
                let (worker_id, route) = store.routed_key(key);
                let key = key.to_vec();
                if store.uses_caller_local_routes() {
                    PyReadViewInner::Owned(
                        store.workers[worker_id]
                            .run_store(move |inner| inner.get_owned_view_routed_local(route, &key)),
                    )
                } else {
                    PyReadViewInner::Owned(
                        store.workers[worker_id]
                            .run_store(move |inner| inner.get_owned_view_local(&key)),
                    )
                }
            }
        }
    }

    fn batch_get(&self, keys: Vec<Vec<u8>>) -> Vec<Option<Vec<u8>>> {
        match self {
            Self::Shared(core) => core.store.batch_get(keys),
            Self::Threaded(store) => {
                let total = keys.len();
                if total == 0 {
                    return Vec::new();
                }
                if store.uses_caller_local_routes() {
                    let mut groups = vec![
                        Vec::<(usize, EmbeddedKeyRoute, Vec<u8>)>::new();
                        store.worker_count()
                    ];
                    for (index, key) in keys.into_iter().enumerate() {
                        let (worker_id, route) = store.routed_key(&key);
                        groups[worker_id].push((index, route, key));
                    }
                    let mut values = vec![None; total];
                    for (worker_id, group) in groups.into_iter().enumerate() {
                        if group.is_empty() {
                            continue;
                        }
                        let group_keys = group
                            .iter()
                            .map(|(_, route, key)| (*route, key.clone()))
                            .collect::<Vec<_>>();
                        let results = store.workers[worker_id].run_store(move |inner| {
                            group_keys
                                .iter()
                                .map(|(route, key)| {
                                    inner.get_ref_routed_local(*route, key).map(<[u8]>::to_vec)
                                })
                                .collect::<Vec<_>>()
                        });
                        for ((index, _, _), value) in group.into_iter().zip(results) {
                            values[index] = value;
                        }
                    }
                    return values;
                }

                let mut groups = vec![Vec::<(usize, Vec<u8>)>::new(); store.worker_count()];
                for (index, key) in keys.into_iter().enumerate() {
                    let worker_id = store.route_key(&key);
                    groups[worker_id].push((index, key));
                }
                let mut values = vec![None; total];
                for (worker_id, group) in groups.into_iter().enumerate() {
                    if group.is_empty() {
                        continue;
                    }
                    let group_keys = group.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
                    let results = store.workers[worker_id]
                        .run_store(move |inner| inner.batch_get(group_keys));
                    for ((index, _), value) in group.into_iter().zip(results) {
                        values[index] = value;
                    }
                }
                values
            }
        }
    }

    fn batch_get_view(&self, keys: &[Bytes]) -> PyReadBatchInner {
        match self {
            Self::Shared(core) => PyReadBatchInner::Single(PyReadBatchViewInner::Shared(
                core.store.batch_get_view(keys),
            )),
            Self::Threaded(store) => {
                let total = keys.len();
                if total == 0 {
                    return PyReadBatchInner::Routed {
                        batches: Vec::new(),
                        entries: Vec::new(),
                        lengths: Vec::new(),
                        hit_count: 0,
                        total_bytes: 0,
                    };
                }

                if store.uses_caller_local_routes() {
                    let mut groups =
                        vec![Vec::<(usize, EmbeddedKeyRoute, Bytes)>::new(); store.worker_count()];
                    for (index, key) in keys.iter().enumerate() {
                        let (worker_id, route) = store.routed_key(key);
                        groups[worker_id].push((index, route, key.clone()));
                    }

                    let mut batches = Vec::new();
                    let mut entries = vec![None; total];
                    let mut lengths = vec![0usize; total];
                    let mut hit_count = 0usize;
                    let mut total_bytes = 0usize;

                    for (worker_id, group) in groups.into_iter().enumerate() {
                        if group.is_empty() {
                            continue;
                        }
                        let group_keys = group
                            .iter()
                            .map(|(_, route, key)| (*route, key.clone()))
                            .collect::<Vec<_>>();
                        let batch = store.workers[worker_id].run_store(move |inner| {
                            inner.batch_get_owned_view_routed_local(&group_keys)
                        });
                        let batch_index = batches.len();
                        let batch_lengths = batch.lengths();
                        for (local_index, (original_index, _, _)) in group.iter().enumerate() {
                            let length = batch_lengths[local_index];
                            lengths[*original_index] = length;
                            if length > 0 {
                                entries[*original_index] = Some((batch_index, local_index));
                                hit_count += 1;
                                total_bytes += length;
                            }
                        }
                        batches.push(PyReadBatchViewInner::Owned(batch));
                    }

                    return PyReadBatchInner::Routed {
                        batches,
                        entries,
                        lengths,
                        hit_count,
                        total_bytes,
                    };
                }

                let mut groups = vec![Vec::<(usize, Bytes)>::new(); store.worker_count()];
                for (index, key) in keys.iter().enumerate() {
                    let worker_id = store.route_key(key);
                    groups[worker_id].push((index, key.clone()));
                }

                let mut batches = Vec::new();
                let mut entries = vec![None; total];
                let mut lengths = vec![0usize; total];
                let mut hit_count = 0usize;
                let mut total_bytes = 0usize;

                for (worker_id, group) in groups.into_iter().enumerate() {
                    if group.is_empty() {
                        continue;
                    }
                    let group_keys = group.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
                    let batch = store.workers[worker_id]
                        .run_store(move |inner| inner.batch_get_owned_view_local(&group_keys));
                    let batch_index = batches.len();
                    let batch_lengths = batch.lengths();
                    for (local_index, (original_index, _)) in group.iter().enumerate() {
                        let length = batch_lengths[local_index];
                        lengths[*original_index] = length;
                        if length > 0 {
                            entries[*original_index] = Some((batch_index, local_index));
                            hit_count += 1;
                            total_bytes += length;
                        }
                    }
                    batches.push(PyReadBatchViewInner::Owned(batch));
                }

                PyReadBatchInner::Routed {
                    batches,
                    entries,
                    lengths,
                    hit_count,
                    total_bytes,
                }
            }
        }
    }

    fn batch_get_session_view(
        &self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> PyReadBatchViewInner {
        match self {
            Self::Shared(core) => PyReadBatchViewInner::Shared(
                core.store.batch_get_session_view(session_prefix, keys),
            ),
            Self::Threaded(store) => {
                let (worker_id, route) = store.routed_session(session_prefix);
                let session_prefix = session_prefix.to_vec();
                let keys = keys.to_vec();
                if store.uses_caller_local_routes() {
                    PyReadBatchViewInner::Owned(store.workers[worker_id].run_store(move |inner| {
                        inner.batch_get_session_owned_view_routed_local(route, &keys)
                    }))
                } else {
                    PyReadBatchViewInner::Owned(store.workers[worker_id].run_store(move |inner| {
                        inner.batch_get_session_owned_view_local(&session_prefix, &keys)
                    }))
                }
            }
        }
    }

    fn batch_get_session_view_prehashed(
        &self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> PyReadBatchViewInner {
        match self {
            Self::Shared(core) => PyReadBatchViewInner::Shared(
                core.store
                    .batch_get_session_view_prehashed(session_prefix, keys, key_hashes),
            ),
            Self::Threaded(store) => {
                let (worker_id, route) = store.routed_session(session_prefix);
                let session_prefix = session_prefix.to_vec();
                let keys = keys.to_vec();
                let key_hashes = key_hashes.to_vec();
                if store.uses_caller_local_routes() {
                    PyReadBatchViewInner::Owned(store.workers[worker_id].run_store(move |inner| {
                        inner.batch_get_session_owned_view_prehashed_routed_local(
                            route,
                            &keys,
                            &key_hashes,
                        )
                    }))
                } else {
                    PyReadBatchViewInner::Owned(store.workers[worker_id].run_store(move |inner| {
                        inner.batch_get_session_owned_view_prehashed_local(
                            &session_prefix,
                            &keys,
                            &key_hashes,
                        )
                    }))
                }
            }
        }
    }

    fn batch_get_session_packed(&self, session_prefix: &[u8], keys: &[Bytes]) -> PackedBatch {
        match self {
            Self::Shared(core) => core.store.batch_get_session_packed(session_prefix, keys),
            Self::Threaded(store) => {
                let (worker_id, route) = store.routed_session(session_prefix);
                let session_prefix = session_prefix.to_vec();
                let keys = keys.to_vec();
                if store.uses_caller_local_routes() {
                    store.workers[worker_id].run_store(move |inner| {
                        inner.batch_get_session_packed_routed_local(route, &keys)
                    })
                } else {
                    store.workers[worker_id].run_store(move |inner| {
                        inner.batch_get_session_packed(&session_prefix, &keys)
                    })
                }
            }
        }
    }

    fn batch_get_packed(&self, keys: &[Bytes]) -> PackedBatch {
        match self {
            Self::Shared(core) => core.store.batch_get_packed(keys),
            Self::Threaded(store) => {
                let total = keys.len();
                if total == 0 {
                    return PackedBatch::default();
                }
                let mut groups = vec![Vec::<(usize, Bytes)>::new(); store.worker_count()];
                for (index, key) in keys.iter().enumerate() {
                    let worker_id = store.route_key(key);
                    groups[worker_id].push((index, key.clone()));
                }

                let mut offsets = vec![usize::MAX; total];
                let mut lengths = vec![0usize; total];
                let mut hit_count = 0usize;
                let mut buffer = Vec::new();

                for (worker_id, group) in groups.into_iter().enumerate() {
                    if group.is_empty() {
                        continue;
                    }
                    let group_keys = group.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
                    let packed = store.workers[worker_id]
                        .run_store(move |inner| inner.batch_get_packed(&group_keys));
                    for (local_index, (original_index, _)) in group.iter().enumerate() {
                        let local_offset = packed.offsets[local_index];
                        let length = packed.lengths[local_index];
                        lengths[*original_index] = length;
                        if local_offset == usize::MAX || length == 0 {
                            continue;
                        }
                        offsets[*original_index] = buffer.len();
                        buffer
                            .extend_from_slice(&packed.buffer[local_offset..local_offset + length]);
                        hit_count += 1;
                    }
                }

                PackedBatch {
                    buffer,
                    offsets,
                    lengths,
                    hit_count,
                }
            }
        }
    }

    fn delete(&self, key: &[u8]) -> bool {
        match self {
            Self::Shared(core) => {
                let shard_id = routed_shard_for_key(
                    core.store.shard_count(),
                    core.store.route_mode(),
                    false,
                    key,
                );
                let key = key.to_vec();
                let deleted = core.store.delete(&key);
                if deleted {
                    core.append_wal(
                        shard_id,
                        MutationOp::Del,
                        key,
                        Vec::new(),
                        None,
                        now_millis(),
                    );
                }
                deleted
            }
            Self::Threaded(store) => {
                let (worker_id, route) = store.routed_key(key);
                let key = key.to_vec();
                let wal_key = key.clone();
                let deleted = if store.uses_caller_local_routes() {
                    store.workers[worker_id]
                        .run_store(move |inner| inner.delete_routed_local(route, &key))
                } else {
                    store.workers[worker_id].run_store(move |inner| inner.delete(&key))
                };
                if deleted {
                    store.append_wal(
                        worker_id,
                        MutationOp::Del,
                        wal_key,
                        Vec::new(),
                        None,
                        now_millis(),
                    );
                }
                deleted
            }
        }
    }

    fn exists(&self, key: &[u8]) -> bool {
        match self {
            Self::Shared(core) => core.store.exists(key),
            Self::Threaded(store) => {
                let (worker_id, route) = store.routed_key(key);
                let key = key.to_vec();
                if store.uses_caller_local_routes() {
                    store.workers[worker_id]
                        .run_store(move |inner| inner.exists_routed_local(route, &key))
                } else {
                    store.workers[worker_id].run_store(move |inner| inner.exists(&key))
                }
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Shared(core) => core.store.len(),
            Self::Threaded(store) => store
                .workers
                .iter()
                .map(|worker| worker.run_store(|inner| inner.len()))
                .sum(),
        }
    }

    fn process_maintenance(&self) -> usize {
        match self {
            Self::Shared(core) => core.store.process_maintenance(),
            Self::Threaded(store) => store
                .workers
                .iter()
                .map(|worker| worker.run_store(|inner| inner.process_maintenance()))
                .sum(),
        }
    }

    #[cfg(feature = "telemetry")]
    fn export_metrics_prometheus(&self) -> Option<String> {
        match self {
            Self::Shared(core) => core.store.export_metrics_prometheus(),
            Self::Threaded(store) => store.export_metrics_prometheus(),
        }
    }

    #[cfg(feature = "telemetry")]
    fn metrics_snapshot(&self) -> Option<CacheMetricsSnapshot> {
        match self {
            Self::Shared(core) => core.store.metrics_snapshot(),
            Self::Threaded(store) => store.metrics_snapshot(),
        }
    }
}

#[pymethods]
impl PyPackedBatchResult {
    /// Returns the packed payload as one Python `bytes` object.
    ///
    /// This is still a copy today, but it avoids building one `bytes` object per
    /// retrieved chunk and gives callers a stable packed representation.
    fn payload(&self) -> Vec<u8> {
        self.inner.buffer.clone()
    }

    /// Total copied payload bytes across the batch.
    fn buffer_len(&self) -> usize {
        self.inner.total_bytes()
    }

    /// Number of requested items in the batch.
    fn item_count(&self) -> usize {
        self.inner.item_count()
    }

    /// Number of hits in the batch.
    fn hit_count(&self) -> usize {
        self.inner.hit_count
    }

    /// Whether every requested item was present.
    fn all_hit(&self) -> bool {
        self.inner.all_hit()
    }

    /// Offset of each item inside the packed payload, or `-1` for misses.
    fn offsets(&self) -> Vec<i64> {
        self.inner
            .offsets
            .iter()
            .map(|offset| {
                if *offset == usize::MAX {
                    -1
                } else {
                    *offset as i64
                }
            })
            .collect()
    }

    /// Length of each requested item. Misses are reported as `0`.
    fn lengths(&self) -> Vec<usize> {
        self.inner.lengths.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "PackedBatchResult(items={}, hits={}, bytes={})",
            self.item_count(),
            self.hit_count(),
            self.buffer_len()
        )
    }
}

#[pyclass(name = "Store")]
struct PyStore {
    inner: Arc<StoreCore>,
    wal_path: Option<String>,
    compress_wal: bool,
    max_memory_bytes: Option<usize>,
    eviction_policy: EvictionPolicy,
}

#[pyclass(name = "DirectVllmRestoreHandle", unsendable)]
struct PyDirectVllmRestoreHandle {
    store: Arc<StoreCore>,
    ticket: RefCell<Option<DirectVllmRestoreTicket>>,
}

impl Drop for PyDirectVllmRestoreHandle {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.get_mut().take() {
            self.store.cancel_vllm_paged_restore(ticket);
        }
    }
}

#[pymethods]
impl PyDirectVllmRestoreHandle {
    fn is_pending(&self) -> bool {
        self.ticket.borrow().is_some()
    }

    fn peek_report(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let ticket = self.ticket.borrow().as_ref().copied().ok_or_else(|| {
            PyValueError::new_err("direct vLLM restore handle is already resolved")
        })?;
        let store = Arc::clone(&self.store);
        let report = py
            .allow_threads(move || store.peek_vllm_paged_restore_report(ticket))
            .map_err(runtime_error_to_py)?;
        direct_vllm_restore_report_dict(py, report)
    }

    fn is_ready(&self, py: Python<'_>) -> PyResult<bool> {
        let Some(ticket) = *self.ticket.borrow() else {
            return Ok(false);
        };
        let store = Arc::clone(&self.store);
        py.allow_threads(move || store.is_vllm_paged_restore_ready(ticket))
            .map_err(runtime_error_to_py)
    }

    fn wait_on_stream(&self, py: Python<'_>, stream_ptr: u64) -> PyResult<bool> {
        let Some(ticket) = *self.ticket.borrow() else {
            return Ok(false);
        };
        let store = Arc::clone(&self.store);
        py.allow_threads(move || store.wait_vllm_paged_restore_on_stream(ticket, stream_ptr))
            .map_err(runtime_error_to_py)
    }

    fn try_wait(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        let Some(ticket) = *self.ticket.borrow() else {
            return Ok(None);
        };
        let store = Arc::clone(&self.store);
        let maybe_report = py
            .allow_threads(move || store.try_wait_vllm_paged_restore(ticket))
            .map_err(runtime_error_to_py)?;
        let Some(report) = maybe_report else {
            return Ok(None);
        };
        self.ticket.borrow_mut().take();
        direct_vllm_restore_report_dict(py, report).map(Some)
    }

    fn wait(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let ticket = self.ticket.borrow_mut().take().ok_or_else(|| {
            PyValueError::new_err("direct vLLM restore handle is already resolved")
        })?;
        let store = Arc::clone(&self.store);
        let report = py
            .allow_threads(move || store.wait_vllm_paged_restore(ticket))
            .map_err(runtime_error_to_py)?;
        direct_vllm_restore_report_dict(py, report)
    }

    fn cancel(&self) -> bool {
        let Some(ticket) = self.ticket.borrow_mut().take() else {
            return false;
        };
        self.store.cancel_vllm_paged_restore(ticket)
    }

    fn __repr__(&self) -> String {
        if let Some(ticket) = *self.ticket.borrow() {
            format!(
                "DirectVllmRestoreHandle(worker_id={}, pending_id={}, path_version='{}', pending=true)",
                ticket.worker_id,
                ticket.pending_id,
                ticket.path_version.as_str()
            )
        } else {
            "DirectVllmRestoreHandle(pending=false)".to_string()
        }
    }
}

#[pyclass(name = "PreparedLmcacheKeys")]
struct PyPreparedLmcacheKeys {
    inner: Arc<PreparedLmcacheKeys>,
}

#[pymethods]
impl PyPreparedLmcacheKeys {
    fn item_count(&self) -> usize {
        self.inner.encoded.len()
    }

    fn has_shared_session(&self) -> bool {
        self.inner.session_prefix.is_some()
    }

    fn __repr__(&self) -> String {
        format!(
            "PreparedLmcacheKeys(items={}, shared_session={})",
            self.item_count(),
            self.has_shared_session()
        )
    }
}

#[pyclass(name = "PreparedLmcachePutBatch")]
struct PyPreparedLmcachePutBatch {
    inner: Arc<PreparedLmcachePutBatch>,
}

#[pymethods]
impl PyPreparedLmcachePutBatch {
    fn item_count(&self) -> usize {
        self.inner.keys.len()
    }

    fn session_group_count(&self) -> usize {
        self.inner.session_groups.len()
    }

    fn has_generic_items(&self) -> bool {
        !self.inner.generic_indices.is_empty()
    }

    fn __repr__(&self) -> String {
        format!(
            "PreparedLmcachePutBatch(items={}, session_groups={}, generic_items={})",
            self.item_count(),
            self.session_group_count(),
            self.has_generic_items()
        )
    }
}

/// Python-visible zero-copy single-value guard.
#[pyclass(name = "ReadView")]
struct PyReadView {
    _owner: Py<PyStore>,
    inner: PyReadViewInner,
}

#[pymethods]
impl PyReadView {
    fn is_hit(&self) -> bool {
        self.inner.is_hit()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn memoryview(slf: &Bound<'_, Self>) -> PyResult<Py<PyMemoryView>> {
        PyMemoryView::from(slf.as_any()).map(Bound::unbind)
    }

    fn to_bytes(&self) -> Option<Vec<u8>> {
        self.inner.slice().map(<[u8]>::to_vec)
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let slice = slf
            .borrow()
            .inner
            .slice_meta()
            .ok_or_else(|| PyBufferError::new_err("requested value is missing"))?;
        unsafe { fill_view_from_readonly_slice(view, flags, slice, slf.into_any()) }
    }

    fn __repr__(&self) -> String {
        format!("ReadView(len={}, hit={})", self.len(), self.is_hit())
    }
}

/// Python-visible zero-copy batch guard.
///
/// The guard keeps one shard-local read epoch open so callers can create
/// `memoryview` objects over the store's live buffers without copying.
#[pyclass(name = "SessionReadBatch")]
struct PySessionReadBatch {
    _owner: Py<PyStore>,
    inner: PyReadBatchViewInner,
}

#[pymethods]
impl PySessionReadBatch {
    fn item_count(&self) -> usize {
        self.inner.item_count()
    }

    fn hit_count(&self) -> usize {
        self.inner.hit_count()
    }

    fn total_bytes(&self) -> usize {
        self.inner.total_bytes()
    }

    fn all_hit(&self) -> bool {
        self.inner.all_hit()
    }

    fn lengths(&self) -> Vec<usize> {
        self.inner.lengths()
    }

    fn __len__(&self) -> usize {
        self.item_count()
    }

    fn chunk(slf: &Bound<'_, Self>, py: Python<'_>, index: usize) -> PyResult<Py<PyChunkReadView>> {
        if index >= slf.borrow().inner.item_count() {
            return Err(PyIndexError::new_err("chunk index out of range"));
        }
        Py::new(
            py,
            PyChunkReadView {
                owner: slf.clone().unbind(),
                index,
            },
        )
    }

    fn memoryview_at(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        index: usize,
    ) -> PyResult<Option<Py<PyMemoryView>>> {
        if index >= slf.borrow().inner.item_count() {
            return Err(PyIndexError::new_err("chunk index out of range"));
        }
        if slf.borrow().inner.slice_meta(index).is_none() {
            return Ok(None);
        }
        let chunk = Py::new(
            py,
            PyChunkReadView {
                owner: slf.clone().unbind(),
                index,
            },
        )?;
        PyMemoryView::from(chunk.bind(py).as_any()).map(|view| Some(view.unbind()))
    }

    fn __repr__(&self) -> String {
        format!(
            "SessionReadBatch(items={}, hits={}, bytes={})",
            self.item_count(),
            self.hit_count(),
            self.total_bytes()
        )
    }
}

/// Python-visible zero-copy generic batch guard.
#[pyclass(name = "ReadBatch")]
struct PyReadBatch {
    _owner: Py<PyStore>,
    inner: PyReadBatchInner,
}

#[pymethods]
impl PyReadBatch {
    fn item_count(&self) -> usize {
        self.inner.item_count()
    }

    fn hit_count(&self) -> usize {
        self.inner.hit_count()
    }

    fn total_bytes(&self) -> usize {
        self.inner.total_bytes()
    }

    fn all_hit(&self) -> bool {
        self.inner.all_hit()
    }

    fn lengths(&self) -> Vec<usize> {
        self.inner.lengths()
    }

    fn __len__(&self) -> usize {
        self.item_count()
    }

    fn chunk(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        index: usize,
    ) -> PyResult<Py<PyReadBatchChunkView>> {
        if index >= slf.borrow().inner.item_count() {
            return Err(PyIndexError::new_err("chunk index out of range"));
        }
        Py::new(
            py,
            PyReadBatchChunkView {
                owner: slf.clone().unbind(),
                index,
            },
        )
    }

    fn memoryview_at(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        index: usize,
    ) -> PyResult<Option<Py<PyMemoryView>>> {
        if index >= slf.borrow().inner.item_count() {
            return Err(PyIndexError::new_err("chunk index out of range"));
        }
        if slf.borrow().inner.slice_meta(index).is_none() {
            return Ok(None);
        }
        let chunk = Py::new(
            py,
            PyReadBatchChunkView {
                owner: slf.clone().unbind(),
                index,
            },
        )?;
        PyMemoryView::from(chunk.bind(py).as_any()).map(|view| Some(view.unbind()))
    }

    fn __repr__(&self) -> String {
        format!(
            "ReadBatch(items={}, hits={}, bytes={})",
            self.item_count(),
            self.hit_count(),
            self.total_bytes()
        )
    }
}

/// A single zero-copy chunk view backed by a `SessionReadBatch`.
#[pyclass(name = "ChunkReadView")]
struct PyChunkReadView {
    owner: Py<PySessionReadBatch>,
    index: usize,
}

#[pymethods]
impl PyChunkReadView {
    fn is_hit(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.slice_meta(py)?.is_some())
    }

    fn len(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.slice_meta(py)?.map_or(0, |slice| slice.len()))
    }

    fn memoryview(slf: &Bound<'_, Self>) -> PyResult<Py<PyMemoryView>> {
        PyMemoryView::from(slf.as_any()).map(Bound::unbind)
    }

    fn to_bytes(&self, py: Python<'_>) -> PyResult<Option<Vec<u8>>> {
        Ok(self.slice_meta(py)?.map(|slice| slice.as_slice().to_vec()))
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let slice = slice_meta_from_chunk(&slf)?;
        unsafe { fill_view_from_readonly_slice(view, flags, slice, slf.into_any()) }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "ChunkReadView(index={}, len={}, hit={})",
            self.index,
            self.len(py)?,
            self.is_hit(py)?
        ))
    }
}

impl PyChunkReadView {
    fn slice_meta(&self, py: Python<'_>) -> PyResult<Option<EmbeddedReadSlice>> {
        let owner = self.owner.bind(py);
        let batch = owner
            .try_borrow()
            .map_err(|_| PyBufferError::new_err("session batch is already mutably borrowed"))?;
        Ok(batch.inner.slice_meta(self.index))
    }
}

/// A single zero-copy chunk view backed by a generic `ReadBatch`.
#[pyclass(name = "BatchChunkReadView")]
struct PyReadBatchChunkView {
    owner: Py<PyReadBatch>,
    index: usize,
}

#[pymethods]
impl PyReadBatchChunkView {
    fn is_hit(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.slice_meta(py)?.is_some())
    }

    fn len(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self.slice_meta(py)?.map_or(0, |slice| slice.len()))
    }

    fn memoryview(slf: &Bound<'_, Self>) -> PyResult<Py<PyMemoryView>> {
        PyMemoryView::from(slf.as_any()).map(Bound::unbind)
    }

    fn to_bytes(&self, py: Python<'_>) -> PyResult<Option<Vec<u8>>> {
        Ok(self.slice_meta(py)?.map(|slice| slice.as_slice().to_vec()))
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let slice = slice_meta_from_read_batch_chunk(&slf)?;
        unsafe { fill_view_from_readonly_slice(view, flags, slice, slf.into_any()) }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "BatchChunkReadView(index={}, len={}, hit={})",
            self.index,
            self.len(py)?,
            self.is_hit(py)?
        ))
    }
}

/// A zero-copy view over the payload section inside one encoded LMCache record.
#[pyclass(name = "BatchRecordPayloadView")]
struct PyReadBatchPayloadView {
    owner: Py<PyReadBatch>,
    index: usize,
    offset: usize,
}

#[pymethods]
impl PyReadBatchPayloadView {
    fn len(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(self
            .slice_meta(py)?
            .map_or(0, |slice| slice.len().saturating_sub(self.offset)))
    }

    fn memoryview(slf: &Bound<'_, Self>) -> PyResult<Py<PyMemoryView>> {
        PyMemoryView::from(slf.as_any()).map(Bound::unbind)
    }

    fn to_bytes(&self, py: Python<'_>) -> PyResult<Option<Vec<u8>>> {
        Ok(self
            .slice_meta(py)?
            .map(|slice| slice.as_slice()[self.offset..].to_vec()))
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let slice = slice_meta_from_read_batch_payload(&slf)?;
        unsafe {
            fill_view_from_readonly_range(
                view,
                flags,
                slice.as_ptr().wrapping_add(slf.borrow().offset),
                slice.len().saturating_sub(slf.borrow().offset),
                slf.into_any(),
            )
        }
    }
}

impl PyReadBatchPayloadView {
    fn slice_meta(&self, py: Python<'_>) -> PyResult<Option<EmbeddedReadSlice>> {
        let owner = self.owner.bind(py);
        let batch = owner
            .try_borrow()
            .map_err(|_| PyBufferError::new_err("read batch is already mutably borrowed"))?;
        Ok(batch.inner.slice_meta(self.index))
    }
}

#[pymethods]
impl PyLmcacheRecordBatch {
    fn item_count(&self) -> usize {
        self.decoded.len()
    }

    fn hit_count(&self) -> usize {
        self.decoded.iter().filter(|item| item.is_some()).count()
    }

    fn __len__(&self) -> usize {
        self.item_count()
    }

    fn is_hit(&self, index: usize) -> PyResult<bool> {
        self.decoded
            .get(index)
            .map(|item| item.is_some())
            .ok_or_else(|| PyIndexError::new_err("record index out of range"))
    }

    fn payload_view_at(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        index: usize,
    ) -> PyResult<Option<Py<PyReadBatchPayloadView>>> {
        let this = slf.borrow();
        let Some(decoded) = this
            .decoded
            .get(index)
            .ok_or_else(|| PyIndexError::new_err("record index out of range"))?
        else {
            return Ok(None);
        };
        Py::new(
            py,
            PyReadBatchPayloadView {
                owner: this.owner.clone_ref(py),
                index,
                offset: decoded.payload_offset,
            },
        )
        .map(Some)
    }

    fn payload_memoryview_at(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        index: usize,
    ) -> PyResult<Option<Py<PyMemoryView>>> {
        let Some(payload_view) = Self::payload_view_at(slf, py, index)? else {
            return Ok(None);
        };
        PyMemoryView::from(payload_view.bind(py).as_any()).map(|view| Some(view.unbind()))
    }

    fn payload_len_at(&self, py: Python<'_>, index: usize) -> PyResult<usize> {
        let Some(decoded) = self
            .decoded
            .get(index)
            .ok_or_else(|| PyIndexError::new_err("record index out of range"))?
        else {
            return Ok(0);
        };
        let owner = self.owner.bind(py);
        let batch = owner
            .try_borrow()
            .map_err(|_| PyBufferError::new_err("read batch is already mutably borrowed"))?;
        Ok(batch.inner.slice_meta(index).map_or(0, |slice| {
            slice.len().saturating_sub(decoded.payload_offset)
        }))
    }

    fn metadata_at(&self, py: Python<'_>, index: usize) -> PyResult<Option<PyObject>> {
        let Some(decoded) = self
            .decoded
            .get(index)
            .ok_or_else(|| PyIndexError::new_err("record index out of range"))?
        else {
            return Ok(None);
        };
        let symbols = lmcache_python_symbols(py)?;
        build_lmcache_metadata(py, decoded.metadata.as_ref(), symbols).map(Some)
    }

    fn __repr__(&self) -> String {
        format!(
            "LmcacheRecordBatch(items={}, hits={})",
            self.item_count(),
            self.hit_count()
        )
    }
}

impl PyReadBatchChunkView {
    fn slice_meta(&self, py: Python<'_>) -> PyResult<Option<EmbeddedReadSlice>> {
        let owner = self.owner.bind(py);
        let batch = owner
            .try_borrow()
            .map_err(|_| PyBufferError::new_err("read batch is already mutably borrowed"))?;
        Ok(batch.inner.slice_meta(self.index))
    }
}

type VllmLayerRestoreSummary = (u32, usize, usize, usize, bool);

#[pymethods]
impl PyStore {
    #[new]
    #[pyo3(signature = (cores=1, wal_path=None, compress_wal=true, max_memory_bytes=None, eviction_policy="none", route_mode="full_key", enable_metrics=false, client_architecture="local_embedded", prefer_session_tags=false, numa_policy="off"))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        cores: usize,
        wal_path: Option<String>,
        compress_wal: bool,
        max_memory_bytes: Option<usize>,
        eviction_policy: &str,
        route_mode: &str,
        enable_metrics: bool,
        client_architecture: &str,
        prefer_session_tags: bool,
        numa_policy: &str,
    ) -> PyResult<Self> {
        let route_mode = parse_route_mode(route_mode)?;
        let eviction_policy = parse_eviction_policy(eviction_policy)?;
        let numa_policy = parse_numa_policy(numa_policy)?;
        let inner = Arc::new(StoreCore::new(
            cores,
            wal_path.as_deref(),
            compress_wal,
            max_memory_bytes,
            eviction_policy,
            route_mode,
            enable_metrics,
            client_architecture,
            prefer_session_tags,
            numa_policy,
        )?);

        Ok(Self {
            inner,
            wal_path,
            compress_wal,
            max_memory_bytes,
            eviction_policy,
        })
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn set(&self, py: Python<'_>, key: Vec<u8>, value: Vec<u8>, ttl: Option<u64>) {
        let ttl_ms = ttl.map(|seconds| seconds.saturating_mul(1_000));
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.set(key, value, ttl_ms));
    }

    #[pyo3(signature = (items, ttl=None))]
    fn batch_set(&self, py: Python<'_>, items: Vec<(Vec<u8>, Vec<u8>)>, ttl: Option<u64>) {
        let ttl_ms = ttl.map(|seconds| seconds.saturating_mul(1_000));
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.batch_set(items, ttl_ms));
    }

    fn batch_set_session_no_ttl(
        &self,
        py: Python<'_>,
        session_prefix: Vec<u8>,
        items: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.batch_set_session_owned_no_ttl(session_prefix, items));
    }

    fn batch_set_session_packed_no_ttl(
        &self,
        py: Python<'_>,
        session_prefix: Vec<u8>,
        items: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || {
            inner.batch_set_session_packed_items_no_ttl(session_prefix, items)
        });
    }

    fn batch_set_vllm_pages_no_ttl(
        &self,
        py: Python<'_>,
        session_prefix: PyBackedBytes,
        layer_index: u32,
        block_hashes: Vec<PyBackedBytes>,
        payloads: Vec<PyBackedBytes>,
    ) -> PyResult<usize> {
        if block_hashes.len() != payloads.len() {
            return Err(PyValueError::new_err(format!(
                "block_hash count {} does not match payload count {}",
                block_hashes.len(),
                payloads.len()
            )));
        }
        let session_prefix = session_prefix.as_ref().to_vec();
        let items = block_hashes
            .into_iter()
            .zip(payloads)
            .map(|(block_hash, payload)| {
                (
                    encode_vllm_page_key(layer_index, block_hash.as_ref()),
                    payload.as_ref().to_vec(),
                )
            })
            .collect::<Vec<_>>();
        let item_count = items.len();
        let inner = Arc::clone(&self.inner);
        let _ = session_prefix;
        py.allow_threads(move || inner.batch_set(items, None));
        Ok(item_count)
    }

    fn batch_set_vllm_layer_payloads_no_ttl(
        &self,
        py: Python<'_>,
        session_prefix: PyBackedBytes,
        layer_groups: Vec<(u32, Vec<PyBackedBytes>, Vec<PyBackedBytes>)>,
    ) -> PyResult<usize> {
        let mut items = Vec::new();
        for (layer_index, block_hashes, payloads) in layer_groups {
            if block_hashes.len() != payloads.len() {
                return Err(PyValueError::new_err(format!(
                    "block_hash count {} does not match payload count {} for layer {}",
                    block_hashes.len(),
                    payloads.len(),
                    layer_index
                )));
            }
            items.extend(
                block_hashes
                    .into_iter()
                    .zip(payloads)
                    .map(|(block_hash, payload)| {
                        (
                            encode_vllm_page_key(layer_index, block_hash.as_ref()),
                            payload.as_ref().to_vec(),
                        )
                    }),
            );
        }
        let item_count = items.len();
        let _ = session_prefix.as_ref();
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.batch_set(items, None));
        Ok(item_count)
    }

    fn batch_set_vllm_pages_from_layer_no_ttl(
        &self,
        py: Python<'_>,
        session_prefix: PyBackedBytes,
        layer_index: u32,
        block_hashes: Vec<PyBackedBytes>,
        block_ids: Vec<usize>,
        kv_layer: PyObject,
    ) -> PyResult<usize> {
        if block_hashes.len() != block_ids.len() {
            return Err(PyValueError::new_err(format!(
                "block_hash count {} does not match block_id count {}",
                block_hashes.len(),
                block_ids.len()
            )));
        }
        let payloads = extract_vllm_layer_payloads(py, kv_layer.bind(py), &block_ids)?;
        let items = block_hashes
            .into_iter()
            .zip(payloads)
            .map(|(block_hash, payload)| {
                (
                    encode_vllm_page_key(layer_index, block_hash.as_ref()),
                    payload,
                )
            })
            .collect::<Vec<_>>();
        let item_count = items.len();
        let _ = session_prefix.as_ref();
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.batch_set(items, None));
        Ok(item_count)
    }

    fn extract_vllm_layer_payload_bytes(
        &self,
        py: Python<'_>,
        kv_layer: PyObject,
        block_ids: Vec<usize>,
    ) -> PyResult<Vec<PyObject>> {
        let payloads = extract_vllm_layer_payloads(py, kv_layer.bind(py), &block_ids)?;
        Ok(payloads
            .into_iter()
            .map(|payload| PyBytes::new(py, &payload).into_any().unbind())
            .collect())
    }

    fn get(&self, py: Python<'_>, key: Vec<u8>) -> Option<Vec<u8>> {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.get(&key))
    }

    /// Returns a zero-copy read guard for one key.
    fn get_view(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        key: Vec<u8>,
    ) -> PyResult<Option<Py<PyReadView>>> {
        let store = Arc::clone(&slf.borrow().inner);
        let inner = py.allow_threads(move || store.get_view(&key));
        if !inner.is_hit() {
            return Ok(None);
        }
        Py::new(
            py,
            PyReadView {
                _owner: slf.clone().unbind(),
                inner,
            },
        )
        .map(Some)
    }

    fn batch_get(&self, py: Python<'_>, keys: Vec<Vec<u8>>) -> Vec<Option<Vec<u8>>> {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.batch_get(keys))
    }

    /// Returns a zero-copy generic batch guard.
    fn batch_get_view(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        keys: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyReadBatch>> {
        let store = Arc::clone(&slf.borrow().inner);
        let inner = py.allow_threads(move || store.batch_get_view(&keys));
        Py::new(
            py,
            PyReadBatch {
                _owner: slf.clone().unbind(),
                inner,
            },
        )
    }

    /// Returns LMCache `BytesBufferMemoryObj` instances with metadata decoded in Rust.
    ///
    /// This is a specialized hot path for the shardcache LMCache backend: it
    /// avoids per-record Python struct unpacking and builds the zero-copy
    /// `BytesBufferMemoryObj(memoryview(payload), metadata=...)` objects
    /// directly inside the extension.
    fn batch_get_lmcache_memory_objs(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        keys: Vec<Vec<u8>>,
    ) -> PyResult<Vec<Option<PyObject>>> {
        build_lmcache_memory_objs(slf, py, Arc::new(prepare_encoded_lmcache_keys(keys)))
    }

    /// Variant that accepts LMCache engine-key objects directly and performs
    /// `to_string()` inside the extension.
    fn batch_get_lmcache_memory_objs_from_engine_keys(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        keys: Vec<PyObject>,
    ) -> PyResult<Vec<Option<PyObject>>> {
        let prepared_keys = encode_lmcache_engine_keys(py, &keys)?;
        build_lmcache_memory_objs(slf, py, Arc::new(prepared_keys))
    }

    /// Returns one lower-level LMCache record batch with zero-copy payload
    /// views and decoded metadata access, without constructing `MemoryObj`
    /// wrappers for every hit.
    fn batch_get_lmcache_record_batch(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        keys: Vec<Vec<u8>>,
    ) -> PyResult<Py<PyLmcacheRecordBatch>> {
        build_lmcache_record_batch(slf, py, Arc::new(prepare_encoded_lmcache_keys(keys)))
    }

    /// Variant that accepts LMCache engine-key objects directly and performs
    /// `to_string()` inside the extension.
    fn batch_get_lmcache_record_batch_from_engine_keys(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        keys: Vec<PyObject>,
    ) -> PyResult<Py<PyLmcacheRecordBatch>> {
        let prepared_keys = encode_lmcache_engine_keys(py, &keys)?;
        build_lmcache_record_batch(slf, py, Arc::new(prepared_keys))
    }

    /// Prepares one reusable LMCache key batch handle from already-encoded keys.
    fn prepare_lmcache_encoded_keys(
        &self,
        py: Python<'_>,
        keys: Vec<Vec<u8>>,
    ) -> PyPreparedLmcacheKeys {
        py.allow_threads(|| PyPreparedLmcacheKeys {
            inner: Arc::new(prepare_encoded_lmcache_keys(keys)),
        })
    }

    fn prepare_lmcache_put_batch_encoded_keys(
        &self,
        py: Python<'_>,
        keys: Vec<PyBackedBytes>,
        metadata_blobs: Vec<PyBackedBytes>,
    ) -> PyResult<PyPreparedLmcachePutBatch> {
        py.allow_threads(|| {
            Ok(PyPreparedLmcachePutBatch {
                inner: Arc::new(prepare_lmcache_put_batch_from_pybacked_parts(
                    &keys,
                    &metadata_blobs,
                )?),
            })
        })
    }

    /// Prepares one reusable LMCache key batch handle from engine-key objects.
    fn prepare_lmcache_engine_keys(
        &self,
        py: Python<'_>,
        keys: Vec<PyObject>,
    ) -> PyResult<PyPreparedLmcacheKeys> {
        let prepared = encode_lmcache_engine_keys(py, &keys)?;
        Ok(PyPreparedLmcacheKeys {
            inner: Arc::new(prepared),
        })
    }

    /// Accepts LMCache engine-key objects and MemoryObj objects directly,
    /// encodes records inside Rust, and stores them without building Python
    /// `(key, bytes)` batches first.
    fn batch_put_lmcache_memory_objs_from_engine_keys(
        &self,
        py: Python<'_>,
        keys: Vec<PyObject>,
        objs: Vec<PyObject>,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let batch = encode_lmcache_put_batch(py, &keys, &objs)?;
        py.allow_threads(move || inner.batch_put_lmcache_encoded_batch(batch));
        Ok(())
    }

    /// Accepts already-encoded LMCache keys plus MemoryObj objects directly.
    fn batch_put_lmcache_memory_objs_encoded_keys(
        &self,
        py: Python<'_>,
        keys: Vec<PyBackedBytes>,
        objs: Vec<PyObject>,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let batch = encode_lmcache_put_batch_from_pybacked_keys(py, &keys, &objs)?;
        py.allow_threads(move || inner.batch_put_lmcache_encoded_batch(batch));
        Ok(())
    }

    /// Accepts already-encoded LMCache keys, payload buffers, and encoded
    /// metadata blobs directly. Callers can cache metadata encoding and avoid
    /// the generic MemoryObj walk on repeated puts.
    fn batch_put_lmcache_payloads_and_metadata_encoded_keys(
        &self,
        py: Python<'_>,
        keys: Vec<PyBackedBytes>,
        payloads: Vec<PyObject>,
        metadata_blobs: Vec<PyBackedBytes>,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let batch =
            encode_lmcache_put_batch_from_pybacked_parts(py, &keys, &payloads, &metadata_blobs)?;
        py.allow_threads(move || inner.batch_put_lmcache_encoded_batch(batch));
        Ok(())
    }

    fn batch_put_lmcache_payload_bytes_and_metadata_encoded_keys(
        &self,
        py: Python<'_>,
        keys: Vec<PyBackedBytes>,
        payloads: Vec<PyBackedBytes>,
        metadata_blobs: Vec<PyBackedBytes>,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || {
            let batch = encode_lmcache_put_batch_from_pybacked_byte_parts(
                &keys,
                &payloads,
                &metadata_blobs,
            )?;
            inner.batch_put_lmcache_encoded_batch(batch);
            Ok(())
        })
    }

    fn batch_put_lmcache_payloads_prepared(
        &self,
        py: Python<'_>,
        prepared: &Bound<'_, PyPreparedLmcachePutBatch>,
        payloads: Vec<PyObject>,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let prepared = Arc::clone(&prepared.borrow().inner);
        let batch = encode_lmcache_put_batch_from_prepared_parts(py, &prepared, &payloads)?;
        py.allow_threads(move || inner.batch_put_lmcache_encoded_batch(batch));
        Ok(())
    }

    fn batch_put_lmcache_payload_bytes_prepared(
        &self,
        py: Python<'_>,
        prepared: &Bound<'_, PyPreparedLmcachePutBatch>,
        payloads: Vec<PyBackedBytes>,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let prepared = Arc::clone(&prepared.borrow().inner);
        py.allow_threads(move || {
            let batch = encode_lmcache_put_batch_from_prepared_byte_parts(&prepared, &payloads)?;
            inner.batch_put_lmcache_encoded_batch(batch);
            Ok(())
        })
    }

    fn batch_put_lmcache_memory_objs_prepared_bytes(
        &self,
        py: Python<'_>,
        prepared: &Bound<'_, PyPreparedLmcachePutBatch>,
        objs: Vec<PyObject>,
    ) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let prepared = Arc::clone(&prepared.borrow().inner);
        let payloads = extract_lmcache_memory_obj_bytes_payloads(py, &objs)?;
        py.allow_threads(move || {
            let batch = encode_lmcache_put_batch_from_prepared_byte_parts(&prepared, &payloads)?;
            inner.batch_put_lmcache_encoded_batch(batch);
            Ok(())
        })
    }

    /// Reuses a prepared LMCache key batch for repeated zero-copy retrievals.
    fn batch_get_lmcache_memory_objs_prepared(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        prepared: &Bound<'_, PyPreparedLmcacheKeys>,
    ) -> PyResult<Vec<Option<PyObject>>> {
        let prepared = Arc::clone(&prepared.borrow().inner);
        build_lmcache_memory_objs(slf, py, prepared)
    }

    /// Reuses a prepared LMCache key batch for repeated lower-level record
    /// retrievals without forcing immediate `MemoryObj` construction.
    fn batch_get_lmcache_record_batch_prepared(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        prepared: &Bound<'_, PyPreparedLmcacheKeys>,
    ) -> PyResult<Py<PyLmcacheRecordBatch>> {
        let prepared = Arc::clone(&prepared.borrow().inner);
        build_lmcache_record_batch(slf, py, prepared)
    }

    fn get_lmcache_memory_obj_from_engine_key(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        key: PyObject,
    ) -> PyResult<Option<PyObject>> {
        let mut results = Self::batch_get_lmcache_memory_objs_from_engine_keys(slf, py, vec![key])?;
        Ok(results.pop().flatten())
    }

    /// Returns one packed result object for a known session batch.
    fn batch_get_session_packed(
        &self,
        py: Python<'_>,
        session_prefix: Vec<u8>,
        keys: Vec<Vec<u8>>,
    ) -> PyPackedBatchResult {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || PyPackedBatchResult {
            inner: inner.batch_get_session_packed(&session_prefix, &keys),
        })
    }

    /// Returns one packed result object for a generic batch.
    fn batch_get_packed(&self, py: Python<'_>, keys: Vec<Vec<u8>>) -> PyPackedBatchResult {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || PyPackedBatchResult {
            inner: inner.batch_get_packed(&keys),
        })
    }

    /// Returns a zero-copy session batch guard. Call `memoryview_at()` on the
    /// result to expose individual chunks without copying.
    fn batch_get_session_view(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        session_prefix: Vec<u8>,
        keys: Vec<Vec<u8>>,
    ) -> PyResult<Py<PySessionReadBatch>> {
        let store = Arc::clone(&slf.borrow().inner);
        let inner = py.allow_threads(move || store.batch_get_session_view(&session_prefix, &keys));
        Py::new(
            py,
            PySessionReadBatch {
                _owner: slf.clone().unbind(),
                inner,
            },
        )
    }

    fn batch_get_vllm_pages_view(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        session_prefix: PyBackedBytes,
        layer_index: u32,
        block_hashes: Vec<PyBackedBytes>,
    ) -> PyResult<Py<PyReadBatch>> {
        let keys = encode_vllm_page_keys(layer_index, &block_hashes);
        let store = Arc::clone(&slf.borrow().inner);
        let _ = session_prefix.as_ref();
        let inner = py.allow_threads(move || store.batch_get_view(&keys));
        Py::new(
            py,
            PyReadBatch {
                _owner: slf.clone().unbind(),
                inner,
            },
        )
    }

    fn restore_vllm_pages_into_layer(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        session_prefix: PyBackedBytes,
        layer_index: u32,
        block_hashes: Vec<PyBackedBytes>,
        block_ids: Vec<usize>,
        kv_layer: PyObject,
    ) -> PyResult<(usize, usize, usize, bool)> {
        let target_count = block_hashes.len().min(block_ids.len());
        if target_count == 0 {
            return Ok((0, 0, 0, true));
        }

        let keys = encode_vllm_page_keys(layer_index, &block_hashes[..target_count]);
        let store = Arc::clone(&slf.borrow().inner);
        let _ = session_prefix.as_ref();
        let inner = py.allow_threads(move || store.batch_get_view(&keys));
        let batch = Py::new(
            py,
            PyReadBatch {
                _owner: slf.clone().unbind(),
                inner,
            },
        )?;

        let kv_layer = kv_layer.bind(py);
        let mut hit_pages = 0usize;
        let mut missed_pages = 0usize;
        for (index, &block_id) in block_ids[..target_count].iter().enumerate() {
            let payload_len = {
                let batch_ref = batch.bind(py).borrow();
                match batch_ref.inner.slice_meta(index) {
                    Some(slice) => slice.len(),
                    None => {
                        missed_pages += 1;
                        continue;
                    }
                }
            };
            let page = extract_vllm_layer_page(py, kv_layer, block_id)?;
            let payload_view = read_batch_memoryview_at(py, &batch, index)?;
            copy_payload_into_vllm_page(
                py,
                page.bind(py),
                payload_view.bind(py).as_any(),
                payload_len,
            )?;
            hit_pages += 1;
        }

        Ok((target_count, hit_pages, missed_pages, missed_pages == 0))
    }

    fn restore_vllm_pages_into_registered_layers(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        session_prefix: PyBackedBytes,
        layer_indices: Vec<u32>,
        block_hashes: Vec<PyBackedBytes>,
        block_ids: Vec<usize>,
        kv_layers: Vec<PyObject>,
    ) -> PyResult<Vec<VllmLayerRestoreSummary>> {
        if layer_indices.len() != kv_layers.len() {
            return Err(PyValueError::new_err(format!(
                "layer_indices length {} does not match kv_layers length {}",
                layer_indices.len(),
                kv_layers.len()
            )));
        }

        let target_count = block_hashes.len().min(block_ids.len());
        if layer_indices.is_empty() || target_count == 0 {
            return Ok(layer_indices
                .into_iter()
                .map(|layer_index| (layer_index, 0, 0, 0, true))
                .collect());
        }

        let mut keys = Vec::with_capacity(layer_indices.len() * target_count);
        for &layer_index in &layer_indices {
            for block_hash in &block_hashes[..target_count] {
                keys.push(encode_vllm_page_key(layer_index, block_hash.as_ref()));
            }
        }

        let store = Arc::clone(&slf.borrow().inner);
        let _ = session_prefix.as_ref();
        let inner = py.allow_threads(move || store.batch_get_view(&keys));
        let batch = Py::new(
            py,
            PyReadBatch {
                _owner: slf.clone().unbind(),
                inner,
            },
        )?;

        let mut reports = Vec::with_capacity(layer_indices.len());
        for (layer_position, (&layer_index, kv_layer)) in
            layer_indices.iter().zip(kv_layers.iter()).enumerate()
        {
            let kv_layer = kv_layer.bind(py);
            let mut hit_pages = 0usize;
            let mut missed_pages = 0usize;
            for (block_position, &block_id) in block_ids[..target_count].iter().enumerate() {
                let payload_index = layer_position * target_count + block_position;
                let payload_len = {
                    let batch_ref = batch.bind(py).borrow();
                    match batch_ref.inner.slice_meta(payload_index) {
                        Some(slice) => slice.len(),
                        None => {
                            missed_pages += 1;
                            continue;
                        }
                    }
                };
                let page = extract_vllm_layer_page(py, kv_layer, block_id)?;
                let payload_view = read_batch_memoryview_at(py, &batch, payload_index)?;
                copy_payload_into_vllm_page(
                    py,
                    page.bind(py),
                    payload_view.bind(py).as_any(),
                    payload_len,
                )?;
                hit_pages += 1;
            }
            reports.push((
                layer_index,
                target_count,
                hit_pages,
                missed_pages,
                missed_pages == 0,
            ));
        }

        Ok(reports)
    }

    /// Benchmark-oriented fast path that avoids building Python result objects.
    fn batch_get_session_stats(
        &self,
        py: Python<'_>,
        session_prefix: Vec<u8>,
        keys: Vec<Vec<u8>>,
    ) -> (usize, bool) {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || {
            let packed = inner.batch_get_session_packed(&session_prefix, &keys);
            (packed.total_bytes(), packed.all_hit())
        })
    }

    fn count_vllm_cached_prefix_blocks(
        &self,
        py: Python<'_>,
        session_prefix: PyBackedBytes,
        block_hashes: Vec<PyBackedBytes>,
        layer_indices: Vec<u32>,
    ) -> usize {
        if block_hashes.is_empty() || layer_indices.is_empty() {
            return 0;
        }
        let mut keys = Vec::with_capacity(block_hashes.len() * layer_indices.len());
        for block_hash in &block_hashes {
            for &layer_index in &layer_indices {
                keys.push(encode_vllm_page_key(layer_index, block_hash.as_ref()));
            }
        }
        let layer_count = layer_indices.len();
        let _ = session_prefix.as_ref();
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || {
            let packed = inner.batch_get_packed(&keys);
            let mut matched_blocks = 0usize;
            for block_lengths in packed.lengths.chunks(layer_count) {
                if block_lengths.iter().all(|length| *length > 0) {
                    matched_blocks += 1;
                } else {
                    break;
                }
            }
            matched_blocks
        })
    }

    fn supported_vllm_restore_paths(&self) -> Vec<String> {
        let _ = self;
        DirectVllmRestorePathVersion::supported_names()
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    #[pyo3(signature = (
        session_prefix,
        requested_pages,
        block_allocations,
        allocation_id=0,
        device_ordinal=0,
        stream_ordinal=0,
        allow_cpu_fallback=true,
        cuda_enabled=true,
        cpu_fallback_host_ptr=None,
        cpu_fallback_base_offset_bytes=0,
        cpu_fallback_allocation_id=0,
        path_version="host_direct_v1"
    ))]
    #[allow(clippy::too_many_arguments)]
    fn submit_vllm_paged_restore(
        &self,
        py: Python<'_>,
        session_prefix: PyBackedBytes,
        requested_pages: Vec<(PyBackedBytes, u32, u32, usize)>,
        block_allocations: Vec<(usize, u64, usize)>,
        allocation_id: u64,
        device_ordinal: usize,
        stream_ordinal: usize,
        allow_cpu_fallback: bool,
        cuda_enabled: bool,
        cpu_fallback_host_ptr: Option<u64>,
        cpu_fallback_base_offset_bytes: u64,
        cpu_fallback_allocation_id: u64,
        path_version: &str,
    ) -> PyResult<Py<PyDirectVllmRestoreHandle>> {
        let path_version = DirectVllmRestorePathVersion::parse(path_version)?;
        let requested_pages = requested_pages
            .into_iter()
            .map(|(key, layer_index, page_index, len_bytes)| {
                VllmRequestedPage::new(key.as_ref().to_vec(), layer_index, page_index, len_bytes)
            })
            .collect::<Vec<_>>();
        let block_allocations = block_allocations
            .into_iter()
            .map(|(block_index, dst_device_ptr, block_size_bytes)| {
                VllmBlockAllocation::new(block_index, dst_device_ptr, block_size_bytes)
            })
            .collect::<Vec<_>>();
        let spec = VllmConnectorLoadSpec::new(
            session_prefix.as_ref().to_vec(),
            requested_pages,
            block_allocations,
        )
        .with_allocation_id(allocation_id)
        .with_gpu_target(device_ordinal, stream_ordinal)
        .with_allow_cpu_fallback(allow_cpu_fallback);
        let cpu_fallback = cpu_fallback_host_ptr.map(|dst_host_ptr| CpuTransferTarget {
            allocation_id: cpu_fallback_allocation_id,
            dst_host_ptr,
            dst_base_offset_bytes: cpu_fallback_base_offset_bytes,
        });
        let mut cuda = CudaConfig {
            enabled: cuda_enabled,
            device_ordinal,
            allow_cpu_fallback,
            ..CudaConfig::default()
        };
        cuda.device_ordinal = device_ordinal;
        let inner = Arc::clone(&self.inner);
        let ticket = py
            .allow_threads(move || {
                inner.submit_vllm_paged_restore_with_path(spec, cuda, cpu_fallback, path_version)
            })
            .map_err(runtime_error_to_py)?;
        Py::new(
            py,
            PyDirectVllmRestoreHandle {
                store: Arc::clone(&self.inner),
                ticket: RefCell::new(Some(ticket)),
            },
        )
    }

    #[pyo3(signature = (
        session_prefix,
        requested_pages,
        block_allocations,
        allocation_id=0,
        device_ordinal=0,
        stream_ordinal=0,
        allow_cpu_fallback=true,
        cuda_enabled=true,
        cpu_fallback_host_ptr=None,
        cpu_fallback_base_offset_bytes=0,
        cpu_fallback_allocation_id=0,
        path_version="host_direct_v1"
    ))]
    #[allow(clippy::too_many_arguments)]
    fn restore_vllm_paged(
        &self,
        py: Python<'_>,
        session_prefix: PyBackedBytes,
        requested_pages: Vec<(PyBackedBytes, u32, u32, usize)>,
        block_allocations: Vec<(usize, u64, usize)>,
        allocation_id: u64,
        device_ordinal: usize,
        stream_ordinal: usize,
        allow_cpu_fallback: bool,
        cuda_enabled: bool,
        cpu_fallback_host_ptr: Option<u64>,
        cpu_fallback_base_offset_bytes: u64,
        cpu_fallback_allocation_id: u64,
        path_version: &str,
    ) -> PyResult<Py<PyDict>> {
        let path_version = DirectVllmRestorePathVersion::parse(path_version)?;
        let requested_pages = requested_pages
            .into_iter()
            .map(|(key, layer_index, page_index, len_bytes)| {
                VllmRequestedPage::new(key.as_ref().to_vec(), layer_index, page_index, len_bytes)
            })
            .collect::<Vec<_>>();
        let block_allocations = block_allocations
            .into_iter()
            .map(|(block_index, dst_device_ptr, block_size_bytes)| {
                VllmBlockAllocation::new(block_index, dst_device_ptr, block_size_bytes)
            })
            .collect::<Vec<_>>();
        let spec = VllmConnectorLoadSpec::new(
            session_prefix.as_ref().to_vec(),
            requested_pages,
            block_allocations,
        )
        .with_allocation_id(allocation_id)
        .with_gpu_target(device_ordinal, stream_ordinal)
        .with_allow_cpu_fallback(allow_cpu_fallback);
        let cpu_fallback = cpu_fallback_host_ptr.map(|dst_host_ptr| CpuTransferTarget {
            allocation_id: cpu_fallback_allocation_id,
            dst_host_ptr,
            dst_base_offset_bytes: cpu_fallback_base_offset_bytes,
        });
        let mut cuda = CudaConfig {
            enabled: cuda_enabled,
            device_ordinal,
            allow_cpu_fallback,
            ..CudaConfig::default()
        };
        cuda.device_ordinal = device_ordinal;
        let inner = Arc::clone(&self.inner);
        let report = py
            .allow_threads(move || {
                inner.restore_vllm_paged_with_path(spec, cuda, cpu_fallback, path_version)
            })
            .map_err(runtime_error_to_py)?;
        direct_vllm_restore_report_dict(py, report)
    }

    /// Benchmark-oriented generic packed stats.
    fn batch_get_stats(&self, py: Python<'_>, keys: Vec<Vec<u8>>) -> (usize, bool) {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || {
            let packed = inner.batch_get_packed(&keys);
            (packed.total_bytes(), packed.all_hit())
        })
    }

    fn delete(&self, py: Python<'_>, key: Vec<u8>) -> bool {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.delete(&key))
    }

    fn exists(&self, py: Python<'_>, key: Vec<u8>) -> bool {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.exists(&key))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn process_maintenance(&self, py: Python<'_>) -> usize {
        let inner = Arc::clone(&self.inner);
        py.allow_threads(move || inner.process_maintenance())
    }

    fn export_metrics_prometheus(&self) -> PyResult<String> {
        #[cfg(feature = "telemetry")]
        {
            self.inner
                .export_metrics_prometheus()
                .ok_or_else(|| PyValueError::new_err("metrics are not enabled for this store"))
        }

        #[cfg(not(feature = "telemetry"))]
        {
            Err(PyValueError::new_err(
                "shardcache-py was built without the telemetry feature",
            ))
        }
    }

    fn metrics_snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        #[cfg(feature = "telemetry")]
        {
            let snapshot = self
                .inner
                .metrics_snapshot()
                .ok_or_else(|| PyValueError::new_err("metrics are not enabled for this store"))?;

            let dict = PyDict::new(py);
            dict.set_item("gets", snapshot.gets)?;
            dict.set_item("sets", snapshot.sets)?;
            dict.set_item("deletes", snapshot.deletes)?;
            dict.set_item("batch_gets", snapshot.batch_gets)?;
            dict.set_item("hits", snapshot.hits)?;
            dict.set_item("misses", snapshot.misses)?;
            dict.set_item("miss_rate", snapshot.miss_rate)?;
            dict.set_item("bytes_read", snapshot.bytes_read)?;
            dict.set_item("bytes_written", snapshot.bytes_written)?;
            dict.set_item("keys_total", snapshot.keys_total)?;
            dict.set_item("memory_bytes", snapshot.memory_bytes)?;
            dict.set_item("expirations", snapshot.expirations)?;
            dict.set_item("wal_writes", snapshot.wal_writes)?;
            dict.set_item("wal_bytes", snapshot.wal_bytes)?;

            let get_latency = PyDict::new(py);
            get_latency.set_item("count", snapshot.get_latency_ns.count)?;
            get_latency.set_item("sum", snapshot.get_latency_ns.sum)?;
            dict.set_item("get_latency_ns", get_latency)?;

            let set_latency = PyDict::new(py);
            set_latency.set_item("count", snapshot.set_latency_ns.count)?;
            set_latency.set_item("sum", snapshot.set_latency_ns.sum)?;
            dict.set_item("set_latency_ns", set_latency)?;

            let batch_latency = PyDict::new(py);
            batch_latency.set_item("count", snapshot.batch_get_latency_ns.count)?;
            batch_latency.set_item("sum", snapshot.batch_get_latency_ns.sum)?;
            dict.set_item("batch_get_latency_ns", batch_latency)?;

            let wal_flush_latency = PyDict::new(py);
            wal_flush_latency.set_item("count", snapshot.wal_flush_latency_ns.count)?;
            wal_flush_latency.set_item("sum", snapshot.wal_flush_latency_ns.sum)?;
            dict.set_item("wal_flush_latency_ns", wal_flush_latency)?;

            let shard_keys = PyDict::new(py);
            for gauge in snapshot.shard_keys {
                shard_keys.set_item(gauge.shard_id, gauge.value)?;
            }
            dict.set_item("shard_keys", shard_keys)?;

            let shard_ops = PyDict::new(py);
            for metric in snapshot.shard_ops {
                let nested = match shard_ops.get_item(metric.shard_id)? {
                    Some(existing) => existing.downcast_into::<PyDict>()?,
                    None => {
                        let created = PyDict::new(py);
                        shard_ops.set_item(metric.shard_id, &created)?;
                        created
                    }
                };
                nested.set_item(metric.op, metric.value)?;
            }
            dict.set_item("shard_ops", shard_ops)?;
            Ok(dict.unbind())
        }

        #[cfg(not(feature = "telemetry"))]
        {
            let _ = py;
            Err(PyValueError::new_err(
                "shardcache-py was built without the telemetry feature",
            ))
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Store(cores={}, wal_path={:?}, compress_wal={}, max_memory_bytes={:?}, eviction_policy={:?}, route_mode={:?})",
            self.inner.shard_count(),
            self.wal_path,
            self.compress_wal,
            self.max_memory_bytes,
            self.eviction_policy,
            self.inner.route_mode().as_str(),
        )
    }
}

#[pyclass(name = "DashMapStore")]
struct PyDashMapStore {
    inner: DashMap<Bytes, DashEntry, xxhash_rust::xxh3::Xxh3DefaultBuilder>,
    shards: usize,
}

#[pymethods]
impl PyDashMapStore {
    #[new]
    #[pyo3(signature = (shards=1))]
    fn new(shards: usize) -> Self {
        let shard_amount = usize::max(1, shards).next_power_of_two();
        Self {
            inner: DashMap::with_capacity_and_hasher_and_shard_amount(
                1_024,
                xxhash_rust::xxh3::Xxh3DefaultBuilder,
                shard_amount,
            ),
            shards: shard_amount,
        }
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn set(&self, py: Python<'_>, key: Vec<u8>, value: Vec<u8>, ttl: Option<u64>) {
        let expire_at_ms =
            ttl.map(|seconds| now_millis().saturating_add(seconds.saturating_mul(1_000)));
        py.allow_threads(|| {
            self.inner.insert(
                key,
                DashEntry {
                    value,
                    expire_at_ms,
                },
            );
        });
    }

    #[pyo3(signature = (items, ttl=None))]
    fn batch_set(&self, py: Python<'_>, items: Vec<(Vec<u8>, Vec<u8>)>, ttl: Option<u64>) {
        let expire_at_ms =
            ttl.map(|seconds| now_millis().saturating_add(seconds.saturating_mul(1_000)));
        py.allow_threads(|| {
            for (key, value) in items {
                self.inner.insert(
                    key,
                    DashEntry {
                        value,
                        expire_at_ms,
                    },
                );
            }
        });
    }

    fn get(&self, py: Python<'_>, key: Vec<u8>) -> Option<Vec<u8>> {
        py.allow_threads(|| self.get_inner(&key))
    }

    fn batch_get(&self, py: Python<'_>, keys: Vec<Vec<u8>>) -> Vec<Option<Vec<u8>>> {
        py.allow_threads(|| keys.into_iter().map(|key| self.get_inner(&key)).collect())
    }

    /// Returns one packed result object for a generic batch.
    fn batch_get_packed(&self, py: Python<'_>, keys: Vec<Vec<u8>>) -> PyPackedBatchResult {
        py.allow_threads(|| PyPackedBatchResult {
            inner: self.build_packed_batch(keys),
        })
    }

    /// Benchmark-oriented fast path that avoids building Python result objects.
    fn batch_get_stats(&self, py: Python<'_>, keys: Vec<Vec<u8>>) -> (usize, bool) {
        py.allow_threads(|| {
            let packed = self.build_packed_batch(keys);
            (packed.total_bytes(), packed.all_hit())
        })
    }

    fn delete(&self, py: Python<'_>, key: Vec<u8>) -> bool {
        py.allow_threads(|| self.inner.remove(&key).is_some())
    }

    fn exists(&self, py: Python<'_>, key: Vec<u8>) -> bool {
        py.allow_threads(|| self.get_inner(&key).is_some())
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn process_maintenance(&self, py: Python<'_>) -> usize {
        py.allow_threads(|| {
            let now_ms = now_millis();
            let expired = self
                .inner
                .iter()
                .filter(|entry| {
                    entry
                        .value()
                        .expire_at_ms
                        .is_some_and(|deadline| deadline <= now_ms)
                })
                .map(|entry| entry.key().clone())
                .collect::<Vec<_>>();
            let removed = expired.len();
            for key in expired {
                let _ = self.inner.remove(&key);
            }
            removed
        })
    }

    fn __repr__(&self) -> String {
        format!("DashMapStore(shards={})", self.shards)
    }
}

impl PyDashMapStore {
    fn get_inner(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_inner_ref(key, now_millis())
            .map(|entry| entry.value.clone())
    }

    fn get_inner_ref<'a>(
        &'a self,
        key: &[u8],
        now_ms: u64,
    ) -> Option<dashmap::mapref::one::Ref<'a, Bytes, DashEntry>> {
        let expired = self.inner.get(key).is_some_and(|entry| {
            entry
                .expire_at_ms
                .is_some_and(|deadline| deadline <= now_ms)
        });
        if expired {
            let _ = self.inner.remove(key);
            return None;
        }
        self.inner.get(key)
    }

    fn build_packed_batch(&self, keys: Vec<Vec<u8>>) -> PackedBatch {
        let now_ms = now_millis();
        let item_count = keys.len();
        let mut packed = PackedBatch::default();
        packed.offsets.reserve(item_count);
        packed.lengths.reserve(item_count);
        for key in keys {
            match self.get_inner_ref(&key, now_ms) {
                Some(value) => {
                    let bytes = value.value.as_slice();
                    if packed.buffer.capacity() == 0 && !bytes.is_empty() {
                        packed
                            .buffer
                            .reserve(bytes.len().saturating_mul(item_count));
                    }
                    let offset = packed.buffer.len();
                    packed.buffer.extend_from_slice(bytes);
                    packed.offsets.push(offset);
                    packed.lengths.push(bytes.len());
                    packed.hit_count += 1;
                }
                None => {
                    packed.offsets.push(usize::MAX);
                    packed.lengths.push(0);
                }
            }
        }
        packed
    }
}

#[pymodule]
fn shardcache(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPackedBatchResult>()?;
    module.add_class::<PyReadView>()?;
    module.add_class::<PyReadBatch>()?;
    module.add_class::<PyReadBatchChunkView>()?;
    module.add_class::<PySessionReadBatch>()?;
    module.add_class::<PyChunkReadView>()?;
    module.add_class::<PyLmcacheRecordBatch>()?;
    module.add_class::<PyPreparedLmcacheKeys>()?;
    module.add_class::<PyPreparedLmcachePutBatch>()?;
    module.add_class::<PyDirectVllmRestoreHandle>()?;
    module.add_class::<PyStore>()?;
    module.add_class::<PyDashMapStore>()?;
    module.add_class::<scnp_store::PyScnpStore>()?;
    module.add_function(wrap_pyfunction!(py_hash_key, module)?)?;
    Ok(())
}

fn runtime_error_to_py(error: shardcache_runtime::RuntimeError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn parse_route_mode(route_mode: &str) -> PyResult<EmbeddedRouteMode> {
    match route_mode {
        "full_key" => Ok(EmbeddedRouteMode::FullKey),
        "session_prefix" => Ok(EmbeddedRouteMode::SessionPrefix),
        other => Err(PyValueError::new_err(format!(
            "unsupported route_mode {other:?}; expected 'full_key' or 'session_prefix'"
        ))),
    }
}

fn parse_eviction_policy(eviction_policy: &str) -> PyResult<EvictionPolicy> {
    match eviction_policy {
        "none" => Ok(EvictionPolicy::None),
        "lru" => Ok(EvictionPolicy::Lru),
        "lfu" => Ok(EvictionPolicy::Lfu),
        #[cfg(feature = "prefix-eviction")]
        "prefix" | "prefix_eviction" | "prefix-lru" | "prefix_lru" => Ok(EvictionPolicy::Prefix),
        #[cfg(not(feature = "prefix-eviction"))]
        "prefix" | "prefix_eviction" | "prefix-lru" | "prefix_lru" => Err(PyValueError::new_err(
            "eviction_policy 'prefix' requires the 'prefix-eviction' cargo feature",
        )),
        other => Err(PyValueError::new_err(format!(
            "unsupported eviction_policy {other:?}; expected {}",
            expected_eviction_policy_values()
        ))),
    }
}

#[cfg(feature = "prefix-eviction")]
fn expected_eviction_policy_values() -> &'static str {
    "'none', 'lru', 'lfu', or 'prefix'"
}

#[cfg(not(feature = "prefix-eviction"))]
fn expected_eviction_policy_values() -> &'static str {
    "'none', 'lru', or 'lfu'"
}

fn parse_numa_policy(numa_policy: &str) -> PyResult<NumaRoutePolicy> {
    match numa_policy {
        "off" | "none" | "disabled" => Ok(NumaRoutePolicy::Off),
        "worker_pinned" | "pin_workers" | "pinned" => Ok(NumaRoutePolicy::WorkerPinned),
        "caller_local" | "thread_local" | "local" => Ok(NumaRoutePolicy::CallerLocal),
        other => Err(PyValueError::new_err(format!(
            "unsupported numa_policy {other:?}; expected 'off', 'worker_pinned', or 'caller_local'"
        ))),
    }
}

fn slice_meta_from_chunk(slf: &Bound<'_, PyChunkReadView>) -> PyResult<EmbeddedReadSlice> {
    let (owner, index) = {
        let chunk = slf.borrow();
        (chunk.owner.clone_ref(slf.py()), chunk.index)
    };

    let owner = owner.bind(slf.py());
    let batch = owner
        .try_borrow()
        .map_err(|_| PyBufferError::new_err("session batch is already mutably borrowed"))?;
    batch
        .inner
        .slice_meta(index)
        .ok_or_else(|| PyBufferError::new_err("requested chunk is missing"))
}

fn slice_meta_from_read_batch_chunk(
    slf: &Bound<'_, PyReadBatchChunkView>,
) -> PyResult<EmbeddedReadSlice> {
    let (owner, index) = {
        let chunk = slf.borrow();
        (chunk.owner.clone_ref(slf.py()), chunk.index)
    };

    let owner = owner.bind(slf.py());
    let batch = owner
        .try_borrow()
        .map_err(|_| PyBufferError::new_err("read batch is already mutably borrowed"))?;
    batch
        .inner
        .slice_meta(index)
        .ok_or_else(|| PyBufferError::new_err("requested chunk is missing"))
}

fn slice_meta_from_read_batch_payload(
    slf: &Bound<'_, PyReadBatchPayloadView>,
) -> PyResult<EmbeddedReadSlice> {
    let (owner, index) = {
        let chunk = slf.borrow();
        (chunk.owner.clone_ref(slf.py()), chunk.index)
    };

    let owner = owner.bind(slf.py());
    let batch = owner
        .try_borrow()
        .map_err(|_| PyBufferError::new_err("read batch is already mutably borrowed"))?;
    batch
        .inner
        .slice_meta(index)
        .ok_or_else(|| PyBufferError::new_err("requested chunk is missing"))
}

/// # Safety
///
/// `view` must be a valid `Py_buffer` pointer provided by CPython.
unsafe fn fill_view_from_readonly_slice(
    view: *mut ffi::Py_buffer,
    flags: c_int,
    slice: EmbeddedReadSlice,
    owner: Bound<'_, PyAny>,
) -> PyResult<()> {
    unsafe { fill_view_from_readonly_range(view, flags, slice.as_ptr(), slice.len(), owner) }
}

/// # Safety
///
/// `view` must be a valid `Py_buffer` pointer provided by CPython.
unsafe fn fill_view_from_readonly_range(
    view: *mut ffi::Py_buffer,
    flags: c_int,
    ptr: *const u8,
    len: usize,
    owner: Bound<'_, PyAny>,
) -> PyResult<()> {
    if view.is_null() {
        return Err(PyBufferError::new_err("view is null"));
    }
    if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
        return Err(PyBufferError::new_err("chunk views are read-only"));
    }

    unsafe {
        (*view).obj = owner.into_ptr();
        (*view).buf = ptr as *mut c_void;
        (*view).len = len as isize;
        (*view).readonly = 1;
        (*view).itemsize = 1;
        (*view).format = ffi::c_str!("B").as_ptr() as *mut _;
        (*view).ndim = 1;
        (*view).shape = &mut (*view).len;
        (*view).strides = &mut (*view).itemsize;
        (*view).suboffsets = ptr::null_mut();
        (*view).internal = ptr::null_mut();
    }
    Ok(())
}

const LMCACHE_MAGIC_V2: &[u8] = b"FCLM2\0";
const LMCACHE_HEADER_SIZE: usize = 4;
const LMCACHE_FIXED_META_SIZE: usize = 26;

#[derive(Debug)]
struct DecodedLmcacheRecord {
    metadata: Arc<DecodedLmcacheMetadata>,
    payload_offset: usize,
}

struct PreparedLmcacheBatch {
    batch: PyReadBatchInner,
    decoded: Vec<Option<DecodedLmcacheRecord>>,
}

/// Python-visible lower-level LMCache record batch.
///
/// This owns the underlying zero-copy `ReadBatch` and exposes payload views and
/// decoded metadata on demand, without forcing immediate `MemoryObj`
/// materialization for every hit.
#[pyclass(name = "LmcacheRecordBatch")]
struct PyLmcacheRecordBatch {
    owner: Py<PyReadBatch>,
    decoded: Vec<Option<DecodedLmcacheRecord>>,
}

struct EncodedLmcachePutBatch {
    packed_sessions: Vec<PackedSessionWrite>,
    generic_items: Vec<(Vec<u8>, Vec<u8>)>,
}

struct PreparedLmcacheKeys {
    encoded: Vec<Vec<u8>>,
    key_hashes: Vec<u64>,
    session_prefix: Option<Vec<u8>>,
}

struct PreparedLmcachePutSessionGroup {
    session_prefix: Vec<u8>,
    indices: Vec<usize>,
}

struct PreparedLmcachePutBatch {
    keys: Vec<Vec<u8>>,
    metadata_blobs: Vec<Vec<u8>>,
    session_groups: Vec<PreparedLmcachePutSessionGroup>,
    generic_indices: Vec<usize>,
}

#[derive(Debug)]
struct DecodedLmcacheMetadata {
    shape: Vec<i64>,
    dtype: Option<Vec<u8>>,
    address: u64,
    phy_size: u32,
    ref_count: u32,
    pin_count: u32,
    fmt_value: u8,
    cached_positions: Option<Vec<i64>>,
    shapes: Option<Vec<Vec<i64>>>,
    dtypes: Option<Vec<Vec<u8>>>,
}

struct LmcachePythonSymbols {
    metadata_cls: Py<PyAny>,
    bytes_buffer_cls: Py<PyAny>,
    memory_format_cls: Py<PyAny>,
    torch_module: Py<PyModule>,
    torch_size_cls: Py<PyAny>,
    torch_tensor_fn: Py<PyAny>,
    torch_int64: Py<PyAny>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LmcacheMetadataTemplateKey {
    shape: Vec<i64>,
    dtype: Option<Vec<u8>>,
    fmt_value: u8,
    shapes: Option<Vec<Vec<i64>>>,
    dtypes: Option<Vec<Vec<u8>>>,
}

struct LmcacheMetadataTemplate {
    shape: Py<PyAny>,
    dtype: Py<PyAny>,
    fmt: Py<PyAny>,
    shapes: Py<PyAny>,
    dtypes: Py<PyAny>,
}

impl LmcacheMetadataTemplate {
    fn clone_ref(&self, py: Python<'_>) -> Self {
        Self {
            shape: self.shape.clone_ref(py),
            dtype: self.dtype.clone_ref(py),
            fmt: self.fmt.clone_ref(py),
            shapes: self.shapes.clone_ref(py),
            dtypes: self.dtypes.clone_ref(py),
        }
    }
}

thread_local! {
    static LMCACHE_METADATA_TEMPLATE_CACHE: RefCell<FastHashMap<LmcacheMetadataTemplateKey, LmcacheMetadataTemplate>> =
        RefCell::new(FastHashMap::default());
    static LMCACHE_METADATA_BINARY_CACHE: RefCell<FastHashMap<usize, LmcacheMetadataBinaryCacheEntry>> =
        RefCell::new(FastHashMap::default());
}

static LMCACHE_PYTHON_SYMBOLS: GILOnceCell<LmcachePythonSymbols> = GILOnceCell::new();

struct LmcacheMetadataBinaryCacheEntry {
    owner: Py<PyAny>,
    encoded: Arc<Vec<u8>>,
}

fn lmcache_python_symbols(py: Python<'_>) -> PyResult<&LmcachePythonSymbols> {
    LMCACHE_PYTHON_SYMBOLS.get_or_try_init(py, || {
        let lmcache_module = PyModule::import(py, "lmcache.v1.memory_management")?;
        let metadata_cls = lmcache_module.getattr("MemoryObjMetadata")?.unbind();
        let bytes_buffer_cls = lmcache_module.getattr("BytesBufferMemoryObj")?.unbind();
        let memory_format_cls = lmcache_module.getattr("MemoryFormat")?.unbind();
        let torch_module = PyModule::import(py, "torch")?.unbind();
        let torch_bound = torch_module.bind(py);
        let torch_size_cls = torch_bound.getattr("Size")?.unbind();
        let torch_tensor_fn = torch_bound.getattr("tensor")?.unbind();
        let torch_int64 = torch_bound.getattr("int64")?.unbind();
        Ok(LmcachePythonSymbols {
            metadata_cls,
            bytes_buffer_cls,
            memory_format_cls,
            torch_module,
            torch_size_cls,
            torch_tensor_fn,
            torch_int64,
        })
    })
}

fn lmcache_metadata_template_key(metadata: &DecodedLmcacheMetadata) -> LmcacheMetadataTemplateKey {
    LmcacheMetadataTemplateKey {
        shape: metadata.shape.clone(),
        dtype: metadata.dtype.clone(),
        fmt_value: metadata.fmt_value,
        shapes: metadata.shapes.clone(),
        dtypes: metadata.dtypes.clone(),
    }
}

fn build_lmcache_metadata_template(
    py: Python<'_>,
    metadata: &DecodedLmcacheMetadata,
    symbols: &LmcachePythonSymbols,
) -> PyResult<LmcacheMetadataTemplate> {
    let torch_size_cls = symbols.torch_size_cls.bind(py);
    let torch_module = symbols.torch_module.bind(py);
    let memory_format_cls = symbols.memory_format_cls.bind(py);

    let shape = build_torch_size(py, torch_size_cls, &metadata.shape)?;
    let dtype = build_torch_dtype(py, torch_module, metadata.dtype.as_deref())?;
    let fmt = memory_format_cls.call1((metadata.fmt_value,))?.unbind();
    let shapes = match &metadata.shapes {
        Some(shapes) => {
            let out = shapes
                .iter()
                .map(|dims| build_torch_size(py, torch_size_cls, dims))
                .collect::<PyResult<Vec<_>>>()?;
            out.into_pyobject(py)?.unbind()
        }
        None => py.None(),
    };
    let dtypes = match &metadata.dtypes {
        Some(dtypes) => {
            let out = dtypes
                .iter()
                .map(|raw| build_torch_dtype(py, torch_module, Some(raw.as_slice())))
                .collect::<PyResult<Vec<_>>>()?;
            out.into_pyobject(py)?.unbind()
        }
        None => py.None(),
    };

    Ok(LmcacheMetadataTemplate {
        shape,
        dtype,
        fmt,
        shapes,
        dtypes,
    })
}

fn cached_lmcache_metadata_template(
    py: Python<'_>,
    metadata: &DecodedLmcacheMetadata,
    symbols: &LmcachePythonSymbols,
) -> PyResult<LmcacheMetadataTemplate> {
    let key = lmcache_metadata_template_key(metadata);
    LMCACHE_METADATA_TEMPLATE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(template) = cache.get(&key) {
            return Ok(template.clone_ref(py));
        }

        // The LMCache metadata shape space is usually tiny. If callers start
        // producing many distinct templates, drop the oldest generation
        // wholesale instead of growing this thread-local cache without bound.
        if cache.len() >= 256 {
            cache.clear();
        }

        let template = build_lmcache_metadata_template(py, metadata, symbols)?;
        cache.insert(key, template.clone_ref(py));
        Ok(template)
    })
}

fn decode_lmcache_record_v2_cached(
    raw: &[u8],
    metadata_cache: &mut FastHashMap<Vec<u8>, Arc<DecodedLmcacheMetadata>>,
) -> PyResult<DecodedLmcacheRecord> {
    if raw.len() < LMCACHE_MAGIC_V2.len() + LMCACHE_HEADER_SIZE {
        return Err(PyValueError::new_err("LMCache record is truncated"));
    }
    if &raw[..LMCACHE_MAGIC_V2.len()] != LMCACHE_MAGIC_V2 {
        return Err(PyValueError::new_err(
            "LMCache fast path only supports FCLM2 records",
        ));
    }

    let mut cursor = LMCACHE_MAGIC_V2.len();
    let meta_len = read_u32_be(raw, &mut cursor)? as usize;
    if raw.len() < cursor + meta_len {
        return Err(PyValueError::new_err("LMCache metadata is truncated"));
    }

    let meta_slice = &raw[cursor..cursor + meta_len];
    let metadata = if let Some(existing) = metadata_cache.get(meta_slice) {
        Arc::clone(existing)
    } else {
        let decoded = Arc::new(decode_lmcache_metadata_v2(meta_slice)?);
        metadata_cache.insert(meta_slice.to_vec(), Arc::clone(&decoded));
        decoded
    };
    cursor += meta_len;
    Ok(DecodedLmcacheRecord {
        metadata,
        payload_offset: cursor,
    })
}

fn encode_lmcache_engine_keys(py: Python<'_>, keys: &[PyObject]) -> PyResult<PreparedLmcacheKeys> {
    let mut encoded = Vec::with_capacity(keys.len());
    for key in keys {
        let key_string = key
            .bind(py)
            .call_method0("to_string")?
            .extract::<String>()?;
        encoded.push(key_string.into_bytes());
    }
    Ok(prepare_encoded_lmcache_keys(encoded))
}

fn encode_lmcache_put_batch(
    py: Python<'_>,
    keys: &[PyObject],
    objs: &[PyObject],
) -> PyResult<EncodedLmcachePutBatch> {
    let mut encoded_keys = Vec::with_capacity(keys.len());
    for key in keys {
        let key_string = key
            .bind(py)
            .call_method0("to_string")?
            .extract::<String>()?;
        encoded_keys.push(key_string.into_bytes());
    }
    encode_lmcache_put_batch_from_encoded_keys(py, &encoded_keys, objs)
}

fn encode_lmcache_put_batch_from_encoded_keys(
    py: Python<'_>,
    keys: &[Vec<u8>],
    objs: &[PyObject],
) -> PyResult<EncodedLmcachePutBatch> {
    if keys.len() != objs.len() {
        return Err(PyValueError::new_err(format!(
            "LMCache put batch length mismatch: {} keys vs {} objects",
            keys.len(),
            objs.len()
        )));
    }

    let mut grouped = FastHashMap::<Vec<u8>, PackedSessionWrite>::default();
    let mut generic_items = Vec::new();
    for (key, obj) in keys.iter().zip(objs) {
        let encoded_key = key.clone();
        let session_prefix = extract_lmcache_session_prefix(&encoded_key);
        if let Some(session_prefix) = session_prefix {
            let batch = grouped.entry(session_prefix.clone()).or_insert_with(|| {
                PackedSessionWrite::with_capacity(session_prefix.clone(), 32, 0)
            });
            let offset = batch.value_buffer_len();
            encode_lmcache_memory_obj_into(py, obj.bind(py), batch.value_buffer_mut())?;
            let len = batch.value_buffer_len() - offset;
            batch.push_prepacked_record(encoded_key, offset, len);
        } else {
            let value = encode_lmcache_memory_obj(py, obj.bind(py))?;
            generic_items.push((encoded_key, value));
        }
    }
    Ok(EncodedLmcachePutBatch {
        packed_sessions: grouped.into_values().collect(),
        generic_items,
    })
}

fn encode_lmcache_put_batch_from_pybacked_keys(
    py: Python<'_>,
    keys: &[PyBackedBytes],
    objs: &[PyObject],
) -> PyResult<EncodedLmcachePutBatch> {
    if keys.len() != objs.len() {
        return Err(PyValueError::new_err(format!(
            "LMCache put batch length mismatch: {} keys vs {} objects",
            keys.len(),
            objs.len()
        )));
    }

    let mut grouped = FastHashMap::<Vec<u8>, PackedSessionWrite>::default();
    let mut generic_items = Vec::new();
    for (key, obj) in keys.iter().zip(objs) {
        let encoded_key = key.as_ref().to_vec();
        let session_prefix = extract_lmcache_session_prefix(key.as_ref());
        if let Some(session_prefix) = session_prefix {
            let batch = grouped.entry(session_prefix.clone()).or_insert_with(|| {
                PackedSessionWrite::with_capacity(session_prefix.clone(), 32, 0)
            });
            let offset = batch.value_buffer_len();
            encode_lmcache_memory_obj_into(py, obj.bind(py), batch.value_buffer_mut())?;
            let len = batch.value_buffer_len() - offset;
            batch.push_prepacked_record(encoded_key, offset, len);
        } else {
            let value = encode_lmcache_memory_obj(py, obj.bind(py))?;
            generic_items.push((encoded_key, value));
        }
    }

    Ok(EncodedLmcachePutBatch {
        packed_sessions: grouped.into_values().collect(),
        generic_items,
    })
}

fn encode_lmcache_put_batch_from_pybacked_parts(
    py: Python<'_>,
    keys: &[PyBackedBytes],
    payloads: &[PyObject],
    metadata_blobs: &[PyBackedBytes],
) -> PyResult<EncodedLmcachePutBatch> {
    if keys.len() != payloads.len() || keys.len() != metadata_blobs.len() {
        return Err(PyValueError::new_err(format!(
            "LMCache put batch length mismatch: {} keys vs {} payloads vs {} metadata blobs",
            keys.len(),
            payloads.len(),
            metadata_blobs.len()
        )));
    }

    let mut grouped = FastHashMap::<Vec<u8>, PackedSessionWrite>::default();
    let mut generic_items = Vec::new();
    for ((key, payload), metadata_blob) in keys.iter().zip(payloads).zip(metadata_blobs) {
        let encoded_key = key.as_ref().to_vec();
        let session_prefix = extract_lmcache_session_prefix(key.as_ref());
        if let Some(session_prefix) = session_prefix {
            let batch = grouped.entry(session_prefix.clone()).or_insert_with(|| {
                PackedSessionWrite::with_capacity(session_prefix.clone(), 32, 0)
            });
            let offset = batch.value_buffer_len();
            append_lmcache_record_from_parts(
                py,
                payload.bind(py),
                metadata_blob.as_ref(),
                batch.value_buffer_mut(),
            )?;
            let len = batch.value_buffer_len() - offset;
            batch.push_prepacked_record(encoded_key, offset, len);
        } else {
            let value =
                encode_lmcache_record_from_parts(py, payload.bind(py), metadata_blob.as_ref())?;
            generic_items.push((encoded_key, value));
        }
    }

    Ok(EncodedLmcachePutBatch {
        packed_sessions: grouped.into_values().collect(),
        generic_items,
    })
}

fn encode_lmcache_put_batch_from_pybacked_byte_parts(
    keys: &[PyBackedBytes],
    payloads: &[PyBackedBytes],
    metadata_blobs: &[PyBackedBytes],
) -> PyResult<EncodedLmcachePutBatch> {
    if keys.len() != payloads.len() || keys.len() != metadata_blobs.len() {
        return Err(PyValueError::new_err(format!(
            "LMCache put batch length mismatch: {} keys vs {} payload bytes vs {} metadata blobs",
            keys.len(),
            payloads.len(),
            metadata_blobs.len()
        )));
    }

    let mut grouped = FastHashMap::<Vec<u8>, PackedSessionWrite>::default();
    let mut generic_items = Vec::new();
    for ((key, payload), metadata_blob) in keys.iter().zip(payloads).zip(metadata_blobs) {
        let encoded_key = key.as_ref().to_vec();
        let session_prefix = extract_lmcache_session_prefix(key.as_ref());
        if let Some(session_prefix) = session_prefix {
            let batch = grouped.entry(session_prefix.clone()).or_insert_with(|| {
                PackedSessionWrite::with_capacity(session_prefix.clone(), 32, 0)
            });
            let offset = batch.value_buffer_len();
            append_lmcache_record_from_bytes_parts(
                payload.as_ref(),
                metadata_blob.as_ref(),
                batch.value_buffer_mut(),
            );
            let len = batch.value_buffer_len() - offset;
            batch.push_prepacked_record(encoded_key, offset, len);
        } else {
            let value =
                encode_lmcache_record_from_bytes_parts(payload.as_ref(), metadata_blob.as_ref());
            generic_items.push((encoded_key, value));
        }
    }

    Ok(EncodedLmcachePutBatch {
        packed_sessions: grouped.into_values().collect(),
        generic_items,
    })
}

fn prepare_lmcache_put_batch_from_pybacked_parts(
    keys: &[PyBackedBytes],
    metadata_blobs: &[PyBackedBytes],
) -> PyResult<PreparedLmcachePutBatch> {
    if keys.len() != metadata_blobs.len() {
        return Err(PyValueError::new_err(format!(
            "LMCache prepared put batch length mismatch: {} keys vs {} metadata blobs",
            keys.len(),
            metadata_blobs.len()
        )));
    }

    let mut session_group_ids = FastHashMap::<Vec<u8>, usize>::default();
    let mut prepared = PreparedLmcachePutBatch {
        keys: Vec::with_capacity(keys.len()),
        metadata_blobs: Vec::with_capacity(metadata_blobs.len()),
        session_groups: Vec::new(),
        generic_indices: Vec::new(),
    };

    for (index, (key, metadata_blob)) in keys.iter().zip(metadata_blobs).enumerate() {
        prepared.keys.push(key.as_ref().to_vec());
        prepared
            .metadata_blobs
            .push(metadata_blob.as_ref().to_vec());

        if let Some(session_prefix) = extract_lmcache_session_prefix(key.as_ref()) {
            let group_id = if let Some(existing) = session_group_ids.get(&session_prefix) {
                *existing
            } else {
                let next_id = prepared.session_groups.len();
                prepared
                    .session_groups
                    .push(PreparedLmcachePutSessionGroup {
                        session_prefix: session_prefix.clone(),
                        indices: Vec::new(),
                    });
                session_group_ids.insert(session_prefix, next_id);
                next_id
            };
            prepared.session_groups[group_id].indices.push(index);
        } else {
            prepared.generic_indices.push(index);
        }
    }

    Ok(prepared)
}

fn encode_lmcache_put_batch_from_prepared_parts(
    py: Python<'_>,
    prepared: &PreparedLmcachePutBatch,
    payloads: &[PyObject],
) -> PyResult<EncodedLmcachePutBatch> {
    if prepared.keys.len() != payloads.len() {
        return Err(PyValueError::new_err(format!(
            "LMCache prepared put payload mismatch: {} prepared items vs {} payloads",
            prepared.keys.len(),
            payloads.len()
        )));
    }

    let mut packed_sessions = Vec::with_capacity(prepared.session_groups.len());
    for group in &prepared.session_groups {
        let mut packed =
            PackedSessionWrite::with_capacity(group.session_prefix.clone(), group.indices.len(), 0);
        for &index in &group.indices {
            let offset = packed.value_buffer_len();
            append_lmcache_record_from_parts(
                py,
                payloads[index].bind(py),
                prepared.metadata_blobs[index].as_slice(),
                packed.value_buffer_mut(),
            )?;
            let len = packed.value_buffer_len() - offset;
            packed.push_prepacked_record(prepared.keys[index].clone(), offset, len);
        }
        packed_sessions.push(packed);
    }

    let mut generic_items = Vec::with_capacity(prepared.generic_indices.len());
    for &index in &prepared.generic_indices {
        let value = encode_lmcache_record_from_parts(
            py,
            payloads[index].bind(py),
            prepared.metadata_blobs[index].as_slice(),
        )?;
        generic_items.push((prepared.keys[index].clone(), value));
    }

    Ok(EncodedLmcachePutBatch {
        packed_sessions,
        generic_items,
    })
}

fn encode_lmcache_put_batch_from_prepared_byte_parts(
    prepared: &PreparedLmcachePutBatch,
    payloads: &[PyBackedBytes],
) -> PyResult<EncodedLmcachePutBatch> {
    if prepared.keys.len() != payloads.len() {
        return Err(PyValueError::new_err(format!(
            "LMCache prepared put payload mismatch: {} prepared items vs {} payload byte blobs",
            prepared.keys.len(),
            payloads.len()
        )));
    }

    let mut packed_sessions = Vec::with_capacity(prepared.session_groups.len());
    for group in &prepared.session_groups {
        let mut packed =
            PackedSessionWrite::with_capacity(group.session_prefix.clone(), group.indices.len(), 0);
        for &index in &group.indices {
            let offset = packed.value_buffer_len();
            append_lmcache_record_from_bytes_parts(
                payloads[index].as_ref(),
                prepared.metadata_blobs[index].as_slice(),
                packed.value_buffer_mut(),
            );
            let len = packed.value_buffer_len() - offset;
            packed.push_prepacked_record(prepared.keys[index].clone(), offset, len);
        }
        packed_sessions.push(packed);
    }

    let mut generic_items = Vec::with_capacity(prepared.generic_indices.len());
    for &index in &prepared.generic_indices {
        let value = encode_lmcache_record_from_bytes_parts(
            payloads[index].as_ref(),
            prepared.metadata_blobs[index].as_slice(),
        );
        generic_items.push((prepared.keys[index].clone(), value));
    }

    Ok(EncodedLmcachePutBatch {
        packed_sessions,
        generic_items,
    })
}

fn encode_lmcache_memory_obj(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let metadata = obj.getattr("metadata")?;
    let meta_bytes = encode_lmcache_metadata_binary_cached(py, &metadata)?;
    let byte_array = obj.getattr("byte_array")?;
    encode_lmcache_record_from_parts(py, &byte_array, meta_bytes.as_slice())
}

fn extract_lmcache_memory_obj_bytes_payloads(
    py: Python<'_>,
    objs: &[PyObject],
) -> PyResult<Vec<PyBackedBytes>> {
    let mut payloads = Vec::with_capacity(objs.len());
    for obj in objs {
        let byte_array = obj.bind(py).getattr("byte_array")?;
        let payload = byte_array.extract::<PyBackedBytes>().map_err(|_| {
            PyTypeError::new_err("LMCache byte_array must be Python bytes for this fast path")
        })?;
        payloads.push(payload);
    }
    Ok(payloads)
}

fn encode_lmcache_memory_obj_into(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    out: &mut Vec<u8>,
) -> PyResult<()> {
    let metadata = obj.getattr("metadata")?;
    let meta_bytes = encode_lmcache_metadata_binary_cached(py, &metadata)?;
    let byte_array = obj.getattr("byte_array")?;
    append_lmcache_record_from_parts(py, &byte_array, meta_bytes.as_slice(), out)
}

fn encode_lmcache_record_from_parts(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
    meta_bytes: &[u8],
) -> PyResult<Vec<u8>> {
    let mut out = Vec::new();
    append_lmcache_record_from_parts(py, payload, meta_bytes, &mut out)?;
    Ok(out)
}

fn encode_lmcache_record_from_bytes_parts(payload: &[u8], meta_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        LMCACHE_MAGIC_V2.len() + LMCACHE_HEADER_SIZE + meta_bytes.len() + payload.len(),
    );
    append_lmcache_record_from_bytes_parts(payload, meta_bytes, &mut out);
    out
}

fn append_lmcache_record_from_parts(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
    meta_bytes: &[u8],
    out: &mut Vec<u8>,
) -> PyResult<()> {
    let prefix_len = LMCACHE_MAGIC_V2.len() + LMCACHE_HEADER_SIZE + meta_bytes.len();
    if let Ok(buffer) = PyBuffer::<u8>::get(payload) {
        if let Some(slice) = buffer.as_slice(py) {
            out.reserve(prefix_len + slice.len());
            out.extend_from_slice(LMCACHE_MAGIC_V2);
            push_u32_be(out, meta_bytes.len() as u32);
            out.extend_from_slice(meta_bytes);
            let raw =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) };
            out.extend_from_slice(raw);
            return Ok(());
        }
        let owned = buffer.to_vec(py)?;
        out.reserve(prefix_len + owned.len());
        out.extend_from_slice(LMCACHE_MAGIC_V2);
        push_u32_be(out, meta_bytes.len() as u32);
        out.extend_from_slice(meta_bytes);
        out.extend_from_slice(&owned);
        return Ok(());
    }
    if let Ok(casted) = payload.call_method1("cast", ("B",))
        && let Ok(buffer) = PyBuffer::<u8>::get(&casted)
    {
        if let Some(slice) = buffer.as_slice(py) {
            out.reserve(prefix_len + slice.len());
            out.extend_from_slice(LMCACHE_MAGIC_V2);
            push_u32_be(out, meta_bytes.len() as u32);
            out.extend_from_slice(meta_bytes);
            let raw =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) };
            out.extend_from_slice(raw);
            return Ok(());
        }
        let owned = buffer.to_vec(py)?;
        out.reserve(prefix_len + owned.len());
        out.extend_from_slice(LMCACHE_MAGIC_V2);
        push_u32_be(out, meta_bytes.len() as u32);
        out.extend_from_slice(meta_bytes);
        out.extend_from_slice(&owned);
        return Ok(());
    }
    if let Ok(bytes_obj) = payload.call_method0("tobytes") {
        let owned = bytes_obj.extract::<Vec<u8>>()?;
        out.reserve(prefix_len + owned.len());
        out.extend_from_slice(LMCACHE_MAGIC_V2);
        push_u32_be(out, meta_bytes.len() as u32);
        out.extend_from_slice(meta_bytes);
        out.extend_from_slice(&owned);
        return Ok(());
    }
    Err(PyValueError::new_err(
        "LMCache byte_array is not a contiguous byte buffer",
    ))
}

fn append_lmcache_record_from_bytes_parts(payload: &[u8], meta_bytes: &[u8], out: &mut Vec<u8>) {
    out.reserve(LMCACHE_MAGIC_V2.len() + LMCACHE_HEADER_SIZE + meta_bytes.len() + payload.len());
    out.extend_from_slice(LMCACHE_MAGIC_V2);
    push_u32_be(out, meta_bytes.len() as u32);
    out.extend_from_slice(meta_bytes);
    out.extend_from_slice(payload);
}

fn encode_lmcache_metadata_binary(metadata: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let shape = metadata.getattr("shape")?.extract::<Vec<i64>>()?;
    if shape.len() > u8::MAX as usize {
        return Err(PyValueError::new_err(
            "LMCache metadata shape rank exceeds u8 range",
        ));
    }

    let dtype_value = metadata.getattr("dtype")?;
    let dtype = encode_optional_dtype_wire(&dtype_value)?;
    let shapes = extract_optional_shapes_compact(&metadata.getattr("shapes")?, &shape)?;
    let dtypes = extract_optional_dtypes_compact(&metadata.getattr("dtypes")?, dtype.as_deref())?;
    let cached_positions = extract_optional_i64_list(&metadata.getattr("cached_positions")?)?;
    let has_shapes = shapes.as_ref().is_some_and(|value| !value.is_empty());
    let has_dtypes = dtypes.as_ref().is_some_and(|value| !value.is_empty());

    let mut out =
        Vec::with_capacity(LMCACHE_FIXED_META_SIZE + shape.len() * std::mem::size_of::<i64>());
    push_u64_be(&mut out, metadata.getattr("address")?.extract::<u64>()?);
    push_u32_be(&mut out, metadata.getattr("phy_size")?.extract::<u32>()?);
    push_u32_be(&mut out, metadata.getattr("ref_count")?.extract::<u32>()?);
    push_u32_be(&mut out, metadata.getattr("pin_count")?.extract::<u32>()?);
    out.push(metadata.getattr("fmt")?.getattr("value")?.extract::<u8>()?);
    out.push(u8::from(dtype.is_some()));
    out.push(u8::from(has_shapes));
    out.push(u8::from(has_dtypes));
    out.push(u8::from(cached_positions.is_some()));
    out.push(shape.len() as u8);

    for dim in &shape {
        push_i64_be(&mut out, *dim);
    }

    if let Some(dtype) = dtype {
        push_blob(&mut out, &dtype)?;
    }

    if let Some(shapes) = shapes.filter(|value| !value.is_empty()) {
        push_u16_checked(&mut out, shapes.len(), "LMCache shapes count")?;
        for dims in shapes {
            push_u16_checked(&mut out, dims.len(), "LMCache shape rank")?;
            for dim in dims {
                push_i64_be(&mut out, dim);
            }
        }
    }

    if let Some(dtypes) = dtypes.filter(|value| !value.is_empty()) {
        push_u16_checked(&mut out, dtypes.len(), "LMCache dtype list length")?;
        for dtype in dtypes {
            push_blob(&mut out, &dtype)?;
        }
    }

    if let Some(cached_positions) = cached_positions {
        push_u32_checked(
            &mut out,
            cached_positions.len(),
            "LMCache cached positions length",
        )?;
        for position in cached_positions {
            push_i64_be(&mut out, position);
        }
    }

    Ok(out)
}

fn encode_lmcache_metadata_binary_cached(
    py: Python<'_>,
    metadata: &Bound<'_, PyAny>,
) -> PyResult<Arc<Vec<u8>>> {
    let ptr = metadata.as_ptr() as usize;
    if let Some(encoded) = LMCACHE_METADATA_BINARY_CACHE.with(|cache| {
        cache.borrow().get(&ptr).and_then(|entry| {
            (entry.owner.bind(py).as_ptr() == metadata.as_ptr()).then(|| Arc::clone(&entry.encoded))
        })
    }) {
        return Ok(encoded);
    }

    let encoded = Arc::new(encode_lmcache_metadata_binary(metadata)?);
    LMCACHE_METADATA_BINARY_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() >= 64 {
            cache.clear();
        }
        cache.insert(
            ptr,
            LmcacheMetadataBinaryCacheEntry {
                owner: metadata.clone().unbind(),
                encoded: Arc::clone(&encoded),
            },
        );
    });
    Ok(encoded)
}

fn encode_optional_dtype_wire(dtype: &Bound<'_, PyAny>) -> PyResult<Option<Vec<u8>>> {
    if dtype.is_none() {
        return Ok(None);
    }
    Ok(Some(dtype.str()?.to_str()?.as_bytes().to_vec()))
}

fn extract_optional_shapes_compact(
    value: &Bound<'_, PyAny>,
    shape: &[i64],
) -> PyResult<Option<Vec<Vec<i64>>>> {
    if value.is_none() {
        return Ok(None);
    }
    if let Ok(items) = value.extract::<Vec<Vec<i64>>>() {
        if items.len() == 1 && items.first().is_some_and(|dims| dims.as_slice() == shape) {
            return Ok(None);
        }
        return Ok(Some(items));
    }
    let mut out = Vec::new();
    for dims in value.try_iter()? {
        out.push(dims?.extract::<Vec<i64>>()?);
    }
    if out.len() == 1 && out.first().is_some_and(|dims| dims.as_slice() == shape) {
        return Ok(None);
    }
    Ok(Some(out))
}

fn extract_optional_dtypes_compact(
    value: &Bound<'_, PyAny>,
    dtype: Option<&[u8]>,
) -> PyResult<Option<Vec<Vec<u8>>>> {
    if value.is_none() {
        return Ok(None);
    }
    let mut out = Vec::new();
    for dtype_value in value.try_iter()? {
        out.push(dtype_value?.str()?.to_str()?.as_bytes().to_vec());
    }
    if out.len() == 1
        && out
            .first()
            .zip(dtype)
            .is_some_and(|(current, expected)| current.as_slice() == expected)
    {
        return Ok(None);
    }
    Ok(Some(out))
}

fn extract_optional_i64_list(value: &Bound<'_, PyAny>) -> PyResult<Option<Vec<i64>>> {
    if value.is_none() {
        return Ok(None);
    }
    if let Ok(items) = value.extract::<Vec<i64>>() {
        return Ok(Some(items));
    }
    Ok(Some(value.call_method0("tolist")?.extract::<Vec<i64>>()?))
}

fn push_blob(out: &mut Vec<u8>, payload: &[u8]) -> PyResult<()> {
    push_u16_checked(out, payload.len(), "LMCache metadata blob length")?;
    out.extend_from_slice(payload);
    Ok(())
}

fn push_u16_checked(out: &mut Vec<u8>, value: usize, label: &str) -> PyResult<()> {
    let value = u16::try_from(value)
        .map_err(|_| PyValueError::new_err(format!("{label} exceeds u16 range")))?;
    push_u16_be(out, value);
    Ok(())
}

fn push_u32_checked(out: &mut Vec<u8>, value: usize, label: &str) -> PyResult<()> {
    let value = u32::try_from(value)
        .map_err(|_| PyValueError::new_err(format!("{label} exceeds u32 range")))?;
    push_u32_be(out, value);
    Ok(())
}

fn push_u16_be(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn push_u32_be(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn push_u64_be(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn push_i64_be(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn build_lmcache_memory_objs(
    slf: &Bound<'_, PyStore>,
    py: Python<'_>,
    keys: Arc<PreparedLmcacheKeys>,
) -> PyResult<Vec<Option<PyObject>>> {
    let store = Arc::clone(&slf.borrow().inner);
    let prepared = py.allow_threads(move || prepare_lmcache_batch(store.as_ref(), &keys))?;
    let symbols = lmcache_python_symbols(py)?;
    let batch = Py::new(
        py,
        PyReadBatch {
            _owner: slf.clone().unbind(),
            inner: prepared.batch,
        },
    )?;
    let bytes_buffer_cls = symbols.bytes_buffer_cls.bind(py);

    let mut results = Vec::with_capacity(prepared.decoded.len());
    for (index, decoded) in prepared.decoded.into_iter().enumerate() {
        let Some(decoded) = decoded else {
            results.push(None);
            continue;
        };
        let payload_view = Py::new(
            py,
            PyReadBatchPayloadView {
                owner: batch.clone_ref(py),
                index,
                offset: decoded.payload_offset,
            },
        )?;
        let payload_memoryview = PyMemoryView::from(payload_view.bind(py).as_any())?;
        let metadata = build_lmcache_metadata(py, decoded.metadata.as_ref(), symbols)?;
        let memory_obj = bytes_buffer_cls.call1((payload_memoryview, metadata))?;
        results.push(Some(memory_obj.unbind()));
    }
    Ok(results)
}

fn build_lmcache_record_batch(
    slf: &Bound<'_, PyStore>,
    py: Python<'_>,
    keys: Arc<PreparedLmcacheKeys>,
) -> PyResult<Py<PyLmcacheRecordBatch>> {
    let store = Arc::clone(&slf.borrow().inner);
    let prepared = py.allow_threads(move || prepare_lmcache_batch(store.as_ref(), &keys))?;
    let batch = Py::new(
        py,
        PyReadBatch {
            _owner: slf.clone().unbind(),
            inner: prepared.batch,
        },
    )?;
    Py::new(
        py,
        PyLmcacheRecordBatch {
            owner: batch,
            decoded: prepared.decoded,
        },
    )
}

fn prepare_encoded_lmcache_keys(encoded: Vec<Vec<u8>>) -> PreparedLmcacheKeys {
    let mut key_hashes = Vec::with_capacity(encoded.len());
    let mut common_session = None::<Vec<u8>>;

    for key in &encoded {
        key_hashes.push(shardmap_crate::storage::hash_key(key));
        let session = extract_lmcache_session_prefix(key);
        common_session = match (common_session, session) {
            (None, Some(session)) => Some(session),
            (Some(existing), Some(session)) if existing == session => Some(existing),
            (Some(_), Some(_)) | (_, None) => None,
        };
    }

    PreparedLmcacheKeys {
        encoded,
        key_hashes,
        session_prefix: common_session,
    }
}

fn prepare_lmcache_batch(
    store: &StoreCore,
    keys: &PreparedLmcacheKeys,
) -> PyResult<PreparedLmcacheBatch> {
    let batch = match &keys.session_prefix {
        Some(session_prefix) => PyReadBatchInner::Single(store.batch_get_session_view_prehashed(
            session_prefix,
            &keys.encoded,
            &keys.key_hashes,
        )),
        None => store.batch_get_view(&keys.encoded),
    };
    let mut decoded = Vec::with_capacity(batch.item_count());
    let mut metadata_cache = FastHashMap::<Vec<u8>, Arc<DecodedLmcacheMetadata>>::default();
    for index in 0..batch.item_count() {
        let item = batch.slice_meta(index);
        match item {
            Some(slice) => decoded.push(Some(decode_lmcache_record_v2_cached(
                slice.as_slice(),
                &mut metadata_cache,
            )?)),
            None => decoded.push(None),
        }
    }
    Ok(PreparedLmcacheBatch { batch, decoded })
}

fn session_route_prefix(key: &[u8]) -> &[u8] {
    if !key.starts_with(b"s:") {
        return key;
    }
    let marker = b":c:";
    if let Some(index) = key
        .windows(marker.len())
        .rposition(|window| window == marker)
    {
        return &key[..index];
    }
    key
}

fn extract_lmcache_session_prefix(encoded_key: &[u8]) -> Option<Vec<u8>> {
    let key_str = std::str::from_utf8(encoded_key).ok()?;
    let session = key_str
        .split('@')
        .find_map(|part| part.strip_prefix("session%"))?;
    Some(format!("lmcache-session:{session}").into_bytes())
}

fn decode_lmcache_metadata_v2(raw: &[u8]) -> PyResult<DecodedLmcacheMetadata> {
    if raw.len() < LMCACHE_FIXED_META_SIZE {
        return Err(PyValueError::new_err(
            "LMCache metadata header is truncated",
        ));
    }

    let mut cursor = 0usize;
    let address = read_u64_be(raw, &mut cursor)?;
    let phy_size = read_u32_be(raw, &mut cursor)?;
    let ref_count = read_u32_be(raw, &mut cursor)?;
    let pin_count = read_u32_be(raw, &mut cursor)?;
    let fmt_value = read_u8(raw, &mut cursor)?;
    let has_dtype = read_u8(raw, &mut cursor)? != 0;
    let has_shapes = read_u8(raw, &mut cursor)? != 0;
    let has_dtypes = read_u8(raw, &mut cursor)? != 0;
    let has_cached_positions = read_u8(raw, &mut cursor)? != 0;
    let shape_rank = read_u8(raw, &mut cursor)? as usize;

    let mut shape = Vec::with_capacity(shape_rank);
    for _ in 0..shape_rank {
        shape.push(read_i64_be(raw, &mut cursor)?);
    }

    let dtype = if has_dtype {
        Some(read_blob(raw, &mut cursor)?.to_vec())
    } else {
        None
    };

    let shapes = if has_shapes {
        let count = read_u16_be(raw, &mut cursor)? as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let rank = read_u16_be(raw, &mut cursor)? as usize;
            let mut dims = Vec::with_capacity(rank);
            for _ in 0..rank {
                dims.push(read_i64_be(raw, &mut cursor)?);
            }
            out.push(dims);
        }
        Some(out)
    } else {
        None
    };

    let dtypes = if has_dtypes {
        let count = read_u16_be(raw, &mut cursor)? as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(read_blob(raw, &mut cursor)?.to_vec());
        }
        Some(out)
    } else {
        None
    };

    let cached_positions = if has_cached_positions {
        let count = read_u32_be(raw, &mut cursor)? as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            out.push(read_i64_be(raw, &mut cursor)?);
        }
        Some(out)
    } else {
        None
    };

    Ok(DecodedLmcacheMetadata {
        shape,
        dtype,
        address,
        phy_size,
        ref_count,
        pin_count,
        fmt_value,
        cached_positions,
        shapes,
        dtypes,
    })
}

fn build_lmcache_metadata(
    py: Python<'_>,
    metadata: &DecodedLmcacheMetadata,
    symbols: &LmcachePythonSymbols,
) -> PyResult<PyObject> {
    let template = cached_lmcache_metadata_template(py, metadata, symbols)?;
    let torch_tensor_fn = symbols.torch_tensor_fn.bind(py);
    let torch_int64 = symbols.torch_int64.bind(py);
    let cached_positions = match &metadata.cached_positions {
        Some(positions) => {
            let kwargs = PyDict::new(py);
            kwargs.set_item("dtype", torch_int64)?;
            torch_tensor_fn
                .call((positions.clone(),), Some(&kwargs))?
                .into_pyobject(py)?
                .into()
        }
        None => py.None(),
    };

    Ok(symbols
        .metadata_cls
        .bind(py)
        .call1((
            template.shape.bind(py),
            template.dtype.bind(py),
            metadata.address,
            metadata.phy_size,
            metadata.ref_count,
            metadata.pin_count,
            template.fmt.bind(py),
            cached_positions,
            template.shapes.bind(py),
            template.dtypes.bind(py),
        ))?
        .unbind())
}

fn build_torch_size(
    py: Python<'_>,
    torch_size_cls: &Bound<'_, PyAny>,
    dims: &[i64],
) -> PyResult<PyObject> {
    Ok(torch_size_cls
        .call1((dims.to_vec(),))?
        .into_pyobject(py)?
        .into())
}

fn build_torch_dtype(
    py: Python<'_>,
    torch_module: &Bound<'_, PyModule>,
    raw: Option<&[u8]>,
) -> PyResult<PyObject> {
    let Some(raw) = raw else {
        return Ok(py.None());
    };
    let name = std::str::from_utf8(raw)
        .map_err(|_| PyValueError::new_err("LMCache dtype is not valid UTF-8"))?
        .trim_start_matches("torch.");
    Ok(torch_module.getattr(name)?.into_pyobject(py)?.into())
}

fn read_u8(raw: &[u8], cursor: &mut usize) -> PyResult<u8> {
    if *cursor >= raw.len() {
        return Err(PyValueError::new_err("LMCache metadata read overflow"));
    }
    let value = raw[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u16_be(raw: &[u8], cursor: &mut usize) -> PyResult<u16> {
    let end = cursor.saturating_add(2);
    let bytes = raw
        .get(*cursor..end)
        .ok_or_else(|| PyValueError::new_err("LMCache metadata read overflow"))?;
    *cursor = end;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32_be(raw: &[u8], cursor: &mut usize) -> PyResult<u32> {
    let end = cursor.saturating_add(4);
    let bytes = raw
        .get(*cursor..end)
        .ok_or_else(|| PyValueError::new_err("LMCache metadata read overflow"))?;
    *cursor = end;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u64_be(raw: &[u8], cursor: &mut usize) -> PyResult<u64> {
    let end = cursor.saturating_add(8);
    let bytes = raw
        .get(*cursor..end)
        .ok_or_else(|| PyValueError::new_err("LMCache metadata read overflow"))?;
    *cursor = end;
    Ok(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn read_i64_be(raw: &[u8], cursor: &mut usize) -> PyResult<i64> {
    let end = cursor.saturating_add(8);
    let bytes = raw
        .get(*cursor..end)
        .ok_or_else(|| PyValueError::new_err("LMCache metadata read overflow"))?;
    *cursor = end;
    Ok(i64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn read_blob<'a>(raw: &'a [u8], cursor: &mut usize) -> PyResult<&'a [u8]> {
    let len = read_u16_be(raw, cursor)? as usize;
    let end = cursor.saturating_add(len);
    let bytes = raw
        .get(*cursor..end)
        .ok_or_else(|| PyValueError::new_err("LMCache metadata read overflow"))?;
    *cursor = end;
    Ok(bytes)
}

#[cfg(test)]
mod tests;
