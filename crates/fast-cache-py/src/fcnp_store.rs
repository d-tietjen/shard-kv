use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use dashmap::DashMap;
use fcnp_client_rs::{FcnpClient, FcnpClientError};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;

#[pyclass(name = "FcnpStore")]
pub(crate) struct PyFcnpStore {
    addr: String,
    clients: DashMap<ThreadId, Arc<Mutex<FcnpClient>>>,
}

#[pymethods]
impl PyFcnpStore {
    #[new]
    fn new(addr: String) -> Self {
        Self {
            addr,
            clients: DashMap::new(),
        }
    }

    fn close(&self) {
        self.clients.clear();
    }

    fn get(&self, py: Python<'_>, key: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
        let mut out = Vec::new();
        let found =
            py.allow_threads(|| self.with_client(|client| client.get_into(&key, &mut out)))?;
        Ok(found.then_some(out))
    }

    fn batch_get(&self, py: Python<'_>, keys: Vec<Vec<u8>>) -> PyResult<Vec<Option<Vec<u8>>>> {
        py.allow_threads(|| self.with_client(|client| pipeline_get_keys(client, &keys)))
    }

    #[pyo3(signature = (key, value, ttl=None))]
    fn set(&self, py: Python<'_>, key: Vec<u8>, value: Vec<u8>, ttl: Option<u64>) -> PyResult<()> {
        reject_ttl(ttl)?;
        py.allow_threads(|| self.with_client(|client| client.set(&key, &value)))
    }

    #[pyo3(signature = (items, ttl=None))]
    fn batch_set(
        &self,
        py: Python<'_>,
        items: Vec<(Vec<u8>, Vec<u8>)>,
        ttl: Option<u64>,
    ) -> PyResult<()> {
        reject_ttl(ttl)?;
        py.allow_threads(|| self.with_client(|client| pipeline_set_items(client, &items)))
    }

    fn prepare_lmcache_put_batch_encoded_keys(
        &self,
        py: Python<'_>,
        keys: Vec<PyBackedBytes>,
        metadata_blobs: Vec<PyBackedBytes>,
    ) -> PyResult<crate::PyPreparedLmcachePutBatch> {
        py.allow_threads(|| {
            Ok(crate::PyPreparedLmcachePutBatch {
                inner: Arc::new(crate::prepare_lmcache_put_batch_from_pybacked_parts(
                    &keys,
                    &metadata_blobs,
                )?),
            })
        })
    }

    fn batch_put_lmcache_payloads_and_metadata_encoded_keys(
        &self,
        py: Python<'_>,
        keys: Vec<PyBackedBytes>,
        payloads: Vec<PyObject>,
        metadata_blobs: Vec<PyBackedBytes>,
    ) -> PyResult<()> {
        let items = lmcache_items_from_parts(py, &keys, &payloads, &metadata_blobs)?;
        py.allow_threads(|| self.with_client(|client| pipeline_set_items(client, &items)))
    }

    fn batch_put_lmcache_payload_bytes_and_metadata_encoded_keys(
        &self,
        py: Python<'_>,
        keys: Vec<PyBackedBytes>,
        payloads: Vec<PyBackedBytes>,
        metadata_blobs: Vec<PyBackedBytes>,
    ) -> PyResult<()> {
        validate_lmcache_put_lengths(
            keys.len(),
            payloads.len(),
            metadata_blobs.len(),
            "payload byte blobs",
        )?;
        py.allow_threads(move || {
            self.with_client(|client| {
                pipeline_lmcache_byte_parts(client, &keys, &payloads, &metadata_blobs)
            })
        })
    }

    fn batch_put_lmcache_payloads_prepared(
        &self,
        py: Python<'_>,
        prepared: &Bound<'_, crate::PyPreparedLmcachePutBatch>,
        payloads: Vec<PyObject>,
    ) -> PyResult<()> {
        let prepared = Arc::clone(&prepared.borrow().inner);
        let items = lmcache_items_from_prepared_parts(py, &prepared, &payloads)?;
        py.allow_threads(|| self.with_client(|client| pipeline_set_items(client, &items)))
    }

    fn batch_put_lmcache_payload_bytes_prepared(
        &self,
        py: Python<'_>,
        prepared: &Bound<'_, crate::PyPreparedLmcachePutBatch>,
        payloads: Vec<PyBackedBytes>,
    ) -> PyResult<()> {
        let prepared = Arc::clone(&prepared.borrow().inner);
        validate_prepared_payload_len(&prepared, payloads.len(), "payload byte blobs")?;
        py.allow_threads(move || {
            self.with_client(|client| {
                pipeline_lmcache_prepared_byte_parts(client, &prepared, &payloads)
            })
        })
    }

    fn batch_put_lmcache_memory_objs_prepared_bytes(
        &self,
        py: Python<'_>,
        prepared: &Bound<'_, crate::PyPreparedLmcachePutBatch>,
        objs: Vec<PyObject>,
    ) -> PyResult<()> {
        let prepared = Arc::clone(&prepared.borrow().inner);
        let payloads = crate::extract_lmcache_memory_obj_bytes_payloads(py, &objs)?;
        validate_prepared_payload_len(&prepared, payloads.len(), "payload byte blobs")?;
        py.allow_threads(move || {
            self.with_client(|client| {
                pipeline_lmcache_prepared_byte_parts(client, &prepared, &payloads)
            })
        })
    }

    fn exists(&self, py: Python<'_>, key: Vec<u8>) -> PyResult<bool> {
        let mut out = Vec::new();
        py.allow_threads(|| self.with_client(|client| client.get_into(&key, &mut out)))
    }

    fn delete(&self, _key: Vec<u8>) -> PyResult<bool> {
        Err(PyValueError::new_err(
            "FCNP TCP LMCache adapter does not support DELETE yet",
        ))
    }
}

impl PyFcnpStore {
    fn with_client<T>(
        &self,
        f: impl FnOnce(&mut FcnpClient) -> fcnp_client_rs::Result<T>,
    ) -> PyResult<T> {
        let client = self.client_for_thread()?;
        let mut guard = client
            .lock()
            .map_err(|_| PyRuntimeError::new_err("FCNP client mutex poisoned"))?;
        f(&mut guard).map_err(fcnp_error_to_py)
    }

    fn client_for_thread(&self) -> PyResult<Arc<Mutex<FcnpClient>>> {
        let thread_id = std::thread::current().id();
        if let Some(client) = self.clients.get(&thread_id) {
            return Ok(Arc::clone(client.value()));
        }

        let client = Arc::new(Mutex::new(
            FcnpClient::connect(self.addr.as_str()).map_err(fcnp_error_to_py)?,
        ));
        self.clients.insert(thread_id, Arc::clone(&client));
        Ok(client)
    }
}

fn reject_ttl(ttl: Option<u64>) -> PyResult<()> {
    if ttl.is_some() {
        return Err(PyValueError::new_err(
            "FCNP TCP LMCache adapter does not support TTL",
        ));
    }
    Ok(())
}

fn pipeline_set_items(
    client: &mut FcnpClient,
    items: &[(Vec<u8>, Vec<u8>)],
) -> fcnp_client_rs::Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    if items.len() == 1 {
        let (key, value) = &items[0];
        return client.set(key, value);
    }

    for (key, value) in items {
        client.begin_pipeline_set(key, value)?;
    }
    client.flush_pipeline()?;
    for _ in items {
        client.finish_pipeline_set()?;
    }
    Ok(())
}

fn pipeline_get_keys(
    client: &mut FcnpClient,
    keys: &[Vec<u8>],
) -> fcnp_client_rs::Result<Vec<Option<Vec<u8>>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    if keys.len() == 1 {
        let mut out = Vec::new();
        return Ok(vec![client.get_into(&keys[0], &mut out)?.then_some(out)]);
    }

    for key in keys {
        client.begin_pipeline_get(key)?;
    }
    client.flush_pipeline()?;

    let mut values = Vec::with_capacity(keys.len());
    let mut out = Vec::new();
    for _ in keys {
        out.clear();
        if client.finish_pipeline_get_into(&mut out)? {
            values.push(Some(out.clone()));
        } else {
            values.push(None);
        }
    }
    Ok(values)
}

fn pipeline_lmcache_byte_parts(
    client: &mut FcnpClient,
    keys: &[PyBackedBytes],
    payloads: &[PyBackedBytes],
    metadata_blobs: &[PyBackedBytes],
) -> fcnp_client_rs::Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    if keys.len() == 1 {
        let value = crate::encode_lmcache_record_from_bytes_parts(
            payloads[0].as_ref(),
            metadata_blobs[0].as_ref(),
        );
        return client.set(keys[0].as_ref(), &value);
    }

    for ((key, payload), metadata_blob) in keys.iter().zip(payloads).zip(metadata_blobs) {
        let value =
            crate::encode_lmcache_record_from_bytes_parts(payload.as_ref(), metadata_blob.as_ref());
        client.begin_pipeline_set(key.as_ref(), &value)?;
    }
    client.flush_pipeline()?;
    for _ in keys {
        client.finish_pipeline_set()?;
    }
    Ok(())
}

fn pipeline_lmcache_prepared_byte_parts(
    client: &mut FcnpClient,
    prepared: &crate::PreparedLmcachePutBatch,
    payloads: &[PyBackedBytes],
) -> fcnp_client_rs::Result<()> {
    if prepared.keys.is_empty() {
        return Ok(());
    }
    if prepared.keys.len() == 1 {
        let value = crate::encode_lmcache_record_from_bytes_parts(
            payloads[0].as_ref(),
            prepared.metadata_blobs[0].as_slice(),
        );
        return client.set(prepared.keys[0].as_slice(), &value);
    }

    for (index, payload) in payloads.iter().enumerate() {
        let value = crate::encode_lmcache_record_from_bytes_parts(
            payload.as_ref(),
            prepared.metadata_blobs[index].as_slice(),
        );
        client.begin_pipeline_set(prepared.keys[index].as_slice(), &value)?;
    }
    client.flush_pipeline()?;
    for _ in &prepared.keys {
        client.finish_pipeline_set()?;
    }
    Ok(())
}

fn lmcache_items_from_parts(
    py: Python<'_>,
    keys: &[PyBackedBytes],
    payloads: &[PyObject],
    metadata_blobs: &[PyBackedBytes],
) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    validate_lmcache_put_lengths(keys.len(), payloads.len(), metadata_blobs.len(), "payloads")?;

    let mut items = Vec::with_capacity(keys.len());
    for ((key, payload), metadata_blob) in keys.iter().zip(payloads).zip(metadata_blobs) {
        items.push((
            key.as_ref().to_vec(),
            crate::encode_lmcache_record_from_parts(py, payload.bind(py), metadata_blob.as_ref())?,
        ));
    }
    Ok(items)
}

fn lmcache_items_from_prepared_parts(
    py: Python<'_>,
    prepared: &crate::PreparedLmcachePutBatch,
    payloads: &[PyObject],
) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
    validate_prepared_payload_len(prepared, payloads.len(), "payloads")?;

    let mut items = Vec::with_capacity(prepared.keys.len());
    for (index, payload) in payloads.iter().enumerate() {
        items.push((
            prepared.keys[index].clone(),
            crate::encode_lmcache_record_from_parts(
                py,
                payload.bind(py),
                prepared.metadata_blobs[index].as_slice(),
            )?,
        ));
    }
    Ok(items)
}

fn validate_lmcache_put_lengths(
    key_count: usize,
    payload_count: usize,
    metadata_count: usize,
    payload_label: &str,
) -> PyResult<()> {
    if key_count != payload_count || key_count != metadata_count {
        return Err(PyValueError::new_err(format!(
            "LMCache put batch length mismatch: {key_count} keys vs {payload_count} {payload_label} vs {metadata_count} metadata blobs"
        )));
    }
    Ok(())
}

fn validate_prepared_payload_len(
    prepared: &crate::PreparedLmcachePutBatch,
    payload_count: usize,
    payload_label: &str,
) -> PyResult<()> {
    if prepared.keys.len() != payload_count {
        return Err(PyValueError::new_err(format!(
            "LMCache prepared put payload mismatch: {} prepared items vs {} {}",
            prepared.keys.len(),
            payload_count,
            payload_label
        )));
    }
    Ok(())
}

fn fcnp_error_to_py(error: FcnpClientError) -> PyErr {
    PyValueError::new_err(error.to_string())
}
