use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use dashmap::DashMap;
use fcnp_client_rs::{FcnpClient, FcnpClientError};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

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
        py.allow_threads(|| {
            self.with_client(|client| {
                let mut out = Vec::new();
                let mut values = Vec::with_capacity(keys.len());
                for key in &keys {
                    out.clear();
                    if client.get_into(key, &mut out)? {
                        values.push(Some(out.clone()));
                    } else {
                        values.push(None);
                    }
                }
                Ok(values)
            })
        })
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
        py.allow_threads(|| {
            self.with_client(|client| {
                for (key, value) in &items {
                    client.set(key, value)?;
                }
                Ok(())
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

fn fcnp_error_to_py(error: FcnpClientError) -> PyErr {
    PyValueError::new_err(error.to_string())
}
