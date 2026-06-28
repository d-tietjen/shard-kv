use std::error::Error;

use clap::ValueEnum;

pub type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Get,
    Set,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ReadMode {
    /// Borrow/reference values on GET without copying into the benchmark buffer.
    Ref,
    /// Materialize GET values into the benchmark scratch buffer.
    Copy,
}

impl ReadMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Ref => "ref",
            Self::Copy => "copy",
        }
    }
}

pub trait Backend: Send + Sync {
    fn id(&self) -> &str;

    fn class(&self) -> BackendClass;

    fn supports_pipelining(&self) -> bool {
        false
    }

    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError>;

    fn warmup_ttl(&self, _keys: &[Vec<u8>], _value: &[u8], _ttl_ms: u64) -> Result<(), BoxError> {
        Err(format!("backend `{}` does not support TTL workloads", self.id()).into())
    }

    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError>;

    fn new_worker_for(
        &self,
        _worker_index: usize,
        _worker_count: usize,
    ) -> Result<Box<dyn Worker>, BoxError> {
        self.new_worker()
    }

    fn worker_key_indices(
        &self,
        _keys: &[Vec<u8>],
        _worker_index: usize,
        _worker_count: usize,
    ) -> Result<Option<Vec<usize>>, BoxError> {
        Ok(None)
    }
}

pub trait Worker: Send {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError>;
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError>;

    fn begin_pipeline_get(&mut self, _key: &[u8]) -> Result<(), BoxError> {
        Err("backend worker does not support pipelined GET".into())
    }

    fn begin_pipeline_set(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), BoxError> {
        Err("backend worker does not support pipelined SET".into())
    }

    fn begin_pipeline_get_index(&mut self, _local_index: usize) -> Result<(), BoxError> {
        Err("backend worker does not support indexed pipelined GET".into())
    }

    fn begin_pipeline_set_index(
        &mut self,
        _local_index: usize,
        _value: &[u8],
    ) -> Result<(), BoxError> {
        Err("backend worker does not support indexed pipelined SET".into())
    }

    fn flush_pipeline(&mut self) -> Result<(), BoxError> {
        Err("backend worker does not support pipelined flush".into())
    }

    fn finish_pipeline_get(&mut self, _scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        Err("backend worker does not support pipelined GET response".into())
    }

    fn finish_pipeline_set(&mut self) -> Result<(), BoxError> {
        Err("backend worker does not support pipelined SET response".into())
    }

    fn set_ttl(&mut self, _key: &[u8], _value: &[u8], _ttl_ms: u64) -> Result<(), BoxError> {
        Err("backend worker does not support TTL writes".into())
    }

    fn supports_indexed_keys(&self) -> bool {
        false
    }

    fn get_index(&mut self, _local_index: usize, _scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        Err("backend worker does not support indexed keys".into())
    }

    fn set_index(&mut self, _local_index: usize, _value: &[u8]) -> Result<(), BoxError> {
        Err("backend worker does not support indexed keys".into())
    }

    fn set_index_ttl(
        &mut self,
        _local_index: usize,
        _value: &[u8],
        _ttl_ms: u64,
    ) -> Result<(), BoxError> {
        Err("backend worker does not support indexed TTL writes".into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendClass {
    Embedded,
    Networked,
}
