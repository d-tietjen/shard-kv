use std::net::ToSocketAddrs;

use crate::commands::get::{self, Get};
use crate::commands::resp::RespCommand;
use crate::commands::set::{self, Set};
use crate::connection::FcnpConnection;
use crate::error::{FcnpClientError, Result};
use crate::routing::{FcnpDirectRouter, FcnpRouteMode};

/// Blocking FCNP client for the ordinary server listener.
#[derive(Debug)]
pub struct FcnpClient {
    conn: FcnpConnection,
}

impl FcnpClient {
    /// Connects to a fast-cache server listener that accepts generic FCNP.
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        Ok(Self {
            conn: FcnpConnection::connect(addr)?,
        })
    }

    /// Reads `key` into `out`, returning `true` on hit.
    pub fn get_into(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool> {
        self.conn.execute(Get::new(key, out))
    }

    /// Sets `key` to `value`.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.conn.execute(Set::new(key, value))
    }

    /// Executes a Redis-compatible command through the generic FCNP wrapper.
    ///
    /// The server returns RESP bytes as an FCNP value. `out` receives those raw
    /// bytes so callers can decode exactly the shape they requested.
    pub fn resp_command_into(&mut self, parts: &[&[u8]], out: &mut Vec<u8>) -> Result<bool> {
        self.conn.execute(RespCommand::new(parts, out))
    }

    /// Runs the global FCNP scan wrapper and returns the RESP scan reply bytes.
    pub fn scan_resp_into(&mut self, cursor: u64, count: usize, out: &mut Vec<u8>) -> Result<bool> {
        let cursor = cursor.to_string();
        let count = count.to_string();
        self.resp_command_into(
            &[b"FCNP.SCAN", cursor.as_bytes(), b"COUNT", count.as_bytes()],
            out,
        )
    }

    /// Runs a shard-local FCNP scan. Call this concurrently per shard to avoid
    /// a server-side fanout scan.
    pub fn scan_shard_resp_into(
        &mut self,
        shard_id: usize,
        cursor: u64,
        count: usize,
        out: &mut Vec<u8>,
    ) -> Result<bool> {
        let shard_id = shard_id.to_string();
        let cursor = cursor.to_string();
        let count = count.to_string();
        self.resp_command_into(
            &[
                b"FCNP.SCANSHARD",
                shard_id.as_bytes(),
                cursor.as_bytes(),
                b"COUNT",
                count.as_bytes(),
            ],
            out,
        )
    }

    /// Writes a GET request without flushing or reading its response.
    pub fn begin_pipeline_get(&mut self, key: &[u8]) -> Result<()> {
        get::write_request(&mut self.conn, None, key)
    }

    /// Writes a SET request without flushing or reading its response.
    pub fn begin_pipeline_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        set::write_request(&mut self.conn, None, key, value)
    }

    /// Flushes all queued pipelined requests.
    pub fn flush_pipeline(&mut self) -> Result<()> {
        self.conn.flush()
    }

    /// Reads the next pipelined GET response.
    pub fn finish_pipeline_get_into(&mut self, out: &mut Vec<u8>) -> Result<bool> {
        self.conn
            .read_value(<Get as crate::commands::FcnpCommand>::NAME, out)
    }

    /// Reads the next pipelined SET response.
    pub fn finish_pipeline_set(&mut self) -> Result<()> {
        self.conn
            .expect_ok(<Set as crate::commands::FcnpCommand>::NAME)
    }
}

impl FcnpDirectRouter {
    /// Connects directly to one shard-owned port.
    pub fn connect_shard(&self, shard_id: usize) -> Result<FcnpDirectShardClient> {
        Ok(FcnpDirectShardClient {
            router: *self,
            shard_id,
            conn: FcnpConnection::connect(self.shard_addr(shard_id)?)?,
        })
    }
}

/// Blocking FCNP client that automatically routes each key to its shard port.
#[derive(Debug)]
pub struct FcnpDirectClient {
    router: FcnpDirectRouter,
    conns: Vec<FcnpConnection>,
}

impl FcnpDirectClient {
    /// Connects to every shard-owned port starting at `addr`.
    ///
    /// `addr` must be the first direct shard port, not the fanout port.
    pub fn connect(addr: impl ToSocketAddrs, shard_count: usize) -> Result<Self> {
        let router = FcnpDirectRouter::new(addr, shard_count)?;
        Self::connect_with_router(router)
    }

    /// Connects to every shard-owned port using an explicit route mode.
    pub fn connect_with_route_mode(
        addr: impl ToSocketAddrs,
        shard_count: usize,
        route_mode: FcnpRouteMode,
    ) -> Result<Self> {
        let router = FcnpDirectRouter::new(addr, shard_count)?.with_route_mode(route_mode);
        Self::connect_with_router(router)
    }

    fn connect_with_router(router: FcnpDirectRouter) -> Result<Self> {
        let mut conns = Vec::with_capacity(router.shard_count());
        for shard_id in 0..router.shard_count() {
            conns.push(FcnpConnection::connect(router.shard_addr(shard_id)?)?);
        }
        Ok(Self { router, conns })
    }

    /// Reads `key` from its owning shard into `out`, returning `true` on hit.
    pub fn get_into(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool> {
        let route = self.router.route_key(key);
        self.conns[route.shard_id].execute(Get::routed(route, key, out))
    }

    /// Sets `key` on its owning shard.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let route = self.router.route_key(key);
        self.conns[route.shard_id].execute(Set::routed(route, key, value))
    }

    /// Runs a shard-local FCNP scan on one direct shard connection. Callers can
    /// invoke this for different shards from different threads for parallel
    /// scans.
    pub fn scan_shard_resp_into(
        &mut self,
        shard_id: usize,
        cursor: u64,
        count: usize,
        out: &mut Vec<u8>,
    ) -> Result<bool> {
        if shard_id >= self.conns.len() {
            return Err(FcnpClientError::Config(format!(
                "shard {shard_id} is outside configured shard count {}",
                self.conns.len()
            )));
        }
        let shard_id_text = shard_id.to_string();
        let cursor = cursor.to_string();
        let count = count.to_string();
        self.conns[shard_id].execute(RespCommand::new(
            &[
                b"FCNP.SCANSHARD",
                shard_id_text.as_bytes(),
                cursor.as_bytes(),
                b"COUNT",
                count.as_bytes(),
            ],
            out,
        ))
    }
}

/// Blocking FCNP client pinned to one shard-owned port.
///
/// This is useful for thread-per-shard clients that pre-partition work.
#[derive(Debug)]
pub struct FcnpDirectShardClient {
    router: FcnpDirectRouter,
    shard_id: usize,
    conn: FcnpConnection,
}

impl FcnpDirectShardClient {
    /// Returns the shard this client is connected to.
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    /// Reads `key` into `out`, returning `true` on hit.
    pub fn get_into(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool> {
        let route = self.checked_route(key)?;
        self.conn.execute(Get::routed(route, key, out))
    }

    /// Sets `key` to `value`.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let route = self.checked_route(key)?;
        self.conn.execute(Set::routed(route, key, value))
    }

    /// Runs a shard-local FCNP scan on this shard-owned connection.
    pub fn scan_resp_into(&mut self, cursor: u64, count: usize, out: &mut Vec<u8>) -> Result<bool> {
        let shard_id = self.shard_id.to_string();
        let cursor = cursor.to_string();
        let count = count.to_string();
        self.conn.execute(RespCommand::new(
            &[
                b"FCNP.SCANSHARD",
                shard_id.as_bytes(),
                cursor.as_bytes(),
                b"COUNT",
                count.as_bytes(),
            ],
            out,
        ))
    }

    /// Writes a routed GET request without flushing or reading its response.
    pub fn begin_pipeline_get(&mut self, key: &[u8]) -> Result<()> {
        let route = self.checked_route(key)?;
        get::write_request(&mut self.conn, Some(route), key)
    }

    /// Writes a routed SET request without flushing or reading its response.
    pub fn begin_pipeline_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let route = self.checked_route(key)?;
        set::write_request(&mut self.conn, Some(route), key, value)
    }

    /// Flushes all queued pipelined requests.
    pub fn flush_pipeline(&mut self) -> Result<()> {
        self.conn.flush()
    }

    /// Reads the next pipelined GET response.
    pub fn finish_pipeline_get_into(&mut self, out: &mut Vec<u8>) -> Result<bool> {
        self.conn
            .read_value(<Get as crate::commands::FcnpCommand>::NAME, out)
    }

    /// Reads the next pipelined SET response.
    pub fn finish_pipeline_set(&mut self) -> Result<()> {
        self.conn
            .expect_ok(<Set as crate::commands::FcnpCommand>::NAME)
    }

    fn checked_route(&self, key: &[u8]) -> Result<crate::routing::FcnpRoute> {
        let route = self.router.route_key(key);
        if route.shard_id != self.shard_id {
            return Err(FcnpClientError::Config(format!(
                "key routes to shard {}, but client is connected to shard {}",
                route.shard_id, self.shard_id
            )));
        }
        Ok(route)
    }
}
