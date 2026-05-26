use std::net::ToSocketAddrs;

use crate::commands::del::{self, Del};
use crate::commands::exists::{self, Exists};
use crate::commands::expire::{self, Expire};
use crate::commands::get::{self, Get};
use crate::commands::getex::{self, GetEx};
#[cfg(feature = "redis")]
use crate::commands::redis::{
    self, RedisCommand as OptimizedRedisCommand, RedisCommandKind, RedisCommandRouteKeys,
    RedisResponse,
};
use crate::commands::resp::RespCommand;
use crate::commands::set::{self, Set};
use crate::commands::setex::{self, SetEx};
use crate::commands::ttl::{self, Ttl};
use crate::connection::FcnpConnection;
use crate::error::{FcnpClientError, Result};
#[cfg(feature = "redis")]
use crate::routing::FcnpRoute;
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

    /// Sets `key` to `value` with a millisecond TTL.
    pub fn set_ex(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<()> {
        self.conn.execute(SetEx::new(key, value, ttl_ms))
    }

    /// Reads `key` into `out` and sets a millisecond TTL, returning `true` on hit.
    pub fn get_ex_into(&mut self, key: &[u8], ttl_ms: u64, out: &mut Vec<u8>) -> Result<bool> {
        self.conn.execute(GetEx::new(key, ttl_ms, out))
    }

    /// Deletes `key`, returning `true` when an entry was removed.
    pub fn del(&mut self, key: &[u8]) -> Result<bool> {
        self.conn.execute(Del::new(key))
    }

    /// Returns whether `key` exists.
    pub fn exists(&mut self, key: &[u8]) -> Result<bool> {
        self.conn.execute(Exists::new(key))
    }

    /// Returns Redis-compatible TTL seconds for `key`.
    pub fn ttl(&mut self, key: &[u8]) -> Result<i64> {
        self.conn.execute(Ttl::new(key))
    }

    /// Sets a millisecond TTL on `key`, returning `true` when the TTL changed.
    pub fn expire(&mut self, key: &[u8], ttl_ms: u64) -> Result<bool> {
        self.conn.execute(Expire::new(key, ttl_ms))
    }

    /// Executes a Redis-compatible command through the compact opcode FCNP wrapper.
    #[cfg(feature = "redis")]
    pub fn redis_command(
        &mut self,
        command: RedisCommandKind,
        args: &[&[u8]],
    ) -> Result<RedisResponse> {
        self.conn.execute(OptimizedRedisCommand::new(command, args))
    }

    /// Executes a Redis-compatible command by name through the compact opcode FCNP wrapper.
    #[cfg(feature = "redis")]
    pub fn redis_command_by_name(
        &mut self,
        command: &[u8],
        args: &[&[u8]],
    ) -> Result<RedisResponse> {
        self.redis_command(redis_command_kind_from_name(command)?, args)
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

    /// Writes a SETEX request without flushing or reading its response.
    pub fn begin_pipeline_set_ex(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<()> {
        setex::write_request(&mut self.conn, None, key, value, ttl_ms)
    }

    /// Writes a GETEX request without flushing or reading its response.
    pub fn begin_pipeline_get_ex(&mut self, key: &[u8], ttl_ms: u64) -> Result<()> {
        getex::write_request(&mut self.conn, None, key, ttl_ms)
    }

    /// Writes a DEL request without flushing or reading its response.
    pub fn begin_pipeline_del(&mut self, key: &[u8]) -> Result<()> {
        del::write_request(&mut self.conn, None, key)
    }

    /// Writes an EXISTS request without flushing or reading its response.
    pub fn begin_pipeline_exists(&mut self, key: &[u8]) -> Result<()> {
        exists::write_request(&mut self.conn, None, key)
    }

    /// Writes a TTL request without flushing or reading its response.
    pub fn begin_pipeline_ttl(&mut self, key: &[u8]) -> Result<()> {
        ttl::write_request(&mut self.conn, None, key)
    }

    /// Writes an EXPIRE request without flushing or reading its response.
    pub fn begin_pipeline_expire(&mut self, key: &[u8], ttl_ms: u64) -> Result<()> {
        expire::write_request(&mut self.conn, None, key, ttl_ms)
    }

    /// Writes a compact Redis command request without flushing or reading its response.
    #[cfg(feature = "redis")]
    pub fn begin_pipeline_redis_command(
        &mut self,
        command: RedisCommandKind,
        args: &[&[u8]],
    ) -> Result<()> {
        redis::write_request(&mut self.conn, command, None, args)
    }

    /// Writes a compact Redis command request by name without flushing or reading its response.
    #[cfg(feature = "redis")]
    pub fn begin_pipeline_redis_command_by_name(
        &mut self,
        command: &[u8],
        args: &[&[u8]],
    ) -> Result<()> {
        self.begin_pipeline_redis_command(redis_command_kind_from_name(command)?, args)
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

    /// Reads the next pipelined SETEX response.
    pub fn finish_pipeline_set_ex(&mut self) -> Result<()> {
        self.conn
            .expect_ok(<SetEx as crate::commands::FcnpCommand>::NAME)
    }

    /// Reads the next pipelined GETEX response.
    pub fn finish_pipeline_get_ex_into(&mut self, out: &mut Vec<u8>) -> Result<bool> {
        self.conn
            .read_value(<GetEx as crate::commands::FcnpCommand>::NAME, out)
    }

    /// Reads the next pipelined DEL response.
    pub fn finish_pipeline_del(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Del as crate::commands::FcnpCommand>::NAME)
            .map(|deleted| deleted != 0)
    }

    /// Reads the next pipelined EXISTS response.
    pub fn finish_pipeline_exists(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Exists as crate::commands::FcnpCommand>::NAME)
            .map(|exists| exists != 0)
    }

    /// Reads the next pipelined TTL response.
    pub fn finish_pipeline_ttl(&mut self) -> Result<i64> {
        self.conn
            .read_integer(<Ttl as crate::commands::FcnpCommand>::NAME)
    }

    /// Reads the next pipelined EXPIRE response.
    pub fn finish_pipeline_expire(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Expire as crate::commands::FcnpCommand>::NAME)
            .map(|changed| changed != 0)
    }

    /// Reads the next pipelined compact Redis command response.
    #[cfg(feature = "redis")]
    pub fn finish_pipeline_redis_command(&mut self) -> Result<RedisResponse> {
        self.conn.read_redis_response("REDIS")
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

    /// Sets `key` on its owning shard with a millisecond TTL.
    pub fn set_ex(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<()> {
        let route = self.router.route_key(key);
        self.conns[route.shard_id].execute(SetEx::routed(route, key, value, ttl_ms))
    }

    /// Reads `key` from its owning shard into `out` and sets a millisecond TTL.
    pub fn get_ex_into(&mut self, key: &[u8], ttl_ms: u64, out: &mut Vec<u8>) -> Result<bool> {
        let route = self.router.route_key(key);
        self.conns[route.shard_id].execute(GetEx::routed(route, key, ttl_ms, out))
    }

    /// Deletes `key` from its owning shard.
    pub fn del(&mut self, key: &[u8]) -> Result<bool> {
        let route = self.router.route_key(key);
        self.conns[route.shard_id].execute(Del::routed(route, key))
    }

    /// Returns whether `key` exists on its owning shard.
    pub fn exists(&mut self, key: &[u8]) -> Result<bool> {
        let route = self.router.route_key(key);
        self.conns[route.shard_id].execute(Exists::routed(route, key))
    }

    /// Returns Redis-compatible TTL seconds for `key` on its owning shard.
    pub fn ttl(&mut self, key: &[u8]) -> Result<i64> {
        let route = self.router.route_key(key);
        self.conns[route.shard_id].execute(Ttl::routed(route, key))
    }

    /// Sets a millisecond TTL on `key` on its owning shard.
    pub fn expire(&mut self, key: &[u8], ttl_ms: u64) -> Result<bool> {
        let route = self.router.route_key(key);
        self.conns[route.shard_id].execute(Expire::routed(route, key, ttl_ms))
    }

    /// Executes a compact Redis command on the owning direct shard.
    ///
    /// Commands that require all shards are rejected; use [`FcnpClient`] against
    /// the fanout listener for those.
    #[cfg(feature = "redis")]
    pub fn redis_command(
        &mut self,
        command: RedisCommandKind,
        args: &[&[u8]],
    ) -> Result<RedisResponse> {
        let route = redis_direct_route(&self.router, command, args)?;
        let shard_id = route.map_or(0, |route| route.shard_id);
        self.conns[shard_id].execute(OptimizedRedisCommand::routed(command, route, args))
    }

    /// Executes a compact Redis command by name on the owning direct shard.
    #[cfg(feature = "redis")]
    pub fn redis_command_by_name(
        &mut self,
        command: &[u8],
        args: &[&[u8]],
    ) -> Result<RedisResponse> {
        self.redis_command(redis_command_kind_from_name(command)?, args)
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

    /// Sets `key` to `value` with a millisecond TTL.
    pub fn set_ex(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<()> {
        let route = self.checked_route(key)?;
        self.conn.execute(SetEx::routed(route, key, value, ttl_ms))
    }

    /// Reads `key` into `out` and sets a millisecond TTL, returning `true` on hit.
    pub fn get_ex_into(&mut self, key: &[u8], ttl_ms: u64, out: &mut Vec<u8>) -> Result<bool> {
        let route = self.checked_route(key)?;
        self.conn.execute(GetEx::routed(route, key, ttl_ms, out))
    }

    /// Deletes `key`, returning `true` when an entry was removed.
    pub fn del(&mut self, key: &[u8]) -> Result<bool> {
        let route = self.checked_route(key)?;
        self.conn.execute(Del::routed(route, key))
    }

    /// Returns whether `key` exists.
    pub fn exists(&mut self, key: &[u8]) -> Result<bool> {
        let route = self.checked_route(key)?;
        self.conn.execute(Exists::routed(route, key))
    }

    /// Returns Redis-compatible TTL seconds for `key`.
    pub fn ttl(&mut self, key: &[u8]) -> Result<i64> {
        let route = self.checked_route(key)?;
        self.conn.execute(Ttl::routed(route, key))
    }

    /// Sets a millisecond TTL on `key`, returning `true` when the TTL changed.
    pub fn expire(&mut self, key: &[u8], ttl_ms: u64) -> Result<bool> {
        let route = self.checked_route(key)?;
        self.conn.execute(Expire::routed(route, key, ttl_ms))
    }

    /// Executes a compact Redis command on this direct shard.
    ///
    /// Commands that require all shards are rejected; use [`FcnpClient`] against
    /// the fanout listener for those.
    #[cfg(feature = "redis")]
    pub fn redis_command(
        &mut self,
        command: RedisCommandKind,
        args: &[&[u8]],
    ) -> Result<RedisResponse> {
        let route = redis_direct_shard_route(&self.router, self.shard_id, command, args)?;
        self.conn
            .execute(OptimizedRedisCommand::routed(command, route, args))
    }

    /// Executes a compact Redis command by name on this direct shard.
    #[cfg(feature = "redis")]
    pub fn redis_command_by_name(
        &mut self,
        command: &[u8],
        args: &[&[u8]],
    ) -> Result<RedisResponse> {
        self.redis_command(redis_command_kind_from_name(command)?, args)
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

    /// Writes a routed SETEX request without flushing or reading its response.
    pub fn begin_pipeline_set_ex(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<()> {
        let route = self.checked_route(key)?;
        setex::write_request(&mut self.conn, Some(route), key, value, ttl_ms)
    }

    /// Writes a routed GETEX request without flushing or reading its response.
    pub fn begin_pipeline_get_ex(&mut self, key: &[u8], ttl_ms: u64) -> Result<()> {
        let route = self.checked_route(key)?;
        getex::write_request(&mut self.conn, Some(route), key, ttl_ms)
    }

    /// Writes a routed DEL request without flushing or reading its response.
    pub fn begin_pipeline_del(&mut self, key: &[u8]) -> Result<()> {
        let route = self.checked_route(key)?;
        del::write_request(&mut self.conn, Some(route), key)
    }

    /// Writes a routed EXISTS request without flushing or reading its response.
    pub fn begin_pipeline_exists(&mut self, key: &[u8]) -> Result<()> {
        let route = self.checked_route(key)?;
        exists::write_request(&mut self.conn, Some(route), key)
    }

    /// Writes a routed TTL request without flushing or reading its response.
    pub fn begin_pipeline_ttl(&mut self, key: &[u8]) -> Result<()> {
        let route = self.checked_route(key)?;
        ttl::write_request(&mut self.conn, Some(route), key)
    }

    /// Writes a routed EXPIRE request without flushing or reading its response.
    pub fn begin_pipeline_expire(&mut self, key: &[u8], ttl_ms: u64) -> Result<()> {
        let route = self.checked_route(key)?;
        expire::write_request(&mut self.conn, Some(route), key, ttl_ms)
    }

    /// Writes a compact Redis command request without flushing or reading its response.
    #[cfg(feature = "redis")]
    pub fn begin_pipeline_redis_command(
        &mut self,
        command: RedisCommandKind,
        args: &[&[u8]],
    ) -> Result<()> {
        let route = redis_direct_shard_route(&self.router, self.shard_id, command, args)?;
        redis::write_request(&mut self.conn, command, route, args)
    }

    /// Writes a compact Redis command request by name without flushing or reading its response.
    #[cfg(feature = "redis")]
    pub fn begin_pipeline_redis_command_by_name(
        &mut self,
        command: &[u8],
        args: &[&[u8]],
    ) -> Result<()> {
        self.begin_pipeline_redis_command(redis_command_kind_from_name(command)?, args)
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

    /// Reads the next pipelined SETEX response.
    pub fn finish_pipeline_set_ex(&mut self) -> Result<()> {
        self.conn
            .expect_ok(<SetEx as crate::commands::FcnpCommand>::NAME)
    }

    /// Reads the next pipelined GETEX response.
    pub fn finish_pipeline_get_ex_into(&mut self, out: &mut Vec<u8>) -> Result<bool> {
        self.conn
            .read_value(<GetEx as crate::commands::FcnpCommand>::NAME, out)
    }

    /// Reads the next pipelined DEL response.
    pub fn finish_pipeline_del(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Del as crate::commands::FcnpCommand>::NAME)
            .map(|deleted| deleted != 0)
    }

    /// Reads the next pipelined EXISTS response.
    pub fn finish_pipeline_exists(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Exists as crate::commands::FcnpCommand>::NAME)
            .map(|exists| exists != 0)
    }

    /// Reads the next pipelined TTL response.
    pub fn finish_pipeline_ttl(&mut self) -> Result<i64> {
        self.conn
            .read_integer(<Ttl as crate::commands::FcnpCommand>::NAME)
    }

    /// Reads the next pipelined EXPIRE response.
    pub fn finish_pipeline_expire(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Expire as crate::commands::FcnpCommand>::NAME)
            .map(|changed| changed != 0)
    }

    /// Reads the next pipelined compact Redis command response.
    #[cfg(feature = "redis")]
    pub fn finish_pipeline_redis_command(&mut self) -> Result<RedisResponse> {
        self.conn.read_redis_response("REDIS")
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

#[cfg(feature = "redis")]
fn redis_command_kind_from_name(command: &[u8]) -> Result<RedisCommandKind> {
    RedisCommandKind::from_name(command).ok_or_else(|| {
        FcnpClientError::Config(format!(
            "unsupported Redis command `{}` for compact FCNP opcode wrapper",
            String::from_utf8_lossy(command)
        ))
    })
}

#[cfg(feature = "redis")]
fn redis_direct_route(
    router: &FcnpDirectRouter,
    command: RedisCommandKind,
    args: &[&[u8]],
) -> Result<Option<FcnpRoute>> {
    let keys = match command.route_keys(args) {
        RedisCommandRouteKeys::None => return Ok(None),
        RedisCommandRouteKeys::AllShards => {
            return Err(FcnpClientError::Config(format!(
                "{} requires all shards; use FcnpClient on the fanout listener",
                command.name()
            )));
        }
        RedisCommandRouteKeys::Keys(keys) if keys.is_empty() => return Ok(None),
        RedisCommandRouteKeys::Keys(keys) => keys,
    };

    let first_route = router.route_key(keys[0]);
    for key in keys.iter().skip(1) {
        let route = router.route_key(key);
        if route.shard_id != first_route.shard_id {
            return Err(FcnpClientError::Config(format!(
                "{} keys span multiple direct shards",
                command.name()
            )));
        }
    }
    Ok(Some(first_route))
}

#[cfg(feature = "redis")]
fn redis_direct_shard_route(
    router: &FcnpDirectRouter,
    shard_id: usize,
    command: RedisCommandKind,
    args: &[&[u8]],
) -> Result<Option<FcnpRoute>> {
    let route = redis_direct_route(router, command, args)?;
    if let Some(route) = route
        && route.shard_id != shard_id
    {
        return Err(FcnpClientError::Config(format!(
            "{} routes to shard {}, but client is connected to shard {}",
            command.name(),
            route.shard_id,
            shard_id
        )));
    }
    Ok(route)
}
