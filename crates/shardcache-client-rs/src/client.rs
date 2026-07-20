#[cfg(feature = "redis")]
use std::collections::VecDeque;
use std::net::ToSocketAddrs;
use std::time::Duration;

#[cfg(feature = "tls")]
use crate::ScnpTlsClientConfig;
use crate::commands::del::{self, Del};
use crate::commands::exists::{self, Exists};
use crate::commands::expire::{self, Expire};
use crate::commands::get::{self, Get};
use crate::commands::getex::{self, GetEx};
#[cfg(feature = "redis")]
use crate::commands::redis::{
    self, RedisCommand as OptimizedRedisCommand, RedisCommandKind, RedisCommandRouteKeys,
    RedisRespCommand, RedisResponse,
};
use crate::commands::resp::RespCommand;
use crate::commands::set::{self, Set};
use crate::commands::setex::{self, SetEx};
use crate::commands::ttl::{self, Ttl};
#[cfg(feature = "vector")]
use crate::commands::vector::{Ping, VAdd, VAddOptions, VRem, VSim, VSimMatch, VSimOptions};
use crate::connection::ScnpConnection;
use crate::error::{Result, ShardCacheClientError};
#[cfg(feature = "redis")]
use crate::routing::ShardCacheRoute;
use crate::routing::{ShardCacheDirectRouter, ShardCacheRouteMode};

#[cfg(feature = "redis")]
#[derive(Debug, Clone, Copy)]
enum RedisPipelineResponse {
    Native,
    Resp,
}

/// Blocking SCNP client for the ordinary server listener.
#[derive(Debug)]
pub struct ShardCacheClient {
    conn: ScnpConnection,
    #[cfg(feature = "redis")]
    redis_pipeline_responses: VecDeque<RedisPipelineResponse>,
}

/// Topology advertised by a shardcache server bootstrap listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardCacheTopology {
    pub node_id: String,
    pub shard_count: usize,
    pub route_mode: String,
    pub direct_shard_base_port: u16,
    pub capabilities: Vec<String>,
}

impl ShardCacheClient {
    /// Connects to a shardcache server listener that accepts generic SCNP.
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        Ok(Self {
            conn: ScnpConnection::connect(addr)?,
            #[cfg(feature = "redis")]
            redis_pipeline_responses: VecDeque::new(),
        })
    }

    /// Connects with explicit TCP connect and per-operation I/O deadlines.
    pub fn connect_with_timeouts(
        addr: impl ToSocketAddrs,
        connect_timeout: Duration,
        operation_timeout: Duration,
    ) -> Result<Self> {
        Self::connect_with_timeouts_and_auth(addr, connect_timeout, operation_timeout, None)
    }

    /// Connects with deadlines and authenticates before issuing SCNP commands.
    pub fn connect_with_timeouts_and_auth(
        addr: impl ToSocketAddrs,
        connect_timeout: Duration,
        operation_timeout: Duration,
        auth_token: Option<&[u8]>,
    ) -> Result<Self> {
        let mut conn =
            ScnpConnection::connect_with_timeouts(addr, connect_timeout, operation_timeout)?;
        conn.authenticate(auth_token)?;
        Ok(Self {
            conn,
            #[cfg(feature = "redis")]
            redis_pipeline_responses: VecDeque::new(),
        })
    }

    /// Connects with TLS, deadlines, and optional SCNP token authentication.
    #[cfg(feature = "tls")]
    pub fn connect_with_timeouts_auth_and_tls(
        addr: impl ToSocketAddrs,
        connect_timeout: Duration,
        operation_timeout: Duration,
        auth_token: Option<&[u8]>,
        tls: &ScnpTlsClientConfig,
    ) -> Result<Self> {
        let mut conn = ScnpConnection::connect_with_timeouts_and_tls(
            addr,
            connect_timeout,
            operation_timeout,
            tls,
        )?;
        conn.authenticate(auth_token)?;
        Ok(Self {
            conn,
            #[cfg(feature = "redis")]
            redis_pipeline_responses: VecDeque::new(),
        })
    }

    /// Reads `key` into `out`, returning `true` on hit.
    pub fn get_into(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool> {
        self.conn.execute(Get::new(key, out))
    }

    /// Reads `key` while rejecting response bodies larger than `max_body_len`.
    pub fn get_into_limited(
        &mut self,
        key: &[u8],
        out: &mut Vec<u8>,
        max_body_len: usize,
    ) -> Result<bool> {
        crate::commands::get::write_request(&mut self.conn, None, key)?;
        self.conn.flush()?;
        self.conn.read_value_limited(
            <Get as crate::commands::ScnpCommand>::NAME,
            out,
            max_body_len,
        )
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

    /// Checks the SCNP server and returns its typed `PONG` payload.
    #[cfg(feature = "vector")]
    pub fn ping(&mut self) -> Result<Vec<u8>> {
        self.conn.execute(Ping)
    }

    /// Adds or updates one vector element through the native SCNP `VADD` opcode.
    #[cfg(feature = "vector")]
    pub fn vadd(
        &mut self,
        key: &[u8],
        element: &[u8],
        vector: &[f32],
        options: VAddOptions<'_>,
    ) -> Result<bool> {
        self.conn
            .execute(VAdd::new(None, key, element, vector, options)?)
    }

    /// Returns scored and attributed nearest matches through native SCNP `VSIM`.
    #[cfg(feature = "vector")]
    pub fn vsim(
        &mut self,
        key: &[u8],
        vector: &[f32],
        options: VSimOptions<'_>,
    ) -> Result<Vec<VSimMatch>> {
        self.conn.execute(VSim::new(None, key, vector, options)?)
    }

    /// Removes one vector element through the native SCNP `VREM` opcode.
    #[cfg(feature = "vector")]
    pub fn vrem(&mut self, key: &[u8], element: &[u8]) -> Result<bool> {
        self.conn.execute(VRem::new(None, key, element))
    }

    /// Returns the first-party Redis command namespace.
    #[cfg(feature = "redis")]
    pub fn redis(&mut self) -> crate::Redis<'_, Self> {
        crate::Redis::new(self)
    }

    /// Executes a Redis-compatible command through the compact opcode SCNP wrapper.
    #[cfg(feature = "redis")]
    pub fn redis_command(
        &mut self,
        command: RedisCommandKind,
        args: &[&[u8]],
    ) -> Result<RedisResponse> {
        self.conn.execute(OptimizedRedisCommand::new(command, args))
    }

    /// Executes a Redis-compatible command by name through native SCNP.
    ///
    /// Commands with compact opcodes use the optimized Redis wrapper. Other
    /// names use the SCNP command-name wrapper and return decoded RESP.
    #[cfg(feature = "redis")]
    pub fn redis_command_by_name(
        &mut self,
        command: &[u8],
        args: &[&[u8]],
    ) -> Result<RedisResponse> {
        match RedisCommandKind::from_name(command) {
            Some(command) => self.redis_command(command, args),
            None => self.redis_resp_command(command, args),
        }
    }

    /// Executes a Redis-compatible command through the SCNP command-name wrapper.
    ///
    /// This path is still native SCNP, but it carries the Redis command name in
    /// the body so it can cover commands that do not have a compact opcode.
    #[cfg(feature = "redis")]
    pub fn redis_resp_command(&mut self, command: &[u8], args: &[&[u8]]) -> Result<RedisResponse> {
        validate_redis_command_name(command)?;
        self.conn.execute(RedisRespCommand::new(command, args))
    }

    /// Executes a Redis-compatible command through the generic SCNP wrapper.
    ///
    /// The server returns RESP bytes as an SCNP value. `out` receives those raw
    /// bytes so callers can decode exactly the shape they requested.
    pub fn resp_command_into(&mut self, parts: &[&[u8]], out: &mut Vec<u8>) -> Result<bool> {
        self.conn.execute(RespCommand::new(parts, out))
    }

    /// Runs the global SCNP scan wrapper and returns the RESP scan reply bytes.
    pub fn scan_resp_into(&mut self, cursor: u64, count: usize, out: &mut Vec<u8>) -> Result<bool> {
        let cursor = cursor.to_string();
        let count = count.to_string();
        self.resp_command_into(
            &[b"SCNP.SCAN", cursor.as_bytes(), b"COUNT", count.as_bytes()],
            out,
        )
    }

    /// Runs a shard-local SCNP scan. Call this concurrently per shard to avoid
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
                b"SCNP.SCANSHARD",
                shard_id.as_bytes(),
                cursor.as_bytes(),
                b"COUNT",
                count.as_bytes(),
            ],
            out,
        )
    }

    /// Reads the server's stable topology and transport capabilities.
    pub fn topology(&mut self) -> Result<ShardCacheTopology> {
        let mut response = Vec::new();
        if !self.resp_command_into(&[b"SCNP.TOPOLOGY"], &mut response)? {
            return Err(ShardCacheClientError::Protocol(
                "SCNP.TOPOLOGY returned null".into(),
            ));
        }
        parse_topology_response(&response)
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
        redis::write_request(&mut self.conn, command, None, args)?;
        self.redis_pipeline_responses
            .push_back(RedisPipelineResponse::Native);
        Ok(())
    }

    /// Writes a Redis command request by name without flushing or reading its response.
    ///
    /// Compact-opcode commands use the optimized Redis wrapper. Other command
    /// names use the SCNP command-name wrapper and decode the RESP payload when
    /// [`finish_pipeline_redis_command`](Self::finish_pipeline_redis_command)
    /// is called.
    #[cfg(feature = "redis")]
    pub fn begin_pipeline_redis_command_by_name(
        &mut self,
        command: &[u8],
        args: &[&[u8]],
    ) -> Result<()> {
        match RedisCommandKind::from_name(command) {
            Some(command) => self.begin_pipeline_redis_command(command, args),
            None => self.begin_pipeline_redis_resp_command(command, args),
        }
    }

    /// Writes a Redis command-name wrapper request without flushing or reading its response.
    #[cfg(feature = "redis")]
    pub fn begin_pipeline_redis_resp_command(
        &mut self,
        command: &[u8],
        args: &[&[u8]],
    ) -> Result<()> {
        validate_redis_command_name(command)?;
        redis::write_resp_request(&mut self.conn, command, args)?;
        self.redis_pipeline_responses
            .push_back(RedisPipelineResponse::Resp);
        Ok(())
    }

    /// Flushes all queued pipelined requests.
    pub fn flush_pipeline(&mut self) -> Result<()> {
        self.conn.flush()
    }

    /// Reads the next pipelined GET response.
    pub fn finish_pipeline_get_into(&mut self, out: &mut Vec<u8>) -> Result<bool> {
        self.conn
            .read_value(<Get as crate::commands::ScnpCommand>::NAME, out)
    }

    /// Reads the next pipelined SET response.
    pub fn finish_pipeline_set(&mut self) -> Result<()> {
        self.conn
            .expect_ok(<Set as crate::commands::ScnpCommand>::NAME)
    }

    /// Reads the next pipelined SETEX response.
    pub fn finish_pipeline_set_ex(&mut self) -> Result<()> {
        self.conn
            .expect_ok(<SetEx as crate::commands::ScnpCommand>::NAME)
    }

    /// Reads the next pipelined GETEX response.
    pub fn finish_pipeline_get_ex_into(&mut self, out: &mut Vec<u8>) -> Result<bool> {
        self.conn
            .read_value(<GetEx as crate::commands::ScnpCommand>::NAME, out)
    }

    /// Reads the next pipelined DEL response.
    pub fn finish_pipeline_del(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Del as crate::commands::ScnpCommand>::NAME)
            .map(|deleted| deleted != 0)
    }

    /// Reads the next pipelined EXISTS response.
    pub fn finish_pipeline_exists(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Exists as crate::commands::ScnpCommand>::NAME)
            .map(|exists| exists != 0)
    }

    /// Reads the next pipelined TTL response.
    pub fn finish_pipeline_ttl(&mut self) -> Result<i64> {
        self.conn
            .read_integer(<Ttl as crate::commands::ScnpCommand>::NAME)
    }

    /// Reads the next pipelined EXPIRE response.
    pub fn finish_pipeline_expire(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Expire as crate::commands::ScnpCommand>::NAME)
            .map(|changed| changed != 0)
    }

    /// Reads the next pipelined compact Redis command response.
    #[cfg(feature = "redis")]
    pub fn finish_pipeline_redis_command(&mut self) -> Result<RedisResponse> {
        match self
            .redis_pipeline_responses
            .pop_front()
            .unwrap_or(RedisPipelineResponse::Native)
        {
            RedisPipelineResponse::Native => self.conn.read_native_redis_response("REDIS"),
            RedisPipelineResponse::Resp => self.conn.read_resp_redis_response("RESP"),
        }
    }
}

fn parse_topology_response(response: &[u8]) -> Result<ShardCacheTopology> {
    if response.first() != Some(&b'$') {
        return Err(ShardCacheClientError::Protocol(
            "SCNP.TOPOLOGY did not return a RESP bulk string".into(),
        ));
    }
    let header_end = response
        .windows(2)
        .position(|bytes| bytes == b"\r\n")
        .ok_or_else(|| ShardCacheClientError::Protocol("invalid topology RESP header".into()))?;
    let length = std::str::from_utf8(&response[1..header_end])
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| ShardCacheClientError::Protocol("invalid topology RESP length".into()))?;
    let body_start = header_end + 2;
    let body_end = body_start.checked_add(length).ok_or_else(|| {
        ShardCacheClientError::Protocol("topology response length overflow".into())
    })?;
    if response.len() < body_end + 2 || &response[body_end..body_end + 2] != b"\r\n" {
        return Err(ShardCacheClientError::Protocol(
            "truncated topology RESP body".into(),
        ));
    }
    let body = std::str::from_utf8(&response[body_start..body_end])
        .map_err(|_| ShardCacheClientError::Protocol("topology body is not UTF-8".into()))?;
    let mut fields = body.split('\t');
    let node_id = fields.next().unwrap_or_default().to_string();
    let shard_count = fields
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| ShardCacheClientError::Protocol("invalid topology shard count".into()))?;
    let route_mode = fields.next().unwrap_or_default().to_string();
    let direct_shard_base_port = fields
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| ShardCacheClientError::Protocol("invalid topology direct port".into()))?;
    let capabilities = fields.map(str::to_string).collect::<Vec<_>>();
    Ok(ShardCacheTopology {
        node_id,
        shard_count,
        route_mode,
        direct_shard_base_port,
        capabilities,
    })
}

impl ShardCacheDirectRouter {
    /// Connects directly to one shard-owned port.
    pub fn connect_shard(&self, shard_id: usize) -> Result<ShardCacheDirectShardClient> {
        Ok(ShardCacheDirectShardClient {
            router: *self,
            shard_id,
            conn: ScnpConnection::connect(self.shard_addr(shard_id)?)?,
        })
    }

    /// Connects directly to one shard-owned port with explicit deadlines.
    pub fn connect_shard_with_timeouts(
        &self,
        shard_id: usize,
        connect_timeout: Duration,
        operation_timeout: Duration,
    ) -> Result<ShardCacheDirectShardClient> {
        self.connect_shard_with_timeouts_and_auth(
            shard_id,
            connect_timeout,
            operation_timeout,
            None,
        )
    }

    /// Connects to one shard-owned port with deadlines and SCNP authentication.
    pub fn connect_shard_with_timeouts_and_auth(
        &self,
        shard_id: usize,
        connect_timeout: Duration,
        operation_timeout: Duration,
        auth_token: Option<&[u8]>,
    ) -> Result<ShardCacheDirectShardClient> {
        let mut conn = ScnpConnection::connect_with_timeouts(
            self.shard_addr(shard_id)?,
            connect_timeout,
            operation_timeout,
        )?;
        conn.authenticate(auth_token)?;
        Ok(ShardCacheDirectShardClient {
            router: *self,
            shard_id,
            conn,
        })
    }

    /// Connects directly to one shard-owned TLS port with deadlines and auth.
    #[cfg(feature = "tls")]
    pub fn connect_shard_with_timeouts_auth_and_tls(
        &self,
        shard_id: usize,
        connect_timeout: Duration,
        operation_timeout: Duration,
        auth_token: Option<&[u8]>,
        tls: &ScnpTlsClientConfig,
    ) -> Result<ShardCacheDirectShardClient> {
        let mut conn = ScnpConnection::connect_with_timeouts_and_tls(
            self.shard_addr(shard_id)?,
            connect_timeout,
            operation_timeout,
            tls,
        )?;
        conn.authenticate(auth_token)?;
        Ok(ShardCacheDirectShardClient {
            router: *self,
            shard_id,
            conn,
        })
    }
}

/// Blocking SCNP client that automatically routes each key to its shard port.
#[derive(Debug)]
pub struct ShardCacheDirectClient {
    router: ShardCacheDirectRouter,
    conns: Vec<ScnpConnection>,
}

impl ShardCacheDirectClient {
    /// Connects to every shard-owned port starting at `addr`.
    ///
    /// `addr` must be the first direct shard port, not the fanout port.
    pub fn connect(addr: impl ToSocketAddrs, shard_count: usize) -> Result<Self> {
        let router = ShardCacheDirectRouter::new(addr, shard_count)?;
        Self::connect_with_router(router)
    }

    /// Connects to every shard-owned port using an explicit route mode.
    pub fn connect_with_route_mode(
        addr: impl ToSocketAddrs,
        shard_count: usize,
        route_mode: ShardCacheRouteMode,
    ) -> Result<Self> {
        let router = ShardCacheDirectRouter::new(addr, shard_count)?.with_route_mode(route_mode);
        Self::connect_with_router(router)
    }

    /// Connects to all direct shard ports with operation deadlines and auth.
    pub fn connect_with_timeouts_and_auth(
        addr: impl ToSocketAddrs,
        shard_count: usize,
        connect_timeout: Duration,
        operation_timeout: Duration,
        auth_token: Option<&[u8]>,
    ) -> Result<Self> {
        let router = ShardCacheDirectRouter::new(addr, shard_count)?;
        let mut conns = Vec::with_capacity(router.shard_count());
        for shard_id in 0..router.shard_count() {
            let mut conn = ScnpConnection::connect_with_timeouts(
                router.shard_addr(shard_id)?,
                connect_timeout,
                operation_timeout,
            )?;
            conn.authenticate(auth_token)?;
            conns.push(conn);
        }
        Ok(Self { router, conns })
    }

    fn connect_with_router(router: ShardCacheDirectRouter) -> Result<Self> {
        let mut conns = Vec::with_capacity(router.shard_count());
        for shard_id in 0..router.shard_count() {
            conns.push(ScnpConnection::connect(router.shard_addr(shard_id)?)?);
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

    /// Checks one direct shard connection and returns its typed `PONG` payload.
    #[cfg(feature = "vector")]
    pub fn ping_shard(&mut self, shard_id: usize) -> Result<Vec<u8>> {
        let shard_count = self.conns.len();
        let conn = self.conns.get_mut(shard_id).ok_or_else(|| {
            ShardCacheClientError::Config(format!(
                "shard {shard_id} is outside configured shard count {shard_count}"
            ))
        })?;
        conn.execute(Ping)
    }

    /// Adds or updates one vector element on the dedicated vector shard.
    #[cfg(feature = "vector")]
    pub fn vadd(
        &mut self,
        key: &[u8],
        element: &[u8],
        vector: &[f32],
        options: VAddOptions<'_>,
    ) -> Result<bool> {
        let route = self.router.route_vector_key(key);
        self.conns[route.shard_id].execute(VAdd::new(Some(route), key, element, vector, options)?)
    }

    /// Returns scored and attributed nearest matches from the dedicated vector shard.
    #[cfg(feature = "vector")]
    pub fn vsim(
        &mut self,
        key: &[u8],
        vector: &[f32],
        options: VSimOptions<'_>,
    ) -> Result<Vec<VSimMatch>> {
        let route = self.router.route_vector_key(key);
        self.conns[route.shard_id].execute(VSim::new(Some(route), key, vector, options)?)
    }

    /// Removes one vector element from the dedicated vector shard.
    #[cfg(feature = "vector")]
    pub fn vrem(&mut self, key: &[u8], element: &[u8]) -> Result<bool> {
        let route = self.router.route_vector_key(key);
        self.conns[route.shard_id].execute(VRem::new(Some(route), key, element))
    }

    /// Returns the first-party Redis command namespace for direct shard routing.
    #[cfg(feature = "redis")]
    pub fn redis(&mut self) -> crate::Redis<'_, Self> {
        crate::Redis::new(self)
    }

    /// Executes a compact Redis command on the owning direct shard.
    ///
    /// Commands that require all shards are rejected; use [`ShardCacheClient`] against
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

    /// Runs a shard-local SCNP scan on one direct shard connection. Callers can
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
            return Err(ShardCacheClientError::Config(format!(
                "shard {shard_id} is outside configured shard count {}",
                self.conns.len()
            )));
        }
        let shard_id_text = shard_id.to_string();
        let cursor = cursor.to_string();
        let count = count.to_string();
        self.conns[shard_id].execute(RespCommand::new(
            &[
                b"SCNP.SCANSHARD",
                shard_id_text.as_bytes(),
                cursor.as_bytes(),
                b"COUNT",
                count.as_bytes(),
            ],
            out,
        ))
    }
}

/// Blocking SCNP client pinned to one shard-owned port.
///
/// This is useful for thread-per-shard clients that pre-partition work.
#[derive(Debug)]
pub struct ShardCacheDirectShardClient {
    router: ShardCacheDirectRouter,
    shard_id: usize,
    conn: ScnpConnection,
}

impl ShardCacheDirectShardClient {
    /// Returns the shard this client is connected to.
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    /// Reads `key` into `out`, returning `true` on hit.
    pub fn get_into(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool> {
        let route = self.checked_route(key)?;
        self.conn.execute(Get::routed(route, key, out))
    }

    /// Reads `key` while rejecting response bodies larger than `max_body_len`.
    pub fn get_into_limited(
        &mut self,
        key: &[u8],
        out: &mut Vec<u8>,
        max_body_len: usize,
    ) -> Result<bool> {
        let route = self.checked_route(key)?;
        crate::commands::get::write_request(&mut self.conn, Some(route), key)?;
        self.conn.flush()?;
        self.conn.read_value_limited(
            <Get as crate::commands::ScnpCommand>::NAME,
            out,
            max_body_len,
        )
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

    /// Checks this direct shard connection and returns its typed `PONG` payload.
    #[cfg(feature = "vector")]
    pub fn ping(&mut self) -> Result<Vec<u8>> {
        self.conn.execute(Ping)
    }

    /// Adds or updates one vector element on the dedicated vector shard.
    #[cfg(feature = "vector")]
    pub fn vadd(
        &mut self,
        key: &[u8],
        element: &[u8],
        vector: &[f32],
        options: VAddOptions<'_>,
    ) -> Result<bool> {
        let route = self.checked_vector_route(key)?;
        self.conn
            .execute(VAdd::new(Some(route), key, element, vector, options)?)
    }

    /// Returns scored and attributed matches from the dedicated vector shard.
    #[cfg(feature = "vector")]
    pub fn vsim(
        &mut self,
        key: &[u8],
        vector: &[f32],
        options: VSimOptions<'_>,
    ) -> Result<Vec<VSimMatch>> {
        let route = self.checked_vector_route(key)?;
        self.conn
            .execute(VSim::new(Some(route), key, vector, options)?)
    }

    /// Removes one vector element from the dedicated vector shard.
    #[cfg(feature = "vector")]
    pub fn vrem(&mut self, key: &[u8], element: &[u8]) -> Result<bool> {
        let route = self.checked_vector_route(key)?;
        self.conn.execute(VRem::new(Some(route), key, element))
    }

    /// Returns the first-party Redis command namespace for this shard.
    #[cfg(feature = "redis")]
    pub fn redis(&mut self) -> crate::Redis<'_, Self> {
        crate::Redis::new(self)
    }

    /// Executes a compact Redis command on this direct shard.
    ///
    /// Commands that require all shards are rejected; use [`ShardCacheClient`] against
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

    /// Runs a shard-local SCNP scan on this shard-owned connection.
    pub fn scan_resp_into(&mut self, cursor: u64, count: usize, out: &mut Vec<u8>) -> Result<bool> {
        let shard_id = self.shard_id.to_string();
        let cursor = cursor.to_string();
        let count = count.to_string();
        self.conn.execute(RespCommand::new(
            &[
                b"SCNP.SCANSHARD",
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
            .read_value(<Get as crate::commands::ScnpCommand>::NAME, out)
    }

    /// Reads the next pipelined SET response.
    pub fn finish_pipeline_set(&mut self) -> Result<()> {
        self.conn
            .expect_ok(<Set as crate::commands::ScnpCommand>::NAME)
    }

    /// Reads the next pipelined SETEX response.
    pub fn finish_pipeline_set_ex(&mut self) -> Result<()> {
        self.conn
            .expect_ok(<SetEx as crate::commands::ScnpCommand>::NAME)
    }

    /// Reads the next pipelined GETEX response.
    pub fn finish_pipeline_get_ex_into(&mut self, out: &mut Vec<u8>) -> Result<bool> {
        self.conn
            .read_value(<GetEx as crate::commands::ScnpCommand>::NAME, out)
    }

    /// Reads the next pipelined DEL response.
    pub fn finish_pipeline_del(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Del as crate::commands::ScnpCommand>::NAME)
            .map(|deleted| deleted != 0)
    }

    /// Reads the next pipelined EXISTS response.
    pub fn finish_pipeline_exists(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Exists as crate::commands::ScnpCommand>::NAME)
            .map(|exists| exists != 0)
    }

    /// Reads the next pipelined TTL response.
    pub fn finish_pipeline_ttl(&mut self) -> Result<i64> {
        self.conn
            .read_integer(<Ttl as crate::commands::ScnpCommand>::NAME)
    }

    /// Reads the next pipelined EXPIRE response.
    pub fn finish_pipeline_expire(&mut self) -> Result<bool> {
        self.conn
            .read_integer(<Expire as crate::commands::ScnpCommand>::NAME)
            .map(|changed| changed != 0)
    }

    /// Reads the next pipelined compact Redis command response.
    #[cfg(feature = "redis")]
    pub fn finish_pipeline_redis_command(&mut self) -> Result<RedisResponse> {
        self.conn.read_native_redis_response("REDIS")
    }

    fn checked_route(&self, key: &[u8]) -> Result<crate::routing::ShardCacheRoute> {
        let route = self.router.route_key(key);
        if route.shard_id != self.shard_id {
            return Err(ShardCacheClientError::Config(format!(
                "key routes to shard {}, but client is connected to shard {}",
                route.shard_id, self.shard_id
            )));
        }
        Ok(route)
    }

    #[cfg(feature = "vector")]
    fn checked_vector_route(&self, key: &[u8]) -> Result<crate::routing::ShardCacheRoute> {
        let route = self.router.route_vector_key(key);
        if route.shard_id != self.shard_id {
            return Err(ShardCacheClientError::Config(format!(
                "vector commands route to shard {}, but client is connected to shard {}",
                route.shard_id, self.shard_id
            )));
        }
        Ok(route)
    }
}

#[cfg(feature = "redis")]
fn redis_command_kind_from_name(command: &[u8]) -> Result<RedisCommandKind> {
    RedisCommandKind::from_name(command).ok_or_else(|| {
        ShardCacheClientError::Config(format!(
            "Redis command `{}` is not available on direct SCNP shard clients; use ShardCacheClient on the fanout listener for command-name fallback",
            String::from_utf8_lossy(command)
        ))
    })
}

#[cfg(feature = "redis")]
fn validate_redis_command_name(command: &[u8]) -> Result<()> {
    if command.is_empty() {
        return Err(ShardCacheClientError::Config(
            "Redis command name cannot be empty".into(),
        ));
    }
    if command.iter().any(|byte| byte.is_ascii_whitespace()) {
        return Err(ShardCacheClientError::Config(format!(
            "Redis command name cannot contain whitespace: `{}`",
            String::from_utf8_lossy(command)
        )));
    }
    Ok(())
}

#[cfg(feature = "redis")]
fn redis_direct_route(
    router: &ShardCacheDirectRouter,
    command: RedisCommandKind,
    args: &[&[u8]],
) -> Result<Option<ShardCacheRoute>> {
    let keys = match command.route_keys(args) {
        RedisCommandRouteKeys::None => return Ok(None),
        RedisCommandRouteKeys::AllShards => {
            return Err(ShardCacheClientError::Config(format!(
                "{} requires all shards; use ShardCacheClient on the fanout listener",
                command.name()
            )));
        }
        RedisCommandRouteKeys::Keys(keys) if keys.is_empty() => return Ok(None),
        RedisCommandRouteKeys::Keys(keys) => keys,
    };

    let first_route = if command.uses_vector_shard() {
        router.route_vector_key(keys[0])
    } else {
        router.route_key(keys[0])
    };
    for key in keys.iter().skip(1) {
        let route = if command.uses_vector_shard() {
            router.route_vector_key(key)
        } else {
            router.route_key(key)
        };
        if route.shard_id != first_route.shard_id {
            return Err(ShardCacheClientError::Config(format!(
                "{} keys span multiple direct shards",
                command.name()
            )));
        }
    }
    Ok(Some(first_route))
}

#[cfg(feature = "redis")]
fn redis_direct_shard_route(
    router: &ShardCacheDirectRouter,
    shard_id: usize,
    command: RedisCommandKind,
    args: &[&[u8]],
) -> Result<Option<ShardCacheRoute>> {
    let route = redis_direct_route(router, command, args)?;
    if let Some(route) = route
        && route.shard_id != shard_id
    {
        return Err(ShardCacheClientError::Config(format!(
            "{} routes to shard {}, but client is connected to shard {}",
            command.name(),
            route.shard_id,
            shard_id
        )));
    }
    Ok(route)
}
