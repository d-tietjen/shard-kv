use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::commands::ScnpCommand;
#[cfg(feature = "redis")]
use crate::commands::redis::RedisResponse;
use crate::error::{Result, ShardCacheClientError};
#[cfg(any(feature = "redis", feature = "vector"))]
use crate::protocol::STATUS_ARRAY;
#[cfg(feature = "redis")]
use crate::protocol::STATUS_FLOAT;
use crate::protocol::{
    FAST_PROTOCOL_VERSION, FAST_REQUEST_MAGIC, FAST_RESPONSE_MAGIC, STATUS_ERROR, STATUS_INTEGER,
    STATUS_NULL, STATUS_OK, STATUS_VALUE,
};
#[cfg(feature = "tls")]
use crate::tls::ScnpTlsClientConfig;

const SCNP_READ_BUFFER_BYTES: usize = 8 * 1024;
const SCNP_WRITE_BUFFER_BYTES: usize = 8 * 1024;
const SCNP_MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024 * 1024;
const SCNP_MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
#[cfg(any(feature = "redis", feature = "vector"))]
const SCNP_MAX_ARRAY_ITEMS: usize = 65_536;
const SCNP_AUTH_MAGIC: &[u8; 8] = b"SCAUTH01";

enum ScnpStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl std::fmt::Debug for ScnpStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => formatter.write_str("Plain(TcpStream)"),
            #[cfg(feature = "tls")]
            Self::Tls(_) => formatter.write_str("Tls(StreamOwned)"),
        }
    }
}

impl Read for ScnpStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for ScnpStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.flush(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ScnpConnection {
    r: BufReader<ScnpStream>,
    pub(crate) w: Vec<u8>,
    scratch: Vec<u8>,
    response_aligned: bool,
}

impl ScnpConnection {
    pub(crate) fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let s = TcpStream::connect(addr)?;
        Self::from_stream(s, None, None)
    }

    pub(crate) fn connect_with_timeouts(
        addr: impl ToSocketAddrs,
        connect_timeout: Duration,
        operation_timeout: Duration,
    ) -> Result<Self> {
        let mut last_error = None;
        for addr in addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&addr, connect_timeout) {
                Ok(stream) => return Self::from_stream(stream, Some(operation_timeout), None),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error
            .unwrap_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "address resolved to no endpoints",
                )
            })
            .into())
    }

    #[cfg(feature = "tls")]
    pub(crate) fn connect_with_timeouts_and_tls(
        addr: impl ToSocketAddrs,
        connect_timeout: Duration,
        operation_timeout: Duration,
        tls: &ScnpTlsClientConfig,
    ) -> Result<Self> {
        let mut last_error = None;
        for addr in addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&addr, connect_timeout) {
                Ok(stream) => {
                    return Self::from_stream(stream, Some(operation_timeout), Some(tls));
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error
            .unwrap_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "address resolved to no endpoints",
                )
            })
            .into())
    }

    fn from_stream(
        s: TcpStream,
        operation_timeout: Option<Duration>,
        #[cfg(feature = "tls")] tls: Option<&ScnpTlsClientConfig>,
        #[cfg(not(feature = "tls"))] _tls: Option<&()>,
    ) -> Result<Self> {
        s.set_nodelay(true)?;
        s.set_read_timeout(operation_timeout)?;
        s.set_write_timeout(operation_timeout)?;
        tune_tcp_stream_buffers(&s);
        #[cfg(feature = "tls")]
        let stream = if let Some(tls) = tls {
            let connection = rustls::ClientConnection::new(tls.rustls_config(), tls.server_name())
                .map_err(|error| ShardCacheClientError::Config(error.to_string()))?;
            let mut stream = rustls::StreamOwned::new(connection, s);
            stream.conn.complete_io(&mut stream.sock)?;
            ScnpStream::Tls(Box::new(stream))
        } else {
            ScnpStream::Plain(s)
        };
        #[cfg(not(feature = "tls"))]
        let stream = ScnpStream::Plain(s);
        Ok(Self {
            r: BufReader::with_capacity(SCNP_READ_BUFFER_BYTES, stream),
            w: Vec::with_capacity(SCNP_WRITE_BUFFER_BYTES),
            scratch: Vec::with_capacity(64),
            response_aligned: true,
        })
    }

    pub(crate) fn authenticate(&mut self, token: Option<&[u8]>) -> Result<()> {
        let Some(token) = token else {
            return Ok(());
        };
        let token_len = u16::try_from(token.len()).map_err(|_| {
            ShardCacheClientError::Protocol("SCNP authentication token exceeds 65535 bytes".into())
        })?;
        self.w.write_all(SCNP_AUTH_MAGIC)?;
        self.w.write_all(&token_len.to_le_bytes())?;
        self.w.write_all(token)?;
        self.flush()?;
        let mut response = [0u8; 1];
        self.r.read_exact(&mut response)?;
        if response == [1] {
            Ok(())
        } else {
            Err(ShardCacheClientError::Protocol(
                "SCNP authentication rejected".into(),
            ))
        }
    }

    pub(crate) fn execute<C: ScnpCommand>(&mut self, command: C) -> Result<C::Output> {
        if !self.response_aligned {
            return Err(ShardCacheClientError::Protocol(
                "SCNP connection is unusable after an invalid response frame".into(),
            ));
        }
        let body_len = u32::try_from(command.body_len()).map_err(|_| {
            ShardCacheClientError::Protocol("SCNP request body exceeds the protocol limit".into())
        })?;
        self.write_header(command.opcode(), command.flags(), body_len)?;
        command.write_body(&mut self.w)?;
        self.flush()?;
        command.read_response(self)
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        if !self.w.is_empty() {
            self.r.get_mut().write_all(&self.w)?;
            if self.w.capacity() > SCNP_WRITE_BUFFER_BYTES * 2 {
                self.w = Vec::with_capacity(SCNP_WRITE_BUFFER_BYTES);
            } else {
                self.w.clear();
            }
        }
        self.r.get_mut().flush()?;
        Ok(())
    }

    pub(crate) fn expect_ok(&mut self, op: &str) -> Result<()> {
        let (status, body_len) = self.read_response_header()?;
        match status {
            STATUS_OK => {
                self.discard(body_len)?;
                Ok(())
            }
            STATUS_ERROR => Err(ShardCacheClientError::Protocol(self.read_error(body_len)?)),
            other => self.unexpected_status(op, other, body_len),
        }
    }

    pub(crate) fn read_value(&mut self, op: &str, out: &mut Vec<u8>) -> Result<bool> {
        self.read_value_limited(op, out, SCNP_MAX_RESPONSE_BODY_BYTES)
    }

    pub(crate) fn read_value_limited(
        &mut self,
        op: &str,
        out: &mut Vec<u8>,
        max_body_len: usize,
    ) -> Result<bool> {
        out.clear();
        let (status, body_len) = self.read_response_header()?;
        match status {
            STATUS_VALUE => {
                if body_len > max_body_len {
                    self.response_aligned = false;
                    return Err(ShardCacheClientError::Protocol(format!(
                        "{op} response body length {body_len} exceeds configured maximum {max_body_len}"
                    )));
                }
                out.try_reserve_exact(body_len).map_err(|_| {
                    ShardCacheClientError::Protocol(format!(
                        "{op} response body allocation of {body_len} bytes failed"
                    ))
                })?;
                out.resize(body_len, 0);
                self.r.read_exact(out.as_mut_slice())?;
                Ok(true)
            }
            STATUS_NULL => {
                self.discard(body_len)?;
                Ok(false)
            }
            STATUS_ERROR => Err(ShardCacheClientError::Protocol(self.read_error(body_len)?)),
            other => self.unexpected_status(op, other, body_len),
        }
    }

    pub(crate) fn read_integer(&mut self, op: &str) -> Result<i64> {
        let (status, body_len) = self.read_response_header()?;
        match status {
            STATUS_INTEGER => {
                if body_len != 8 {
                    self.discard(body_len)?;
                    return Err(ShardCacheClientError::Protocol(format!(
                        "{op} integer response body length was {body_len}, expected 8"
                    )));
                }
                let mut value = [0u8; 8];
                self.r.read_exact(&mut value)?;
                Ok(i64::from_le_bytes(value))
            }
            STATUS_ERROR => Err(ShardCacheClientError::Protocol(self.read_error(body_len)?)),
            other => self.unexpected_status(op, other, body_len),
        }
    }

    #[cfg(feature = "vector")]
    pub(crate) fn read_typed_integer(&mut self, op: &str) -> Result<i64> {
        let (status, body_len) = self.read_response_header()?;
        match status {
            STATUS_INTEGER => self.read_integer_body(op, body_len),
            STATUS_VALUE => {
                let body = self.read_limited_body(op, body_len, 64)?;
                parse_resp_integer(body, op)
            }
            STATUS_ERROR => Err(ShardCacheClientError::Protocol(self.read_error(body_len)?)),
            other => self.unexpected_status(op, other, body_len),
        }
    }

    #[cfg(feature = "vector")]
    pub(crate) fn read_typed_value(&mut self, op: &str, max_body_len: usize) -> Result<Vec<u8>> {
        let (status, body_len) = self.read_response_header()?;
        match status {
            STATUS_VALUE => {
                let body = self.read_limited_body(op, body_len, max_body_len)?;
                parse_native_or_resp_value(body, op)
            }
            STATUS_ERROR => Err(ShardCacheClientError::Protocol(self.read_error(body_len)?)),
            other => self.unexpected_status(op, other, body_len),
        }
    }

    #[cfg(feature = "vector")]
    pub(crate) fn read_typed_array(
        &mut self,
        op: &str,
        max_body_len: usize,
        max_items: usize,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let (status, body_len) = self.read_response_header()?;
        match status {
            STATUS_ARRAY => self.read_optional_array_body(op, body_len, max_body_len, max_items),
            STATUS_VALUE => {
                let body = self.read_limited_body(op, body_len, max_body_len)?;
                parse_resp_flat_array(body, op, max_items)
            }
            STATUS_ERROR => Err(ShardCacheClientError::Protocol(self.read_error(body_len)?)),
            other => self.unexpected_status(op, other, body_len),
        }
    }

    #[cfg(feature = "redis")]
    pub(crate) fn read_native_redis_response(&mut self, op: &str) -> Result<RedisResponse> {
        let (status, body_len) = self.read_response_header()?;
        match status {
            STATUS_OK => {
                self.discard(body_len)?;
                Ok(RedisResponse::Ok)
            }
            STATUS_NULL => {
                self.discard(body_len)?;
                Ok(RedisResponse::Null)
            }
            STATUS_ERROR => Err(ShardCacheClientError::Protocol(self.read_error(body_len)?)),
            STATUS_INTEGER => {
                if body_len != 8 {
                    self.discard(body_len)?;
                    return Err(ShardCacheClientError::Protocol(format!(
                        "{op} integer response body length was {body_len}, expected 8"
                    )));
                }
                let mut value = [0u8; 8];
                self.r.read_exact(&mut value)?;
                Ok(RedisResponse::Integer(i64::from_le_bytes(value)))
            }
            STATUS_VALUE => {
                let mut value = vec![0; body_len];
                self.r.read_exact(value.as_mut_slice())?;
                Ok(RedisResponse::Value(value))
            }
            STATUS_ARRAY => self.read_array_response(op, body_len),
            STATUS_FLOAT => {
                if body_len != 8 {
                    self.discard(body_len)?;
                    return Err(ShardCacheClientError::Protocol(format!(
                        "{op} float response body length was {body_len}, expected 8"
                    )));
                }
                let mut value = [0u8; 8];
                self.r.read_exact(&mut value)?;
                Ok(RedisResponse::Float(f64::from_le_bytes(value)))
            }
            other => Err(ShardCacheClientError::Protocol(format!(
                "{op} unexpected response status: {other}"
            ))),
        }
    }

    #[cfg(feature = "redis")]
    pub(crate) fn read_resp_redis_response(&mut self, op: &str) -> Result<RedisResponse> {
        let (status, body_len) = self.read_response_header()?;
        match status {
            STATUS_OK => {
                self.discard(body_len)?;
                Ok(RedisResponse::Ok)
            }
            STATUS_NULL => {
                self.discard(body_len)?;
                Ok(RedisResponse::Null)
            }
            STATUS_ERROR => Err(ShardCacheClientError::Protocol(self.read_error(body_len)?)),
            STATUS_INTEGER => {
                if body_len != 8 {
                    self.discard(body_len)?;
                    return Err(ShardCacheClientError::Protocol(format!(
                        "{op} integer response body length was {body_len}, expected 8"
                    )));
                }
                let mut value = [0u8; 8];
                self.r.read_exact(&mut value)?;
                Ok(RedisResponse::Integer(i64::from_le_bytes(value)))
            }
            STATUS_VALUE => {
                if body_len > SCNP_MAX_RESPONSE_BODY_BYTES {
                    return Err(ShardCacheClientError::Protocol(format!(
                        "{op} RESP response body length {body_len} exceeds maximum {SCNP_MAX_RESPONSE_BODY_BYTES}"
                    )));
                }
                self.scratch.try_reserve_exact(body_len).map_err(|_| {
                    ShardCacheClientError::Protocol(format!(
                        "{op} RESP response allocation of {body_len} bytes failed"
                    ))
                })?;
                self.scratch.resize(body_len, 0);
                self.r.read_exact(&mut self.scratch[..body_len])?;
                RedisResponse::from_resp_bytes(&self.scratch[..body_len])
            }
            STATUS_ARRAY => self.read_array_response(op, body_len),
            STATUS_FLOAT => {
                if body_len != 8 {
                    self.discard(body_len)?;
                    return Err(ShardCacheClientError::Protocol(format!(
                        "{op} float response body length was {body_len}, expected 8"
                    )));
                }
                let mut value = [0u8; 8];
                self.r.read_exact(&mut value)?;
                Ok(RedisResponse::Float(f64::from_le_bytes(value)))
            }
            other => Err(ShardCacheClientError::Protocol(format!(
                "{op} unexpected response status: {other}"
            ))),
        }
    }

    pub(crate) fn write_header(&mut self, cmd: u8, flags: u8, body_len: u32) -> Result<()> {
        let header = [
            FAST_REQUEST_MAGIC,
            FAST_PROTOCOL_VERSION,
            cmd,
            flags,
            body_len as u8,
            (body_len >> 8) as u8,
            (body_len >> 16) as u8,
            (body_len >> 24) as u8,
        ];
        self.w.write_all(&header)?;
        Ok(())
    }

    fn read_response_header(&mut self) -> Result<(u8, usize)> {
        let mut header = [0u8; 8];
        self.r.read_exact(&mut header)?;
        if header[0] != FAST_RESPONSE_MAGIC {
            return Err(ShardCacheClientError::Protocol(format!(
                "bad response magic: 0x{:02X}",
                header[0]
            )));
        }
        if header[1] != FAST_PROTOCOL_VERSION {
            return Err(ShardCacheClientError::Protocol(format!(
                "bad response version: {}",
                header[1]
            )));
        }
        let status = header[2];
        let body_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        Ok((status, body_len))
    }

    #[cfg(feature = "redis")]
    fn read_array_response(&mut self, op: &str, body_len: usize) -> Result<RedisResponse> {
        self.read_optional_array_body(
            op,
            body_len,
            SCNP_MAX_RESPONSE_BODY_BYTES,
            SCNP_MAX_ARRAY_ITEMS,
        )
        .map(|values| {
            RedisResponse::Array(
                values
                    .into_iter()
                    .map(|value| value.map_or(RedisResponse::Null, RedisResponse::Value))
                    .collect(),
            )
        })
    }

    #[cfg(any(feature = "redis", feature = "vector"))]
    fn read_optional_array_body(
        &mut self,
        op: &str,
        body_len: usize,
        max_body_len: usize,
        max_items: usize,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let effective_max = max_body_len.min(SCNP_MAX_RESPONSE_BODY_BYTES);
        if body_len > effective_max {
            self.response_aligned = false;
            return Err(ShardCacheClientError::Protocol(format!(
                "{op} array response body length {body_len} exceeds maximum {effective_max}"
            )));
        }
        self.scratch.try_reserve_exact(body_len).map_err(|_| {
            ShardCacheClientError::Protocol(format!(
                "{op} array response allocation of {body_len} bytes failed"
            ))
        })?;
        self.scratch.resize(body_len, 0);
        self.r.read_exact(&mut self.scratch[..body_len])?;
        if body_len < 4 {
            return Err(ShardCacheClientError::Protocol(format!(
                "{op} array response body length was {body_len}, expected at least 4"
            )));
        }

        let mut cursor = 0usize;
        let count = read_u32(&self.scratch, &mut cursor, op)? as usize;
        if count > max_items
            || count > SCNP_MAX_ARRAY_ITEMS
            || count > self.scratch.len().saturating_sub(cursor) / 4
        {
            return Err(ShardCacheClientError::Protocol(format!(
                "{op} array item count exceeds the configured or response-body limit"
            )));
        }
        let mut values = Vec::new();
        values.try_reserve_exact(count).map_err(|_| {
            ShardCacheClientError::Protocol(format!(
                "{op} array item allocation of {count} entries failed"
            ))
        })?;
        for _ in 0..count {
            let len = read_u32(&self.scratch, &mut cursor, op)?;
            if len == u32::MAX {
                values.push(None);
                continue;
            }
            let len = len as usize;
            let end = cursor.checked_add(len).ok_or_else(|| {
                ShardCacheClientError::Protocol(format!("{op} array item length overflow"))
            })?;
            if end > self.scratch.len() {
                return Err(ShardCacheClientError::Protocol(format!(
                    "{op} array item exceeds response body"
                )));
            }
            values.push(Some(self.scratch[cursor..end].to_vec()));
            cursor = end;
        }
        if cursor != self.scratch.len() {
            return Err(ShardCacheClientError::Protocol(format!(
                "{op} array response has trailing bytes"
            )));
        }
        Ok(values)
    }

    #[cfg(feature = "vector")]
    fn read_integer_body(&mut self, op: &str, body_len: usize) -> Result<i64> {
        if body_len != 8 {
            self.discard(body_len)?;
            return Err(ShardCacheClientError::Protocol(format!(
                "{op} integer response body length was {body_len}, expected 8"
            )));
        }
        let mut value = [0u8; 8];
        self.r.read_exact(&mut value)?;
        Ok(i64::from_le_bytes(value))
    }

    #[cfg(feature = "vector")]
    fn read_limited_body(
        &mut self,
        op: &str,
        body_len: usize,
        max_body_len: usize,
    ) -> Result<&[u8]> {
        let effective_max = max_body_len.min(SCNP_MAX_RESPONSE_BODY_BYTES);
        if body_len > effective_max {
            self.response_aligned = false;
            return Err(ShardCacheClientError::Protocol(format!(
                "{op} response body length {body_len} exceeds configured maximum {effective_max}"
            )));
        }
        self.scratch.try_reserve_exact(body_len).map_err(|_| {
            ShardCacheClientError::Protocol(format!(
                "{op} response allocation of {body_len} bytes failed"
            ))
        })?;
        self.scratch.resize(body_len, 0);
        self.r.read_exact(&mut self.scratch[..body_len])?;
        Ok(&self.scratch[..body_len])
    }

    fn unexpected_status<T>(&mut self, op: &str, status: u8, body_len: usize) -> Result<T> {
        if body_len != 0 {
            self.response_aligned = false;
        }
        Err(ShardCacheClientError::Protocol(format!(
            "{op} unexpected response status: {status}"
        )))
    }

    fn discard(&mut self, n: usize) -> Result<()> {
        let mut remaining = n;
        if self.scratch.len() < SCNP_READ_BUFFER_BYTES {
            self.scratch.resize(SCNP_READ_BUFFER_BYTES, 0);
        }
        while remaining > 0 {
            let chunk = remaining.min(self.scratch.len());
            self.r.read_exact(&mut self.scratch[..chunk])?;
            remaining -= chunk;
        }
        Ok(())
    }

    fn read_error(&mut self, body_len: usize) -> Result<String> {
        let retained = body_len.min(SCNP_MAX_ERROR_BODY_BYTES);
        self.scratch.try_reserve_exact(retained).map_err(|_| {
            ShardCacheClientError::Protocol("SCNP error response allocation failed".into())
        })?;
        self.scratch.resize(retained, 0);
        self.r.read_exact(&mut self.scratch[..retained])?;
        let message = String::from_utf8_lossy(&self.scratch[..retained]).into_owned();
        self.discard(body_len - retained)?;
        if body_len > retained {
            Ok(format!("{message} [truncated from {body_len} bytes]"))
        } else {
            Ok(message)
        }
    }
}

#[cfg(unix)]
fn configured_tcp_buffer_bytes() -> Option<usize> {
    static VALUE: std::sync::OnceLock<Option<usize>> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("SCNP_CLIENT_TCP_BUFFER_BYTES")
            .or_else(|_| std::env::var("SHARDCACHE_TCP_BUFFER_BYTES"))
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
    })
}

#[cfg(unix)]
fn tune_tcp_stream_buffers(stream: &TcpStream) {
    let Some(buffer_bytes) = configured_tcp_buffer_bytes() else {
        return;
    };
    let Ok(value) = libc::c_int::try_from(buffer_bytes) else {
        return;
    };

    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    let value_ptr = (&value as *const libc::c_int).cast::<libc::c_void>();
    let value_len = std::mem::size_of_val(&value) as libc::socklen_t;

    unsafe {
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, value_ptr, value_len);
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, value_ptr, value_len);
    }
}

#[cfg(not(unix))]
fn tune_tcp_stream_buffers(_stream: &TcpStream) {}

#[cfg(any(feature = "redis", feature = "vector"))]
fn read_u32(buf: &[u8], cursor: &mut usize, op: &str) -> Result<u32> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| ShardCacheClientError::Protocol(format!("{op} array cursor overflow")))?;
    if end > buf.len() {
        return Err(ShardCacheClientError::Protocol(format!(
            "{op} truncated array response"
        )));
    }
    let value = u32::from_le_bytes(buf[*cursor..end].try_into().unwrap());
    *cursor = end;
    Ok(value)
}

#[cfg(feature = "vector")]
fn parse_resp_integer(body: &[u8], op: &str) -> Result<i64> {
    if body.first() != Some(&b':') {
        return Err(ShardCacheClientError::Protocol(format!(
            "{op} legacy response was not a RESP integer"
        )));
    }
    let mut cursor = 1;
    let line = read_resp_line(body, &mut cursor, op)?;
    if cursor != body.len() {
        return Err(ShardCacheClientError::Protocol(format!(
            "{op} RESP integer has trailing bytes"
        )));
    }
    std::str::from_utf8(line)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| ShardCacheClientError::Protocol(format!("{op} RESP integer is invalid")))
}

#[cfg(feature = "vector")]
fn parse_native_or_resp_value(body: &[u8], op: &str) -> Result<Vec<u8>> {
    match body.first().copied() {
        Some(b'+') => {
            let mut cursor = 1;
            let value = read_resp_line(body, &mut cursor, op)?.to_vec();
            ensure_resp_consumed(body, cursor, op)?;
            Ok(value)
        }
        Some(b'$') => {
            let mut cursor = 1;
            let value = read_resp_bulk(body, &mut cursor, op)?.ok_or_else(|| {
                ShardCacheClientError::Protocol(format!("{op} returned a null RESP value"))
            })?;
            ensure_resp_consumed(body, cursor, op)?;
            Ok(value)
        }
        Some(b'-') => {
            let mut cursor = 1;
            let message = read_resp_line(body, &mut cursor, op)?;
            Err(ShardCacheClientError::Protocol(
                String::from_utf8_lossy(message).into_owned(),
            ))
        }
        _ => Ok(body.to_vec()),
    }
}

#[cfg(feature = "vector")]
fn parse_resp_flat_array(body: &[u8], op: &str, max_items: usize) -> Result<Vec<Option<Vec<u8>>>> {
    if body.first() == Some(&b'-') {
        let mut cursor = 1;
        let message = read_resp_line(body, &mut cursor, op)?;
        return Err(ShardCacheClientError::Protocol(
            String::from_utf8_lossy(message).into_owned(),
        ));
    }
    if body.first() != Some(&b'*') {
        return Err(ShardCacheClientError::Protocol(format!(
            "{op} legacy response was not a RESP array"
        )));
    }
    let mut cursor = 1;
    let count = parse_resp_len(read_resp_line(body, &mut cursor, op)?, op)?
        .ok_or_else(|| ShardCacheClientError::Protocol(format!("{op} returned a null array")))?;
    if count > max_items || count > SCNP_MAX_ARRAY_ITEMS {
        return Err(ShardCacheClientError::Protocol(format!(
            "{op} RESP array item count exceeds the configured limit"
        )));
    }
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|_| {
        ShardCacheClientError::Protocol(format!(
            "{op} RESP array allocation of {count} entries failed"
        ))
    })?;
    for _ in 0..count {
        let prefix = body.get(cursor).copied().ok_or_else(|| {
            ShardCacheClientError::Protocol(format!("{op} RESP array is truncated"))
        })?;
        cursor += 1;
        match prefix {
            b'$' => values.push(read_resp_bulk(body, &mut cursor, op)?),
            b'+' => values.push(Some(read_resp_line(body, &mut cursor, op)?.to_vec())),
            b'_' => {
                let end = cursor.checked_add(2).ok_or_else(|| {
                    ShardCacheClientError::Protocol(format!("{op} RESP null length overflow"))
                })?;
                if body.get(cursor..end) != Some(b"\r\n") {
                    return Err(ShardCacheClientError::Protocol(format!(
                        "{op} RESP null is malformed"
                    )));
                }
                cursor = end;
                values.push(None);
            }
            b'-' => {
                let message = read_resp_line(body, &mut cursor, op)?;
                return Err(ShardCacheClientError::Protocol(
                    String::from_utf8_lossy(message).into_owned(),
                ));
            }
            other => {
                return Err(ShardCacheClientError::Protocol(format!(
                    "{op} RESP array contains unsupported prefix 0x{other:02X}"
                )));
            }
        }
    }
    ensure_resp_consumed(body, cursor, op)?;
    Ok(values)
}

#[cfg(feature = "vector")]
fn read_resp_bulk(body: &[u8], cursor: &mut usize, op: &str) -> Result<Option<Vec<u8>>> {
    let len = parse_resp_len(read_resp_line(body, cursor, op)?, op)?;
    let Some(len) = len else {
        return Ok(None);
    };
    let end = cursor.checked_add(len).ok_or_else(|| {
        ShardCacheClientError::Protocol(format!("{op} RESP bulk length overflow"))
    })?;
    let frame_end = end.checked_add(2).ok_or_else(|| {
        ShardCacheClientError::Protocol(format!("{op} RESP bulk frame length overflow"))
    })?;
    if body.get(end..frame_end) != Some(b"\r\n") {
        return Err(ShardCacheClientError::Protocol(format!(
            "{op} RESP bulk value is truncated"
        )));
    }
    let value = body
        .get(*cursor..end)
        .ok_or_else(|| ShardCacheClientError::Protocol(format!("{op} RESP bulk is truncated")))?
        .to_vec();
    *cursor = frame_end;
    Ok(Some(value))
}

#[cfg(feature = "vector")]
fn parse_resp_len(line: &[u8], op: &str) -> Result<Option<usize>> {
    let value = std::str::from_utf8(line)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| ShardCacheClientError::Protocol(format!("{op} RESP length is invalid")))?;
    if value == -1 {
        return Ok(None);
    }
    usize::try_from(value)
        .map(Some)
        .map_err(|_| ShardCacheClientError::Protocol(format!("{op} RESP length is negative")))
}

#[cfg(feature = "vector")]
fn read_resp_line<'a>(body: &'a [u8], cursor: &mut usize, op: &str) -> Result<&'a [u8]> {
    let relative_end = body
        .get(*cursor..)
        .and_then(|remaining| remaining.windows(2).position(|bytes| bytes == b"\r\n"))
        .ok_or_else(|| ShardCacheClientError::Protocol(format!("{op} RESP line is truncated")))?;
    let end = *cursor + relative_end;
    let line = &body[*cursor..end];
    *cursor = end + 2;
    Ok(line)
}

#[cfg(feature = "vector")]
fn ensure_resp_consumed(body: &[u8], cursor: usize, op: &str) -> Result<()> {
    if cursor == body.len() {
        Ok(())
    } else {
        Err(ShardCacheClientError::Protocol(format!(
            "{op} RESP response has trailing bytes"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "vector")]
    use crate::commands::vector::Ping;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn limited_value_rejects_advertised_body_before_allocation() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let body_len = 64 * 1024 * 1024u32;
            stream
                .write_all(&[
                    FAST_RESPONSE_MAGIC,
                    FAST_PROTOCOL_VERSION,
                    STATUS_VALUE,
                    0,
                    body_len as u8,
                    (body_len >> 8) as u8,
                    (body_len >> 16) as u8,
                    (body_len >> 24) as u8,
                ])
                .unwrap();
        });
        let mut connection = ScnpConnection::connect(address).unwrap();
        let mut output = Vec::new();

        let error = connection
            .read_value_limited("GET", &mut output, 1024)
            .unwrap_err();
        assert!(error.to_string().contains("exceeds configured maximum"));
        assert!(output.capacity() < 64 * 1024 * 1024);
        server.join().unwrap();
    }

    #[test]
    fn truncated_value_body_returns_error_without_uninitialized_output() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .write_all(&[
                    FAST_RESPONSE_MAGIC,
                    FAST_PROTOCOL_VERSION,
                    STATUS_VALUE,
                    0,
                    8,
                    0,
                    0,
                    0,
                    b'a',
                    b'b',
                ])
                .unwrap();
        });
        let mut connection = ScnpConnection::connect(address).unwrap();
        let mut output = Vec::new();

        let error = connection
            .read_value_limited("GET", &mut output, 8)
            .unwrap_err();
        assert!(matches!(
            error,
            ShardCacheClientError::Io(ref error)
                if error.kind() == std::io::ErrorKind::UnexpectedEof
        ));
        assert_eq!(&output[..2], b"ab");
        assert!(output[2..].iter().all(|byte| *byte == 0));
        server.join().unwrap();
    }

    #[cfg(feature = "vector")]
    #[test]
    fn typed_ping_uses_auth_and_native_value_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut auth_header = [0u8; 10];
            stream.read_exact(&mut auth_header).unwrap();
            assert_eq!(&auth_header[..8], SCNP_AUTH_MAGIC);
            let token_len = u16::from_le_bytes(auth_header[8..].try_into().unwrap()) as usize;
            let mut token = vec![0u8; token_len];
            stream.read_exact(&mut token).unwrap();
            assert_eq!(token, b"rotating-secret");
            stream.write_all(&[1]).unwrap();

            let mut request = [0u8; 9];
            stream.read_exact(&mut request).unwrap();
            assert_eq!(
                request,
                [
                    FAST_REQUEST_MAGIC,
                    FAST_PROTOCOL_VERSION,
                    9,
                    crate::protocol::FAST_FLAG_REDIS_COMMAND_ARGS,
                    1,
                    0,
                    0,
                    0,
                    0,
                ]
            );
            stream
                .write_all(&[
                    FAST_RESPONSE_MAGIC,
                    FAST_PROTOCOL_VERSION,
                    STATUS_VALUE,
                    0,
                    4,
                    0,
                    0,
                    0,
                    b'P',
                    b'O',
                    b'N',
                    b'G',
                ])
                .unwrap();
        });

        let timeout = Duration::from_secs(2);
        let mut connection =
            ScnpConnection::connect_with_timeouts(address, timeout, timeout).unwrap();
        connection.authenticate(Some(b"rotating-secret")).unwrap();
        assert_eq!(connection.execute(Ping).unwrap(), b"PONG");
        server.join().unwrap();
    }

    #[cfg(feature = "vector")]
    #[test]
    fn oversized_typed_response_does_not_allocate_or_reuse_the_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .write_all(&[
                    FAST_RESPONSE_MAGIC,
                    FAST_PROTOCOL_VERSION,
                    STATUS_VALUE,
                    0,
                    65,
                    0,
                    0,
                    0,
                ])
                .unwrap();
        });

        let mut connection = ScnpConnection::connect(address).unwrap();
        let error = connection.read_typed_value("PING", 64).unwrap_err();
        assert!(error.to_string().contains("exceeds configured maximum"));
        assert!(connection.scratch.capacity() < 65);
        assert!(
            connection
                .execute(Ping)
                .unwrap_err()
                .to_string()
                .contains("unusable")
        );
        server.join().unwrap();
    }

    #[cfg(feature = "vector")]
    #[test]
    fn legacy_vector_response_parsers_reject_truncation_and_trailing_data() {
        assert_eq!(parse_resp_integer(b":1\r\n", "VADD").unwrap(), 1);
        assert!(parse_resp_integer(b":1\r\nextra", "VADD").is_err());
        assert!(parse_resp_flat_array(b"*1\r\n_", "VSIM", 3).is_err());
        assert!(parse_resp_flat_array(b"*1\r\n$8\r\nshort\r\n", "VSIM", 3).is_err());
    }
}
