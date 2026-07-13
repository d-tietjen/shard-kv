use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::commands::ScnpCommand;
#[cfg(feature = "redis")]
use crate::commands::redis::RedisResponse;
use crate::error::{Result, ShardCacheClientError};
use crate::protocol::{
    FAST_PROTOCOL_VERSION, FAST_REQUEST_MAGIC, FAST_RESPONSE_MAGIC, STATUS_ERROR, STATUS_INTEGER,
    STATUS_NULL, STATUS_OK, STATUS_VALUE,
};
#[cfg(feature = "redis")]
use crate::protocol::{STATUS_ARRAY, STATUS_FLOAT};

#[derive(Debug)]
pub(crate) struct ScnpConnection {
    r: BufReader<TcpStream>,
    pub(crate) w: BufWriter<TcpStream>,
    scratch: Vec<u8>,
}

impl ScnpConnection {
    pub(crate) fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let s = TcpStream::connect(addr)?;
        Self::from_stream(s, None)
    }

    pub(crate) fn connect_with_timeouts(
        addr: impl ToSocketAddrs,
        connect_timeout: Duration,
        operation_timeout: Duration,
    ) -> Result<Self> {
        let mut last_error = None;
        for addr in addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&addr, connect_timeout) {
                Ok(stream) => return Self::from_stream(stream, Some(operation_timeout)),
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

    fn from_stream(s: TcpStream, operation_timeout: Option<Duration>) -> Result<Self> {
        s.set_nodelay(true)?;
        s.set_read_timeout(operation_timeout)?;
        s.set_write_timeout(operation_timeout)?;
        tune_tcp_stream_buffers(&s);
        let s2 = s.try_clone()?;
        Ok(Self {
            r: BufReader::with_capacity(64 * 1024, s),
            w: BufWriter::with_capacity(64 * 1024, s2),
            scratch: Vec::with_capacity(64),
        })
    }

    pub(crate) fn execute<C: ScnpCommand>(&mut self, command: C) -> Result<C::Output> {
        self.write_header(command.opcode(), command.flags(), command.body_len() as u32)?;
        command.write_body(&mut self.w)?;
        self.w.flush()?;
        command.read_response(self)
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        self.w.flush()?;
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
            other => Err(ShardCacheClientError::Protocol(format!(
                "{op} unexpected response status: {other}"
            ))),
        }
    }

    pub(crate) fn read_value(&mut self, op: &str, out: &mut Vec<u8>) -> Result<bool> {
        out.clear();
        let (status, body_len) = self.read_response_header()?;
        match status {
            STATUS_VALUE => {
                out.resize(body_len, 0);
                self.r.read_exact(out.as_mut_slice())?;
                Ok(true)
            }
            STATUS_NULL => {
                self.discard(body_len)?;
                Ok(false)
            }
            STATUS_ERROR => Err(ShardCacheClientError::Protocol(self.read_error(body_len)?)),
            other => Err(ShardCacheClientError::Protocol(format!(
                "{op} unexpected response status: {other}"
            ))),
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
            other => Err(ShardCacheClientError::Protocol(format!(
                "{op} unexpected response status: {other}"
            ))),
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
        self.scratch.resize(body_len, 0);
        self.r.read_exact(&mut self.scratch[..body_len])?;
        if body_len < 4 {
            return Err(ShardCacheClientError::Protocol(format!(
                "{op} array response body length was {body_len}, expected at least 4"
            )));
        }

        let mut cursor = 0usize;
        let count = read_u32(&self.scratch, &mut cursor, op)? as usize;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let len = read_u32(&self.scratch, &mut cursor, op)?;
            if len == u32::MAX {
                values.push(RedisResponse::Null);
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
            values.push(RedisResponse::Value(self.scratch[cursor..end].to_vec()));
            cursor = end;
        }
        if cursor != self.scratch.len() {
            return Err(ShardCacheClientError::Protocol(format!(
                "{op} array response has trailing bytes"
            )));
        }
        Ok(RedisResponse::Array(values))
    }

    fn discard(&mut self, n: usize) -> Result<()> {
        if n == 0 {
            return Ok(());
        }
        self.scratch.resize(n, 0);
        self.r.read_exact(&mut self.scratch[..n])?;
        Ok(())
    }

    fn read_error(&mut self, body_len: usize) -> Result<String> {
        self.scratch.resize(body_len, 0);
        self.r.read_exact(&mut self.scratch[..body_len])?;
        Ok(String::from_utf8_lossy(&self.scratch[..body_len]).into_owned())
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

#[cfg(feature = "redis")]
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
