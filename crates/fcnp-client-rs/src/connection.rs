use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};

use crate::commands::FcnpCommand;
use crate::error::{FcnpClientError, Result};
use crate::protocol::{
    FAST_PROTOCOL_VERSION, FAST_REQUEST_MAGIC, FAST_RESPONSE_MAGIC, STATUS_ERROR, STATUS_NULL,
    STATUS_OK, STATUS_VALUE,
};

#[derive(Debug)]
pub(crate) struct FcnpConnection {
    r: BufReader<TcpStream>,
    pub(crate) w: BufWriter<TcpStream>,
    scratch: Vec<u8>,
}

impl FcnpConnection {
    pub(crate) fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let s = TcpStream::connect(addr)?;
        s.set_nodelay(true)?;
        tune_tcp_stream_buffers(&s);
        let s2 = s.try_clone()?;
        Ok(Self {
            r: BufReader::with_capacity(64 * 1024, s),
            w: BufWriter::with_capacity(64 * 1024, s2),
            scratch: Vec::with_capacity(64),
        })
    }

    pub(crate) fn execute<C: FcnpCommand>(&mut self, command: C) -> Result<C::Output> {
        self.write_header(C::OPCODE, command.flags(), command.body_len() as u32)?;
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
            STATUS_ERROR => Err(FcnpClientError::Protocol(self.read_error(body_len)?)),
            other => Err(FcnpClientError::Protocol(format!(
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
            STATUS_ERROR => Err(FcnpClientError::Protocol(self.read_error(body_len)?)),
            other => Err(FcnpClientError::Protocol(format!(
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
            return Err(FcnpClientError::Protocol(format!(
                "bad response magic: 0x{:02X}",
                header[0]
            )));
        }
        if header[1] != FAST_PROTOCOL_VERSION {
            return Err(FcnpClientError::Protocol(format!(
                "bad response version: {}",
                header[1]
            )));
        }
        let status = header[2];
        let body_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        Ok((status, body_len))
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
        std::env::var("FCNP_CLIENT_TCP_BUFFER_BYTES")
            .or_else(|_| std::env::var("FAST_CACHE_TCP_BUFFER_BYTES"))
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
