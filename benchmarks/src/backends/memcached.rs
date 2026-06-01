use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;

use crate::backend::{Backend, BackendClass, BoxError, Worker};

pub struct MemcachedBackend {
    addr: String,
}

impl MemcachedBackend {
    pub fn new(addr: &str) -> Result<Self, BoxError> {
        let probe = MemcachedConn::connect(addr)?;
        drop(probe);
        Ok(Self {
            addr: addr.to_string(),
        })
    }
}

impl Backend for MemcachedBackend {
    fn id(&self) -> &str {
        "memcached"
    }

    fn class(&self) -> BackendClass {
        BackendClass::Networked
    }

    fn supports_pipelining(&self) -> bool {
        true
    }

    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        let mut conn = MemcachedConn::connect(&self.addr)?;
        for key in keys {
            conn.set(key, value, 0)?;
        }
        Ok(())
    }

    fn warmup_ttl(&self, keys: &[Vec<u8>], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        let ttl_seconds = ttl_seconds(ttl_ms);
        let mut conn = MemcachedConn::connect(&self.addr)?;
        for key in keys {
            conn.set(key, value, ttl_seconds)?;
        }
        Ok(())
    }

    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        Ok(Box::new(MemcachedWorker {
            conn: MemcachedConn::connect(&self.addr)?,
        }))
    }
}

struct MemcachedWorker {
    conn: MemcachedConn,
}

impl Worker for MemcachedWorker {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        self.conn.get(key, scratch)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.conn.set(key, value, 0)
    }

    fn begin_pipeline_get(&mut self, key: &[u8]) -> Result<(), BoxError> {
        self.conn.write_get_request(key)
    }

    fn begin_pipeline_set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.conn.write_set_request(key, value, 0)
    }

    fn flush_pipeline(&mut self) -> Result<(), BoxError> {
        self.conn.flush()
    }

    fn finish_pipeline_get(&mut self, scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        self.conn.read_get_response(scratch)
    }

    fn finish_pipeline_set(&mut self) -> Result<(), BoxError> {
        self.conn.read_set_response()
    }

    fn set_ttl(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        self.conn.set(key, value, ttl_seconds(ttl_ms))
    }
}

struct MemcachedConn {
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
    line: Vec<u8>,
}

impl MemcachedConn {
    fn connect(addr: &str) -> Result<Self, BoxError> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let writer = stream.try_clone()?;
        Ok(Self {
            r: BufReader::with_capacity(64 * 1024, stream),
            w: BufWriter::with_capacity(64 * 1024, writer),
            line: Vec::with_capacity(256),
        })
    }

    fn get(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool, BoxError> {
        self.write_get_request(key)?;
        self.flush()?;
        self.read_get_response(out)
    }

    fn set(&mut self, key: &[u8], value: &[u8], ttl_seconds: u32) -> Result<(), BoxError> {
        self.write_set_request(key, value, ttl_seconds)?;
        self.flush()?;
        self.read_set_response()
    }

    fn write_get_request(&mut self, key: &[u8]) -> Result<(), BoxError> {
        validate_key(key)?;
        self.w.write_all(b"get ")?;
        self.w.write_all(key)?;
        self.w.write_all(b"\r\n")?;
        Ok(())
    }

    fn write_set_request(
        &mut self,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u32,
    ) -> Result<(), BoxError> {
        validate_key(key)?;
        self.w.write_all(b"set ")?;
        self.w.write_all(key)?;
        self.w.write_all(b" 0 ")?;
        write_u32(&mut self.w, ttl_seconds)?;
        self.w.write_all(b" ")?;
        write_usize(&mut self.w, value.len())?;
        self.w.write_all(b"\r\n")?;
        self.w.write_all(value)?;
        self.w.write_all(b"\r\n")?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), BoxError> {
        self.w.flush()?;
        Ok(())
    }

    fn read_set_response(&mut self) -> Result<(), BoxError> {
        read_line(&mut self.r, &mut self.line)?;
        match self.line.as_slice() {
            b"STORED" => Ok(()),
            b"NOT_STORED" => Err("memcached SET failed: NOT_STORED".into()),
            b"SERVER_ERROR out of memory" => Err("memcached SET failed: out of memory".into()),
            line => Err(format!(
                "memcached SET unexpected reply: {}",
                String::from_utf8_lossy(line)
            )
            .into()),
        }
    }

    fn read_get_response(&mut self, out: &mut Vec<u8>) -> Result<bool, BoxError> {
        out.clear();
        read_line(&mut self.r, &mut self.line)?;
        if self.line.as_slice() == b"END" {
            return Ok(false);
        }
        if !self.line.starts_with(b"VALUE ") {
            return Err(format!(
                "memcached GET unexpected reply: {}",
                String::from_utf8_lossy(&self.line)
            )
            .into());
        }
        let bytes = parse_value_bytes(&self.line)?;
        out.resize(bytes, 0);
        self.r.read_exact(out.as_mut_slice())?;
        let mut crlf = [0u8; 2];
        self.r.read_exact(&mut crlf)?;
        if crlf != *b"\r\n" {
            return Err("memcached GET value missing trailing CRLF".into());
        }
        read_line(&mut self.r, &mut self.line)?;
        if self.line.as_slice() != b"END" {
            return Err(format!(
                "memcached GET missing END: {}",
                String::from_utf8_lossy(&self.line)
            )
            .into());
        }
        Ok(true)
    }
}

fn ttl_seconds(ttl_ms: u64) -> u32 {
    ttl_ms.div_ceil(1_000).clamp(1, u32::MAX as u64) as u32
}

fn validate_key(key: &[u8]) -> Result<(), BoxError> {
    if key.is_empty() {
        return Err("memcached key must not be empty".into());
    }
    if key.len() > 250 {
        return Err(format!("memcached key exceeds 250 bytes: {}", key.len()).into());
    }
    if key.iter().any(|byte| *byte <= b' ' || *byte == 0x7f) {
        return Err("memcached key contains whitespace/control byte".into());
    }
    Ok(())
}

fn parse_value_bytes(line: &[u8]) -> Result<usize, BoxError> {
    let text = std::str::from_utf8(line)?;
    let mut parts = text.split_whitespace();
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("VALUE"), Some(_key), Some(_flags), Some(bytes)) => Ok(bytes.parse()?),
        _ => Err(format!("invalid memcached VALUE header: {text}").into()),
    }
}

fn read_line<R: Read>(reader: &mut BufReader<R>, line: &mut Vec<u8>) -> Result<(), BoxError> {
    line.clear();
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        match byte[0] {
            b'\r' => {
                reader.read_exact(&mut byte)?;
                if byte[0] != b'\n' {
                    return Err("memcached protocol: CR not followed by LF".into());
                }
                return Ok(());
            }
            byte => {
                line.push(byte);
                if line.len() > 64 * 1024 {
                    return Err("memcached protocol line too long".into());
                }
            }
        }
    }
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), BoxError> {
    let mut buf = itoa_buf();
    writer.write_all(format_u64(value as u64, &mut buf))?;
    Ok(())
}

fn write_usize<W: Write>(writer: &mut W, value: usize) -> Result<(), BoxError> {
    let mut buf = itoa_buf();
    writer.write_all(format_u64(value as u64, &mut buf))?;
    Ok(())
}

fn itoa_buf() -> [u8; 20] {
    [0; 20]
}

fn format_u64(mut value: u64, buf: &mut [u8; 20]) -> &[u8] {
    if value == 0 {
        buf[19] = b'0';
        return &buf[19..];
    }
    let mut index = buf.len();
    while value != 0 {
        index -= 1;
        buf[index] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    &buf[index..]
}
