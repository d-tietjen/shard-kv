use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;

use crate::backend::{Backend, BackendClass, BoxError, Worker};

pub struct RespBackend {
    id: &'static str,
    addr: String,
}

impl RespBackend {
    pub fn new(id: &str, addr: &str) -> Result<Self, BoxError> {
        let id_static: &'static str = match id {
            "fc-server-resp" => "fc-server-resp",
            "redis" => "redis",
            "valkey" => "valkey",
            "dragonfly" => "dragonfly",
            other => return Err(format!("unknown RESP backend id: {other}").into()),
        };
        // Connectivity probe.
        let probe = RespConn::connect(addr)?;
        drop(probe);
        Ok(Self {
            id: id_static,
            addr: addr.to_string(),
        })
    }
}

impl Backend for RespBackend {
    fn id(&self) -> &str {
        self.id
    }
    fn class(&self) -> BackendClass {
        BackendClass::Networked
    }
    fn supports_pipelining(&self) -> bool {
        true
    }
    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        let mut c = RespConn::connect(&self.addr)?;
        for k in keys {
            c.set(k, value)?;
        }
        Ok(())
    }
    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        Ok(Box::new(RespWorker {
            conn: RespConn::connect(&self.addr)?,
        }))
    }
}

struct RespWorker {
    conn: RespConn,
}

impl Worker for RespWorker {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        self.conn.get(key, scratch)
    }
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.conn.set(key, value)
    }
    fn begin_pipeline_get(&mut self, key: &[u8]) -> Result<(), BoxError> {
        self.conn.write_get_request(key)
    }
    fn begin_pipeline_set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.conn.write_set_request(key, value)
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
}

/// Minimal RESP2 client. Supports GET, SET, and ordered pipelining.
struct RespConn {
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
    line: Vec<u8>,
}

impl RespConn {
    fn connect(addr: &str) -> Result<Self, BoxError> {
        let s = TcpStream::connect(addr)?;
        s.set_nodelay(true)?;
        let s2 = s.try_clone()?;
        Ok(Self {
            r: BufReader::with_capacity(64 * 1024, s),
            w: BufWriter::with_capacity(64 * 1024, s2),
            line: Vec::with_capacity(64),
        })
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.write_set_request(key, value)?;
        self.flush()?;
        self.read_set_response()
    }

    fn get(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool, BoxError> {
        self.write_get_request(key)?;
        self.flush()?;
        self.read_get_response(out)
    }

    fn write_set_request(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        write_array_header(&mut self.w, 3)?;
        write_bulk(&mut self.w, b"SET")?;
        write_bulk(&mut self.w, key)?;
        write_bulk(&mut self.w, value)?;
        Ok(())
    }

    fn write_get_request(&mut self, key: &[u8]) -> Result<(), BoxError> {
        write_array_header(&mut self.w, 2)?;
        write_bulk(&mut self.w, b"GET")?;
        write_bulk(&mut self.w, key)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), BoxError> {
        self.w.flush()?;
        Ok(())
    }

    fn read_set_response(&mut self) -> Result<(), BoxError> {
        read_line(&mut self.r, &mut self.line)?;
        if self.line.first() != Some(&b'+') {
            return Err(format!("SET error: {}", String::from_utf8_lossy(&self.line)).into());
        }
        Ok(())
    }

    fn read_get_response(&mut self, out: &mut Vec<u8>) -> Result<bool, BoxError> {
        out.clear();
        read_line(&mut self.r, &mut self.line)?;
        let first = self.line.first().copied();
        match first {
            Some(b'$') => {
                let n: i64 = parse_len(&self.line[1..])?;
                if n < 0 {
                    return Ok(false);
                }
                let n = n as usize;
                out.resize(n, 0);
                self.r.read_exact(out.as_mut_slice())?;
                // Consume trailing \r\n
                let mut crlf = [0u8; 2];
                self.r.read_exact(&mut crlf)?;
                Ok(true)
            }
            Some(b'-') => Err(format!("GET error: {}", String::from_utf8_lossy(&self.line)).into()),
            _ => Err(format!(
                "unexpected GET reply: {}",
                String::from_utf8_lossy(&self.line)
            )
            .into()),
        }
    }
}

fn write_array_header<W: Write>(w: &mut W, n: usize) -> Result<(), BoxError> {
    w.write_all(b"*")?;
    write_len(w, n as i64)?;
    Ok(())
}

fn write_bulk<W: Write>(w: &mut W, b: &[u8]) -> Result<(), BoxError> {
    w.write_all(b"$")?;
    write_len(w, b.len() as i64)?;
    w.write_all(b)?;
    w.write_all(b"\r\n")?;
    Ok(())
}

fn write_len<W: Write>(w: &mut W, n: i64) -> Result<(), BoxError> {
    let mut buf = [0u8; 24];
    let s = format_i64(n, &mut buf);
    w.write_all(s)?;
    w.write_all(b"\r\n")?;
    Ok(())
}

fn format_i64(n: i64, buf: &mut [u8; 24]) -> &[u8] {
    use std::io::Cursor;
    let mut c = Cursor::new(&mut buf[..]);
    let _ = std::io::Write::write_all(&mut c, n.to_string().as_bytes());
    let pos = c.position() as usize;
    &buf[..pos]
}

fn read_line<R: Read>(r: &mut BufReader<R>, line: &mut Vec<u8>) -> Result<(), BoxError> {
    line.clear();
    loop {
        let mut byte = [0u8; 1];
        r.read_exact(&mut byte)?;
        if byte[0] == b'\r' {
            // expect \n next
            r.read_exact(&mut byte)?;
            if byte[0] != b'\n' {
                return Err("RESP framing: lone CR".into());
            }
            return Ok(());
        }
        line.push(byte[0]);
        if line.len() > 64 * 1024 {
            return Err("RESP framing: line too long".into());
        }
    }
}

fn parse_len(s: &[u8]) -> Result<i64, BoxError> {
    std::str::from_utf8(s)
        .map_err(|e| BoxError::from(format!("bulk len utf8: {e}")))?
        .parse::<i64>()
        .map_err(|e| BoxError::from(format!("bulk len: {e}")))
}
