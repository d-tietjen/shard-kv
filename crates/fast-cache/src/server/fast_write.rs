use super::wire::ServerWire;
use super::*;

#[cfg(feature = "embedded")]
pub(super) enum FastWriteItem {
    Bytes(bytes::Bytes),
    FastValue {
        header: [u8; 8],
        payload: bytes::Bytes,
    },
    RespValue {
        header: [u8; RESP_HEADER_MAX_LEN],
        header_len: u8,
        payload: bytes::Bytes,
    },
}

#[cfg(feature = "embedded")]
#[derive(Default)]
pub(crate) struct FastWriteQueue {
    items: Vec<FastWriteItem>,
}

#[cfg(feature = "embedded")]
impl FastWriteQueue {
    #[inline(always)]
    pub(crate) fn flush_bytes(&mut self, out: &mut BytesMut) {
        if !out.is_empty() {
            self.items.push(FastWriteItem::Bytes(out.split().freeze()));
        }
    }

    #[inline(always)]
    pub(crate) fn push_fast_value(&mut self, out: &mut BytesMut, payload: &bytes::Bytes) {
        if payload.len() == 64 {
            ServerWire::write_fast_value_64(out, payload.as_ref());
        } else if payload.len() >= FCNP_ZERO_COPY_VALUE_THRESHOLD {
            self.flush_bytes(out);
            self.items.push(FastWriteItem::FastValue {
                header: ServerWire::fast_value_header(payload.len()),
                payload: payload.clone(),
            });
        } else {
            ServerWire::write_fast_value(out, payload.as_ref());
        }
    }

    #[inline(always)]
    pub(crate) fn push_resp_value(&mut self, out: &mut BytesMut, payload: &bytes::Bytes) {
        if payload.len() < RESP_ZERO_COPY_VALUE_THRESHOLD {
            ServerWire::write_resp_blob_string(out, payload.as_ref());
            return;
        }

        self.flush_bytes(out);
        let (header, header_len) = ServerWire::resp_blob_header(payload.len());
        self.items.push(FastWriteItem::RespValue {
            header,
            header_len,
            payload: payload.clone(),
        });
    }

    #[inline(always)]
    fn materialize_into(&mut self, out: &mut BytesMut) {
        for item in self.items.drain(..) {
            match item {
                FastWriteItem::Bytes(bytes) => out.extend_from_slice(bytes.as_ref()),
                FastWriteItem::FastValue { header, payload } => {
                    out.extend_from_slice(&header);
                    out.extend_from_slice(payload.as_ref());
                }
                FastWriteItem::RespValue {
                    header,
                    header_len,
                    payload,
                } => {
                    out.extend_from_slice(&header[..header_len as usize]);
                    out.extend_from_slice(payload.as_ref());
                    out.extend_from_slice(RESP_CRLF);
                }
            }
        }
    }

    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    #[inline(always)]
    pub(super) fn materialize_optional(queue: Option<&mut FastWriteQueue>, out: &mut BytesMut) {
        if let Some(queue) = queue
            && !queue.is_empty()
        {
            queue.materialize_into(out);
        }
    }

    #[cfg(all(target_os = "linux", feature = "monoio"))]
    #[inline(always)]
    pub(super) fn drain_iovec_batch(&mut self, max_iovecs: usize) -> Vec<FastWriteItem> {
        let mut take = 0usize;
        let mut iovecs = 0usize;
        for item in &self.items {
            let item_iovecs = item.iovec_count();
            if take > 0 && iovecs + item_iovecs > max_iovecs {
                break;
            }
            take += 1;
            iovecs += item_iovecs;
            if iovecs >= max_iovecs {
                break;
            }
        }
        self.items.drain(..take).collect()
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl FastWriteItem {
    #[inline(always)]
    fn iovec_count(&self) -> usize {
        match self {
            FastWriteItem::Bytes(_) => 1,
            FastWriteItem::FastValue { .. } => 2,
            FastWriteItem::RespValue { .. } => 3,
        }
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
pub(super) struct FastWriteBatchIoVec {
    items: Vec<FastWriteItem>,
    iovecs: Vec<libc::iovec>,
    offset: usize,
    remaining_len: usize,
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
impl FastWriteBatchIoVec {
    #[inline(always)]
    pub(super) fn new(items: Vec<FastWriteItem>) -> Self {
        let iovec_count = items.iter().map(FastWriteItem::iovec_count).sum();
        let mut batch = Self {
            items,
            iovecs: Vec::with_capacity(iovec_count),
            offset: 0,
            remaining_len: 0,
        };
        for item in &batch.items {
            match item {
                FastWriteItem::Bytes(bytes) => {
                    batch.remaining_len += bytes.len();
                    batch.iovecs.push(libc::iovec {
                        iov_base: bytes.as_ptr() as *mut libc::c_void,
                        iov_len: bytes.len(),
                    });
                }
                FastWriteItem::FastValue { header, payload } => {
                    batch.remaining_len += header.len() + payload.len();
                    batch.iovecs.push(libc::iovec {
                        iov_base: header.as_ptr() as *mut libc::c_void,
                        iov_len: header.len(),
                    });
                    batch.iovecs.push(libc::iovec {
                        iov_base: payload.as_ptr() as *mut libc::c_void,
                        iov_len: payload.len(),
                    });
                }
                FastWriteItem::RespValue {
                    header,
                    header_len,
                    payload,
                } => {
                    let header_len = *header_len as usize;
                    batch.remaining_len += header_len + payload.len() + RESP_CRLF.len();
                    batch.iovecs.push(libc::iovec {
                        iov_base: header.as_ptr() as *mut libc::c_void,
                        iov_len: header_len,
                    });
                    batch.iovecs.push(libc::iovec {
                        iov_base: payload.as_ptr() as *mut libc::c_void,
                        iov_len: payload.len(),
                    });
                    batch.iovecs.push(libc::iovec {
                        iov_base: RESP_CRLF.as_ptr() as *mut libc::c_void,
                        iov_len: RESP_CRLF.len(),
                    });
                }
            }
        }
        batch
    }

    #[inline(always)]
    fn remaining_len(&self) -> usize {
        self.remaining_len
    }

    #[inline(always)]
    fn consume(&mut self, mut amt: usize) {
        let consumed = amt;
        while amt > 0 {
            let Some(iovec) = self.iovecs.get_mut(self.offset) else {
                self.remaining_len = 0;
                return;
            };
            match iovec.iov_len.cmp(&amt) {
                std::cmp::Ordering::Less => {
                    amt -= iovec.iov_len;
                    self.offset += 1;
                }
                std::cmp::Ordering::Equal => {
                    self.offset += 1;
                    break;
                }
                std::cmp::Ordering::Greater => {
                    // SAFETY: `amt < iov_len`, so the adjusted base remains
                    // inside the same valid iovec buffer.
                    iovec.iov_base = unsafe { iovec.iov_base.add(amt) };
                    iovec.iov_len -= amt;
                    break;
                }
            }
        }
        self.remaining_len = self.remaining_len.saturating_sub(consumed);
    }

    pub(super) async fn write_all(
        stream: &mut monoio::net::TcpStream,
        mut batch: FastWriteBatchIoVec,
    ) -> std::io::Result<()> {
        use monoio::io::AsyncWriteRent;

        while batch.remaining_len() > 0 {
            let remaining = batch.remaining_len();
            let (res, returned) = stream.writev(batch).await;
            batch = returned;
            let written = res?;
            if written == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write whole vectored response batch",
                ));
            }
            if written > remaining {
                return Err(std::io::Error::other(
                    "writev reported more bytes than the response batch owns",
                ));
            }
            batch.consume(written);
        }
        Ok(())
    }
}

#[cfg(all(target_os = "linux", feature = "embedded", feature = "monoio"))]
unsafe impl monoio::buf::IoVecBuf for FastWriteBatchIoVec {
    fn read_iovec_ptr(&self) -> *const libc::iovec {
        // SAFETY: offset is advanced only through `consume`, which keeps it at
        // or below `iovecs.len()`.
        unsafe { self.iovecs.as_ptr().add(self.offset) }
    }

    fn read_iovec_len(&self) -> usize {
        self.iovecs.len().saturating_sub(self.offset)
    }
}
