use super::*;

const REQUEST_HEADER_LEN: usize = 8;
const SUPPORTED_FLAGS: u8 = FAST_FLAG_KEY_HASH | FAST_FLAG_ROUTE_SHARD | FAST_FLAG_KEY_TAG;

impl DirectProtocol {
    #[inline(always)]
    pub(in crate::server) fn try_execute_scnp_fast(
        buf: &[u8],
        store: &EmbeddedStore,
        out: &mut BytesMut,
        mut fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        transaction_coordinator: Option<&TransactionCoordinator>,
    ) -> ScnpDispatch {
        match ScnpFrameDecoder::new(buf).decode_for_catalog() {
            ScnpFrameDecode::Ready(decoded) => {
                match ScnpCommandDispatcher::find_opcode(decoded.opcode) {
                    Some(entry) => {
                        let _transaction_guard = transaction_coordinator.and_then(|coordinator| {
                            decoded.frame.read_key_prefix().map(|prefix| {
                                coordinator.read_guard_for_scnp_key_hash(store, prefix.key_hash)
                            })
                        });
                        ScnpMutationBarrier::from_bool(entry.mutates_value())
                            .materialize(fast_write_queue.as_deref_mut(), out);
                        entry.try_execute_scnp(ScnpCommandContext {
                            frame: decoded.frame,
                            store,
                            out,
                            fast_write_queue,
                            single_threaded,
                            owned_shard_id,
                        })
                    }
                    None => ScnpDispatch::Unsupported,
                }
            }
            ScnpFrameDecode::Incomplete => ScnpDispatch::Incomplete,
            ScnpFrameDecode::Unsupported => ScnpDispatch::Unsupported,
        }
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn reject_unsupported_owned_scnp_frame(
        buf: &[u8],
        out: &mut BytesMut,
    ) -> ScnpDispatch {
        match ScnpFrameDecoder::new(buf).decode_any() {
            ScnpFrameDecode::Ready(decoded) => {
                ServerWire::write_fast_error(
                    out,
                    "ERR SCNP direct shard port requires a routed shard-local command",
                );
                ScnpDispatch::Complete(decoded.frame.frame_len)
            }
            ScnpFrameDecode::Incomplete => ScnpDispatch::Incomplete,
            ScnpFrameDecode::Unsupported => ScnpDispatch::Unsupported,
        }
    }
}

enum ScnpMutationBarrier {
    Materialize,
    Skip,
}

impl ScnpMutationBarrier {
    #[inline(always)]
    fn from_bool(mutates_value: bool) -> Self {
        match mutates_value {
            true => Self::Materialize,
            false => Self::Skip,
        }
    }

    #[inline(always)]
    fn materialize(self, queue: Option<&mut FastWriteQueue>, out: &mut BytesMut) {
        match self {
            Self::Materialize => FastWriteQueue::materialize_optional(queue, out),
            Self::Skip => {}
        }
    }
}

struct ScnpDecodedFrame<'buf> {
    opcode: u8,
    frame: ScnpFrame<'buf>,
}

enum ScnpFrameDecode<'buf> {
    Ready(ScnpDecodedFrame<'buf>),
    Incomplete,
    Unsupported,
}

struct ScnpFrameDecoder<'buf> {
    buf: &'buf [u8],
}

impl<'buf> ScnpFrameDecoder<'buf> {
    #[inline(always)]
    fn new(buf: &'buf [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    fn decode_for_catalog(&self) -> ScnpFrameDecode<'buf> {
        self.decode(ScnpDecodePolicy::RegisteredCommand)
    }

    #[inline(always)]
    fn decode_any(&self) -> ScnpFrameDecode<'buf> {
        self.decode(ScnpDecodePolicy::AnyScnpFrame)
    }

    #[inline(always)]
    fn decode(&self, policy: ScnpDecodePolicy) -> ScnpFrameDecode<'buf> {
        match self.header() {
            ScnpHeaderDecode::Ready(header) => match policy.allows(header.flags) {
                true => self.decode_body(header),
                false => ScnpFrameDecode::Unsupported,
            },
            ScnpHeaderDecode::Incomplete => ScnpFrameDecode::Incomplete,
            ScnpHeaderDecode::Unsupported => ScnpFrameDecode::Unsupported,
        }
    }

    #[inline(always)]
    fn header(&self) -> ScnpHeaderDecode {
        match self.buf.len() >= REQUEST_HEADER_LEN {
            false => ScnpHeaderDecode::Incomplete,
            true => match (self.buf[0], self.buf[1]) {
                (FAST_REQUEST_MAGIC, FAST_PROTOCOL_VERSION) => {
                    ScnpHeaderDecode::Ready(ScnpHeader {
                        opcode: self.buf[2],
                        flags: self.buf[3],
                    })
                }
                _ => ScnpHeaderDecode::Unsupported,
            },
        }
    }

    #[inline(always)]
    fn decode_body(&self, header: ScnpHeader) -> ScnpFrameDecode<'buf> {
        // SAFETY: `header()` guarantees bytes 4..8 exist.
        let body_len = unsafe { DirectProtocol::read_le_u32_at(self.buf, 4) } as usize;
        match REQUEST_HEADER_LEN.checked_add(body_len) {
            Some(frame_len) if self.buf.len() >= frame_len => {
                ScnpFrameDecode::Ready(ScnpDecodedFrame {
                    opcode: header.opcode,
                    frame: ScnpFrame {
                        buf: &self.buf[..frame_len],
                        body_len,
                        frame_len,
                        flags: header.flags,
                    },
                })
            }
            Some(_) => ScnpFrameDecode::Incomplete,
            None => ScnpFrameDecode::Unsupported,
        }
    }
}

#[derive(Clone, Copy)]
struct ScnpHeader {
    opcode: u8,
    flags: u8,
}

enum ScnpHeaderDecode {
    Ready(ScnpHeader),
    Incomplete,
    Unsupported,
}

enum ScnpDecodePolicy {
    RegisteredCommand,
    AnyScnpFrame,
}

impl ScnpDecodePolicy {
    #[inline(always)]
    fn allows(&self, flags: u8) -> bool {
        match self {
            Self::RegisteredCommand => {
                flags & !SUPPORTED_FLAGS == 0 && flags & FAST_FLAG_KEY_HASH != 0
            }
            Self::AnyScnpFrame => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_body_larger_than_received_bytes_stays_incomplete() {
        let mut frame = vec![
            FAST_REQUEST_MAGIC,
            FAST_PROTOCOL_VERSION,
            1,
            FAST_FLAG_KEY_HASH,
            64,
            0,
            0,
            0,
        ];
        frame.extend_from_slice(b"attacker-controlled");

        assert!(matches!(
            ScnpFrameDecoder::new(&frame).decode_for_catalog(),
            ScnpFrameDecode::Incomplete
        ));
    }

    #[test]
    fn decoder_never_includes_bytes_beyond_declared_frame() {
        let mut frame = vec![
            FAST_REQUEST_MAGIC,
            FAST_PROTOCOL_VERSION,
            1,
            FAST_FLAG_KEY_HASH,
            8,
            0,
            0,
            0,
        ];
        frame.extend_from_slice(&42u64.to_le_bytes());
        frame.extend_from_slice(b"adjacent-secret");

        let ScnpFrameDecode::Ready(decoded) = ScnpFrameDecoder::new(&frame).decode_for_catalog()
        else {
            panic!("complete frame was not decoded");
        };
        assert_eq!(decoded.frame.frame_len, REQUEST_HEADER_LEN + 8);
        assert_eq!(decoded.frame.body_end(), REQUEST_HEADER_LEN + 8);
        assert_eq!(decoded.frame.buf.len(), REQUEST_HEADER_LEN + 8);
        assert!(!decoded.frame.buf.ends_with(b"adjacent-secret"));
    }
}
