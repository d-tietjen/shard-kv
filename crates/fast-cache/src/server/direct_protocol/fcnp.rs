use super::*;

const REQUEST_HEADER_LEN: usize = 8;
const SUPPORTED_FLAGS: u8 = FAST_FLAG_KEY_HASH | FAST_FLAG_ROUTE_SHARD | FAST_FLAG_KEY_TAG;

impl DirectProtocol {
    #[inline(always)]
    pub(in crate::server) fn try_execute_fcnp_fast(
        buf: &[u8],
        store: &EmbeddedStore,
        out: &mut BytesMut,
        mut fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
    ) -> FcnpDispatch {
        match FcnpFrameDecoder::new(buf).decode_for_catalog() {
            FcnpFrameDecode::Ready(decoded) => {
                match FcnpCommandDispatcher::find_opcode(decoded.opcode) {
                    Some(entry) => {
                        FcnpMutationBarrier::from_bool(entry.mutates_value())
                            .materialize(fast_write_queue.as_deref_mut(), out);
                        entry.try_execute_fcnp(FcnpCommandContext {
                            frame: decoded.frame,
                            store,
                            out,
                            fast_write_queue,
                            single_threaded,
                            owned_shard_id,
                        })
                    }
                    None => FcnpDispatch::Unsupported,
                }
            }
            FcnpFrameDecode::Incomplete => FcnpDispatch::Incomplete,
            FcnpFrameDecode::Unsupported => FcnpDispatch::Unsupported,
        }
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn reject_unsupported_owned_fcnp_frame(
        buf: &[u8],
        out: &mut BytesMut,
    ) -> FcnpDispatch {
        match FcnpFrameDecoder::new(buf).decode_any() {
            FcnpFrameDecode::Ready(decoded) => {
                ServerWire::write_fast_error(
                    out,
                    "ERR FCNP direct shard port requires routed GET/SET/DEL",
                );
                FcnpDispatch::Complete(decoded.frame.frame_len)
            }
            FcnpFrameDecode::Incomplete => FcnpDispatch::Incomplete,
            FcnpFrameDecode::Unsupported => FcnpDispatch::Unsupported,
        }
    }
}

enum FcnpMutationBarrier {
    Materialize,
    Skip,
}

impl FcnpMutationBarrier {
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

struct FcnpDecodedFrame<'buf> {
    opcode: u8,
    frame: FcnpFrame<'buf>,
}

enum FcnpFrameDecode<'buf> {
    Ready(FcnpDecodedFrame<'buf>),
    Incomplete,
    Unsupported,
}

struct FcnpFrameDecoder<'buf> {
    buf: &'buf [u8],
}

impl<'buf> FcnpFrameDecoder<'buf> {
    #[inline(always)]
    fn new(buf: &'buf [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    fn decode_for_catalog(&self) -> FcnpFrameDecode<'buf> {
        self.decode(FcnpDecodePolicy::RegisteredCommand)
    }

    #[inline(always)]
    fn decode_any(&self) -> FcnpFrameDecode<'buf> {
        self.decode(FcnpDecodePolicy::AnyFcnpFrame)
    }

    #[inline(always)]
    fn decode(&self, policy: FcnpDecodePolicy) -> FcnpFrameDecode<'buf> {
        match self.header() {
            FcnpHeaderDecode::Ready(header) => match policy.allows(header.flags) {
                true => self.decode_body(header),
                false => FcnpFrameDecode::Unsupported,
            },
            FcnpHeaderDecode::Incomplete => FcnpFrameDecode::Incomplete,
            FcnpHeaderDecode::Unsupported => FcnpFrameDecode::Unsupported,
        }
    }

    #[inline(always)]
    fn header(&self) -> FcnpHeaderDecode {
        match self.buf.len() >= REQUEST_HEADER_LEN {
            false => FcnpHeaderDecode::Incomplete,
            true => match (self.buf[0], self.buf[1]) {
                (FAST_REQUEST_MAGIC, FAST_PROTOCOL_VERSION) => {
                    FcnpHeaderDecode::Ready(FcnpHeader {
                        opcode: self.buf[2],
                        flags: self.buf[3],
                    })
                }
                _ => FcnpHeaderDecode::Unsupported,
            },
        }
    }

    #[inline(always)]
    fn decode_body(&self, header: FcnpHeader) -> FcnpFrameDecode<'buf> {
        // SAFETY: `header()` guarantees bytes 4..8 exist.
        let body_len = unsafe { DirectProtocol::read_le_u32_at(self.buf, 4) } as usize;
        match REQUEST_HEADER_LEN.checked_add(body_len) {
            Some(frame_len) if self.buf.len() >= frame_len => {
                FcnpFrameDecode::Ready(FcnpDecodedFrame {
                    opcode: header.opcode,
                    frame: FcnpFrame {
                        buf: self.buf,
                        body_len,
                        frame_len,
                        flags: header.flags,
                    },
                })
            }
            Some(_) => FcnpFrameDecode::Incomplete,
            None => FcnpFrameDecode::Unsupported,
        }
    }
}

#[derive(Clone, Copy)]
struct FcnpHeader {
    opcode: u8,
    flags: u8,
}

enum FcnpHeaderDecode {
    Ready(FcnpHeader),
    Incomplete,
    Unsupported,
}

enum FcnpDecodePolicy {
    RegisteredCommand,
    AnyFcnpFrame,
}

impl FcnpDecodePolicy {
    #[inline(always)]
    fn allows(&self, flags: u8) -> bool {
        match self {
            Self::RegisteredCommand => {
                flags & !SUPPORTED_FLAGS == 0 && flags & FAST_FLAG_KEY_HASH != 0
            }
            Self::AnyFcnpFrame => true,
        }
    }
}
