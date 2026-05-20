use super::*;

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    pub(in crate::server) fn process_shared_request_buffer(
        buf: &[u8],
        store: &EmbeddedStore,
        write_buffer: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        started_at: Instant,
    ) -> Result<usize> {
        SharedRequestBufferProcessor {
            buf,
            store,
            write_buffer,
            fast_write_queue,
            single_threaded,
            owned_shard_id,
            started_at,
            consumed_total: 0,
        }
        .process()
    }
}

#[cfg(feature = "embedded")]
struct SharedRequestBufferProcessor<'buf, 'store, 'out, 'queue> {
    buf: &'buf [u8],
    store: &'store EmbeddedStore,
    write_buffer: &'out mut BytesMut,
    fast_write_queue: Option<&'queue mut FastWriteQueue>,
    single_threaded: bool,
    owned_shard_id: Option<usize>,
    started_at: Instant,
    consumed_total: usize,
}

#[cfg(feature = "embedded")]
enum RequestBufferStep {
    Consumed(usize),
    Pending,
}

#[cfg(feature = "embedded")]
impl<'buf> SharedRequestBufferProcessor<'buf, '_, '_, '_> {
    fn process(mut self) -> Result<usize> {
        while let RequestBufferStep::Consumed(consumed) = self.process_next()? {
            self.consumed_total += consumed;
        }
        Ok(self.consumed_total)
    }

    fn process_next(&mut self) -> Result<RequestBufferStep> {
        let slice = &self.buf[self.consumed_total..];
        match slice.first().copied() {
            Some(first_byte) if FastCodec::is_fast_request_prefix(first_byte) => {
                self.process_fcnp_or_fast(slice)
            }
            Some(_) => self.process_resp(slice),
            None => Ok(RequestBufferStep::Pending),
        }
    }

    fn process_fcnp_or_fast(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        match DirectProtocol::try_execute_fcnp_fast(
            slice,
            self.store,
            self.write_buffer,
            self.fast_write_queue.as_deref_mut(),
            self.single_threaded,
            self.owned_shard_id,
        ) {
            FcnpDispatch::Complete(consumed) => Ok(RequestBufferStep::Consumed(consumed)),
            FcnpDispatch::Incomplete => Ok(RequestBufferStep::Pending),
            FcnpDispatch::Unsupported => self.process_unsupported_fcnp(slice),
        }
    }

    fn process_unsupported_fcnp(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        match self.owned_shard_id {
            Some(_) => self.reject_unsupported_owned_fcnp(slice),
            None => self.process_generic_fast(slice),
        }
    }

    fn reject_unsupported_owned_fcnp(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        if let Some(step) = self.process_owned_scan_fcnp(slice)? {
            return Ok(step);
        }
        match DirectProtocol::reject_unsupported_owned_fcnp_frame(slice, self.write_buffer) {
            FcnpDispatch::Complete(consumed) => Ok(RequestBufferStep::Consumed(consumed)),
            FcnpDispatch::Incomplete => Ok(RequestBufferStep::Pending),
            FcnpDispatch::Unsupported => Err(crate::FastCacheError::Protocol(
                "direct shard ports only accept routed FCNP frames".into(),
            )),
        }
    }

    fn process_owned_scan_fcnp(&mut self, slice: &'buf [u8]) -> Result<Option<RequestBufferStep>> {
        let Some(owned_shard_id) = self.owned_shard_id else {
            return Ok(None);
        };
        if slice.get(2).copied() != Some(200) {
            return Ok(None);
        }
        let Some((request, consumed)) = FastCodec::decode_request(slice)? else {
            return Ok(Some(RequestBufferStep::Pending));
        };
        match &request.command {
            FastCommand::RespCommand { parts } if is_fcnp_scan_shard(parts) => {
                match fcnp_scan_shard_matches(parts, owned_shard_id) {
                    true => DirectProtocol::shared_execute_fast_into(
                        self.store,
                        request,
                        self.write_buffer,
                        self.single_threaded,
                        self.started_at,
                    ),
                    false => ServerWire::write_fast_error(
                        self.write_buffer,
                        "ERR FCNP scan shard mismatch",
                    ),
                }
                Ok(Some(RequestBufferStep::Consumed(consumed)))
            }
            _ => Ok(None),
        }
    }

    fn process_generic_fast(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        match FastCodec::decode_request(slice)? {
            Some((request, consumed)) => {
                self.materialize_on_mutation(DirectProtocol::fast_command_mutates_value(
                    &request.command,
                ));
                DirectProtocol::shared_execute_fast_into(
                    self.store,
                    request,
                    self.write_buffer,
                    self.single_threaded,
                    self.started_at,
                );
                Ok(RequestBufferStep::Consumed(consumed))
            }
            None => Ok(RequestBufferStep::Pending),
        }
    }

    fn process_resp(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        match self.owned_shard_id {
            Some(_) => Err(crate::FastCacheError::Protocol(
                "direct shard ports only accept routed FCNP frames".into(),
            )),
            None => self.process_direct_or_borrowed_resp(slice),
        }
    }

    fn process_direct_or_borrowed_resp(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        match DirectProtocol::try_resp_direct_dispatch(slice) {
            Some((consumed, command)) => {
                self.execute_resp_direct(command);
                Ok(RequestBufferStep::Consumed(consumed))
            }
            None => self.process_borrowed_resp(slice),
        }
    }

    fn execute_resp_direct(&mut self, command: RespDirectCommandBox<'buf>) {
        self.materialize_on_mutation(command.mutates_value());
        DirectProtocol::shared_execute_resp_direct_cmd_into(
            self.store,
            command,
            self.write_buffer,
            self.fast_write_queue.as_deref_mut(),
            self.single_threaded,
            self.started_at,
        );
    }

    fn process_borrowed_resp(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        match RespCodec::decode_command(slice)? {
            Some((frame, consumed)) => {
                self.execute_borrowed_frame(frame);
                Ok(RequestBufferStep::Consumed(consumed))
            }
            None => Ok(RequestBufferStep::Pending),
        }
    }

    fn execute_borrowed_frame(&mut self, frame: crate::protocol::BorrowedCommandFrame<'buf>) {
        match BorrowedCommand::from_frame(frame) {
            Ok(command) => self.execute_borrowed_command(command),
            Err(error) => {
                ServerWire::write_resp_error(self.write_buffer, &format!("ERR {error}"));
            }
        }
    }

    fn execute_borrowed_command(&mut self, command: BorrowedCommand<'buf>) {
        self.materialize_on_mutation(DirectProtocol::borrowed_command_mutates_value(&command));
        match self.single_threaded {
            true => {
                // SAFETY: caller only enables this when exactly one worker
                // thread can access the store.
                unsafe {
                    DirectProtocol::shared_execute_borrowed_into_single_threaded(
                        self.store,
                        command,
                        self.write_buffer,
                        self.fast_write_queue.as_deref_mut(),
                        self.started_at,
                    );
                }
            }
            false => DirectProtocol::shared_execute_borrowed_into(
                self.store,
                command,
                self.write_buffer,
                self.fast_write_queue.as_deref_mut(),
                self.started_at,
            ),
        }
    }

    fn materialize_on_mutation(&mut self, mutates_value: bool) {
        MutationBarrier::from_bool(mutates_value)
            .materialize(self.fast_write_queue.as_deref_mut(), self.write_buffer);
    }
}

fn is_fcnp_scan_shard(parts: &[&[u8]]) -> bool {
    parts.first().is_some_and(|command| {
        command.eq_ignore_ascii_case(b"FCNP.SCANSHARD")
            || command.eq_ignore_ascii_case(b"FCNP.SCAN.SHARD")
    })
}

fn fcnp_scan_shard_matches(parts: &[&[u8]], owned_shard_id: usize) -> bool {
    parts
        .get(1)
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|raw| raw.parse::<usize>().ok())
        == Some(owned_shard_id)
}

#[cfg(feature = "embedded")]
enum MutationBarrier {
    Materialize,
    Skip,
}

#[cfg(feature = "embedded")]
impl MutationBarrier {
    fn from_bool(mutates_value: bool) -> Self {
        match mutates_value {
            true => Self::Materialize,
            false => Self::Skip,
        }
    }

    fn materialize(self, queue: Option<&mut FastWriteQueue>, out: &mut BytesMut) {
        match self {
            Self::Materialize => FastWriteQueue::materialize_optional(queue, out),
            Self::Skip => {}
        }
    }
}
