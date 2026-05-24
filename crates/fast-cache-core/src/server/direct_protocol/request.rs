use super::*;
use crate::protocol::FastRedisRouteKeys;
use crate::storage::{hash_key, hash_key_tag_from_hash};

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[allow(dead_code)]
    pub(in crate::server) fn process_shared_request_buffer(
        buf: &[u8],
        store: &EmbeddedStore,
        write_buffer: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        owned_shard_id: Option<usize>,
        started_at: Instant,
    ) -> Result<usize> {
        let mut transaction_state = TransactionState::default();
        Self::process_shared_request_buffer_with_context(
            buf,
            store,
            write_buffer,
            fast_write_queue,
            SharedRequestBufferContext {
                single_threaded,
                owned_shard_id,
                started_at,
                transaction_coordinator: None,
                transaction_state: &mut transaction_state,
            },
        )
    }

    #[cfg(feature = "embedded")]
    pub(in crate::server) fn process_shared_request_buffer_with_context(
        buf: &[u8],
        store: &EmbeddedStore,
        write_buffer: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        context: SharedRequestBufferContext<'_>,
    ) -> Result<usize> {
        SharedRequestBufferProcessor {
            buf,
            store,
            write_buffer,
            fast_write_queue,
            single_threaded: context.single_threaded,
            owned_shard_id: context.owned_shard_id,
            started_at: context.started_at,
            transaction_coordinator: context.transaction_coordinator,
            transaction_state: context.transaction_state,
            consumed_total: 0,
        }
        .process()
    }
}

#[cfg(feature = "embedded")]
pub(in crate::server) struct SharedRequestBufferContext<'tx> {
    pub(in crate::server) single_threaded: bool,
    pub(in crate::server) owned_shard_id: Option<usize>,
    pub(in crate::server) started_at: Instant,
    pub(in crate::server) transaction_coordinator: Option<&'tx TransactionCoordinator>,
    pub(in crate::server) transaction_state: &'tx mut TransactionState,
}

#[cfg(feature = "embedded")]
struct SharedRequestBufferProcessor<'buf, 'store, 'out, 'queue, 'tx> {
    buf: &'buf [u8],
    store: &'store EmbeddedStore,
    write_buffer: &'out mut BytesMut,
    fast_write_queue: Option<&'queue mut FastWriteQueue>,
    single_threaded: bool,
    owned_shard_id: Option<usize>,
    started_at: Instant,
    transaction_coordinator: Option<&'tx TransactionCoordinator>,
    transaction_state: &'tx mut TransactionState,
    consumed_total: usize,
}

#[cfg(feature = "embedded")]
enum RequestBufferStep {
    Consumed(usize),
    Pending,
}

#[cfg(feature = "embedded")]
impl<'buf> SharedRequestBufferProcessor<'buf, '_, '_, '_, '_> {
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
        if self.transaction_state.is_active() {
            return self.process_unsupported_fcnp(slice);
        }
        match DirectProtocol::try_execute_fcnp_fast(
            slice,
            self.store,
            self.write_buffer,
            self.fast_write_queue.as_deref_mut(),
            self.single_threaded,
            self.owned_shard_id,
            self.transaction_coordinator,
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
        if let Some(step) = self.process_owned_redis_opcode_fcnp(slice)? {
            return Ok(step);
        }
        if let Some(step) = self.process_owned_decoded_fast_fcnp(slice)? {
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
                    true => {
                        let _transaction_guard = self
                            .transaction_coordinator
                            .map(|coordinator| coordinator.read_guard_for_parts(self.store, parts));
                        DirectProtocol::shared_execute_fast_into(
                            self.store,
                            request,
                            self.write_buffer,
                            self.fast_write_queue.as_deref_mut(),
                            self.single_threaded,
                            self.started_at,
                        )
                    }
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

    fn process_owned_redis_opcode_fcnp(
        &mut self,
        slice: &'buf [u8],
    ) -> Result<Option<RequestBufferStep>> {
        let Some(owned_shard_id) = self.owned_shard_id else {
            return Ok(None);
        };
        let Some((request, consumed)) = FastCodec::decode_request(slice)? else {
            return Ok(Some(RequestBufferStep::Pending));
        };
        let FastCommand::RedisCommand { kind, args } = &request.command else {
            return Ok(None);
        };
        let route_matches = Self::redis_opcode_matches_owned_route(
            self.store,
            *kind,
            args,
            request.key_hash,
            request.route_shard,
            request.key_tag,
            owned_shard_id,
        );
        if !route_matches {
            ServerWire::write_fast_error(self.write_buffer, "ERR FCNP route shard mismatch");
            return Ok(Some(RequestBufferStep::Consumed(consumed)));
        }
        self.materialize_on_mutation(DirectProtocol::fast_command_mutates_value(&request.command));
        let _transaction_guard = self
            .transaction_coordinator
            .map(|coordinator| coordinator.read_guard_for_fast_request(self.store, &request));
        DirectProtocol::shared_execute_fast_into(
            self.store,
            request,
            self.write_buffer,
            self.fast_write_queue.as_deref_mut(),
            self.single_threaded,
            self.started_at,
        );
        Ok(Some(RequestBufferStep::Consumed(consumed)))
    }

    fn redis_opcode_matches_owned_route(
        store: &EmbeddedStore,
        kind: crate::protocol::FastCommandKind,
        args: &[&[u8]],
        request_key_hash: Option<u64>,
        request_route_shard: Option<u32>,
        request_key_tag: Option<u64>,
        owned_shard_id: usize,
    ) -> bool {
        let route_keys = match kind.redis_route_keys(args) {
            FastRedisRouteKeys::None => return true,
            FastRedisRouteKeys::AllShards => return false,
            FastRedisRouteKeys::Keys(keys) if keys.is_empty() => return true,
            FastRedisRouteKeys::Keys(keys) => keys,
        };

        let Some((&first_key, remaining_keys)) = route_keys.split_first() else {
            return true;
        };
        if !Self::redis_opcode_first_key_matches_owned_route(
            store,
            first_key,
            request_key_hash,
            request_route_shard,
            request_key_tag,
            owned_shard_id,
        ) {
            return false;
        }
        remaining_keys
            .iter()
            .all(|key| Self::redis_opcode_key_matches_owned_route(store, key, owned_shard_id))
    }

    fn redis_opcode_first_key_matches_owned_route(
        store: &EmbeddedStore,
        key: &[u8],
        request_key_hash: Option<u64>,
        request_route_shard: Option<u32>,
        request_key_tag: Option<u64>,
        owned_shard_id: usize,
    ) -> bool {
        match (
            store.route_mode(),
            request_key_hash,
            request_route_shard.and_then(|shard| usize::try_from(shard).ok()),
        ) {
            (EmbeddedRouteMode::FullKey, Some(key_hash), Some(route_shard)) => {
                route_shard == owned_shard_id
                    && route_shard < store.shard_count()
                    && crate::storage::stripe_index(
                        key_hash,
                        crate::storage::shift_for(store.shard_count()),
                    ) == owned_shard_id
                    && request_key_tag
                        .is_none_or(|key_tag| key_tag == hash_key_tag_from_hash(key_hash))
                    && hash_key(key) == key_hash
            }
            _ => Self::redis_opcode_key_matches_owned_route(store, key, owned_shard_id),
        }
    }

    fn redis_opcode_key_matches_owned_route(
        store: &EmbeddedStore,
        key: &[u8],
        owned_shard_id: usize,
    ) -> bool {
        owned_shard_id < store.shard_count() && store.route_key(key).shard_id == owned_shard_id
    }

    fn process_owned_decoded_fast_fcnp(
        &mut self,
        slice: &'buf [u8],
    ) -> Result<Option<RequestBufferStep>> {
        let Some(owned_shard_id) = self.owned_shard_id else {
            return Ok(None);
        };
        let Some((request, consumed)) = FastCodec::decode_request(slice)? else {
            return Ok(Some(RequestBufferStep::Pending));
        };
        if matches!(
            request.command,
            FastCommand::RespCommand { .. } | FastCommand::RedisCommand { .. }
        ) {
            return Ok(None);
        }
        let Some(decoded) = DirectProtocol::decoded_fast_redis_command(&request.command) else {
            return Ok(None);
        };
        let args = decoded.args.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let mut parts = Vec::with_capacity(args.len() + 1);
        parts.push(decoded.command);
        parts.extend(args.iter().copied());
        let route_matches = crate::server::transactions::command_shards(self.store, &parts)
            .into_iter()
            .all(|shard_id| shard_id == owned_shard_id);
        if !route_matches {
            ServerWire::write_fast_error(self.write_buffer, "ERR FCNP route shard mismatch");
            return Ok(Some(RequestBufferStep::Consumed(consumed)));
        }
        self.materialize_on_mutation(DirectProtocol::fast_command_mutates_value(&request.command));
        let _transaction_guard = self
            .transaction_coordinator
            .map(|coordinator| coordinator.read_guard_for_fast_request(self.store, &request));
        DirectProtocol::shared_execute_fast_into(
            self.store,
            request,
            self.write_buffer,
            self.fast_write_queue.as_deref_mut(),
            self.single_threaded,
            self.started_at,
        );
        Ok(Some(RequestBufferStep::Consumed(consumed)))
    }

    fn process_generic_fast(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        match FastCodec::decode_request(slice)? {
            Some((request, consumed)) => {
                if self.process_fast_resp_transaction(&request) {
                    return Ok(RequestBufferStep::Consumed(consumed));
                }
                if self.transaction_state.is_active() {
                    self.transaction_state.mark_dirty();
                    ServerWire::write_fast_error(
                        self.write_buffer,
                        "ERR typed FCNP commands are not supported inside MULTI; use RESP command frames",
                    );
                    return Ok(RequestBufferStep::Consumed(consumed));
                }
                self.materialize_on_mutation(DirectProtocol::fast_command_mutates_value(
                    &request.command,
                ));
                let _transaction_guard = self.transaction_coordinator.map(|coordinator| {
                    coordinator.read_guard_for_fast_request(self.store, &request)
                });
                DirectProtocol::shared_execute_fast_into(
                    self.store,
                    request,
                    self.write_buffer,
                    self.fast_write_queue.as_deref_mut(),
                    self.single_threaded,
                    self.started_at,
                );
                Ok(RequestBufferStep::Consumed(consumed))
            }
            None => Ok(RequestBufferStep::Pending),
        }
    }

    fn process_fast_resp_transaction(&mut self, request: &FastRequest<'buf>) -> bool {
        match &request.command {
            FastCommand::RespCommand { parts } => self.process_fast_transaction_parts(parts),
            FastCommand::RedisCommand { kind, args } => {
                let Some(command) = kind.redis_name() else {
                    return false;
                };
                let mut parts = Vec::with_capacity(args.len() + 1);
                parts.push(command.as_bytes());
                parts.extend(args.iter().copied());
                self.process_fast_transaction_parts(&parts)
            }
            _ => false,
        }
    }

    fn process_fast_transaction_parts(&mut self, parts: &[&[u8]]) -> bool {
        let start = ServerWire::begin_fast_value(self.write_buffer);
        if !self.transaction_state.handle_resp_command(
            self.transaction_coordinator,
            self.store,
            parts,
            self.write_buffer,
        ) {
            self.write_buffer.truncate(start);
            return false;
        }

        ServerWire::finish_fast_value(self.write_buffer, start);
        true
    }

    fn process_resp(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        self.process_direct_or_borrowed_resp(slice)
    }

    fn process_direct_or_borrowed_resp(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        let Some((consumed, command_name, args)) = DirectProtocol::try_resp_command_parts(slice)
        else {
            return self.process_borrowed_resp(slice);
        };
        let mut parts = crate::protocol::BorrowedCommandParts::new();
        parts.push(command_name);
        parts.extend(args.iter().copied());

        if self.reject_unsupported_owned_resp_transaction(&parts) {
            return Ok(RequestBufferStep::Consumed(consumed));
        }
        if !self.resp_parts_match_owned_shard(&parts) {
            ServerWire::write_resp_error(self.write_buffer, "ERR direct shard route mismatch");
            return Ok(RequestBufferStep::Consumed(consumed));
        }
        if self.transaction_state.handle_resp_command(
            self.transaction_coordinator,
            self.store,
            &parts,
            self.write_buffer,
        ) {
            return Ok(RequestBufferStep::Consumed(consumed));
        }

        match DirectProtocol::parse_resp_direct_command(command_name, args) {
            Some(command) => {
                self.execute_resp_direct_with_parts(command, &parts);
                Ok(RequestBufferStep::Consumed(consumed))
            }
            None => {
                self.execute_borrowed_parts(parts);
                Ok(RequestBufferStep::Consumed(consumed))
            }
        }
    }

    fn execute_resp_direct_with_parts(
        &mut self,
        command: RespDirectCommand<'buf>,
        parts: &[&[u8]],
    ) {
        if let Some(owned_shard_id) = self.owned_shard_id
            && command.try_execute_owned_shard(self.store, self.write_buffer, owned_shard_id)
        {
            return;
        }
        self.materialize_on_mutation(command.mutates_value());
        let _transaction_guard = self
            .transaction_coordinator
            .map(|coordinator| coordinator.read_guard_for_parts(self.store, parts));
        DirectProtocol::shared_execute_resp_direct_cmd_into(
            self.store,
            command,
            self.write_buffer,
            self.fast_write_queue.as_deref_mut(),
            self.single_threaded,
            self.started_at,
        );
    }

    fn execute_borrowed_parts(&mut self, parts: crate::protocol::BorrowedCommandParts<'buf>) {
        match BorrowedCommand::from_parts(&parts) {
            Ok(command) => self.execute_borrowed_command_with_parts(command, &parts),
            Err(error) => {
                ServerWire::write_resp_error(self.write_buffer, &format!("ERR {error}"));
            }
        }
    }

    fn process_borrowed_resp(&mut self, slice: &'buf [u8]) -> Result<RequestBufferStep> {
        match RespCodec::decode_command(slice)? {
            Some((frame, consumed)) => {
                if self.reject_unsupported_owned_resp_transaction(&frame.parts) {
                    return Ok(RequestBufferStep::Consumed(consumed));
                }
                if !self.resp_parts_match_owned_shard(&frame.parts) {
                    ServerWire::write_resp_error(
                        self.write_buffer,
                        "ERR direct shard route mismatch",
                    );
                    return Ok(RequestBufferStep::Consumed(consumed));
                }
                if self.transaction_state.handle_resp_command(
                    self.transaction_coordinator,
                    self.store,
                    &frame.parts,
                    self.write_buffer,
                ) {
                    return Ok(RequestBufferStep::Consumed(consumed));
                }
                self.execute_borrowed_frame(frame);
                Ok(RequestBufferStep::Consumed(consumed))
            }
            None => Ok(RequestBufferStep::Pending),
        }
    }

    fn execute_borrowed_frame(&mut self, frame: crate::protocol::BorrowedCommandFrame<'buf>) {
        let parts = frame.parts;
        match BorrowedCommand::from_parts(&parts) {
            Ok(command) => self.execute_borrowed_command_with_parts(command, &parts),
            Err(error) => {
                ServerWire::write_resp_error(self.write_buffer, &format!("ERR {error}"));
            }
        }
    }

    fn execute_borrowed_command_with_parts(
        &mut self,
        command: BorrowedCommand<'buf>,
        parts: &[&[u8]],
    ) {
        self.materialize_on_mutation(DirectProtocol::borrowed_command_mutates_value(&command));
        let _transaction_guard = self
            .transaction_coordinator
            .map(|coordinator| coordinator.read_guard_for_parts(self.store, parts));
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

    fn reject_unsupported_owned_resp_transaction(&mut self, parts: &[&[u8]]) -> bool {
        if self.owned_shard_id.is_none() {
            return false;
        }
        let Some(command) = parts.first().copied() else {
            return false;
        };
        if self.transaction_state.is_active() || is_resp_transaction_command(command) {
            ServerWire::write_resp_error(
                self.write_buffer,
                "ERR transactions are not supported on direct shard RESP ports",
            );
            return true;
        }
        false
    }

    fn resp_parts_match_owned_shard(&self, parts: &[&[u8]]) -> bool {
        let Some(owned_shard_id) = self.owned_shard_id else {
            return true;
        };
        crate::server::transactions::command_shards(self.store, parts)
            .into_iter()
            .all(|shard_id| shard_id == owned_shard_id)
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

fn is_resp_transaction_command(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"MULTI")
        || command.eq_ignore_ascii_case(b"EXEC")
        || command.eq_ignore_ascii_case(b"DISCARD")
        || command.eq_ignore_ascii_case(b"WATCH")
        || command.eq_ignore_ascii_case(b"UNWATCH")
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
