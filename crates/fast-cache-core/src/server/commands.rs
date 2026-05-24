use super::direct_protocol::*;
use super::*;

#[cfg(feature = "embedded")]
pub(crate) use super::direct_protocol::FcnpDispatch;
#[cfg(feature = "embedded")]
pub(crate) use super::fast_write::FastWriteQueue;

#[cfg(feature = "embedded")]
pub(crate) struct RawCommandContext<'store, 'args, 'out, 'queue> {
    pub(crate) store: &'store EmbeddedStore,
    pub(crate) args: RespDirectArgs<'args>,
    pub(crate) out: &'out mut BytesMut,
    pub(crate) fast_write_queue: Option<&'queue mut FastWriteQueue>,
    pub(crate) single_threaded: bool,
}

#[cfg(feature = "embedded")]
pub(crate) trait RawDirectCommand: crate::commands::CommandMetadata {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>);

    fn execute_fast(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        let RawCommandContext {
            store, args, out, ..
        } = ctx;
        let start = crate::server::wire::ServerWire::begin_fast_value(out);
        self.execute(RawCommandContext {
            store,
            args,
            out,
            fast_write_queue: None,
            single_threaded: false,
        });
        crate::server::wire::ServerWire::finish_fast_value(out, start);
    }

    fn execute_owned_shard(
        &self,
        store: &EmbeddedStore,
        args: &[&[u8]],
        out: &mut BytesMut,
        owned_shard_id: usize,
    ) -> bool {
        let _ = (store, args, out, owned_shard_id);
        false
    }
}

#[cfg(feature = "embedded")]
pub(crate) struct BorrowedCommandContext<'store, 'out, 'queue> {
    pub(crate) store: &'store EmbeddedStore,
    pub(crate) out: &'out mut BytesMut,
    pub(crate) fast_write_queue: Option<&'queue mut FastWriteQueue>,
    pub(crate) single_threaded: bool,
}

#[cfg(feature = "embedded")]
pub(crate) struct FastCommandContext<'store, 'out> {
    pub(crate) store: &'store EmbeddedStore,
    pub(crate) key_hash: Option<u64>,
    pub(crate) out: &'out mut BytesMut,
    pub(crate) single_threaded: bool,
}

#[cfg(feature = "embedded")]
pub(crate) trait FastDirectCommand: crate::commands::DecodedFastCommand {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>);
}

pub(crate) struct DirectCommandContext {
    pub(crate) now_ms: u64,
}

impl DirectCommandContext {
    #[inline(always)]
    pub(crate) fn new(now_ms: u64) -> Self {
        Self { now_ms }
    }
}

pub(crate) trait DirectFastCommand: crate::commands::DecodedFastCommand {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse;
}

#[cfg(feature = "embedded")]
#[derive(Clone, Copy)]
pub(crate) struct FcnpFrame<'buf> {
    pub(crate) buf: &'buf [u8],
    pub(crate) body_len: usize,
    pub(crate) frame_len: usize,
    pub(crate) flags: u8,
}

#[cfg(feature = "embedded")]
#[derive(Clone, Copy)]
pub(crate) struct FcnpKeyPrefix {
    pub(crate) key_hash: u64,
    pub(crate) key_tag: Option<u64>,
    pub(crate) cursor: usize,
}

#[cfg(feature = "embedded")]
impl FcnpFrame<'_> {
    pub(crate) const REQUEST_HEADER_LEN: usize = 8;

    #[inline(always)]
    pub(crate) fn body_end(self) -> usize {
        self.frame_len
    }

    #[inline(always)]
    pub(crate) unsafe fn read_u32_at(self, offset: usize) -> u32 {
        debug_assert!(self.buf.len() >= offset + 4);
        #[cfg(not(feature = "unsafe"))]
        {
            u32::from_le_bytes(
                self.buf[offset..offset + 4]
                    .try_into()
                    .expect("validated u32 read has four bytes"),
            )
        }
        #[cfg(feature = "unsafe")]
        {
            // SAFETY: callers check that at least four bytes are available.
            u32::from_le(unsafe {
                std::ptr::read_unaligned(self.buf.as_ptr().add(offset).cast::<u32>())
            })
        }
    }

    #[inline(always)]
    pub(crate) unsafe fn read_u64_at(self, offset: usize) -> u64 {
        debug_assert!(self.buf.len() >= offset + 8);
        #[cfg(not(feature = "unsafe"))]
        {
            u64::from_le_bytes(
                self.buf[offset..offset + 8]
                    .try_into()
                    .expect("validated u64 read has eight bytes"),
            )
        }
        #[cfg(feature = "unsafe")]
        {
            // SAFETY: callers check that at least eight bytes are available.
            u64::from_le(unsafe {
                std::ptr::read_unaligned(self.buf.as_ptr().add(offset).cast::<u64>())
            })
        }
    }

    #[inline(always)]
    pub(crate) fn read_key_prefix(self) -> Option<FcnpKeyPrefix> {
        let mut cursor = FcnpFrameCursor::new(self);
        let key_hash = cursor.read_u64()?;
        cursor.skip_optional_route_shard()?;
        let key_tag = cursor.read_optional_key_tag()?;
        Some(FcnpKeyPrefix {
            key_hash,
            key_tag,
            cursor: cursor.position(),
        })
    }
}

#[cfg(feature = "embedded")]
struct FcnpFrameCursor<'buf> {
    frame: FcnpFrame<'buf>,
    cursor: usize,
}

#[cfg(feature = "embedded")]
impl<'buf> FcnpFrameCursor<'buf> {
    #[inline(always)]
    fn new(frame: FcnpFrame<'buf>) -> Self {
        Self {
            frame,
            cursor: FcnpFrame::REQUEST_HEADER_LEN,
        }
    }

    #[inline(always)]
    fn position(&self) -> usize {
        self.cursor
    }

    #[inline(always)]
    fn read_u64(&mut self) -> Option<u64> {
        match self.has_remaining(8) {
            true => {
                // SAFETY: `has_remaining(8)` guarantees eight bytes at `cursor`.
                let value = unsafe { self.frame.read_u64_at(self.cursor) };
                self.cursor += 8;
                Some(value)
            }
            false => None,
        }
    }

    #[inline(always)]
    fn skip_u32(&mut self) -> Option<()> {
        match self.has_remaining(4) {
            true => {
                self.cursor += 4;
                Some(())
            }
            false => None,
        }
    }

    #[inline(always)]
    fn skip_optional_route_shard(&mut self) -> Option<()> {
        match self.frame.flags & FAST_FLAG_ROUTE_SHARD != 0 {
            true => self.skip_u32(),
            false => Some(()),
        }
    }

    #[inline(always)]
    fn read_optional_key_tag(&mut self) -> Option<Option<u64>> {
        match self.frame.flags & FAST_FLAG_KEY_TAG != 0 {
            true => self.read_u64().map(Some),
            false => Some(None),
        }
    }

    #[inline(always)]
    fn has_remaining(&self, len: usize) -> bool {
        self.frame.body_end().saturating_sub(self.cursor) >= len
    }
}

#[cfg(feature = "embedded")]
pub(crate) struct FcnpCommandContext<'buf, 'store, 'out, 'queue> {
    pub(crate) frame: FcnpFrame<'buf>,
    pub(crate) store: &'store EmbeddedStore,
    pub(crate) out: &'out mut BytesMut,
    pub(crate) fast_write_queue: Option<&'queue mut FastWriteQueue>,
    pub(crate) single_threaded: bool,
    pub(crate) owned_shard_id: Option<usize>,
}

#[cfg(feature = "embedded")]
impl FcnpCommandContext<'_, '_, '_, '_> {
    #[inline(always)]
    pub(crate) fn request_matches_owned_shard(&self, route_shard: usize, key_hash: u64) -> bool {
        let shard_count = self.store.shard_count();
        self.owned_shard_id.is_some_and(|owned_shard_id| {
            route_shard == owned_shard_id
                && owned_shard_id < shard_count
                && crate::storage::stripe_index(key_hash, crate::storage::shift_for(shard_count))
                    == owned_shard_id
        })
    }

    #[inline(always)]
    pub(crate) fn request_matches_owned_shard_for_key(
        &self,
        route_shard: usize,
        key_hash: u64,
        key: &[u8],
    ) -> bool {
        match self.store.route_mode() {
            EmbeddedRouteMode::FullKey => self.request_matches_owned_shard(route_shard, key_hash),
            EmbeddedRouteMode::SessionPrefix => self.owned_shard_id.is_some_and(|owned_shard_id| {
                let route = self.store.route_key(key);
                route_shard == owned_shard_id
                    && owned_shard_id < self.store.shard_count()
                    && route.shard_id == owned_shard_id
                    && route.key_hash == key_hash
            }),
        }
    }

    #[inline(always)]
    pub(crate) fn fcnp_route_matches_owned_shard_for_key(
        &self,
        route_shard: Option<impl TryInto<usize>>,
        key_hash: u64,
        key: &[u8],
    ) -> bool {
        let route_shard = route_shard.and_then(|route_shard| route_shard.try_into().ok());
        match self.owned_shard_id {
            None => true,
            Some(owned_shard_id) => matches!(
                route_shard,
                Some(route_shard)
                    if route_shard == owned_shard_id
                        && self.request_matches_owned_shard_for_key(route_shard, key_hash, key)
            ),
        }
    }

    #[inline(always)]
    pub(crate) fn request_matches_owned_session_hash(
        &self,
        route_shard: usize,
        session_hash: u64,
    ) -> bool {
        let shard_count = self.store.shard_count();
        self.owned_shard_id.is_some_and(|owned_shard_id| {
            route_shard == owned_shard_id
                && owned_shard_id < shard_count
                && crate::storage::stripe_index(
                    session_hash,
                    crate::storage::shift_for(shard_count),
                ) == owned_shard_id
        })
    }
}

#[cfg(feature = "embedded")]
pub(crate) trait FcnpDirectCommand: crate::commands::CommandMetadata {
    fn opcode(&self) -> u8;

    fn try_execute_fcnp(&self, ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch;
}

#[cfg(feature = "embedded")]
pub(super) struct RawCommandDispatcher;

#[cfg(feature = "embedded")]
pub(super) struct FastCommandDispatcher;

pub(super) struct DirectFastCommandDispatcher;

#[cfg(feature = "embedded")]
static RAW_DIRECT_CATALOG: &[&dyn RawDirectCommand] = &[
    &crate::commands::get::COMMAND,
    &crate::commands::set::COMMAND,
    &crate::commands::del::COMMAND,
    &crate::commands::exists::COMMAND,
    &crate::commands::ttl::COMMAND,
    &crate::commands::pttl::COMMAND,
    &crate::commands::expire::COMMAND,
    &crate::commands::pexpire::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::expireat::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::pexpireat::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::expiretime::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::pexpiretime::COMMAND,
    &crate::commands::persist::COMMAND,
    &crate::commands::getex::COMMAND,
    &crate::commands::setex::COMMAND,
    &crate::commands::psetex::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::command::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::keys::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::scan::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::object::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::touch::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::randomkey::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::flush::FLUSHDB_COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::flush::FLUSHALL_COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::memory::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::copy::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::dump::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::restore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::rename::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::renamenx::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::unlink::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::strlen::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::getrange::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::getbit::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::setbit::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::bitcount::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::bitpos::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::bitop::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::bitfield::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::setnx::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::mget::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::hstrlen::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::hget::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::hlen::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::hmset::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::hmget::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::hkeys::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::hvals::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::hgetall::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::hscan::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::lrange::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::llen::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::lindex::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::rpoplpush::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::lmove::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::blmove::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::lmpop::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::blmpop::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::scard::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::smembers::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::sscan::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::srandmember::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::smismember::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zunion::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zinter::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zdiff::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zintercard::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zrandmember::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zrange::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zrevrange::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zscore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zcount::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zrevrangebyscore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zrangebylex::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zrevrangebylex::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zlexcount::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zremrangebyrank::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zremrangebyscore::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zremrangebylex::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zrank::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zrevrank::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zscan::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::zmpop::COMMAND,
    #[cfg(feature = "redis-compat")]
    &crate::commands::bzmpop::COMMAND,
];

#[cfg(feature = "embedded")]
static FAST_DIRECT_CATALOG: &[&dyn FastDirectCommand] = &[
    &crate::commands::get::COMMAND,
    &crate::commands::set::COMMAND,
    &crate::commands::del::COMMAND,
    &crate::commands::exists::COMMAND,
    &crate::commands::ttl::COMMAND,
    &crate::commands::expire::COMMAND,
    &crate::commands::getex::COMMAND,
    &crate::commands::setex::COMMAND,
];

static DIRECT_FAST_CATALOG: &[&dyn DirectFastCommand] = &[
    &crate::commands::get::COMMAND,
    &crate::commands::set::COMMAND,
    &crate::commands::del::COMMAND,
    &crate::commands::exists::COMMAND,
    &crate::commands::ttl::COMMAND,
    &crate::commands::expire::COMMAND,
    &crate::commands::getex::COMMAND,
    &crate::commands::setex::COMMAND,
];

#[cfg(feature = "embedded")]
pub(super) struct FcnpCommandDispatcher;

#[cfg(feature = "embedded")]
#[derive(Clone, Copy)]
pub(super) struct FcnpCommandEntry {
    command: &'static dyn FcnpDirectCommand,
    mutates_value: bool,
}

#[cfg(feature = "embedded")]
impl FcnpCommandEntry {
    #[inline(always)]
    pub(super) fn mutates_value(self) -> bool {
        self.mutates_value
    }

    #[inline(always)]
    pub(super) fn try_execute_fcnp(self, ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        self.command.try_execute_fcnp(ctx)
    }
}

#[cfg(feature = "embedded")]
static FCNP_DIRECT_BY_OPCODE: [Option<FcnpCommandEntry>; 9] = [
    None,
    Some(FcnpCommandEntry {
        command: &crate::commands::get::COMMAND,
        mutates_value: <crate::commands::get::Get as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(FcnpCommandEntry {
        command: &crate::commands::set::COMMAND,
        mutates_value: <crate::commands::set::Set as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(FcnpCommandEntry {
        command: &crate::commands::setex::COMMAND,
        mutates_value:
            <crate::commands::setex::SetEx as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(FcnpCommandEntry {
        command: &crate::commands::getex::COMMAND,
        mutates_value:
            <crate::commands::getex::GetEx as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(FcnpCommandEntry {
        command: &crate::commands::del::COMMAND,
        mutates_value: <crate::commands::del::Del as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(FcnpCommandEntry {
        command: &crate::commands::exists::COMMAND,
        mutates_value:
            <crate::commands::exists::Exists as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(FcnpCommandEntry {
        command: &crate::commands::ttl::COMMAND,
        mutates_value: <crate::commands::ttl::Ttl as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(FcnpCommandEntry {
        command: &crate::commands::expire::COMMAND,
        mutates_value:
            <crate::commands::expire::Expire as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
];

#[cfg(feature = "embedded")]
impl FcnpCommandDispatcher {
    #[inline(always)]
    pub(super) fn find_opcode(opcode: u8) -> Option<FcnpCommandEntry> {
        let entry = FCNP_DIRECT_BY_OPCODE
            .get(opcode as usize)
            .copied()
            .flatten();
        debug_assert!(entry.is_none_or(|entry| entry.command.opcode() == opcode));
        entry
    }
}

impl DirectFastCommandDispatcher {
    fn find(command: &FastCommand<'_>) -> Option<&'static dyn DirectFastCommand> {
        DIRECT_FAST_CATALOG
            .iter()
            .copied()
            .find(|candidate| candidate.matches_decoded_fast(command))
    }

    pub(super) fn execute(
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> Option<FastResponse> {
        Self::find(&request.command).map(|handler| handler.execute_direct_fast(ctx, request))
    }
}

#[cfg(feature = "embedded")]
impl FastCommandDispatcher {
    fn find(command: &FastCommand<'_>) -> Option<&'static dyn FastDirectCommand> {
        FAST_DIRECT_CATALOG
            .iter()
            .copied()
            .find(|candidate| candidate.matches_decoded_fast(command))
    }

    pub(super) fn mutates_value(command: &FastCommand<'_>) -> bool {
        Self::find(command).is_some_and(|command| command.mutates_value())
    }

    pub(super) fn execute(
        store: &EmbeddedStore,
        request: FastRequest<'_>,
        out: &mut BytesMut,
        single_threaded: bool,
    ) -> bool {
        let Some(command) = Self::find(&request.command) else {
            return false;
        };
        command.execute_fast(
            FastCommandContext {
                store,
                key_hash: request.key_hash,
                out,
                single_threaded,
            },
            request.command,
        );
        true
    }
}

#[cfg(feature = "embedded")]
impl RawCommandDispatcher {
    pub(super) fn find(command: &[u8]) -> Option<&'static dyn RawDirectCommand> {
        RAW_DIRECT_CATALOG
            .iter()
            .copied()
            .find(|candidate| candidate.matches(command))
    }

    #[inline(always)]
    pub(super) fn find_redis_opcode(
        kind: crate::protocol::FastCommandKind,
    ) -> Option<&'static dyn RawDirectCommand> {
        match kind {
            crate::protocol::FastCommandKind::Get => Some(&crate::commands::get::COMMAND),
            crate::protocol::FastCommandKind::Set => Some(&crate::commands::set::COMMAND),
            crate::protocol::FastCommandKind::Delete => Some(&crate::commands::del::COMMAND),
            crate::protocol::FastCommandKind::Exists => Some(&crate::commands::exists::COMMAND),
            crate::protocol::FastCommandKind::Ttl => Some(&crate::commands::ttl::COMMAND),
            crate::protocol::FastCommandKind::PTtl => Some(&crate::commands::pttl::COMMAND),
            crate::protocol::FastCommandKind::Expire => Some(&crate::commands::expire::COMMAND),
            crate::protocol::FastCommandKind::PExpire => Some(&crate::commands::pexpire::COMMAND),
            crate::protocol::FastCommandKind::Persist => Some(&crate::commands::persist::COMMAND),
            crate::protocol::FastCommandKind::GetEx => Some(&crate::commands::getex::COMMAND),
            crate::protocol::FastCommandKind::SetEx => Some(&crate::commands::setex::COMMAND),
            crate::protocol::FastCommandKind::PSetEx => Some(&crate::commands::psetex::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ExpireAt => Some(&crate::commands::expireat::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::PExpireAt => {
                Some(&crate::commands::pexpireat::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ExpireTime => {
                Some(&crate::commands::expiretime::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::PExpireTime => {
                Some(&crate::commands::pexpiretime::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Command => Some(&crate::commands::command::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Keys => Some(&crate::commands::keys::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Scan => Some(&crate::commands::scan::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Object => Some(&crate::commands::object::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Touch => Some(&crate::commands::touch::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::RandomKey => {
                Some(&crate::commands::randomkey::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::FlushDb => {
                Some(&crate::commands::flush::FLUSHDB_COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::FlushAll => {
                Some(&crate::commands::flush::FLUSHALL_COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Memory => Some(&crate::commands::memory::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Copy => Some(&crate::commands::copy::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Dump => Some(&crate::commands::dump::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Restore => Some(&crate::commands::restore::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Rename => Some(&crate::commands::rename::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::RenameNx => Some(&crate::commands::renamenx::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::Unlink => Some(&crate::commands::unlink::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::StrLen => Some(&crate::commands::strlen::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::GetRange => Some(&crate::commands::getrange::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::GetBit => Some(&crate::commands::getbit::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::SetBit => Some(&crate::commands::setbit::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::BitCount => Some(&crate::commands::bitcount::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::BitPos => Some(&crate::commands::bitpos::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::BitOp => Some(&crate::commands::bitop::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::BitField => Some(&crate::commands::bitfield::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::SetNx => Some(&crate::commands::setnx::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::MGet => Some(&crate::commands::mget::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::HStrLen => Some(&crate::commands::hstrlen::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::HGet => Some(&crate::commands::hget::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::HLen => Some(&crate::commands::hlen::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::HMSet => Some(&crate::commands::hmset::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::HMGet => Some(&crate::commands::hmget::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::HKeys => Some(&crate::commands::hkeys::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::HVals => Some(&crate::commands::hvals::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::HGetAll => Some(&crate::commands::hgetall::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::HScan => Some(&crate::commands::hscan::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::LRange => Some(&crate::commands::lrange::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::LLen => Some(&crate::commands::llen::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::LIndex => Some(&crate::commands::lindex::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::RPopLPush => {
                Some(&crate::commands::rpoplpush::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::LMove => Some(&crate::commands::lmove::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::BLMove => Some(&crate::commands::blmove::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::LMPop => Some(&crate::commands::lmpop::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::BLMPop => Some(&crate::commands::blmpop::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::SCard => Some(&crate::commands::scard::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::SMembers => Some(&crate::commands::smembers::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::SScan => Some(&crate::commands::sscan::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::SRandMember => {
                Some(&crate::commands::srandmember::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::SMIsMember => {
                Some(&crate::commands::smismember::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZUnion => Some(&crate::commands::zunion::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZInter => Some(&crate::commands::zinter::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZDiff => Some(&crate::commands::zdiff::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZInterCard => {
                Some(&crate::commands::zintercard::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRandMember => {
                Some(&crate::commands::zrandmember::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRange => Some(&crate::commands::zrange::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRevRange => {
                Some(&crate::commands::zrevrange::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZScore => Some(&crate::commands::zscore::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZCount => Some(&crate::commands::zcount::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRevRangeByScore => {
                Some(&crate::commands::zrevrangebyscore::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRangeByLex => {
                Some(&crate::commands::zrangebylex::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRevRangeByLex => {
                Some(&crate::commands::zrevrangebylex::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZLexCount => {
                Some(&crate::commands::zlexcount::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRemRangeByRank => {
                Some(&crate::commands::zremrangebyrank::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRemRangeByScore => {
                Some(&crate::commands::zremrangebyscore::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRemRangeByLex => {
                Some(&crate::commands::zremrangebylex::COMMAND)
            }
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRank => Some(&crate::commands::zrank::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZRevRank => Some(&crate::commands::zrevrank::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZScan => Some(&crate::commands::zscan::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::ZMPop => Some(&crate::commands::zmpop::COMMAND),
            #[cfg(feature = "redis-compat")]
            crate::protocol::FastCommandKind::BZMPop => Some(&crate::commands::bzmpop::COMMAND),
            _ => None,
        }
    }

    pub(super) fn mutates_value(command: &[u8]) -> bool {
        Self::find(command).is_some_and(|command| command.mutates_value())
    }
}
