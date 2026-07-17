use super::direct_protocol::*;
use super::wire::RespProtocolVersion;
use super::*;

#[cfg(feature = "embedded")]
pub(crate) use super::direct_protocol::ScnpDispatch;
#[cfg(feature = "embedded")]
pub(crate) use super::fast_write::FastWriteQueue;

#[cfg(feature = "embedded")]
pub(crate) struct RawCommandContext<'store, 'args, 'out, 'queue> {
    pub(crate) store: &'store EmbeddedStore,
    pub(crate) args: RespDirectArgs<'args>,
    pub(crate) out: &'out mut BytesMut,
    pub(crate) fast_write_queue: Option<&'queue mut FastWriteQueue>,
    pub(crate) single_threaded: bool,
    pub(crate) resp_protocol: RespProtocolVersion,
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
            resp_protocol: RespProtocolVersion::Resp2,
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
    pub(crate) resp_protocol: RespProtocolVersion,
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
pub(crate) struct ScnpFrame<'buf> {
    pub(crate) buf: &'buf [u8],
    pub(crate) body_len: usize,
    pub(crate) frame_len: usize,
    pub(crate) flags: u8,
}

#[cfg(feature = "embedded")]
#[derive(Clone, Copy)]
pub(crate) struct ScnpKeyPrefix {
    pub(crate) key_hash: u64,
    pub(crate) key_tag: Option<u64>,
    pub(crate) cursor: usize,
}

#[cfg(feature = "embedded")]
impl ScnpFrame<'_> {
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
    pub(crate) fn read_key_prefix(self) -> Option<ScnpKeyPrefix> {
        let mut cursor = ScnpFrameCursor::new(self);
        let key_hash = cursor.read_u64()?;
        cursor.skip_optional_route_shard()?;
        let key_tag = cursor.read_optional_key_tag()?;
        Some(ScnpKeyPrefix {
            key_hash,
            key_tag,
            cursor: cursor.position(),
        })
    }
}

#[cfg(feature = "embedded")]
struct ScnpFrameCursor<'buf> {
    frame: ScnpFrame<'buf>,
    cursor: usize,
}

#[cfg(feature = "embedded")]
impl<'buf> ScnpFrameCursor<'buf> {
    #[inline(always)]
    fn new(frame: ScnpFrame<'buf>) -> Self {
        Self {
            frame,
            cursor: ScnpFrame::REQUEST_HEADER_LEN,
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
pub(crate) struct ScnpCommandContext<'buf, 'store, 'out, 'queue> {
    pub(crate) frame: ScnpFrame<'buf>,
    pub(crate) store: &'store EmbeddedStore,
    pub(crate) out: &'out mut BytesMut,
    pub(crate) fast_write_queue: Option<&'queue mut FastWriteQueue>,
    pub(crate) single_threaded: bool,
    pub(crate) owned_shard_id: Option<usize>,
}

#[cfg(feature = "embedded")]
impl ScnpCommandContext<'_, '_, '_, '_> {
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
            EmbeddedRouteMode::OverflowSlot => self.owned_shard_id.is_some_and(|owned_shard_id| {
                let route = self.store.route_key(key);
                route_shard == owned_shard_id
                    && route.shard_id == owned_shard_id
                    && route.key_hash == key_hash
                    && crate::storage::overflow_slot_shard(
                        key,
                        crate::storage::shift_for(self.store.shard_count()),
                    ) == Some(owned_shard_id)
            }),
        }
    }

    #[inline(always)]
    pub(crate) fn scnp_route_matches_owned_shard_for_key(
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
pub(crate) trait ScnpDirectCommand: crate::commands::CommandMetadata {
    fn opcode(&self) -> u8;

    fn try_execute_scnp(&self, ctx: ScnpCommandContext<'_, '_, '_, '_>) -> ScnpDispatch;
}

#[cfg(feature = "embedded")]
pub(super) struct RawCommandDispatcher;

#[cfg(feature = "embedded")]
pub(super) struct FastCommandDispatcher;

pub(super) struct DirectFastCommandDispatcher;

#[cfg(feature = "embedded")]
pub(super) static RAW_DIRECT_CATALOG: &[&dyn RawDirectCommand] = &[
    &crate::commands::get::COMMAND,
    &crate::commands::set::COMMAND,
    &crate::commands::del::COMMAND,
    &crate::commands::exists::COMMAND,
    &crate::commands::ttl::COMMAND,
    &crate::commands::pttl::COMMAND,
    &crate::commands::expire::COMMAND,
    &crate::commands::pexpire::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::expireat::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pexpireat::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::expiretime::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pexpiretime::COMMAND,
    &crate::commands::persist::COMMAND,
    &crate::commands::getex::COMMAND,
    &crate::commands::setex::COMMAND,
    &crate::commands::psetex::COMMAND,
    &crate::commands::semantic::SEMANTIC_SET_COMMAND,
    &crate::commands::semantic::SEMANTIC_SEARCH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::ASKING_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::BGREWRITEAOF_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::BGSAVE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::CLUSTER_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::DEBUG_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::FAILOVER_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::acl::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::reset::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lpos::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lcs::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stralgo::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XAUTOCLAIM_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::HOST_WARNING_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::LASTSAVE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::LATENCY_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::LOLWUT_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::MIGRATE_COMMAND,
    #[cfg(feature = "redis-modules")]
    &crate::commands::admin::MODULE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::MONITOR_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::MOVE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::POST_WARNING_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::PSYNC_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::READONLY_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::READWRITE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::REPLCONF_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::REPLICAOF_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::ROLE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SAVE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SHUTDOWN_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SLOWLOG_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SORT_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SWAPDB_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SYNC_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::WAIT_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::command::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::EVAL_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::EVAL_RO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::EVALSHA_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::EVALSHA_RO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::SCRIPT_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::keys::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scan::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::object::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::touch::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::randomkey::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::flush::FLUSHDB_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::flush::FLUSHALL_COMMAND,
    #[cfg(feature = "redis-functions")]
    &crate::commands::function_cmd::FUNCTION_COMMAND,
    #[cfg(feature = "redis-functions")]
    &crate::commands::function_cmd::FCALL_COMMAND,
    #[cfg(feature = "redis-functions")]
    &crate::commands::function_cmd::FCALL_RO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::memory::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::PUBLISH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::SPUBLISH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::PUBSUB_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::SUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::UNSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::PSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::PUNSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::SSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::SUNSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFADD_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFCOUNT_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFMERGE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFDEBUG_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFSELFTEST_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOADD_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEODIST_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOHASH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOPOS_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEORADIUS_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEORADIUSBYMEMBER_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEORADIUS_RO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEORADIUSBYMEMBER_RO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOSEARCH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOSEARCHSTORE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XACK_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XADD_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XCLAIM_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XDEL_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XGROUP_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XINFO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XLEN_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XPENDING_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XRANGE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XREAD_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XREADGROUP_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XREVRANGE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XSETID_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XTRIM_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::copy::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::dump::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::restore::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::rename::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::renamenx::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::unlink::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::strlen::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::getrange::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::getbit::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::setbit::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bitcount::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bitpos::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bitop::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bitfield::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::setnx::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::mget::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hstrlen::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hget::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hlen::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hmset::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hmget::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hkeys::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hvals::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hgetall::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hscan::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lrange::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::llen::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lindex::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::rpoplpush::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::brpoplpush::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lmove::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::blmove::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lmpop::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::blmpop::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scard::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::smembers::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::sscan::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::srandmember::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::smismember::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zunion::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zinter::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zdiff::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zintercard::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrandmember::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrange::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrangebyscore::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrevrange::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zscore::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zcount::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrevrangebyscore::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrangebylex::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrevrangebylex::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zlexcount::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zremrangebyrank::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zremrangebyscore::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zremrangebylex::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrank::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrevrank::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zscan::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zmpop::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bzmpop::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_3: &[&dyn RawDirectCommand] = &[
    &crate::commands::get::COMMAND,
    &crate::commands::set::COMMAND,
    &crate::commands::del::COMMAND,
    &crate::commands::ttl::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::acl::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lcs::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_4: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::httl::COMMAND,
    &crate::commands::pttl::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::MOVE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::POST_WARNING_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::ROLE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SAVE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SORT_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SYNC_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::WAIT_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::EVAL_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::keys::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scan::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::mget::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hget::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hlen::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::llen::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lpos::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::copy::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::dump::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XACK_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XADD_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XDEL_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XLEN_COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_5: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::hpttl::COMMAND,
    &crate::commands::getex::COMMAND,
    &crate::commands::setex::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::DEBUG_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::HOST_WARNING_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::PSYNC_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::reset::COMMAND,
    #[cfg(feature = "redis-functions")]
    &crate::commands::function_cmd::FCALL_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::touch::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hscan::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hmget::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hmset::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hkeys::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hvals::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bitop::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::setnx::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lmove::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lmpop::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scard::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::sscan::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zdiff::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrank::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zscan::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zmpop::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFADD_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XINFO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XREAD_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XTRIM_COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_6: &[&dyn RawDirectCommand] = &[
    &crate::commands::exists::COMMAND,
    &crate::commands::expire::COMMAND,
    &crate::commands::psetex::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::ASKING_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::BGSAVE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::LOLWUT_COMMAND,
    #[cfg(feature = "redis-modules")]
    &crate::commands::admin::MODULE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SWAPDB_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::SCRIPT_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::object::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::memory::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::client::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::PUBSUB_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOADD_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOPOS_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XCLAIM_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XGROUP_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XRANGE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XSETID_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::rename::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::unlink::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::strlen::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::getbit::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::setbit::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bitpos::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lrange::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::lindex::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::blmove::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::blmpop::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zunion::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zinter::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrange::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zscore::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zcount::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bzmpop::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_7: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::admin::SORT_RO_COMMAND,
    &crate::commands::pexpire::COMMAND,
    &crate::commands::persist::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::waitaof::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hexpire::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::CLUSTER_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::LATENCY_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::MIGRATE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::MONITOR_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SLOWLOG_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::command::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::EVALSHA_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::EVAL_RO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::flush::FLUSHDB_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::PUBLISH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFCOUNT_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFMERGE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFDEBUG_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEODIST_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOHASH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stralgo::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::restore::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hstrlen::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hgetall::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_8: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::hpersist::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hpexpire::COMMAND,
    #[cfg(feature = "redis-functions")]
    &crate::commands::function_cmd::FUNCTION_COMMAND,
    #[cfg(feature = "redis-functions")]
    &crate::commands::function_cmd::FCALL_RO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::SPUBLISH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::expireat::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::FAILOVER_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::LASTSAVE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::READONLY_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::REPLCONF_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::SHUTDOWN_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::flush::FLUSHALL_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XPENDING_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::renamenx::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::getrange::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bitcount::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::bitfield::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::smembers::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrevrank::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_9: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::hexpireat::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pexpireat::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::READWRITE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::REPLICAOF_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::randomkey::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::SUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEORADIUS_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOSEARCH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XREVRANGE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::rpoplpush::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrevrange::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zlexcount::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_10: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::hpexpireat::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::expiretime::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::PSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::SSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::scripting::EVALSHA_RO_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hll::PFSELFTEST_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XREADGROUP_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::stream::XAUTOCLAIM_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::brpoplpush::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::smismember::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::sintercard::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zintercard::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_11: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::bitfield_ro::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::hexpiretime::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pexpiretime::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::UNSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::srandmember::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrandmember::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zrangebylex::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_12: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::hpexpiretime::COMMAND,
    &crate::commands::semantic::SEMANTIC_SET_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::admin::BGREWRITEAOF_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::PUNSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::pubsub::SUNSUBSCRIBE_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEORADIUS_RO_COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_13: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::zrangebyscore::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_14: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::zrevrangebylex::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zremrangebylex::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEOSEARCHSTORE_COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_15: &[&dyn RawDirectCommand] = &[
    &crate::commands::semantic::SEMANTIC_SEARCH_COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zremrangebyrank::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_16: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::zrevrangebyscore::COMMAND,
    #[cfg(feature = "redis")]
    &crate::commands::zremrangebyscore::COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_17: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEORADIUSBYMEMBER_COMMAND,
];

#[cfg(feature = "embedded")]
static RAW_DIRECT_LEN_20: &[&dyn RawDirectCommand] = &[
    #[cfg(feature = "redis")]
    &crate::commands::geo::GEORADIUSBYMEMBER_RO_COMMAND,
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
pub(super) struct ScnpCommandDispatcher;

#[cfg(feature = "embedded")]
#[derive(Clone, Copy)]
pub(super) struct ScnpCommandEntry {
    command: &'static dyn ScnpDirectCommand,
    mutates_value: bool,
}

#[cfg(feature = "embedded")]
impl ScnpCommandEntry {
    #[inline(always)]
    pub(super) fn mutates_value(self) -> bool {
        self.mutates_value
    }

    #[inline(always)]
    pub(super) fn try_execute_scnp(self, ctx: ScnpCommandContext<'_, '_, '_, '_>) -> ScnpDispatch {
        self.command.try_execute_scnp(ctx)
    }
}

#[cfg(feature = "embedded")]
static SCNP_DIRECT_BY_OPCODE: [Option<ScnpCommandEntry>; 9] = [
    None,
    Some(ScnpCommandEntry {
        command: &crate::commands::get::COMMAND,
        mutates_value: <crate::commands::get::Get as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(ScnpCommandEntry {
        command: &crate::commands::set::COMMAND,
        mutates_value: <crate::commands::set::Set as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(ScnpCommandEntry {
        command: &crate::commands::setex::COMMAND,
        mutates_value:
            <crate::commands::setex::SetEx as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(ScnpCommandEntry {
        command: &crate::commands::getex::COMMAND,
        mutates_value:
            <crate::commands::getex::GetEx as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(ScnpCommandEntry {
        command: &crate::commands::del::COMMAND,
        mutates_value: <crate::commands::del::Del as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(ScnpCommandEntry {
        command: &crate::commands::exists::COMMAND,
        mutates_value:
            <crate::commands::exists::Exists as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(ScnpCommandEntry {
        command: &crate::commands::ttl::COMMAND,
        mutates_value: <crate::commands::ttl::Ttl as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
    Some(ScnpCommandEntry {
        command: &crate::commands::expire::COMMAND,
        mutates_value:
            <crate::commands::expire::Expire as crate::commands::CommandSpec>::MUTATES_VALUE,
    }),
];

#[cfg(feature = "embedded")]
impl ScnpCommandDispatcher {
    #[inline(always)]
    pub(super) fn find_opcode(opcode: u8) -> Option<ScnpCommandEntry> {
        let entry = SCNP_DIRECT_BY_OPCODE
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
        match Self::find(&request.command) {
            Some(command) => {
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
            None => false,
        }
    }
}

#[cfg(feature = "embedded")]
#[inline(always)]
fn find_hot_raw_command(command: &[u8]) -> Option<&'static dyn RawDirectCommand> {
    match command.len() {
        3 if command.eq_ignore_ascii_case(b"GET") => Some(&crate::commands::get::COMMAND),
        3 if command.eq_ignore_ascii_case(b"SET") => Some(&crate::commands::set::COMMAND),
        3 if command.eq_ignore_ascii_case(b"DEL") => Some(&crate::commands::del::COMMAND),
        3 if command.eq_ignore_ascii_case(b"TTL") => Some(&crate::commands::ttl::COMMAND),
        4 if command.eq_ignore_ascii_case(b"PTTL") => Some(&crate::commands::pttl::COMMAND),
        5 if command.eq_ignore_ascii_case(b"GETEX") => Some(&crate::commands::getex::COMMAND),
        6 if command.eq_ignore_ascii_case(b"EXISTS") => Some(&crate::commands::exists::COMMAND),
        6 if command.eq_ignore_ascii_case(b"EXPIRE") => Some(&crate::commands::expire::COMMAND),
        7 if command.eq_ignore_ascii_case(b"PEXPIRE") => Some(&crate::commands::pexpire::COMMAND),
        7 if command.eq_ignore_ascii_case(b"PERSIST") => Some(&crate::commands::persist::COMMAND),
        #[cfg(feature = "redis")]
        4 if command.eq_ignore_ascii_case(b"PING") => Some(&crate::commands::ping::COMMAND),
        #[cfg(feature = "redis")]
        4 if command.eq_ignore_ascii_case(b"ECHO") => Some(&crate::commands::echo::COMMAND),
        #[cfg(feature = "redis")]
        3 if command.eq_ignore_ascii_case(b"ACL") => Some(&crate::commands::acl::COMMAND),
        #[cfg(feature = "redis")]
        3 if command.eq_ignore_ascii_case(b"LCS") => Some(&crate::commands::lcs::COMMAND),
        #[cfg(feature = "redis")]
        4 if command.eq_ignore_ascii_case(b"LPOS") => Some(&crate::commands::lpos::COMMAND),
        #[cfg(feature = "redis")]
        5 if command.eq_ignore_ascii_case(b"RESET") => Some(&crate::commands::reset::COMMAND),
        #[cfg(feature = "redis")]
        4 if command.eq_ignore_ascii_case(b"VADD") => {
            Some(&crate::commands::vector_set::VADD_COMMAND)
        }
        #[cfg(feature = "redis")]
        5 if command.eq_ignore_ascii_case(b"VCARD") => {
            Some(&crate::commands::vector_set::VCARD_COMMAND)
        }
        #[cfg(feature = "redis")]
        4 if command.eq_ignore_ascii_case(b"VDIM") => {
            Some(&crate::commands::vector_set::VDIM_COMMAND)
        }
        #[cfg(feature = "redis")]
        4 if command.eq_ignore_ascii_case(b"VEMB") => {
            Some(&crate::commands::vector_set::VEMB_COMMAND)
        }
        #[cfg(feature = "redis")]
        8 if command.eq_ignore_ascii_case(b"VGETATTR") => {
            Some(&crate::commands::vector_set::VGETATTR_COMMAND)
        }
        #[cfg(feature = "redis")]
        5 if command.eq_ignore_ascii_case(b"VINFO") => {
            Some(&crate::commands::vector_set::VINFO_COMMAND)
        }
        #[cfg(feature = "redis")]
        9 if command.eq_ignore_ascii_case(b"VISMEMBER") => {
            Some(&crate::commands::vector_set::VISMEMBER_COMMAND)
        }
        #[cfg(feature = "redis")]
        6 if command.eq_ignore_ascii_case(b"VLINKS") => {
            Some(&crate::commands::vector_set::VLINKS_COMMAND)
        }
        #[cfg(feature = "redis")]
        11 if command.eq_ignore_ascii_case(b"VRANDMEMBER") => {
            Some(&crate::commands::vector_set::VRANDMEMBER_COMMAND)
        }
        #[cfg(feature = "redis")]
        6 if command.eq_ignore_ascii_case(b"VRANGE") => {
            Some(&crate::commands::vector_set::VRANGE_COMMAND)
        }
        #[cfg(feature = "redis")]
        4 if command.eq_ignore_ascii_case(b"VREM") => {
            Some(&crate::commands::vector_set::VREM_COMMAND)
        }
        #[cfg(feature = "redis")]
        8 if command.eq_ignore_ascii_case(b"VSETATTR") => {
            Some(&crate::commands::vector_set::VSETATTR_COMMAND)
        }
        #[cfg(feature = "redis")]
        4 if command.eq_ignore_ascii_case(b"VSIM") => {
            Some(&crate::commands::vector_set::VSIM_COMMAND)
        }
        #[cfg(feature = "redis-functions")]
        5 if command.eq_ignore_ascii_case(b"FCALL") => {
            Some(&crate::commands::function_cmd::FCALL_COMMAND)
        }
        #[cfg(feature = "redis")]
        6 if command.eq_ignore_ascii_case(b"CLIENT") => Some(&crate::commands::client::COMMAND),
        #[cfg(feature = "redis")]
        6 if command.eq_ignore_ascii_case(b"OBJECT") => Some(&crate::commands::object::COMMAND),
        #[cfg(feature = "redis-functions")]
        8 if command.eq_ignore_ascii_case(b"FUNCTION") => {
            Some(&crate::commands::function_cmd::FUNCTION_COMMAND)
        }
        #[cfg(feature = "redis-functions")]
        8 if command.eq_ignore_ascii_case(b"FCALL_RO") => {
            Some(&crate::commands::function_cmd::FCALL_RO_COMMAND)
        }
        #[cfg(feature = "redis")]
        7 if command.eq_ignore_ascii_case(b"EVAL_RO") => {
            Some(&crate::commands::scripting::EVAL_RO_COMMAND)
        }
        #[cfg(feature = "redis")]
        10 if command.eq_ignore_ascii_case(b"EVALSHA_RO") => {
            Some(&crate::commands::scripting::EVALSHA_RO_COMMAND)
        }
        #[cfg(feature = "redis")]
        6 if command.eq_ignore_ascii_case(b"PUBSUB") => {
            Some(&crate::commands::pubsub::PUBSUB_COMMAND)
        }
        #[cfg(feature = "redis")]
        7 if command.eq_ignore_ascii_case(b"PUBLISH") => {
            Some(&crate::commands::pubsub::PUBLISH_COMMAND)
        }
        #[cfg(feature = "redis")]
        8 if command.eq_ignore_ascii_case(b"SPUBLISH") => {
            Some(&crate::commands::pubsub::SPUBLISH_COMMAND)
        }
        #[cfg(feature = "redis")]
        9 if command.eq_ignore_ascii_case(b"SUBSCRIBE") => {
            Some(&crate::commands::pubsub::SUBSCRIBE_COMMAND)
        }
        #[cfg(feature = "redis")]
        10 if command.eq_ignore_ascii_case(b"SSUBSCRIBE") => {
            Some(&crate::commands::pubsub::SSUBSCRIBE_COMMAND)
        }
        #[cfg(feature = "redis")]
        10 if command.eq_ignore_ascii_case(b"PSUBSCRIBE") => {
            Some(&crate::commands::pubsub::PSUBSCRIBE_COMMAND)
        }
        #[cfg(feature = "redis")]
        11 if command.eq_ignore_ascii_case(b"UNSUBSCRIBE") => {
            Some(&crate::commands::pubsub::UNSUBSCRIBE_COMMAND)
        }
        #[cfg(feature = "redis")]
        12 if command.eq_ignore_ascii_case(b"PUNSUBSCRIBE") => {
            Some(&crate::commands::pubsub::PUNSUBSCRIBE_COMMAND)
        }
        #[cfg(feature = "redis")]
        12 if command.eq_ignore_ascii_case(b"SUNSUBSCRIBE") => {
            Some(&crate::commands::pubsub::SUNSUBSCRIBE_COMMAND)
        }
        _ => None,
    }
}

#[cfg(feature = "embedded")]
#[inline(always)]
pub(super) fn find_primary_raw_command(command: &[u8]) -> Option<&'static dyn RawDirectCommand> {
    let bucket = match command.len() {
        3 => RAW_DIRECT_LEN_3,
        4 => RAW_DIRECT_LEN_4,
        5 => RAW_DIRECT_LEN_5,
        6 => RAW_DIRECT_LEN_6,
        7 => RAW_DIRECT_LEN_7,
        8 => RAW_DIRECT_LEN_8,
        9 => RAW_DIRECT_LEN_9,
        10 => RAW_DIRECT_LEN_10,
        11 => RAW_DIRECT_LEN_11,
        12 => RAW_DIRECT_LEN_12,
        13 => RAW_DIRECT_LEN_13,
        14 => RAW_DIRECT_LEN_14,
        15 => RAW_DIRECT_LEN_15,
        16 => RAW_DIRECT_LEN_16,
        17 => RAW_DIRECT_LEN_17,
        20 => RAW_DIRECT_LEN_20,
        _ => return None,
    };
    find_raw_command_in_bucket(command, bucket)
}

#[cfg(feature = "embedded")]
#[inline(always)]
fn find_raw_command_in_bucket(
    command: &[u8],
    bucket: &[&'static dyn RawDirectCommand],
) -> Option<&'static dyn RawDirectCommand> {
    bucket
        .iter()
        .copied()
        .find(|candidate| candidate.name().as_bytes().eq_ignore_ascii_case(command))
}

#[cfg(feature = "embedded")]
impl RawCommandDispatcher {
    pub(super) fn find(command: &[u8]) -> Option<&'static dyn RawDirectCommand> {
        if let Some(command) = find_hot_raw_command(command) {
            return Some(command);
        }
        if let Some(command) = find_primary_raw_command(command) {
            return Some(command);
        }
        if let Some(command) = RAW_DIRECT_CATALOG
            .iter()
            .copied()
            .find(|candidate| candidate.matches(command))
        {
            return Some(command);
        }
        #[cfg(feature = "redis-modules")]
        {
            crate::commands::redis_modules::find_raw_direct_command(command)
        }
        #[cfg(not(feature = "redis-modules"))]
        {
            None
        }
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
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ExpireAt => Some(&crate::commands::expireat::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::PExpireAt => {
                Some(&crate::commands::pexpireat::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ExpireTime => {
                Some(&crate::commands::expiretime::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::PExpireTime => {
                Some(&crate::commands::pexpiretime::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Command => Some(&crate::commands::command::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Keys => Some(&crate::commands::keys::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Scan => Some(&crate::commands::scan::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Object => Some(&crate::commands::object::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Touch => Some(&crate::commands::touch::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::RandomKey => {
                Some(&crate::commands::randomkey::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::FlushDb => {
                Some(&crate::commands::flush::FLUSHDB_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::FlushAll => {
                Some(&crate::commands::flush::FLUSHALL_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Memory => Some(&crate::commands::memory::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Copy => Some(&crate::commands::copy::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Dump => Some(&crate::commands::dump::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Restore => Some(&crate::commands::restore::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::RestoreAsking => {
                Some(&crate::commands::restore::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Rename => Some(&crate::commands::rename::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::RenameNx => Some(&crate::commands::renamenx::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Unlink => Some(&crate::commands::unlink::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::StrLen => Some(&crate::commands::strlen::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::GetRange => Some(&crate::commands::getrange::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::Substr => Some(&crate::commands::getrange::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::GetBit => Some(&crate::commands::getbit::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::SetBit => Some(&crate::commands::setbit::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::BitCount => Some(&crate::commands::bitcount::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::BitPos => Some(&crate::commands::bitpos::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::BitOp => Some(&crate::commands::bitop::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::BitField => Some(&crate::commands::bitfield::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::SetNx => Some(&crate::commands::setnx::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::MGet => Some(&crate::commands::mget::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::HStrLen => Some(&crate::commands::hstrlen::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::HGet => Some(&crate::commands::hget::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::HLen => Some(&crate::commands::hlen::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::HMSet => Some(&crate::commands::hmset::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::HMGet => Some(&crate::commands::hmget::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::HKeys => Some(&crate::commands::hkeys::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::HVals => Some(&crate::commands::hvals::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::HGetAll => Some(&crate::commands::hgetall::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::HScan => Some(&crate::commands::hscan::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::LRange => Some(&crate::commands::lrange::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::LLen => Some(&crate::commands::llen::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::LIndex => Some(&crate::commands::lindex::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::RPopLPush => {
                Some(&crate::commands::rpoplpush::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::BRPopLPush => {
                Some(&crate::commands::brpoplpush::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::LMove => Some(&crate::commands::lmove::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::BLMove => Some(&crate::commands::blmove::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::LMPop => Some(&crate::commands::lmpop::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::BLMPop => Some(&crate::commands::blmpop::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::SCard => Some(&crate::commands::scard::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::SMembers => Some(&crate::commands::smembers::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::SScan => Some(&crate::commands::sscan::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::SRandMember => {
                Some(&crate::commands::srandmember::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::SMIsMember => {
                Some(&crate::commands::smismember::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZUnion => Some(&crate::commands::zunion::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZInter => Some(&crate::commands::zinter::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZDiff => Some(&crate::commands::zdiff::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZInterCard => {
                Some(&crate::commands::zintercard::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRandMember => {
                Some(&crate::commands::zrandmember::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRange => Some(&crate::commands::zrange::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRevRange => {
                Some(&crate::commands::zrevrange::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZScore => Some(&crate::commands::zscore::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZCount => Some(&crate::commands::zcount::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRangeByScore => {
                Some(&crate::commands::zrangebyscore::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRevRangeByScore => {
                Some(&crate::commands::zrevrangebyscore::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRangeByLex => {
                Some(&crate::commands::zrangebylex::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRevRangeByLex => {
                Some(&crate::commands::zrevrangebylex::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZLexCount => {
                Some(&crate::commands::zlexcount::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRemRangeByRank => {
                Some(&crate::commands::zremrangebyrank::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRemRangeByScore => {
                Some(&crate::commands::zremrangebyscore::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRemRangeByLex => {
                Some(&crate::commands::zremrangebylex::COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRank => Some(&crate::commands::zrank::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZRevRank => Some(&crate::commands::zrevrank::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZScan => Some(&crate::commands::zscan::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::ZMPop => Some(&crate::commands::zmpop::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::BZMPop => Some(&crate::commands::bzmpop::COMMAND),
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VAdd => {
                Some(&crate::commands::vector_set::VADD_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VCard => {
                Some(&crate::commands::vector_set::VCARD_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VDim => {
                Some(&crate::commands::vector_set::VDIM_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VEmb => {
                Some(&crate::commands::vector_set::VEMB_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VGetAttr => {
                Some(&crate::commands::vector_set::VGETATTR_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VInfo => {
                Some(&crate::commands::vector_set::VINFO_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VIsMember => {
                Some(&crate::commands::vector_set::VISMEMBER_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VLinks => {
                Some(&crate::commands::vector_set::VLINKS_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VRandMember => {
                Some(&crate::commands::vector_set::VRANDMEMBER_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VRange => {
                Some(&crate::commands::vector_set::VRANGE_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VRem => {
                Some(&crate::commands::vector_set::VREM_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VSetAttr => {
                Some(&crate::commands::vector_set::VSETATTR_COMMAND)
            }
            #[cfg(feature = "redis")]
            crate::protocol::FastCommandKind::VSim => {
                Some(&crate::commands::vector_set::VSIM_COMMAND)
            }
            _ => None,
        }
    }

    pub(super) fn mutates_value(command: &[u8]) -> bool {
        Self::find(command).is_some_and(|command| command.mutates_value())
    }
}
