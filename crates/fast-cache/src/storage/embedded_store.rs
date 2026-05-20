use crossbeam_utils::CachePadded;
#[cfg(not(feature = "embedded-read-biased-lock"))]
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "embedded-read-biased-lock")]
use rblock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "telemetry")]
use std::sync::Arc;
#[cfg(feature = "telemetry")]
use std::time::Instant;

use crate::config::EvictionPolicy;
use crate::storage::{
    Bytes, PackedBatch, PreparedPointKey, StoredEntry, hash_key, hash_key_tag_from_hash, now_millis,
};
#[cfg(feature = "telemetry")]
use crate::storage::{CacheTelemetry, CacheTelemetryHandle};
#[cfg(feature = "redis-compat")]
use crate::storage::{
    RedisObjectBucket, RedisObjectError, RedisObjectReadOutcome, RedisObjectResult,
    RedisObjectStore, RedisObjectValue, RedisObjectWriteAttempt, RedisStringLookup,
};
#[cfg(feature = "embedded")]
use crate::storage::{ShardStatsSnapshot, TierStatsSnapshot};

mod batch;
mod batch_results;
mod core;
mod lifecycle;
#[cfg(feature = "redis-compat")]
mod objects;
mod owned;
mod point;
mod routing;
mod session_slots;
mod shard;
mod views;
mod write;

pub use owned::{
    EmbeddedShardHandle, OwnedEmbeddedShard, OwnedEmbeddedWorkerReadSession,
    OwnedEmbeddedWorkerShards,
};
#[cfg(feature = "unsafe")]
use routing::can_skip_session_lookup;
pub use routing::{
    EmbeddedKeyRoute, EmbeddedRouteMode, EmbeddedSessionRoute, shift_for, stripe_index,
};
pub(crate) use routing::{assert_valid_shard_count, compute_key_route, compute_session_shard};
use routing::{
    batch_derived_session_storage_prefix, can_route_with_key_hash, can_use_route_hash_as_key_hash,
    derived_session_storage_prefix, point_write_session_storage_prefix, session_route_prefix,
    uses_flat_key_storage,
};
pub use session_slots::PackedSessionWrite;
pub(crate) use session_slots::SessionSlotMap;
pub(crate) use shard::EmbeddedShard;
pub use views::{
    EmbeddedBatchReadView, EmbeddedReadSlice, EmbeddedReadView, EmbeddedRef, EmbeddedRefMut,
    EmbeddedSessionBatchView, OwnedEmbeddedBatchReadView, OwnedEmbeddedReadView,
    OwnedEmbeddedRefMut, OwnedEmbeddedSessionBatchView, OwnedEmbeddedSessionPackedView,
};

/// Shared embedded in-memory database.
///
/// `EmbeddedStore` is internally sharded and can be shared across threads. It
/// offers byte-string key/value methods, TTL management, batch reads and
/// writes, and session-oriented packed transfer APIs. Redis/Valkey object
/// helpers are available with the `redis-compat` feature.
#[derive(Debug)]
pub struct EmbeddedStore {
    shards: Box<[CachePadded<RwLock<EmbeddedShard>>]>,
    shift: u32,
    #[cfg(feature = "redis-compat")]
    objects: RedisObjectStore,
    route_mode: EmbeddedRouteMode,
    #[cfg(feature = "telemetry")]
    metrics: Option<Arc<CacheTelemetry>>,
}

#[inline(always)]
fn reserve_batch_capacity(buffer: &mut Vec<u8>, next_len: usize, item_count: usize) {
    if buffer.capacity() == 0 && next_len > 0 {
        // Reserve for the whole batch on the first hit so large chunk reads avoid
        // repeated reallocations while packing the response payload.
        buffer.reserve(next_len.saturating_mul(item_count));
    }
}

#[inline(always)]
#[cfg(feature = "embedded")]
fn accumulate_tier_stats(target: &mut TierStatsSnapshot, snapshot: &TierStatsSnapshot) {
    target.len = target.len.saturating_add(snapshot.len);
    target.capacity = target.capacity.saturating_add(snapshot.capacity);
    target.hits = target.hits.saturating_add(snapshot.hits);
    target.misses = target.misses.saturating_add(snapshot.misses);
    target.promotions = target.promotions.saturating_add(snapshot.promotions);
    target.demotions = target.demotions.saturating_add(snapshot.demotions);
    target.evictions = target.evictions.saturating_add(snapshot.evictions);
    target.expirations = target.expirations.saturating_add(snapshot.expirations);
}

#[inline(always)]
fn write_now_ms(ttl_ms: Option<u64>, memory_limit_bytes: Option<usize>) -> u64 {
    if ttl_ms.is_some() || memory_limit_bytes.is_some() {
        now_millis()
    } else {
        0
    }
}

#[inline(always)]
fn write_resp_blob_string_into(out: &mut bytes::BytesMut, value: &[u8]) {
    #[cfg(not(feature = "unsafe"))]
    {
        let mut buf = itoa::Buffer::new();
        let len_str = buf.format(value.len()).as_bytes();
        out.extend_from_slice(b"$");
        out.extend_from_slice(len_str);
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }
    #[cfg(feature = "unsafe")]
    {
        if value.len() == 64 {
            const HEADER: &[u8] = b"$64\r\n";
            let total = HEADER.len() + 64 + 2;
            out.reserve(total);
            // SAFETY: reserve(total) ensures `total` bytes of spare capacity.
            unsafe {
                let start = out.len();
                let dst = out.as_mut_ptr().add(start);
                std::ptr::copy_nonoverlapping(HEADER.as_ptr(), dst, HEADER.len());
                std::ptr::copy_nonoverlapping(value.as_ptr(), dst.add(HEADER.len()), 64);
                *dst.add(HEADER.len() + 64) = b'\r';
                *dst.add(HEADER.len() + 65) = b'\n';
                out.set_len(start + total);
            }
            return;
        }

        let mut buf = itoa::Buffer::new();
        let len_str = buf.format(value.len()).as_bytes();
        let total = 1 + len_str.len() + 2 + value.len() + 2;
        out.reserve(total);
        // SAFETY: reserve(total) ensures `total` bytes of spare capacity.
        unsafe {
            let start = out.len();
            let dst = out.as_mut_ptr().add(start);
            *dst = b'$';
            let mut pos = 1usize;
            std::ptr::copy_nonoverlapping(len_str.as_ptr(), dst.add(pos), len_str.len());
            pos += len_str.len();
            *dst.add(pos) = b'\r';
            *dst.add(pos + 1) = b'\n';
            pos += 2;
            std::ptr::copy_nonoverlapping(value.as_ptr(), dst.add(pos), value.len());
            pos += value.len();
            *dst.add(pos) = b'\r';
            *dst.add(pos + 1) = b'\n';
            out.set_len(start + total);
        }
    }
}

#[cfg(test)]
mod tests;
