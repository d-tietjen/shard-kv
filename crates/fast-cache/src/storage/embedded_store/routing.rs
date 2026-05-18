use crate::storage::{Bytes, hash_key};

#[cfg(feature = "unsafe")]
use super::SessionSlotMap;

/// Precomputed routing metadata for one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedKeyRoute {
    /// Shard selected for the key.
    pub shard_id: usize,
    /// Precomputed primary key hash.
    pub key_hash: u64,
}

/// Precomputed shard placement for one session prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedSessionRoute {
    /// Shard selected for the session.
    pub shard_id: usize,
}

/// Selects how embedded database traffic is routed across shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EmbeddedRouteMode {
    /// Route by the full key bytes. This matches the generic store behavior.
    #[default]
    FullKey,
    /// Route all `s:<session>:c:<chunk>` keys for a session to the same shard.
    SessionPrefix,
}

impl EmbeddedRouteMode {
    /// Returns the stable configuration string for this route mode.
    #[inline(always)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullKey => "full_key",
            Self::SessionPrefix => "session_prefix",
        }
    }
}

#[inline(always)]
pub(crate) fn compute_key_route(
    route_mode: EmbeddedRouteMode,
    shift: u32,
    key: &[u8],
) -> EmbeddedKeyRoute {
    let key_hash = hash_key(key);
    let route_hash = match route_mode {
        EmbeddedRouteMode::FullKey => key_hash,
        EmbeddedRouteMode::SessionPrefix => hash_key(session_route_prefix(key)),
    };
    EmbeddedKeyRoute {
        shard_id: stripe_index(route_hash, shift),
        key_hash,
    }
}

#[inline(always)]
pub(crate) fn compute_session_shard(shift: u32, session_prefix: &[u8]) -> usize {
    stripe_index(hash_key(session_prefix), shift)
}

#[inline(always)]
pub fn stripe_index(hash: u64, shift: u32) -> usize {
    if shift == usize::BITS {
        0
    } else {
        ((hash as usize) << 7) >> shift
    }
}

#[inline(always)]
pub fn shift_for(shard_count: usize) -> u32 {
    debug_assert!(shard_count > 0 && shard_count.is_power_of_two());
    usize::BITS - shard_count.trailing_zeros()
}

#[inline(always)]
pub(crate) fn assert_valid_shard_count(shard_count: usize) {
    assert!(
        shard_count > 0 && shard_count.is_power_of_two(),
        "shard_count must be a non-zero power of two; got {shard_count}"
    );
}

#[cfg(feature = "unsafe")]
#[inline(always)]
pub(super) fn can_skip_session_lookup(key: &[u8], session_slots: &SessionSlotMap) -> bool {
    session_slots.is_empty() || (!key.starts_with(b"s:") && !key.contains(&b'@'))
}

#[inline(always)]
pub(super) fn can_route_with_key_hash(
    route_mode: EmbeddedRouteMode,
    shard_count: usize,
    key: &[u8],
) -> bool {
    route_mode == EmbeddedRouteMode::FullKey || shard_count == 1 || !key.starts_with(b"s:")
}

#[inline(always)]
pub(super) fn can_use_route_hash_as_key_hash(route_mode: EmbeddedRouteMode, key: &[u8]) -> bool {
    route_mode == EmbeddedRouteMode::FullKey || !key.starts_with(b"s:")
}

#[inline(always)]
pub(super) fn uses_flat_key_storage(route_mode: EmbeddedRouteMode, key: &[u8]) -> bool {
    route_mode == EmbeddedRouteMode::FullKey || derived_session_storage_prefix(key).is_none()
}

#[inline(always)]
pub(super) fn session_route_prefix(key: &[u8]) -> &[u8] {
    if !key.starts_with(b"s:") {
        return key;
    }

    if let Some(index) = session_chunk_separator(key) {
        return &key[..index];
    }

    key
}

#[inline(always)]
fn session_chunk_separator(key: &[u8]) -> Option<usize> {
    if key.len() < 3 {
        return None;
    }

    let mut index = key.len() - 3;
    loop {
        if key[index] == b':' && key[index + 1] == b'c' && key[index + 2] == b':' {
            return Some(index);
        }
        if index == 0 {
            return None;
        }
        index -= 1;
    }
}

#[inline(always)]
pub(super) fn derived_session_storage_prefix(key: &[u8]) -> Option<Bytes> {
    if key.starts_with(b"s:") {
        return Some(session_route_prefix(key).to_vec());
    }

    // Fast reject ordinary keys before doing UTF-8 decoding and string splits.
    if !key.contains(&b'@') {
        return None;
    }

    let key_str = std::str::from_utf8(key).ok()?;
    let session = key_str
        .split('@')
        .find_map(|part| part.strip_prefix("session%"))?;
    Some(format!("lmcache-session:{session}").into_bytes())
}

#[inline(always)]
pub(super) fn point_write_session_storage_prefix(key: &[u8]) -> Option<Bytes> {
    if key.starts_with(b"s:") {
        Some(session_route_prefix(key).to_vec())
    } else {
        None
    }
}

#[inline(always)]
pub(super) fn batch_derived_session_storage_prefix(keys: &[Bytes]) -> Option<Bytes> {
    let first = derived_session_storage_prefix(keys.first()?.as_slice())?;
    if keys[1..].iter().all(|key| {
        derived_session_storage_prefix(key.as_slice()).as_deref() == Some(first.as_slice())
    }) {
        Some(first)
    } else {
        None
    }
}
