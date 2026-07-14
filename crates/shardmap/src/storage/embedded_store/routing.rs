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
    /// Route versioned key-value overflow keys by their encoded remote shard.
    OverflowSlot,
}

impl EmbeddedRouteMode {
    /// Returns the stable configuration string for this route mode.
    #[inline(always)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullKey => "full_key",
            Self::SessionPrefix => "session_prefix",
            Self::OverflowSlot => "overflow_slot",
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
    if route_mode == EmbeddedRouteMode::OverflowSlot
        && let Some(shard_id) = overflow_slot_shard(key, shift)
    {
        return EmbeddedKeyRoute { shard_id, key_hash };
    }
    let route_hash = match route_mode {
        EmbeddedRouteMode::FullKey | EmbeddedRouteMode::OverflowSlot => key_hash,
        EmbeddedRouteMode::SessionPrefix => hash_key(session_route_prefix(key)),
    };
    EmbeddedKeyRoute {
        shard_id: stripe_index(route_hash, shift),
        key_hash,
    }
}

pub(crate) const OVERFLOW_SLOT_KEY_MAGIC: &[u8; 8] = b"SCKVKEY1";

/// Returns the encoded overflow destination when `key` has a valid internal
/// header for the configured power-of-two shard geometry.
#[inline]
pub(crate) fn overflow_slot_shard(key: &[u8], shift: u32) -> Option<usize> {
    if key.len() < 18 || &key[..8] != OVERFLOW_SLOT_KEY_MAGIC {
        return None;
    }
    let shard_id = u32::from_le_bytes(key[8..12].try_into().ok()?) as usize;
    let shard_count = if shift == usize::BITS {
        1
    } else {
        1usize << (usize::BITS - shift)
    };
    (shard_id < shard_count).then_some(shard_id)
}

#[inline]
pub(crate) fn route_hash_for_shard(shard_id: usize, shift: u32) -> u64 {
    if shift == usize::BITS {
        0
    } else {
        (shard_id as u64) << (shift - 7)
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
    match route_mode {
        EmbeddedRouteMode::FullKey => true,
        EmbeddedRouteMode::SessionPrefix => shard_count == 1 || !key.starts_with(b"s:"),
        EmbeddedRouteMode::OverflowSlot => shard_count == 1,
    }
}

#[inline(always)]
pub(super) fn can_use_route_hash_as_key_hash(route_mode: EmbeddedRouteMode, key: &[u8]) -> bool {
    match route_mode {
        EmbeddedRouteMode::FullKey => true,
        EmbeddedRouteMode::SessionPrefix => !key.starts_with(b"s:"),
        EmbeddedRouteMode::OverflowSlot => false,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn overflow_key(remote_shard: u32) -> Vec<u8> {
        let mut key = OVERFLOW_SLOT_KEY_MAGIC.to_vec();
        key.extend_from_slice(&remote_shard.to_le_bytes());
        key.extend_from_slice(&7u32.to_le_bytes());
        key.extend_from_slice(&1u16.to_le_bytes());
        key.extend_from_slice(b"c");
        key.extend_from_slice(b"original");
        key
    }

    #[test]
    fn overflow_slot_routes_only_valid_encoded_shards() {
        let shift = shift_for(4);
        for shard in 0..4 {
            assert_eq!(
                compute_key_route(EmbeddedRouteMode::OverflowSlot, shift, &overflow_key(shard))
                    .shard_id,
                shard as usize
            );
        }

        let malformed = overflow_key(4);
        assert_eq!(overflow_slot_shard(&malformed, shift), None);
        assert!(compute_key_route(EmbeddedRouteMode::OverflowSlot, shift, &malformed).shard_id < 4);
    }

    #[test]
    fn overflow_slot_prehashed_shortcuts_are_disabled() {
        let key = overflow_key(3);
        assert!(!can_route_with_key_hash(
            EmbeddedRouteMode::OverflowSlot,
            4,
            &key
        ));
        assert!(!can_use_route_hash_as_key_hash(
            EmbeddedRouteMode::OverflowSlot,
            &key
        ));
    }
}
