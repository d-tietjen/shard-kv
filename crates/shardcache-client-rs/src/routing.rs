use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::error::{Result, ShardCacheClientError};

/// Router for shard-owned SCNP direct ports.
///
/// The base address is the first shard-owned port. Shard `n` is expected at
/// `base_port + n`; when the server also exposes a fanout port, this base is
/// usually `SHARDCACHE_DIRECT_SHARD_BASE_PORT` or the fanout port + 1.
#[derive(Debug, Clone, Copy)]
pub struct ShardCacheDirectRouter {
    base_addr: SocketAddr,
    shard_count: usize,
    shift: u32,
    route_mode: ShardCacheRouteMode,
}

impl ShardCacheDirectRouter {
    /// Creates a direct router for `shard_count` server shards.
    pub fn new(addr: impl ToSocketAddrs, shard_count: usize) -> Result<Self> {
        if shard_count == 0 || !shard_count.is_power_of_two() {
            return Err(ShardCacheClientError::Config(format!(
                "SCNP direct shard count must be a non-zero power of two: {shard_count}"
            )));
        }
        let base_addr = resolve_one(addr)?;
        Ok(Self {
            base_addr,
            shard_count,
            shift: shift_for(shard_count),
            route_mode: ShardCacheRouteMode::FullKey,
        })
    }

    /// Sets how direct shard routing chooses the owning shard.
    ///
    /// `FullKey` is the normal point-key mode. `SessionPrefix` routes keys of
    /// the form `s:<session>:c:<chunk>` by `s:<session>` while preserving the
    /// full-key hash used for lookup within that shard.
    pub fn with_route_mode(mut self, route_mode: ShardCacheRouteMode) -> Self {
        self.route_mode = route_mode;
        self
    }

    /// Returns the number of direct shard ports.
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Computes the routed SCNP metadata for `key`.
    pub fn route_key(&self, key: &[u8]) -> ShardCacheRoute {
        let key_hash = hash_key(key);
        let shard_id = match self.route_mode {
            ShardCacheRouteMode::OverflowSlot => overflow_slot_shard(key, self.shard_count)
                .unwrap_or_else(|| stripe_index(key_hash, self.shift)),
            mode => stripe_index(mode.route_hash(key, key_hash), self.shift),
        };
        ShardCacheRoute {
            key_hash,
            key_tag: hash_key_tag_from_hash(key_hash),
            shard_id,
        }
    }

    /// Returns the socket address for `shard_id`.
    pub fn shard_addr(&self, shard_id: usize) -> Result<SocketAddr> {
        if shard_id >= self.shard_count {
            return Err(ShardCacheClientError::Config(format!(
                "SCNP direct shard {shard_id} out of range for {} shards",
                self.shard_count
            )));
        }
        let mut addr = self.base_addr;
        let offset = u16::try_from(shard_id).map_err(|_| {
            ShardCacheClientError::Config(format!("SCNP direct shard id exceeds u16: {shard_id}"))
        })?;
        let port = self.base_addr.port().checked_add(offset).ok_or_else(|| {
            ShardCacheClientError::Config(format!(
                "SCNP direct shard port overflows for shard {shard_id}"
            ))
        })?;
        addr.set_port(port);
        Ok(addr)
    }
}

/// Shard-routing mode for direct SCNP clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardCacheRouteMode {
    /// Route every key by its full key hash.
    FullKey,
    /// Route `s:<session>:c:<chunk>` keys by the session prefix.
    SessionPrefix,
    /// Route a versioned internal overflow key by its encoded shard field.
    OverflowSlot,
}

impl ShardCacheRouteMode {
    /// Parses the route mode used by benchmark and deployment knobs.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "full_key" | "full-key" | "point" => Ok(Self::FullKey),
            "session_prefix" | "session-prefix" | "session" => Ok(Self::SessionPrefix),
            "overflow_slot" | "overflow-slot" | "overflow" => Ok(Self::OverflowSlot),
            other => Err(ShardCacheClientError::Config(format!(
                "unknown SCNP direct route mode `{other}`; use full_key, session_prefix, or overflow_slot"
            ))),
        }
    }

    fn route_hash(self, key: &[u8], key_hash: u64) -> u64 {
        match self {
            Self::FullKey => key_hash,
            Self::SessionPrefix => hash_key(session_route_prefix(key)),
            Self::OverflowSlot => key_hash,
        }
    }
}

const OVERFLOW_SLOT_KEY_MAGIC: &[u8; 8] = b"SCKVKEY1";

fn overflow_slot_shard(key: &[u8], shard_count: usize) -> Option<usize> {
    if key.len() < 18 || &key[..8] != OVERFLOW_SLOT_KEY_MAGIC {
        return None;
    }
    let shard_id = u32::from_le_bytes(key[8..12].try_into().ok()?) as usize;
    (shard_id < shard_count).then_some(shard_id)
}

/// Precomputed routed SCNP metadata for a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardCacheRoute {
    /// Primary key hash.
    pub key_hash: u64,
    /// Secondary key tag used by direct full-key lookups.
    pub key_tag: u64,
    /// Owning shard id.
    pub shard_id: usize,
}

impl ShardCacheRoute {
    pub(crate) fn write_to<W: Write>(&self, w: &mut W) -> Result<()> {
        w.write_all(&self.key_hash.to_le_bytes())?;
        w.write_all(&(self.shard_id as u32).to_le_bytes())?;
        w.write_all(&self.key_tag.to_le_bytes())?;
        Ok(())
    }
}

/// Computes shardcache's primary XXH3 key hash.
pub fn hash_key(key: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(key)
}

/// Computes shardcache's secondary key fingerprint.
pub fn hash_key_tag(key: &[u8]) -> u64 {
    hash_key_tag_from_hash(hash_key(key))
}

/// Computes the secondary key fingerprint from an already-computed primary hash.
pub fn hash_key_tag_from_hash(hash: u64) -> u64 {
    hash >> 56
}

fn session_route_prefix(key: &[u8]) -> &[u8] {
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

/// Computes the shard index for `hash` and `shard_count`.
pub fn shard_index(hash: u64, shard_count: usize) -> Result<usize> {
    if shard_count == 0 || !shard_count.is_power_of_two() {
        return Err(ShardCacheClientError::Config(format!(
            "shard count must be a non-zero power of two: {shard_count}"
        )));
    }
    Ok(stripe_index(hash, shift_for(shard_count)))
}

fn stripe_index(hash: u64, shift: u32) -> usize {
    if shift == usize::BITS {
        0
    } else {
        ((hash as usize) << 7) >> shift
    }
}

fn shift_for(shard_count: usize) -> u32 {
    debug_assert!(shard_count > 0 && shard_count.is_power_of_two());
    usize::BITS - shard_count.trailing_zeros()
}

fn resolve_one(addr: impl ToSocketAddrs) -> Result<SocketAddr> {
    addr.to_socket_addrs()?.next().ok_or_else(|| {
        ShardCacheClientError::Config("SCNP address resolved to no socket addresses".into())
    })
}
