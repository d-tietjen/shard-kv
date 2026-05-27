use std::io::Write;

use crate::error::Result;
use crate::protocol::{FAST_FLAG_KEY_HASH, ROUTED_FLAGS};
use crate::routing::{ShardCacheRoute, hash_key};

pub(crate) fn flags(route: Option<ShardCacheRoute>) -> u8 {
    route.map_or(FAST_FLAG_KEY_HASH, |_| ROUTED_FLAGS)
}

pub(crate) fn key_body_len(route: Option<ShardCacheRoute>, key_len: usize) -> usize {
    route_prefix_len(route) + 4 + key_len
}

pub(crate) fn ttl_key_body_len(route: Option<ShardCacheRoute>, key_len: usize) -> usize {
    route_prefix_len(route) + 8 + 4 + key_len
}

pub(crate) fn ttl_key_value_body_len(
    route: Option<ShardCacheRoute>,
    key_len: usize,
    value_len: usize,
) -> usize {
    route_prefix_len(route) + 8 + 4 + 4 + key_len + value_len
}

pub(crate) fn write_key_body<W: Write>(
    w: &mut W,
    route: Option<ShardCacheRoute>,
    key: &[u8],
) -> Result<()> {
    write_route_prefix(w, route, key)?;
    write_len_prefixed(w, key)
}

pub(crate) fn write_ttl_key_body<W: Write>(
    w: &mut W,
    route: Option<ShardCacheRoute>,
    key: &[u8],
    ttl_ms: u64,
) -> Result<()> {
    write_route_prefix(w, route, key)?;
    w.write_all(&ttl_ms.to_le_bytes())?;
    write_len_prefixed(w, key)
}

pub(crate) fn write_ttl_key_value_body<W: Write>(
    w: &mut W,
    route: Option<ShardCacheRoute>,
    key: &[u8],
    value: &[u8],
    ttl_ms: u64,
) -> Result<()> {
    write_route_prefix(w, route, key)?;
    w.write_all(&ttl_ms.to_le_bytes())?;
    w.write_all(&(key.len() as u32).to_le_bytes())?;
    w.write_all(&(value.len() as u32).to_le_bytes())?;
    w.write_all(key)?;
    w.write_all(value)?;
    Ok(())
}

fn route_prefix_len(route: Option<ShardCacheRoute>) -> usize {
    if route.is_some() { 20 } else { 8 }
}

fn write_route_prefix<W: Write>(
    w: &mut W,
    route: Option<ShardCacheRoute>,
    key: &[u8],
) -> Result<()> {
    if let Some(route) = route {
        route.write_to(w)?;
    } else {
        w.write_all(&hash_key(key).to_le_bytes())?;
    }
    Ok(())
}

fn write_len_prefixed<W: Write>(w: &mut W, value: &[u8]) -> Result<()> {
    w.write_all(&(value.len() as u32).to_le_bytes())?;
    w.write_all(value)?;
    Ok(())
}
