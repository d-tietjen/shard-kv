use std::io::Write;

use crate::commands::ScnpCommand;
use crate::connection::ScnpConnection;
use crate::error::Result;
use crate::protocol::{FAST_FLAG_KEY_HASH, ROUTED_FLAGS};
use crate::routing::{ShardCacheRoute, hash_key};

pub(crate) struct Set<'a> {
    key: &'a [u8],
    value: &'a [u8],
    route: Option<ShardCacheRoute>,
}

impl<'a> Set<'a> {
    pub(crate) fn new(key: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            key,
            value,
            route: None,
        }
    }

    pub(crate) fn routed(route: ShardCacheRoute, key: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            key,
            value,
            route: Some(route),
        }
    }
}

impl ScnpCommand for Set<'_> {
    type Output = ();

    const NAME: &'static str = "SET";
    const OPCODE: u8 = 2;

    fn flags(&self) -> u8 {
        flags(self.route)
    }

    fn body_len(&self) -> usize {
        body_len(self.route, self.key.len(), self.value.len())
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        write_body(w, self.route, self.key, self.value)
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        conn.expect_ok(Self::NAME)
    }
}

pub(crate) fn write_request(
    conn: &mut ScnpConnection,
    route: Option<ShardCacheRoute>,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    conn.write_header(
        <Set as ScnpCommand>::OPCODE,
        flags(route),
        body_len(route, key.len(), value.len()) as u32,
    )?;
    write_body(&mut conn.w, route, key, value)
}

fn flags(route: Option<ShardCacheRoute>) -> u8 {
    route.map_or(FAST_FLAG_KEY_HASH, |_| ROUTED_FLAGS)
}

fn body_len(route: Option<ShardCacheRoute>, key_len: usize, value_len: usize) -> usize {
    if route.is_some() {
        28 + key_len + value_len
    } else {
        16 + key_len + value_len
    }
}

fn write_body<W: Write>(
    w: &mut W,
    route: Option<ShardCacheRoute>,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    if let Some(route) = route {
        route.write_to(w)?;
    } else {
        w.write_all(&hash_key(key).to_le_bytes())?;
    }
    w.write_all(&(key.len() as u32).to_le_bytes())?;
    w.write_all(&(value.len() as u32).to_le_bytes())?;
    w.write_all(key)?;
    w.write_all(value)?;
    Ok(())
}
