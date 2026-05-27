use std::io::Write;

use crate::commands::ScnpCommand;
use crate::connection::ScnpConnection;
use crate::error::Result;
use crate::protocol::{FAST_FLAG_KEY_HASH, ROUTED_FLAGS};
use crate::routing::{ShardCacheRoute, hash_key};

pub(crate) struct Get<'key, 'out> {
    key: &'key [u8],
    out: &'out mut Vec<u8>,
    route: Option<ShardCacheRoute>,
}

impl<'key, 'out> Get<'key, 'out> {
    pub(crate) fn new(key: &'key [u8], out: &'out mut Vec<u8>) -> Self {
        Self {
            key,
            out,
            route: None,
        }
    }

    pub(crate) fn routed(route: ShardCacheRoute, key: &'key [u8], out: &'out mut Vec<u8>) -> Self {
        Self {
            key,
            out,
            route: Some(route),
        }
    }
}

impl ScnpCommand for Get<'_, '_> {
    type Output = bool;

    const NAME: &'static str = "GET";
    const OPCODE: u8 = 1;

    fn flags(&self) -> u8 {
        flags(self.route)
    }

    fn body_len(&self) -> usize {
        body_len(self.route, self.key.len())
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        write_body(w, self.route, self.key)
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        conn.read_value(Self::NAME, self.out)
    }
}

pub(crate) fn write_request(
    conn: &mut ScnpConnection,
    route: Option<ShardCacheRoute>,
    key: &[u8],
) -> Result<()> {
    conn.write_header(
        <Get as ScnpCommand>::OPCODE,
        flags(route),
        body_len(route, key.len()) as u32,
    )?;
    write_body(&mut conn.w, route, key)
}

fn flags(route: Option<ShardCacheRoute>) -> u8 {
    route.map_or(FAST_FLAG_KEY_HASH, |_| ROUTED_FLAGS)
}

fn body_len(route: Option<ShardCacheRoute>, key_len: usize) -> usize {
    if route.is_some() {
        24 + key_len
    } else {
        12 + key_len
    }
}

fn write_body<W: Write>(w: &mut W, route: Option<ShardCacheRoute>, key: &[u8]) -> Result<()> {
    if let Some(route) = route {
        route.write_to(w)?;
    } else {
        w.write_all(&hash_key(key).to_le_bytes())?;
    }
    w.write_all(&(key.len() as u32).to_le_bytes())?;
    w.write_all(key)?;
    Ok(())
}
