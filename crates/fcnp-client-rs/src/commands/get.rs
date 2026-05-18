use std::io::Write;

use crate::commands::FcnpCommand;
use crate::connection::FcnpConnection;
use crate::error::Result;
use crate::protocol::{FAST_FLAG_KEY_HASH, ROUTED_FLAGS};
use crate::routing::{FcnpRoute, hash_key};

pub(crate) struct Get<'key, 'out> {
    key: &'key [u8],
    out: &'out mut Vec<u8>,
    route: Option<FcnpRoute>,
}

impl<'key, 'out> Get<'key, 'out> {
    pub(crate) fn new(key: &'key [u8], out: &'out mut Vec<u8>) -> Self {
        Self {
            key,
            out,
            route: None,
        }
    }

    pub(crate) fn routed(route: FcnpRoute, key: &'key [u8], out: &'out mut Vec<u8>) -> Self {
        Self {
            key,
            out,
            route: Some(route),
        }
    }
}

impl FcnpCommand for Get<'_, '_> {
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

    fn read_response(self, conn: &mut FcnpConnection) -> Result<Self::Output> {
        conn.read_value(Self::NAME, self.out)
    }
}

pub(crate) fn write_request(
    conn: &mut FcnpConnection,
    route: Option<FcnpRoute>,
    key: &[u8],
) -> Result<()> {
    conn.write_header(
        <Get as FcnpCommand>::OPCODE,
        flags(route),
        body_len(route, key.len()) as u32,
    )?;
    write_body(&mut conn.w, route, key)
}

fn flags(route: Option<FcnpRoute>) -> u8 {
    route.map_or(FAST_FLAG_KEY_HASH, |_| ROUTED_FLAGS)
}

fn body_len(route: Option<FcnpRoute>, key_len: usize) -> usize {
    if route.is_some() {
        24 + key_len
    } else {
        12 + key_len
    }
}

fn write_body<W: Write>(w: &mut W, route: Option<FcnpRoute>, key: &[u8]) -> Result<()> {
    if let Some(route) = route {
        route.write_to(w)?;
    } else {
        w.write_all(&hash_key(key).to_le_bytes())?;
    }
    w.write_all(&(key.len() as u32).to_le_bytes())?;
    w.write_all(key)?;
    Ok(())
}
