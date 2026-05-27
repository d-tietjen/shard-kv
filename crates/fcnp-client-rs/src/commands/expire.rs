use std::io::Write;

use crate::commands::{FcnpCommand, common};
use crate::connection::FcnpConnection;
use crate::error::Result;
use crate::routing::FcnpRoute;

pub(crate) struct Expire<'key> {
    key: &'key [u8],
    ttl_ms: u64,
    route: Option<FcnpRoute>,
}

impl<'key> Expire<'key> {
    pub(crate) fn new(key: &'key [u8], ttl_ms: u64) -> Self {
        Self {
            key,
            ttl_ms,
            route: None,
        }
    }

    pub(crate) fn routed(route: FcnpRoute, key: &'key [u8], ttl_ms: u64) -> Self {
        Self {
            key,
            ttl_ms,
            route: Some(route),
        }
    }
}

impl FcnpCommand for Expire<'_> {
    type Output = bool;

    const NAME: &'static str = "EXPIRE";
    const OPCODE: u8 = 8;

    fn flags(&self) -> u8 {
        common::flags(self.route)
    }

    fn body_len(&self) -> usize {
        common::ttl_key_body_len(self.route, self.key.len())
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        common::write_ttl_key_body(w, self.route, self.key, self.ttl_ms)
    }

    fn read_response(self, conn: &mut FcnpConnection) -> Result<Self::Output> {
        conn.read_integer(Self::NAME).map(|changed| changed != 0)
    }
}

pub(crate) fn write_request(
    conn: &mut FcnpConnection,
    route: Option<FcnpRoute>,
    key: &[u8],
    ttl_ms: u64,
) -> Result<()> {
    conn.write_header(
        <Expire as FcnpCommand>::OPCODE,
        common::flags(route),
        common::ttl_key_body_len(route, key.len()) as u32,
    )?;
    common::write_ttl_key_body(&mut conn.w, route, key, ttl_ms)
}
