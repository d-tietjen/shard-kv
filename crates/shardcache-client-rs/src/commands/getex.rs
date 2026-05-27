use std::io::Write;

use crate::commands::{ScnpCommand, common};
use crate::connection::ScnpConnection;
use crate::error::Result;
use crate::routing::ShardCacheRoute;

pub(crate) struct GetEx<'key, 'out> {
    key: &'key [u8],
    ttl_ms: u64,
    out: &'out mut Vec<u8>,
    route: Option<ShardCacheRoute>,
}

impl<'key, 'out> GetEx<'key, 'out> {
    pub(crate) fn new(key: &'key [u8], ttl_ms: u64, out: &'out mut Vec<u8>) -> Self {
        Self {
            key,
            ttl_ms,
            out,
            route: None,
        }
    }

    pub(crate) fn routed(
        route: ShardCacheRoute,
        key: &'key [u8],
        ttl_ms: u64,
        out: &'out mut Vec<u8>,
    ) -> Self {
        Self {
            key,
            ttl_ms,
            out,
            route: Some(route),
        }
    }
}

impl ScnpCommand for GetEx<'_, '_> {
    type Output = bool;

    const NAME: &'static str = "GETEX";
    const OPCODE: u8 = 4;

    fn flags(&self) -> u8 {
        common::flags(self.route)
    }

    fn body_len(&self) -> usize {
        common::ttl_key_body_len(self.route, self.key.len())
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        common::write_ttl_key_body(w, self.route, self.key, self.ttl_ms)
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        conn.read_value(Self::NAME, self.out)
    }
}

pub(crate) fn write_request(
    conn: &mut ScnpConnection,
    route: Option<ShardCacheRoute>,
    key: &[u8],
    ttl_ms: u64,
) -> Result<()> {
    conn.write_header(
        <GetEx as ScnpCommand>::OPCODE,
        common::flags(route),
        common::ttl_key_body_len(route, key.len()) as u32,
    )?;
    common::write_ttl_key_body(&mut conn.w, route, key, ttl_ms)
}
