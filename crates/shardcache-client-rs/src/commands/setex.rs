use std::io::Write;

use crate::commands::{ScnpCommand, common};
use crate::connection::ScnpConnection;
use crate::error::Result;
use crate::routing::ShardCacheRoute;

pub(crate) struct SetEx<'a> {
    key: &'a [u8],
    value: &'a [u8],
    ttl_ms: u64,
    route: Option<ShardCacheRoute>,
}

impl<'a> SetEx<'a> {
    pub(crate) fn new(key: &'a [u8], value: &'a [u8], ttl_ms: u64) -> Self {
        Self {
            key,
            value,
            ttl_ms,
            route: None,
        }
    }

    pub(crate) fn routed(
        route: ShardCacheRoute,
        key: &'a [u8],
        value: &'a [u8],
        ttl_ms: u64,
    ) -> Self {
        Self {
            key,
            value,
            ttl_ms,
            route: Some(route),
        }
    }
}

impl ScnpCommand for SetEx<'_> {
    type Output = ();

    const NAME: &'static str = "SETEX";
    const OPCODE: u8 = 3;

    fn flags(&self) -> u8 {
        common::flags(self.route)
    }

    fn body_len(&self) -> usize {
        common::ttl_key_value_body_len(self.route, self.key.len(), self.value.len())
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        common::write_ttl_key_value_body(w, self.route, self.key, self.value, self.ttl_ms)
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
    ttl_ms: u64,
) -> Result<()> {
    conn.write_header(
        <SetEx as ScnpCommand>::OPCODE,
        common::flags(route),
        common::ttl_key_value_body_len(route, key.len(), value.len()) as u32,
    )?;
    common::write_ttl_key_value_body(&mut conn.w, route, key, value, ttl_ms)
}
