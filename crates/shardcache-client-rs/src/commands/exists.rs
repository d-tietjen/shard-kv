use std::io::Write;

use crate::commands::{ScnpCommand, common};
use crate::connection::ScnpConnection;
use crate::error::Result;
use crate::routing::ShardCacheRoute;

pub(crate) struct Exists<'key> {
    key: &'key [u8],
    route: Option<ShardCacheRoute>,
}

impl<'key> Exists<'key> {
    pub(crate) fn new(key: &'key [u8]) -> Self {
        Self { key, route: None }
    }

    pub(crate) fn routed(route: ShardCacheRoute, key: &'key [u8]) -> Self {
        Self {
            key,
            route: Some(route),
        }
    }
}

impl ScnpCommand for Exists<'_> {
    type Output = bool;

    const NAME: &'static str = "EXISTS";
    const OPCODE: u8 = 6;

    fn flags(&self) -> u8 {
        common::flags(self.route)
    }

    fn body_len(&self) -> usize {
        common::key_body_len(self.route, self.key.len())
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        common::write_key_body(w, self.route, self.key)
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        conn.read_integer(Self::NAME).map(|exists| exists != 0)
    }
}

pub(crate) fn write_request(
    conn: &mut ScnpConnection,
    route: Option<ShardCacheRoute>,
    key: &[u8],
) -> Result<()> {
    conn.write_header(
        <Exists as ScnpCommand>::OPCODE,
        common::flags(route),
        common::key_body_len(route, key.len()) as u32,
    )?;
    common::write_key_body(&mut conn.w, route, key)
}
