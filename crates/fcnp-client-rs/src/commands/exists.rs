use std::io::Write;

use crate::commands::{FcnpCommand, common};
use crate::connection::FcnpConnection;
use crate::error::Result;
use crate::routing::FcnpRoute;

pub(crate) struct Exists<'key> {
    key: &'key [u8],
    route: Option<FcnpRoute>,
}

impl<'key> Exists<'key> {
    pub(crate) fn new(key: &'key [u8]) -> Self {
        Self { key, route: None }
    }

    pub(crate) fn routed(route: FcnpRoute, key: &'key [u8]) -> Self {
        Self {
            key,
            route: Some(route),
        }
    }
}

impl FcnpCommand for Exists<'_> {
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

    fn read_response(self, conn: &mut FcnpConnection) -> Result<Self::Output> {
        conn.read_integer(Self::NAME).map(|exists| exists != 0)
    }
}

pub(crate) fn write_request(
    conn: &mut FcnpConnection,
    route: Option<FcnpRoute>,
    key: &[u8],
) -> Result<()> {
    conn.write_header(
        <Exists as FcnpCommand>::OPCODE,
        common::flags(route),
        common::key_body_len(route, key.len()) as u32,
    )?;
    common::write_key_body(&mut conn.w, route, key)
}
