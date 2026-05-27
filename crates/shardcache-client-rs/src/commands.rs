//! Per-command SCNP request implementations.
//!
//! New commands should live in their own `commands/*.rs` file and implement
//! [`ScnpCommand`]. The connection layer only knows how to send a command and
//! read its typed response through that trait.

use std::io::Write;

use crate::connection::ScnpConnection;
use crate::error::Result;

pub(crate) mod common;
pub(crate) mod del;
pub(crate) mod exists;
pub(crate) mod expire;
pub(crate) mod get;
pub(crate) mod getex;
#[cfg(feature = "redis")]
pub(crate) mod redis;
pub(crate) mod resp;
pub(crate) mod set;
pub(crate) mod setex;
pub(crate) mod ttl;

pub(crate) trait ScnpCommand {
    type Output;

    const NAME: &'static str;
    const OPCODE: u8;

    fn opcode(&self) -> u8 {
        Self::OPCODE
    }

    fn flags(&self) -> u8 {
        0
    }

    fn body_len(&self) -> usize;

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()>;

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output>;
}
