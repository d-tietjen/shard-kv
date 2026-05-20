use std::io::Write;

use crate::commands::FcnpCommand;
use crate::connection::FcnpConnection;
use crate::error::Result;

pub(crate) struct RespCommand<'parts, 'out> {
    parts: &'parts [&'parts [u8]],
    out: &'out mut Vec<u8>,
}

impl<'parts, 'out> RespCommand<'parts, 'out> {
    pub(crate) fn new(parts: &'parts [&'parts [u8]], out: &'out mut Vec<u8>) -> Self {
        Self { parts, out }
    }
}

impl FcnpCommand for RespCommand<'_, '_> {
    type Output = bool;

    const NAME: &'static str = "RESP";
    const OPCODE: u8 = 200;

    fn body_len(&self) -> usize {
        4 + self
            .parts
            .iter()
            .map(|part| 4usize.saturating_add(part.len()))
            .sum::<usize>()
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        w.write_all(&(self.parts.len() as u32).to_le_bytes())?;
        for part in self.parts {
            w.write_all(&(part.len() as u32).to_le_bytes())?;
            w.write_all(part)?;
        }
        Ok(())
    }

    fn read_response(self, conn: &mut FcnpConnection) -> Result<Self::Output> {
        conn.read_value(Self::NAME, self.out)
    }
}
