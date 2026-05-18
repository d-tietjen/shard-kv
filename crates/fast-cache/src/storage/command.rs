use crate::Result;
use crate::commands::{BorrowedCommandBox, CommandCatalog, OwnedCommandBox};
use crate::protocol::{BorrowedCommandFrame, Frame, RespCodec};

#[derive(Debug)]
pub struct Command {
    inner: OwnedCommandBox,
}

impl Command {
    pub(crate) fn new(inner: OwnedCommandBox) -> Self {
        Self { inner }
    }

    pub fn from_frame(frame: Frame) -> Result<Self> {
        let parts = RespCodec::as_command(frame)?.parts;
        CommandCatalog::parse_owned(&parts)
    }

    pub fn name(&self) -> &'static str {
        self.inner.name()
    }

    pub fn mutates_value(&self) -> bool {
        self.inner.mutates_value()
    }

    pub fn route_key(&self) -> Option<&[u8]> {
        self.inner.route_key()
    }

    pub(crate) fn to_borrowed_command(&self) -> BorrowedCommand<'_> {
        BorrowedCommand::new(self.inner.to_borrowed_command())
    }
}

#[derive(Debug)]
pub struct BorrowedCommand<'a> {
    inner: BorrowedCommandBox<'a>,
}

impl<'a> BorrowedCommand<'a> {
    pub(crate) fn new(inner: BorrowedCommandBox<'a>) -> Self {
        Self { inner }
    }

    pub fn from_frame(frame: BorrowedCommandFrame<'a>) -> Result<Self> {
        Self::from_parts(&frame.parts)
    }

    pub fn from_parts(parts: &[&'a [u8]]) -> Result<Self> {
        CommandCatalog::parse_borrowed(parts).map(Self::new)
    }

    pub fn name(&self) -> &'static str {
        self.inner.name()
    }

    pub fn mutates_value(&self) -> bool {
        self.inner.mutates_value()
    }

    pub fn route_key(&self) -> Option<&'a [u8]> {
        self.inner.route_key()
    }

    pub(crate) fn supports_spanned_resp(&self) -> bool {
        self.inner.supports_spanned_resp()
    }

    pub fn to_owned_command(&self) -> Command {
        self.inner.to_owned_command()
    }

    pub(crate) fn execute_engine<'b>(
        &'b self,
        ctx: crate::storage::EngineCommandContext<'b>,
    ) -> crate::storage::EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        self.inner.execute_engine(ctx)
    }

    #[cfg(feature = "server")]
    pub(crate) fn execute_borrowed_frame(
        &self,
        store: &crate::storage::EmbeddedStore,
        now_ms: u64,
    ) -> Frame {
        self.inner.execute_borrowed_frame(store, now_ms)
    }

    #[cfg(feature = "server")]
    pub(crate) fn execute_borrowed(
        &self,
        ctx: crate::server::commands::BorrowedCommandContext<'_, '_, '_>,
    ) {
        self.inner.execute_borrowed(ctx);
    }

    #[cfg(feature = "server")]
    pub(crate) fn execute_direct_borrowed(
        &self,
        ctx: crate::server::commands::DirectCommandContext,
    ) -> Frame {
        self.inner.execute_direct_borrowed(ctx)
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::CommandCatalog;
    use crate::protocol::Frame;

    use super::{BorrowedCommand, Command};

    #[test]
    fn parses_set_with_ttl() {
        let command = Command::from_frame(Frame::Array(vec![
            Frame::BlobString(b"SET".to_vec()),
            Frame::BlobString(b"alpha".to_vec()),
            Frame::BlobString(b"beta".to_vec()),
            Frame::BlobString(b"EX".to_vec()),
            Frame::BlobString(b"5".to_vec()),
        ]))
        .unwrap();

        assert_eq!(command.name(), "SET");
        assert!(command.mutates_value());
        assert_eq!(command.route_key(), Some(b"alpha".as_slice()));
    }

    #[test]
    fn parses_get_command() {
        let command =
            BorrowedCommand::from_parts(&[b"GET".as_slice(), b"alpha".as_slice()]).unwrap();

        assert_eq!(command.name(), "GET");
        assert!(!command.mutates_value());
        assert_eq!(command.route_key(), Some(b"alpha".as_slice()));
    }

    #[test]
    fn parses_del_command() {
        let command = BorrowedCommand::from_parts(&[
            b"DEL".as_slice(),
            b"alpha".as_slice(),
            b"beta".as_slice(),
        ])
        .unwrap();

        assert_eq!(command.name(), "DEL");
        assert!(command.mutates_value());
        assert_eq!(command.route_key(), Some(b"alpha".as_slice()));
        assert_eq!(
            command.to_owned_command().route_key(),
            Some(b"alpha".as_slice())
        );
    }

    #[test]
    fn parses_borrowed_command_object() {
        let command = CommandCatalog::parse_borrowed(&[
            b"SET".as_slice(),
            b"alpha".as_slice(),
            b"beta".as_slice(),
        ])
        .unwrap();

        assert_eq!(command.name(), "SET");
        assert!(command.mutates_value());
        assert_eq!(command.route_key(), Some(b"alpha".as_slice()));
        assert_eq!(
            command.to_owned_command().route_key(),
            Some(b"alpha".as_slice())
        );
    }
}
