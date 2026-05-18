use super::*;
use crate::server::commands::BorrowedCommandContext;

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[allow(dead_code)]
    pub(in crate::server) fn shared_execute_borrowed(
        store: &EmbeddedStore,
        command: BorrowedCommand<'_>,
        now_ms: u64,
    ) -> Frame {
        command.execute_borrowed_frame(store, now_ms)
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn borrowed_command_mutates_value(command: &BorrowedCommand<'_>) -> bool {
        command.mutates_value()
    }
}

/// SAFETY: caller must guarantee this is the only thread accessing the store.
impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline]
    pub(in crate::server) unsafe fn shared_execute_borrowed_into_single_threaded(
        store: &EmbeddedStore,
        command: BorrowedCommand<'_>,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        _started_at: Instant,
    ) {
        command.execute_borrowed(BorrowedCommandContext {
            store,
            out,
            fast_write_queue,
            single_threaded: true,
        });
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline]
    pub(in crate::server) fn shared_execute_borrowed_into(
        store: &EmbeddedStore,
        command: BorrowedCommand<'_>,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        _started_at: Instant,
    ) {
        command.execute_borrowed(BorrowedCommandContext {
            store,
            out,
            fast_write_queue,
            single_threaded: false,
        });
    }
}
