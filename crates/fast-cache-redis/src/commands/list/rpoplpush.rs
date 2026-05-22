use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, write_frame, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(RPopLPush, "RPOPLPUSH", true);

impl crate::commands::redis::RedisCommand for RPopLPush {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [source, dest] => {
                crate::commands::lmove::move_between_lists(store, source, dest, false, true)
            }
            _ => wrong_arity("RPOPLPUSH"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [source, dest] => {
                crate::commands::lmove::write_move_between_lists_resp(
                    store, source, dest, false, true, out,
                );
            }
            _ => write_frame(out, &wrong_arity("RPOPLPUSH")),
        }
    }
}
