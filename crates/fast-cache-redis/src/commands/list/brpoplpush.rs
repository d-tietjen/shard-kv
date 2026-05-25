use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, write_frame, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BRPopLPush, "BRPOPLPUSH", true);

impl crate::commands::redis::RedisCommand for BRPopLPush {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [source, dest, _timeout] => {
                crate::commands::lmove::move_between_lists(store, source, dest, false, true)
            }
            _ => wrong_arity("BRPOPLPUSH"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [source, dest, _timeout] => {
                crate::commands::lmove::write_move_between_lists_resp(
                    store, source, dest, false, true, out,
                );
            }
            _ => write_frame(out, &wrong_arity("BRPOPLPUSH")),
        }
    }
}
