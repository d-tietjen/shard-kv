use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, write_frame, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BLMove, "BLMOVE", true);

impl crate::commands::redis::RedisCommand for BLMove {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [source, dest, source_side, dest_side, _timeout] => {
                crate::commands::lmove::execute_lmove(store, source, dest, source_side, dest_side)
            }
            _ => wrong_arity("BLMOVE"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [source, dest, source_side, dest_side, _timeout] => {
                crate::commands::lmove::write_lmove_resp(
                    store,
                    source,
                    dest,
                    source_side,
                    dest_side,
                    out,
                );
            }
            _ => write_frame(out, &wrong_arity("BLMOVE")),
        }
    }
}
