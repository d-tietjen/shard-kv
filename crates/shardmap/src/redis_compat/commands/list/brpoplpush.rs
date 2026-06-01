use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, write_frame, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BRPopLPush, "BRPOPLPUSH", true);

impl crate::commands::redis::RedisCommand for BRPopLPush {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [source, dest, timeout] => execute_brpoplpush(store, source, dest, timeout),
            _ => wrong_arity("BRPOPLPUSH"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [source, dest, timeout] => {
                write_frame(out, &execute_brpoplpush(store, source, dest, timeout));
            }
            _ => write_frame(out, &wrong_arity("BRPOPLPUSH")),
        }
    }
}

fn execute_brpoplpush(store: &EmbeddedStore, source: &[u8], dest: &[u8], timeout: &[u8]) -> Frame {
    let timeout = match crate::commands::blocking::parse_blocking_timeout(timeout) {
        Ok(timeout) => timeout,
        Err(frame) => return frame,
    };
    let keys = [source];
    let shard_id = match crate::commands::blocking::single_shard_for_keys(store, &keys) {
        Ok(shard_id) => shard_id,
        Err(frame) => return frame,
    };
    crate::commands::blocking::block_on_shard(store, shard_id, timeout, || {
        crate::commands::lmove::move_between_lists(store, source, dest, false, true)
    })
}
