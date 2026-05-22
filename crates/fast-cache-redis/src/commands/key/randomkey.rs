use bytes::BytesMut;

use crate::commands::redis::{bulk, define_redis_command, write_frame, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{DEFAULT_SCAN_COUNT, EmbeddedStore, RedisKeyScanType, now_millis};

define_redis_command!(RandomKey, "RANDOMKEY", false);

impl crate::commands::redis::RedisCommand for RandomKey {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if !args.is_empty() {
            return wrong_arity("RANDOMKEY");
        }
        match random_key_bytes(store) {
            Some(key) => bulk(key),
            None => Frame::Null,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if !args.is_empty() {
            write_frame(out, &wrong_arity("RANDOMKEY"));
            return;
        }
        match random_key_bytes(store) {
            Some(key) => ServerWire::write_resp_blob_string(out, &key),
            None => out.extend_from_slice(b"$-1\r\n"),
        }
    }
}

fn random_key_bytes(store: &EmbeddedStore) -> Option<Vec<u8>> {
    let shard_count = store.shard_count();
    let start = (now_millis() as usize) % shard_count.max(1);
    for offset in 0..shard_count {
        let shard_id = (start + offset) % shard_count;
        let mut found = None;
        let _ = store.scan_redis_keys_in_shard_visit(
            shard_id,
            0,
            DEFAULT_SCAN_COUNT.min(1),
            RedisKeyScanType::All,
            &mut |key| {
                found = Some(key.to_vec());
                true
            },
        );
        if found.is_some() {
            return found;
        }
    }
    None
}
