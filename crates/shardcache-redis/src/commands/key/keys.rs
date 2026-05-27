#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{array_bulk, define_redis_command, filter_key_pattern, wrong_arity};
#[cfg(feature = "server")]
use crate::commands::redis::{key_pattern_matches, write_frame, write_resp_array_header};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Keys, "KEYS", false);

impl crate::commands::redis::RedisCommand for Keys {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [pattern] => array_bulk(filter_key_pattern(store.key_snapshot_unsorted(), pattern)),
            _ => wrong_arity("KEYS"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [pattern] => write_keys_resp(store, pattern, out),
            _ => write_frame(out, &wrong_arity("KEYS")),
        }
    }
}

#[cfg(feature = "server")]
fn write_keys_resp(store: &EmbeddedStore, pattern: &[u8], out: &mut BytesMut) {
    let mut items = BytesMut::new();
    let mut count = 0usize;
    store.visit_redis_keys(&mut |key| {
        if key_pattern_matches(key, pattern) {
            ServerWire::write_resp_blob_string(&mut items, key);
            count += 1;
        }
        true
    });

    write_resp_array_header(out, count);
    out.extend_from_slice(&items);
}
