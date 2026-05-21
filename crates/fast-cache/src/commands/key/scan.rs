#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, key_pattern_matches, parse_key_scan_args, scan_array_with_cursor,
};
#[cfg(feature = "server")]
use crate::commands::redis::{write_frame, write_scan_resp_from_options};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(Scan, "SCAN", false);

impl crate::commands::redis::RedisCommand for Scan {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match parse_key_scan_args(args, "SCAN") {
            Ok(options) => {
                let mut keys = Vec::with_capacity(options.count.min(1024));
                let result = store.scan_redis_keys_visit(
                    options.cursor,
                    options.count,
                    options.scan_type(),
                    &mut |key| {
                        if key_pattern_matches(key, options.pattern) {
                            keys.push(key.to_vec());
                            true
                        } else {
                            false
                        }
                    },
                );
                scan_array_with_cursor(result.cursor, keys)
            }
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match parse_key_scan_args(args, "SCAN") {
            Ok(options) => write_scan_resp_from_options(store, options, out),
            Err(frame) => write_frame(out, &frame),
        }
    }
}
