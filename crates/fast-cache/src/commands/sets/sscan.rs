#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, scan_from_result, wrong_arity};
#[cfg(feature = "server")]
use crate::commands::redis::{
    finish_scan_object_array_visit, write_frame, write_scan_object_array_item,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(SScan, "SSCAN", false);

impl crate::commands::redis::RedisCommand for SScan {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 2 {
            return wrong_arity("SSCAN");
        }
        scan_from_result(store.smembers(args[0]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 2 {
            write_frame(out, &wrong_arity("SSCAN"));
            return;
        }
        let outcome = store.smembers_visit(args[0], |item| write_scan_object_array_item(out, item));
        finish_scan_object_array_visit(out, outcome);
    }
}
