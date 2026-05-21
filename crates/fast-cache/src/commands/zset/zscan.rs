#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, reserve_resp_bulk_array_hint, scan_array, write_frame,
    write_resp_array_header, wrong_arity, wrongtype, zentries_flat,
};
use crate::commands::zset_shared::write_resp_score;
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{
    EmbeddedStore, RedisObjectError, RedisObjectReadOutcome, RedisObjectZSetRangeItem,
};

define_redis_command!(ZScan, "ZSCAN", false);

impl crate::commands::redis::RedisCommand for ZScan {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 2 {
            return wrong_arity("ZSCAN");
        }
        match store.zentries(args[0]) {
            Ok(entries) => scan_array(zentries_flat(entries)),
            Err(RedisObjectError::WrongType) => wrongtype(),
            Err(RedisObjectError::MissingKey) => scan_array(Vec::new()),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 2 {
            write_frame(out, &wrong_arity("ZSCAN"));
            return;
        }
        match store.zrange_entries_visit(args[0], 0, -1, false, |item| match item {
            RedisObjectZSetRangeItem::Begin(count) => {
                reserve_resp_bulk_array_hint(out, count.saturating_mul(2));
                write_resp_array_header(out, 2);
                ServerWire::write_resp_blob_string(out, b"0");
                write_resp_array_header(out, count.saturating_mul(2));
            }
            RedisObjectZSetRangeItem::Entry { member, score } => {
                ServerWire::write_resp_blob_string(out, member);
                write_resp_score(out, score);
            }
        }) {
            RedisObjectReadOutcome::Written => {}
            RedisObjectReadOutcome::Missing => {
                write_resp_array_header(out, 2);
                ServerWire::write_resp_blob_string(out, b"0");
                write_resp_array_header(out, 0);
            }
            RedisObjectReadOutcome::WrongType => write_frame(out, &wrongtype()),
        }
    }
}
