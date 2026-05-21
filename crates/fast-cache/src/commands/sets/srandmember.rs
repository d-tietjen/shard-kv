#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, frame_from_result, parse_i64, wrong_arity,
};
#[cfg(feature = "server")]
use crate::commands::redis::{finish_object_array_visit, write_frame, write_object_array_item};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(SRandMember, "SRANDMEMBER", false);

impl crate::commands::redis::RedisCommand for SRandMember {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => frame_from_result(store.srandmember(key, None)),
            [key, count] => match parse_i64(count) {
                Ok(count) => frame_from_result(store.srandmember(key, Some(count))),
                Err(_) => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("SRANDMEMBER"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => write_frame(out, &frame_from_result(store.srandmember(key, None))),
            [key, count] => match parse_i64(count) {
                Ok(count) if count >= 0 => {
                    let outcome = store.srandmember_positive_visit(key, count as usize, |item| {
                        write_object_array_item(out, item)
                    });
                    finish_object_array_visit(out, outcome);
                }
                Ok(count) => {
                    write_frame(out, &frame_from_result(store.srandmember(key, Some(count))))
                }
                Err(_) => write_frame(out, &error("ERR value is not an integer or out of range")),
            },
            _ => write_frame(out, &wrong_arity("SRANDMEMBER")),
        }
    }
}
