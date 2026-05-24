#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, frame_from_result, parse_i64,
    write_resp_wrong_arity, write_result_resp, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(HRandField, "HRANDFIELD", false);

impl crate::commands::redis::RedisCommand for HRandField {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.is_empty() || args.len() > 3 {
            return wrong_arity("HRANDFIELD");
        }
        let count = match args.get(1) {
            Some(value) => match parse_i64(value) {
                Ok(value) => Some(value),
                Err(_) => return error("ERR value is not an integer or out of range"),
            },
            None => None,
        };
        let with_values = args
            .get(2)
            .is_some_and(|value| eq_ignore_ascii_case(value, b"WITHVALUES"));
        frame_from_result(store.hrandfield(args[0], count, with_values))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.is_empty() || args.len() > 3 {
            write_resp_wrong_arity(out, "HRANDFIELD");
            return;
        }
        let count = match args.get(1) {
            Some(value) => match parse_i64(value) {
                Ok(value) => Some(value),
                Err(_) => {
                    ServerWire::write_resp_error(
                        out,
                        "ERR value is not an integer or out of range",
                    );
                    return;
                }
            },
            None => None,
        };
        let with_values = args
            .get(2)
            .is_some_and(|value| eq_ignore_ascii_case(value, b"WITHVALUES"));
        write_result_resp(out, store.hrandfield(args[0], count, with_values));
    }
}
