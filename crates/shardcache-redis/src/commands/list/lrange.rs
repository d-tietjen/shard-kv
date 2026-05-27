use crate::storage::RedisListStore;
#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::FastObjectArrayWriter;
use crate::commands::redis::{
    define_redis_command, error, finish_object_array_visit, frame_from_result, parse_i64,
    write_frame, write_object_array_item, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(LRange, "LRANGE", false);

impl crate::commands::redis::RedisCommand for LRange {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, start, stop] => match (parse_i64(start), parse_i64(stop)) {
                (Ok(start), Ok(stop)) => frame_from_result(store.lrange(key, start, stop)),
                _ => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_arity("LRANGE"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, start, stop] => {
                let Ok(start) = parse_i64(start) else {
                    write_frame(out, &error("ERR value is not an integer or out of range"));
                    return;
                };
                let Ok(stop) = parse_i64(stop) else {
                    write_frame(out, &error("ERR value is not an integer or out of range"));
                    return;
                };
                let outcome =
                    store.lrange_visit(key, start, stop, |item| write_object_array_item(out, item));
                finish_object_array_visit(out, outcome);
            }
            _ => write_frame(out, &wrong_arity("LRANGE")),
        }
    }

    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, start, stop] => {
                let Ok(start) = parse_i64(start) else {
                    ServerWire::write_fast_error(
                        out,
                        "ERR value is not an integer or out of range",
                    );
                    return;
                };
                let Ok(stop) = parse_i64(stop) else {
                    ServerWire::write_fast_error(
                        out,
                        "ERR value is not an integer or out of range",
                    );
                    return;
                };
                let mut writer = FastObjectArrayWriter::new(out);
                let outcome = store.lrange_visit(key, start, stop, |item| writer.write(item));
                writer.finish(outcome);
            }
            _ => ServerWire::write_fast_error(
                out,
                "ERR wrong number of arguments for 'lrange' command",
            ),
        }
    }
}
