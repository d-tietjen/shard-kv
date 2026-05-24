#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    FastObjectArrayWriter, define_redis_command, frame_from_result, write_object_array_item,
    write_resp_array_header, write_resp_null, write_resp_wrong_arity, write_resp_wrongtype,
    wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectReadOutcome};

define_redis_command!(HMGet, "HMGET", false);

impl crate::commands::redis::RedisCommand for HMGet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() < 2 {
            return wrong_arity("HMGET");
        }
        frame_from_result(store.hmget(args[0], &args[1..]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 2 {
            write_resp_wrong_arity(out, "HMGET");
            return;
        }
        let fields = &args[1..];
        match store.hmget_visit(args[0], fields, |item| write_object_array_item(out, item)) {
            RedisObjectReadOutcome::Written => {}
            RedisObjectReadOutcome::Missing => {
                write_resp_array_header(out, fields.len());
                for _ in fields {
                    write_resp_null(out);
                }
            }
            RedisObjectReadOutcome::WrongType => write_resp_wrongtype(out),
        }
    }

    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() < 2 {
            ServerWire::write_fast_error(out, "ERR wrong number of arguments for 'hmget' command");
            return;
        }
        let fields = &args[1..];
        let mut writer = FastObjectArrayWriter::new(out);
        match store.hmget_visit(args[0], fields, |item| writer.write(item)) {
            RedisObjectReadOutcome::Written => writer.finish(RedisObjectReadOutcome::Written),
            RedisObjectReadOutcome::Missing => {
                let out = writer.into_inner();
                let start = ServerWire::begin_fast_array(out, fields.len());
                for _ in fields {
                    ServerWire::write_fast_array_item(out, None);
                }
                ServerWire::finish_fast_array(out, start);
            }
            RedisObjectReadOutcome::WrongType => {
                writer.finish(RedisObjectReadOutcome::WrongType);
            }
        }
    }
}
