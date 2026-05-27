use bytes::BytesMut;

use crate::commands::redis::{
    bulk, define_redis_command, error, write_frame, write_resp_null, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Object, "OBJECT", false);

impl crate::commands::redis::RedisCommand for Object {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [subcommand, key] if subcommand.eq_ignore_ascii_case(b"ENCODING") => {
                match store.object_encoding(key) {
                    Some(encoding) => bulk(encoding.as_bytes().to_vec()),
                    None => Frame::Null,
                }
            }
            [_, _] => error("ERR syntax error"),
            _ => wrong_arity("OBJECT"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [subcommand, key] if subcommand.eq_ignore_ascii_case(b"ENCODING") => {
                match store.object_encoding(key) {
                    Some(encoding) => ServerWire::write_resp_blob_string(out, encoding.as_bytes()),
                    None => write_resp_null(out),
                }
            }
            [_, _] => ServerWire::write_resp_error(out, "ERR syntax error"),
            _ => write_frame(out, &wrong_arity("OBJECT")),
        }
    }
}
