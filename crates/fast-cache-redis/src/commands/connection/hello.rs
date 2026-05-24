#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{bulk, define_redis_command, int, wrong_arity};
#[cfg(feature = "server")]
use crate::commands::redis::{write_frame, write_resp_array_header};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Hello, "HELLO", false);

impl crate::commands::redis::RedisCommand for Hello {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() > 1 {
            return wrong_arity("HELLO");
        }
        Frame::Array(vec![
            bulk(b"server".to_vec()),
            bulk(b"fast-cache".to_vec()),
            bulk(b"version".to_vec()),
            bulk(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
            bulk(b"proto".to_vec()),
            int(2),
            bulk(b"id".to_vec()),
            int(0),
            bulk(b"mode".to_vec()),
            bulk(b"standalone".to_vec()),
            bulk(b"role".to_vec()),
            bulk(b"master".to_vec()),
            bulk(b"modules".to_vec()),
            Frame::Array(Vec::new()),
        ])
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() > 1 {
            write_frame(out, &wrong_arity("HELLO"));
            return;
        }

        write_resp_array_header(out, 14);
        ServerWire::write_resp_blob_string(out, b"server");
        ServerWire::write_resp_blob_string(out, b"fast-cache");
        ServerWire::write_resp_blob_string(out, b"version");
        ServerWire::write_resp_blob_string(out, env!("CARGO_PKG_VERSION").as_bytes());
        ServerWire::write_resp_blob_string(out, b"proto");
        ServerWire::write_resp_integer(out, 2);
        ServerWire::write_resp_blob_string(out, b"id");
        ServerWire::write_resp_integer(out, 0);
        ServerWire::write_resp_blob_string(out, b"mode");
        ServerWire::write_resp_blob_string(out, b"standalone");
        ServerWire::write_resp_blob_string(out, b"role");
        ServerWire::write_resp_blob_string(out, b"master");
        ServerWire::write_resp_blob_string(out, b"modules");
        write_resp_array_header(out, 0);
    }
}
