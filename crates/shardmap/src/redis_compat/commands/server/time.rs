#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{bulk, define_redis_command, wrong_arity};
#[cfg(feature = "server")]
use crate::commands::redis::{write_frame, write_resp_array_header};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Time, "TIME", false);

impl crate::commands::redis::RedisCommand for Time {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if !args.is_empty() {
            return wrong_arity("TIME");
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Frame::Array(vec![
            bulk(now.as_secs().to_string().into_bytes()),
            bulk(now.subsec_micros().to_string().into_bytes()),
        ])
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if !args.is_empty() {
            write_frame(out, &wrong_arity("TIME"));
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let mut seconds = itoa::Buffer::new();
        let mut micros = itoa::Buffer::new();
        write_resp_array_header(out, 2);
        ServerWire::write_resp_blob_string(out, seconds.format(now.as_secs()).as_bytes());
        ServerWire::write_resp_blob_string(out, micros.format(now.subsec_micros()).as_bytes());
    }
}
