#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_frame;
use crate::commands::redis::{bulk, define_redis_command, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Info, "INFO", false);

impl crate::commands::redis::RedisCommand for Info {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.len() > 1 {
            return wrong_arity("INFO");
        }
        bulk(
            format!(
                "# Server\r\nredis_version:{}\r\nshardcache_version:{}\r\n# Keyspace\r\ndb0:keys={},expires=0,avg_ttl=0\r\n",
                env!("CARGO_PKG_VERSION"),
                env!("CARGO_PKG_VERSION"),
                store.len()
            )
            .into_bytes(),
        )
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.len() > 1 {
            write_frame(out, &wrong_arity("INFO"));
            return;
        }
        let payload = format!(
            "# Server\r\nredis_version:{}\r\nshardcache_version:{}\r\n# Keyspace\r\ndb0:keys={},expires=0,avg_ttl=0\r\n",
            env!("CARGO_PKG_VERSION"),
            env!("CARGO_PKG_VERSION"),
            store.len()
        );
        ServerWire::write_resp_blob_string(out, payload.as_bytes());
    }
}
