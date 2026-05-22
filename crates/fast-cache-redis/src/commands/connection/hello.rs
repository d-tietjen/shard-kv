use crate::commands::redis::{bulk, define_redis_command, int, wrong_arity};
use crate::protocol::Frame;
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
}
