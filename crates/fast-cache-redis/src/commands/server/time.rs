use crate::commands::redis::{bulk, define_redis_command, wrong_arity};
use crate::protocol::Frame;
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
}
