use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, int, write_frame, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Touch, "TOUCH", false);

impl crate::commands::redis::RedisCommand for Touch {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.is_empty() {
            return wrong_arity("TOUCH");
        }
        int(args.iter().filter(|key| store.exists(key)).count() as i64)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.is_empty() {
            write_frame(out, &wrong_arity("TOUCH"));
            return;
        }
        ServerWire::write_resp_integer(
            out,
            args.iter().filter(|key| store.exists(key)).count() as i64,
        );
    }
}
