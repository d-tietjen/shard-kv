#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{define_redis_command, int, write_resp_wrong_arity, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(MSetNx, "MSETNX", true);

impl crate::commands::redis::RedisCommand for MSetNx {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.is_empty() || !args.len().is_multiple_of(2) {
            return wrong_arity("MSETNX");
        }
        if args.chunks_exact(2).any(|pair| store.exists(pair[0])) {
            return int(0);
        }
        for pair in args.chunks_exact(2) {
            store.set(pair[0].to_vec(), pair[1].to_vec(), None);
        }
        int(1)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.is_empty() || !args.len().is_multiple_of(2) {
            write_resp_wrong_arity(out, "MSETNX");
            return;
        }
        if args.chunks_exact(2).any(|pair| store.exists(pair[0])) {
            ServerWire::write_resp_integer(out, 0);
            return;
        }
        for pair in args.chunks_exact(2) {
            store.set(pair[0].to_vec(), pair[1].to_vec(), None);
        }
        ServerWire::write_resp_integer(out, 1);
    }
}
