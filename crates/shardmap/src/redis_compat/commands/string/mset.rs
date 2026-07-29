#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, simple, write_resp_simple_string, write_resp_wrong_arity,
    wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(MSet, "MSET", true);

impl crate::commands::redis::RedisCommand for MSet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.is_empty() || !args.len().is_multiple_of(2) {
            return wrong_arity("MSET");
        }
        if args
            .chunks_exact(2)
            .any(|pair| !store.point_mutation_is_accepted(pair[0], pair[1].len(), None))
        {
            return error("ERR mutation rejected by an installed storage extension");
        }
        for pair in args.chunks_exact(2) {
            store.set(pair[0].to_vec(), pair[1].to_vec(), None);
        }
        simple("OK")
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.is_empty() || !args.len().is_multiple_of(2) {
            write_resp_wrong_arity(out, "MSET");
            return;
        }
        if args
            .chunks_exact(2)
            .any(|pair| !store.point_mutation_is_accepted(pair[0], pair[1].len(), None))
        {
            ServerWire::write_resp_error(
                out,
                "ERR mutation rejected by an installed storage extension",
            );
            return;
        }
        for pair in args.chunks_exact(2) {
            store.set(pair[0].to_vec(), pair[1].to_vec(), None);
        }
        write_resp_simple_string(out, "OK");
    }
}
