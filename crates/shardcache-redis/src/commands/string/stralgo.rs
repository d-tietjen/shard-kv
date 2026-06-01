use crate::commands::redis::{define_redis_command, eq_ignore_ascii_case, error, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(StrAlgo, "STRALGO", false);

impl crate::commands::redis::RedisCommand for StrAlgo {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [algorithm, rest @ ..] = args else {
            return wrong_arity("STRALGO");
        };
        match algorithm {
            algorithm if eq_ignore_ascii_case(algorithm, b"LCS") => {
                crate::commands::lcs::execute_lcs(store, rest)
            }
            _ => error("ERR unknown algorithm name. Try STRALGO LCS"),
        }
    }
}
