use crate::commands::redis::{define_redis_command, error, int, parse_i64, wrong_arity};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(WaitAof, "WAITAOF", false);

impl crate::commands::redis::RedisCommand for WaitAof {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        // WAITAOF numlocal numreplicas timeout
        let [numlocal, numreplicas, timeout] = args else {
            return wrong_arity("WAITAOF");
        };

        let numlocal = match parse_i64(numlocal) {
            Ok(value) => value,
            Err(_) => return error("ERR value is not an integer or out of range"),
        };
        // numreplicas and timeout are validated as integers but otherwise unused:
        // this server has no replicas and never blocks.
        if parse_i64(numreplicas).is_err() || parse_i64(timeout).is_err() {
            return error("ERR value is not an integer or out of range");
        }

        if !(0..=1).contains(&numlocal) {
            return error("ERR WAITAOF numlocal value should be 0 or 1.");
        }
        // Persistence is disabled in this server, so a local AOF fsync can never
        // be acknowledged. Redis returns this exact error in that case.
        if numlocal == 1 {
            return error(
                "ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.",
            );
        }

        // [numlocal acked, numreplicas acked]; no AOF and no replicas => [0, 0].
        Frame::Array(vec![int(0), int(0)])
    }
}
