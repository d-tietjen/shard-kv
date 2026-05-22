use crate::storage::RedisZSetStore;
#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, object_result, write_frame, wrong_arity, wrongtype,
};
use crate::commands::zset_shared::write_resp_score;
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectError};

define_redis_command!(ZScore, "ZSCORE", false);

impl crate::commands::redis::RedisCommand for ZScore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("ZSCORE", args, 2, || store.zscore(args[0], args[1]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, member] => match store.zscore_value(key, member) {
                Ok(Some(score)) => write_resp_score(out, score),
                Ok(None) | Err(RedisObjectError::MissingKey) => out.extend_from_slice(b"$-1\r\n"),
                Err(RedisObjectError::WrongType) => write_frame(out, &wrongtype()),
            },
            _ => write_frame(out, &wrong_arity("ZSCORE")),
        }
    }
}
