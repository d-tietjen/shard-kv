use crate::storage::RedisHashStore;
#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::{
    FastObjectArrayWriter, finish_object_array_visit, write_frame, write_object_array_item,
    wrong_arity,
};
use crate::commands::redis::{define_redis_command, object_result};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(HKeys, "HKEYS", false);

impl crate::commands::redis::RedisCommand for HKeys {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        object_result("HKEYS", args, 1, || store.hkeys(args[0]))
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => {
                let outcome = store.hkeys_visit(key, |item| write_object_array_item(out, item));
                finish_object_array_visit(out, outcome);
            }
            _ => write_frame(out, &wrong_arity("HKEYS")),
        }
    }

    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => {
                let mut writer = FastObjectArrayWriter::new(out);
                let outcome = store.hkeys_visit(key, |item| writer.write(item));
                writer.finish(outcome);
            }
            _ => ServerWire::write_fast_error(
                out,
                "ERR wrong number of arguments for 'hkeys' command",
            ),
        }
    }
}
