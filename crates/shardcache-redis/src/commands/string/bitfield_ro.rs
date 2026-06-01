#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::bitfield::bitfield_frame;
#[cfg(feature = "server")]
use crate::commands::bitfield::write_bitfield_resp;
use crate::commands::redis::define_redis_command;
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

define_redis_command!(BitFieldRo, "BITFIELD_RO", false);

impl crate::commands::redis::RedisCommand for BitFieldRo {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        bitfield_frame(store, args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_bitfield_resp(store, args, true, out);
    }
}
