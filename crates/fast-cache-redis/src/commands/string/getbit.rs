use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, write_frame, wrong_arity, wrongtype,
};
use crate::commands::string_bits::{parse_bit_offset, read_bit};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

define_redis_command!(GetBit, "GETBIT", false);

impl crate::commands::redis::RedisCommand for GetBit {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, offset] => {
                let Ok(offset) = parse_bit_offset(offset) else {
                    return error("ERR bit offset is not an integer or out of range");
                };
                match getbit_value(store, key, offset) {
                    Ok(value) => int(value),
                    Err(()) => wrongtype(),
                }
            }
            _ => wrong_arity("GETBIT"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, offset] => {
                let Ok(offset) = parse_bit_offset(offset) else {
                    ServerWire::write_resp_error(
                        out,
                        "ERR bit offset is not an integer or out of range",
                    );
                    return;
                };
                match getbit_value(store, key, offset) {
                    Ok(value) => ServerWire::write_resp_integer(out, value),
                    Err(()) => write_frame(out, &wrongtype()),
                }
            }
            _ => write_frame(out, &wrong_arity("GETBIT")),
        }
    }
}

fn getbit_value(store: &EmbeddedStore, key: &[u8], offset: usize) -> std::result::Result<i64, ()> {
    let mut value = 0_i64;
    match store.get_string_value_into(key, |bytes| {
        value = read_bit(bytes, offset) as i64;
    }) {
        RedisStringLookup::Hit => Ok(value),
        RedisStringLookup::Miss => Ok(0),
        RedisStringLookup::WrongType => Err(()),
    }
}
