use crate::storage::RedisStringStore;
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, write_frame, wrong_arity, wrongtype,
};
use crate::commands::string_bits::{parse_bit_offset, parse_bit_value, read_bit, write_bit};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

define_redis_command!(SetBit, "SETBIT", true);

impl crate::commands::redis::RedisCommand for SetBit {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key, offset, bit] => match setbit_value(store, key, offset, bit) {
                Ok(previous) => int(previous),
                Err(frame) => frame,
            },
            _ => wrong_arity("SETBIT"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key, offset, bit] => match setbit_value(store, key, offset, bit) {
                Ok(previous) => ServerWire::write_resp_integer(out, previous),
                Err(frame) => write_frame(out, &frame),
            },
            _ => write_frame(out, &wrong_arity("SETBIT")),
        }
    }
}

fn setbit_value(
    store: &EmbeddedStore,
    key: &[u8],
    offset: &[u8],
    bit: &[u8],
) -> std::result::Result<i64, Frame> {
    let offset = parse_bit_offset(offset)
        .map_err(|()| error("ERR bit offset is not an integer or out of range"))?;
    let bit =
        parse_bit_value(bit).map_err(|()| error("ERR bit is not an integer or out of range"))?;
    let byte_index = offset / 8;
    let mut previous = None;
    match store.mutate_string_value_no_ttl_in_place(key, |value| {
        if byte_index < value.len() {
            let old = read_bit(value, offset);
            write_bit(value, offset, bit);
            previous = Some(old as i64);
        }
    }) {
        RedisStringLookup::Hit if previous.is_some() => {
            return Ok(previous.expect("previous bit captured on hit"));
        }
        RedisStringLookup::WrongType => return Err(wrongtype()),
        RedisStringLookup::Hit | RedisStringLookup::Miss => {}
    }

    store.transform_string_value_no_ttl(
        key,
        |existing| {
            let mut value = existing.map_or_else(Vec::new, ToOwned::to_owned);
            if value.len() <= byte_index {
                value.resize(byte_index + 1, 0);
            }
            let previous = read_bit(&value, offset);
            write_bit(&mut value, offset, bit);
            Ok((previous as i64, value))
        },
        wrongtype,
    )
}
