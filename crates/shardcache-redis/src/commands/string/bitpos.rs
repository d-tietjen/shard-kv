use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, parse_i64, write_frame, wrong_arity, wrongtype,
};
use crate::commands::string_bits::{BitPosSpec, BitRangeUnit, find_bit_position, parse_bit_value};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

define_redis_command!(BitPos, "BITPOS", false);

impl crate::commands::redis::RedisCommand for BitPos {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match parse_bitpos_args(args) {
            Ok((key, spec)) => match bitpos_value(store, key, spec) {
                Ok(value) => int(value),
                Err(()) => wrongtype(),
            },
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match parse_bitpos_args(args) {
            Ok((key, spec)) => match bitpos_value(store, key, spec) {
                Ok(value) => ServerWire::write_resp_integer(out, value),
                Err(()) => write_frame(out, &wrongtype()),
            },
            Err(frame) => write_frame(out, &frame),
        }
    }
}

fn parse_bitpos_args<'a>(
    args: &'a [&'a [u8]],
) -> std::result::Result<(&'a [u8], BitPosSpec), Frame> {
    let (key, bit, start, stop, unit) = match args {
        [key, bit] => (*key, *bit, None, None, BitRangeUnit::Byte),
        [key, bit, start] => (*key, *bit, Some(*start), None, BitRangeUnit::Byte),
        [key, bit, start, stop] => (*key, *bit, Some(*start), Some(*stop), BitRangeUnit::Byte),
        [key, bit, start, stop, unit] => {
            let Some(unit) = BitRangeUnit::parse(unit) else {
                return Err(error("ERR syntax error"));
            };
            (*key, *bit, Some(*start), Some(*stop), unit)
        }
        _ => return Err(wrong_arity("BITPOS")),
    };
    let Ok(bit) = parse_bit_value(bit) else {
        return Err(error("ERR bit is not an integer or out of range"));
    };
    let start = match start {
        Some(start) => match parse_i64(start) {
            Ok(start) => Some(start),
            Err(_) => return Err(error("ERR value is not an integer or out of range")),
        },
        None => None,
    };
    let stop = match stop {
        Some(stop) => match parse_i64(stop) {
            Ok(stop) => Some(stop),
            Err(_) => return Err(error("ERR value is not an integer or out of range")),
        },
        None => None,
    };
    Ok((
        key,
        BitPosSpec {
            bit,
            start,
            stop,
            unit,
        },
    ))
}

fn bitpos_value(
    store: &EmbeddedStore,
    key: &[u8],
    spec: BitPosSpec,
) -> std::result::Result<i64, ()> {
    let mut position = if spec.bit { -1 } else { 0 };
    match store.get_string_value_into(key, |bytes| {
        position = find_bit_position(bytes, spec.bit, spec.start, spec.stop, spec.unit);
    }) {
        RedisStringLookup::Hit => Ok(position),
        RedisStringLookup::Miss => Ok(position),
        RedisStringLookup::WrongType => Err(()),
    }
}
