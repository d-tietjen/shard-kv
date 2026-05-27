use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, parse_i64, write_frame, wrong_arity, wrongtype,
};
use crate::commands::string_bits::{BitCountSpec, BitRangeUnit, count_bits, count_bits_in_range};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

define_redis_command!(BitCount, "BITCOUNT", false);

impl crate::commands::redis::RedisCommand for BitCount {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match parse_bitcount_args(args) {
            Ok((key, spec)) => match bitcount_value(store, key, spec) {
                Ok(value) => int(value),
                Err(()) => wrongtype(),
            },
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match parse_bitcount_args(args) {
            Ok((key, spec)) => match bitcount_value(store, key, spec) {
                Ok(value) => ServerWire::write_resp_integer(out, value),
                Err(()) => write_frame(out, &wrongtype()),
            },
            Err(frame) => write_frame(out, &frame),
        }
    }
}

fn parse_bitcount_args<'a>(
    args: &'a [&'a [u8]],
) -> std::result::Result<(&'a [u8], BitCountSpec), Frame> {
    match args {
        [key] => Ok((*key, BitCountSpec::Full)),
        [key, start, stop] => parse_bitcount_range(key, start, stop, BitRangeUnit::Byte),
        [key, start, stop, unit] => match BitRangeUnit::parse(unit) {
            Some(unit) => parse_bitcount_range(key, start, stop, unit),
            None => Err(error("ERR syntax error")),
        },
        _ => Err(wrong_arity("BITCOUNT")),
    }
}

fn parse_bitcount_range<'a>(
    key: &'a [u8],
    start: &[u8],
    stop: &[u8],
    unit: BitRangeUnit,
) -> std::result::Result<(&'a [u8], BitCountSpec), Frame> {
    let (Ok(start), Ok(stop)) = (parse_i64(start), parse_i64(stop)) else {
        return Err(error("ERR value is not an integer or out of range"));
    };
    Ok((key, BitCountSpec::Range { start, stop, unit }))
}

fn bitcount_value(
    store: &EmbeddedStore,
    key: &[u8],
    spec: BitCountSpec,
) -> std::result::Result<i64, ()> {
    let mut count = 0_i64;
    match store.get_string_value_into(key, |bytes| {
        count = match spec {
            BitCountSpec::Full => count_bits(bytes) as i64,
            BitCountSpec::Range { start, stop, unit } => {
                count_bits_in_range(bytes, start, stop, unit) as i64
            }
        };
    }) {
        RedisStringLookup::Hit => Ok(count),
        RedisStringLookup::Miss => Ok(0),
        RedisStringLookup::WrongType => Err(()),
    }
}
