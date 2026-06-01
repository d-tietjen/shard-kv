use crate::storage::RedisStringStore;
use bytes::BytesMut;

use crate::commands::redis::{
    define_redis_command, error, int, optional_string_value, parse_i64, write_frame,
    write_resp_array_header, write_resp_null, wrong_arity, wrongtype,
};
use crate::commands::string_bits::read_bit;
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

const REDIS_STRING_MAX_BYTES: usize = 512 * 1024 * 1024;
const REDIS_STRING_MAX_BIT_OFFSET: usize = REDIS_STRING_MAX_BYTES * 8 - 1;

define_redis_command!(BitField, "BITFIELD", true);

impl crate::commands::redis::RedisCommand for BitField {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        bitfield_frame(store, args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_bitfield_resp(store, args, false, out);
    }
}

/// Build the RESP `Frame` reply for BITFIELD / BITFIELD_RO.
pub(crate) fn bitfield_frame(store: &EmbeddedStore, args: &[&[u8]], read_only: bool) -> Frame {
    match bitfield_values(store, args, read_only) {
        Ok(values) => Frame::Array(
            values
                .into_iter()
                .map(|value| value.map_or(Frame::Null, int))
                .collect(),
        ),
        Err(frame) => frame,
    }
}

/// Stream the RESP reply for BITFIELD / BITFIELD_RO.
#[cfg(feature = "server")]
pub(crate) fn write_bitfield_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    read_only: bool,
    out: &mut BytesMut,
) {
    match bitfield_values(store, args, read_only) {
        Ok(values) => {
            write_resp_array_header(out, values.len());
            for value in values {
                match value {
                    Some(value) => ServerWire::write_resp_integer(out, value),
                    None => write_resp_null(out),
                }
            }
        }
        Err(frame) => write_frame(out, &frame),
    }
}

#[derive(Clone, Copy)]
struct BitEncoding {
    signed: bool,
    bits: u8,
}

#[derive(Clone, Copy)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

enum BitFieldOp {
    Get {
        encoding: BitEncoding,
        offset: usize,
    },
    Set {
        encoding: BitEncoding,
        offset: usize,
        value: i64,
    },
    IncrBy {
        encoding: BitEncoding,
        offset: usize,
        increment: i64,
        overflow: Overflow,
    },
}

fn bitfield_values(
    store: &EmbeddedStore,
    args: &[&[u8]],
    read_only: bool,
) -> std::result::Result<Vec<Option<i64>>, Frame> {
    let name = if read_only { "BITFIELD_RO" } else { "BITFIELD" };
    let [key, tail @ ..] = args else {
        return Err(wrong_arity(name));
    };
    if tail.is_empty() {
        return Err(wrong_arity(name));
    }
    let ops = parse_ops(tail)?;
    let has_writes = ops
        .iter()
        .any(|op| matches!(op, BitFieldOp::Set { .. } | BitFieldOp::IncrBy { .. }));

    // BITFIELD_RO accepts only GET; any mutating subcommand is an error.
    if read_only && has_writes {
        return Err(error("ERR BITFIELD_RO only supports the GET subcommand"));
    }

    if has_writes {
        apply_write_ops(store, key, &ops)
    } else {
        apply_read_ops(store, key, &ops)
    }
}

fn parse_ops(args: &[&[u8]]) -> std::result::Result<Vec<BitFieldOp>, Frame> {
    let mut ops = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut cursor = 0;
    while cursor < args.len() {
        match args[cursor] {
            command if command.eq_ignore_ascii_case(b"OVERFLOW") => {
                let Some(policy) = args.get(cursor + 1).and_then(|raw| parse_overflow(raw)) else {
                    return Err(error("ERR syntax error"));
                };
                overflow = policy;
                cursor += 2;
            }
            command if command.eq_ignore_ascii_case(b"GET") => {
                let Some((encoding, offset)) = parse_encoding_and_offset(args, cursor + 1)? else {
                    return Err(wrong_arity("BITFIELD"));
                };
                ops.push(BitFieldOp::Get { encoding, offset });
                cursor += 3;
            }
            command if command.eq_ignore_ascii_case(b"SET") => {
                let Some((encoding, offset)) = parse_encoding_and_offset(args, cursor + 1)? else {
                    return Err(wrong_arity("BITFIELD"));
                };
                let value = args
                    .get(cursor + 3)
                    .ok_or_else(|| wrong_arity("BITFIELD"))
                    .and_then(|raw| parse_i64(raw).map_err(|()| integer_error()))?;
                ops.push(BitFieldOp::Set {
                    encoding,
                    offset,
                    value,
                });
                cursor += 4;
            }
            command if command.eq_ignore_ascii_case(b"INCRBY") => {
                let Some((encoding, offset)) = parse_encoding_and_offset(args, cursor + 1)? else {
                    return Err(wrong_arity("BITFIELD"));
                };
                let increment = args
                    .get(cursor + 3)
                    .ok_or_else(|| wrong_arity("BITFIELD"))
                    .and_then(|raw| parse_i64(raw).map_err(|()| integer_error()))?;
                ops.push(BitFieldOp::IncrBy {
                    encoding,
                    offset,
                    increment,
                    overflow,
                });
                cursor += 4;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok(ops)
}

fn parse_encoding_and_offset(
    args: &[&[u8]],
    cursor: usize,
) -> std::result::Result<Option<(BitEncoding, usize)>, Frame> {
    let Some(raw_encoding) = args.get(cursor) else {
        return Ok(None);
    };
    let Some(raw_offset) = args.get(cursor + 1) else {
        return Ok(None);
    };
    let encoding = parse_encoding(raw_encoding)?;
    let offset = parse_field_offset(raw_offset, encoding.bits)?;
    Ok(Some((encoding, offset)))
}

fn parse_encoding(raw: &[u8]) -> std::result::Result<BitEncoding, Frame> {
    let Some((kind, bits)) = raw.split_first() else {
        return Err(error("ERR invalid bitfield type"));
    };
    let signed = match kind {
        b'i' | b'I' => true,
        b'u' | b'U' => false,
        _ => return Err(error("ERR invalid bitfield type")),
    };
    let bits = parse_i64(bits).map_err(|()| error("ERR invalid bitfield type"))?;
    let valid = match signed {
        true => (1..=64).contains(&bits),
        false => (1..=63).contains(&bits),
    };
    if !valid {
        return Err(error("ERR invalid bitfield type"));
    }
    Ok(BitEncoding {
        signed,
        bits: bits as u8,
    })
}

fn parse_field_offset(raw: &[u8], bits: u8) -> std::result::Result<usize, Frame> {
    let (raw, multiplier) = match raw.split_first() {
        Some((b'#', tail)) => (tail, bits as usize),
        _ => (raw, 1),
    };
    let offset = parse_i64(raw).map_err(|()| integer_error())?;
    if offset < 0 {
        return Err(error("ERR bit offset is not an integer or out of range"));
    }
    let offset = usize::try_from(offset).map_err(|_| integer_error())?;
    let offset = offset
        .checked_mul(multiplier)
        .ok_or_else(|| error("ERR bit offset is not an integer or out of range"))?;
    let last = offset
        .checked_add(bits as usize)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| error("ERR bit offset is not an integer or out of range"))?;
    if last > REDIS_STRING_MAX_BIT_OFFSET {
        return Err(error("ERR bit offset is not an integer or out of range"));
    }
    Ok(offset)
}

fn parse_overflow(raw: &[u8]) -> Option<Overflow> {
    match raw {
        value if value.eq_ignore_ascii_case(b"WRAP") => Some(Overflow::Wrap),
        value if value.eq_ignore_ascii_case(b"SAT") => Some(Overflow::Sat),
        value if value.eq_ignore_ascii_case(b"FAIL") => Some(Overflow::Fail),
        _ => None,
    }
}

fn apply_read_ops(
    store: &EmbeddedStore,
    key: &[u8],
    ops: &[BitFieldOp],
) -> std::result::Result<Vec<Option<i64>>, Frame> {
    let value = optional_string_value(store, key, true)?.unwrap_or_default();
    Ok(ops
        .iter()
        .map(|op| match *op {
            BitFieldOp::Get { encoding, offset } => Some(read_field(&value, encoding, offset)),
            BitFieldOp::Set { .. } | BitFieldOp::IncrBy { .. } => unreachable!(),
        })
        .collect())
}

fn apply_write_ops(
    store: &EmbeddedStore,
    key: &[u8],
    ops: &[BitFieldOp],
) -> std::result::Result<Vec<Option<i64>>, Frame> {
    store.transform_string_value_no_ttl(
        key,
        |existing| {
            let mut current = existing.map_or_else(Vec::new, ToOwned::to_owned);
            let mut responses = Vec::with_capacity(ops.len());
            for op in ops {
                match *op {
                    BitFieldOp::Get { encoding, offset } => {
                        responses.push(Some(read_field(&current, encoding, offset)));
                    }
                    BitFieldOp::Set {
                        encoding,
                        offset,
                        value,
                    } => {
                        responses.push(Some(read_field(&current, encoding, offset)));
                        write_field(&mut current, encoding, offset, value as i128);
                    }
                    BitFieldOp::IncrBy {
                        encoding,
                        offset,
                        increment,
                        overflow,
                    } => {
                        let old = read_field(&current, encoding, offset) as i128;
                        match apply_increment(old, increment as i128, encoding, overflow) {
                            Some(value) => {
                                write_field(&mut current, encoding, offset, value);
                                responses.push(Some(value as i64));
                            }
                            None => responses.push(None),
                        }
                    }
                }
            }
            Ok((responses, current))
        },
        wrongtype,
    )
}

fn read_field(value: &[u8], encoding: BitEncoding, offset: usize) -> i64 {
    let raw = read_unsigned_field(value, offset, encoding.bits);
    match encoding.signed {
        false => raw as i64,
        true => sign_extend(raw, encoding.bits),
    }
}

fn read_unsigned_field(value: &[u8], offset: usize, bits: u8) -> u64 {
    let mut raw = 0u64;
    for bit in 0..bits as usize {
        raw <<= 1;
        if read_bit(value, offset + bit) {
            raw |= 1;
        }
    }
    raw
}

fn write_field(value: &mut Vec<u8>, encoding: BitEncoding, offset: usize, field_value: i128) {
    let bits = encoding.bits as usize;
    let last_bit = offset + bits - 1;
    let bytes = last_bit / 8 + 1;
    if value.len() < bytes {
        value.resize(bytes, 0);
    }
    let raw = truncate_to_bits(field_value, encoding.bits);
    for bit in 0..bits {
        let source_shift = bits - bit - 1;
        let next = (raw >> source_shift) & 1 == 1;
        let offset = offset + bit;
        let mask = 0x80 >> (offset % 8);
        let byte = &mut value[offset / 8];
        match next {
            true => *byte |= mask,
            false => *byte &= !mask,
        }
    }
}

fn apply_increment(
    old: i128,
    increment: i128,
    encoding: BitEncoding,
    overflow: Overflow,
) -> Option<i128> {
    let value = old.saturating_add(increment);
    let (min, max) = value_bounds(encoding);
    if (min..=max).contains(&value) {
        return Some(value);
    }
    match overflow {
        Overflow::Fail => None,
        Overflow::Sat => Some(value.clamp(min, max)),
        Overflow::Wrap => Some(wrap_value(value, encoding)),
    }
}

fn value_bounds(encoding: BitEncoding) -> (i128, i128) {
    let bits = encoding.bits as u32;
    match encoding.signed {
        true => (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1),
        false => (0, (1i128 << bits) - 1),
    }
}

fn wrap_value(value: i128, encoding: BitEncoding) -> i128 {
    let bits = encoding.bits as u32;
    let modulo = 1i128 << bits;
    let wrapped = value.rem_euclid(modulo);
    match encoding.signed {
        true if wrapped >= (1i128 << (bits - 1)) => wrapped - modulo,
        _ => wrapped,
    }
}

fn truncate_to_bits(value: i128, bits: u8) -> u64 {
    if bits == 64 {
        value as u64
    } else {
        let mask = (1u128 << bits) - 1;
        (value as u128 & mask) as u64
    }
}

fn sign_extend(raw: u64, bits: u8) -> i64 {
    if bits == 64 {
        return raw as i64;
    }
    let sign_bit = 1u64 << (bits - 1);
    let mask = (1u64 << bits) - 1;
    if raw & sign_bit == 0 {
        (raw & mask) as i64
    } else {
        (raw | !mask) as i64
    }
}

fn integer_error() -> Frame {
    error("ERR value is not an integer or out of range")
}
