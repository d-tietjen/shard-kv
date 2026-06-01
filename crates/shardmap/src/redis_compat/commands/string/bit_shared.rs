use crate::commands::formal_range::normalize_redis_range;
use crate::commands::redis::{eq_ignore_ascii_case, parse_u64};

const REDIS_STRING_MAX_BYTES: u64 = 512 * 1024 * 1024;
const REDIS_STRING_MAX_BIT_OFFSET: u64 = REDIS_STRING_MAX_BYTES * 8 - 1;

#[derive(Clone, Copy)]
pub(crate) enum BitCountSpec {
    Full,
    Range {
        start: i64,
        stop: i64,
        unit: BitRangeUnit,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct BitPosSpec {
    pub(crate) bit: bool,
    pub(crate) start: Option<i64>,
    pub(crate) stop: Option<i64>,
    pub(crate) unit: BitRangeUnit,
}

#[derive(Clone, Copy)]
pub(crate) enum BitRangeUnit {
    Byte,
    Bit,
}

impl BitRangeUnit {
    pub(crate) fn parse(value: &[u8]) -> Option<Self> {
        match value {
            value if eq_ignore_ascii_case(value, b"BYTE") => Some(Self::Byte),
            value if eq_ignore_ascii_case(value, b"BIT") => Some(Self::Bit),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum BitOpKind {
    And,
    Or,
    Xor,
    Not,
}

impl BitOpKind {
    pub(crate) fn parse(value: &[u8]) -> Option<Self> {
        match value {
            value if eq_ignore_ascii_case(value, b"AND") => Some(Self::And),
            value if eq_ignore_ascii_case(value, b"OR") => Some(Self::Or),
            value if eq_ignore_ascii_case(value, b"XOR") => Some(Self::Xor),
            value if eq_ignore_ascii_case(value, b"NOT") => Some(Self::Not),
            _ => None,
        }
    }
}

pub(crate) fn parse_bit_offset(raw: &[u8]) -> std::result::Result<usize, ()> {
    let offset = parse_u64(raw)?;
    match offset <= REDIS_STRING_MAX_BIT_OFFSET {
        true => Ok(offset as usize),
        false => Err(()),
    }
}

pub(crate) fn parse_bit_value(raw: &[u8]) -> std::result::Result<bool, ()> {
    match raw {
        b"0" => Ok(false),
        b"1" => Ok(true),
        _ => Err(()),
    }
}

pub(crate) fn read_bit(value: &[u8], offset: usize) -> bool {
    let byte_index = offset / 8;
    value
        .get(byte_index)
        .is_some_and(|byte| byte & bit_mask(offset) != 0)
}

pub(crate) fn write_bit(value: &mut [u8], offset: usize, bit: bool) {
    let mask = bit_mask(offset);
    let byte = &mut value[offset / 8];
    match bit {
        true => *byte |= mask,
        false => *byte &= !mask,
    }
}

pub(crate) fn count_bits(value: &[u8]) -> u64 {
    value.iter().map(|byte| byte.count_ones() as u64).sum()
}

pub(crate) fn count_bits_in_range(value: &[u8], start: i64, stop: i64, unit: BitRangeUnit) -> u64 {
    match unit {
        BitRangeUnit::Byte => normalize_redis_range(value.len(), start, stop)
            .map(|range| {
                let (start, stop) = range.into_bounds();
                count_bits(&value[start..=stop])
            })
            .unwrap_or(0),
        BitRangeUnit::Bit => normalize_redis_range(value.len().saturating_mul(8), start, stop)
            .map(|range| {
                let (start, stop) = range.into_bounds();
                (start..=stop)
                    .filter(|offset| read_bit(value, *offset))
                    .count() as u64
            })
            .unwrap_or(0),
    }
}

pub(crate) fn find_bit_position(
    value: &[u8],
    bit: bool,
    start: Option<i64>,
    stop: Option<i64>,
    unit: BitRangeUnit,
) -> i64 {
    let len = match unit {
        BitRangeUnit::Byte => value.len(),
        BitRangeUnit::Bit => value.len().saturating_mul(8),
    };
    let unbounded = stop.is_none();
    let start = start.unwrap_or(0);
    let stop = stop.unwrap_or(len as i64 - 1);
    let Some(range) = normalize_redis_range(len, start, stop) else {
        return match (bit, unbounded) {
            (false, true) => first_unbounded_zero_position(value, start, unit),
            _ => -1,
        };
    };
    let (start, stop) = range.into_bounds();
    let (start_bit, stop_bit) = match unit {
        BitRangeUnit::Byte => (start.saturating_mul(8), stop.saturating_mul(8) + 7),
        BitRangeUnit::Bit => (start, stop),
    };
    for offset in start_bit..=stop_bit {
        if read_bit(value, offset) == bit {
            return offset as i64;
        }
    }
    match (bit, unbounded) {
        (false, true) => value.len().saturating_mul(8) as i64,
        _ => -1,
    }
}

pub(crate) fn apply_bitop(operation: BitOpKind, values: &[Vec<u8>]) -> Vec<u8> {
    if operation == BitOpKind::Not {
        return values[0].iter().map(|byte| !byte).collect();
    }
    let len = values.iter().map(Vec::len).max().unwrap_or(0);
    let mut result = match operation {
        BitOpKind::And => vec![u8::MAX; len],
        BitOpKind::Or | BitOpKind::Xor => vec![0; len],
        BitOpKind::Not => unreachable!(),
    };
    for value in values {
        for (index, result_byte) in result.iter_mut().enumerate() {
            let byte = value.get(index).copied().unwrap_or(0);
            match operation {
                BitOpKind::And => *result_byte &= byte,
                BitOpKind::Or => *result_byte |= byte,
                BitOpKind::Xor => *result_byte ^= byte,
                BitOpKind::Not => unreachable!(),
            }
        }
    }
    result
}

fn bit_mask(offset: usize) -> u8 {
    0x80 >> (offset % 8)
}

fn first_unbounded_zero_position(value: &[u8], start: i64, unit: BitRangeUnit) -> i64 {
    let len_bits = value.len().saturating_mul(8) as i64;
    if start < 0 {
        return len_bits;
    }
    match unit {
        BitRangeUnit::Byte => start.saturating_mul(8).max(len_bits),
        BitRangeUnit::Bit => start.max(len_bits),
    }
}
