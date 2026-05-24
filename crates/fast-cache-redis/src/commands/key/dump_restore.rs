use crate::storage::RedisObjectValue;
#[cfg(feature = "server")]
use bytes::BytesMut;

const RDB_VERSION: u16 = 9;
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;
const CRC64_JONES_POLY_REFLECTED: u64 = 0x95ac_9329_ac4b_c9b5;
const CRC64_JONES_TABLES: [[u64; 256]; 8] = make_crc64_jones_tables();

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DumpRestoreValue {
    String(Vec<u8>),
    Object(RedisObjectValue),
}

pub(crate) fn encode_string_dump_value(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(string_dump_payload_len(value.len()));
    out.push(RDB_TYPE_STRING);
    write_raw_string(&mut out, value);
    finish_dump_payload(&mut out);
    out
}

#[cfg(feature = "server")]
pub(crate) fn write_string_dump_resp(out: &mut BytesMut, value: &[u8]) {
    let payload_len = string_dump_payload_len(value.len());
    write_resp_bulk_header(out, payload_len);

    let payload_start = out.len();
    out.reserve(payload_len + 2);
    out.extend_from_slice(&[RDB_TYPE_STRING]);
    write_raw_string_bytes(out, value);
    out.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let checksum = crc64_jones(0, &out[payload_start..]);
    out.extend_from_slice(&checksum.to_le_bytes());
    debug_assert_eq!(out.len() - payload_start, payload_len);
    out.extend_from_slice(b"\r\n");
}

pub(crate) fn encode_dump_value(value: &DumpRestoreValue) -> Vec<u8> {
    if let DumpRestoreValue::String(value) = value {
        return encode_string_dump_value(value);
    }

    let mut out = Vec::new();
    match value {
        DumpRestoreValue::String(_) => unreachable!("string values returned above"),
        DumpRestoreValue::Object(RedisObjectValue::List(values)) => {
            out.push(RDB_TYPE_LIST);
            write_len(&mut out, values.len() as u64);
            for value in values {
                write_raw_string(&mut out, value);
            }
        }
        DumpRestoreValue::Object(RedisObjectValue::Set(values)) => {
            let mut values = values.clone();
            values.sort_unstable();
            out.push(RDB_TYPE_SET);
            write_len(&mut out, values.len() as u64);
            for value in &values {
                write_raw_string(&mut out, value);
            }
        }
        DumpRestoreValue::Object(RedisObjectValue::Hash(entries)) => {
            let mut entries = entries.clone();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            out.push(RDB_TYPE_HASH);
            write_len(&mut out, entries.len() as u64);
            for (field, value) in &entries {
                write_raw_string(&mut out, field);
                write_raw_string(&mut out, value);
            }
        }
        DumpRestoreValue::Object(RedisObjectValue::ZSet(entries)) => {
            let mut entries = entries.clone();
            entries.sort_unstable_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| left.0.cmp(&right.0))
            });
            out.push(RDB_TYPE_ZSET_2);
            write_len(&mut out, entries.len() as u64);
            for (member, score) in &entries {
                write_raw_string(&mut out, member);
                out.extend_from_slice(&score.to_le_bytes());
            }
        }
    }

    finish_dump_payload(&mut out);
    out
}

fn finish_dump_payload(out: &mut Vec<u8>) {
    out.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let checksum = crc64_jones(0, &out);
    out.extend_from_slice(&checksum.to_le_bytes());
}

pub(crate) fn decode_dump_payload(payload: &[u8]) -> Result<DumpRestoreValue, ()> {
    verify_payload(payload)?;
    let body_len = payload.len() - 10;
    let body = &payload[..body_len];
    let mut reader = RdbReader::new(body);
    let rdb_type = reader.read_u8()?;
    let value = match rdb_type {
        RDB_TYPE_STRING => DumpRestoreValue::String(reader.read_raw_string()?),
        RDB_TYPE_LIST => {
            let len = reader.read_len()? as usize;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(reader.read_raw_string()?);
            }
            DumpRestoreValue::Object(RedisObjectValue::List(values))
        }
        RDB_TYPE_SET => {
            let len = reader.read_len()? as usize;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(reader.read_raw_string()?);
            }
            DumpRestoreValue::Object(RedisObjectValue::Set(values))
        }
        RDB_TYPE_HASH => {
            let len = reader.read_len()? as usize;
            let mut entries = Vec::with_capacity(len);
            for _ in 0..len {
                let field = reader.read_raw_string()?;
                let value = reader.read_raw_string()?;
                entries.push((field, value));
            }
            DumpRestoreValue::Object(RedisObjectValue::Hash(entries))
        }
        RDB_TYPE_ZSET_2 => {
            let len = reader.read_len()? as usize;
            let mut entries = Vec::with_capacity(len);
            for _ in 0..len {
                let member = reader.read_raw_string()?;
                let score = reader.read_f64_le()?;
                if !score.is_finite() {
                    return Err(());
                }
                entries.push((member, score));
            }
            DumpRestoreValue::Object(RedisObjectValue::ZSet(entries))
        }
        _ => return Err(()),
    };
    reader.finish()?;
    Ok(value)
}

pub(crate) fn decode_string_dump_payload_slice(payload: &[u8]) -> Result<&[u8], ()> {
    verify_payload(payload)?;
    let body_len = payload.len() - 10;
    let body = &payload[..body_len];
    let mut reader = RdbReader::new(body);
    if reader.read_u8()? != RDB_TYPE_STRING {
        return Err(());
    }
    let value = reader.read_raw_string_slice()?;
    reader.finish()?;
    Ok(value)
}

#[inline(always)]
pub(crate) fn crc64_jones(mut crc: u64, bytes: &[u8]) -> u64 {
    if bytes.len() < 64 {
        return crc64_jones_bytewise(crc, bytes);
    }

    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let word = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        crc ^= word;
        crc = CRC64_JONES_TABLES[7][(crc & 0xff) as usize]
            ^ CRC64_JONES_TABLES[6][((crc >> 8) & 0xff) as usize]
            ^ CRC64_JONES_TABLES[5][((crc >> 16) & 0xff) as usize]
            ^ CRC64_JONES_TABLES[4][((crc >> 24) & 0xff) as usize]
            ^ CRC64_JONES_TABLES[3][((crc >> 32) & 0xff) as usize]
            ^ CRC64_JONES_TABLES[2][((crc >> 40) & 0xff) as usize]
            ^ CRC64_JONES_TABLES[1][((crc >> 48) & 0xff) as usize]
            ^ CRC64_JONES_TABLES[0][((crc >> 56) & 0xff) as usize];
    }
    for byte in chunks.remainder() {
        crc = CRC64_JONES_TABLES[0][((crc as u8) ^ *byte) as usize] ^ (crc >> 8);
    }
    crc
}

#[inline(always)]
fn crc64_jones_bytewise(mut crc: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        crc = CRC64_JONES_TABLES[0][((crc as u8) ^ *byte) as usize] ^ (crc >> 8);
    }
    crc
}

fn verify_payload(payload: &[u8]) -> Result<(), ()> {
    if payload.len() < 10 {
        return Err(());
    }
    let version_offset = payload.len() - 10;
    let checksum_offset = payload.len() - 8;
    let version = u16::from_le_bytes(
        payload[version_offset..checksum_offset]
            .try_into()
            .map_err(|_| ())?,
    );
    if version > RDB_VERSION {
        return Err(());
    }
    let expected = u64::from_le_bytes(payload[checksum_offset..].try_into().map_err(|_| ())?);
    let actual = crc64_jones(0, &payload[..checksum_offset]);
    (actual == expected).then_some(()).ok_or(())
}

fn write_raw_string(out: &mut Vec<u8>, value: &[u8]) {
    write_len(out, value.len() as u64);
    out.extend_from_slice(value);
}

#[cfg(feature = "server")]
fn write_raw_string_bytes(out: &mut BytesMut, value: &[u8]) {
    write_len_bytes(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn write_len(out: &mut Vec<u8>, len: u64) {
    if len < (1 << 6) {
        out.push(len as u8);
    } else if len < (1 << 14) {
        out.push(((len >> 8) as u8) | 0x40);
        out.push((len & 0xff) as u8);
    } else if u32::try_from(len).is_ok() {
        out.push(0x80);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(0x81);
        out.extend_from_slice(&len.to_be_bytes());
    }
}

#[cfg(feature = "server")]
fn write_len_bytes(out: &mut BytesMut, len: u64) {
    if len < (1 << 6) {
        out.extend_from_slice(&[len as u8]);
    } else if len < (1 << 14) {
        out.extend_from_slice(&[((len >> 8) as u8) | 0x40, (len & 0xff) as u8]);
    } else if u32::try_from(len).is_ok() {
        out.extend_from_slice(&[0x80]);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.extend_from_slice(&[0x81]);
        out.extend_from_slice(&len.to_be_bytes());
    }
}

const fn make_crc64_jones_tables() -> [[u64; 256]; 8] {
    let mut table = [0_u64; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ CRC64_JONES_POLY_REFLECTED
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    let mut tables = [[0_u64; 256]; 8];
    tables[0] = table;
    let mut table_index = 1;
    while table_index < 8 {
        let mut value_index = 0;
        while value_index < 256 {
            let previous = tables[table_index - 1][value_index];
            tables[table_index][value_index] =
                (previous >> 8) ^ tables[0][(previous & 0xff) as usize];
            value_index += 1;
        }
        table_index += 1;
    }
    tables
}

fn rdb_len_width(len: u64) -> usize {
    if len < (1 << 6) {
        1
    } else if len < (1 << 14) {
        2
    } else if u32::try_from(len).is_ok() {
        5
    } else {
        9
    }
}

fn string_dump_payload_len(value_len: usize) -> usize {
    1 + rdb_len_width(value_len as u64) + value_len + 10
}

#[cfg(feature = "server")]
fn write_resp_bulk_header(out: &mut BytesMut, payload_len: usize) {
    out.extend_from_slice(b"$");
    let mut len_buf = itoa::Buffer::new();
    out.extend_from_slice(len_buf.format(payload_len).as_bytes());
    out.extend_from_slice(b"\r\n");
}

struct RdbReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> RdbReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn finish(self) -> Result<(), ()> {
        (self.cursor == self.bytes.len()).then_some(()).ok_or(())
    }

    fn read_u8(&mut self) -> Result<u8, ()> {
        let Some(value) = self.bytes.get(self.cursor).copied() else {
            return Err(());
        };
        self.cursor += 1;
        Ok(value)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ()> {
        let end = self.cursor.checked_add(len).ok_or(())?;
        let slice = self.bytes.get(self.cursor..end).ok_or(())?;
        self.cursor = end;
        Ok(slice)
    }

    fn read_len_or_encoding(&mut self) -> Result<RdbLen, ()> {
        let first = self.read_u8()?;
        match first >> 6 {
            0 => Ok(RdbLen::Len(u64::from(first & 0x3f))),
            1 => {
                let second = self.read_u8()?;
                Ok(RdbLen::Len(
                    (u64::from(first & 0x3f) << 8) | u64::from(second),
                ))
            }
            2 => match first {
                0x80 => {
                    let raw = self.read_exact(4)?;
                    Ok(RdbLen::Len(u64::from(u32::from_be_bytes(
                        raw.try_into().map_err(|_| ())?,
                    ))))
                }
                0x81 => {
                    let raw = self.read_exact(8)?;
                    Ok(RdbLen::Len(u64::from_be_bytes(
                        raw.try_into().map_err(|_| ())?,
                    )))
                }
                _ => Err(()),
            },
            3 => Ok(RdbLen::Encoded(first & 0x3f)),
            _ => unreachable!("two-bit length selector is exhaustive"),
        }
    }

    fn read_len(&mut self) -> Result<u64, ()> {
        match self.read_len_or_encoding()? {
            RdbLen::Len(len) => Ok(len),
            RdbLen::Encoded(_) => Err(()),
        }
    }

    fn read_raw_string(&mut self) -> Result<Vec<u8>, ()> {
        match self.read_len_or_encoding()? {
            RdbLen::Len(len) => {
                let len = usize::try_from(len).map_err(|_| ())?;
                Ok(self.read_exact(len)?.to_vec())
            }
            RdbLen::Encoded(RDB_ENC_INT8) => {
                let value = self.read_u8()? as i8;
                Ok(value.to_string().into_bytes())
            }
            RdbLen::Encoded(RDB_ENC_INT16) => {
                let raw = self.read_exact(2)?;
                let value = i16::from_le_bytes(raw.try_into().map_err(|_| ())?);
                Ok(value.to_string().into_bytes())
            }
            RdbLen::Encoded(RDB_ENC_INT32) => {
                let raw = self.read_exact(4)?;
                let value = i32::from_le_bytes(raw.try_into().map_err(|_| ())?);
                Ok(value.to_string().into_bytes())
            }
            RdbLen::Encoded(RDB_ENC_LZF) => Err(()),
            RdbLen::Encoded(_) => Err(()),
        }
    }

    fn read_raw_string_slice(&mut self) -> Result<&'a [u8], ()> {
        match self.read_len_or_encoding()? {
            RdbLen::Len(len) => {
                let len = usize::try_from(len).map_err(|_| ())?;
                self.read_exact(len)
            }
            RdbLen::Encoded(_) => Err(()),
        }
    }

    fn read_f64_le(&mut self) -> Result<f64, ()> {
        let raw = self.read_exact(8)?;
        Ok(f64::from_le_bytes(raw.try_into().map_err(|_| ())?))
    }
}

enum RdbLen {
    Len(u64),
    Encoded(u8),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_crc64_jones_known_vector() {
        assert_eq!(crc64_jones(0, b"123456789"), 0xe9c6_d914_c4b8_d9ca);
    }

    #[test]
    fn crc64_slice_by_8_matches_bytewise_for_offsets() {
        let data = (0..257)
            .map(|index| (index * 37 + 11) as u8)
            .collect::<Vec<_>>();
        for len in 0..=data.len() {
            let bytes = &data[..len];
            assert_eq!(
                crc64_jones(0, bytes),
                crc64_jones_bytewise(0, bytes),
                "len={len}"
            );
        }
    }

    #[test]
    fn dump_payload_round_trips_string() {
        let payload = encode_dump_value(&DumpRestoreValue::String(b"value".to_vec()));
        assert_eq!(
            decode_dump_payload(&payload),
            Ok(DumpRestoreValue::String(b"value".to_vec()))
        );
    }

    #[test]
    fn dump_payload_rejects_bad_checksum() {
        let mut payload = encode_dump_value(&DumpRestoreValue::String(b"value".to_vec()));
        payload[1] ^= 0xff;
        assert_eq!(decode_dump_payload(&payload), Err(()));
    }
}
