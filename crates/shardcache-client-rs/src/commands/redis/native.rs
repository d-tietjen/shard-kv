//! Recursive SCNP Redis values and bounded decoding.
//!
//! This module depends only on the standard library. Transport and legacy RESP
//! decoding stay in the parent module.

const NATIVE_REDIS_MAX_BODY_BYTES: usize = 256 * 1024 * 1024;
const RESP_MAX_CONTAINER_ITEMS: usize = 65_536;
const NATIVE_REDIS_MAX_NESTING_DEPTH: usize = 128;
const NATIVE_REDIS_MAX_VALUES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisResponse {
    Ok,
    Null,
    Integer(i64),
    Value(Vec<u8>),
    Array(Vec<RedisResponse>),
    Map(Vec<(RedisResponse, RedisResponse)>),
    Set(Vec<RedisResponse>),
    Push(Vec<RedisResponse>),
    Float(f64),
    Boolean(bool),
    /// An error nested in a native collection, such as an EXEC response.
    Error(Vec<u8>),
    BigNumber(Vec<u8>),
    Verbatim {
        format: Vec<u8>,
        value: Vec<u8>,
    },
    Attribute {
        attributes: Vec<(RedisResponse, RedisResponse)>,
        data: Box<RedisResponse>,
    },
}

#[derive(Debug)]
pub(super) struct NativeDecodeError(pub(super) String);
type Result<T> = std::result::Result<T, NativeDecodeError>;

pub(super) fn decode(bytes: &[u8]) -> Result<RedisResponse> {
    if bytes.len() > NATIVE_REDIS_MAX_BODY_BYTES {
        return Err(NativeDecodeError(
            "native Redis response exceeds body limit".into(),
        ));
    }
    let mut cursor = 0usize;
    let mut remaining_values = NATIVE_REDIS_MAX_VALUES;
    let response = parse_native_value(bytes, &mut cursor, 0, &mut remaining_values)?;
    if cursor != bytes.len() {
        return Err(NativeDecodeError(
            "native Redis response has trailing bytes".into(),
        ));
    }
    match response {
        RedisResponse::Error(message) => Err(NativeDecodeError(
            String::from_utf8_lossy(&message).into_owned(),
        )),
        response => Ok(response),
    }
}

fn parse_native_value(
    buf: &[u8],
    cursor: &mut usize,
    depth: usize,
    remaining_values: &mut usize,
) -> Result<RedisResponse> {
    *remaining_values = remaining_values
        .checked_sub(1)
        .ok_or_else(|| NativeDecodeError("native Redis response exceeds value budget".into()))?;
    if depth > NATIVE_REDIS_MAX_NESTING_DEPTH {
        return Err(NativeDecodeError(format!(
            "native Redis response nesting exceeds maximum depth {NATIVE_REDIS_MAX_NESTING_DEPTH}"
        )));
    }
    let tag = take_native_byte(buf, cursor, "native Redis response tag")?;
    match tag {
        0 => {
            let value = take_native_bytes(buf, cursor, "native simple string")?;
            if value == b"OK" {
                Ok(RedisResponse::Ok)
            } else {
                Ok(RedisResponse::Value(value.to_vec()))
            }
        }
        1 => Ok(RedisResponse::Value(
            take_native_bytes(buf, cursor, "native blob string")?.to_vec(),
        )),
        2 => Ok(RedisResponse::Integer(take_native_i64(
            buf,
            cursor,
            "native integer",
        )?)),
        3 => {
            let count = take_native_count(buf, cursor, "native array", 1, 0, *remaining_values)?;
            let mut values = native_vec(count)?;
            for _ in 0..count {
                values.push(parse_native_value(
                    buf,
                    cursor,
                    depth + 1,
                    remaining_values,
                )?);
            }
            Ok(RedisResponse::Array(values))
        }
        4 => {
            let count = take_native_count(buf, cursor, "native map", 2, 0, *remaining_values)?;
            let mut entries = native_vec(count)?;
            for _ in 0..count {
                let key = parse_native_value(buf, cursor, depth + 1, remaining_values)?;
                let value = parse_native_value(buf, cursor, depth + 1, remaining_values)?;
                entries.push((key, value));
            }
            Ok(RedisResponse::Map(entries))
        }
        5 => {
            let count = take_native_count(buf, cursor, "native set", 1, 0, *remaining_values)?;
            let mut values = native_vec(count)?;
            for _ in 0..count {
                values.push(parse_native_value(
                    buf,
                    cursor,
                    depth + 1,
                    remaining_values,
                )?);
            }
            Ok(RedisResponse::Set(values))
        }
        6 => {
            let count = take_native_count(buf, cursor, "native push", 1, 0, *remaining_values)?;
            let mut values = native_vec(count)?;
            for _ in 0..count {
                values.push(parse_native_value(
                    buf,
                    cursor,
                    depth + 1,
                    remaining_values,
                )?);
            }
            Ok(RedisResponse::Push(values))
        }
        7 => Ok(RedisResponse::Null),
        8 => match take_native_byte(buf, cursor, "native boolean")? {
            0 => Ok(RedisResponse::Boolean(false)),
            1 => Ok(RedisResponse::Boolean(true)),
            other => Err(NativeDecodeError(format!(
                "invalid native Redis boolean value {other}"
            ))),
        },
        9 => Ok(RedisResponse::Float(take_native_f64(
            buf,
            cursor,
            "native double",
        )?)),
        10 => Ok(RedisResponse::BigNumber(
            take_native_bytes(buf, cursor, "native big number")?.to_vec(),
        )),
        11 => Ok(RedisResponse::Verbatim {
            format: take_native_bytes(buf, cursor, "native verbatim format")?.to_vec(),
            value: take_native_bytes(buf, cursor, "native verbatim value")?.to_vec(),
        }),
        12 => {
            let count =
                take_native_count(buf, cursor, "native attributes", 2, 1, *remaining_values)?;
            let mut attributes = native_vec(count)?;
            for _ in 0..count {
                let key = parse_native_value(buf, cursor, depth + 1, remaining_values)?;
                let value = parse_native_value(buf, cursor, depth + 1, remaining_values)?;
                attributes.push((key, value));
            }
            let data = Box::new(parse_native_value(
                buf,
                cursor,
                depth + 1,
                remaining_values,
            )?);
            Ok(RedisResponse::Attribute { attributes, data })
        }
        13 => Ok(RedisResponse::Error(
            take_native_bytes(buf, cursor, "native error")?.to_vec(),
        )),
        other => Err(NativeDecodeError(format!(
            "unsupported native Redis response tag {other}"
        ))),
    }
}

fn take_native_byte(buf: &[u8], cursor: &mut usize, field: &str) -> Result<u8> {
    let value = buf
        .get(*cursor)
        .copied()
        .ok_or_else(|| NativeDecodeError(format!("{field} is truncated")))?;
    *cursor += 1;
    Ok(value)
}

fn take_native_fixed<const N: usize>(
    buf: &[u8],
    cursor: &mut usize,
    field: &str,
) -> Result<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .ok_or_else(|| NativeDecodeError(format!("{field} length overflow")))?;
    let bytes = buf
        .get(*cursor..end)
        .ok_or_else(|| NativeDecodeError(format!("{field} is truncated")))?;
    let value = bytes
        .try_into()
        .map_err(|_| NativeDecodeError(format!("{field} has an invalid width")))?;
    *cursor = end;
    Ok(value)
}

fn take_native_u32(buf: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
    Ok(u32::from_le_bytes(take_native_fixed(buf, cursor, field)?))
}

fn take_native_i64(buf: &[u8], cursor: &mut usize, field: &str) -> Result<i64> {
    Ok(i64::from_le_bytes(take_native_fixed(buf, cursor, field)?))
}

fn take_native_f64(buf: &[u8], cursor: &mut usize, field: &str) -> Result<f64> {
    Ok(f64::from_le_bytes(take_native_fixed(buf, cursor, field)?))
}

fn take_native_bytes<'a>(buf: &'a [u8], cursor: &mut usize, field: &str) -> Result<&'a [u8]> {
    let len = take_native_u32(buf, cursor, field)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| NativeDecodeError(format!("{field} length overflow")))?;
    let value = buf
        .get(*cursor..end)
        .ok_or_else(|| NativeDecodeError(format!("{field} is truncated")))?;
    *cursor = end;
    Ok(value)
}

fn take_native_count(
    buf: &[u8],
    cursor: &mut usize,
    field: &str,
    values_per_entry: usize,
    trailing_values: usize,
    remaining_values: usize,
) -> Result<usize> {
    let count = take_native_u32(buf, cursor, field)? as usize;
    let minimum_values = count
        .saturating_mul(values_per_entry)
        .saturating_add(trailing_values);
    if count > RESP_MAX_CONTAINER_ITEMS
        || minimum_values > buf.len().saturating_sub(*cursor)
        || minimum_values > remaining_values
    {
        return Err(NativeDecodeError(format!(
            "{field} item count exceeds the response-body or value limit"
        )));
    }
    Ok(count)
}

fn native_vec<T>(count: usize) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| NativeDecodeError("native Redis collection allocation failed".into()))?;
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(tag: u8, value: &[u8]) -> Vec<u8> {
        let mut encoded = vec![tag];
        encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(value);
        encoded
    }

    fn collection(tag: u8, count: u32, values: &[Vec<u8>]) -> Vec<u8> {
        let mut encoded = vec![tag];
        encoded.extend_from_slice(&count.to_le_bytes());
        for value in values {
            encoded.extend_from_slice(value);
        }
        encoded
    }

    #[test]
    fn decodes_all_scalar_tags_and_binary_payloads() {
        for (encoded, expected) in [
            (bytes(0, b"OK"), RedisResponse::Ok),
            (
                bytes(0, b"QUEUED"),
                RedisResponse::Value(b"QUEUED".to_vec()),
            ),
            (
                bytes(1, b"\0\r\n\xff"),
                RedisResponse::Value(b"\0\r\n\xff".to_vec()),
            ),
            (
                [vec![2], i64::MIN.to_le_bytes().to_vec()].concat(),
                RedisResponse::Integer(i64::MIN),
            ),
            (vec![7], RedisResponse::Null),
            (vec![8, 0], RedisResponse::Boolean(false)),
            (vec![8, 1], RedisResponse::Boolean(true)),
            (
                [vec![9], (-1.25f64).to_le_bytes().to_vec()].concat(),
                RedisResponse::Float(-1.25),
            ),
            (
                bytes(10, b"123456789012345678901234567890"),
                RedisResponse::BigNumber(b"123456789012345678901234567890".to_vec()),
            ),
        ] {
            assert_eq!(decode(&encoded).unwrap(), expected);
        }
        for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let encoded = [vec![9], value.to_le_bytes().to_vec()].concat();
            let RedisResponse::Float(decoded) = decode(&encoded).unwrap() else {
                panic!("expected float")
            };
            assert!(decoded == value || (decoded.is_nan() && value.is_nan()));
        }
    }

    #[test]
    fn decodes_recursive_collections_without_losing_nested_errors() {
        let values = vec![bytes(1, b"value"), vec![7], bytes(13, b"WRONGTYPE nested")];
        let expected = vec![
            RedisResponse::Value(b"value".to_vec()),
            RedisResponse::Null,
            RedisResponse::Error(b"WRONGTYPE nested".to_vec()),
        ];
        for (tag, response) in [
            (3, RedisResponse::Array(expected.clone())),
            (5, RedisResponse::Set(expected.clone())),
            (6, RedisResponse::Push(expected.clone())),
        ] {
            assert_eq!(decode(&collection(tag, 3, &values)).unwrap(), response);
        }
        let map = collection(4, 1, &[bytes(1, b"key"), collection(3, 3, &values)]);
        assert_eq!(
            decode(&map).unwrap(),
            RedisResponse::Map(vec![(
                RedisResponse::Value(b"key".to_vec()),
                RedisResponse::Array(expected)
            )])
        );
        assert!(
            decode(&bytes(13, b"ERR root"))
                .unwrap_err()
                .0
                .contains("ERR root")
        );
        let trailing = [bytes(13, b"ERR root"), vec![7]].concat();
        assert!(decode(&trailing).unwrap_err().0.contains("trailing"));
    }

    #[test]
    fn decodes_verbatim_and_attributes() {
        let mut verbatim = vec![11];
        verbatim.extend_from_slice(&bytes(0, b"txt")[1..]);
        verbatim.extend_from_slice(&bytes(0, b"a\0b")[1..]);
        let value = RedisResponse::Verbatim {
            format: b"txt".to_vec(),
            value: b"a\0b".to_vec(),
        };
        assert_eq!(decode(&verbatim).unwrap(), value);
        let attribute = collection(
            12,
            1,
            &[
                bytes(1, b"ttl"),
                [vec![2], 7i64.to_le_bytes().to_vec()].concat(),
                verbatim,
            ],
        );
        assert_eq!(
            decode(&attribute).unwrap(),
            RedisResponse::Attribute {
                attributes: vec![(
                    RedisResponse::Value(b"ttl".to_vec()),
                    RedisResponse::Integer(7)
                )],
                data: Box::new(value),
            }
        );
        assert!(decode(&[12, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn rejects_every_truncated_prefix_and_trailing_data() {
        let fixtures = [
            bytes(1, b"hello"),
            [vec![2], 42i64.to_le_bytes().to_vec()].concat(),
            [vec![9], 1.25f64.to_le_bytes().to_vec()].concat(),
            collection(3, 2, &[bytes(1, b"binary\0"), vec![8, 1]]),
            collection(4, 1, &[bytes(1, b"key"), vec![7]]),
        ];
        for fixture in fixtures {
            for end in 0..fixture.len() {
                assert!(
                    decode(&fixture[..end]).is_err(),
                    "accepted prefix {end} of {fixture:?}"
                );
            }
            let mut trailing = fixture;
            trailing.push(7);
            assert!(decode(&trailing).is_err());
        }
    }

    #[test]
    fn rejects_invalid_tags_booleans_lengths_and_collection_counts() {
        for invalid in [vec![255], vec![8, 2], bytes(255, b"bad")] {
            assert!(decode(&invalid).is_err());
        }
        for tag in [3, 4, 5, 6, 12] {
            assert!(decode(&collection(tag, 65_536, &[])).is_err());
            assert!(decode(&collection(tag, u32::MAX, &[])).is_err());
        }
        for tag in [0, 1, 10, 11, 13] {
            assert!(decode(&collection(tag, u32::MAX, &[])).is_err());
        }
        assert!(decode(&collection(4, 1, &[vec![7]])).is_err());
        assert!(decode(&collection(12, 1, &[vec![7], vec![7]])).is_err());
    }

    #[test]
    fn enforces_depth_and_total_value_limits() {
        let mut nested = vec![7];
        for _ in 0..NATIVE_REDIS_MAX_NESTING_DEPTH {
            nested = collection(3, 1, &[nested]);
        }
        assert!(decode(&nested).is_ok());
        nested = collection(3, 1, &[nested]);
        assert!(decode(&nested).unwrap_err().0.contains("depth"));

        let values = collection(3, 2, &[collection(3, 1, &[vec![7]]), vec![7]]);
        let mut cursor = 0;
        assert!(parse_native_value(&values, &mut cursor, 0, &mut 3).is_err());
        let mut cursor = 0;
        assert!(parse_native_value(&values, &mut cursor, 0, &mut 4).is_ok());
    }
}
