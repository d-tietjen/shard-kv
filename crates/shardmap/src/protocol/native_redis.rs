//! Bounded encoding of Redis response values for SCNP.

use super::{FastResponse, Frame};
use crate::{Result, ShardCacheError};

pub(crate) fn encode_native_redis_frame(
    frame: &Frame,
) -> std::result::Result<Vec<u8>, &'static str> {
    let mut body = Vec::new();
    let mut remaining_values = NATIVE_REDIS_MAX_VALUES;
    encode_native_frame(&mut body, frame, 0, &mut remaining_values)?;
    Ok(body)
}

impl FastResponse {
    pub(crate) fn from_redis_frame(frame: Frame) -> Result<Self> {
        let response =
            match frame {
                Frame::SimpleString(ref value) if value == "OK" => Self::Ok,
                Frame::BlobString(value) if value.len() <= NATIVE_REDIS_MAX_BODY_BYTES => {
                    Self::Value(value)
                }
                Frame::BlobString(_) => {
                    return Err(ShardCacheError::Protocol(
                        "native Redis response exceeds body limit".into(),
                    ));
                }
                Frame::Integer(value) => Self::Integer(value),
                Frame::Null => Self::Null,
                Frame::Boolean(value) => Self::Boolean(value),
                Frame::Double(value) => Self::Float(value.parse().map_err(|_| {
                    ShardCacheError::Protocol("invalid native Redis double".into())
                })?),
                Frame::Error(message) => Self::Error(message.into_bytes()),
                frame => Self::NativeFrame(
                    encode_native_redis_frame(&frame)
                        .map_err(|error| ShardCacheError::Protocol(error.into()))?,
                ),
            };
        Ok(response)
    }
}

const NATIVE_REDIS_MAX_BODY_BYTES: usize = 256 * 1024 * 1024;
const NATIVE_REDIS_MAX_VALUES: usize = 1_048_576;

fn encode_native_frame(
    out: &mut Vec<u8>,
    frame: &Frame,
    depth: usize,
    remaining_values: &mut usize,
) -> std::result::Result<(), &'static str> {
    if depth > 128 || *remaining_values == 0 {
        return Err("ERR native Redis response exceeds nesting or value limit");
    }
    *remaining_values -= 1;
    // A scalar header needs at most nine bytes. Variable payloads are checked
    // separately before copying them into the bounded temporary frame.
    native_room(out, 9)?;
    match frame {
        Frame::SimpleString(value) => {
            out.extend_from_slice(&[0]);
            write_native_bytes(out, value.as_bytes())?;
        }
        Frame::BlobString(value) => {
            out.extend_from_slice(&[1]);
            write_native_bytes(out, value)?;
        }
        Frame::Integer(value) => {
            out.extend_from_slice(&[2]);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Frame::Array(values) | Frame::Set(values) | Frame::Push(values) => {
            let tag = match frame {
                Frame::Array(_) => 3,
                Frame::Set(_) => 5,
                _ => 6,
            };
            out.extend_from_slice(&[tag]);
            write_native_count(out, values.len())?;
            for value in values {
                encode_native_frame(out, value, depth + 1, remaining_values)?;
            }
        }
        Frame::Map(entries) => {
            out.extend_from_slice(&[4]);
            write_native_count(out, entries.len())?;
            for (key, value) in entries {
                encode_native_frame(out, key, depth + 1, remaining_values)?;
                encode_native_frame(out, value, depth + 1, remaining_values)?;
            }
        }
        Frame::Null => out.extend_from_slice(&[7]),
        Frame::Boolean(value) => out.extend_from_slice(&[8, *value as u8]),
        Frame::Double(value) => {
            let value = value
                .parse::<f64>()
                .map_err(|_| "ERR invalid native Redis double")?;
            out.extend_from_slice(&[9]);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Frame::BigNumber(value) => {
            out.extend_from_slice(&[10]);
            write_native_bytes(out, value.as_bytes())?;
        }
        Frame::VerbatimString { format, value } => {
            out.extend_from_slice(&[11]);
            write_native_bytes(out, format.as_bytes())?;
            write_native_bytes(out, value)?;
        }
        Frame::Attribute { attributes, data } => {
            out.extend_from_slice(&[12]);
            write_native_count(out, attributes.len())?;
            for (key, value) in attributes {
                encode_native_frame(out, key, depth + 1, remaining_values)?;
                encode_native_frame(out, value, depth + 1, remaining_values)?;
            }
            encode_native_frame(out, data, depth + 1, remaining_values)?;
        }
        Frame::Error(message) => {
            out.extend_from_slice(&[13]);
            write_native_bytes(out, message.as_bytes())?;
        }
    }
    Ok(())
}

fn native_room(out: &mut Vec<u8>, additional: usize) -> std::result::Result<(), &'static str> {
    if additional > NATIVE_REDIS_MAX_BODY_BYTES.saturating_sub(out.len()) {
        return Err("ERR native Redis response exceeds body limit");
    }
    out.try_reserve(additional)
        .map_err(|_| "ERR native Redis response allocation failed")?;
    Ok(())
}

fn write_native_count(out: &mut Vec<u8>, count: usize) -> std::result::Result<(), &'static str> {
    if count > 65_536 {
        return Err("ERR native Redis response exceeds collection limit");
    }
    out.extend_from_slice(&(count as u32).to_le_bytes());
    Ok(())
}

fn write_native_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> std::result::Result<(), &'static str> {
    native_room(out, 4)?;
    native_room(out, 4usize.saturating_add(bytes.len()))?;
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardcache_client_rs::RedisResponse;

    #[test]
    fn recursive_server_encoding_round_trips_through_client_decoder() {
        let frame = Frame::Attribute {
            attributes: vec![(Frame::SimpleString("meta".into()), Frame::Boolean(true))],
            data: Box::new(Frame::Array(vec![
                Frame::Map(vec![(
                    Frame::BlobString(b"key\0".to_vec()),
                    Frame::Integer(-42),
                )]),
                Frame::Set(vec![Frame::Null]),
                Frame::Push(vec![Frame::SimpleString("message".into())]),
                Frame::Double("1.25".into()),
                Frame::BigNumber("12345678901234567890".into()),
                Frame::VerbatimString {
                    format: "txt".into(),
                    value: b"v\xff".to_vec(),
                },
                Frame::Error("ERR nested".into()),
            ])),
        };
        let expected = RedisResponse::Attribute {
            attributes: vec![(
                RedisResponse::Value(b"meta".to_vec()),
                RedisResponse::Boolean(true),
            )],
            data: Box::new(RedisResponse::Array(vec![
                RedisResponse::Map(vec![(
                    RedisResponse::Value(b"key\0".to_vec()),
                    RedisResponse::Integer(-42),
                )]),
                RedisResponse::Set(vec![RedisResponse::Null]),
                RedisResponse::Push(vec![RedisResponse::Value(b"message".to_vec())]),
                RedisResponse::Float(1.25),
                RedisResponse::BigNumber(b"12345678901234567890".to_vec()),
                RedisResponse::Verbatim {
                    format: b"txt".to_vec(),
                    value: b"v\xff".to_vec(),
                },
                RedisResponse::Error(b"ERR nested".to_vec()),
            ])),
        };
        let FastResponse::NativeFrame(body) = FastResponse::from_redis_frame(frame).unwrap() else {
            panic!("expected recursive frame");
        };
        assert_eq!(RedisResponse::from_native_bytes(&body).unwrap(), expected);
    }

    #[test]
    fn engine_response_conversion_preserves_scalar_statuses() {
        for (frame, expected) in [
            (Frame::SimpleString("OK".into()), FastResponse::Ok),
            (Frame::Null, FastResponse::Null),
            (Frame::Integer(-7), FastResponse::Integer(-7)),
            (Frame::Boolean(true), FastResponse::Boolean(true)),
            (Frame::Double("1.25".into()), FastResponse::Float(1.25)),
            (
                Frame::BlobString(b"v\0".to_vec()),
                FastResponse::Value(b"v\0".to_vec()),
            ),
            (
                Frame::Error("ERR expected".into()),
                FastResponse::Error(b"ERR expected".to_vec()),
            ),
        ] {
            assert_eq!(FastResponse::from_redis_frame(frame).unwrap(), expected);
        }
    }

    #[test]
    fn encoder_enforces_depth_values_collections_and_body_limits() {
        let mut nested = Frame::Null;
        for _ in 0..128 {
            nested = Frame::Array(vec![nested]);
        }
        assert!(encode_native_redis_frame(&nested).is_ok());
        nested = Frame::Array(vec![nested]);
        assert!(encode_native_redis_frame(&nested).is_err());
        assert!(encode_native_redis_frame(&Frame::Array(vec![Frame::Null; 65_537])).is_err());
        assert!(encode_native_redis_frame(&Frame::Double("invalid".into())).is_err());
        assert!(FastResponse::from_redis_frame(Frame::Double("invalid".into())).is_err());
        let mut out = Vec::new();
        assert!(
            encode_native_frame(&mut out, &Frame::Array(vec![Frame::Null; 2]), 0, &mut 2).is_err()
        );
        assert!(native_room(&mut Vec::new(), NATIVE_REDIS_MAX_BODY_BYTES + 1).is_err());
    }
}
