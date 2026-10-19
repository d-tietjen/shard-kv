//! Fixed-header read experiments, independent of production WAL/snapshot decoding.
//! `read_from_prefix` copies these small headers; it is not payload zero-copy proof.

use crate::backend::BoxError;
use zerocopy::FromBytes;
use zerocopy::byteorder::{LE, U32, U64};

#[derive(Clone, Copy, FromBytes)]
#[repr(C)]
struct FrameBytes {
    magic: [u8; 4],
    flags: u8,
    payload_len: [u8; 4],
}

#[derive(Clone, Copy, FromBytes)]
#[repr(C)]
struct LegacyBytes {
    op: u8,
    key_len: [u8; 4],
    value_len: [u8; 4],
    timestamp: [u8; 8],
    expiration: [u8; 8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrameFields {
    magic: [u8; 4],
    flags: u8,
    payload_len: u32,
}

impl FrameFields {
    fn witness(self) -> u64 {
        u32::from_le_bytes(self.magic) as u64 + self.flags as u64 + self.payload_len as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LegacyFields {
    op: u8,
    key_len: u32,
    value_len: u32,
    timestamp: u64,
    expiration: i64,
}

impl LegacyFields {
    fn witness(self) -> u64 {
        self.timestamp
            .wrapping_add(self.expiration as u64)
            .wrapping_add(self.key_len as u64)
            .wrapping_add(self.value_len as u64)
            .wrapping_add(self.op as u64)
    }
}

fn frame_native(bytes: &[u8]) -> Option<FrameFields> {
    Some(FrameFields {
        magic: bytes.get(..4)?.try_into().ok()?,
        flags: *bytes.get(4)?,
        payload_len: u32::from_le_bytes(bytes.get(5..9)?.try_into().ok()?),
    })
}

fn frame_typed(bytes: &[u8]) -> Option<FrameFields> {
    let (header, _) = FrameBytes::read_from_prefix(bytes).ok()?;
    Some(FrameFields {
        magic: header.magic,
        flags: header.flags,
        payload_len: u32::from_le_bytes(header.payload_len),
    })
}

fn legacy_native(bytes: &[u8]) -> Option<LegacyFields> {
    Some(LegacyFields {
        op: *bytes.first()?,
        key_len: u32::from_le_bytes(bytes.get(1..5)?.try_into().ok()?),
        value_len: u32::from_le_bytes(bytes.get(5..9)?.try_into().ok()?),
        timestamp: u64::from_le_bytes(bytes.get(9..17)?.try_into().ok()?),
        expiration: i64::from_le_bytes(bytes.get(17..25)?.try_into().ok()?),
    })
}

fn legacy_typed(bytes: &[u8]) -> Option<LegacyFields> {
    let (header, _) = LegacyBytes::read_from_prefix(bytes).ok()?;
    Some(LegacyFields {
        op: header.op,
        key_len: u32::from_le_bytes(header.key_len),
        value_len: u32::from_le_bytes(header.value_len),
        timestamp: u64::from_le_bytes(header.timestamp),
        expiration: i64::from_le_bytes(header.expiration),
    })
}

/// Layouts explored by the inherited fixed-header experiments.
#[derive(Debug, Clone, Copy)]
pub enum HeaderKind {
    Frame,
    Legacy,
    U32,
    U64,
}

impl HeaderKind {
    /// Reads the fixed prefix using checked scalar byte conversions.
    pub fn native(self, bytes: &[u8]) -> Result<u64, BoxError> {
        match self {
            Self::Frame => frame_native(bytes).map(FrameFields::witness),
            Self::Legacy => legacy_native(bytes).map(LegacyFields::witness),
            Self::U32 => bytes
                .get(..4)
                .and_then(|b| b.try_into().ok())
                .map(u32::from_le_bytes)
                .map(u64::from),
            Self::U64 => bytes
                .get(..8)
                .and_then(|b| b.try_into().ok())
                .map(u64::from_le_bytes),
        }
        .ok_or_else(|| "truncated fixed header".into())
    }

    /// Reads the same prefix with the inherited typed-header approach.
    pub fn typed(self, bytes: &[u8]) -> Result<u64, BoxError> {
        match self {
            Self::Frame => frame_typed(bytes).map(FrameFields::witness),
            Self::Legacy => legacy_typed(bytes).map(LegacyFields::witness),
            Self::U32 => U32::<LE>::read_from_prefix(bytes)
                .ok()
                .map(|(value, _)| value.get() as u64),
            Self::U64 => U64::<LE>::read_from_prefix(bytes)
                .ok()
                .map(|(value, _)| value.get()),
        }
        .ok_or_else(|| "truncated fixed header".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_and_native_prefixes_agree_on_byte_order_and_every_truncation() {
        let mut frame = b"TEST\x01".to_vec();
        frame.extend_from_slice(&0x12345678u32.to_le_bytes());
        assert_eq!(
            frame_typed(&frame),
            Some(FrameFields {
                magic: *b"TEST",
                flags: 1,
                payload_len: 0x12345678
            })
        );
        assert_eq!(frame_native(&frame), frame_typed(&frame));
        let mut legacy = vec![2];
        legacy.extend_from_slice(&0x12345678u32.to_le_bytes());
        legacy.extend_from_slice(&0x87654321u32.to_le_bytes());
        legacy.extend_from_slice(&0x123456789abcdef0u64.to_le_bytes());
        legacy.extend_from_slice(&(-2i64).to_le_bytes());
        assert_eq!(
            legacy_typed(&legacy),
            Some(LegacyFields {
                op: 2,
                key_len: 0x12345678,
                value_len: 0x87654321,
                timestamp: 0x123456789abcdef0,
                expiration: -2
            })
        );
        assert_eq!(legacy_native(&legacy), legacy_typed(&legacy));
        for (kind, bytes) in [
            (HeaderKind::Frame, frame),
            (HeaderKind::Legacy, legacy),
            (HeaderKind::U32, 0x12345678u32.to_le_bytes().to_vec()),
            (
                HeaderKind::U64,
                0x123456789abcdef0u64.to_le_bytes().to_vec(),
            ),
        ] {
            for end in 0..bytes.len() {
                assert!(kind.native(&bytes[..end]).is_err());
                assert!(kind.typed(&bytes[..end]).is_err());
            }
            let expected = kind.native(&bytes).unwrap();
            assert_eq!(kind.typed(&bytes).unwrap(), expected);
            let mut trailing = bytes.clone();
            trailing.push(255);
            assert_eq!(kind.native(&trailing).unwrap(), expected);
            assert_eq!(kind.typed(&trailing).unwrap(), expected);
        }
        assert_eq!(HeaderKind::U32.typed(&[1, 2, 3, 4]).unwrap(), 0x04030201);
        assert_eq!(
            HeaderKind::U64.native(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap(),
            0x0807060504030201
        );
    }
}
