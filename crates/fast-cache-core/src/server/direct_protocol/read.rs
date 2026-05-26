use super::*;

/// ```lean, anneal, spec
/// requires (h_bound): offset.val + 4 <= buf.len.val
/// ```
impl DirectProtocol {
    #[inline(always)]
    pub(in crate::server) unsafe fn read_le_u32_at(buf: &[u8], offset: usize) -> u32 {
        debug_assert!(buf.len() >= offset + 4);
        #[cfg(not(feature = "unsafe"))]
        {
            u32::from_le_bytes(
                buf[offset..offset + 4]
                    .try_into()
                    .expect("validated u32 read has four bytes"),
            )
        }
        #[cfg(feature = "unsafe")]
        // SAFETY: callers check that at least four bytes are available.
        {
            u32::from_le(unsafe {
                std::ptr::read_unaligned(buf.as_ptr().add(offset).cast::<u32>())
            })
        }
    }
}

/// Reads `$<digits>\r\n` and returns (digits_value, total_consumed_incl_dollar).
/// Keeps the common 1-4 digit path branch-light, with a bounded tail for
/// larger RESP bulk strings that should still stay on the direct server path.
impl DirectProtocol {
    #[inline(always)]
    pub(in crate::server) fn read_short_int_header(buf: &[u8]) -> Option<(usize, usize)> {
        if buf.len() < 4 || buf[0] != b'$' {
            return None;
        }
        let (val, consumed) = DirectProtocol::read_short_int_header_no_dollar(&buf[1..])?;
        Some((val, 1 + consumed))
    }
}

/// Reads `<digits>\r\n` and returns (value, consumed).
impl DirectProtocol {
    #[inline(always)]
    pub(in crate::server) fn read_short_int_header_no_dollar(buf: &[u8]) -> Option<(usize, usize)> {
        let b0 = buf.first().copied()?;
        if !b0.is_ascii_digit() {
            return None;
        }
        let d0 = (b0 - b'0') as usize;
        if buf.len() >= 3 && buf[1] == b'\r' && buf[2] == b'\n' {
            return Some((d0, 3));
        }
        let b1 = *buf.get(1)?;
        if !b1.is_ascii_digit() {
            return None;
        }
        let d1 = (b1 - b'0') as usize;
        if buf.len() >= 4 && buf[2] == b'\r' && buf[3] == b'\n' {
            return Some((d0 * 10 + d1, 4));
        }
        let b2 = *buf.get(2)?;
        if !b2.is_ascii_digit() {
            return None;
        }
        let d2 = (b2 - b'0') as usize;
        if buf.len() >= 5 && buf[3] == b'\r' && buf[4] == b'\n' {
            return Some((d0 * 100 + d1 * 10 + d2, 5));
        }
        let b3 = *buf.get(3)?;
        if !b3.is_ascii_digit() {
            return None;
        }
        let d3 = (b3 - b'0') as usize;
        if buf.len() >= 6 && buf[4] == b'\r' && buf[5] == b'\n' {
            return Some((d0 * 1000 + d1 * 100 + d2 * 10 + d3, 6));
        }
        let mut value = d0 * 1000 + d1 * 100 + d2 * 10 + d3;
        let mut index = 4;
        while index < 10 {
            let byte = *buf.get(index)?;
            if byte == b'\r' {
                if buf.get(index + 1).copied() == Some(b'\n') {
                    return Some((value, index + 2));
                }
                return None;
            }
            if !byte.is_ascii_digit() {
                return None;
            }
            value = value * 10 + (byte - b'0') as usize;
            index += 1;
        }
        None
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn read_resp_array_header(buf: &[u8]) -> Option<(usize, usize)> {
        if buf.len() < 4 || buf[0] != b'*' {
            return None;
        }
        let (value, consumed) = DirectProtocol::read_short_int_header_no_dollar(&buf[1..])?;
        Some((value, 1 + consumed))
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn read_resp_bulk_arg<'a>(
        buf: &'a [u8],
        pos: &mut usize,
    ) -> Option<&'a [u8]> {
        let (len, consumed) = DirectProtocol::read_short_int_header(buf.get(*pos..)?)?;
        *pos += consumed;
        if buf.len() < *pos + len + 2 {
            return None;
        }
        let value = &buf[*pos..*pos + len];
        *pos += len;
        if buf[*pos] != b'\r' || buf[*pos + 1] != b'\n' {
            return None;
        }
        *pos += 2;
        Some(value)
    }
}
