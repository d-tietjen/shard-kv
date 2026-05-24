use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

pub(crate) struct ServerWire;

#[allow(dead_code)]
impl ServerWire {
    pub(crate) fn write_fast_ok(out: &mut BytesMut) {
        const HEADER: [u8; 8] = [
            FAST_RESPONSE_MAGIC,
            FAST_PROTOCOL_VERSION,
            FAST_STATUS_OK,
            0,
            0,
            0,
            0,
            0,
        ];
        out.extend_from_slice(&HEADER);
    }

    #[inline(always)]
    pub(crate) fn write_fast_null(out: &mut BytesMut) {
        const HEADER: [u8; 8] = [
            FAST_RESPONSE_MAGIC,
            FAST_PROTOCOL_VERSION,
            FAST_STATUS_NULL,
            0,
            0,
            0,
            0,
            0,
        ];
        out.extend_from_slice(&HEADER);
    }

    #[inline(always)]
    pub(crate) fn write_fast_integer(out: &mut BytesMut, value: i64) {
        const HEADER: [u8; 8] = [
            FAST_RESPONSE_MAGIC,
            FAST_PROTOCOL_VERSION,
            FAST_STATUS_INTEGER,
            0,
            8,
            0,
            0,
            0,
        ];
        #[cfg(not(feature = "unsafe"))]
        {
            out.extend_from_slice(&HEADER);
            out.extend_from_slice(&value.to_le_bytes());
        }
        #[cfg(feature = "unsafe")]
        {
            out.reserve(16);
            // SAFETY: reserve(16) ensures enough spare capacity for header + value.
            unsafe {
                let start = out.len();
                let dst = out.as_mut_ptr().add(start);
                std::ptr::copy_nonoverlapping(HEADER.as_ptr(), dst, HEADER.len());
                std::ptr::copy_nonoverlapping(value.to_le_bytes().as_ptr(), dst.add(8), 8);
                out.set_len(start + 16);
            }
        }
    }

    #[inline(always)]
    pub(crate) fn write_fast_error(out: &mut BytesMut, message: &str) {
        let len = (message.len() as u32).to_le_bytes();
        out.extend_from_slice(&[
            FAST_RESPONSE_MAGIC,
            FAST_PROTOCOL_VERSION,
            FAST_STATUS_ERROR,
            0,
            len[0],
            len[1],
            len[2],
            len[3],
        ]);
        out.extend_from_slice(message.as_bytes());
    }

    #[inline(always)]
    pub(super) fn write_fast_float(out: &mut BytesMut, value: f64) {
        out.extend_from_slice(&[
            FAST_RESPONSE_MAGIC,
            FAST_PROTOCOL_VERSION,
            FAST_STATUS_FLOAT,
            0,
            8,
            0,
            0,
            0,
        ]);
        out.extend_from_slice(&value.to_le_bytes());
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(super) fn write_fast_optional_value(out: &mut BytesMut, value: Option<&[u8]>) {
        match value {
            Some(value) => ServerWire::write_fast_value(out, value),
            None => ServerWire::write_fast_null(out),
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn begin_fast_array(out: &mut BytesMut, len: usize) -> usize {
        let start = out.len();
        out.extend_from_slice(&[
            FAST_RESPONSE_MAGIC,
            FAST_PROTOCOL_VERSION,
            FAST_STATUS_ARRAY,
            0,
            0,
            0,
            0,
            0,
        ]);
        out.extend_from_slice(&(len as u32).to_le_bytes());
        start
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn write_fast_array_item(out: &mut BytesMut, value: Option<&[u8]>) {
        match value {
            Some(value) => {
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value);
            }
            None => out.extend_from_slice(&u32::MAX.to_le_bytes()),
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn finish_fast_array(out: &mut BytesMut, start: usize) {
        let body_len = (out.len() - start - 8) as u32;
        out[start + 4..start + 8].copy_from_slice(&body_len.to_le_bytes());
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(super) fn write_fast_null_array(out: &mut BytesMut, len: usize) {
        let start = ServerWire::begin_fast_array(out, len);
        for _ in 0..len {
            ServerWire::write_fast_array_item(out, None);
        }
        ServerWire::finish_fast_array(out, start);
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn write_fast_empty_array(out: &mut BytesMut) {
        ServerWire::write_fast_null_array(out, 0);
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(super) fn write_fast_owned_array(out: &mut BytesMut, values: Vec<Option<Vec<u8>>>) {
        let start = ServerWire::begin_fast_array(out, values.len());
        for value in &values {
            ServerWire::write_fast_array_item(out, value.as_deref());
        }
        ServerWire::finish_fast_array(out, start);
    }

    #[cfg(feature = "embedded")]
    pub(super) fn write_fast_hello(out: &mut BytesMut) {
        ServerWire::write_fast_owned_array(
            out,
            vec![
                Some(b"server".to_vec()),
                Some(b"fast-cache".to_vec()),
                Some(b"version".to_vec()),
                Some(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
                Some(b"proto".to_vec()),
                Some(b"2".to_vec()),
                Some(b"id".to_vec()),
                Some(b"0".to_vec()),
                Some(b"mode".to_vec()),
                Some(b"standalone".to_vec()),
                Some(b"role".to_vec()),
                Some(b"master".to_vec()),
                Some(b"modules".to_vec()),
                Some(b"[]".to_vec()),
            ],
        );
    }

    #[cfg(feature = "embedded")]
    pub(super) fn write_fast_time(out: &mut BytesMut) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        ServerWire::write_fast_owned_array(
            out,
            vec![
                Some(now.as_secs().to_string().into_bytes()),
                Some(now.subsec_micros().to_string().into_bytes()),
            ],
        );
    }

    #[cfg(all(feature = "embedded", feature = "redis-compat"))]
    #[inline(always)]
    pub(super) fn finish_fast_object_read(
        out: &mut BytesMut,
        outcome: RedisObjectReadOutcome,
        write_missing: impl FnOnce(&mut BytesMut),
    ) {
        match outcome {
            RedisObjectReadOutcome::Written => {}
            RedisObjectReadOutcome::Missing => write_missing(out),
            RedisObjectReadOutcome::WrongType => {
                ServerWire::write_fast_error(out, WRONGTYPE_MESSAGE)
            }
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(super) fn write_fast_score(out: &mut BytesMut, score: Option<f64>) {
        match score {
            Some(score) => ServerWire::write_fast_float(out, score),
            None => ServerWire::write_fast_null(out),
        }
    }

    #[cfg(all(feature = "embedded", feature = "redis-compat"))]
    pub(super) fn write_fast_redis_object_result(out: &mut BytesMut, result: RedisObjectResult) {
        match result {
            RedisObjectResult::Simple("OK") => ServerWire::write_fast_ok(out),
            RedisObjectResult::Simple(message) if message.starts_with("ERR ") => {
                ServerWire::write_fast_error(out, message)
            }
            RedisObjectResult::Simple(message) => {
                ServerWire::write_fast_value(out, message.as_bytes())
            }
            RedisObjectResult::Integer(value) => ServerWire::write_fast_integer(out, value),
            RedisObjectResult::IntegerArray(values) => {
                ServerWire::write_fast_owned_array(
                    out,
                    values
                        .into_iter()
                        .map(|value| Some(value.to_string().into_bytes()))
                        .collect(),
                );
            }
            RedisObjectResult::Bulk(Some(value)) => ServerWire::write_fast_value(out, &value),
            RedisObjectResult::Bulk(None) => ServerWire::write_fast_null(out),
            RedisObjectResult::Array(values) => ServerWire::write_fast_owned_array(out, values),
            RedisObjectResult::WrongType => ServerWire::write_fast_error(out, WRONGTYPE_MESSAGE),
        }
    }

    #[inline(always)]
    pub(super) fn fast_value_header(payload_len: usize) -> [u8; 8] {
        let len = (payload_len as u32).to_le_bytes();
        [
            FAST_RESPONSE_MAGIC,
            FAST_PROTOCOL_VERSION,
            FAST_STATUS_VALUE,
            0,
            len[0],
            len[1],
            len[2],
            len[3],
        ]
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn begin_fast_value(out: &mut BytesMut) -> usize {
        let start = out.len();
        out.extend_from_slice(&[
            FAST_RESPONSE_MAGIC,
            FAST_PROTOCOL_VERSION,
            FAST_STATUS_VALUE,
            0,
            0,
            0,
            0,
            0,
        ]);
        start
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn finish_fast_value(out: &mut BytesMut, start: usize) {
        let payload_len = out.len().saturating_sub(start + 8);
        out[start + 4..start + 8].copy_from_slice(&(payload_len as u32).to_le_bytes());
    }

    #[inline(always)]
    pub(super) fn resp_blob_header(payload_len: usize) -> ([u8; RESP_HEADER_MAX_LEN], u8) {
        let mut header = [0_u8; RESP_HEADER_MAX_LEN];
        let mut len_buf = itoa::Buffer::new();
        let len = len_buf.format(payload_len).as_bytes();
        debug_assert!(1 + len.len() + 2 <= RESP_HEADER_MAX_LEN);

        header[0] = b'$';
        header[1..1 + len.len()].copy_from_slice(len);
        let end = 1 + len.len();
        header[end] = b'\r';
        header[end + 1] = b'\n';
        (header, (end + 2) as u8)
    }

    #[cfg(feature = "unsafe")]
    #[inline(always)]
    unsafe fn copy_64_unaligned(dst: *mut u8, src: *const u8) {
        for offset in [0usize, 8, 16, 24, 32, 40, 48, 56] {
            // SAFETY: caller guarantees both buffers are valid for 64 bytes.
            unsafe {
                let chunk = std::ptr::read_unaligned(src.add(offset).cast::<u64>());
                std::ptr::write_unaligned(dst.add(offset).cast::<u64>(), chunk);
            }
        }
    }

    #[inline(always)]
    pub(super) fn write_fast_value_64(out: &mut BytesMut, payload: &[u8]) {
        #[cfg(not(feature = "unsafe"))]
        {
            debug_assert_eq!(payload.len(), 64);
            out.extend_from_slice(&ServerWire::fast_value_header(64));
            out.extend_from_slice(payload);
        }
        #[cfg(feature = "unsafe")]
        {
            const TOTAL: usize = 72;
            const VALUE_64_HEADER_U64: u64 = 0x0000_0040_0004_02fb;

            debug_assert_eq!(payload.len(), 64);
            out.reserve(TOTAL);
            // SAFETY: reserve(TOTAL) ensures enough spare capacity for header + value,
            // and the caller only reaches this function for 64-byte payloads.
            unsafe {
                let start = out.len();
                let dst = out.as_mut_ptr().add(start);
                std::ptr::write_unaligned(dst.cast::<u64>(), VALUE_64_HEADER_U64);
                ServerWire::copy_64_unaligned(dst.add(8), payload.as_ptr());
                out.set_len(start + TOTAL);
            }
        }
    }

    #[inline(always)]
    pub(crate) fn write_fast_value(out: &mut BytesMut, payload: &[u8]) {
        #[cfg(not(feature = "unsafe"))]
        {
            out.extend_from_slice(&ServerWire::fast_value_header(payload.len()));
            out.extend_from_slice(payload);
        }
        #[cfg(feature = "unsafe")]
        {
            const VALUE_64_HEADER: [u8; 8] = [
                FAST_RESPONSE_MAGIC,
                FAST_PROTOCOL_VERSION,
                FAST_STATUS_VALUE,
                0,
                64,
                0,
                0,
                0,
            ];
            let total = 8 + payload.len();
            out.reserve(total);
            if payload.len() == 64 {
                // SAFETY: reserve(total) above reserves 72 bytes, and this branch
                // verifies that the payload is exactly 64 bytes.
                unsafe {
                    let start = out.len();
                    let dst = out.as_mut_ptr().add(start);
                    std::ptr::copy_nonoverlapping(VALUE_64_HEADER.as_ptr(), dst, 8);
                    ServerWire::copy_64_unaligned(dst.add(8), payload.as_ptr());
                    out.set_len(start + total);
                }
                return;
            }
            // SAFETY: reserve(total) ensures enough spare capacity for header + value.
            unsafe {
                let start = out.len();
                let dst = out.as_mut_ptr().add(start);
                *dst = FAST_RESPONSE_MAGIC;
                *dst.add(1) = FAST_PROTOCOL_VERSION;
                *dst.add(2) = FAST_STATUS_VALUE;
                *dst.add(3) = 0;
                std::ptr::write_unaligned(dst.add(4).cast::<u32>(), (payload.len() as u32).to_le());
                std::ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(8), payload.len());
                out.set_len(start + total);
            }
        }
    }

    pub(crate) fn write_resp_blob_string(out: &mut BytesMut, payload: &[u8]) {
        if payload.len() < 10 {
            let header = [b'$', b'0' + payload.len() as u8, b'\r', b'\n'];
            out.extend_from_slice(&header);
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n");
            return;
        }

        #[cfg(not(feature = "unsafe"))]
        {
            let mut buf = itoa::Buffer::new();
            let len_str = buf.format(payload.len()).as_bytes();
            out.extend_from_slice(b"$");
            out.extend_from_slice(len_str);
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(payload);
            out.extend_from_slice(b"\r\n");
        }
        #[cfg(feature = "unsafe")]
        {
            if payload.len() == 64 {
                const HEADER: &[u8] = b"$64\r\n";
                const TOTAL: usize = HEADER.len() + 64 + 2;

                out.reserve(TOTAL);
                // SAFETY: reserve(TOTAL) ensures `total` bytes of spare capacity.
                unsafe {
                    let start = out.len();
                    let dst = out.as_mut_ptr().add(start);
                    std::ptr::copy_nonoverlapping(HEADER.as_ptr(), dst, HEADER.len());
                    ServerWire::copy_64_unaligned(dst.add(HEADER.len()), payload.as_ptr());
                    *dst.add(HEADER.len() + 64) = b'\r';
                    *dst.add(HEADER.len() + 65) = b'\n';
                    out.set_len(start + TOTAL);
                }
                return;
            }

            let mut buf = itoa::Buffer::new();
            let len_str = buf.format(payload.len()).as_bytes();
            let total = 1 + len_str.len() + 2 + payload.len() + 2;
            out.reserve(total);
            // Single bulk write via direct buffer access. Avoids 5 separate
            // `extend_from_slice` calls each with bounds checking.
            unsafe {
                let start = out.len();
                let dst = out.as_mut_ptr().add(start);
                *dst = b'$';
                let mut pos = 1usize;
                std::ptr::copy_nonoverlapping(len_str.as_ptr(), dst.add(pos), len_str.len());
                pos += len_str.len();
                *dst.add(pos) = b'\r';
                *dst.add(pos + 1) = b'\n';
                pos += 2;
                std::ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(pos), payload.len());
                pos += payload.len();
                *dst.add(pos) = b'\r';
                *dst.add(pos + 1) = b'\n';
                out.set_len(start + total);
            }
        }
    }

    #[inline(always)]
    pub(crate) fn write_resp_integer(out: &mut BytesMut, value: i64) {
        let literal = match value {
            -2 => Some(b":-2\r\n".as_slice()),
            -1 => Some(b":-1\r\n".as_slice()),
            0 => Some(b":0\r\n".as_slice()),
            1 => Some(b":1\r\n".as_slice()),
            2 => Some(b":2\r\n".as_slice()),
            3 => Some(b":3\r\n".as_slice()),
            4 => Some(b":4\r\n".as_slice()),
            5 => Some(b":5\r\n".as_slice()),
            6 => Some(b":6\r\n".as_slice()),
            7 => Some(b":7\r\n".as_slice()),
            8 => Some(b":8\r\n".as_slice()),
            9 => Some(b":9\r\n".as_slice()),
            _ => None,
        };
        if let Some(literal) = literal {
            out.extend_from_slice(literal);
            return;
        }
        if let Ok(value) = u32::try_from(value)
            && value < 10_000
        {
            ServerWire::write_resp_small_positive_integer(out, value);
            return;
        }
        let mut buf = itoa::Buffer::new();
        let s = buf.format(value);
        let total = 1 + s.len() + 2;
        out.reserve(total);
        #[cfg(not(feature = "unsafe"))]
        {
            out.extend_from_slice(b":");
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        #[cfg(feature = "unsafe")]
        {
            // SAFETY: reserve(total) above guarantees enough spare capacity for
            // ':' + decimal digits + CRLF, and all writes stay within that range.
            unsafe {
                let start = out.len();
                let dst = out.as_mut_ptr().add(start);
                *dst = b':';
                std::ptr::copy_nonoverlapping(s.as_ptr(), dst.add(1), s.len());
                *dst.add(1 + s.len()) = b'\r';
                *dst.add(2 + s.len()) = b'\n';
                out.set_len(start + total);
            }
        }
    }

    #[inline(always)]
    pub(super) fn write_resp_small_positive_integer(out: &mut BytesMut, value: u32) {
        debug_assert!((10..10_000).contains(&value));
        #[cfg(not(feature = "unsafe"))]
        {
            let mut buffer = [0u8; 7];
            let len = ServerWire::encode_resp_small_positive_integer(value, &mut buffer);
            out.extend_from_slice(&buffer[..len]);
        }
        #[cfg(feature = "unsafe")]
        {
            let total = match value {
                0..=99 => 5,
                100..=999 => 6,
                _ => 7,
            };
            out.reserve(total);
            // SAFETY: reserve(total) guarantees the exact spare capacity needed
            // for ':' + digits + CRLF. The branch-selected `total` matches the
            // number of bytes written for the corresponding value range.
            unsafe {
                let start = out.len();
                let dst = out.as_mut_ptr().add(start);
                ServerWire::write_resp_small_positive_integer_ptr(dst, value);
                out.set_len(start + total);
            }
        }
    }

    #[cfg(not(feature = "unsafe"))]
    #[inline(always)]
    fn encode_resp_small_positive_integer(value: u32, out: &mut [u8; 7]) -> usize {
        out[0] = b':';
        match value {
            0..=99 => {
                out[1] = b'0' + (value / 10) as u8;
                out[2] = b'0' + (value % 10) as u8;
                out[3] = b'\r';
                out[4] = b'\n';
                5
            }
            100..=999 => {
                out[1] = b'0' + (value / 100) as u8;
                out[2] = b'0' + ((value / 10) % 10) as u8;
                out[3] = b'0' + (value % 10) as u8;
                out[4] = b'\r';
                out[5] = b'\n';
                6
            }
            _ => {
                out[1] = b'0' + (value / 1_000) as u8;
                out[2] = b'0' + ((value / 100) % 10) as u8;
                out[3] = b'0' + ((value / 10) % 10) as u8;
                out[4] = b'0' + (value % 10) as u8;
                out[5] = b'\r';
                out[6] = b'\n';
                7
            }
        }
    }

    #[cfg(feature = "unsafe")]
    #[inline(always)]
    unsafe fn write_resp_small_positive_integer_ptr(dst: *mut u8, value: u32) {
        // SAFETY: the caller guarantees `dst` points to enough initialized spare
        // buffer capacity for the selected value range.
        unsafe {
            *dst = b':';
            match value {
                0..=99 => {
                    *dst.add(1) = b'0' + (value / 10) as u8;
                    *dst.add(2) = b'0' + (value % 10) as u8;
                    *dst.add(3) = b'\r';
                    *dst.add(4) = b'\n';
                }
                100..=999 => {
                    *dst.add(1) = b'0' + (value / 100) as u8;
                    *dst.add(2) = b'0' + ((value / 10) % 10) as u8;
                    *dst.add(3) = b'0' + (value % 10) as u8;
                    *dst.add(4) = b'\r';
                    *dst.add(5) = b'\n';
                }
                _ => {
                    *dst.add(1) = b'0' + (value / 1_000) as u8;
                    *dst.add(2) = b'0' + ((value / 100) % 10) as u8;
                    *dst.add(3) = b'0' + ((value / 10) % 10) as u8;
                    *dst.add(4) = b'0' + (value % 10) as u8;
                    *dst.add(5) = b'\r';
                    *dst.add(6) = b'\n';
                }
            }
        }
    }

    #[inline]
    pub(crate) fn write_resp_error(out: &mut BytesMut, msg: &str) {
        out.reserve(1 + msg.len() + 2);
        out.extend_from_slice(b"-");
        out.extend_from_slice(msg.as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(super) fn write_resp_array_header(out: &mut BytesMut, len: usize) {
        out.extend_from_slice(b"*");
        let mut len_buf = itoa::Buffer::new();
        out.extend_from_slice(len_buf.format(len).as_bytes());
        out.extend_from_slice(b"\r\n");
    }

    #[cfg(feature = "embedded")]
    pub(super) fn write_resp_hello(out: &mut BytesMut) {
        ServerWire::write_resp_array_header(out, 14);
        ServerWire::write_resp_blob_string(out, b"server");
        ServerWire::write_resp_blob_string(out, b"fast-cache");
        ServerWire::write_resp_blob_string(out, b"version");
        ServerWire::write_resp_blob_string(out, env!("CARGO_PKG_VERSION").as_bytes());
        ServerWire::write_resp_blob_string(out, b"proto");
        ServerWire::write_resp_integer(out, 2);
        ServerWire::write_resp_blob_string(out, b"id");
        ServerWire::write_resp_integer(out, 0);
        ServerWire::write_resp_blob_string(out, b"mode");
        ServerWire::write_resp_blob_string(out, b"standalone");
        ServerWire::write_resp_blob_string(out, b"role");
        ServerWire::write_resp_blob_string(out, b"master");
        ServerWire::write_resp_blob_string(out, b"modules");
        ServerWire::write_resp_array_header(out, 0);
    }

    #[cfg(feature = "embedded")]
    pub(super) fn write_resp_time(out: &mut BytesMut) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        ServerWire::write_resp_array_header(out, 2);

        let mut seconds = itoa::Buffer::new();
        ServerWire::write_resp_blob_string(out, seconds.format(now.as_secs()).as_bytes());

        let mut micros = itoa::Buffer::new();
        ServerWire::write_resp_blob_string(out, micros.format(now.subsec_micros()).as_bytes());
    }

    #[cfg(all(feature = "embedded", feature = "redis-compat"))]
    #[inline(always)]
    pub(super) fn write_redis_object_result(out: &mut BytesMut, result: RedisObjectResult) {
        match result {
            RedisObjectResult::Simple("OK") => out.extend_from_slice(b"+OK\r\n"),
            RedisObjectResult::Simple(message) if message.starts_with("ERR ") => {
                ServerWire::write_resp_error(out, message)
            }
            RedisObjectResult::Simple(message) => {
                ServerWire::write_resp_blob_string(out, message.as_bytes())
            }
            RedisObjectResult::Integer(value) => ServerWire::write_resp_integer(out, value),
            RedisObjectResult::IntegerArray(values) => {
                ServerWire::write_resp_array_header(out, values.len());
                for value in values {
                    ServerWire::write_resp_integer(out, value);
                }
            }
            RedisObjectResult::Bulk(Some(value)) => ServerWire::write_resp_blob_string(out, &value),
            RedisObjectResult::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
            RedisObjectResult::Array(values) => {
                ServerWire::write_resp_array_header(out, values.len());
                for value in values {
                    match value {
                        Some(value) => ServerWire::write_resp_blob_string(out, &value),
                        None => out.extend_from_slice(b"$-1\r\n"),
                    }
                }
            }
            RedisObjectResult::WrongType => ServerWire::write_resp_error(out, WRONGTYPE_MESSAGE),
        }
    }

    #[cfg(feature = "embedded")]
    pub(super) fn parse_score(score: &[u8]) -> std::result::Result<f64, String> {
        if let Some(value) = ServerWire::parse_integer_score_fast(score) {
            return Ok(value);
        }
        let text = std::str::from_utf8(score)
            .map_err(|error| format!("ERR ZADD score is not valid UTF-8: {error}"))?;
        let value = text
            .parse::<f64>()
            .map_err(|error| format!("ERR ZADD score is not a float: {error}"))?;
        if value.is_nan() {
            return Err("ERR ZADD score is not a valid float".into());
        }
        Ok(value)
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    fn parse_integer_score_fast(score: &[u8]) -> Option<f64> {
        let (negative, digits) = match score.split_first()? {
            (b'-', tail) if !tail.is_empty() => (true, tail),
            _ => (false, score),
        };
        if digits.len() > 18 {
            return None;
        }
        let mut value = 0u64;
        for &byte in digits {
            if !byte.is_ascii_digit() {
                return None;
            }
            value = value
                .saturating_mul(10)
                .saturating_add((byte - b'0') as u64);
        }
        let value = value as f64;
        Some(if negative { -value } else { value })
    }
}
