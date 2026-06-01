use bytes::Bytes;
#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_frame;
use crate::commands::redis::{
    bulk, define_redis_command, eq_ignore_ascii_case, error, int, parse_i64, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

define_redis_command!(Lcs, "LCS", false);

impl crate::commands::redis::RedisCommand for Lcs {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        execute_lcs(store, args)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key_a, key_b, option] if eq_ignore_ascii_case(option, b"LEN") => {
                let value_a = match string_bytes_or_empty(store, key_a) {
                    Ok(value) => value,
                    Err(frame) => {
                        write_frame(out, &frame);
                        return;
                    }
                };
                let value_b = match string_bytes_or_empty(store, key_b) {
                    Ok(value) => value,
                    Err(frame) => {
                        write_frame(out, &frame);
                        return;
                    }
                };
                ServerWire::write_resp_integer(out, lcs_length(&value_a, &value_b) as i64);
            }
            _ => write_frame(out, &execute_lcs(store, args)),
        }
    }
}

struct LcsMatch {
    a_start: usize,
    a_end: usize,
    b_start: usize,
    b_end: usize,
    len: usize,
}

/// Shared LCS evaluation used by both `LCS` and `STRALGO LCS`.
pub(crate) fn execute_lcs(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let [key_a, key_b, options @ ..] = args else {
        return wrong_arity("LCS");
    };

    let mut want_len = false;
    let mut want_idx = false;
    let mut with_match_len = false;
    let mut min_match_len: i64 = 0;
    let mut index = 0;
    while index < options.len() {
        let option = options[index];
        match option {
            option if eq_ignore_ascii_case(option, b"LEN") => {
                want_len = true;
                index += 1;
            }
            option if eq_ignore_ascii_case(option, b"IDX") => {
                want_idx = true;
                index += 1;
            }
            option if eq_ignore_ascii_case(option, b"WITHMATCHLEN") => {
                with_match_len = true;
                index += 1;
            }
            option if eq_ignore_ascii_case(option, b"MINMATCHLEN") => {
                let Some(raw) = options.get(index + 1) else {
                    return error("ERR syntax error");
                };
                match parse_i64(raw) {
                    Ok(value) => min_match_len = value.max(0),
                    Err(_) => return error("ERR value is not an integer or out of range"),
                }
                index += 2;
            }
            _ => return error("ERR syntax error"),
        }
    }

    if want_len && want_idx {
        return error("ERR If you want both the length and indexes, please just use IDX.");
    }

    let value_a = match string_bytes_or_empty(store, key_a) {
        Ok(value) => value,
        Err(frame) => return frame,
    };
    let value_b = match string_bytes_or_empty(store, key_b) {
        Ok(value) => value,
        Err(frame) => return frame,
    };

    // The LEN-only reply just needs the scalar length, so use a rolling two-row DP
    // (O(min(a, b)) memory) and skip backtracking entirely. The IDX and default
    // (subsequence) replies need the full table to reconstruct matches.
    if want_len {
        return int(lcs_length(&value_a, &value_b) as i64);
    }

    let (sequence, matches, total) = longest_common_subsequence(&value_a, &value_b);

    if want_idx {
        let min_match_len = min_match_len as usize;
        let match_frames = matches
            .into_iter()
            .filter(|entry| entry.len >= min_match_len)
            .map(|entry| {
                let mut parts = vec![
                    Frame::Array(vec![int(entry.a_start as i64), int(entry.a_end as i64)]),
                    Frame::Array(vec![int(entry.b_start as i64), int(entry.b_end as i64)]),
                ];
                if with_match_len {
                    parts.push(int(entry.len as i64));
                }
                Frame::Array(parts)
            })
            .collect();
        return Frame::Array(vec![
            bulk(b"matches".to_vec()),
            Frame::Array(match_frames),
            bulk(b"len".to_vec()),
            int(total as i64),
        ]);
    }

    bulk(sequence)
}

fn string_bytes_or_empty(store: &EmbeddedStore, key: &[u8]) -> Result<Bytes, Frame> {
    let mut value = None;
    match store.get_string_value_into(key, |bytes| value = Some(bytes.clone())) {
        RedisStringLookup::Hit => Ok(value.unwrap_or_default()),
        RedisStringLookup::Miss => Ok(Bytes::new()),
        RedisStringLookup::WrongType => Err(wrongtype()),
    }
}

/// Length-only LCS using a single rolling row. The narrower input becomes the
/// row width so memory stays at O(min(a, b)) regardless of argument order. The
/// diagonal and left neighbours are carried in registers, so the inner loop
/// touches memory just once per cell (read+write of `row[j]`).
fn lcs_length(a: &[u8], b: &[u8]) -> usize {
    let (outer, inner) = if a.len() >= b.len() { (a, b) } else { (b, a) };
    if inner.is_empty() {
        return 0;
    }
    let width = inner.len();

    const STACK_ROW_WIDTH: usize = 64;
    if width <= STACK_ROW_WIDTH {
        let mut row = [0u32; STACK_ROW_WIDTH];
        return lcs_length_with_row(outer, inner, &mut row[..width]);
    }

    let mut row = vec![0u32; width];
    lcs_length_with_row(outer, inner, &mut row)
}

fn lcs_length_with_row(outer: &[u8], inner: &[u8], row: &mut [u32]) -> usize {
    for &outer_byte in outer {
        let mut diag = 0u32; // dp[i-1][j-1]
        let mut left = 0u32; // dp[i][j-1]
        for (cell, &inner_byte) in row.iter_mut().zip(inner) {
            let up = *cell; // dp[i-1][j], overwritten below
            let val = if outer_byte == inner_byte {
                diag + 1
            } else {
                up.max(left)
            };
            *cell = val;
            diag = up;
            left = val;
        }
    }
    row[inner.len() - 1] as usize
}

fn longest_common_subsequence(a: &[u8], b: &[u8]) -> (Vec<u8>, Vec<LcsMatch>, usize) {
    let alen = a.len();
    let blen = b.len();
    let cols = blen + 1;
    let mut dp = vec![0u32; (alen + 1) * cols];
    let at = |i: usize, j: usize| i * cols + j;
    for i in 1..=alen {
        for j in 1..=blen {
            dp[at(i, j)] = if a[i - 1] == b[j - 1] {
                dp[at(i - 1, j - 1)] + 1
            } else {
                dp[at(i - 1, j)].max(dp[at(i, j - 1)])
            };
        }
    }

    let total = dp[at(alen, blen)] as usize;
    let mut sequence = Vec::with_capacity(total);
    let mut matches: Vec<LcsMatch> = Vec::new();
    let mut range: Option<LcsMatch> = None;
    let mut i = alen;
    let mut j = blen;
    while i > 0 && j > 0 {
        match () {
            _ if a[i - 1] == b[j - 1] => {
                sequence.push(a[i - 1]);
                let ai = i - 1;
                let bi = j - 1;
                match range.as_mut() {
                    Some(current) if current.a_start == ai + 1 && current.b_start == bi + 1 => {
                        current.a_start = ai;
                        current.b_start = bi;
                        current.len += 1;
                    }
                    _ => {
                        if let Some(finished) = range.take() {
                            matches.push(finished);
                        }
                        range = Some(LcsMatch {
                            a_start: ai,
                            a_end: ai,
                            b_start: bi,
                            b_end: bi,
                            len: 1,
                        });
                    }
                }
                i -= 1;
                j -= 1;
            }
            _ if dp[at(i - 1, j)] >= dp[at(i, j - 1)] => {
                i -= 1;
            }
            _ => {
                j -= 1;
            }
        }
    }
    if let Some(finished) = range.take() {
        matches.push(finished);
    }
    sequence.reverse();
    (sequence, matches, total)
}
