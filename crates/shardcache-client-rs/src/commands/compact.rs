use std::io::Write;

use crate::error::Result;

#[cfg(feature = "redis")]
pub(crate) fn arg_list_len(args: &[&[u8]]) -> usize {
    var_u32_len(args.len() as u32)
        + args
            .iter()
            .map(|arg| var_u32_len(arg.len() as u32) + arg.len())
            .sum::<usize>()
}

#[cfg(feature = "redis")]
pub(crate) fn write_arg_list<W: Write>(w: &mut W, args: &[&[u8]]) -> Result<()> {
    write_var_u32(w, args.len() as u32)?;
    for arg in args {
        write_var_u32(w, arg.len() as u32)?;
        w.write_all(arg)?;
    }
    Ok(())
}

#[cfg(feature = "vector")]
pub(crate) fn owned_arg_list_len(args: &[Vec<u8>]) -> usize {
    var_u32_len(args.len() as u32)
        + args
            .iter()
            .map(|arg| var_u32_len(arg.len() as u32) + arg.len())
            .sum::<usize>()
}

#[cfg(feature = "vector")]
pub(crate) fn write_owned_arg_list<W: Write>(w: &mut W, args: &[Vec<u8>]) -> Result<()> {
    write_var_u32(w, args.len() as u32)?;
    for arg in args {
        write_var_u32(w, arg.len() as u32)?;
        w.write_all(arg)?;
    }
    Ok(())
}

fn var_u32_len(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn write_var_u32<W: Write>(w: &mut W, mut value: u32) -> Result<()> {
    while value >= 0x80 {
        w.write_all(&[((value as u8) & 0x7f) | 0x80])?;
        value >>= 7;
    }
    w.write_all(&[value as u8])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "redis")]
    fn borrowed_encoded_length_matches_compact_wire_body() {
        let args = [b"alpha".as_slice(), b"beta-beta".as_slice(), b"".as_slice()];
        let mut encoded = Vec::new();
        write_arg_list(&mut encoded, &args).unwrap();
        assert_eq!(encoded.len(), arg_list_len(&args));
        assert_eq!(encoded, b"\x03\x05alpha\x09beta-beta\x00");
    }

    #[cfg(feature = "vector")]
    #[test]
    fn owned_encoded_length_matches_compact_wire_body() {
        let args = vec![b"alpha".to_vec(), b"beta-beta".to_vec(), Vec::new()];
        let mut encoded = Vec::new();
        write_owned_arg_list(&mut encoded, &args).unwrap();
        assert_eq!(encoded.len(), owned_arg_list_len(&args));
        assert_eq!(encoded, b"\x03\x05alpha\x09beta-beta\x00");
    }
}
