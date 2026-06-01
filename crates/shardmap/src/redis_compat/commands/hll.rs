use std::collections::BTreeSet;

use crate::commands::redis::{
    array_bulk, bulk, error, int, optional_string_value, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisStringStore};

const HLL_PREFIX: &[u8] = b"FC:HLL:v1\0";

macro_rules! define_hll_command {
    ($type:ident, $static_name:ident, $name:literal, $mutates:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static $static_name: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;
        }
    };
}

define_hll_command!(PFAdd, PFADD_COMMAND, "PFADD", true);
define_hll_command!(PFCount, PFCOUNT_COMMAND, "PFCOUNT", false);
define_hll_command!(PFMerge, PFMERGE_COMMAND, "PFMERGE", true);
define_hll_command!(PFDebug, PFDEBUG_COMMAND, "PFDEBUG", false);
define_hll_command!(PFSelfTest, PFSELFTEST_COMMAND, "PFSELFTEST", false);

impl crate::commands::redis::RedisCommand for PFAdd {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, elements @ ..] = args else {
            return wrong_arity("PFADD");
        };
        let result = store.transform_string_value_no_ttl(
            key,
            |existing| {
                let mut members = decode_hll(existing)?;
                let before = members.len();
                for element in elements {
                    members.insert(element.to_vec());
                }
                Ok(((members.len() != before) as i64, encode_hll(&members)))
            },
            wrongtype,
        );
        match result {
            Ok(value) => int(value),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for PFCount {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.is_empty() {
            return wrong_arity("PFCOUNT");
        }
        let mut members = BTreeSet::new();
        for key in args {
            match optional_string_value(store, key, true) {
                Ok(Some(value)) => match decode_hll(Some(&value)) {
                    Ok(values) => members.extend(values),
                    Err(frame) => return frame,
                },
                Ok(None) => {}
                Err(frame) => return frame,
            }
        }
        int(members.len() as i64)
    }
}

impl crate::commands::redis::RedisCommand for PFMerge {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [dest, sources @ ..] = args else {
            return wrong_arity("PFMERGE");
        };
        let mut members = BTreeSet::new();
        for source in sources {
            match optional_string_value(store, source, true) {
                Ok(Some(value)) => match decode_hll(Some(&value)) {
                    Ok(values) => members.extend(values),
                    Err(frame) => return frame,
                },
                Ok(None) => {}
                Err(frame) => return frame,
            }
        }
        let encoded = encode_hll(&members);
        match store.transform_string_value_no_ttl(dest, |_| Ok(((), encoded)), wrongtype) {
            Ok(()) => crate::commands::redis::simple("OK"),
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for PFDebug {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub, key] if sub.eq_ignore_ascii_case(b"ENCODING") => {
                match optional_string_value(store, key, true) {
                    Ok(Some(value)) => match decode_hll(Some(&value)) {
                        Ok(_) => bulk(b"shardcache-exact".to_vec()),
                        Err(frame) => frame,
                    },
                    Ok(None) => bulk(b"empty".to_vec()),
                    Err(frame) => frame,
                }
            }
            [sub, key] if sub.eq_ignore_ascii_case(b"DECODE") => {
                match optional_string_value(store, key, true) {
                    Ok(Some(value)) => match decode_hll(Some(&value)) {
                        Ok(values) => array_bulk(
                            values
                                .into_iter()
                                .map(|member| {
                                    let mut line = b"member ".to_vec();
                                    line.extend_from_slice(&member);
                                    line
                                })
                                .collect(),
                        ),
                        Err(frame) => frame,
                    },
                    Ok(None) => Frame::Array(Vec::new()),
                    Err(frame) => frame,
                }
            }
            [sub, key] if sub.eq_ignore_ascii_case(b"GETREG") => {
                match optional_string_value(store, key, true) {
                    Ok(Some(value)) => match decode_hll(Some(&value)) {
                        Ok(values) => Frame::Array(
                            values
                                .into_iter()
                                .map(
                                    |member| int((xxhash_rust::xxh3::xxh3_64(&member) & 63) as i64),
                                )
                                .collect(),
                        ),
                        Err(frame) => frame,
                    },
                    Ok(None) => Frame::Array(Vec::new()),
                    Err(frame) => frame,
                }
            }
            _ => error("ERR unknown PFDEBUG subcommand or wrong number of arguments"),
        }
    }
}

impl crate::commands::redis::RedisCommand for PFSelfTest {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => crate::commands::redis::simple("OK"),
            _ => wrong_arity("PFSELFTEST"),
        }
    }
}

fn decode_hll(existing: Option<&[u8]>) -> Result<BTreeSet<Vec<u8>>, Frame> {
    let Some(value) = existing else {
        return Ok(BTreeSet::new());
    };
    if !value.starts_with(HLL_PREFIX) {
        return Err(error(
            "WRONGTYPE Key is not a valid HyperLogLog string value.",
        ));
    }
    let mut cursor = HLL_PREFIX.len();
    let count = read_u32(value, &mut cursor)? as usize;
    let mut members = BTreeSet::new();
    for _ in 0..count {
        let len = read_u32(value, &mut cursor)? as usize;
        let end = cursor
            .checked_add(len)
            .ok_or_else(|| error("WRONGTYPE Key is not a valid HyperLogLog string value."))?;
        if end > value.len() {
            return Err(error(
                "WRONGTYPE Key is not a valid HyperLogLog string value.",
            ));
        }
        members.insert(value[cursor..end].to_vec());
        cursor = end;
    }
    if cursor != value.len() {
        return Err(error(
            "WRONGTYPE Key is not a valid HyperLogLog string value.",
        ));
    }
    Ok(members)
}

fn encode_hll(members: &BTreeSet<Vec<u8>>) -> Vec<u8> {
    let mut out = Vec::with_capacity(HLL_PREFIX.len().saturating_add(members.len() * 12));
    out.extend_from_slice(HLL_PREFIX);
    out.extend_from_slice(&(members.len() as u32).to_le_bytes());
    for member in members {
        out.extend_from_slice(&(member.len() as u32).to_le_bytes());
        out.extend_from_slice(member);
    }
    out
}

fn read_u32(value: &[u8], cursor: &mut usize) -> Result<u32, Frame> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| error("WRONGTYPE Key is not a valid HyperLogLog string value."))?;
    let bytes = value
        .get(*cursor..end)
        .ok_or_else(|| error("WRONGTYPE Key is not a valid HyperLogLog string value."))?;
    *cursor = end;
    Ok(u32::from_le_bytes(
        bytes.try_into().expect("slice length was checked"),
    ))
}
