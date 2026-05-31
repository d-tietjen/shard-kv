#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-roaring")]
impl EmbeddedStore {
    pub(crate) fn roaring_api_execute(
        &self,
        command: &str,
        args: &[&[u8]],
    ) -> RedisModuleApiResult {
        let cmd = command.to_ascii_uppercase();
        match cmd.as_str() {
            "R.SETBIT" | "R64.SETBIT" if args.len() >= 2 => {
                let Some(bit) = parse_u64_lossy(args[1]) else {
                    return invalid_arg("invalid bit");
                };
                let set = args.get(2).is_none_or(|value| *value != b"0");
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let bits = shard.bits.entry(args[0].to_vec()).or_default();
                let previous = bits.contains(&bit);
                if set {
                    bits.insert(bit);
                } else {
                    bits.remove(&bit);
                }
                RedisModuleApiResult::Integer(if previous { 1 } else { 0 })
            }
            "R.GETBIT" | "R64.GETBIT" | "R.CONTAINS" | "R64.CONTAINS" if args.len() >= 2 => {
                let Some(bit) = parse_u64_lossy(args[1]) else {
                    return invalid_arg("invalid bit");
                };
                let exists = self.roaring_bits(args[0]).contains(&bit);
                RedisModuleApiResult::Integer(if exists { 1 } else { 0 })
            }
            "R.SETBITARRAY" | "R64.SETBITARRAY" | "R.SETINTARRAY" | "R64.SETINTARRAY"
            | "R.APPENDINTARRAY" | "R64.APPENDINTARRAY"
                if args.len() >= 2 =>
            {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let bits = shard.bits.entry(args[0].to_vec()).or_default();
                if cmd.contains("SET") {
                    bits.clear();
                }
                for raw in &args[1..] {
                    let Some(bit) = parse_u64_lossy(raw) else {
                        return invalid_arg("invalid integer");
                    };
                    bits.insert(bit);
                }
                RedisModuleApiResult::Integer(bits.len() as i64)
            }
            "R.DELETEINTARRAY" | "R64.DELETEINTARRAY" | "R.CLEARBITS" | "R64.CLEARBITS"
                if args.len() >= 2 =>
            {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let Some(bits) = shard.bits.get_mut(args[0]) else {
                    return RedisModuleApiResult::Integer(0);
                };
                let mut removed = 0;
                for raw in &args[1..] {
                    if parse_u64_lossy(raw).is_some_and(|bit| bits.remove(&bit)) {
                        removed += 1;
                    }
                }
                RedisModuleApiResult::Integer(removed)
            }
            "R.GETBITARRAY" | "R64.GETBITARRAY" | "R.GETINTARRAY" | "R64.GETINTARRAY"
            | "R.GETBITS" | "R64.GETBITS"
                if !args.is_empty() =>
            {
                RedisModuleApiResult::Array(
                    self.roaring_bits(args[0])
                        .into_iter()
                        .map(|bit| RedisModuleApiResult::Integer(bit as i64))
                        .collect(),
                )
            }
            "R.RANGEINTARRAY" | "R64.RANGEINTARRAY" if args.len() >= 3 => {
                let start = parse_u64_lossy(args[1]).unwrap_or(0);
                let end = parse_u64_lossy(args[2]).unwrap_or(u64::MAX);
                RedisModuleApiResult::Array(
                    self.roaring_bits(args[0])
                        .range(start..=end)
                        .map(|bit| RedisModuleApiResult::Integer(*bit as i64))
                        .collect(),
                )
            }
            "R.BITCOUNT" | "R64.BITCOUNT" if !args.is_empty() => {
                RedisModuleApiResult::Integer(self.roaring_bits(args[0]).len() as i64)
            }
            "R.BITPOS" | "R64.BITPOS" if args.len() >= 2 => {
                let Some(target_bit) = parse_u64_lossy(args[1]) else {
                    return invalid_arg("invalid bit value");
                };
                if target_bit > 1 {
                    return invalid_arg("bit must be 0 or 1");
                }
                let start = args
                    .get(2)
                    .and_then(|raw| parse_u64_lossy(raw))
                    .unwrap_or(0);
                let end = args
                    .get(3)
                    .and_then(|raw| parse_u64_lossy(raw))
                    .unwrap_or(u64::MAX);
                if start > end {
                    return RedisModuleApiResult::Integer(-1);
                }
                let bits = self.roaring_bits(args[0]);
                if target_bit == 1 {
                    bits.range(start..=end)
                        .next()
                        .copied()
                        .map_or(RedisModuleApiResult::Integer(-1), |bit| {
                            RedisModuleApiResult::Integer(bit as i64)
                        })
                } else {
                    let mut candidate = start;
                    for bit in bits.range(start..=end) {
                        if *bit > candidate {
                            return RedisModuleApiResult::Integer(candidate as i64);
                        }
                        if *bit == u64::MAX {
                            break;
                        }
                        candidate = bit.saturating_add(1);
                    }
                    if candidate <= end {
                        RedisModuleApiResult::Integer(candidate as i64)
                    } else {
                        RedisModuleApiResult::Integer(-1)
                    }
                }
            }
            "R.MIN" | "R64.MIN" if !args.is_empty() => self
                .roaring_bits(args[0])
                .first()
                .copied()
                .map_or_else(result_null, |bit| RedisModuleApiResult::Integer(bit as i64)),
            "R.MAX" | "R64.MAX" if !args.is_empty() => self
                .roaring_bits(args[0])
                .last()
                .copied()
                .map_or_else(result_null, |bit| RedisModuleApiResult::Integer(bit as i64)),
            "R.CLEAR" | "R64.CLEAR" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let removed = self
                    .module_state
                    .write(route)
                    .bits
                    .remove(args[0])
                    .is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            "R.SETRANGE" | "R64.SETRANGE" if args.len() >= 3 => {
                let start = parse_u64_lossy(args[1]).unwrap_or(0);
                let end = parse_u64_lossy(args[2]).unwrap_or(start);
                if end.saturating_sub(start) > 100_000 {
                    return invalid_arg("range too large");
                }
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let bits = shard.bits.entry(args[0].to_vec()).or_default();
                for bit in start..=end {
                    bits.insert(bit);
                }
                RedisModuleApiResult::Integer(bits.len() as i64)
            }
            "R.SETFULL" | "R64.SETFULL" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .bits
                    .entry(args[0].to_vec())
                    .or_default()
                    .insert(0);
                RedisModuleApiResult::Simple("OK")
            }
            "R.DIFF" | "R64.DIFF" if args.len() >= 2 => {
                let mut bits = self.roaring_bits(args[0]);
                for key in &args[1..] {
                    for bit in self.roaring_bits(key) {
                        bits.remove(&bit);
                    }
                }
                RedisModuleApiResult::Array(
                    bits.into_iter()
                        .map(|bit| RedisModuleApiResult::Integer(bit as i64))
                        .collect(),
                )
            }
            "R.BITOP" | "R64.BITOP" if args.len() >= 4 => {
                let op = args[0];
                let dest = args[1];
                let mut result = self.roaring_bits(args[2]);
                for key in &args[3..] {
                    let other = self.roaring_bits(key);
                    if bytes_eq(op, b"AND") {
                        result = result.intersection(&other).copied().collect();
                    } else if bytes_eq(op, b"XOR") {
                        result = result.symmetric_difference(&other).copied().collect();
                    } else if bytes_eq(op, b"DIFF") {
                        result = result.difference(&other).copied().collect();
                    } else {
                        result.extend(other);
                    }
                }
                let route = self.route_key(dest);
                let len = result.len();
                self.module_state
                    .write(route)
                    .bits
                    .insert(dest.to_vec(), result);
                RedisModuleApiResult::Integer(len as i64)
            }
            "R.JACCARD" | "R64.JACCARD" if args.len() >= 2 => {
                let left = self.roaring_bits(args[0]);
                let right = self.roaring_bits(args[1]);
                let intersection = left.intersection(&right).count();
                let union = left.union(&right).count();
                let score = if union == 0 {
                    1.0
                } else {
                    intersection as f64 / union as f64
                };
                result_bulk_string(score.to_string())
            }
            "R.STAT" if !args.is_empty() => {
                let len = self.roaring_bits(args[0]).len();
                RedisModuleApiResult::Array(vec![
                    result_bulk_string("cardinality"),
                    RedisModuleApiResult::Integer(len as i64),
                ])
            }
            "R.OPTIMIZE" | "R64.OPTIMIZE" => RedisModuleApiResult::Simple("OK"),
            _ => self.module_record_command(RedisModuleFamily::RedisRoaring, command, args),
        }
    }

    fn roaring_bits(&self, key: &[u8]) -> BTreeSet<u64> {
        let route = self.route_key(key);
        self.module_state
            .read(route)
            .bits
            .get(key)
            .cloned()
            .unwrap_or_default()
    }
}
