#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-topk")]
impl EmbeddedStore {
    pub(crate) fn topk_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "TOPK.RESERVE" if args.len() == 2 || args.len() == 5 => {
                let Ok(k) = parse_usize_arg(args[1]) else {
                    return RedisModuleApiResult::Error("invalid topk value".to_string());
                };
                let (width, depth, decay) = if args.len() == 5 {
                    let Ok(width) = parse_usize_arg(args[2]) else {
                        return RedisModuleApiResult::Error("invalid width value".to_string());
                    };
                    let Ok(depth) = parse_usize_arg(args[3]) else {
                        return RedisModuleApiResult::Error("invalid depth value".to_string());
                    };
                    let Ok(decay) = parse_f64_arg(args[4]) else {
                        return RedisModuleApiResult::Error("invalid decay value".to_string());
                    };
                    (width, depth, decay)
                } else {
                    (8, 7, 0.9)
                };
                match self.topk_reserve(args[0], k, width, depth, decay) {
                    Ok(()) => RedisModuleApiResult::Simple("OK"),
                    Err(err) => RedisModuleApiResult::Error(format!("{err:?}")),
                }
            }
            "TOPK.ADD" if args.len() >= 2 => match self.topk_add(args[0], &args[1..]) {
                Ok(dropped) => RedisModuleApiResult::Array(
                    dropped
                        .into_iter()
                        .map(RedisModuleApiResult::Bulk)
                        .collect(),
                ),
                Err(err) => RedisModuleApiResult::Error(format!("{err:?}")),
            },
            "TOPK.INCRBY" if args.len() >= 3 && args[1..].len().is_multiple_of(2) => {
                let mut updates = Vec::with_capacity(args[1..].len() / 2);
                for pair in args[1..].chunks_exact(2) {
                    let Ok(increment) = parse_i64_arg(pair[1]) else {
                        return RedisModuleApiResult::Error("invalid increment".to_string());
                    };
                    updates.push((pair[0].to_vec(), increment));
                }
                match self.topk_incrby(args[0], &updates) {
                    Ok(dropped) => RedisModuleApiResult::Array(
                        dropped
                            .into_iter()
                            .map(RedisModuleApiResult::Bulk)
                            .collect(),
                    ),
                    Err(err) => RedisModuleApiResult::Error(format!("{err:?}")),
                }
            }
            "TOPK.QUERY" if args.len() >= 2 => match self.topk_query(args[0], &args[1..]) {
                Ok(values) => RedisModuleApiResult::Array(
                    values
                        .into_iter()
                        .map(|value| RedisModuleApiResult::Integer(if value { 1 } else { 0 }))
                        .collect(),
                ),
                Err(err) => RedisModuleApiResult::Error(format!("{err:?}")),
            },
            "TOPK.COUNT" if args.len() >= 2 => match self.topk_counts(args[0], &args[1..]) {
                Ok(values) => RedisModuleApiResult::Array(
                    values
                        .into_iter()
                        .map(RedisModuleApiResult::Integer)
                        .collect(),
                ),
                Err(err) => RedisModuleApiResult::Error(format!("{err:?}")),
            },
            "TOPK.LIST" if args.len() == 1 => match self.topk_list(args[0]) {
                Ok(entries) => RedisModuleApiResult::Array(
                    entries
                        .into_iter()
                        .map(|(item, _)| RedisModuleApiResult::Bulk(Some(item)))
                        .collect(),
                ),
                Err(err) => RedisModuleApiResult::Error(format!("{err:?}")),
            },
            "TOPK.LIST" if args.len() == 2 && args[1].eq_ignore_ascii_case(b"WITHCOUNT") => {
                match self.topk_list(args[0]) {
                    Ok(entries) => {
                        let mut items = Vec::with_capacity(entries.len() * 2);
                        for (item, count) in entries {
                            items.push(RedisModuleApiResult::Bulk(Some(item)));
                            items.push(RedisModuleApiResult::Integer(count));
                        }
                        RedisModuleApiResult::Array(items)
                    }
                    Err(err) => RedisModuleApiResult::Error(format!("{err:?}")),
                }
            }
            "TOPK.INFO" if args.len() == 1 => match self.topk_info(args[0]) {
                Ok(info) => RedisModuleApiResult::TopKInfo {
                    k: info.k,
                    width: info.width,
                    depth: info.depth,
                    decay: info.decay,
                },
                Err(err) => RedisModuleApiResult::Error(format!("{err:?}")),
            },
            _ => RedisModuleApiResult::Unsupported {
                family: RedisModuleFamily::TopK,
                command: command.to_string(),
            },
        }
    }
}

#[cfg(feature = "redis-module-topk")]
fn parse_usize_arg(raw: &[u8]) -> Result<usize, ()> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(())?;
    usize::try_from(value).map_err(|_| ())
}

#[cfg(feature = "redis-module-topk")]
fn parse_i64_arg(raw: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(())
}

#[cfg(feature = "redis-module-topk")]
fn parse_f64_arg(raw: &[u8]) -> Result<f64, ()> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .ok_or(())?;
    value.is_finite().then_some(value).ok_or(())
}
