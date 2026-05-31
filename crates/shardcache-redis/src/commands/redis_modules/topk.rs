use super::{EnabledModuleInfo, RedisModuleCommand, module_command_error};
use crate::commands::redis::{
    bulk, eq_ignore_ascii_case, error, int, parse_f64, parse_i64, parse_usize, simple, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "topk",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "topk";
    "TOPK.ADD" => true,
    "TOPK.COUNT" => false,
    "TOPK.INCRBY" => true,
    "TOPK.INFO" => false,
    "TOPK.LIST" => false,
    "TOPK.QUERY" => false,
    "TOPK.RESERVE" => true,
];

#[cfg(feature = "redis-module-topk")]
pub(super) fn execute(name: &str, store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match name {
        name if eq_ignore_ascii_case(name.as_bytes(), b"TOPK.RESERVE") => topk_reserve(store, args),
        name if eq_ignore_ascii_case(name.as_bytes(), b"TOPK.ADD") => topk_add(store, args),
        name if eq_ignore_ascii_case(name.as_bytes(), b"TOPK.INCRBY") => topk_incrby(store, args),
        name if eq_ignore_ascii_case(name.as_bytes(), b"TOPK.QUERY") => topk_query(store, args),
        name if eq_ignore_ascii_case(name.as_bytes(), b"TOPK.COUNT") => topk_count(store, args),
        name if eq_ignore_ascii_case(name.as_bytes(), b"TOPK.LIST") => topk_list(store, args),
        name if eq_ignore_ascii_case(name.as_bytes(), b"TOPK.INFO") => topk_info(store, args),
        _ => module_command_error("topk"),
    }
}

#[cfg(feature = "redis-module-topk")]
fn topk_reserve(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let ([key, k] | [key, k, _, _, _]) = args else {
        return wrong_arity("TOPK.RESERVE");
    };
    let k = match parse_usize(k) {
        Ok(k) => k,
        Err(()) => return error("ERR invalid topk value"),
    };
    let (width, depth, decay) = match args {
        [_, _] => (8, 7, 0.9),
        [_, _, width, depth, decay] => {
            let width = match parse_usize(width) {
                Ok(width) => width,
                Err(()) => return error("ERR invalid width value"),
            };
            let depth = match parse_usize(depth) {
                Ok(depth) => depth,
                Err(()) => return error("ERR invalid depth value"),
            };
            let decay = match parse_f64(decay) {
                Ok(decay) => decay,
                Err(()) => return error("ERR invalid decay value"),
            };
            (width, depth, decay)
        }
        _ => unreachable!(),
    };
    match store.topk_reserve(key, k, width, depth, decay) {
        Ok(()) => simple("OK"),
        Err(crate::storage::TopKError::AlreadyExists) => error("ERR item exists"),
        Err(crate::storage::TopKError::WrongType) => error(crate::storage::WRONGTYPE_MESSAGE),
        Err(crate::storage::TopKError::InvalidArgument) => {
            error("ERR invalid TOPK.RESERVE argument")
        }
        Err(crate::storage::TopKError::MissingKey) => error("ERR no such key"),
    }
}

#[cfg(feature = "redis-module-topk")]
fn topk_add(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let Some((key, items)) = args.split_first() else {
        return wrong_arity("TOPK.ADD");
    };
    if items.is_empty() {
        return wrong_arity("TOPK.ADD");
    }
    topk_dropped_array(store.topk_add(key, items))
}

#[cfg(feature = "redis-module-topk")]
fn topk_incrby(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let Some((key, rest)) = args.split_first() else {
        return wrong_arity("TOPK.INCRBY");
    };
    if rest.is_empty() || rest.len() % 2 != 0 {
        return wrong_arity("TOPK.INCRBY");
    }
    let mut updates = Vec::with_capacity(rest.len() / 2);
    for pair in rest.chunks_exact(2) {
        let increment = match parse_i64(pair[1]) {
            Ok(increment) => increment,
            Err(()) => return error("ERR invalid increment"),
        };
        updates.push((pair[0].to_vec(), increment));
    }
    topk_dropped_array(store.topk_incrby(key, &updates))
}

#[cfg(feature = "redis-module-topk")]
fn topk_query(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let Some((key, items)) = args.split_first() else {
        return wrong_arity("TOPK.QUERY");
    };
    if items.is_empty() {
        return wrong_arity("TOPK.QUERY");
    }
    match store.topk_query(key, items) {
        Ok(values) => Frame::Array(
            values
                .into_iter()
                .map(|value| int(if value { 1 } else { 0 }))
                .collect(),
        ),
        Err(err) => topk_error_frame(err),
    }
}

#[cfg(feature = "redis-module-topk")]
fn topk_count(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let Some((key, items)) = args.split_first() else {
        return wrong_arity("TOPK.COUNT");
    };
    if items.is_empty() {
        return wrong_arity("TOPK.COUNT");
    }
    match store.topk_counts(key, items) {
        Ok(values) => Frame::Array(values.into_iter().map(int).collect()),
        Err(err) => topk_error_frame(err),
    }
}

#[cfg(feature = "redis-module-topk")]
fn topk_list(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let ([key] | [key, _]) = args else {
        return wrong_arity("TOPK.LIST");
    };
    let with_count = match args {
        [_] => false,
        [_, option] if eq_ignore_ascii_case(option, b"WITHCOUNT") => true,
        [_, _] => return error("ERR syntax error"),
        _ => unreachable!(),
    };
    match store.topk_list(key) {
        Ok(entries) => {
            let mut frames = Vec::with_capacity(if with_count {
                entries.len() * 2
            } else {
                entries.len()
            });
            for (item, count) in entries {
                frames.push(bulk(item));
                if with_count {
                    frames.push(int(count));
                }
            }
            Frame::Array(frames)
        }
        Err(err) => topk_error_frame(err),
    }
}

#[cfg(feature = "redis-module-topk")]
fn topk_info(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let [key] = args else {
        return wrong_arity("TOPK.INFO");
    };
    match store.topk_info(key) {
        Ok(info) => Frame::Array(vec![
            simple("k"),
            int(info.k as i64),
            simple("width"),
            int(info.width as i64),
            simple("depth"),
            int(info.depth as i64),
            simple("decay"),
            simple(&format!("{:.17}", info.decay)),
        ]),
        Err(err) => topk_error_frame(err),
    }
}

#[cfg(feature = "redis-module-topk")]
fn topk_dropped_array(
    result: std::result::Result<Vec<Option<Vec<u8>>>, crate::storage::TopKError>,
) -> Frame {
    match result {
        Ok(values) => Frame::Array(
            values
                .into_iter()
                .map(|value| value.map(bulk).unwrap_or(Frame::Null))
                .collect(),
        ),
        Err(err) => topk_error_frame(err),
    }
}

#[cfg(feature = "redis-module-topk")]
fn topk_error_frame(err: crate::storage::TopKError) -> Frame {
    match err {
        crate::storage::TopKError::AlreadyExists => error("ERR item exists"),
        crate::storage::TopKError::MissingKey => error("ERR no such key"),
        crate::storage::TopKError::WrongType => error(crate::storage::WRONGTYPE_MESSAGE),
        crate::storage::TopKError::InvalidArgument => error("ERR invalid TopK argument"),
    }
}
