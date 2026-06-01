#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_frame;
use crate::commands::redis::{
    bulk, error, frame_from_result, parse_usize, write_resp_wrong_arity, write_result_resp,
    wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

pub(crate) fn push_list(
    store: &EmbeddedStore,
    args: &[&[u8]],
    front: bool,
    existing: bool,
    name: &str,
) -> Frame {
    if args.len() < 2 {
        return wrong_arity(name);
    }
    let result = match (front, existing) {
        (true, false) => store.lpush(args[0], &args[1..]),
        (false, false) => store.rpush(args[0], &args[1..]),
        (true, true) => store.lpushx(args[0], &args[1..]),
        (false, true) => store.rpushx(args[0], &args[1..]),
    };
    frame_from_result(result)
}

#[cfg(feature = "server")]
pub(crate) fn write_push_list_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    front: bool,
    existing: bool,
    name: &str,
    out: &mut BytesMut,
) {
    if args.len() < 2 {
        write_frame(out, &wrong_arity(name));
        return;
    }
    let result = match (front, existing) {
        (true, false) => store.lpush(args[0], &args[1..]),
        (false, false) => store.rpush(args[0], &args[1..]),
        (true, true) => store.lpushx(args[0], &args[1..]),
        (false, true) => store.rpushx(args[0], &args[1..]),
    };
    write_result_resp(out, result);
}

pub(crate) fn pop_list(store: &EmbeddedStore, args: &[&[u8]], front: bool, name: &str) -> Frame {
    match args {
        [key] => frame_from_result(if front {
            store.lpop(key)
        } else {
            store.rpop(key)
        }),
        [key, count] => match parse_usize(count) {
            Ok(count) => frame_from_result(if front {
                store.lpop_count(key, count)
            } else {
                store.rpop_count(key, count)
            }),
            Err(_) => error("ERR value is not an integer or out of range"),
        },
        _ => wrong_arity(name),
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_pop_list_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    front: bool,
    name: &str,
    out: &mut BytesMut,
) {
    match args {
        [key] => write_result_resp(
            out,
            if front {
                store.lpop(key)
            } else {
                store.rpop(key)
            },
        ),
        [key, count] => match parse_usize(count) {
            Ok(count) => write_result_resp(
                out,
                if front {
                    store.lpop_count(key, count)
                } else {
                    store.rpop_count(key, count)
                },
            ),
            Err(_) => {
                ServerWire::write_resp_error(out, "ERR value is not an integer or out of range")
            }
        },
        _ => write_resp_wrong_arity(out, name),
    }
}

pub(crate) fn blocking_pop(
    store: &EmbeddedStore,
    args: &[&[u8]],
    front: bool,
    name: &str,
) -> Frame {
    if args.len() < 2 {
        return wrong_arity(name);
    }
    let keys = &args[..args.len() - 1];
    let timeout = match crate::commands::blocking::parse_blocking_timeout(args[args.len() - 1]) {
        Ok(timeout) => timeout,
        Err(frame) => return frame,
    };
    let frame = blocking_pop_once(store, keys, front);
    if !matches!(frame, Frame::Null) {
        return frame;
    }
    let shard_id = match crate::commands::blocking::single_shard_for_keys(store, keys) {
        Ok(shard_id) => shard_id,
        Err(frame) => return frame,
    };
    crate::commands::blocking::block_on_shard(store, shard_id, timeout, || {
        blocking_pop_once(store, keys, front)
    })
}

fn blocking_pop_once(store: &EmbeddedStore, keys: &[&[u8]], front: bool) -> Frame {
    for key in keys {
        let popped = if front {
            store.lpop(key)
        } else {
            store.rpop(key)
        };
        match frame_from_result(popped) {
            Frame::BlobString(value) => {
                return Frame::Array(vec![bulk((*key).to_vec()), bulk(value)]);
            }
            Frame::Error(error) => return Frame::Error(error),
            _ => {}
        }
    }
    Frame::Null
}

pub(crate) fn list_mpop(
    store: &EmbeddedStore,
    args: &[&[u8]],
    blocking: bool,
    name: &str,
) -> Frame {
    let parsed = match parse_list_mpop_args(args, blocking, name) {
        Ok(parsed) => parsed,
        Err(frame) => return frame,
    };
    if blocking {
        let frame = list_mpop_once(store, parsed);
        if !matches!(frame, Frame::Null) {
            return frame;
        }
        let shard_id = match crate::commands::blocking::single_shard_for_keys(store, parsed.keys) {
            Ok(shard_id) => shard_id,
            Err(frame) => return frame,
        };
        crate::commands::blocking::block_on_shard(store, shard_id, parsed.timeout, || {
            list_mpop_once(store, parsed)
        })
    } else {
        list_mpop_once(store, parsed)
    }
}

fn list_mpop_once(store: &EmbeddedStore, parsed: ListMpopArgs<'_>) -> Frame {
    for key in parsed.keys {
        let popped = if parsed.front {
            store.lpop_count(key, parsed.count)
        } else {
            store.rpop_count(key, parsed.count)
        };
        match frame_from_result(popped) {
            Frame::Array(values) if !values.is_empty() => {
                return Frame::Array(vec![bulk((*key).to_vec()), Frame::Array(values)]);
            }
            Frame::Error(error) => return Frame::Error(error),
            _ => {}
        }
    }
    Frame::Null
}

#[derive(Clone, Copy)]
struct ListMpopArgs<'a> {
    keys: &'a [&'a [u8]],
    front: bool,
    count: usize,
    timeout: Option<std::time::Duration>,
}

fn parse_list_mpop_args<'a>(
    args: &'a [&'a [u8]],
    blocking: bool,
    name: &str,
) -> std::result::Result<ListMpopArgs<'a>, Frame> {
    let offset = usize::from(blocking);
    if args.len() < offset + 3 {
        return Err(wrong_arity(name));
    }
    let timeout = match blocking {
        true => crate::commands::blocking::parse_blocking_timeout(args[0])?,
        false => None,
    };
    let Ok(numkeys) = parse_usize(args[offset]) else {
        return Err(error("ERR value is not an integer or out of range"));
    };
    if numkeys == 0 {
        return Err(error("ERR numkeys should be greater than 0"));
    }
    let direction_index = offset + 1 + numkeys;
    if args.len() <= direction_index {
        return Err(error("ERR syntax error"));
    }
    let front = match args[direction_index] {
        value if crate::commands::redis::eq_ignore_ascii_case(value, b"LEFT") => true,
        value if crate::commands::redis::eq_ignore_ascii_case(value, b"RIGHT") => false,
        _ => return Err(error("ERR syntax error")),
    };
    let mut count = 1usize;
    let mut index = direction_index + 1;
    while index < args.len() {
        if crate::commands::redis::eq_ignore_ascii_case(args[index], b"COUNT")
            && index + 1 < args.len()
        {
            let Ok(parsed) = parse_usize(args[index + 1]) else {
                return Err(error("ERR value is not an integer or out of range"));
            };
            if parsed == 0 {
                return Err(error("ERR count should be greater than 0"));
            }
            count = parsed;
            index += 2;
            continue;
        }
        return Err(error("ERR syntax error"));
    }
    Ok(ListMpopArgs {
        keys: &args[offset + 1..direction_index],
        front,
        count,
        timeout,
    })
}

#[cfg(feature = "server")]
pub(crate) fn write_blocking_pop_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    front: bool,
    name: &str,
    out: &mut BytesMut,
) {
    if args.len() < 2 {
        write_resp_wrong_arity(out, name);
        return;
    }
    write_frame(out, &blocking_pop(store, args, front, name));
}

#[cfg(feature = "server")]
pub(crate) fn write_list_mpop_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    blocking: bool,
    name: &str,
    out: &mut BytesMut,
) {
    write_frame(out, &list_mpop(store, args, blocking, name));
}
