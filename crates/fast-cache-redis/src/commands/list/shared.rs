#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_frame;
use crate::commands::redis::{
    bulk, error, frame_from_result, parse_usize, write_result_resp, wrong_arity,
};
use crate::protocol::Frame;
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

pub(crate) fn blocking_pop(
    store: &EmbeddedStore,
    args: &[&[u8]],
    front: bool,
    name: &str,
) -> Frame {
    if args.len() < 2 {
        return wrong_arity(name);
    }
    for key in &args[..args.len() - 1] {
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
