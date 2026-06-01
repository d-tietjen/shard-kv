use bytes::BytesMut;

use crate::commands::redis::{
    bulk, define_redis_command, eq_ignore_ascii_case, error, frame_from_result, write_frame,
    write_resp_null, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectResult};

define_redis_command!(LMove, "LMOVE", true);

impl crate::commands::redis::RedisCommand for LMove {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [source, dest, source_side, dest_side] => {
                execute_lmove(store, source, dest, source_side, dest_side)
            }
            _ => wrong_arity("LMOVE"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [source, dest, source_side, dest_side] = args else {
            write_frame(out, &wrong_arity("LMOVE"));
            return;
        };
        write_lmove_resp(store, source, dest, source_side, dest_side, out);
    }
}

pub(crate) fn execute_lmove(
    store: &EmbeddedStore,
    source: &[u8],
    dest: &[u8],
    source_side: &[u8],
    dest_side: &[u8],
) -> Frame {
    let Some(from_front) = parse_list_side(source_side) else {
        return error("ERR syntax error");
    };
    let Some(to_front) = parse_list_side(dest_side) else {
        return error("ERR syntax error");
    };
    move_between_lists(store, source, dest, from_front, to_front)
}

pub(crate) fn move_between_lists(
    store: &EmbeddedStore,
    source: &[u8],
    dest: &[u8],
    from_front: bool,
    to_front: bool,
) -> Frame {
    match store.redis_type(source) {
        "none" => return Frame::Null,
        "list" => {}
        _ => return wrongtype(),
    }
    match store.redis_type(dest) {
        "none" | "list" => {}
        _ => return wrongtype(),
    }

    let popped = if from_front {
        store.lpop(source)
    } else {
        store.rpop(source)
    };
    match frame_from_result(popped) {
        Frame::BlobString(value) => {
            let values = [value.as_slice()];
            let result = if to_front {
                store.lpush(dest, &values)
            } else {
                store.rpush(dest, &values)
            };
            match result {
                RedisObjectResult::WrongType => wrongtype(),
                _ => bulk(value),
            }
        }
        Frame::Null => Frame::Null,
        other => other,
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_lmove_resp(
    store: &EmbeddedStore,
    source: &[u8],
    dest: &[u8],
    source_side: &[u8],
    dest_side: &[u8],
    out: &mut BytesMut,
) {
    let Some(from_front) = parse_list_side(source_side) else {
        ServerWire::write_resp_error(out, "ERR syntax error");
        return;
    };
    let Some(to_front) = parse_list_side(dest_side) else {
        ServerWire::write_resp_error(out, "ERR syntax error");
        return;
    };
    write_move_between_lists_resp(store, source, dest, from_front, to_front, out);
}

#[cfg(feature = "server")]
pub(crate) fn write_move_between_lists_resp(
    store: &EmbeddedStore,
    source: &[u8],
    dest: &[u8],
    from_front: bool,
    to_front: bool,
    out: &mut BytesMut,
) {
    match store.redis_type(source) {
        "none" => {
            write_resp_null(out);
            return;
        }
        "list" => {}
        _ => {
            write_frame(out, &wrongtype());
            return;
        }
    }
    match store.redis_type(dest) {
        "none" | "list" => {}
        _ => {
            write_frame(out, &wrongtype());
            return;
        }
    }

    let popped = if from_front {
        store.lpop(source)
    } else {
        store.rpop(source)
    };
    match popped {
        RedisObjectResult::Bulk(Some(value)) => {
            let values = [value.as_ref()];
            let result = if to_front {
                store.lpush(dest, &values)
            } else {
                store.rpush(dest, &values)
            };
            match result {
                RedisObjectResult::WrongType => write_frame(out, &wrongtype()),
                _ => ServerWire::write_resp_blob_string(out, &value),
            }
        }
        RedisObjectResult::Bulk(None) => write_resp_null(out),
        RedisObjectResult::WrongType => write_frame(out, &wrongtype()),
        _ => write_resp_null(out),
    }
}

fn parse_list_side(value: &[u8]) -> Option<bool> {
    match value {
        value if eq_ignore_ascii_case(value, b"LEFT") => Some(true),
        value if eq_ignore_ascii_case(value, b"RIGHT") => Some(false),
        _ => None,
    }
}
