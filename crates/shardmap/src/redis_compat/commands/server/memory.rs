#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    array_bulk, define_redis_command, error, int, parse_usize, write_frame, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectError, RedisObjectResult};

define_redis_command!(Memory, "MEMORY", false);

impl crate::commands::redis::RedisCommand for Memory {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        memory(store, args)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_frame(out, &memory(store, args));
    }
}

fn memory(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    match args {
        [subcommand] if crate::commands::redis::eq_ignore_ascii_case(subcommand, b"HELP") => {
            memory_help()
        }
        [subcommand, key] if crate::commands::redis::eq_ignore_ascii_case(subcommand, b"USAGE") => {
            memory_usage(store, key)
        }
        [subcommand, key, samples, count]
            if crate::commands::redis::eq_ignore_ascii_case(subcommand, b"USAGE")
                && crate::commands::redis::eq_ignore_ascii_case(samples, b"SAMPLES") =>
        {
            match parse_usize(count) {
                Ok(_) => memory_usage(store, key),
                Err(_) => error("ERR value is not an integer or out of range"),
            }
        }
        [subcommand, ..]
            if !crate::commands::redis::eq_ignore_ascii_case(subcommand, b"USAGE")
                && !crate::commands::redis::eq_ignore_ascii_case(subcommand, b"HELP") =>
        {
            error("ERR syntax error")
        }
        _ => wrong_arity("MEMORY"),
    }
}

fn memory_help() -> Frame {
    array_bulk(vec![
        b"MEMORY USAGE <key> [SAMPLES <count>]".to_vec(),
        b"    Return an estimate of the memory usage of the key.".to_vec(),
        b"MEMORY HELP".to_vec(),
        b"    Return this help.".to_vec(),
    ])
}

fn memory_usage(store: &EmbeddedStore, key: &[u8]) -> Frame {
    let usage = match store.redis_type(key) {
        "none" => return Frame::Null,
        "string" => match store.get_value_bytes(key) {
            Some(value) => key.len().saturating_add(value.len()),
            None => return Frame::Null,
        },
        "hash" => match store.hgetall(key) {
            RedisObjectResult::Array(values) => object_array_usage(key, values),
            RedisObjectResult::WrongType => return wrongtype(),
            _ => return Frame::Null,
        },
        "list" => match store.lrange(key, 0, -1) {
            RedisObjectResult::Array(values) => object_array_usage(key, values),
            RedisObjectResult::WrongType => return wrongtype(),
            _ => return Frame::Null,
        },
        "set" => match store.set_members(key) {
            Ok(values) => values
                .into_iter()
                .fold(key.len(), |sum, value| sum.saturating_add(value.len())),
            Err(RedisObjectError::WrongType) => return wrongtype(),
            Err(RedisObjectError::MissingKey) => return Frame::Null,
        },
        "zset" => match store.zentries(key) {
            Ok(entries) => entries.into_iter().fold(key.len(), |sum, (member, score)| {
                sum.saturating_add(member.len())
                    .saturating_add(score.to_string().len())
            }),
            Err(RedisObjectError::WrongType) => return wrongtype(),
            Err(RedisObjectError::MissingKey) => return Frame::Null,
        },
        _ => return Frame::Null,
    };
    int(usage as i64)
}

fn object_array_usage(key: &[u8], values: Vec<Option<Vec<u8>>>) -> usize {
    values.into_iter().fold(key.len(), |sum, value| {
        sum.saturating_add(value.map_or(0, |value| value.len()))
    })
}
