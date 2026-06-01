#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    eq_ignore_ascii_case, simple, write_frame, write_resp_simple_string, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

#[derive(Debug, Clone, Copy)]
pub(crate) struct FlushDb;

#[derive(Debug, Clone, Copy)]
pub(crate) struct FlushAll;

pub(crate) static FLUSHDB_COMMAND: FlushDb = FlushDb;
pub(crate) static FLUSHALL_COMMAND: FlushAll = FlushAll;

impl crate::commands::CommandSpec for FlushDb {
    const NAME: &'static str = "FLUSHDB";
    const MUTATES_VALUE: bool = true;
}

impl crate::commands::CommandSpec for FlushAll {
    const NAME: &'static str = "FLUSHALL";
    const MUTATES_VALUE: bool = true;
}

impl crate::commands::redis::RedisCommand for FlushDb {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        flush(store, args, "FLUSHDB")
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_flush_resp(store, args, "FLUSHDB", out);
    }
}

impl crate::commands::redis::RedisCommand for FlushAll {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        flush(store, args, "FLUSHALL")
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_flush_resp(store, args, "FLUSHALL", out);
    }
}

fn flush(store: &EmbeddedStore, args: &[&[u8]], command: &str) -> Frame {
    match args {
        [] => {
            delete_all_keys(store);
            simple("OK")
        }
        [mode] if is_flush_mode(mode) => {
            delete_all_keys(store);
            simple("OK")
        }
        [..] if args.len() > 1 => wrong_arity(command),
        _ => crate::commands::redis::error("ERR syntax error"),
    }
}

#[cfg(feature = "server")]
fn write_flush_resp(store: &EmbeddedStore, args: &[&[u8]], command: &str, out: &mut BytesMut) {
    match args {
        [] => {
            delete_all_keys(store);
            write_resp_simple_string(out, "OK");
        }
        [mode] if is_flush_mode(mode) => {
            delete_all_keys(store);
            write_resp_simple_string(out, "OK");
        }
        [..] if args.len() > 1 => write_frame(out, &wrong_arity(command)),
        _ => ServerWire::write_resp_error(out, "ERR syntax error"),
    }
}

fn is_flush_mode(value: &[u8]) -> bool {
    eq_ignore_ascii_case(value, b"ASYNC") || eq_ignore_ascii_case(value, b"SYNC")
}

fn delete_all_keys(store: &EmbeddedStore) {
    let keys = store.key_snapshot_unsorted();
    for key in keys {
        let _ = store.delete(&key);
    }
}
