#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    bulk, define_redis_command, optional_string_value, write_resp_array_header, write_resp_null,
    write_resp_wrong_arity, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisStringLookup};

define_redis_command!(MGet, "MGET", false);

impl crate::commands::redis::RedisCommand for MGet {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.is_empty() {
            return wrong_arity("MGET");
        }
        Frame::Array(
            args.iter()
                .map(|key| match optional_string_value(store, key, false) {
                    Ok(Some(value)) => bulk(value),
                    _ => Frame::Null,
                })
                .collect(),
        )
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.is_empty() {
            write_resp_wrong_arity(out, "MGET");
            return;
        }
        write_resp_array_header(out, args.len());
        for key in args {
            match store.get_string_value_into(key, |bytes| {
                ServerWire::write_resp_blob_string(out, bytes);
            }) {
                RedisStringLookup::Hit => {}
                RedisStringLookup::Miss | RedisStringLookup::WrongType => write_resp_null(out),
            }
        }
    }

    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        if args.is_empty() {
            ServerWire::write_fast_error(out, "ERR wrong number of arguments for 'mget' command");
            return;
        }
        let start = ServerWire::begin_fast_array(out, args.len());
        for key in args {
            match store.get_string_value_into(key, |bytes| {
                ServerWire::write_fast_array_item(out, Some(bytes));
            }) {
                RedisStringLookup::Hit => {}
                RedisStringLookup::Miss | RedisStringLookup::WrongType => {
                    ServerWire::write_fast_array_item(out, None)
                }
            }
        }
        ServerWire::finish_fast_array(out, start);
    }
}
