#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{bulk, define_redis_command, eq_ignore_ascii_case, int, simple};
#[cfg(feature = "server")]
use crate::commands::redis::{write_resp_null, write_resp_simple_string};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Client, "CLIENT", false);

const CLIENT_INFO_LINE: &[u8] =
    b"id=0 addr=127.0.0.1:0 name= db=0 flags=N resp=2 lib-name= lib-ver= cmd=client|info";

impl crate::commands::redis::RedisCommand for Client {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub] if eq_ignore_ascii_case(sub, b"GETNAME") => Frame::Null,
            [sub, _] if eq_ignore_ascii_case(sub, b"SETNAME") => simple("OK"),
            [sub] if eq_ignore_ascii_case(sub, b"ID") => int(0),
            [sub] if eq_ignore_ascii_case(sub, b"INFO") => bulk(CLIENT_INFO_LINE.to_vec()),
            [sub] if eq_ignore_ascii_case(sub, b"LIST") => bulk(Vec::new()),
            [sub, ..] if eq_ignore_ascii_case(sub, b"KILL") => int(0),
            _ => simple("OK"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [sub] if eq_ignore_ascii_case(sub, b"GETNAME") => write_resp_null(out),
            [sub, _] if eq_ignore_ascii_case(sub, b"SETNAME") => out.extend_from_slice(b"+OK\r\n"),
            [sub] if eq_ignore_ascii_case(sub, b"ID") => out.extend_from_slice(b":0\r\n"),
            [sub] if eq_ignore_ascii_case(sub, b"INFO") => {
                ServerWire::write_resp_blob_string(out, CLIENT_INFO_LINE)
            }
            [sub] if eq_ignore_ascii_case(sub, b"LIST") => {
                ServerWire::write_resp_blob_string(out, b"")
            }
            [sub, ..] if eq_ignore_ascii_case(sub, b"KILL") => out.extend_from_slice(b":0\r\n"),
            _ => write_resp_simple_string(out, "OK"),
        }
    }
}
