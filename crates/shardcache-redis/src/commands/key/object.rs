use bytes::BytesMut;

use crate::commands::redis::{
    bulk, define_redis_command, eq_ignore_ascii_case, error, int, write_frame, write_resp_null,
    wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(Object, "OBJECT", false);

const NO_SUCH_KEY: &str = "ERR no such key";
const LFU_POLICY: &str = "ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust.";

const OBJECT_HELP: &[&str] = &[
    "OBJECT ENCODING <key>",
    "OBJECT FREQ <key>",
    "OBJECT IDLETIME <key>",
    "OBJECT REFCOUNT <key>",
    "OBJECT HELP",
];

impl crate::commands::redis::RedisCommand for Object {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [subcommand] if eq_ignore_ascii_case(subcommand, b"HELP") => Frame::Array(
                OBJECT_HELP
                    .iter()
                    .map(|line| bulk(line.as_bytes().to_vec()))
                    .collect(),
            ),
            [subcommand, key] if eq_ignore_ascii_case(subcommand, b"ENCODING") => {
                match store.object_encoding(key) {
                    Some(encoding) => bulk(encoding.as_bytes().to_vec()),
                    None => Frame::Null,
                }
            }
            [subcommand, key] if eq_ignore_ascii_case(subcommand, b"REFCOUNT") => {
                if store.object_encoding(key).is_some() {
                    int(1)
                } else {
                    error(NO_SUCH_KEY)
                }
            }
            [subcommand, key] if eq_ignore_ascii_case(subcommand, b"IDLETIME") => {
                if store.object_encoding(key).is_some() {
                    int(0)
                } else {
                    error(NO_SUCH_KEY)
                }
            }
            [subcommand, _key] if eq_ignore_ascii_case(subcommand, b"FREQ") => error(LFU_POLICY),
            [_, _] => error("ERR syntax error"),
            _ => wrong_arity("OBJECT"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [subcommand, key] if eq_ignore_ascii_case(subcommand, b"ENCODING") => {
                match store.object_encoding(key) {
                    Some("raw") => out.extend_from_slice(b"$3\r\nraw\r\n"),
                    Some(encoding) => ServerWire::write_resp_blob_string(out, encoding.as_bytes()),
                    None => write_resp_null(out),
                }
            }
            _ => write_frame(out, &Self::execute(store, args)),
        }
    }
}
