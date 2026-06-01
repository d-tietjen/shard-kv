#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_frame;
use crate::commands::redis::{
    bulk, define_redis_command, eq_ignore_ascii_case, error, int, simple,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, now_millis};

define_redis_command!(Acl, "ACL", false);

const DEFAULT_USER_RULE: &str = "user default on nopass sanitize-payload ~* &* +@all";

const ACL_CATEGORIES: &[&str] = &[
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
];

const ACL_HELP: &[&str] = &[
    "ACL CAT [<category>]",
    "ACL DELUSER <username> [<username> ...]",
    "ACL GENPASS [<bits>]",
    "ACL GETUSER <username>",
    "ACL LIST",
    "ACL LOAD",
    "ACL SAVE",
    "ACL SETUSER <username> <attribute> [<attribute> ...]",
    "ACL USERS",
    "ACL WHOAMI",
    "ACL HELP",
];

impl crate::commands::redis::RedisCommand for Acl {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [subcommand, rest @ ..] = args else {
            return error("ERR wrong number of arguments for 'acl' command");
        };

        match subcommand {
            subcommand if eq_ignore_ascii_case(subcommand, b"WHOAMI") => bulk(b"default".to_vec()),
            subcommand if eq_ignore_ascii_case(subcommand, b"LIST") => {
                Frame::Array(vec![bulk(DEFAULT_USER_RULE.as_bytes().to_vec())])
            }
            subcommand if eq_ignore_ascii_case(subcommand, b"USERS") => {
                Frame::Array(vec![bulk(b"default".to_vec())])
            }
            subcommand if eq_ignore_ascii_case(subcommand, b"CAT") => match rest {
                [] => Frame::Array(
                    ACL_CATEGORIES
                        .iter()
                        .map(|category| bulk(category.as_bytes().to_vec()))
                        .collect(),
                ),
                [_category] => Frame::Array(Vec::new()),
                _ => error("ERR Unknown ACL CAT argument"),
            },
            subcommand if eq_ignore_ascii_case(subcommand, b"GETUSER") => match rest {
                [user] if user.eq_ignore_ascii_case(b"default") => default_user_description(),
                [_user] => Frame::Null,
                _ => error("ERR wrong number of arguments for 'acl|getuser' command"),
            },
            subcommand if eq_ignore_ascii_case(subcommand, b"SETUSER") => simple("OK"),
            subcommand if eq_ignore_ascii_case(subcommand, b"DELUSER") => int(0),
            subcommand if eq_ignore_ascii_case(subcommand, b"GENPASS") => bulk(generate_password()),
            subcommand
                if eq_ignore_ascii_case(subcommand, b"LOAD")
                    || eq_ignore_ascii_case(subcommand, b"SAVE") =>
            {
                error("ERR This Redis instance is not configured to use an ACL file.")
            }
            subcommand if eq_ignore_ascii_case(subcommand, b"HELP") => Frame::Array(
                ACL_HELP
                    .iter()
                    .map(|line| bulk(line.as_bytes().to_vec()))
                    .collect(),
            ),
            _ => error("ERR Unknown ACL subcommand or wrong number of arguments"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"WHOAMI") => {
                out.extend_from_slice(b"$7\r\ndefault\r\n");
            }
            _ => write_frame(out, &Self::execute(store, args)),
        }
    }
}

fn default_user_description() -> Frame {
    Frame::Array(vec![
        bulk(b"flags".to_vec()),
        Frame::Array(vec![
            bulk(b"on".to_vec()),
            bulk(b"allkeys".to_vec()),
            bulk(b"allchannels".to_vec()),
            bulk(b"nopass".to_vec()),
        ]),
        bulk(b"passwords".to_vec()),
        Frame::Array(Vec::new()),
        bulk(b"commands".to_vec()),
        bulk(b"+@all".to_vec()),
        bulk(b"keys".to_vec()),
        bulk(b"~*".to_vec()),
        bulk(b"channels".to_vec()),
        bulk(b"&*".to_vec()),
    ])
}

fn generate_password() -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut state = now_millis() | 1;
    let mut password = Vec::with_capacity(64);
    for _ in 0..64 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        password.push(HEX[((state >> 60) & 0xf) as usize]);
    }
    password
}
