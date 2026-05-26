#[cfg(feature = "server")]
use bytes::BytesMut;
use std::sync::OnceLock;

use crate::commands::redis::{
    array_bulk, bulk, define_redis_command, eq_ignore_ascii_case, int, write_frame, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

define_redis_command!(CommandInfo, "COMMAND", false);

const COMMAND_HELP_LINES: &[&[u8]] = &[
    b"COMMAND COUNT",
    b"COMMAND LIST",
    b"COMMAND INFO [command-name ...]",
    b"COMMAND GETKEYS command [arg ...]",
    b"COMMAND GETKEYSANDFLAGS command [arg ...]",
];

const EXTRA_COMMANDS: &[CommandInfoMetadata] = &[
    CommandInfoMetadata {
        name: "DISCARD",
        mutates: false,
    },
    CommandInfoMetadata {
        name: "EXEC",
        mutates: true,
    },
    CommandInfoMetadata {
        name: "MULTI",
        mutates: false,
    },
    CommandInfoMetadata {
        name: "RESTORE-ASKING",
        mutates: true,
    },
    CommandInfoMetadata {
        name: "SLAVEOF",
        mutates: false,
    },
    CommandInfoMetadata {
        name: "SUBSTR",
        mutates: false,
    },
    CommandInfoMetadata {
        name: "UNWATCH",
        mutates: false,
    },
    CommandInfoMetadata {
        name: "WATCH",
        mutates: false,
    },
];

#[derive(Debug, Clone, Copy)]
struct CommandInfoMetadata {
    name: &'static str,
    mutates: bool,
}

impl CommandInfoMetadata {
    fn name_bytes(self) -> &'static [u8] {
        self.name.as_bytes()
    }

    fn matches(self, name: &[u8]) -> bool {
        name.eq_ignore_ascii_case(self.name_bytes())
    }

    fn info_frame(self) -> Frame {
        command_info_frame(self.name, self.mutates)
    }
}

#[derive(Debug, Clone, Copy)]
struct CommandAccessMetadata {
    flag: &'static [u8],
    category: &'static [u8],
}

impl CommandAccessMetadata {
    const READ: Self = Self {
        flag: b"readonly",
        category: b"@read",
    };
    const WRITE: Self = Self {
        flag: b"write",
        category: b"@write",
    };

    fn for_mutates(mutates: bool) -> Self {
        match mutates {
            true => Self::WRITE,
            false => Self::READ,
        }
    }
}

impl crate::commands::redis::RedisCommand for CommandInfo {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => Frame::Array(command_info_frames(None)),
            [subcommand] if eq_ignore_ascii_case(subcommand, b"COUNT") => {
                int(command_names().len() as i64)
            }
            [subcommand] if eq_ignore_ascii_case(subcommand, b"LIST") => array_bulk(
                command_names()
                    .iter()
                    .map(|name| (*name).to_vec())
                    .collect(),
            ),
            [subcommand, rest @ ..] if eq_ignore_ascii_case(subcommand, b"INFO") => {
                let names = match rest.is_empty() {
                    true => None,
                    false => Some(rest),
                };
                Frame::Array(command_info_frames(names))
            }
            [subcommand, command @ ..] if eq_ignore_ascii_case(subcommand, b"GETKEYS") => {
                command_getkeys(command, false)
            }
            [subcommand, command @ ..] if eq_ignore_ascii_case(subcommand, b"GETKEYSANDFLAGS") => {
                command_getkeys(command, true)
            }
            [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"DOCS") => {
                Frame::Array(Vec::new())
            }
            [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"HELP") => command_help(),
            _ => wrong_arity("COMMAND"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let _ = store;
        match args {
            [] => out.extend_from_slice(command_resp_bytes()),
            [subcommand] if eq_ignore_ascii_case(subcommand, b"COUNT") => {
                ServerWire::write_resp_integer(out, command_names().len() as i64);
            }
            [subcommand] if eq_ignore_ascii_case(subcommand, b"LIST") => {
                out.extend_from_slice(command_list_resp_bytes());
            }
            [subcommand] if eq_ignore_ascii_case(subcommand, b"INFO") => {
                out.extend_from_slice(command_resp_bytes());
            }
            [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"DOCS") => {
                out.extend_from_slice(b"*0\r\n");
            }
            [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"HELP") => {
                out.extend_from_slice(command_help_resp_bytes());
            }
            _ => write_frame(out, &Self::execute(store, args)),
        }
    }

    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let _ = store;
        match args {
            [] => ServerWire::write_fast_value(out, command_resp_bytes()),
            [subcommand] if eq_ignore_ascii_case(subcommand, b"COUNT") => {
                ServerWire::write_fast_integer(out, command_names().len() as i64);
            }
            [subcommand] if eq_ignore_ascii_case(subcommand, b"LIST") => {
                write_fast_static_array(out, command_names());
            }
            [subcommand] if eq_ignore_ascii_case(subcommand, b"INFO") => {
                ServerWire::write_fast_value(out, command_resp_bytes());
            }
            [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"DOCS") => {
                ServerWire::write_fast_empty_array(out);
            }
            [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"HELP") => {
                write_fast_static_array(out, COMMAND_HELP_LINES);
            }
            _ => {
                let start = ServerWire::begin_fast_value(out);
                Self::write_resp(store, args, out);
                ServerWire::finish_fast_value(out, start);
            }
        }
    }
}

fn command_names() -> &'static [&'static [u8]] {
    static COMMAND_NAMES: OnceLock<Vec<&'static [u8]>> = OnceLock::new();
    COMMAND_NAMES
        .get_or_init(|| {
            command_metadata()
                .iter()
                .map(|command| command.name_bytes())
                .collect()
        })
        .as_slice()
}

fn command_metadata() -> &'static [CommandInfoMetadata] {
    static COMMAND_METADATA: OnceLock<Vec<CommandInfoMetadata>> = OnceLock::new();
    COMMAND_METADATA
        .get_or_init(|| {
            let mut commands = crate::commands::CATALOG
                .iter()
                .map(|command| CommandInfoMetadata {
                    name: command.name(),
                    mutates: command.mutates_value(),
                })
                .collect::<Vec<_>>();
            commands.extend_from_slice(EXTRA_COMMANDS);
            commands.sort_unstable_by(|left, right| left.name.cmp(right.name));
            commands.dedup_by(|left, right| left.name.eq_ignore_ascii_case(right.name));
            commands
        })
        .as_slice()
}

fn find_command_metadata(name: &[u8]) -> Option<CommandInfoMetadata> {
    command_metadata()
        .iter()
        .copied()
        .find(|command| command.matches(name))
}

#[cfg(feature = "server")]
fn command_resp_bytes() -> &'static [u8] {
    static COMMAND_RESP_BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    COMMAND_RESP_BYTES
        .get_or_init(|| {
            let mut out = BytesMut::new();
            write_frame(&mut out, &Frame::Array(command_info_frames(None)));
            out.to_vec()
        })
        .as_slice()
}

#[cfg(feature = "server")]
fn command_list_resp_bytes() -> &'static [u8] {
    static COMMAND_LIST_RESP_BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    COMMAND_LIST_RESP_BYTES
        .get_or_init(|| {
            let mut out = BytesMut::new();
            write_frame(
                &mut out,
                &array_bulk(command_names().iter().map(|name| name.to_vec()).collect()),
            );
            out.to_vec()
        })
        .as_slice()
}

#[cfg(feature = "server")]
fn command_help_resp_bytes() -> &'static [u8] {
    static COMMAND_HELP_RESP_BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    COMMAND_HELP_RESP_BYTES
        .get_or_init(|| {
            let mut out = BytesMut::new();
            write_frame(&mut out, &command_help());
            out.to_vec()
        })
        .as_slice()
}

#[cfg(feature = "server")]
fn write_fast_static_array(out: &mut BytesMut, values: &[&[u8]]) {
    let start = ServerWire::begin_fast_array(out, values.len());
    for value in values {
        ServerWire::write_fast_array_item(out, Some(value));
    }
    ServerWire::finish_fast_array(out, start);
}

fn command_info_frames(names: Option<&[&[u8]]>) -> Vec<Frame> {
    match names {
        Some(names) => names
            .iter()
            .map(|name| {
                find_command_metadata(name)
                    .map(CommandInfoMetadata::info_frame)
                    .unwrap_or(Frame::Null)
            })
            .collect(),
        None => command_metadata()
            .iter()
            .copied()
            .map(CommandInfoMetadata::info_frame)
            .collect(),
    }
}

fn command_info_frame(name: &'static str, mutates: bool) -> Frame {
    let no_key = command_has_no_key(name.as_bytes());
    let access = CommandAccessMetadata::for_mutates(mutates);
    let flags = bulk_frames([access.flag, b"fast"]);
    let categories = bulk_frames([access.category, b"@fast"]);
    Frame::Array(vec![
        bulk(name.as_bytes().to_ascii_lowercase()),
        int(-1),
        Frame::Array(flags),
        int(if no_key { 0 } else { 1 }),
        int(if no_key { 0 } else { 1 }),
        int(if no_key { 0 } else { 1 }),
        Frame::Array(categories),
        Frame::Array(Vec::new()),
        Frame::Array(Vec::new()),
        Frame::Array(Vec::new()),
    ])
}

fn bulk_frames<const N: usize>(values: [&[u8]; N]) -> Vec<Frame> {
    values
        .into_iter()
        .map(|value| bulk(value.to_vec()))
        .collect()
}

fn command_getkeys(command: &[&[u8]], with_flags: bool) -> Frame {
    let Some((name, args)) = command.split_first() else {
        return wrong_arity("COMMAND");
    };
    if command_has_no_key(name) {
        return Frame::Array(Vec::new());
    }
    if find_command_metadata(name).is_none() {
        return crate::commands::redis::error("ERR invalid command specified");
    }
    let keys = key_args_for_command(name, args);
    if with_flags {
        Frame::Array(
            keys.into_iter()
                .map(|key| {
                    Frame::Array(vec![
                        bulk(key.to_vec()),
                        Frame::Array(vec![bulk(b"RW".to_vec()), bulk(b"access".to_vec())]),
                    ])
                })
                .collect(),
        )
    } else {
        array_bulk(keys.into_iter().map(Vec::from).collect())
    }
}

fn key_args_for_command<'a>(name: &[u8], args: &'a [&'a [u8]]) -> Vec<&'a [u8]> {
    if eq_ignore_ascii_case(name, b"MEMORY") {
        return args.get(1).copied().into_iter().collect();
    }
    if eq_ignore_ascii_case(name, b"MSET") || eq_ignore_ascii_case(name, b"MSETNX") {
        return args.iter().step_by(2).copied().collect();
    }
    if eq_ignore_ascii_case(name, b"WATCH") {
        return args.to_vec();
    }
    if eq_ignore_ascii_case(name, b"MGET")
        || eq_ignore_ascii_case(name, b"DEL")
        || eq_ignore_ascii_case(name, b"EXISTS")
        || eq_ignore_ascii_case(name, b"TOUCH")
        || eq_ignore_ascii_case(name, b"UNLINK")
    {
        return args.to_vec();
    }
    if eq_ignore_ascii_case(name, b"LMPOP")
        || eq_ignore_ascii_case(name, b"ZMPOP")
        || eq_ignore_ascii_case(name, b"ZUNION")
        || eq_ignore_ascii_case(name, b"ZINTER")
        || eq_ignore_ascii_case(name, b"ZDIFF")
        || eq_ignore_ascii_case(name, b"ZINTERCARD")
    {
        return counted_keys(args, 0);
    }
    if eq_ignore_ascii_case(name, b"EVAL") || eq_ignore_ascii_case(name, b"EVALSHA") {
        return counted_keys(args, 1);
    }
    if eq_ignore_ascii_case(name, b"BLMPOP") || eq_ignore_ascii_case(name, b"BZMPOP") {
        return counted_keys(args, 1);
    }
    if eq_ignore_ascii_case(name, b"ZUNIONSTORE")
        || eq_ignore_ascii_case(name, b"ZINTERSTORE")
        || eq_ignore_ascii_case(name, b"ZDIFFSTORE")
    {
        return zaggregate_store_keys(args);
    }
    args.first().copied().into_iter().collect()
}

fn counted_keys<'a>(args: &'a [&'a [u8]], numkeys_index: usize) -> Vec<&'a [u8]> {
    counted_key_span(args, numkeys_index)
        .map(|keys| keys.to_vec())
        .unwrap_or_default()
}

fn counted_key_span<'a>(args: &'a [&'a [u8]], numkeys_index: usize) -> Option<&'a [&'a [u8]]> {
    let numkeys = args
        .get(numkeys_index)
        .and_then(|raw| parse_ascii_usize(raw))?;
    let key_start = numkeys_index.checked_add(1)?;
    let key_end = key_start.checked_add(numkeys)?;
    match numkeys {
        0 => None,
        _ => args.get(key_start..key_end),
    }
}

fn parse_ascii_usize(raw: &[u8]) -> Option<usize> {
    std::str::from_utf8(raw).ok()?.parse().ok()
}

fn zaggregate_store_keys<'a>(args: &'a [&'a [u8]]) -> Vec<&'a [u8]> {
    let Some(raw_numkeys) = args.get(1) else {
        return args.first().copied().into_iter().collect();
    };
    let Some(numkeys) = parse_ascii_usize(raw_numkeys) else {
        return Vec::new();
    };
    let mut keys = Vec::with_capacity(numkeys.saturating_add(1));
    if let Some(dest) = args.first().copied() {
        keys.push(dest);
    }
    keys.extend(args.iter().skip(2).take(numkeys).copied());
    keys
}

fn command_has_no_key(name: &[u8]) -> bool {
    matches!(
        name,
        name if eq_ignore_ascii_case(name, b"PING")
            || eq_ignore_ascii_case(name, b"AUTH")
            || eq_ignore_ascii_case(name, b"HELLO")
            || eq_ignore_ascii_case(name, b"SELECT")
            || eq_ignore_ascii_case(name, b"QUIT")
            || eq_ignore_ascii_case(name, b"ECHO")
            || eq_ignore_ascii_case(name, b"MULTI")
            || eq_ignore_ascii_case(name, b"EXEC")
            || eq_ignore_ascii_case(name, b"DISCARD")
            || eq_ignore_ascii_case(name, b"UNWATCH")
            || eq_ignore_ascii_case(name, b"SLAVEOF")
            || eq_ignore_ascii_case(name, b"COMMAND")
            || eq_ignore_ascii_case(name, b"CONFIG")
            || eq_ignore_ascii_case(name, b"CLIENT")
            || eq_ignore_ascii_case(name, b"DBSIZE")
            || eq_ignore_ascii_case(name, b"TIME")
            || eq_ignore_ascii_case(name, b"INFO")
            || eq_ignore_ascii_case(name, b"SCAN")
            || eq_ignore_ascii_case(name, b"RANDOMKEY")
            || eq_ignore_ascii_case(name, b"FLUSHDB")
            || eq_ignore_ascii_case(name, b"FLUSHALL")
            || eq_ignore_ascii_case(name, b"SCRIPT")
    )
}

fn command_help() -> Frame {
    array_bulk(
        COMMAND_HELP_LINES
            .iter()
            .map(|line| (*line).to_vec())
            .collect(),
    )
}
