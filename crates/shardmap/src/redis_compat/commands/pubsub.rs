#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{array_bulk, bulk, error, int, wrong_arity};
#[cfg(feature = "server")]
use crate::commands::redis::{write_resp_array_header, write_resp_null, write_resp_wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

macro_rules! define_pubsub_command {
    ($type:ident, $static_name:ident, $name:literal, $mutates:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static $static_name: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;
        }
    };
}

define_pubsub_command!(Publish, PUBLISH_COMMAND, "PUBLISH", false);
define_pubsub_command!(SPublish, SPUBLISH_COMMAND, "SPUBLISH", false);
define_pubsub_command!(PubSub, PUBSUB_COMMAND, "PUBSUB", false);
define_pubsub_command!(Subscribe, SUBSCRIBE_COMMAND, "SUBSCRIBE", false);
define_pubsub_command!(Unsubscribe, UNSUBSCRIBE_COMMAND, "UNSUBSCRIBE", false);
define_pubsub_command!(PSubscribe, PSUBSCRIBE_COMMAND, "PSUBSCRIBE", false);
define_pubsub_command!(PUnsubscribe, PUNSUBSCRIBE_COMMAND, "PUNSUBSCRIBE", false);
define_pubsub_command!(SSubscribe, SSUBSCRIBE_COMMAND, "SSUBSCRIBE", false);
define_pubsub_command!(SUnsubscribe, SUNSUBSCRIBE_COMMAND, "SUNSUBSCRIBE", false);

impl crate::commands::redis::RedisCommand for Publish {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [_channel, _message] => int(0),
            _ => wrong_arity("PUBLISH"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [_channel, _message] => ServerWire::write_resp_integer(out, 0),
            _ => write_resp_wrong_arity(out, "PUBLISH"),
        }
    }
}

impl crate::commands::redis::RedisCommand for SPublish {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [_channel, _message] => int(0),
            _ => wrong_arity("SPUBLISH"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [_channel, _message] => ServerWire::write_resp_integer(out, 0),
            _ => write_resp_wrong_arity(out, "SPUBLISH"),
        }
    }
}

impl crate::commands::redis::RedisCommand for PubSub {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => wrong_arity("PUBSUB"),
            [sub] if sub.eq_ignore_ascii_case(b"CHANNELS") => Frame::Array(Vec::new()),
            [sub, _pattern] if sub.eq_ignore_ascii_case(b"CHANNELS") => Frame::Array(Vec::new()),
            [sub] if sub.eq_ignore_ascii_case(b"SHARDCHANNELS") => Frame::Array(Vec::new()),
            [sub, _pattern] if sub.eq_ignore_ascii_case(b"SHARDCHANNELS") => {
                Frame::Array(Vec::new())
            }
            [sub] if sub.eq_ignore_ascii_case(b"NUMPAT") => int(0),
            [sub] if sub.eq_ignore_ascii_case(b"SHARDNUMSUB") => Frame::Array(Vec::new()),
            [sub, channels @ ..] if sub.eq_ignore_ascii_case(b"NUMSUB") => {
                let mut items = Vec::with_capacity(channels.len().saturating_mul(2));
                for channel in channels {
                    items.push(bulk(channel.to_vec()));
                    items.push(int(0));
                }
                Frame::Array(items)
            }
            [sub, channels @ ..] if sub.eq_ignore_ascii_case(b"SHARDNUMSUB") => {
                let mut items = Vec::with_capacity(channels.len().saturating_mul(2));
                for channel in channels {
                    items.push(bulk(channel.to_vec()));
                    items.push(int(0));
                }
                Frame::Array(items)
            }
            [sub] if sub.eq_ignore_ascii_case(b"HELP") => array_bulk(vec![
                b"PUBSUB CHANNELS [pattern]".to_vec(),
                b"PUBSUB NUMSUB [channel ...]".to_vec(),
                b"PUBSUB NUMPAT".to_vec(),
                b"PUBSUB SHARDCHANNELS [pattern]".to_vec(),
                b"PUBSUB SHARDNUMSUB [channel ...]".to_vec(),
            ]),
            _ => error("ERR unknown PUBSUB subcommand or wrong number of arguments"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [] => write_resp_wrong_arity(out, "PUBSUB"),
            [sub] if sub.eq_ignore_ascii_case(b"CHANNELS") => write_resp_array_header(out, 0),
            [sub, _pattern] if sub.eq_ignore_ascii_case(b"CHANNELS") => {
                write_resp_array_header(out, 0);
            }
            [sub] if sub.eq_ignore_ascii_case(b"SHARDCHANNELS") => {
                write_resp_array_header(out, 0);
            }
            [sub, _pattern] if sub.eq_ignore_ascii_case(b"SHARDCHANNELS") => {
                write_resp_array_header(out, 0);
            }
            [sub] if sub.eq_ignore_ascii_case(b"NUMPAT") => {
                ServerWire::write_resp_integer(out, 0);
            }
            [sub] if sub.eq_ignore_ascii_case(b"SHARDNUMSUB") => {
                write_resp_array_header(out, 0);
            }
            [sub, channels @ ..] if sub.eq_ignore_ascii_case(b"NUMSUB") => {
                write_resp_array_header(out, channels.len().saturating_mul(2));
                for channel in channels {
                    ServerWire::write_resp_blob_string(out, channel);
                    ServerWire::write_resp_integer(out, 0);
                }
            }
            [sub, channels @ ..] if sub.eq_ignore_ascii_case(b"SHARDNUMSUB") => {
                write_resp_array_header(out, channels.len().saturating_mul(2));
                for channel in channels {
                    ServerWire::write_resp_blob_string(out, channel);
                    ServerWire::write_resp_integer(out, 0);
                }
            }
            [sub] if sub.eq_ignore_ascii_case(b"HELP") => {
                write_resp_array_header(out, 5);
                ServerWire::write_resp_blob_string(out, b"PUBSUB CHANNELS [pattern]");
                ServerWire::write_resp_blob_string(out, b"PUBSUB NUMSUB [channel ...]");
                ServerWire::write_resp_blob_string(out, b"PUBSUB NUMPAT");
                ServerWire::write_resp_blob_string(out, b"PUBSUB SHARDCHANNELS [pattern]");
                ServerWire::write_resp_blob_string(out, b"PUBSUB SHARDNUMSUB [channel ...]");
            }
            _ => ServerWire::write_resp_error(
                out,
                "ERR unknown PUBSUB subcommand or wrong number of arguments",
            ),
        }
    }
}

impl crate::commands::redis::RedisCommand for Subscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("subscribe", args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_subscription_ack_resp(out, b"subscribe", args, true);
    }
}

impl crate::commands::redis::RedisCommand for PSubscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("psubscribe", args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_subscription_ack_resp(out, b"psubscribe", args, true);
    }
}

impl crate::commands::redis::RedisCommand for Unsubscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("unsubscribe", args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_subscription_ack_resp(out, b"unsubscribe", args, false);
    }
}

impl crate::commands::redis::RedisCommand for PUnsubscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("punsubscribe", args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_subscription_ack_resp(out, b"punsubscribe", args, false);
    }
}

impl crate::commands::redis::RedisCommand for SSubscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("ssubscribe", args, true)
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_subscription_ack_resp(out, b"ssubscribe", args, true);
    }
}

impl crate::commands::redis::RedisCommand for SUnsubscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("sunsubscribe", args, false)
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_subscription_ack_resp(out, b"sunsubscribe", args, false);
    }
}

fn subscription_ack(kind: &str, args: &[&[u8]], require_channel: bool) -> Frame {
    match args {
        [] if require_channel => wrong_arity(kind),
        [] => Frame::Array(vec![bulk(kind.as_bytes().to_vec()), Frame::Null, int(0)]),
        [channel] => Frame::Array(vec![
            bulk(kind.as_bytes().to_vec()),
            bulk(channel.to_vec()),
            int(if require_channel { 1 } else { 0 }),
        ]),
        channels => Frame::Array(
            channels
                .iter()
                .enumerate()
                .map(|(index, channel)| {
                    Frame::Array(vec![
                        bulk(kind.as_bytes().to_vec()),
                        bulk(channel.to_vec()),
                        int(if require_channel {
                            index.saturating_add(1) as i64
                        } else {
                            0
                        }),
                    ])
                })
                .collect(),
        ),
    }
}

#[cfg(feature = "server")]
fn write_subscription_ack_resp(
    out: &mut BytesMut,
    kind: &'static [u8],
    args: &[&[u8]],
    require_channel: bool,
) {
    match args {
        [] if require_channel => {
            let command = std::str::from_utf8(kind).unwrap_or("subscribe");
            write_resp_wrong_arity(out, command);
        }
        [] => write_subscription_ack_item_resp(out, kind, None, 0),
        [channel] => write_subscription_ack_item_resp(
            out,
            kind,
            Some(channel),
            if require_channel { 1 } else { 0 },
        ),
        channels => {
            write_resp_array_header(out, channels.len());
            for (index, channel) in channels.iter().enumerate() {
                write_subscription_ack_item_resp(
                    out,
                    kind,
                    Some(channel),
                    if require_channel {
                        index.saturating_add(1) as i64
                    } else {
                        0
                    },
                );
            }
        }
    }
}

#[cfg(feature = "server")]
fn write_subscription_ack_item_resp(
    out: &mut BytesMut,
    kind: &[u8],
    channel: Option<&[u8]>,
    count: i64,
) {
    write_resp_array_header(out, 3);
    ServerWire::write_resp_blob_string(out, kind);
    match channel {
        Some(channel) => ServerWire::write_resp_blob_string(out, channel),
        None => write_resp_null(out),
    }
    ServerWire::write_resp_integer(out, count);
}
