use crate::commands::redis::{array_bulk, bulk, error, int, wrong_arity};
use crate::protocol::Frame;
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
define_pubsub_command!(PubSub, PUBSUB_COMMAND, "PUBSUB", false);
define_pubsub_command!(Subscribe, SUBSCRIBE_COMMAND, "SUBSCRIBE", false);
define_pubsub_command!(Unsubscribe, UNSUBSCRIBE_COMMAND, "UNSUBSCRIBE", false);
define_pubsub_command!(PSubscribe, PSUBSCRIBE_COMMAND, "PSUBSCRIBE", false);
define_pubsub_command!(PUnsubscribe, PUNSUBSCRIBE_COMMAND, "PUNSUBSCRIBE", false);

impl crate::commands::redis::RedisCommand for Publish {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [_channel, _message] => int(0),
            _ => wrong_arity("PUBLISH"),
        }
    }
}

impl crate::commands::redis::RedisCommand for PubSub {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => wrong_arity("PUBSUB"),
            [sub] if sub.eq_ignore_ascii_case(b"CHANNELS") => Frame::Array(Vec::new()),
            [sub, _pattern] if sub.eq_ignore_ascii_case(b"CHANNELS") => Frame::Array(Vec::new()),
            [sub] if sub.eq_ignore_ascii_case(b"NUMPAT") => int(0),
            [sub, channels @ ..] if sub.eq_ignore_ascii_case(b"NUMSUB") => {
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
            ]),
            _ => error("ERR unknown PUBSUB subcommand or wrong number of arguments"),
        }
    }
}

impl crate::commands::redis::RedisCommand for Subscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("subscribe", args, true)
    }
}

impl crate::commands::redis::RedisCommand for PSubscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("psubscribe", args, true)
    }
}

impl crate::commands::redis::RedisCommand for Unsubscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("unsubscribe", args, false)
    }
}

impl crate::commands::redis::RedisCommand for PUnsubscribe {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        subscription_ack("punsubscribe", args, false)
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
