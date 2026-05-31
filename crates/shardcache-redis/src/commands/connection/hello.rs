#[cfg(feature = "server")]
use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::redis::write_frame;
use crate::commands::redis::{bulk, define_redis_command, error, int};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::{RespProtocolVersion, ServerWire};
use crate::storage::EmbeddedStore;

define_redis_command!(Hello, "HELLO", false);

impl crate::commands::redis::RedisCommand for Hello {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match parse_protocol(args) {
            Ok(HelloProtocol::Resp2) => hello_array(2),
            Ok(HelloProtocol::Resp3) => hello_map(),
            Err(HelloError::UnsupportedProtocol) => error("NOPROTO unsupported protocol version"),
            Err(HelloError::Syntax) => error("ERR syntax error"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(_store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match parse_protocol(args) {
            Ok(HelloProtocol::Resp2) => {
                ServerWire::write_resp_hello(out, RespProtocolVersion::Resp2)
            }
            Ok(HelloProtocol::Resp3) => {
                ServerWire::write_resp_hello(out, RespProtocolVersion::Resp3)
            }
            Err(HelloError::UnsupportedProtocol) => {
                write_frame(out, &error("NOPROTO unsupported protocol version"));
            }
            Err(HelloError::Syntax) => write_frame(out, &error("ERR syntax error")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelloProtocol {
    Resp2,
    Resp3,
}

impl HelloProtocol {
    fn from_argument(arg: &[u8]) -> std::result::Result<Self, HelloError> {
        match arg {
            b"2" => Ok(Self::Resp2),
            b"3" => Ok(Self::Resp3),
            _ => Err(HelloError::UnsupportedProtocol),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelloError {
    UnsupportedProtocol,
    Syntax,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelloOption {
    Auth,
    SetName,
}

impl HelloOption {
    const NAMES: &'static [(&'static [u8], Self)] =
        &[(b"AUTH", Self::Auth), (b"SETNAME", Self::SetName)];

    fn from_name(name: &[u8]) -> Option<Self> {
        Self::NAMES
            .iter()
            .find_map(|(candidate, option)| name.eq_ignore_ascii_case(candidate).then_some(*option))
    }

    fn argument_count(self) -> usize {
        match self {
            Self::Auth => 2,
            Self::SetName => 1,
        }
    }
}

fn parse_protocol(args: &[&[u8]]) -> std::result::Result<HelloProtocol, HelloError> {
    let (protocol, options) = split_protocol_arg(args)?;
    validate_options(options)?;
    Ok(protocol)
}

fn split_protocol_arg<'args, 'value>(
    args: &'args [&'value [u8]],
) -> std::result::Result<(HelloProtocol, &'args [&'value [u8]]), HelloError> {
    match args.split_first() {
        Some((first, rest)) if HelloOption::from_name(first).is_none() => {
            Ok((HelloProtocol::from_argument(first)?, rest))
        }
        _ => Ok((HelloProtocol::Resp2, args)),
    }
}

fn validate_options(mut args: &[&[u8]]) -> std::result::Result<(), HelloError> {
    while let Some((name, rest)) = args.split_first() {
        let option = HelloOption::from_name(name).ok_or(HelloError::Syntax)?;
        args = rest
            .get(option.argument_count()..)
            .ok_or(HelloError::Syntax)?;
    }
    Ok(())
}

fn hello_array(proto: i64) -> Frame {
    Frame::Array(vec![
        bulk(b"server".to_vec()),
        bulk(b"shardcache".to_vec()),
        bulk(b"version".to_vec()),
        bulk(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
        bulk(b"proto".to_vec()),
        int(proto),
        bulk(b"id".to_vec()),
        int(0),
        bulk(b"mode".to_vec()),
        bulk(b"standalone".to_vec()),
        bulk(b"role".to_vec()),
        bulk(b"master".to_vec()),
        bulk(b"modules".to_vec()),
        hello_modules(),
    ])
}

fn hello_map() -> Frame {
    Frame::Map(vec![
        (bulk(b"server".to_vec()), bulk(b"shardcache".to_vec())),
        (
            bulk(b"version".to_vec()),
            bulk(env!("CARGO_PKG_VERSION").as_bytes().to_vec()),
        ),
        (bulk(b"proto".to_vec()), int(3)),
        (bulk(b"id".to_vec()), int(0)),
        (bulk(b"mode".to_vec()), bulk(b"standalone".to_vec())),
        (bulk(b"role".to_vec()), bulk(b"master".to_vec())),
        (bulk(b"modules".to_vec()), hello_modules()),
    ])
}

fn hello_modules() -> Frame {
    #[cfg(feature = "redis-modules")]
    {
        crate::commands::redis_modules::module_list_frame()
    }
    #[cfg(not(feature = "redis-modules"))]
    {
        Frame::Array(Vec::new())
    }
}
