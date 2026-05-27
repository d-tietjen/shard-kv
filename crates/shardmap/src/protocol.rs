//! Wire protocol codecs.
//!
//! [`RespCodec`] parses and encodes RESP frames for Redis-compatible clients.
//! [`FastCodec`] handles the crate's native binary protocol used by
//! shardcache clients that want lower framing overhead.

mod fast;
mod resp;

pub use fast::{
    FAST_FLAG_KEY_HASH, FAST_FLAG_KEY_TAG, FAST_FLAG_REDIS_COMMAND_ARGS, FAST_FLAG_ROUTE_SHARD,
    FAST_PROTOCOL_VERSION, FAST_REDIS_COMMAND_OPCODES, FAST_REQUEST_MAGIC, FAST_RESPONSE_MAGIC,
    FastCodec, FastCommand, FastCommandKind, FastRedisCommandOpcode, FastRedisRouteKeys,
    FastRequest, FastRequestDecodeResult, FastResponse, FastResponseDecodeResult,
};
pub use resp::{
    BorrowedCommandFrame, BorrowedCommandParts, CommandFrame, CommandSpanFrame, Frame, RespCodec,
    RespCommandDecodeResult, RespCommandSpanDecodeResult, RespDecodeResult,
};
