pub(crate) const FAST_REQUEST_MAGIC: u8 = 0xFA;
pub(crate) const FAST_RESPONSE_MAGIC: u8 = 0xFB;
pub(crate) const FAST_PROTOCOL_VERSION: u8 = 2;

pub(crate) const FAST_FLAG_KEY_HASH: u8 = 0x01;
pub(crate) const FAST_FLAG_ROUTE_SHARD: u8 = 0x02;
pub(crate) const FAST_FLAG_KEY_TAG: u8 = 0x04;
#[cfg(feature = "redis")]
pub(crate) const FAST_FLAG_REDIS_COMMAND_ARGS: u8 = 0x08;
pub(crate) const ROUTED_FLAGS: u8 = FAST_FLAG_KEY_HASH | FAST_FLAG_ROUTE_SHARD | FAST_FLAG_KEY_TAG;

pub(crate) const STATUS_OK: u8 = 0;
pub(crate) const STATUS_NULL: u8 = 1;
pub(crate) const STATUS_ERROR: u8 = 2;
pub(crate) const STATUS_INTEGER: u8 = 3;
pub(crate) const STATUS_VALUE: u8 = 4;
#[cfg(feature = "redis")]
pub(crate) const STATUS_ARRAY: u8 = 6;
#[cfg(feature = "redis")]
pub(crate) const STATUS_FLOAT: u8 = 7;
