#![doc = include_str!("../README.md")]

mod client;
mod commands;
mod connection;
mod error;
mod protocol;
mod routing;

pub use client::{FcnpClient, FcnpDirectClient, FcnpDirectShardClient};
pub use error::{FcnpClientError, Result};
pub use routing::{
    FcnpDirectRouter, FcnpRoute, FcnpRouteMode, hash_key, hash_key_tag, shard_index,
};
