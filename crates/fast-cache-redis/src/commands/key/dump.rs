use bytes::BytesMut;

#[cfg(feature = "server")]
use crate::commands::dump_restore::write_string_dump_resp;
use crate::commands::dump_restore::{
    DumpRestoreValue, encode_dump_value, encode_string_dump_value,
};
use crate::commands::redis::{bulk, define_redis_command, write_frame, wrong_arity};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
#[cfg(feature = "server")]
use crate::storage::hash_key_tag_from_hash;
use crate::storage::{EmbeddedStore, RedisKeyStore};

define_redis_command!(Dump, "DUMP", false);

impl crate::commands::redis::RedisCommand for Dump {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [key] => dump_key(store, key).map_or(Frame::Null, bulk),
            _ => wrong_arity("DUMP"),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match args {
            [key] => write_dump_resp(store, key, out),
            _ => write_frame(out, &wrong_arity("DUMP")),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp_owned_shard(
        store: &EmbeddedStore,
        args: &[&[u8]],
        owned_shard_id: usize,
        out: &mut BytesMut,
    ) -> bool {
        match args {
            [key] => write_dump_resp_owned_shard(store, key, owned_shard_id, out),
            _ => false,
        }
    }
}

#[cfg(feature = "server")]
fn write_dump_resp(store: &EmbeddedStore, key: &[u8], out: &mut BytesMut) {
    if let Some(value) = store.get_ref(key) {
        write_string_dump_resp(out, value.value());
        return;
    }
    match store
        .clone_object_value(key)
        .map(|value| encode_dump_value(&DumpRestoreValue::Object(value)))
    {
        Some(payload) => ServerWire::write_resp_blob_string(out, &payload),
        None => out.extend_from_slice(b"$-1\r\n"),
    }
}

#[cfg(feature = "server")]
fn write_dump_resp_owned_shard(
    store: &EmbeddedStore,
    key: &[u8],
    owned_shard_id: usize,
    out: &mut BytesMut,
) -> bool {
    let route = store.route_key(key);
    if route.shard_id != owned_shard_id {
        return false;
    }

    let key_tag = hash_key_tag_from_hash(route.key_hash);
    #[cfg(feature = "unsafe")]
    {
        // SAFETY: direct-shard RESP requests are route-checked before command
        // dispatch, so this worker owns the shard for this key.
        let outcome = unsafe {
            store.with_shared_value_bytes_full_key_tagged_owned_shard_no_ttl(
                owned_shard_id,
                route.key_hash,
                key_tag,
                key.len(),
                |value| write_string_dump_resp(out, value),
            )
        };
        if matches!(outcome, Some(true)) {
            return true;
        }
    }

    #[cfg(not(feature = "unsafe"))]
    {
        let _ = key_tag;
        let outcome = store.with_shared_value_bytes_full_key_owned_shard_no_ttl(
            owned_shard_id,
            route.key_hash,
            key,
            |value| write_string_dump_resp(out, value),
        );
        if matches!(outcome, Some(true)) {
            return true;
        }
    }

    false
}

pub(crate) fn dump_key(store: &EmbeddedStore, key: &[u8]) -> Option<Vec<u8>> {
    if let Some(value) = store.get_value_bytes(key) {
        return Some(encode_string_dump_value(value.as_ref()));
    }
    store
        .clone_object_value(key)
        .map(|value| encode_dump_value(&DumpRestoreValue::Object(value)))
}
