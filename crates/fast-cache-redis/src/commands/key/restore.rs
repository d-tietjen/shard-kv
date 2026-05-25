use bytes::BytesMut;

use crate::commands::dump_restore::{
    DumpRestoreValue, decode_dump_payload, decode_string_dump_payload_slice,
};
use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, parse_i64, simple, write_frame,
    write_resp_simple_string, wrong_arity,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
#[cfg(feature = "server")]
use crate::storage::hash_key_tag_from_hash;
use crate::storage::{EmbeddedStore, RedisKeyStore, now_millis};

define_redis_command!(Restore, "RESTORE", true, aliases: ["RESTORE-ASKING"]);

impl crate::commands::redis::RedisCommand for Restore {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match restore_key(store, args) {
            Ok(()) => simple("OK"),
            Err(RestoreError::WrongArity) => wrong_arity("RESTORE"),
            Err(RestoreError::Syntax) => error("ERR syntax error"),
            Err(RestoreError::InvalidTtl) => error("ERR Invalid TTL value, must be >= 0"),
            Err(RestoreError::BusyKey) => error("BUSYKEY Target key name already exists."),
            Err(RestoreError::BadPayload) => {
                error("ERR DUMP payload version or checksum are wrong")
            }
            Err(RestoreError::InvalidIdleOrFreq) => {
                error("ERR value is not an integer or out of range")
            }
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        match restore_key(store, args) {
            Ok(()) => write_resp_simple_string(out, "OK"),
            Err(error) => write_restore_error(out, error),
        }
    }

    #[cfg(feature = "server")]
    fn write_resp_owned_shard(
        store: &EmbeddedStore,
        args: &[&[u8]],
        owned_shard_id: usize,
        out: &mut BytesMut,
    ) -> bool {
        let Some(result) = restore_key_owned_shard(store, args, owned_shard_id) else {
            return false;
        };
        match result {
            Ok(()) => write_resp_simple_string(out, "OK"),
            Err(error) => write_restore_error(out, error),
        }
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestoreError {
    WrongArity,
    Syntax,
    InvalidTtl,
    BusyKey,
    BadPayload,
    InvalidIdleOrFreq,
}

#[derive(Debug, Default)]
struct RestoreOptions {
    replace: bool,
    absttl: bool,
    idletime: bool,
    freq: bool,
}

fn restore_key(store: &EmbeddedStore, args: &[&[u8]]) -> Result<(), RestoreError> {
    let [key, ttl, payload, options @ ..] = args else {
        return Err(RestoreError::WrongArity);
    };
    let ttl = parse_i64(ttl).map_err(|_| RestoreError::InvalidTtl)?;
    if ttl < 0 {
        return Err(RestoreError::InvalidTtl);
    }
    let options = parse_options(options)?;
    if !options.replace && store.exists(key) {
        return Err(RestoreError::BusyKey);
    }
    let value = decode_dump_payload(payload).map_err(|_| RestoreError::BadPayload)?;
    let ttl_ms = match (ttl, options.absttl) {
        (0, _) => None,
        (ttl, true) => {
            let now_ms = now_millis();
            if ttl as u64 <= now_ms {
                if options.replace {
                    store.delete(key);
                }
                return Ok(());
            }
            Some((ttl as u64).saturating_sub(now_ms))
        }
        (ttl, false) => Some(ttl as u64),
    };

    match value {
        DumpRestoreValue::String(value) => store.set_value_bytes(key, value.into(), ttl_ms),
        DumpRestoreValue::Object(value) => store.set_object_value(key, value, ttl_ms),
    }
    Ok(())
}

#[cfg(feature = "server")]
fn restore_key_owned_shard(
    store: &EmbeddedStore,
    args: &[&[u8]],
    owned_shard_id: usize,
) -> Option<Result<(), RestoreError>> {
    let [key, ttl, payload, options @ ..] = args else {
        return Some(Err(RestoreError::WrongArity));
    };
    if *ttl != b"0" {
        return None;
    }
    let options = match parse_options(options) {
        Ok(options) => options,
        Err(error) => return Some(Err(error)),
    };
    if !options.replace || options.absttl || options.idletime || options.freq {
        return None;
    }

    let value = match decode_string_dump_payload_slice(payload) {
        Ok(value) => value,
        Err(_) => return None,
    };
    let route = store.route_key(key);
    if route.shard_id != owned_shard_id {
        return None;
    }
    let key_tag = hash_key_tag_from_hash(route.key_hash);

    #[cfg(feature = "unsafe")]
    {
        // SAFETY: direct-shard RESP requests are route-checked before command
        // dispatch, so this worker owns the shard for this key.
        if unsafe {
            store.set_slice_hashed_tagged_owned_shard_no_ttl_hot(
                owned_shard_id,
                route.key_hash,
                key_tag,
                key,
                value,
            )
        } {
            return Some(Ok(()));
        }
    }

    #[cfg(not(feature = "unsafe"))]
    {
        if store.set_slice_hashed_tagged_owned_shard_no_ttl(
            owned_shard_id,
            route.key_hash,
            key_tag,
            key,
            value,
        ) {
            return Some(Ok(()));
        }
    }

    None
}

#[cfg(feature = "server")]
fn write_restore_error(out: &mut BytesMut, error: RestoreError) {
    match error {
        RestoreError::WrongArity => write_frame(out, &wrong_arity("RESTORE")),
        RestoreError::Syntax => ServerWire::write_resp_error(out, "ERR syntax error"),
        RestoreError::InvalidTtl => {
            ServerWire::write_resp_error(out, "ERR Invalid TTL value, must be >= 0")
        }
        RestoreError::BusyKey => {
            ServerWire::write_resp_error(out, "BUSYKEY Target key name already exists.")
        }
        RestoreError::BadPayload => {
            ServerWire::write_resp_error(out, "ERR DUMP payload version or checksum are wrong")
        }
        RestoreError::InvalidIdleOrFreq => {
            ServerWire::write_resp_error(out, "ERR value is not an integer or out of range")
        }
    }
}

fn parse_options(options: &[&[u8]]) -> Result<RestoreOptions, RestoreError> {
    let mut parsed = RestoreOptions::default();
    let mut cursor = 0;
    while cursor < options.len() {
        let option = options[cursor];
        if eq_ignore_ascii_case(option, b"REPLACE") {
            parsed.replace = true;
            cursor += 1;
        } else if eq_ignore_ascii_case(option, b"ABSTTL") {
            parsed.absttl = true;
            cursor += 1;
        } else if eq_ignore_ascii_case(option, b"IDLETIME") {
            if parsed.freq || parsed.idletime {
                return Err(RestoreError::Syntax);
            }
            let Some(value) = options.get(cursor + 1) else {
                return Err(RestoreError::Syntax);
            };
            let value = parse_i64(value).map_err(|_| RestoreError::InvalidIdleOrFreq)?;
            if value < 0 {
                return Err(RestoreError::InvalidIdleOrFreq);
            }
            parsed.idletime = true;
            cursor += 2;
        } else if eq_ignore_ascii_case(option, b"FREQ") {
            if parsed.idletime || parsed.freq {
                return Err(RestoreError::Syntax);
            }
            let Some(value) = options.get(cursor + 1) else {
                return Err(RestoreError::Syntax);
            };
            let value = parse_i64(value).map_err(|_| RestoreError::InvalidIdleOrFreq)?;
            if !(0..=255).contains(&value) {
                return Err(RestoreError::InvalidIdleOrFreq);
            }
            parsed.freq = true;
            cursor += 2;
        } else {
            return Err(RestoreError::Syntax);
        }
    }
    Ok(parsed)
}
