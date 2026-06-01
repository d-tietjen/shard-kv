use crate::storage::RedisKeyStore;
#[cfg(feature = "server")]
use bytes::BytesMut;

use crate::commands::redis::{
    array_bulk, int, reserve_resp_bulk_array_hint, write_resp_array_header, write_resp_wrong_arity,
    write_resp_wrongtype, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, RedisObjectError, RedisObjectValue};

#[derive(Clone, Copy)]
pub(crate) enum SetOp {
    Union,
    Inter,
    Diff,
}

impl SetOp {
    pub(crate) fn name(self, store: bool) -> &'static str {
        match (self, store) {
            (Self::Union, false) => "SUNION",
            (Self::Inter, false) => "SINTER",
            (Self::Diff, false) => "SDIFF",
            (Self::Union, true) => "SUNIONSTORE",
            (Self::Inter, true) => "SINTERSTORE",
            (Self::Diff, true) => "SDIFFSTORE",
        }
    }
}

pub(crate) fn set_op(
    store: &EmbeddedStore,
    args: &[&[u8]],
    op: SetOp,
    dest: Option<&[u8]>,
) -> Frame {
    if args.is_empty() {
        return wrong_arity(op.name(dest.is_some()));
    }
    let result = match compute_set_op(store, args, op) {
        Ok(values) => values,
        Err(frame) => return frame,
    };
    match dest {
        Some(dest) => {
            let len = result.len();
            store.set_object_value(dest, RedisObjectValue::Set(result), None);
            int(len as i64)
        }
        None => array_bulk(result),
    }
}

pub(crate) fn set_store(store: &EmbeddedStore, args: &[&[u8]], op: SetOp) -> Frame {
    if args.len() < 2 {
        return wrong_arity(op.name(true));
    }
    set_op(store, &args[1..], op, Some(args[0]))
}

#[cfg(feature = "server")]
pub(crate) fn write_set_op_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    op: SetOp,
    out: &mut BytesMut,
) {
    if args.is_empty() {
        write_resp_wrong_arity(out, op.name(false));
        return;
    }
    let result = match compute_set_op(store, args, op) {
        Ok(values) => values,
        Err(_) => {
            write_resp_wrongtype(out);
            return;
        }
    };
    reserve_resp_bulk_array_hint(out, result.len());
    write_resp_array_header(out, result.len());
    for value in result {
        ServerWire::write_resp_blob_string(out, &value);
    }
}

#[cfg(feature = "server")]
pub(crate) fn write_set_store_resp(
    store: &EmbeddedStore,
    args: &[&[u8]],
    op: SetOp,
    out: &mut BytesMut,
) {
    if args.len() < 2 {
        write_resp_wrong_arity(out, op.name(true));
        return;
    }
    let result = match compute_set_op(store, &args[1..], op) {
        Ok(values) => values,
        Err(_) => {
            write_resp_wrongtype(out);
            return;
        }
    };
    let len = result.len();
    store.set_object_value(args[0], RedisObjectValue::Set(result), None);
    ServerWire::write_resp_integer(out, len as i64);
}

pub(crate) fn compute_set_op(
    store: &EmbeddedStore,
    keys: &[&[u8]],
    op: SetOp,
) -> std::result::Result<Vec<Vec<u8>>, Frame> {
    let mut sets = Vec::with_capacity(keys.len());
    for key in keys {
        match store.set_members(key) {
            Ok(members) => sets.push(members),
            Err(RedisObjectError::WrongType) => return Err(wrongtype()),
            Err(RedisObjectError::MissingKey) => sets.push(Vec::new()),
        }
    }
    let mut result = std::collections::BTreeSet::<Vec<u8>>::new();
    match op {
        SetOp::Union => {
            for set in sets {
                result.extend(set);
            }
        }
        SetOp::Inter => {
            if let Some((first, rest)) = sets.split_first() {
                result.extend(
                    first
                        .iter()
                        .filter(|&member| rest.iter().all(|set| set.binary_search(member).is_ok()))
                        .cloned(),
                );
            }
        }
        SetOp::Diff => {
            if let Some((first, rest)) = sets.split_first() {
                result.extend(
                    first
                        .iter()
                        .filter(|member| !rest.iter().any(|set| set.binary_search(*member).is_ok()))
                        .cloned(),
                );
            }
        }
    }
    Ok(result.into_iter().collect())
}
