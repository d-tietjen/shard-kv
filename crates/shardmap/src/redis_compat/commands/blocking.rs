use std::time::{Duration, Instant};

use crate::commands::redis::{error, parse_f64};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

const CROSSSLOT_ERROR: &str = "CROSSSLOT Keys in request don't hash to the same shard";

pub(crate) fn parse_blocking_timeout(raw: &[u8]) -> std::result::Result<Option<Duration>, Frame> {
    let timeout =
        parse_f64(raw).map_err(|_| error("ERR timeout is not a float or out of range"))?;
    if timeout < 0.0 {
        return Err(error("ERR timeout is negative"));
    }
    if timeout == 0.0 {
        return Ok(None);
    }
    if timeout >= u64::MAX as f64 {
        return Err(error("ERR timeout is not a float or out of range"));
    }
    Ok(Some(Duration::from_secs_f64(timeout)))
}

pub(crate) fn single_shard_for_keys(
    store: &EmbeddedStore,
    keys: &[&[u8]],
) -> std::result::Result<usize, Frame> {
    let Some(first) = keys.first() else {
        return Err(error("ERR syntax error"));
    };
    let shard_id = store.route_key(first).shard_id;
    match keys
        .iter()
        .all(|key| store.route_key(key).shard_id == shard_id)
    {
        true => Ok(shard_id),
        false => Err(error(CROSSSLOT_ERROR)),
    }
}

pub(crate) fn block_on_shard(
    store: &EmbeddedStore,
    shard_id: usize,
    timeout: Option<Duration>,
    mut try_once: impl FnMut() -> Frame,
) -> Frame {
    let deadline = timeout.and_then(|duration| Instant::now().checked_add(duration));
    loop {
        let observed_generation = store.redis_object_shard_wait_generation(shard_id);
        let frame = try_once();
        if !matches!(frame, Frame::Null) {
            return frame;
        }
        let wait_timeout = match remaining_timeout(deadline) {
            TimeoutState::Ready(remaining) => remaining,
            TimeoutState::Expired => return Frame::Null,
        };
        store.wait_for_redis_object_shard_change(shard_id, observed_generation, wait_timeout);
    }
}

enum TimeoutState {
    Ready(Option<Duration>),
    Expired,
}

fn remaining_timeout(deadline: Option<Instant>) -> TimeoutState {
    match deadline {
        Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
            Some(remaining) if !remaining.is_zero() => TimeoutState::Ready(Some(remaining)),
            _ => TimeoutState::Expired,
        },
        None => TimeoutState::Ready(None),
    }
}
