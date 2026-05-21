use std::collections::BTreeSet;

use bytes::BytesMut;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use smallvec::SmallVec;

use crate::config::TransactionMode;
use crate::protocol::{BorrowedCommandParts, FastCommand, FastRequest, Frame, RespCodec};
use crate::storage::{BorrowedCommand, EmbeddedRouteMode, EmbeddedStore};

use super::wire::ServerWire;

const CROSSSLOT_ERROR: &str = "CROSSSLOT Keys in request don't hash to the same shard";

#[derive(Debug)]
pub(super) struct TransactionCoordinator {
    mode: TransactionMode,
    gates: Vec<RwLock<()>>,
}

impl TransactionCoordinator {
    pub(super) fn new(shard_count: usize, mode: TransactionMode) -> Option<Self> {
        match mode {
            TransactionMode::Disabled => None,
            TransactionMode::ShardLocal | TransactionMode::CoordinatedCrossShard => Some(Self {
                mode,
                gates: (0..shard_count).map(|_| RwLock::new(())).collect(),
            }),
        }
    }

    pub(super) fn read_guard_for_parts<'a>(
        &'a self,
        store: &EmbeddedStore,
        parts: &[&[u8]],
    ) -> TransactionReadGuard<'a> {
        let shards = command_shards(store, parts);
        self.read_guard_for_shards(&shards)
    }

    pub(super) fn read_guard_for_fast_request<'a>(
        &'a self,
        store: &EmbeddedStore,
        request: &FastRequest<'_>,
    ) -> TransactionReadGuard<'a> {
        let shards = fast_request_shards(store, request);
        self.read_guard_for_shards(&shards)
    }

    pub(super) fn read_guard_for_fcnp_key_hash<'a>(
        &'a self,
        store: &EmbeddedStore,
        key_hash: u64,
    ) -> TransactionReadGuard<'a> {
        match store.route_mode() {
            EmbeddedRouteMode::FullKey => {
                let shard_id = crate::storage::stripe_index(
                    key_hash,
                    crate::storage::shift_for(store.shard_count()),
                );
                self.read_guard_for_shards(&[shard_id])
            }
            EmbeddedRouteMode::SessionPrefix => {
                let shards = (0..store.shard_count()).collect::<SmallVec<[usize; 8]>>();
                self.read_guard_for_shards(&shards)
            }
        }
    }

    fn read_guard_for_shards<'a>(&'a self, shards: &[usize]) -> TransactionReadGuard<'a> {
        TransactionReadGuard {
            _guards: shards
                .iter()
                .map(|shard_id| self.gates[*shard_id].read())
                .collect(),
        }
    }

    fn write_guard_for_shards<'a>(&'a self, shards: &[usize]) -> TransactionWriteGuard<'a> {
        TransactionWriteGuard {
            _guards: shards
                .iter()
                .map(|shard_id| self.gates[*shard_id].write())
                .collect(),
        }
    }

    fn execute(&self, store: &EmbeddedStore, commands: &[QueuedCommand], out: &mut BytesMut) {
        let shards = transaction_shards(store, commands);
        if self.mode == TransactionMode::ShardLocal && shards.len() > 1 {
            ServerWire::write_resp_error(out, CROSSSLOT_ERROR);
            return;
        }

        let _guard = self.write_guard_for_shards(&shards);
        ServerWire::write_resp_array_header(out, commands.len());
        let now_ms = crate::storage::now_millis();
        for command in commands {
            let parts = command.borrowed_parts();
            let frame = match BorrowedCommand::from_parts(&parts) {
                Ok(command) => command.execute_borrowed_frame(store, now_ms),
                Err(error) => Frame::Error(format!("ERR {error}")),
            };
            let mut encoded = Vec::new();
            RespCodec::encode(&frame, &mut encoded);
            out.extend_from_slice(&encoded);
        }
    }
}

pub(super) struct TransactionReadGuard<'a> {
    _guards: SmallVec<[RwLockReadGuard<'a, ()>; 8]>,
}

struct TransactionWriteGuard<'a> {
    _guards: SmallVec<[RwLockWriteGuard<'a, ()>; 8]>,
}

#[derive(Debug, Default)]
pub(super) struct TransactionState {
    queued: Vec<QueuedCommand>,
    dirty: bool,
    active: bool,
}

impl TransactionState {
    pub(super) fn is_active(&self) -> bool {
        self.active
    }

    pub(super) fn mark_dirty(&mut self) {
        if self.active {
            self.dirty = true;
        }
    }

    pub(super) fn handle_resp_command(
        &mut self,
        coordinator: Option<&TransactionCoordinator>,
        store: &EmbeddedStore,
        parts: &[&[u8]],
        out: &mut BytesMut,
    ) -> bool {
        let Some(command) = parts.first().copied() else {
            return false;
        };

        if command.eq_ignore_ascii_case(b"MULTI") {
            self.multi(coordinator, parts, out);
            return true;
        }
        if command.eq_ignore_ascii_case(b"DISCARD") {
            self.discard(parts, out);
            return true;
        }
        if command.eq_ignore_ascii_case(b"EXEC") {
            self.exec(coordinator, store, parts, out);
            return true;
        }

        if !self.active {
            return false;
        }

        self.queue_command(parts, out);
        true
    }

    fn multi(
        &mut self,
        coordinator: Option<&TransactionCoordinator>,
        parts: &[&[u8]],
        out: &mut BytesMut,
    ) {
        match (coordinator.is_some(), parts.len(), self.active) {
            (false, _, _) => ServerWire::write_resp_error(out, "ERR transactions are disabled"),
            (true, len, _) if len != 1 => write_wrong_arity(out, "multi"),
            (true, 1, true) => {
                ServerWire::write_resp_error(out, "ERR MULTI calls can not be nested")
            }
            (true, 1, false) => {
                self.active = true;
                self.dirty = false;
                self.queued.clear();
                write_simple_string(out, "OK");
            }
            (true, _, _) => unreachable!("non-unit MULTI arity is handled by guard"),
        }
    }

    fn discard(&mut self, parts: &[&[u8]], out: &mut BytesMut) {
        match (parts.len(), self.active) {
            (len, _) if len != 1 => write_wrong_arity(out, "discard"),
            (1, false) => ServerWire::write_resp_error(out, "ERR DISCARD without MULTI"),
            (1, true) => {
                self.clear();
                write_simple_string(out, "OK");
            }
            (_, _) => unreachable!("non-unit DISCARD arity is handled by guard"),
        }
    }

    fn exec(
        &mut self,
        coordinator: Option<&TransactionCoordinator>,
        store: &EmbeddedStore,
        parts: &[&[u8]],
        out: &mut BytesMut,
    ) {
        if parts.len() != 1 {
            write_wrong_arity(out, "exec");
            return;
        }
        if !self.active {
            ServerWire::write_resp_error(out, "ERR EXEC without MULTI");
            return;
        }
        if self.dirty {
            self.clear();
            ServerWire::write_resp_error(
                out,
                "EXECABORT Transaction discarded because of previous errors.",
            );
            return;
        }

        let Some(coordinator) = coordinator else {
            self.clear();
            ServerWire::write_resp_error(out, "ERR transactions are disabled");
            return;
        };
        let queued = std::mem::take(&mut self.queued);
        self.active = false;
        coordinator.execute(store, &queued, out);
    }

    fn queue_command(&mut self, parts: &[&[u8]], out: &mut BytesMut) {
        match BorrowedCommand::from_parts(parts) {
            Ok(_) => {
                self.queued.push(QueuedCommand::new(parts));
                write_simple_string(out, "QUEUED");
            }
            Err(error) => {
                self.dirty = true;
                ServerWire::write_resp_error(out, &format!("ERR {error}"));
            }
        }
    }

    fn clear(&mut self) {
        self.queued.clear();
        self.dirty = false;
        self.active = false;
    }
}

#[derive(Debug)]
struct QueuedCommand {
    parts: Vec<Vec<u8>>,
}

impl QueuedCommand {
    fn new(parts: &[&[u8]]) -> Self {
        Self {
            parts: parts.iter().map(|part| part.to_vec()).collect(),
        }
    }

    fn borrowed_parts(&self) -> BorrowedCommandParts<'_> {
        self.parts.iter().map(Vec::as_slice).collect()
    }
}

fn transaction_shards(store: &EmbeddedStore, commands: &[QueuedCommand]) -> Vec<usize> {
    let mut shards = BTreeSet::new();
    for command in commands {
        shards.extend(command_shards(store, &command.borrowed_parts()));
    }
    shards.into_iter().collect()
}

fn command_shards(store: &EmbeddedStore, parts: &[&[u8]]) -> Vec<usize> {
    let Some((command, args)) = parts.split_first() else {
        return Vec::new();
    };
    if is_fcnp_scan_shard_command(command) {
        return fcnp_scan_shard(store, args);
    }
    let keys = command_keys(command, args);
    let mut shards = BTreeSet::new();
    match keys {
        CommandKeys::None => {}
        CommandKeys::AllShards => shards.extend(0..store.shard_count()),
        CommandKeys::Keys(keys) => {
            for key in keys {
                shards.insert(store.route_key(key).shard_id);
            }
        }
    }
    shards.into_iter().collect()
}

fn fast_request_shards(store: &EmbeddedStore, request: &FastRequest<'_>) -> Vec<usize> {
    let mut shards = BTreeSet::new();
    match &request.command {
        FastCommand::RespCommand { parts } => return command_shards(store, parts),
        FastCommand::MGet { keys } => {
            shards.extend(keys.iter().map(|key| store.route_key(key).shard_id));
        }
        FastCommand::MSet { items } => {
            shards.extend(items.iter().map(|(key, _)| store.route_key(key).shard_id));
        }
        command => {
            if let Some(key) = command.route_key() {
                shards.insert(store.route_key(key).shard_id);
            }
        }
    }
    shards.into_iter().collect()
}

enum CommandKeys<'a> {
    None,
    AllShards,
    Keys(SmallVec<[&'a [u8]; 8]>),
}

fn command_keys<'a>(command: &[u8], args: &'a [&'a [u8]]) -> CommandKeys<'a> {
    if is_no_key_command(command) {
        return CommandKeys::None;
    }
    if is_all_shard_command(command) {
        return CommandKeys::AllShards;
    }
    if command.eq_ignore_ascii_case(b"DEL")
        || command.eq_ignore_ascii_case(b"UNLINK")
        || command.eq_ignore_ascii_case(b"TOUCH")
        || command.eq_ignore_ascii_case(b"MGET")
        || command.eq_ignore_ascii_case(b"SUNION")
        || command.eq_ignore_ascii_case(b"SINTER")
        || command.eq_ignore_ascii_case(b"SDIFF")
    {
        return CommandKeys::Keys(args.iter().copied().collect());
    }
    if command.eq_ignore_ascii_case(b"MSET") || command.eq_ignore_ascii_case(b"MSETNX") {
        return every_nth_key(args, 0, 2);
    }
    if command.eq_ignore_ascii_case(b"COPY")
        || command.eq_ignore_ascii_case(b"RENAME")
        || command.eq_ignore_ascii_case(b"RENAMENX")
        || command.eq_ignore_ascii_case(b"RPOPLPUSH")
        || command.eq_ignore_ascii_case(b"LMOVE")
        || command.eq_ignore_ascii_case(b"BLMOVE")
        || command.eq_ignore_ascii_case(b"SMOVE")
    {
        return first_n_keys(args, 2);
    }
    if command.eq_ignore_ascii_case(b"OBJECT") {
        return first_n_keys(args.get(1..).unwrap_or_default(), 1);
    }
    if command.eq_ignore_ascii_case(b"BITOP") {
        return first_and_tail_keys(args, 1, 2);
    }
    if command.eq_ignore_ascii_case(b"BLPOP")
        || command.eq_ignore_ascii_case(b"BRPOP")
        || command.eq_ignore_ascii_case(b"BZPOPMIN")
        || command.eq_ignore_ascii_case(b"BZPOPMAX")
    {
        return keys_before_last_arg(args);
    }
    if command.eq_ignore_ascii_case(b"SUNIONSTORE")
        || command.eq_ignore_ascii_case(b"SINTERSTORE")
        || command.eq_ignore_ascii_case(b"SDIFFSTORE")
    {
        return first_and_tail_keys(args, 0, 1);
    }
    if command.eq_ignore_ascii_case(b"ZUNIONSTORE")
        || command.eq_ignore_ascii_case(b"ZINTERSTORE")
        || command.eq_ignore_ascii_case(b"ZDIFFSTORE")
    {
        return zaggregate_store_keys(args);
    }
    if command.eq_ignore_ascii_case(b"ZRANGESTORE") {
        return first_n_keys(args, 2);
    }
    first_n_keys(args, 1)
}

fn is_no_key_command(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"PING")
        || command.eq_ignore_ascii_case(b"AUTH")
        || command.eq_ignore_ascii_case(b"HELLO")
        || command.eq_ignore_ascii_case(b"SELECT")
        || command.eq_ignore_ascii_case(b"QUIT")
        || command.eq_ignore_ascii_case(b"ECHO")
        || command.eq_ignore_ascii_case(b"COMMAND")
        || command.eq_ignore_ascii_case(b"CONFIG")
        || command.eq_ignore_ascii_case(b"CLIENT")
        || command.eq_ignore_ascii_case(b"TIME")
        || command.eq_ignore_ascii_case(b"INFO")
        || command.eq_ignore_ascii_case(b"DBSIZE")
}

fn is_all_shard_command(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"KEYS")
        || command.eq_ignore_ascii_case(b"SCAN")
        || command.eq_ignore_ascii_case(b"RANDOMKEY")
        || command.eq_ignore_ascii_case(b"FCNP.SCAN")
}

fn is_fcnp_scan_shard_command(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"FCNP.SCANSHARD")
        || command.eq_ignore_ascii_case(b"FCNP.SCAN.SHARD")
}

fn fcnp_scan_shard(store: &EmbeddedStore, args: &[&[u8]]) -> Vec<usize> {
    match args
        .first()
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|shard_id| *shard_id < store.shard_count())
    {
        Some(shard_id) => vec![shard_id],
        None => (0..store.shard_count()).collect(),
    }
}

fn first_n_keys<'a>(args: &'a [&'a [u8]], count: usize) -> CommandKeys<'a> {
    CommandKeys::Keys(args.iter().take(count).copied().collect())
}

fn every_nth_key<'a>(args: &'a [&'a [u8]], start: usize, step: usize) -> CommandKeys<'a> {
    CommandKeys::Keys(args.iter().skip(start).step_by(step).copied().collect())
}

fn first_and_tail_keys<'a>(
    args: &'a [&'a [u8]],
    first_count: usize,
    tail_start: usize,
) -> CommandKeys<'a> {
    let mut keys = SmallVec::new();
    keys.extend(args.iter().take(first_count).copied());
    keys.extend(args.iter().skip(tail_start).copied());
    CommandKeys::Keys(keys)
}

fn keys_before_last_arg<'a>(args: &'a [&'a [u8]]) -> CommandKeys<'a> {
    CommandKeys::Keys(
        args.iter()
            .take(args.len().saturating_sub(1))
            .copied()
            .collect(),
    )
}

fn zaggregate_store_keys<'a>(args: &'a [&'a [u8]]) -> CommandKeys<'a> {
    if args.len() < 2 {
        return first_n_keys(args, 1);
    }
    let Ok(numkeys) = std::str::from_utf8(args[1])
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or(())
    else {
        return CommandKeys::AllShards;
    };
    let mut keys = SmallVec::new();
    keys.push(args[0]);
    keys.extend(args.iter().skip(2).take(numkeys).copied());
    CommandKeys::Keys(keys)
}

fn write_simple_string(out: &mut BytesMut, value: &str) {
    out.extend_from_slice(b"+");
    out.extend_from_slice(value.as_bytes());
    out.extend_from_slice(b"\r\n");
}

fn write_wrong_arity(out: &mut BytesMut, command: &str) {
    ServerWire::write_resp_error(
        out,
        &format!("ERR wrong number of arguments for '{}' command", command),
    );
}
