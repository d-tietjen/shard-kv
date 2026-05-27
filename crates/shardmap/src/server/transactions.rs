use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::BytesMut;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use smallvec::SmallVec;

use crate::config::TransactionMode;
use crate::protocol::{
    BorrowedCommandParts, FastCommand, FastCommandKind, FastRedisRouteKeys, FastRequest,
};
use crate::storage::{BorrowedCommand, EmbeddedRouteMode, EmbeddedStore};
#[cfg(feature = "redis")]
use crate::storage::{RedisKeyStore, RedisObjectValue};

use super::commands::BorrowedCommandContext;
use super::direct_protocol::ScnpScanCommand;
use super::wire::{RespProtocolVersion, ServerWire};

const CROSSSLOT_ERROR: &str = "CROSSSLOT Keys in request don't hash to the same shard";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RespTransactionCommand {
    #[cfg(feature = "redis")]
    Watch,
    #[cfg(feature = "redis")]
    Unwatch,
    Multi,
    Discard,
    Exec,
}

impl RespTransactionCommand {
    const NAMES: &'static [(&'static [u8], Self)] = &[
        #[cfg(feature = "redis")]
        (b"WATCH", Self::Watch),
        #[cfg(feature = "redis")]
        (b"UNWATCH", Self::Unwatch),
        (b"MULTI", Self::Multi),
        (b"DISCARD", Self::Discard),
        (b"EXEC", Self::Exec),
    ];

    pub(super) fn from_name(name: &[u8]) -> Option<Self> {
        Self::NAMES.iter().find_map(|(candidate, command)| {
            name.eq_ignore_ascii_case(candidate).then_some(*command)
        })
    }
}

#[derive(Debug)]
pub(super) struct TransactionCoordinator {
    mode: TransactionMode,
    gates: Vec<RwLock<()>>,
    active_transactions: AtomicUsize,
}

impl TransactionCoordinator {
    pub(super) fn new(shard_count: usize, mode: TransactionMode) -> Option<Self> {
        match mode {
            TransactionMode::Disabled => None,
            TransactionMode::ShardLocal | TransactionMode::CoordinatedCrossShard => Some(Self {
                mode,
                gates: (0..shard_count).map(|_| RwLock::new(())).collect(),
                active_transactions: AtomicUsize::new(0),
            }),
        }
    }

    pub(super) fn read_guard_for_parts<'a>(
        &'a self,
        store: &EmbeddedStore,
        parts: &[&[u8]],
    ) -> TransactionReadGuard<'a> {
        if !self.has_active_transactions() {
            return TransactionReadGuard::empty();
        }
        let shards = command_shards(store, parts);
        self.read_guard_for_shards(&shards)
    }

    pub(super) fn read_guard_for_fast_request<'a>(
        &'a self,
        store: &EmbeddedStore,
        request: &FastRequest<'_>,
    ) -> TransactionReadGuard<'a> {
        if !self.has_active_transactions() {
            return TransactionReadGuard::empty();
        }
        let shards = fast_request_shards(store, request);
        self.read_guard_for_shards(&shards)
    }

    pub(super) fn read_guard_for_scnp_key_hash<'a>(
        &'a self,
        store: &EmbeddedStore,
        key_hash: u64,
    ) -> TransactionReadGuard<'a> {
        if !self.has_active_transactions() {
            return TransactionReadGuard::empty();
        }
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

    fn begin_transaction(&self) {
        self.active_transactions.fetch_add(1, Ordering::AcqRel);
    }

    fn end_transaction(&self) {
        let previous = self.active_transactions.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "transaction coordinator underflow");
    }

    pub(super) fn has_active_transactions(&self) -> bool {
        self.active_transactions.load(Ordering::Acquire) != 0
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

    fn execute(
        &self,
        store: &EmbeddedStore,
        commands: &[QueuedCommand],
        out: &mut BytesMut,
        resp_protocol: RespProtocolVersion,
    ) {
        let shards = transaction_shards(store, commands);
        if self.mode == TransactionMode::ShardLocal && shards.len() > 1 {
            ServerWire::write_resp_error(out, CROSSSLOT_ERROR);
            return;
        }

        let _guard = self.write_guard_for_shards(&shards);
        ServerWire::write_resp_array_header(out, commands.len());
        for command in commands {
            let parts = command.borrowed_parts();
            match BorrowedCommand::from_parts(&parts) {
                Ok(command) => command.execute_borrowed(BorrowedCommandContext {
                    store,
                    out,
                    fast_write_queue: None,
                    single_threaded: false,
                    resp_protocol,
                }),
                Err(error) => ServerWire::write_resp_error(out, &format!("ERR {error}")),
            }
        }
    }
}

pub(super) struct TransactionReadGuard<'a> {
    _guards: SmallVec<[RwLockReadGuard<'a, ()>; 8]>,
}

impl<'a> TransactionReadGuard<'a> {
    fn empty() -> Self {
        Self {
            _guards: SmallVec::new(),
        }
    }
}

struct TransactionWriteGuard<'a> {
    _guards: SmallVec<[RwLockWriteGuard<'a, ()>; 8]>,
}

#[derive(Debug, Default)]
pub(super) struct TransactionState {
    queued: Vec<QueuedCommand>,
    dirty: bool,
    active: bool,
    counted_active: bool,
    #[cfg(feature = "redis")]
    watched: Vec<WatchedKey>,
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
        resp_protocol: RespProtocolVersion,
    ) -> bool {
        match parts
            .first()
            .copied()
            .and_then(RespTransactionCommand::from_name)
        {
            #[cfg(feature = "redis")]
            Some(RespTransactionCommand::Watch) => {
                self.watch(coordinator, store, parts, out);
                true
            }
            #[cfg(feature = "redis")]
            Some(RespTransactionCommand::Unwatch) => {
                self.unwatch(coordinator, parts, out);
                true
            }
            Some(RespTransactionCommand::Multi) => {
                self.multi(coordinator, parts, out);
                true
            }
            Some(RespTransactionCommand::Discard) => {
                self.discard(coordinator, parts, out);
                true
            }
            Some(RespTransactionCommand::Exec) => {
                self.exec(coordinator, store, parts, out, resp_protocol);
                true
            }
            None if self.active => {
                self.queue_command(parts, out);
                true
            }
            None => false,
        }
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
                if let Some(coordinator) = coordinator
                    && !self.counted_active
                {
                    coordinator.begin_transaction();
                    self.counted_active = true;
                }
                self.queued.clear();
                write_simple_string(out, "OK");
            }
            (true, _, _) => unreachable!("non-unit MULTI arity is handled by guard"),
        }
    }

    fn discard(
        &mut self,
        coordinator: Option<&TransactionCoordinator>,
        parts: &[&[u8]],
        out: &mut BytesMut,
    ) {
        match (parts.len(), self.active) {
            (len, _) if len != 1 => write_wrong_arity(out, "discard"),
            (1, false) => ServerWire::write_resp_error(out, "ERR DISCARD without MULTI"),
            (1, true) => {
                self.clear(coordinator);
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
        resp_protocol: RespProtocolVersion,
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
            self.clear(coordinator);
            ServerWire::write_resp_error(
                out,
                "EXECABORT Transaction discarded because of previous errors.",
            );
            return;
        }
        #[cfg(feature = "redis")]
        if self.watched_keys_changed(store) {
            self.clear(coordinator);
            write_null_array(out);
            return;
        }

        let Some(coordinator) = coordinator else {
            self.clear(None);
            ServerWire::write_resp_error(out, "ERR transactions are disabled");
            return;
        };
        let queued = std::mem::take(&mut self.queued);
        self.active = false;
        #[cfg(feature = "redis")]
        self.watched.clear();
        coordinator.execute(store, &queued, out, resp_protocol);
        self.finish_counted_transaction(Some(coordinator));
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

    pub(super) fn close(&mut self, coordinator: Option<&TransactionCoordinator>) {
        self.clear(coordinator);
    }

    fn clear(&mut self, coordinator: Option<&TransactionCoordinator>) {
        self.finish_counted_transaction(coordinator);
        self.queued.clear();
        self.dirty = false;
        self.active = false;
        #[cfg(feature = "redis")]
        self.watched.clear();
    }

    fn finish_counted_transaction(&mut self, coordinator: Option<&TransactionCoordinator>) {
        if self.counted_active {
            if let Some(coordinator) = coordinator {
                coordinator.end_transaction();
            }
            self.counted_active = false;
        }
    }

    #[cfg(feature = "redis")]
    fn watch(
        &mut self,
        coordinator: Option<&TransactionCoordinator>,
        store: &EmbeddedStore,
        parts: &[&[u8]],
        out: &mut BytesMut,
    ) {
        match (coordinator.is_some(), parts.len(), self.active) {
            (false, _, _) => ServerWire::write_resp_error(out, "ERR transactions are disabled"),
            (true, 0 | 1, _) => write_wrong_arity(out, "watch"),
            (true, _, true) => {
                ServerWire::write_resp_error(out, "ERR WATCH inside MULTI is not allowed")
            }
            (true, _, false) => {
                for key in &parts[1..] {
                    let snapshot = WatchedSnapshot::capture(store, key);
                    match self.watched.iter_mut().find(|watched| watched.key == *key) {
                        Some(watched) => watched.snapshot = snapshot,
                        None => self.watched.push(WatchedKey {
                            key: key.to_vec(),
                            snapshot,
                        }),
                    }
                }
                write_simple_string(out, "OK");
            }
        }
    }

    #[cfg(feature = "redis")]
    fn unwatch(
        &mut self,
        coordinator: Option<&TransactionCoordinator>,
        parts: &[&[u8]],
        out: &mut BytesMut,
    ) {
        match (coordinator.is_some(), parts.len()) {
            (false, _) => ServerWire::write_resp_error(out, "ERR transactions are disabled"),
            (true, 1) => {
                self.watched.clear();
                write_simple_string(out, "OK");
            }
            (true, _) => write_wrong_arity(out, "unwatch"),
        }
    }

    #[cfg(feature = "redis")]
    fn watched_keys_changed(&self, store: &EmbeddedStore) -> bool {
        self.watched
            .iter()
            .any(|watched| WatchedSnapshot::capture(store, &watched.key) != watched.snapshot)
    }
}

#[cfg(feature = "redis")]
#[derive(Debug)]
struct WatchedKey {
    key: Vec<u8>,
    snapshot: WatchedSnapshot,
}

#[cfg(feature = "redis")]
#[derive(Debug, Clone, PartialEq)]
enum WatchedSnapshot {
    Missing,
    String(Vec<u8>),
    Object(RedisObjectValue),
}

#[cfg(feature = "redis")]
impl WatchedSnapshot {
    fn capture(store: &EmbeddedStore, key: &[u8]) -> Self {
        if let Some(value) = store.get_value_bytes(key) {
            return Self::String(value.to_vec());
        }
        if let Some(value) = store.clone_object_value(key) {
            return Self::Object(value);
        }
        Self::Missing
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

pub(super) fn command_shards(store: &EmbeddedStore, parts: &[&[u8]]) -> Vec<usize> {
    let Some((command, args)) = parts.split_first() else {
        return Vec::new();
    };
    if ScnpScanCommand::from_name(command) == Some(ScnpScanCommand::ScanShard) {
        return scnp_scan_shard(store, args);
    }
    route_keys_to_shards(store, command_route_keys(command, args))
}

fn fast_request_shards(store: &EmbeddedStore, request: &FastRequest<'_>) -> Vec<usize> {
    match &request.command {
        FastCommand::RespCommand { parts } => command_shards(store, parts),
        command => route_keys_to_shards(store, command.route_keys()),
    }
}

fn route_keys_to_shards(store: &EmbeddedStore, route_keys: FastRedisRouteKeys<'_>) -> Vec<usize> {
    let mut shards = BTreeSet::new();
    match route_keys {
        FastRedisRouteKeys::None => {}
        FastRedisRouteKeys::AllShards => shards.extend(0..store.shard_count()),
        FastRedisRouteKeys::Keys(keys) => {
            for key in keys {
                shards.insert(store.route_key(key).shard_id);
            }
        }
    }
    shards.into_iter().collect()
}

fn command_route_keys<'a>(command: &[u8], args: &'a [&'a [u8]]) -> FastRedisRouteKeys<'a> {
    if ScnpScanCommand::from_name(command) == Some(ScnpScanCommand::Scan) {
        return FastRedisRouteKeys::AllShards;
    }
    if let Some(kind) = FastCommandKind::from_redis_name(command) {
        return kind.redis_route_keys(args);
    }
    supplemental_command_key_spec(command)
        .map(|spec| spec.route_keys(args))
        .unwrap_or_else(|| first_n_route_keys(args, 1))
}

fn scnp_scan_shard(store: &EmbeddedStore, args: &[&[u8]]) -> Vec<usize> {
    match args
        .first()
        .and_then(|raw| parse_ascii_usize(raw))
        .filter(|shard_id| *shard_id < store.shard_count())
    {
        Some(shard_id) => vec![shard_id],
        None => (0..store.shard_count()).collect(),
    }
}

#[derive(Clone, Copy)]
enum SupplementalCommandKeySpec {
    None,
    AllShards,
    AllArgs,
    At(usize),
    Counted { numkeys_index: usize },
    StreamRead,
    Sort,
}

impl SupplementalCommandKeySpec {
    fn route_keys<'a>(self, args: &'a [&'a [u8]]) -> FastRedisRouteKeys<'a> {
        match self {
            Self::None => FastRedisRouteKeys::None,
            Self::AllShards => FastRedisRouteKeys::AllShards,
            Self::AllArgs => all_route_keys(args),
            Self::At(index) => args
                .get(index)
                .copied()
                .map(|key| FastRedisRouteKeys::Keys(vec![key]))
                .unwrap_or(FastRedisRouteKeys::None),
            Self::Counted { numkeys_index } => counted_route_keys(args, numkeys_index),
            Self::StreamRead => stream_read_route_keys(args),
            Self::Sort => sort_route_keys(args),
        }
    }
}

struct SupplementalCommandKeyEntry {
    names: &'static [&'static [u8]],
    spec: SupplementalCommandKeySpec,
}

const SUPPLEMENTAL_COMMAND_KEY_SPECS: &[SupplementalCommandKeyEntry] = &[
    SupplementalCommandKeyEntry {
        names: &[
            b"ASKING",
            b"BGREWRITEAOF",
            b"BGSAVE",
            b"CLUSTER",
            b"DEBUG",
            b"HOST:",
            b"LASTSAVE",
            b"LATENCY",
            b"LOLWUT",
            b"MODULE",
            b"MONITOR",
            b"POST",
            b"PSUBSCRIBE",
            b"PSYNC",
            b"PUBSUB",
            b"PUNSUBSCRIBE",
            b"READONLY",
            b"READWRITE",
            b"REPLCONF",
            b"REPLICAOF",
            b"ROLE",
            b"SAVE",
            b"SCRIPT",
            b"SHUTDOWN",
            b"SLAVEOF",
            b"SLOWLOG",
            b"SUBSCRIBE",
            b"SYNC",
            b"UNSUBSCRIBE",
            b"WAIT",
        ],
        spec: SupplementalCommandKeySpec::None,
    },
    SupplementalCommandKeyEntry {
        names: &[b"EVAL", b"EVALSHA"],
        spec: SupplementalCommandKeySpec::Counted { numkeys_index: 1 },
    },
    SupplementalCommandKeyEntry {
        names: &[b"MIGRATE", b"SWAPDB"],
        spec: SupplementalCommandKeySpec::AllShards,
    },
    SupplementalCommandKeyEntry {
        names: &[b"PFCOUNT", b"PFMERGE"],
        spec: SupplementalCommandKeySpec::AllArgs,
    },
    SupplementalCommandKeyEntry {
        names: &[b"PFDEBUG", b"XGROUP", b"XINFO"],
        spec: SupplementalCommandKeySpec::At(1),
    },
    SupplementalCommandKeyEntry {
        names: &[b"PFSELFTEST", b"PUBLISH"],
        spec: SupplementalCommandKeySpec::None,
    },
    SupplementalCommandKeyEntry {
        names: &[b"XREAD", b"XREADGROUP"],
        spec: SupplementalCommandKeySpec::StreamRead,
    },
    SupplementalCommandKeyEntry {
        names: &[b"SORT"],
        spec: SupplementalCommandKeySpec::Sort,
    },
];

fn supplemental_command_key_spec(command: &[u8]) -> Option<SupplementalCommandKeySpec> {
    SUPPLEMENTAL_COMMAND_KEY_SPECS
        .iter()
        .find(|entry| {
            entry
                .names
                .iter()
                .any(|name| command.eq_ignore_ascii_case(name))
        })
        .map(|entry| entry.spec)
}

fn all_route_keys<'a>(args: &'a [&'a [u8]]) -> FastRedisRouteKeys<'a> {
    FastRedisRouteKeys::Keys(args.to_vec())
}

fn first_n_route_keys<'a>(args: &'a [&'a [u8]], count: usize) -> FastRedisRouteKeys<'a> {
    FastRedisRouteKeys::Keys(args.iter().take(count).copied().collect())
}

fn counted_route_keys<'a>(args: &'a [&'a [u8]], numkeys_index: usize) -> FastRedisRouteKeys<'a> {
    match counted_key_span(args, numkeys_index) {
        Some(keys) => FastRedisRouteKeys::Keys(keys.to_vec()),
        None => FastRedisRouteKeys::AllShards,
    }
}

fn counted_key_span<'a>(args: &'a [&'a [u8]], numkeys_index: usize) -> Option<&'a [&'a [u8]]> {
    let numkeys = args
        .get(numkeys_index)
        .and_then(|raw| parse_ascii_usize(raw))?;
    let key_start = numkeys_index.checked_add(1)?;
    let key_end = key_start.checked_add(numkeys)?;
    match numkeys {
        0 => None,
        _ => args.get(key_start..key_end),
    }
}

fn parse_ascii_usize(raw: &[u8]) -> Option<usize> {
    std::str::from_utf8(raw).ok()?.parse().ok()
}

fn stream_read_route_keys<'a>(args: &'a [&'a [u8]]) -> FastRedisRouteKeys<'a> {
    let Some(streams_index) = args
        .iter()
        .position(|arg| arg.eq_ignore_ascii_case(b"STREAMS"))
    else {
        return FastRedisRouteKeys::AllShards;
    };
    let stream_args = &args[streams_index + 1..];
    if stream_args.len() < 2 || !stream_args.len().is_multiple_of(2) {
        return FastRedisRouteKeys::AllShards;
    }
    let key_count = stream_args.len() / 2;
    FastRedisRouteKeys::Keys(stream_args.iter().take(key_count).copied().collect())
}

fn sort_route_keys<'a>(args: &'a [&'a [u8]]) -> FastRedisRouteKeys<'a> {
    let Some(source) = args.first().copied() else {
        return FastRedisRouteKeys::None;
    };
    let mut keys = vec![source];
    let mut index = 1;
    while index + 1 < args.len() {
        if args[index].eq_ignore_ascii_case(b"STORE") {
            keys.push(args[index + 1]);
            break;
        }
        index += 1;
    }
    FastRedisRouteKeys::Keys(keys)
}

fn write_simple_string(out: &mut BytesMut, value: &str) {
    out.extend_from_slice(b"+");
    out.extend_from_slice(value.as_bytes());
    out.extend_from_slice(b"\r\n");
}

#[cfg(feature = "redis")]
fn write_null_array(out: &mut BytesMut) {
    out.extend_from_slice(b"*-1\r\n");
}

fn write_wrong_arity(out: &mut BytesMut, command: &str) {
    ServerWire::write_resp_error(
        out,
        &format!("ERR wrong number of arguments for '{}' command", command),
    );
}
