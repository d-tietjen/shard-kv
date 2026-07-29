#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize};
use std::sync::{Condvar, Mutex};

use hashbrown::HashTable;
use indextreemap::IndexTreeSet;
use parking_lot::RwLock;
use smallvec::SmallVec;

use crate::storage::{Bytes, FastHashMap, FastHashSet, hash_key, local_table_hash};

const OBJECT_BUCKETS: usize = 256;
const SMALL_HASH_INLINE: usize = 4;
const SMALL_LIST_INLINE: usize = 8;
const LIST_CHUNK_CAPACITY: usize = 32;
const SMALL_SET_INLINE: usize = 4;
const SMALL_ZSET_INLINE: usize = 4;

pub(crate) const WRONGTYPE_MESSAGE: &str =
    "WRONGTYPE Operation against a key holding the wrong kind of value";

type SlotId = usize;
type SmallHashEntries = SmallVec<[(Bytes, Bytes); SMALL_HASH_INLINE]>;
type SmallSetEntries = SmallVec<[Bytes; SMALL_SET_INLINE]>;
type SmallZSetEntries = SmallVec<[(Bytes, f64); SMALL_ZSET_INLINE]>;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisObjectResult {
    Simple(&'static str),
    Integer(i64),
    IntegerArray(Vec<i64>),
    Bulk(Option<Bytes>),
    Array(Vec<Option<Bytes>>),
    WrongType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisObjectError {
    WrongType,
    MissingKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisKeyError {
    WrongType,
    MissingKey,
    ReplicationLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisStringLookup {
    Hit,
    Miss,
    WrongType,
    BackendError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RedisObjectReadOutcome {
    Written,
    Missing,
    WrongType,
}

pub(crate) enum RedisObjectWriteAttempt {
    Complete(RedisObjectResult),
    Missing,
}

pub(crate) enum RedisObjectArrayItem<'a> {
    Begin(usize),
    Bulk(Option<&'a [u8]>),
}

pub(crate) enum RedisObjectZSetRangeItem<'a> {
    Begin(usize),
    Entry { member: &'a [u8], score: f64 },
}

#[derive(Debug)]
struct SlotEntry {
    hash: u64,
    key: Bytes,
    slot: SlotId,
}

#[derive(Debug, Default)]
struct SlotMap {
    table: HashTable<SlotEntry>,
}

#[derive(Debug)]
pub(crate) struct RedisObjectStore {
    shards: Vec<RedisObjectShard>,
    has_objects_hint: AtomicBool,
}

#[derive(Debug)]
pub(crate) struct RedisObjectShard {
    buckets: Vec<RwLock<RedisObjectBucket>>,
    object_count: AtomicIsize,
    wait_generation: Mutex<u64>,
    waiter_count: AtomicUsize,
    wait_condvar: Condvar,
}

#[derive(Debug, Default)]
pub(crate) struct RedisObjectBucket {
    hashes: SlotMap,
    lists: SlotMap,
    sets: SlotMap,
    zsets: SlotMap,
    expire_at_ms: FastHashMap<Bytes, u64>,
    /// Per-field hash TTLs (Redis 7.2+): hash key -> field -> absolute unix ms.
    /// Empty until the first HEXPIRE-family call, so TTL-free hashes cost
    /// nothing. A field is logically gone once `now_ms >= its value`; reads
    /// filter lazily and writes purge opportunistically.
    hash_field_expire_at_ms: FastHashMap<Bytes, FastHashMap<Bytes, u64>>,
    hash_slab: ObjectSlab<HashObject>,
    list_slab: ObjectSlab<ListObject>,
    set_slab: ObjectSlab<SetObject>,
    zset_slab: ObjectSlab<ZSetObject>,
}

#[derive(Debug)]
struct ObjectSlab<T> {
    entries: Vec<Option<T>>,
    free: Vec<usize>,
}

impl<T> Default for ObjectSlab<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
enum HashObject {
    Small(SmallHashEntries),
    Map(FastHashMap<Bytes, Bytes>),
}

#[derive(Debug, Clone)]
enum ListObject {
    Empty,
    Single(Bytes),
    Small(SmallListDeque),
    Segmented(SegmentedList),
}

#[derive(Debug, Clone)]
struct SmallListDeque {
    entries: [Option<Bytes>; SMALL_LIST_INLINE],
    head: usize,
    len: usize,
}

struct SmallListIter<'a> {
    list: &'a SmallListDeque,
    index: usize,
}

#[derive(Debug, Clone)]
struct ListChunk {
    entries: [Option<Bytes>; LIST_CHUNK_CAPACITY],
    head: usize,
    len: usize,
}

struct ListChunkIter<'a> {
    chunk: &'a ListChunk,
    index: usize,
}

#[derive(Debug, Clone)]
struct SegmentedList {
    chunks: VecDeque<ListChunk>,
    len: usize,
}

struct SegmentedListIter<'a> {
    chunks: std::collections::vec_deque::Iter<'a, ListChunk>,
    current: Option<ListChunkIter<'a>>,
}

#[derive(Debug, Clone)]
enum SetObject {
    Small(SmallSetEntries),
    Map(FastHashSet<Bytes>),
}

#[derive(Debug, Clone)]
enum ZSetObject {
    Small(SmallZSetEntries),
    Map {
        scores: FastHashMap<Bytes, f64>,
        ordered: IndexTreeSet<ZSetOrderKey>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RedisObjectValue {
    Hash(Vec<(Bytes, Bytes)>),
    List(Vec<Bytes>),
    Set(Vec<Bytes>),
    ZSet(Vec<(Bytes, f64)>),
}

#[derive(Debug, Clone)]
struct ZSetOrderKey {
    score: f64,
    member: Bytes,
}

impl Eq for ZSetOrderKey {}

impl PartialEq for ZSetOrderKey {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score).is_eq() && self.member == other.member
    }
}

impl PartialOrd for ZSetOrderKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ZSetOrderKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.member.cmp(&other.member))
    }
}
#[path = "redis_objects/bucket_core.rs"]
mod bucket_core;
#[path = "redis_objects/bucket_hash.rs"]
mod bucket_hash;
#[path = "redis_objects/bucket_hash_ttl.rs"]
mod bucket_hash_ttl;
pub(crate) use bucket_hash_ttl::{
    HashFieldExpireCond, HashFieldGetExpireAction, HashFieldSetCondition, HashFieldSetExpireAction,
};
#[path = "redis_objects/bucket_list.rs"]
mod bucket_list;
#[path = "redis_objects/bucket_set.rs"]
mod bucket_set;
#[path = "redis_objects/bucket_zset.rs"]
mod bucket_zset;
#[path = "redis_objects/hash_object.rs"]
mod hash_object;
#[path = "redis_objects/helpers.rs"]
mod helpers;
#[path = "redis_objects/list_chunk.rs"]
mod list_chunk;
#[path = "redis_objects/list_deque.rs"]
mod list_deque;
#[path = "redis_objects/list_object.rs"]
mod list_object;
#[path = "redis_objects/segmented_list.rs"]
mod segmented_list;
#[path = "redis_objects/set_object.rs"]
mod set_object;
#[path = "redis_objects/slab.rs"]
mod slab;
#[path = "redis_objects/slot_map.rs"]
mod slot_map;
#[path = "redis_objects/store.rs"]
mod store;
#[path = "redis_objects/zset_object.rs"]
mod zset_object;
