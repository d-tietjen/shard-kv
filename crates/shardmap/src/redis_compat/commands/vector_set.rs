use std::cmp::Ordering;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

use crate::commands::redis::{
    array_bulk, bulk, eq_ignore_ascii_case, error, int, parse_f64, parse_i64, parse_usize,
    wrong_arity, wrongtype,
};
#[cfg(feature = "server")]
use crate::commands::redis::{
    write_fast_frame, write_frame, write_resp_array_header, write_resp_null,
    write_resp_wrong_arity, write_resp_wrongtype,
};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{
    Bytes, EmbeddedStore, RedisStringStore, VECTOR_SET_PREFIX, hash_key, shift_for, stripe_index,
};
#[cfg(feature = "server")]
use bytes::BytesMut;

const HNSW_VECTOR_SET_FORMAT: u32 = 0x484e_5357; // HNSW
const HNSW_VECTOR_SET_FORMAT_LEGACY_TYPO: u32 = 0x4853_4e57; // HSNW
const HNSW_GOVERNED_VECTOR_SET_FORMAT: u32 = 0x484e_5347; // HNSG
const DEFAULT_HNSW_M: usize = 16;
const DEFAULT_HNSW_EF_CONSTRUCTION: usize = 64;
const DEFAULT_HNSW_EF_SEARCH: usize = 64;
const MAX_HNSW_LEVEL: usize = 16;
const VECTOR_SCAN_PARALLEL_MIN: usize = 4096;
const VECTOR_DECODE_CACHE_MAX_ENTRIES: usize = 64;
const VECTOR_DECODE_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
const VECTOR_DECODE_CACHE_MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
const VECTOR_LEX_RANGE_CACHE_MAX_ENTRIES: usize = 128;
const VECTOR_LEX_RANGE_CACHE_MAX_BYTES: usize = 16 * 1024 * 1024;
const VECTOR_LOOKUP_CACHE_MAX_ENTRIES: usize = 256;
const VECTOR_LOOKUP_CACHE_MAX_BYTES: usize = 16 * 1024 * 1024;
const VECTOR_ATTRIBUTE_VALIDATION_CACHE_MAX_ENTRIES: usize = 128;
const VECTOR_ATTRIBUTE_VALIDATION_CACHE_MAX_VALUE_BYTES: usize = 64 * 1024;
const MAX_VECTOR_GOVERNANCE_BYTES: usize = 64 * 1024;

macro_rules! define_vector_command {
    ($type:ident, $static_name:ident, $name:literal, $mutates:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static $static_name: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;
        }
    };
}

#[cfg(feature = "server")]
macro_rules! vector_write_fast_from_resp {
    () => {
        #[inline(always)]
        fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
            write_vector_fast_value(store, args, out, Self::write_resp);
        }
    };
}

define_vector_command!(VAdd, VADD_COMMAND, "VADD", true);
define_vector_command!(VCard, VCARD_COMMAND, "VCARD", false);
define_vector_command!(VDim, VDIM_COMMAND, "VDIM", false);
define_vector_command!(VEmb, VEMB_COMMAND, "VEMB", false);
define_vector_command!(VGetAttr, VGETATTR_COMMAND, "VGETATTR", false);
define_vector_command!(VInfo, VINFO_COMMAND, "VINFO", false);
define_vector_command!(VIsMember, VISMEMBER_COMMAND, "VISMEMBER", false);
define_vector_command!(VLinks, VLINKS_COMMAND, "VLINKS", false);
define_vector_command!(VRandMember, VRANDMEMBER_COMMAND, "VRANDMEMBER", false);
define_vector_command!(VRange, VRANGE_COMMAND, "VRANGE", false);
define_vector_command!(VRem, VREM_COMMAND, "VREM", true);
define_vector_command!(VSetAttr, VSETATTR_COMMAND, "VSETATTR", true);
define_vector_command!(VSim, VSIM_COMMAND, "VSIM", false);

#[inline(always)]
pub(crate) fn is_vector_command_name(name: &[u8]) -> bool {
    const NAMES: &[&[u8]] = &[
        b"VADD",
        b"VCARD",
        b"VDIM",
        b"VEMB",
        b"VGETATTR",
        b"VINFO",
        b"VISMEMBER",
        b"VLINKS",
        b"VRANDMEMBER",
        b"VRANGE",
        b"VREM",
        b"VSETATTR",
        b"VSIM",
    ];
    NAMES
        .iter()
        .any(|candidate| name.eq_ignore_ascii_case(candidate))
}

#[derive(Debug, Clone)]
struct VectorSetState {
    dim: usize,
    original_dim: usize,
    quantization: Quantization,
    hnsw_m: usize,
    ef_construction: usize,
    max_level: usize,
    next_uid: u64,
    entries: Vec<VectorEntry>,
}

impl Default for VectorSetState {
    fn default() -> Self {
        Self {
            dim: 0,
            original_dim: 0,
            quantization: Quantization::default(),
            hnsw_m: DEFAULT_HNSW_M,
            ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
            max_level: 0,
            next_uid: 1,
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct VectorEntry {
    uid: u64,
    level: usize,
    element: Bytes,
    vector: Vec<f64>,
    attributes: Option<Bytes>,
    governance: Option<Bytes>,
    links: Vec<Vec<u64>>,
}

#[derive(Debug, Clone, Copy)]
struct VectorSetMetadata {
    dim: usize,
    original_dim: usize,
    quantization: Quantization,
    hnsw_m: usize,
    ef_construction: usize,
    max_level: usize,
    next_uid: u64,
    count: usize,
}

#[derive(Debug, Clone)]
enum VectorEntryLookup {
    MissingKey,
    MissingElement,
    Found(VectorEntrySnapshot),
}

#[derive(Debug, Clone)]
struct VectorEntrySnapshot {
    vector: Option<Vec<f64>>,
    attributes: Option<Bytes>,
    governance: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorDecodeMode {
    Full,
    EntriesOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorLookupProjection {
    Exists,
    Attributes,
    Vector,
    VectorAttributesAndGovernance,
}

impl VectorLookupProjection {
    #[inline(always)]
    fn include_vector(self) -> bool {
        matches!(self, Self::Vector | Self::VectorAttributesAndGovernance)
    }

    #[inline(always)]
    fn include_attributes(self) -> bool {
        matches!(self, Self::Attributes | Self::VectorAttributesAndGovernance)
    }

    #[inline(always)]
    fn include_governance(self) -> bool {
        matches!(self, Self::VectorAttributesAndGovernance)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VectorDecodeCacheKey {
    mode: VectorDecodeMode,
    ptr: usize,
    len: usize,
}

struct VectorDecodeCacheEntry {
    key: VectorDecodeCacheKey,
    raw_len: usize,
    _raw: bytes::Bytes,
    set: Arc<VectorSetState>,
}

#[derive(Default)]
struct VectorDecodeCache {
    entries: Vec<VectorDecodeCacheEntry>,
    raw_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VectorLexRangeCacheKey {
    value_ptr: usize,
    value_len: usize,
    start_len: usize,
    start_hash: u64,
    start_head: u64,
    start_tail: u64,
    end_len: usize,
    end_hash: u64,
    end_head: u64,
    end_tail: u64,
    limit: usize,
}

struct VectorLexRangeCacheEntry {
    key: VectorLexRangeCacheKey,
    bytes: usize,
    _raw: bytes::Bytes,
    elements: Arc<Vec<Bytes>>,
}

#[derive(Default)]
struct VectorLexRangeCache {
    entries: Vec<VectorLexRangeCacheEntry>,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VectorLookupCacheKey {
    value_ptr: usize,
    value_len: usize,
    element_len: usize,
    element_hash: u64,
    element_head: u64,
    element_tail: u64,
    projection: VectorLookupProjection,
}

struct VectorLookupCacheEntry {
    key: VectorLookupCacheKey,
    bytes: usize,
    _raw: bytes::Bytes,
    lookup: Arc<VectorEntryLookup>,
}

#[derive(Default)]
struct VectorLookupCache {
    entries: Vec<VectorLookupCacheEntry>,
    bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VectorAttributeValidationCacheKey {
    ptr: usize,
    len: usize,
}

struct VectorAttributeValidationCacheEntry {
    key: VectorAttributeValidationCacheKey,
    raw: Bytes,
    valid: bool,
}

#[derive(Default)]
struct VectorAttributeValidationCache {
    entries: Vec<VectorAttributeValidationCacheEntry>,
}

#[derive(Debug)]
enum VectorWriteResult {
    Changed(Frame),
    Unchanged(Frame),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Quantization {
    NoQuant,
    #[default]
    Q8,
    Bin,
}

impl VectorDecodeCache {
    fn get(&self, key: VectorDecodeCacheKey) -> Option<Arc<VectorSetState>> {
        self.entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| Arc::clone(&entry.set))
    }

    fn insert(&mut self, key: VectorDecodeCacheKey, raw: bytes::Bytes, set: Arc<VectorSetState>) {
        let raw_len = raw.len();
        self.raw_bytes = self.raw_bytes.saturating_add(raw_len);
        self.entries.push(VectorDecodeCacheEntry {
            key,
            raw_len,
            _raw: raw,
            set,
        });
        while self.entries.len() > VECTOR_DECODE_CACHE_MAX_ENTRIES
            || self.raw_bytes > VECTOR_DECODE_CACHE_MAX_BYTES
        {
            if self.entries.is_empty() {
                self.raw_bytes = 0;
                break;
            }
            let removed = self.entries.remove(0);
            self.raw_bytes = self.raw_bytes.saturating_sub(removed.raw_len);
        }
    }
}

impl VectorLexRangeCache {
    fn get(&self, key: VectorLexRangeCacheKey) -> Option<Arc<Vec<Bytes>>> {
        self.entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| Arc::clone(&entry.elements))
    }

    fn insert(
        &mut self,
        key: VectorLexRangeCacheKey,
        raw: bytes::Bytes,
        elements: Arc<Vec<Bytes>>,
    ) {
        let bytes = elements
            .iter()
            .map(Vec::len)
            .sum::<usize>()
            .saturating_add(elements.len().saturating_mul(std::mem::size_of::<Bytes>()))
            .saturating_add(raw.len());
        if bytes > VECTOR_LEX_RANGE_CACHE_MAX_BYTES {
            return;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.push(VectorLexRangeCacheEntry {
            key,
            bytes,
            _raw: raw,
            elements,
        });
        while self.entries.len() > VECTOR_LEX_RANGE_CACHE_MAX_ENTRIES
            || self.bytes > VECTOR_LEX_RANGE_CACHE_MAX_BYTES
        {
            if self.entries.is_empty() {
                self.bytes = 0;
                break;
            }
            let removed = self.entries.remove(0);
            self.bytes = self.bytes.saturating_sub(removed.bytes);
        }
    }
}

impl VectorLookupCache {
    fn get(&self, key: VectorLookupCacheKey) -> Option<Arc<VectorEntryLookup>> {
        self.entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| Arc::clone(&entry.lookup))
    }

    fn insert(
        &mut self,
        key: VectorLookupCacheKey,
        raw: bytes::Bytes,
        lookup: Arc<VectorEntryLookup>,
    ) {
        let bytes = vector_lookup_cache_bytes(&lookup).saturating_add(raw.len());
        if bytes > VECTOR_LOOKUP_CACHE_MAX_BYTES {
            return;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.push(VectorLookupCacheEntry {
            key,
            bytes,
            _raw: raw,
            lookup,
        });
        while self.entries.len() > VECTOR_LOOKUP_CACHE_MAX_ENTRIES
            || self.bytes > VECTOR_LOOKUP_CACHE_MAX_BYTES
        {
            if self.entries.is_empty() {
                self.bytes = 0;
                break;
            }
            let removed = self.entries.remove(0);
            self.bytes = self.bytes.saturating_sub(removed.bytes);
        }
    }
}

impl VectorAttributeValidationCache {
    fn get(&self, key: VectorAttributeValidationCacheKey, raw: &[u8]) -> Option<bool> {
        self.entries
            .iter()
            .find(|entry| entry.key == key && entry.raw.as_slice() == raw)
            .map(|entry| entry.valid)
    }

    fn insert(&mut self, key: VectorAttributeValidationCacheKey, raw: &[u8], valid: bool) {
        if raw.len() > VECTOR_ATTRIBUTE_VALIDATION_CACHE_MAX_VALUE_BYTES {
            return;
        }
        self.entries.push(VectorAttributeValidationCacheEntry {
            key,
            raw: raw.to_vec(),
            valid,
        });
        while self.entries.len() > VECTOR_ATTRIBUTE_VALIDATION_CACHE_MAX_ENTRIES {
            self.entries.remove(0);
        }
    }
}

fn vector_decode_cache() -> &'static Mutex<VectorDecodeCache> {
    static CACHE: OnceLock<Mutex<VectorDecodeCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(VectorDecodeCache::default()))
}

fn vector_lex_range_cache() -> &'static Mutex<VectorLexRangeCache> {
    static CACHE: OnceLock<Mutex<VectorLexRangeCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(VectorLexRangeCache::default()))
}

fn vector_lookup_cache() -> &'static Mutex<VectorLookupCache> {
    static CACHE: OnceLock<Mutex<VectorLookupCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(VectorLookupCache::default()))
}

fn vector_attribute_validation_cache() -> &'static Mutex<VectorAttributeValidationCache> {
    static CACHE: OnceLock<Mutex<VectorAttributeValidationCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(VectorAttributeValidationCache::default()))
}

impl crate::commands::redis::RedisCommand for VAdd {
    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_fast_frame(out, &Self::execute(store, args));
    }

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, rest @ ..] = args else {
            return wrong_arity("VADD");
        };
        let mut parsed = match parse_vadd_args(rest) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        match vector_read_metadata(store, key) {
            Ok(Some(metadata)) => {
                let original_dim = parsed.vector.len();
                match (metadata.dim, metadata.original_dim, parsed.reduce_dim) {
                    (_, _, Some(reduce_dim)) => {
                        parsed.vector = reduce_vector(&parsed.vector, reduce_dim);
                    }
                    (dim, original, None)
                        if original != 0 && original_dim == original && dim != original =>
                    {
                        parsed.vector = reduce_vector(&parsed.vector, dim);
                    }
                    _ => {}
                }
                if metadata.dim != parsed.vector.len() {
                    return error("ERR vector dimension mismatch");
                }
                if parsed
                    .quantization
                    .is_some_and(|quantization| quantization != metadata.quantization)
                {
                    return error("ERR vector set quantization mismatch");
                }
                match vector_lookup_entry(
                    store,
                    key,
                    &parsed.element,
                    VectorLookupProjection::VectorAttributesAndGovernance,
                ) {
                    Ok(VectorEntryLookup::Found(snapshot)) => {
                        let vector_changed =
                            snapshot.vector.as_deref() != Some(parsed.vector.as_slice());
                        let attributes_changed =
                            parsed.attributes.as_deref().is_some_and(|attributes| {
                                Some(attributes) != snapshot.attributes.as_deref()
                            });
                        let governance_changed =
                            parsed.governance.as_deref().is_some_and(|governance| {
                                Some(governance) != snapshot.governance.as_deref()
                            });
                        if !vector_changed && !attributes_changed && !governance_changed {
                            return int(0);
                        }
                    }
                    Ok(VectorEntryLookup::MissingElement | VectorEntryLookup::MissingKey) => {}
                    Err(frame) => return frame,
                }
            }
            Ok(None) => {}
            Err(frame) => return frame,
        }
        vector_write_maybe(store, key, |set| {
            let original_dim = parsed.vector.len();
            match (set.dim, set.original_dim, parsed.reduce_dim) {
                (_, _, Some(reduce_dim)) => {
                    parsed.vector = reduce_vector(&parsed.vector, reduce_dim);
                }
                (dim, original, None)
                    if dim != 0 && original != 0 && original_dim == original && dim != original =>
                {
                    parsed.vector = reduce_vector(&parsed.vector, dim);
                }
                _ => {}
            }
            if set.dim == 0 {
                set.dim = parsed.vector.len();
                set.original_dim = original_dim;
                set.hnsw_m = parsed.hnsw_m.unwrap_or(DEFAULT_HNSW_M);
                set.ef_construction = parsed
                    .ef_construction
                    .unwrap_or(DEFAULT_HNSW_EF_CONSTRUCTION);
            } else if set.dim != parsed.vector.len() {
                return VectorWriteResult::Unchanged(error("ERR vector dimension mismatch"));
            }
            if set.entries.is_empty() {
                set.quantization = parsed.quantization.unwrap_or_default();
            } else if parsed
                .quantization
                .is_some_and(|quantization| quantization != set.quantization)
            {
                return VectorWriteResult::Unchanged(error("ERR vector set quantization mismatch"));
            }
            match set
                .entries
                .iter_mut()
                .find(|entry| entry.element == parsed.element)
            {
                Some(entry) => {
                    let vector_changed = entry.vector != parsed.vector;
                    let attributes_changed = parsed
                        .attributes
                        .as_deref()
                        .is_some_and(|attributes| Some(attributes) != entry.attributes.as_deref());
                    let governance_changed = parsed
                        .governance
                        .as_deref()
                        .is_some_and(|governance| Some(governance) != entry.governance.as_deref());
                    if !vector_changed && !attributes_changed && !governance_changed {
                        return VectorWriteResult::Unchanged(int(0));
                    }
                    if vector_changed {
                        entry.vector = parsed.vector;
                    }
                    if attributes_changed {
                        entry.attributes = parsed.attributes;
                    }
                    if governance_changed {
                        entry.governance = parsed.governance;
                    }
                    if vector_changed {
                        set.rebuild_hnsw();
                    }
                    VectorWriteResult::Changed(int(0))
                }
                None => {
                    let uid = set.next_uid;
                    set.next_uid = set.next_uid.saturating_add(1).max(1);
                    set.insert_hnsw_entry(VectorEntry {
                        uid,
                        level: hnsw_level(&parsed.element),
                        element: parsed.element,
                        vector: parsed.vector,
                        attributes: parsed.attributes,
                        governance: parsed.governance,
                        links: Vec::new(),
                    });
                    VectorWriteResult::Changed(int(1))
                }
            }
        })
    }
}

impl crate::commands::redis::RedisCommand for VCard {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key] = args else {
            return wrong_arity("VCARD");
        };
        match vector_read_metadata(store, key) {
            Ok(Some(metadata)) => int(metadata.count as i64),
            Ok(None) => int(0),
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [key] = args else {
            write_resp_wrong_arity(out, "VCARD");
            return;
        };
        write_vector_metadata_integer_resp(store, key, out, |metadata| metadata.count as i64, 0);
    }
}

impl crate::commands::redis::RedisCommand for VDim {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key] = args else {
            return wrong_arity("VDIM");
        };
        match vector_read_metadata(store, key) {
            Ok(Some(metadata)) => int(metadata.dim as i64),
            Ok(None) => error("ERR no such key"),
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [key] = args else {
            write_resp_wrong_arity(out, "VDIM");
            return;
        };
        match vector_read_metadata(store, key) {
            Ok(Some(metadata)) => ServerWire::write_resp_integer(out, metadata.dim as i64),
            Ok(None) => write_frame(out, &error("ERR no such key")),
            Err(_) => write_resp_wrongtype(out),
        }
    }
}

impl crate::commands::redis::RedisCommand for VEmb {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element, options @ ..] = args else {
            return wrong_arity("VEMB");
        };
        let raw = match options {
            [] => false,
            [option] if eq_ignore_ascii_case(option, b"RAW") => true,
            _ => return error("ERR syntax error"),
        };
        let metadata = match vector_read_metadata(store, key) {
            Ok(Some(metadata)) => metadata,
            Ok(None) => return Frame::Null,
            Err(frame) => return frame,
        };
        match vector_lookup_entry(store, key, element, VectorLookupProjection::Vector) {
            Ok(VectorEntryLookup::Found(snapshot)) => match snapshot.vector {
                Some(vector) if raw => raw_vector_values_frame(&vector, metadata.quantization),
                Some(vector) => array_bulk(
                    vector
                        .iter()
                        .map(|value| format_number(*value).into_bytes())
                        .collect(),
                ),
                None => Frame::Null,
            },
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => Frame::Null,
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for VGetAttr {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element] = args else {
            return wrong_arity("VGETATTR");
        };
        match vector_lookup_entry(store, key, element, VectorLookupProjection::Attributes) {
            Ok(VectorEntryLookup::Found(snapshot)) => snapshot.attributes.map_or(Frame::Null, bulk),
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => Frame::Null,
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [key, element] = args else {
            write_resp_wrong_arity(out, "VGETATTR");
            return;
        };
        write_vector_attribute_resp(store, key, element, out);
    }
}

impl crate::commands::redis::RedisCommand for VSetAttr {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element, attributes] = args else {
            return wrong_arity("VSETATTR");
        };
        let next_attributes = (!attributes.is_empty()).then(|| (*attributes).to_vec());
        match vector_lookup_entry(store, key, element, VectorLookupProjection::Attributes) {
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => {
                if let Err(frame) = validate_attributes(attributes) {
                    return frame;
                }
                return int(0);
            }
            Ok(VectorEntryLookup::Found(snapshot)) if snapshot.attributes == next_attributes => {
                return int(1);
            }
            Ok(VectorEntryLookup::Found(_)) => {}
            Err(frame) => return frame,
        }
        if let Err(frame) = validate_attributes(attributes) {
            return frame;
        }
        vector_write_existing_maybe(store, key, |set| {
            let Some(entry) = set.entry_mut(element) else {
                return VectorWriteResult::Unchanged(int(0));
            };
            let next_attributes = (!attributes.is_empty()).then(|| (*attributes).to_vec());
            if entry.attributes == next_attributes {
                return VectorWriteResult::Unchanged(int(1));
            }
            entry.attributes = next_attributes;
            VectorWriteResult::Changed(int(1))
        })
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [key, element, attributes] = args else {
            write_resp_wrong_arity(out, "VSETATTR");
            return;
        };
        let next_attributes = (!attributes.is_empty()).then_some(*attributes);
        match vector_lookup_entry(store, key, element, VectorLookupProjection::Attributes) {
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => {
                if let Err(frame) = validate_attributes(attributes) {
                    write_frame(out, &frame);
                    return;
                }
                ServerWire::write_resp_integer(out, 0);
            }
            Ok(VectorEntryLookup::Found(snapshot))
                if snapshot.attributes.as_deref() == next_attributes =>
            {
                ServerWire::write_resp_integer(out, 1);
            }
            Ok(VectorEntryLookup::Found(_)) => {
                if let Err(frame) = validate_attributes(attributes) {
                    write_frame(out, &frame);
                    return;
                }
                write_frame(out, &vector_setattr_update(store, key, element, attributes));
            }
            Err(frame) => write_frame(out, &frame),
        }
    }
}

impl crate::commands::redis::RedisCommand for VIsMember {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element] = args else {
            return wrong_arity("VISMEMBER");
        };
        match vector_lookup_entry(store, key, element, VectorLookupProjection::Exists) {
            Ok(VectorEntryLookup::Found(_)) => int(1),
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => int(0),
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [key, element] = args else {
            write_resp_wrong_arity(out, "VISMEMBER");
            return;
        };
        write_vector_exists_resp(store, key, element, out);
    }
}

impl crate::commands::redis::RedisCommand for VRem {
    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_fast_frame(out, &Self::execute(store, args));
    }

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element] = args else {
            return wrong_arity("VREM");
        };
        match vector_lookup_entry(store, key, element, VectorLookupProjection::Exists) {
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => {
                return int(0);
            }
            Ok(VectorEntryLookup::Found(_)) => {}
            Err(frame) => return frame,
        }
        vector_write_existing_maybe(store, key, |set| {
            let before = set.entries.len();
            set.entries
                .retain(|entry| entry.element.as_slice() != *element);
            if before == set.entries.len() {
                return VectorWriteResult::Unchanged(int(0));
            } else {
                set.rebuild_hnsw();
            }
            VectorWriteResult::Changed(int(1))
        })
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [key, element] = args else {
            write_resp_wrong_arity(out, "VREM");
            return;
        };
        match vector_lookup_entry(store, key, element, VectorLookupProjection::Exists) {
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => {
                ServerWire::write_resp_integer(out, 0);
            }
            Ok(VectorEntryLookup::Found(_)) => {
                write_frame(out, &vector_rem_remove(store, key, element));
            }
            Err(frame) => write_frame(out, &frame),
        }
    }
}

impl crate::commands::redis::RedisCommand for VInfo {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key] = args else {
            return wrong_arity("VINFO");
        };
        match vector_read_metadata(store, key) {
            Ok(Some(metadata)) => Frame::Array(vec![
                bulk(b"quant-type".to_vec()),
                bulk(metadata.quantization.vinfo_name().as_bytes().to_vec()),
                bulk(b"vector-dim".to_vec()),
                int(metadata.dim as i64),
                bulk(b"size".to_vec()),
                int(metadata.count as i64),
                bulk(b"max-level".to_vec()),
                int(metadata.max_level as i64),
                bulk(b"hnsw-m".to_vec()),
                int(metadata.hnsw_m as i64),
                bulk(b"hnsw-ef-construction".to_vec()),
                int(metadata.ef_construction as i64),
                bulk(b"vset-uid".to_vec()),
                int(1),
                bulk(b"hnsw-max-node-uid".to_vec()),
                int(metadata.next_uid.saturating_sub(1) as i64),
            ]),
            Ok(None) => Frame::Null,
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [key] = args else {
            write_resp_wrong_arity(out, "VINFO");
            return;
        };
        match vector_read_metadata(store, key) {
            Ok(Some(metadata)) => write_vinfo_resp(out, metadata),
            Ok(None) => write_resp_null(out),
            Err(_) => write_resp_wrongtype(out),
        }
    }
}

impl crate::commands::redis::RedisCommand for VLinks {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element, options @ ..] = args else {
            return wrong_arity("VLINKS");
        };
        let with_scores = match options {
            [] => false,
            [option] if eq_ignore_ascii_case(option, b"WITHSCORES") => true,
            _ => return error("ERR syntax error"),
        };
        match vector_read_cached(store, key, VectorDecodeMode::Full) {
            Ok(Some(set)) => {
                let Some(source) = set.entry(element) else {
                    return Frame::Null;
                };
                let levels = source
                    .links
                    .iter()
                    .map(|links| {
                        let mut level = Vec::new();
                        for uid in links {
                            let Some(neighbor) = set.entry_by_uid(*uid) else {
                                continue;
                            };
                            level.push(bulk(neighbor.element.clone()));
                            if with_scores {
                                level.push(bulk(
                                    format_number(cosine_similarity(
                                        &source.vector,
                                        &neighbor.vector,
                                    ))
                                    .into_bytes(),
                                ));
                            }
                        }
                        Frame::Array(level)
                    })
                    .collect();
                Frame::Array(levels)
            }
            Ok(None) => Frame::Null,
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for VRandMember {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, count @ ..] = args else {
            return wrong_arity("VRANDMEMBER");
        };
        let count = match count {
            [] => None,
            [raw] => match parse_i64(raw) {
                Ok(value) => Some(value),
                Err(()) => return error("ERR value is not an integer or out of range"),
            },
            _ => return wrong_arity("VRANDMEMBER"),
        };
        if count.is_none_or(|count| count >= 0) {
            let limit = count.map_or(1, |count| count as usize);
            return match vector_read_prefix_elements(store, key, limit) {
                Ok(Some(elements)) => match count {
                    None => elements
                        .first()
                        .map(|element| bulk(element.clone()))
                        .unwrap_or(Frame::Null),
                    Some(_) => array_bulk(elements),
                },
                Ok(None) => {
                    if count.is_some() {
                        Frame::Array(Vec::new())
                    } else {
                        Frame::Null
                    }
                }
                Err(frame) => frame,
            };
        }
        match vector_read_elements(store, key) {
            Ok(Some(elements)) => match count {
                None => elements
                    .first()
                    .map(|element| bulk(element.clone()))
                    .unwrap_or(Frame::Null),
                Some(count) => {
                    if elements.is_empty() {
                        return Frame::Array(Vec::new());
                    }
                    let count_abs = count.unsigned_abs() as usize;
                    let values = (0..count_abs)
                        .map(|index| elements[index % elements.len()].clone())
                        .collect();
                    array_bulk(values)
                }
            },
            Ok(None) => {
                if count.is_some() {
                    Frame::Array(Vec::new())
                } else {
                    Frame::Null
                }
            }
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for VRange {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, start, end, count @ ..] = args else {
            return wrong_arity("VRANGE");
        };
        let count = match count {
            [] => None,
            [raw] => match parse_i64(raw) {
                Ok(value) => Some(value),
                Err(()) => return error("ERR value is not an integer or out of range"),
            },
            _ => return wrong_arity("VRANGE"),
        };
        if let Some(count) = count
            && count >= 0
        {
            return match vector_read_lex_range(store, key, start, end, count as usize) {
                Ok(Some(elements)) => array_bulk(elements),
                Ok(None) => Frame::Array(Vec::new()),
                Err(frame) => frame,
            };
        }
        match vector_read_elements(store, key) {
            Ok(Some(mut elements)) => {
                elements.sort();
                elements.retain(|element| lex_in_range(element, start, end));
                if let Some(count) = count
                    && count >= 0
                {
                    elements.truncate(count as usize);
                }
                array_bulk(elements)
            }
            Ok(None) => Frame::Array(Vec::new()),
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let [key, start, end, count @ ..] = args else {
            write_resp_wrong_arity(out, "VRANGE");
            return;
        };
        let count = match count {
            [] => None,
            [raw] => match parse_i64(raw) {
                Ok(value) => Some(value),
                Err(()) => {
                    write_frame(out, &error("ERR value is not an integer or out of range"));
                    return;
                }
            },
            _ => {
                write_resp_wrong_arity(out, "VRANGE");
                return;
            }
        };
        if let Some(count) = count
            && count >= 0
        {
            match vector_read_lex_range(store, key, start, end, count as usize) {
                Ok(Some(elements)) => write_vector_elements_resp(out, &elements),
                Ok(None) => write_resp_array_header(out, 0),
                Err(frame) => write_frame(out, &frame),
            }
            return;
        }
        match vector_read_elements(store, key) {
            Ok(Some(mut elements)) => {
                elements.sort();
                elements.retain(|element| lex_in_range(element, start, end));
                write_vector_elements_resp(out, &elements);
            }
            Ok(None) => write_resp_array_header(out, 0),
            Err(frame) => write_frame(out, &frame),
        }
    }
}

impl crate::commands::redis::RedisCommand for VSim {
    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_vector_array_fast(Self::execute(store, args), out);
    }

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, rest @ ..] = args else {
            return wrong_arity("VSIM");
        };
        let exact_requested = vsim_requires_exact(rest);
        let set = match if exact_requested {
            vector_read_cached(store, key, VectorDecodeMode::EntriesOnly)
        } else {
            vector_read_cached(store, key, VectorDecodeMode::Full)
        } {
            Ok(Some(set)) => set,
            Ok(None) => return Frame::Array(Vec::new()),
            Err(frame) => return frame,
        };
        let mut parsed = match parse_vsim_args(rest, &set) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        if set.original_dim != 0
            && parsed.vector.len() == set.original_dim
            && set.original_dim != set.dim
        {
            parsed.vector = reduce_vector(&parsed.vector, set.dim);
        }
        if parsed.vector.len() != set.dim {
            return error("ERR vector dimension mismatch");
        }
        let scored = if parsed.truth || parsed.filter.is_some() {
            exact_vector_scores(
                &set,
                &parsed.vector,
                parsed.filter.as_deref(),
                store.shard_count(),
            )
        } else {
            hnsw_search(
                &set,
                &parsed.vector,
                parsed.count,
                parsed.ef_search.unwrap_or(DEFAULT_HNSW_EF_SEARCH),
            )
        };
        let mut scored = scored;
        scored.truncate(parsed.count);
        if !parsed.with_scores && !parsed.with_attribs && !parsed.with_governance {
            return array_bulk(
                scored
                    .into_iter()
                    .map(|(entry, _)| entry.element.clone())
                    .collect(),
            );
        }
        let mut out = Vec::new();
        for (entry, score) in scored {
            out.push(bulk(entry.element.clone()));
            if parsed.with_scores {
                out.push(bulk(format_number(score).into_bytes()));
            }
            if parsed.with_attribs {
                out.push(entry.attributes.clone().map_or(Frame::Null, bulk));
            }
            if parsed.with_governance {
                out.push(entry.governance.clone().map_or(Frame::Null, bulk));
            }
        }
        Frame::Array(out)
    }
}

fn vsim_requires_exact(args: &[&[u8]]) -> bool {
    args.iter().any(|token| {
        eq_ignore_ascii_case(token, b"FILTER") || eq_ignore_ascii_case(token, b"TRUTH")
    })
}

#[derive(Debug)]
struct VAddArgs {
    element: Bytes,
    vector: Vec<f64>,
    attributes: Option<Bytes>,
    governance: Option<Bytes>,
    quantization: Option<Quantization>,
    reduce_dim: Option<usize>,
    hnsw_m: Option<usize>,
    ef_construction: Option<usize>,
}

#[derive(Debug)]
struct VSimArgs {
    vector: Vec<f64>,
    count: usize,
    with_scores: bool,
    with_attribs: bool,
    with_governance: bool,
    filter: Option<Bytes>,
    ef_search: Option<usize>,
    truth: bool,
}

impl VectorSetState {
    fn entry(&self, element: &[u8]) -> Option<&VectorEntry> {
        self.entries
            .iter()
            .find(|entry| entry.element.as_slice() == element)
    }

    fn entry_mut(&mut self, element: &[u8]) -> Option<&mut VectorEntry> {
        self.entries
            .iter_mut()
            .find(|entry| entry.element.as_slice() == element)
    }

    fn entry_by_uid(&self, uid: u64) -> Option<&VectorEntry> {
        self.entries.iter().find(|entry| entry.uid == uid)
    }

    fn rebuild_hnsw(&mut self) {
        let m = self.hnsw_m.max(1);
        self.max_level = self
            .entries
            .iter()
            .map(|entry| entry.level)
            .max()
            .unwrap_or(0);

        for entry in &mut self.entries {
            entry.links = vec![Vec::new(); entry.level.saturating_add(1)];
        }

        for level in 0..=self.max_level {
            let active = self
                .entries
                .iter()
                .enumerate()
                .filter_map(|(index, entry)| (entry.level >= level).then_some(index))
                .collect::<Vec<_>>();

            for &index in &active {
                let mut neighbors = active
                    .iter()
                    .copied()
                    .filter(|candidate| *candidate != index)
                    .map(|candidate| {
                        (
                            candidate,
                            cosine_similarity(
                                &self.entries[index].vector,
                                &self.entries[candidate].vector,
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                neighbors.sort_by(|(left_index, left_score), (right_index, right_score)| {
                    right_score
                        .partial_cmp(left_score)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| {
                            self.entries[*left_index]
                                .element
                                .cmp(&self.entries[*right_index].element)
                        })
                });

                let new_links = neighbors
                    .into_iter()
                    .take(m)
                    .map(|(candidate, _)| self.entries[candidate].uid)
                    .collect();
                if let Some(links) = self.entries[index].links.get_mut(level) {
                    *links = new_links;
                }
            }
        }
    }

    fn insert_hnsw_entry(&mut self, mut entry: VectorEntry) {
        let m = self.hnsw_m.max(1);
        entry.links = vec![Vec::new(); entry.level.saturating_add(1)];
        self.max_level = self.max_level.max(entry.level);
        let new_uid = entry.uid;
        let new_level = entry.level;
        let new_vector = entry.vector.clone();
        let new_element = entry.element.clone();
        let new_index = self.entries.len();

        let mut selected_by_level = Vec::with_capacity(new_level.saturating_add(1));
        for level in 0..=new_level {
            let mut neighbors = self
                .entries
                .iter()
                .enumerate()
                .filter(|(_, candidate)| candidate.level >= level)
                .map(|(index, candidate)| {
                    (
                        index,
                        cosine_similarity(&new_vector, &candidate.vector),
                        candidate.element.clone(),
                    )
                })
                .collect::<Vec<_>>();
            neighbors.sort_by(
                |(_, left_score, left_element), (_, right_score, right_element)| {
                    right_score
                        .partial_cmp(left_score)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| left_element.cmp(right_element))
                },
            );
            selected_by_level.push(
                neighbors
                    .into_iter()
                    .take(m)
                    .map(|(index, _, _)| index)
                    .collect::<Vec<_>>(),
            );
        }

        self.entries.push(entry);
        for (level, neighbors) in selected_by_level.into_iter().enumerate() {
            self.entries[new_index].links[level] = neighbors
                .iter()
                .map(|index| self.entries[*index].uid)
                .collect();
            for neighbor_index in neighbors {
                self.ensure_link_layer(neighbor_index, level);
                if !self.entries[neighbor_index].links[level].contains(&new_uid) {
                    self.entries[neighbor_index].links[level].push(new_uid);
                }
                self.prune_links_for_index(neighbor_index, level, m);
            }
        }
        if new_level == 0 && self.entries[new_index].links.is_empty() {
            self.entries[new_index].links.push(Vec::new());
        }
        debug_assert_eq!(self.entries[new_index].element, new_element);
    }

    fn ensure_link_layer(&mut self, index: usize, level: usize) {
        while self.entries[index].links.len() <= level {
            self.entries[index].links.push(Vec::new());
        }
    }

    fn prune_links_for_index(&mut self, index: usize, level: usize, m: usize) {
        let source_vector = self.entries[index].vector.clone();
        let links = self.entries[index]
            .links
            .get(level)
            .cloned()
            .unwrap_or_default();
        let mut scored = links
            .into_iter()
            .filter_map(|uid| {
                self.entry_by_uid(uid).map(|entry| {
                    (
                        uid,
                        cosine_similarity(&source_vector, &entry.vector),
                        entry.element.clone(),
                    )
                })
            })
            .collect::<Vec<_>>();
        scored.sort_by(
            |(_, left_score, left_element), (_, right_score, right_element)| {
                right_score
                    .partial_cmp(left_score)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| left_element.cmp(right_element))
            },
        );
        scored.dedup_by_key(|(uid, _, _)| *uid);
        self.entries[index].links[level] =
            scored.into_iter().take(m).map(|(uid, _, _)| uid).collect();
    }
}

fn vector_write_maybe(
    store: &EmbeddedStore,
    key: &[u8],
    op: impl FnOnce(&mut VectorSetState) -> VectorWriteResult,
) -> Frame {
    let result = store.transform_raw_vector_value_preserve_ttl(
        key,
        |existing| {
            let mut set = match decode_vector_set(existing) {
                Ok(set) => set,
                Err(()) => return Err(wrongtype()),
            };
            match op(&mut set) {
                VectorWriteResult::Changed(frame) => Ok(((frame, true), encode_vector_set(&set))),
                VectorWriteResult::Unchanged(frame) => {
                    let value = existing
                        .map(<[u8]>::to_vec)
                        .unwrap_or_else(|| encode_vector_set(&set));
                    Ok(((frame, false), value))
                }
            }
        },
        wrongtype,
    );
    match result {
        Ok((frame, true)) => {
            if let Some((value, expire_at_ms)) = store.clone_vector_value_state(key) {
                store.notify_vector_mutation(
                    crate::storage::VectorMutationKind::Set,
                    key,
                    Some(value),
                    expire_at_ms,
                );
            }
            frame
        }
        Ok((frame, false)) | Err(frame) => frame,
    }
}

fn vector_write_existing_maybe(
    store: &EmbeddedStore,
    key: &[u8],
    op: impl FnOnce(&mut VectorSetState) -> VectorWriteResult,
) -> Frame {
    match vector_key_exists(store, key) {
        Ok(true) => vector_write_maybe(store, key, op),
        Ok(false) => int(0),
        Err(frame) => frame,
    }
}

fn vector_setattr_update(
    store: &EmbeddedStore,
    key: &[u8],
    element: &[u8],
    attributes: &[u8],
) -> Frame {
    vector_write_existing_maybe(store, key, |set| {
        let Some(entry) = set.entry_mut(element) else {
            return VectorWriteResult::Unchanged(int(0));
        };
        let next_attributes = (!attributes.is_empty()).then(|| attributes.to_vec());
        if entry.attributes == next_attributes {
            return VectorWriteResult::Unchanged(int(1));
        }
        entry.attributes = next_attributes;
        VectorWriteResult::Changed(int(1))
    })
}

fn vector_rem_remove(store: &EmbeddedStore, key: &[u8], element: &[u8]) -> Frame {
    vector_write_existing_maybe(store, key, |set| {
        let before = set.entries.len();
        set.entries
            .retain(|entry| entry.element.as_slice() != element);
        if before == set.entries.len() {
            return VectorWriteResult::Unchanged(int(0));
        } else {
            set.rebuild_hnsw();
        }
        VectorWriteResult::Changed(int(1))
    })
}

#[cfg(feature = "server")]
fn write_vector_metadata_integer_resp(
    store: &EmbeddedStore,
    key: &[u8],
    out: &mut BytesMut,
    value: impl FnOnce(VectorSetMetadata) -> i64,
    missing: i64,
) {
    match vector_read_metadata(store, key) {
        Ok(Some(metadata)) => ServerWire::write_resp_integer(out, value(metadata)),
        Ok(None) => ServerWire::write_resp_integer(out, missing),
        Err(_) => write_resp_wrongtype(out),
    }
}

#[cfg(feature = "server")]
fn write_vector_exists_resp(store: &EmbeddedStore, key: &[u8], element: &[u8], out: &mut BytesMut) {
    match vector_lookup_entry(store, key, element, VectorLookupProjection::Exists) {
        Ok(VectorEntryLookup::Found(_)) => ServerWire::write_resp_integer(out, 1),
        Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => {
            ServerWire::write_resp_integer(out, 0);
        }
        Err(_) => write_resp_wrongtype(out),
    }
}

#[cfg(feature = "server")]
fn write_vector_attribute_resp(
    store: &EmbeddedStore,
    key: &[u8],
    element: &[u8],
    out: &mut BytesMut,
) {
    match vector_lookup_entry(store, key, element, VectorLookupProjection::Attributes) {
        Ok(VectorEntryLookup::Found(snapshot)) => match snapshot.attributes {
            Some(attributes) => ServerWire::write_resp_blob_string(out, &attributes),
            None => write_resp_null(out),
        },
        Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => {
            write_resp_null(out);
        }
        Err(_) => write_resp_wrongtype(out),
    }
}

#[cfg(feature = "server")]
fn write_vector_elements_resp(out: &mut BytesMut, elements: &[Bytes]) {
    write_resp_array_header(out, elements.len());
    for element in elements {
        ServerWire::write_resp_blob_string(out, element);
    }
}

#[cfg(feature = "server")]
#[inline(always)]
fn write_vector_fast_value(
    store: &EmbeddedStore,
    args: &[&[u8]],
    out: &mut BytesMut,
    write: impl FnOnce(&EmbeddedStore, &[&[u8]], &mut BytesMut),
) {
    let start = ServerWire::begin_fast_value(out);
    write(store, args, out);
    ServerWire::finish_fast_value(out, start);
}

#[cfg(feature = "server")]
fn write_vector_array_fast(frame: Frame, out: &mut BytesMut) {
    let Frame::Array(items) = frame else {
        write_fast_frame(out, &frame);
        return;
    };
    if items.iter().any(|item| {
        !matches!(
            item,
            Frame::BlobString(_) | Frame::SimpleString(_) | Frame::Null
        )
    }) {
        write_fast_frame(out, &Frame::Array(items));
        return;
    }
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Frame::BlobString(value) => values.push(Some(value)),
            Frame::SimpleString(value) => values.push(Some(value.into_bytes())),
            Frame::Null => values.push(None),
            _ => unreachable!("vector array item shape was validated"),
        }
    }
    let start = ServerWire::begin_fast_array(out, values.len());
    for value in &values {
        ServerWire::write_fast_array_item(out, value.as_deref());
    }
    ServerWire::finish_fast_array(out, start);
}

#[cfg(feature = "server")]
fn write_vinfo_resp(out: &mut BytesMut, metadata: VectorSetMetadata) {
    write_resp_array_header(out, 16);
    ServerWire::write_resp_blob_string(out, b"quant-type");
    ServerWire::write_resp_blob_string(out, metadata.quantization.vinfo_name().as_bytes());
    ServerWire::write_resp_blob_string(out, b"vector-dim");
    ServerWire::write_resp_integer(out, metadata.dim as i64);
    ServerWire::write_resp_blob_string(out, b"size");
    ServerWire::write_resp_integer(out, metadata.count as i64);
    ServerWire::write_resp_blob_string(out, b"max-level");
    ServerWire::write_resp_integer(out, metadata.max_level as i64);
    ServerWire::write_resp_blob_string(out, b"hnsw-m");
    ServerWire::write_resp_integer(out, metadata.hnsw_m as i64);
    ServerWire::write_resp_blob_string(out, b"hnsw-ef-construction");
    ServerWire::write_resp_integer(out, metadata.ef_construction as i64);
    ServerWire::write_resp_blob_string(out, b"vset-uid");
    ServerWire::write_resp_integer(out, 1);
    ServerWire::write_resp_blob_string(out, b"hnsw-max-node-uid");
    ServerWire::write_resp_integer(out, metadata.next_uid.saturating_sub(1) as i64);
}

fn vector_key_exists(store: &EmbeddedStore, key: &[u8]) -> Result<bool, Frame> {
    match vector_read_metadata(store, key) {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(frame) => Err(frame),
    }
}

fn vector_read_cached(
    store: &EmbeddedStore,
    key: &[u8],
    mode: VectorDecodeMode,
) -> Result<Option<Arc<VectorSetState>>, Frame> {
    let mut decoded = Ok(None);
    match store.get_raw_vector_value_into(key, |bytes| {
        decoded = if bytes.starts_with(VECTOR_SET_PREFIX) {
            decode_vector_set_cached(bytes, mode)
                .map(Some)
                .map_err(|_| wrongtype())
        } else {
            Err(wrongtype())
        };
    }) {
        crate::storage::RedisStringLookup::Hit => decoded,
        crate::storage::RedisStringLookup::Miss => Ok(None),
        crate::storage::RedisStringLookup::WrongType => Err(wrongtype()),
    }
}

fn vector_read_metadata(
    store: &EmbeddedStore,
    key: &[u8],
) -> Result<Option<VectorSetMetadata>, Frame> {
    let mut decoded = Ok(None);
    match store.get_raw_vector_value_into(key, |bytes| {
        decoded = if bytes.starts_with(VECTOR_SET_PREFIX) {
            decode_vector_set_metadata(bytes.as_ref())
                .map(Some)
                .map_err(|_| wrongtype())
        } else {
            Err(wrongtype())
        };
    }) {
        crate::storage::RedisStringLookup::Hit => decoded,
        crate::storage::RedisStringLookup::Miss => Ok(None),
        crate::storage::RedisStringLookup::WrongType => Err(wrongtype()),
    }
}

fn vector_lookup_entry(
    store: &EmbeddedStore,
    key: &[u8],
    element: &[u8],
    projection: VectorLookupProjection,
) -> Result<VectorEntryLookup, Frame> {
    let mut decoded = Ok(VectorEntryLookup::MissingKey);
    match store.get_raw_vector_value_into(key, |bytes| {
        decoded = if bytes.starts_with(VECTOR_SET_PREFIX) {
            cached_vector_lookup(bytes, element, projection).map_err(|_| wrongtype())
        } else {
            Err(wrongtype())
        };
    }) {
        crate::storage::RedisStringLookup::Hit => decoded,
        crate::storage::RedisStringLookup::Miss => Ok(VectorEntryLookup::MissingKey),
        crate::storage::RedisStringLookup::WrongType => Err(wrongtype()),
    }
}

fn cached_vector_lookup(
    existing: &bytes::Bytes,
    element: &[u8],
    projection: VectorLookupProjection,
) -> Result<VectorEntryLookup, ()> {
    if existing.len() > VECTOR_DECODE_CACHE_MAX_VALUE_BYTES {
        return collect_vector_lookup(existing.as_ref(), element, projection);
    }
    let key = vector_lookup_cache_key(existing, element, projection);
    if let Some(lookup) = vector_lookup_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(key))
    {
        return Ok(lookup.as_ref().clone());
    }
    let lookup = collect_vector_lookup(existing.as_ref(), element, projection)?;
    if let Ok(mut cache) = vector_lookup_cache().lock() {
        cache.insert(key, existing.clone(), Arc::new(lookup.clone()));
    }
    Ok(lookup)
}

fn collect_vector_lookup(
    existing: &[u8],
    element: &[u8],
    projection: VectorLookupProjection,
) -> Result<VectorEntryLookup, ()> {
    match hnsw_find_entry(existing, element, projection) {
        Ok(Some(snapshot)) => Ok(VectorEntryLookup::Found(snapshot)),
        Ok(None) => Ok(VectorEntryLookup::MissingElement),
        Err(()) => decode_vector_set(Some(existing)).map(|set| {
            set.entry(element)
                .map_or(VectorEntryLookup::MissingElement, |entry| {
                    VectorEntryLookup::Found(VectorEntrySnapshot {
                        vector: projection.include_vector().then(|| entry.vector.clone()),
                        attributes: projection
                            .include_attributes()
                            .then(|| entry.attributes.clone())
                            .flatten(),
                        governance: projection
                            .include_governance()
                            .then(|| entry.governance.clone())
                            .flatten(),
                    })
                })
        }),
    }
}

fn vector_read_elements(store: &EmbeddedStore, key: &[u8]) -> Result<Option<Vec<Bytes>>, Frame> {
    let mut decoded = Ok(None);
    match store.get_raw_vector_value_into(key, |bytes| {
        decoded = if bytes.starts_with(VECTOR_SET_PREFIX) {
            match hnsw_collect_elements(bytes.as_ref()) {
                Ok(elements) => Ok(Some(elements)),
                Err(()) => decode_vector_set(Some(bytes.as_ref()))
                    .map(|set| {
                        Some(
                            set.entries
                                .into_iter()
                                .map(|entry| entry.element)
                                .collect::<Vec<_>>(),
                        )
                    })
                    .map_err(|_| wrongtype()),
            }
        } else {
            Err(wrongtype())
        };
    }) {
        crate::storage::RedisStringLookup::Hit => decoded,
        crate::storage::RedisStringLookup::Miss => Ok(None),
        crate::storage::RedisStringLookup::WrongType => Err(wrongtype()),
    }
}

fn vector_read_prefix_elements(
    store: &EmbeddedStore,
    key: &[u8],
    limit: usize,
) -> Result<Option<Vec<Bytes>>, Frame> {
    let mut decoded = Ok(None);
    match store.get_raw_vector_value_into(key, |bytes| {
        decoded = if bytes.starts_with(VECTOR_SET_PREFIX) {
            match hnsw_collect_prefix_elements(bytes.as_ref(), limit) {
                Ok(elements) => Ok(Some(elements)),
                Err(()) => decode_vector_set(Some(bytes.as_ref()))
                    .map(|set| {
                        Some(
                            set.entries
                                .into_iter()
                                .take(limit)
                                .map(|entry| entry.element)
                                .collect::<Vec<_>>(),
                        )
                    })
                    .map_err(|_| wrongtype()),
            }
        } else {
            Err(wrongtype())
        };
    }) {
        crate::storage::RedisStringLookup::Hit => decoded,
        crate::storage::RedisStringLookup::Miss => Ok(None),
        crate::storage::RedisStringLookup::WrongType => Err(wrongtype()),
    }
}

fn vector_read_lex_range(
    store: &EmbeddedStore,
    key: &[u8],
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Result<Option<Vec<Bytes>>, Frame> {
    let mut decoded = Ok(None);
    match store.get_raw_vector_value_into(key, |bytes| {
        decoded = if bytes.starts_with(VECTOR_SET_PREFIX) {
            cached_vector_lex_range(bytes, start, end, limit)
                .map(Some)
                .map_err(|_| wrongtype())
        } else {
            Err(wrongtype())
        };
    }) {
        crate::storage::RedisStringLookup::Hit => decoded,
        crate::storage::RedisStringLookup::Miss => Ok(None),
        crate::storage::RedisStringLookup::WrongType => Err(wrongtype()),
    }
}

fn parse_vadd_args(args: &[&[u8]]) -> Result<VAddArgs, Frame> {
    let mut index = 0usize;
    let mut reduce_dim = None;
    if args
        .get(index)
        .is_some_and(|token| eq_ignore_ascii_case(token, b"REDUCE"))
    {
        let Some(dim) = args.get(index + 1) else {
            return Err(error("ERR syntax error"));
        };
        let dim =
            parse_usize(dim).map_err(|_| error("ERR value is not an integer or out of range"))?;
        if dim == 0 {
            return Err(error("ERR vector dimension must be positive"));
        }
        reduce_dim = Some(dim);
        index += 2;
    }
    let vector = parse_vector_arg(args, &mut index)?;
    let Some(element) = args.get(index) else {
        return Err(wrong_arity("VADD"));
    };
    index += 1;
    let mut attributes = None;
    let mut governance = None;
    let mut quantization = None;
    let mut hnsw_m = None;
    let mut ef_construction = None;
    while index < args.len() {
        match args[index] {
            token if eq_ignore_ascii_case(token, b"CAS") => {
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"NOQUANT") => {
                quantization = Some(Quantization::NoQuant);
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"Q8") => {
                quantization = Some(Quantization::Q8);
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"BIN") => {
                quantization = Some(Quantization::Bin);
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"EF") => {
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                ef_construction = Some(parse_hnsw_usize(raw)?);
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"M") => {
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                hnsw_m = Some(parse_hnsw_usize(raw)?);
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"SETATTR") => {
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                validate_attributes(raw)?;
                attributes = Some((*raw).to_vec());
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"GOVERNANCE") => {
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                if raw.len() > MAX_VECTOR_GOVERNANCE_BYTES {
                    return Err(error("ERR vector governance metadata is too large"));
                }
                governance = Some((*raw).to_vec());
                index += 2;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok(VAddArgs {
        element: (*element).to_vec(),
        vector,
        attributes,
        governance,
        quantization,
        reduce_dim,
        hnsw_m,
        ef_construction,
    })
}

fn parse_vsim_args(args: &[&[u8]], set: &VectorSetState) -> Result<VSimArgs, Frame> {
    let mut index = 0usize;
    let vector = match args.get(index) {
        Some(token) if eq_ignore_ascii_case(token, b"ELE") => {
            let Some(element) = args.get(index + 1) else {
                return Err(error("ERR syntax error"));
            };
            index += 2;
            match set.entry(element) {
                Some(entry) => entry.vector.clone(),
                None => return Err(error("ERR no such element")),
            }
        }
        _ => parse_vector_arg(args, &mut index)?,
    };
    let mut count = 10usize;
    let mut with_scores = false;
    let mut with_attribs = false;
    let mut with_governance = false;
    let mut filter = None;
    let mut ef_search = None;
    let mut truth = false;
    while index < args.len() {
        match args[index] {
            token if eq_ignore_ascii_case(token, b"WITHSCORES") => {
                with_scores = true;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"WITHATTRIBS") => {
                with_attribs = true;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"WITHGOVERNANCE") => {
                with_governance = true;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"COUNT") => {
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                count = parse_usize(raw)
                    .map_err(|_| error("ERR value is not an integer or out of range"))?;
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"FILTER") => {
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                filter = Some((*raw).to_vec());
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"EF") => {
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                ef_search = Some(parse_hnsw_usize(raw)?);
                index += 2;
            }
            token
                if eq_ignore_ascii_case(token, b"EPSILON")
                    || eq_ignore_ascii_case(token, b"FILTER-EF") =>
            {
                if args.get(index + 1).is_none() {
                    return Err(error("ERR syntax error"));
                }
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"TRUTH") => {
                truth = true;
                index += 1;
            }
            token if eq_ignore_ascii_case(token, b"NOTHREAD") => {
                index += 1;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok(VSimArgs {
        vector,
        count,
        with_scores,
        with_attribs,
        with_governance,
        filter,
        ef_search,
        truth,
    })
}

fn parse_hnsw_usize(raw: &[u8]) -> Result<usize, Frame> {
    let value =
        parse_usize(raw).map_err(|_| error("ERR value is not an integer or out of range"))?;
    if value == 0 {
        return Err(error("ERR value is not an integer or out of range"));
    }
    Ok(value)
}

fn parse_vector_arg(args: &[&[u8]], index: &mut usize) -> Result<Vec<f64>, Frame> {
    match args.get(*index) {
        Some(token) if eq_ignore_ascii_case(token, b"VALUES") => {
            let Some(raw_count) = args.get(*index + 1) else {
                return Err(error("ERR syntax error"));
            };
            let count = parse_usize(raw_count)
                .map_err(|_| error("ERR value is not an integer or out of range"))?;
            let start = *index + 2;
            let end = start + count;
            let Some(raw_values) = args.get(start..end) else {
                return Err(error("ERR syntax error"));
            };
            let values = raw_values
                .iter()
                .map(|raw| parse_f64(raw).map_err(|_| error("ERR value is not a float")))
                .collect::<Result<Vec<_>, _>>()?;
            *index = end;
            Ok(values)
        }
        Some(token) if eq_ignore_ascii_case(token, b"FP32") => {
            let Some(blob) = args.get(*index + 1) else {
                return Err(error("ERR syntax error"));
            };
            let values = fp32_values(blob)?;
            *index += 2;
            Ok(values)
        }
        _ => Err(error("ERR syntax error")),
    }
}

fn exact_vector_scores<'a>(
    set: &'a VectorSetState,
    query: &[f64],
    filter: Option<&[u8]>,
    shard_count: usize,
) -> Vec<(&'a VectorEntry, f64)> {
    let compiled_filter = filter.and_then(CompiledVectorFilter::parse);
    let workers = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    if set.entries.len() < VECTOR_SCAN_PARALLEL_MIN || workers <= 1 {
        return exact_vector_scores_sequential(set, query, filter, compiled_filter.as_ref());
    }

    let shard_count = shard_count.max(1);
    let mut shard_entries = (0..shard_count)
        .map(|_| Vec::new())
        .collect::<Vec<Vec<&'a VectorEntry>>>();
    for entry in &set.entries {
        let shard_id = stripe_index(hash_key(&entry.element), shift_for(shard_count));
        shard_entries[shard_id].push(entry);
    }
    let non_empty_shards = shard_entries
        .iter()
        .filter_map(|entries| (!entries.is_empty()).then_some(entries.as_slice()))
        .collect::<Vec<_>>();

    let workers = workers.min(non_empty_shards.len()).max(1);
    let chunk_size = non_empty_shards.len().div_ceil(workers);
    let mut scored = Vec::with_capacity(set.entries.len());
    let compiled_filter = compiled_filter.as_ref();
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for shard_chunk in non_empty_shards.chunks(chunk_size) {
            handles.push(scope.spawn(move || {
                let mut shard_scores = Vec::new();
                for entries in shard_chunk {
                    shard_scores.extend(entries.iter().filter_map(|entry| {
                        if !entry_matches_filter(entry, filter, compiled_filter) {
                            return None;
                        }
                        Some((*entry, cosine_similarity(&entry.vector, query)))
                    }));
                }
                shard_scores
            }));
        }
        for handle in handles {
            scored.extend(handle.join().expect("parallel vector scan worker panicked"));
        }
    });
    sorted_scores(scored)
}

fn exact_vector_scores_sequential<'a>(
    set: &'a VectorSetState,
    query: &[f64],
    filter: Option<&[u8]>,
    compiled_filter: Option<&CompiledVectorFilter>,
) -> Vec<(&'a VectorEntry, f64)> {
    sorted_scores(
        set.entries
            .iter()
            .filter(|entry| entry_matches_filter(entry, filter, compiled_filter))
            .map(|entry| (entry, cosine_similarity(&entry.vector, query)))
            .collect(),
    )
}

fn entry_matches_filter(
    entry: &VectorEntry,
    filter: Option<&[u8]>,
    compiled_filter: Option<&CompiledVectorFilter>,
) -> bool {
    match (filter, compiled_filter) {
        (None, _) => true,
        (Some(_), Some(compiled)) => compiled.matches(entry.attributes.as_deref()),
        (Some(expression), None) => attributes_match(entry.attributes.as_deref(), expression),
    }
}

fn hnsw_search<'a>(
    set: &'a VectorSetState,
    query: &[f64],
    count: usize,
    ef_search: usize,
) -> Vec<(&'a VectorEntry, f64)> {
    if set.entries.is_empty() {
        return Vec::new();
    }

    let mut current_index = set
        .entries
        .iter()
        .enumerate()
        .max_by_key(|(_, entry)| entry.level)
        .map(|(index, _)| index)
        .unwrap_or(0);
    let uid_to_index = build_uid_index(set);

    for level in (1..=set.max_level).rev() {
        current_index = greedy_hnsw_layer_index(set, &uid_to_index, current_index, query, level);
    }

    let mut candidates = vec![current_index];
    let mut visited = vec![current_index];
    let mut seen = vec![false; set.entries.len()];
    seen[current_index] = true;
    let mut cursor = 0usize;
    let limit = ef_search.max(count).max(1);

    while cursor < candidates.len() && visited.len() < limit {
        let index = candidates[cursor];
        cursor += 1;
        let Some(links) = set.entries[index].links.first() else {
            continue;
        };
        for uid in links {
            let Some(next_index) = lookup_uid_index(&uid_to_index, *uid) else {
                continue;
            };
            if seen[next_index] {
                continue;
            }
            seen[next_index] = true;
            visited.push(next_index);
            candidates.push(next_index);
            if visited.len() >= limit {
                break;
            }
        }
    }

    if visited.len() < count.min(set.entries.len()) {
        for (index, was_seen) in seen.iter_mut().enumerate().take(set.entries.len()) {
            if !*was_seen {
                *was_seen = true;
                visited.push(index);
            }
            if visited.len() >= limit.max(count) {
                break;
            }
        }
    }

    sorted_scores(
        visited
            .into_iter()
            .map(|index| {
                let entry = &set.entries[index];
                (entry, cosine_similarity(&entry.vector, query))
            })
            .collect(),
    )
}

fn build_uid_index(set: &VectorSetState) -> Vec<Option<usize>> {
    let max_uid = set.entries.iter().map(|entry| entry.uid).max().unwrap_or(0) as usize;
    let mut uid_to_index = vec![None; max_uid.saturating_add(1)];
    for (index, entry) in set.entries.iter().enumerate() {
        if let Some(slot) = uid_to_index.get_mut(entry.uid as usize) {
            *slot = Some(index);
        }
    }
    uid_to_index
}

fn lookup_uid_index(uid_to_index: &[Option<usize>], uid: u64) -> Option<usize> {
    uid_to_index.get(uid as usize).and_then(|index| *index)
}

fn greedy_hnsw_layer_index(
    set: &VectorSetState,
    uid_to_index: &[Option<usize>],
    start_index: usize,
    query: &[f64],
    level: usize,
) -> usize {
    let mut current_index = start_index;
    let mut current_score = cosine_similarity(&set.entries[current_index].vector, query);

    loop {
        let mut improved = false;
        let links = set.entries[current_index]
            .links
            .get(level)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        for uid in links {
            let Some(candidate_index) = lookup_uid_index(uid_to_index, *uid) else {
                continue;
            };
            let candidate = &set.entries[candidate_index];
            let score = cosine_similarity(&candidate.vector, query);
            if score > current_score {
                current_score = score;
                current_index = candidate_index;
                improved = true;
            }
        }
        if !improved {
            return current_index;
        }
    }
}

fn sorted_scores(mut scored: Vec<(&VectorEntry, f64)>) -> Vec<(&VectorEntry, f64)> {
    scored.sort_by(|(left, left_score), (right, right_score)| {
        right_score
            .partial_cmp(left_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.element.cmp(&right.element))
    });
    scored
}

fn decode_vector_set(existing: Option<&[u8]>) -> Result<VectorSetState, ()> {
    let Some(mut raw) = existing else {
        return Ok(VectorSetState::default());
    };
    if !raw.starts_with(VECTOR_SET_PREFIX) {
        return Err(());
    }
    raw = &raw[VECTOR_SET_PREFIX.len()..];
    decode_vector_set_payload(raw, VectorPayloadFormat::HnswGoverned, true)
        .or_else(|_| decode_vector_set_payload(raw, VectorPayloadFormat::Hnsw, true))
        .or_else(|_| decode_vector_set_payload(raw, VectorPayloadFormat::Current, true))
        .or_else(|_| decode_vector_set_payload(raw, VectorPayloadFormat::Quantized, true))
        .or_else(|_| decode_vector_set_payload(raw, VectorPayloadFormat::Legacy, true))
}

fn decode_vector_set_entries(existing: Option<&[u8]>) -> Result<VectorSetState, ()> {
    let Some(mut raw) = existing else {
        return Ok(VectorSetState::default());
    };
    if !raw.starts_with(VECTOR_SET_PREFIX) {
        return Err(());
    }
    raw = &raw[VECTOR_SET_PREFIX.len()..];
    decode_vector_set_payload(raw, VectorPayloadFormat::HnswGoverned, false)
        .or_else(|_| decode_vector_set_payload(raw, VectorPayloadFormat::Hnsw, false))
        .or_else(|_| decode_vector_set_payload(raw, VectorPayloadFormat::Current, false))
        .or_else(|_| decode_vector_set_payload(raw, VectorPayloadFormat::Quantized, false))
        .or_else(|_| decode_vector_set_payload(raw, VectorPayloadFormat::Legacy, false))
}

fn decode_vector_set_cached(
    existing: &bytes::Bytes,
    mode: VectorDecodeMode,
) -> Result<Arc<VectorSetState>, ()> {
    if existing.len() > VECTOR_DECODE_CACHE_MAX_VALUE_BYTES {
        return decode_vector_set_for_mode(existing.as_ref(), mode).map(Arc::new);
    }
    let key = vector_decode_cache_key(existing, mode);
    if let Some(set) = vector_decode_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(key))
    {
        return Ok(set);
    }
    let set = Arc::new(decode_vector_set_for_mode(existing.as_ref(), mode)?);
    if let Ok(mut cache) = vector_decode_cache().lock() {
        cache.insert(key, existing.clone(), Arc::clone(&set));
    }
    Ok(set)
}

fn decode_vector_set_for_mode(
    existing: &[u8],
    mode: VectorDecodeMode,
) -> Result<VectorSetState, ()> {
    match mode {
        VectorDecodeMode::Full => decode_vector_set(Some(existing)),
        VectorDecodeMode::EntriesOnly => decode_vector_set_entries(Some(existing)),
    }
}

fn cached_vector_lex_range(
    existing: &bytes::Bytes,
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Result<Vec<Bytes>, ()> {
    if existing.len() > VECTOR_DECODE_CACHE_MAX_VALUE_BYTES {
        return collect_vector_lex_range(existing.as_ref(), start, end, limit);
    }
    let key = vector_lex_range_cache_key(existing, start, end, limit);
    if let Some(elements) = vector_lex_range_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(key))
    {
        return Ok(elements.as_ref().clone());
    }
    let elements = collect_vector_lex_range(existing.as_ref(), start, end, limit)?;
    if let Ok(mut cache) = vector_lex_range_cache().lock() {
        cache.insert(key, existing.clone(), Arc::new(elements.clone()));
    }
    Ok(elements)
}

fn collect_vector_lex_range(
    existing: &[u8],
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Result<Vec<Bytes>, ()> {
    hnsw_collect_lex_range(existing, start, end, limit).or_else(|_| {
        decode_vector_set(Some(existing)).map(|set| {
            let mut elements = set
                .entries
                .into_iter()
                .map(|entry| entry.element)
                .filter(|element| lex_in_range(element, start, end))
                .collect::<Vec<_>>();
            elements.sort();
            elements.truncate(limit);
            elements
        })
    })
}

fn vector_decode_cache_key(
    existing: &bytes::Bytes,
    mode: VectorDecodeMode,
) -> VectorDecodeCacheKey {
    VectorDecodeCacheKey {
        mode,
        ptr: existing.as_ptr() as usize,
        len: existing.len(),
    }
}

fn vector_lex_range_cache_key(
    existing: &bytes::Bytes,
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> VectorLexRangeCacheKey {
    VectorLexRangeCacheKey {
        value_ptr: existing.as_ptr() as usize,
        value_len: existing.len(),
        start_len: start.len(),
        start_hash: xxhash_rust::xxh3::xxh3_64(start),
        start_head: cache_edge_u64(start, 0),
        start_tail: cache_edge_u64(start, start.len().saturating_sub(8)),
        end_len: end.len(),
        end_hash: xxhash_rust::xxh3::xxh3_64(end),
        end_head: cache_edge_u64(end, 0),
        end_tail: cache_edge_u64(end, end.len().saturating_sub(8)),
        limit,
    }
}

fn vector_lookup_cache_key(
    existing: &bytes::Bytes,
    element: &[u8],
    projection: VectorLookupProjection,
) -> VectorLookupCacheKey {
    VectorLookupCacheKey {
        value_ptr: existing.as_ptr() as usize,
        value_len: existing.len(),
        element_len: element.len(),
        element_hash: xxhash_rust::xxh3::xxh3_64(element),
        element_head: cache_edge_u64(element, 0),
        element_tail: cache_edge_u64(element, element.len().saturating_sub(8)),
        projection,
    }
}

fn vector_attribute_validation_cache_key(raw: &[u8]) -> VectorAttributeValidationCacheKey {
    VectorAttributeValidationCacheKey {
        ptr: raw.as_ptr() as usize,
        len: raw.len(),
    }
}

fn vector_lookup_cache_bytes(lookup: &VectorEntryLookup) -> usize {
    match lookup {
        VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement => 0,
        VectorEntryLookup::Found(snapshot) => snapshot
            .attributes
            .as_ref()
            .map_or(0, Vec::len)
            .saturating_add(snapshot.governance.as_ref().map_or(0, Vec::len))
            .saturating_add(snapshot.vector.as_ref().map_or(0, |vector| {
                vector.len().saturating_mul(std::mem::size_of::<f64>())
            })),
    }
}

fn cache_edge_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut out = [0u8; 8];
    if let Some(slice) = bytes.get(offset..offset.saturating_add(8)) {
        out[..slice.len()].copy_from_slice(slice);
    }
    u64::from_le_bytes(out)
}

fn decode_vector_set_metadata(existing: &[u8]) -> Result<VectorSetMetadata, ()> {
    let mut raw = existing.strip_prefix(VECTOR_SET_PREFIX).ok_or(())?;
    decode_vector_set_metadata_payload(raw, VectorPayloadFormat::HnswGoverned)
        .or_else(|_| {
            raw = existing.strip_prefix(VECTOR_SET_PREFIX).ok_or(())?;
            decode_vector_set_metadata_payload(raw, VectorPayloadFormat::Hnsw)
        })
        .or_else(|_| {
            raw = existing.strip_prefix(VECTOR_SET_PREFIX).ok_or(())?;
            decode_vector_set_metadata_payload(raw, VectorPayloadFormat::Current)
        })
        .or_else(|_| {
            raw = existing.strip_prefix(VECTOR_SET_PREFIX).ok_or(())?;
            decode_vector_set_metadata_payload(raw, VectorPayloadFormat::Quantized)
        })
        .or_else(|_| {
            raw = existing.strip_prefix(VECTOR_SET_PREFIX).ok_or(())?;
            decode_vector_set_metadata_payload(raw, VectorPayloadFormat::Legacy)
        })
}

fn hnsw_find_entry(
    existing: &[u8],
    element: &[u8],
    projection: VectorLookupProjection,
) -> Result<Option<VectorEntrySnapshot>, ()> {
    scan_hnsw_entries(existing, |entry| {
        if entry.element == element {
            Some(VectorEntrySnapshot {
                vector: projection
                    .include_vector()
                    .then(|| read_f64_slice(entry.vector_raw))
                    .transpose()
                    .ok()?,
                attributes: projection
                    .include_attributes()
                    .then(|| entry.attributes.map(<[u8]>::to_vec))
                    .flatten(),
                governance: projection
                    .include_governance()
                    .then(|| entry.governance.map(<[u8]>::to_vec))
                    .flatten(),
            })
        } else {
            None
        }
    })
}

fn hnsw_collect_elements(existing: &[u8]) -> Result<Vec<Bytes>, ()> {
    let mut elements = Vec::new();
    let matched = scan_hnsw_entries(existing, |entry| {
        elements.push(entry.element.to_vec());
        None::<()>
    })?;
    debug_assert!(matched.is_none());
    Ok(elements)
}

fn hnsw_collect_prefix_elements(existing: &[u8], limit: usize) -> Result<Vec<Bytes>, ()> {
    if limit == 0 {
        validate_hnsw_payload(existing)?;
        return Ok(Vec::new());
    }
    let mut elements = Vec::with_capacity(limit.min(16));
    let matched = scan_hnsw_entries(existing, |entry| {
        elements.push(entry.element.to_vec());
        (elements.len() >= limit).then_some(())
    })?;
    debug_assert!(matched.is_some() || elements.len() < limit);
    Ok(elements)
}

fn hnsw_collect_lex_range(
    existing: &[u8],
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Result<Vec<Bytes>, ()> {
    if limit == 0 {
        validate_hnsw_payload(existing)?;
        return Ok(Vec::new());
    }
    let mut elements = Vec::with_capacity(limit.min(16));
    let matched = scan_hnsw_entries(existing, |entry| {
        if lex_in_range(entry.element, start, end) {
            insert_bounded_lex(&mut elements, entry.element, limit);
        }
        None::<()>
    })?;
    debug_assert!(matched.is_none());
    Ok(elements)
}

fn validate_hnsw_payload(existing: &[u8]) -> Result<(), ()> {
    let mut raw = existing.strip_prefix(VECTOR_SET_PREFIX).ok_or(())?;
    let format = read_u32(&mut raw)?;
    if is_hnsw_format(format) {
        Ok(())
    } else {
        Err(())
    }
}

#[inline(always)]
fn is_hnsw_format(format: u32) -> bool {
    matches!(
        format,
        HNSW_VECTOR_SET_FORMAT
            | HNSW_VECTOR_SET_FORMAT_LEGACY_TYPO
            | HNSW_GOVERNED_VECTOR_SET_FORMAT
    )
}

fn insert_bounded_lex(elements: &mut Vec<Bytes>, element: &[u8], limit: usize) {
    let position = elements
        .binary_search_by(|probe| probe.as_slice().cmp(element))
        .unwrap_or_else(|position| position);
    if position >= limit {
        return;
    }
    elements.insert(position, element.to_vec());
    if elements.len() > limit {
        elements.pop();
    }
}

struct HnswEntryView<'a> {
    element: &'a [u8],
    vector_raw: &'a [u8],
    attributes: Option<&'a [u8]>,
    governance: Option<&'a [u8]>,
}

fn scan_hnsw_entries<T>(
    existing: &[u8],
    mut visit: impl FnMut(HnswEntryView<'_>) -> Option<T>,
) -> Result<Option<T>, ()> {
    let mut raw = existing.strip_prefix(VECTOR_SET_PREFIX).ok_or(())?;
    let format = read_u32(&mut raw)?;
    if !is_hnsw_format(format) {
        return Err(());
    }
    let governed = format == HNSW_GOVERNED_VECTOR_SET_FORMAT;
    let _dim = read_u32(&mut raw)?;
    let _quantization = Quantization::from_tag(read_u32(&mut raw)?).ok_or(())?;
    let _original_dim = read_u32(&mut raw)?;
    let _hnsw_m = read_u32(&mut raw)?;
    let _ef_construction = read_u32(&mut raw)?;
    let _max_level = read_u32(&mut raw)?;
    let _next_uid = read_u64(&mut raw)?;
    let count = read_u32(&mut raw)? as usize;
    for _ in 0..count {
        let _uid = read_u64(&mut raw)?;
        let _level = read_u32(&mut raw)?;
        let element = read_bytes_slice(&mut raw)?;
        let vector_len = read_u32(&mut raw)? as usize;
        let vector_bytes = vector_len.checked_mul(8).ok_or(())?;
        if raw.len() < vector_bytes {
            return Err(());
        }
        let (vector_raw, tail) = raw.split_at(vector_bytes);
        raw = tail;
        let has_attributes = read_u32(&mut raw)? != 0;
        let attributes = if has_attributes {
            Some(read_bytes_slice(&mut raw)?)
        } else {
            None
        };
        let governance = if governed && read_u32(&mut raw)? != 0 {
            let governance = read_bytes_slice(&mut raw)?;
            if governance.len() > MAX_VECTOR_GOVERNANCE_BYTES {
                return Err(());
            }
            Some(governance)
        } else {
            None
        };
        if let Some(found) = visit(HnswEntryView {
            element,
            vector_raw,
            attributes,
            governance,
        }) {
            return Ok(Some(found));
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, Copy)]
enum VectorPayloadFormat {
    Hnsw,
    HnswGoverned,
    Current,
    Quantized,
    Legacy,
}

fn decode_vector_set_metadata_payload(
    mut raw: &[u8],
    format: VectorPayloadFormat,
) -> Result<VectorSetMetadata, ()> {
    let dim = read_u32(&mut raw)? as usize;
    if matches!(
        format,
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned
    ) && !is_hnsw_format(dim as u32)
    {
        return Err(());
    }
    if matches!(format, VectorPayloadFormat::HnswGoverned)
        && dim as u32 != HNSW_GOVERNED_VECTOR_SET_FORMAT
    {
        return Err(());
    }
    let dim = if matches!(
        format,
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned
    ) {
        read_u32(&mut raw)? as usize
    } else {
        dim
    };
    let quantization = match format {
        VectorPayloadFormat::Hnsw
        | VectorPayloadFormat::HnswGoverned
        | VectorPayloadFormat::Current
        | VectorPayloadFormat::Quantized => Quantization::from_tag(read_u32(&mut raw)?).ok_or(())?,
        VectorPayloadFormat::Legacy => Quantization::default(),
    };
    let original_dim = match format {
        VectorPayloadFormat::Hnsw
        | VectorPayloadFormat::HnswGoverned
        | VectorPayloadFormat::Current => read_u32(&mut raw)? as usize,
        VectorPayloadFormat::Quantized | VectorPayloadFormat::Legacy => dim,
    };
    let (hnsw_m, ef_construction, max_level, next_uid) = match format {
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned => (
            read_u32(&mut raw)? as usize,
            read_u32(&mut raw)? as usize,
            read_u32(&mut raw)? as usize,
            read_u64(&mut raw)?,
        ),
        VectorPayloadFormat::Current
        | VectorPayloadFormat::Quantized
        | VectorPayloadFormat::Legacy => (DEFAULT_HNSW_M, DEFAULT_HNSW_EF_CONSTRUCTION, 0, 1),
    };
    let count = read_u32(&mut raw)? as usize;
    let next_uid = match format {
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned => next_uid,
        VectorPayloadFormat::Current
        | VectorPayloadFormat::Quantized
        | VectorPayloadFormat::Legacy => count as u64 + 1,
    };
    Ok(VectorSetMetadata {
        dim,
        original_dim,
        quantization,
        hnsw_m,
        ef_construction,
        max_level,
        next_uid,
        count,
    })
}

fn decode_vector_set_payload(
    mut raw: &[u8],
    format: VectorPayloadFormat,
    decode_links: bool,
) -> Result<VectorSetState, ()> {
    let dim = read_u32(&mut raw)? as usize;
    if matches!(
        format,
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned
    ) && !is_hnsw_format(dim as u32)
    {
        return Err(());
    }
    if matches!(format, VectorPayloadFormat::HnswGoverned)
        && dim as u32 != HNSW_GOVERNED_VECTOR_SET_FORMAT
    {
        return Err(());
    }
    let dim = if matches!(
        format,
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned
    ) {
        read_u32(&mut raw)? as usize
    } else {
        dim
    };
    let quantization = match format {
        VectorPayloadFormat::Hnsw
        | VectorPayloadFormat::HnswGoverned
        | VectorPayloadFormat::Current
        | VectorPayloadFormat::Quantized => Quantization::from_tag(read_u32(&mut raw)?).ok_or(())?,
        VectorPayloadFormat::Legacy => Quantization::default(),
    };
    let original_dim = match format {
        VectorPayloadFormat::Hnsw
        | VectorPayloadFormat::HnswGoverned
        | VectorPayloadFormat::Current => read_u32(&mut raw)? as usize,
        VectorPayloadFormat::Quantized | VectorPayloadFormat::Legacy => dim,
    };
    let (hnsw_m, ef_construction, max_level, next_uid) = match format {
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned => (
            read_u32(&mut raw)? as usize,
            read_u32(&mut raw)? as usize,
            read_u32(&mut raw)? as usize,
            read_u64(&mut raw)?,
        ),
        VectorPayloadFormat::Current
        | VectorPayloadFormat::Quantized
        | VectorPayloadFormat::Legacy => (DEFAULT_HNSW_M, DEFAULT_HNSW_EF_CONSTRUCTION, 0, 1),
    };
    let count = read_u32(&mut raw)? as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let (uid, level) = match format {
            VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned => {
                (read_u64(&mut raw)?, read_u32(&mut raw)? as usize)
            }
            VectorPayloadFormat::Current
            | VectorPayloadFormat::Quantized
            | VectorPayloadFormat::Legacy => {
                let uid = entries.len() as u64 + 1;
                (uid, hnsw_level_from_uid(uid))
            }
        };
        let element = read_bytes(&mut raw)?;
        let vector_len = read_u32(&mut raw)? as usize;
        let mut vector = Vec::with_capacity(vector_len);
        for _ in 0..vector_len {
            vector.push(read_f64(&mut raw)?);
        }
        let has_attributes = read_u32(&mut raw)? != 0;
        let attributes = if has_attributes {
            Some(read_bytes(&mut raw)?)
        } else {
            None
        };
        let governance =
            if matches!(format, VectorPayloadFormat::HnswGoverned) && read_u32(&mut raw)? != 0 {
                let governance = read_bytes(&mut raw)?;
                if governance.len() > MAX_VECTOR_GOVERNANCE_BYTES {
                    return Err(());
                }
                Some(governance)
            } else {
                None
            };
        entries.push(VectorEntry {
            uid,
            level,
            element,
            vector,
            attributes,
            governance,
            links: Vec::new(),
        });
    }
    let mut read_hnsw_links = false;
    if matches!(
        format,
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned
    ) && !raw.is_empty()
        && decode_links
    {
        for entry in &mut entries {
            let layer_count = read_u32(&mut raw)? as usize;
            let mut links = Vec::with_capacity(layer_count);
            for _ in 0..layer_count {
                let link_count = read_u32(&mut raw)? as usize;
                let mut layer = Vec::with_capacity(link_count);
                for _ in 0..link_count {
                    layer.push(read_u64(&mut raw)?);
                }
                links.push(layer);
            }
            entry.links = links;
        }
        read_hnsw_links = true;
    } else if matches!(
        format,
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned
    ) && !decode_links
    {
        raw = &[];
    }
    if !raw.is_empty() {
        return Err(());
    }
    let mut set = VectorSetState {
        dim,
        original_dim,
        quantization,
        entries,
        hnsw_m,
        ef_construction,
        max_level,
        next_uid,
    };
    if !matches!(
        format,
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned
    ) {
        set.next_uid = set.entries.len() as u64 + 1;
        set.rebuild_hnsw();
    } else if decode_links && !read_hnsw_links {
        set.rebuild_hnsw();
    }
    Ok(set)
}

fn encode_vector_set(set: &VectorSetState) -> Bytes {
    let mut out = Vec::new();
    out.extend_from_slice(VECTOR_SET_PREFIX);
    let governed = set.entries.iter().any(|entry| entry.governance.is_some());
    let format = if governed {
        HNSW_GOVERNED_VECTOR_SET_FORMAT
    } else {
        HNSW_VECTOR_SET_FORMAT
    };
    out.extend_from_slice(&format.to_le_bytes());
    out.extend_from_slice(&(set.dim as u32).to_le_bytes());
    out.extend_from_slice(&set.quantization.tag().to_le_bytes());
    out.extend_from_slice(&(set.original_dim as u32).to_le_bytes());
    out.extend_from_slice(&(set.hnsw_m as u32).to_le_bytes());
    out.extend_from_slice(&(set.ef_construction as u32).to_le_bytes());
    out.extend_from_slice(&(set.max_level as u32).to_le_bytes());
    out.extend_from_slice(&set.next_uid.to_le_bytes());
    out.extend_from_slice(&(set.entries.len() as u32).to_le_bytes());
    for entry in &set.entries {
        out.extend_from_slice(&entry.uid.to_le_bytes());
        out.extend_from_slice(&(entry.level as u32).to_le_bytes());
        write_bytes(&mut out, &entry.element);
        out.extend_from_slice(&(entry.vector.len() as u32).to_le_bytes());
        for value in &entry.vector {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out.extend_from_slice(&(entry.attributes.is_some() as u32).to_le_bytes());
        if let Some(attributes) = &entry.attributes {
            write_bytes(&mut out, attributes);
        }
        if governed {
            out.extend_from_slice(&(entry.governance.is_some() as u32).to_le_bytes());
            if let Some(governance) = &entry.governance {
                write_bytes(&mut out, governance);
            }
        }
    }
    for entry in &set.entries {
        out.extend_from_slice(&(entry.links.len() as u32).to_le_bytes());
        for layer in &entry.links {
            out.extend_from_slice(&(layer.len() as u32).to_le_bytes());
            for uid in layer {
                out.extend_from_slice(&uid.to_le_bytes());
            }
        }
    }
    out
}

fn fp32_values(blob: &[u8]) -> Result<Vec<f64>, Frame> {
    if !blob.len().is_multiple_of(4) {
        return Err(error("ERR invalid FP32 vector length"));
    }
    Ok(blob
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()) as f64)
        .collect())
}

fn fp32_blob(values: &[f64]) -> Bytes {
    let mut out = Vec::with_capacity(values.len().saturating_mul(4));
    for value in values {
        out.extend_from_slice(&(*value as f32).to_le_bytes());
    }
    out
}

fn raw_vector_values_frame(vector: &[f64], quantization: Quantization) -> Frame {
    let norm = l2_norm(vector);
    match quantization {
        Quantization::NoQuant => Frame::Array(vec![
            Frame::SimpleString("fp32".to_string()),
            bulk(fp32_blob(vector)),
            Frame::SimpleString(format_number(norm)),
        ]),
        Quantization::Q8 => {
            let (blob, range) = q8_blob(vector);
            Frame::Array(vec![
                Frame::SimpleString("q8".to_string()),
                bulk(blob),
                Frame::SimpleString(format_number(norm)),
                Frame::SimpleString(format_number(range)),
            ])
        }
        Quantization::Bin => Frame::Array(vec![
            Frame::SimpleString("bin".to_string()),
            bulk(bin_blob(vector)),
            Frame::SimpleString(format_number(norm)),
        ]),
    }
}

fn q8_blob(values: &[f64]) -> (Bytes, f64) {
    let max_abs = values
        .iter()
        .map(|value| value.abs())
        .fold(0.0_f64, f64::max);
    let range = if max_abs == 0.0 { 1.0 } else { max_abs / 127.0 };
    let bytes = values
        .iter()
        .map(|value| {
            let quantized = (*value / range).round().clamp(-128.0, 127.0) as i8;
            quantized as u8
        })
        .collect();
    (bytes, range)
}

fn bin_blob(values: &[f64]) -> Bytes {
    let mut out = vec![0u8; values.len().div_ceil(8)];
    for (index, value) in values.iter().enumerate() {
        if *value >= 0.0 {
            out[index / 8] |= 1 << (index % 8);
        }
    }
    out
}

fn reduce_vector(values: &[f64], dim: usize) -> Vec<f64> {
    if values.len() == dim {
        return values.to_vec();
    }
    if values.is_empty() {
        return vec![0.0; dim];
    }
    (0..dim)
        .map(|target| {
            let start = target * values.len() / dim;
            let mut end = (target + 1) * values.len() / dim;
            if end <= start {
                end = start + 1;
            }
            let end = end.min(values.len());
            let slice = &values[start..end];
            slice.iter().sum::<f64>() / slice.len() as f64
        })
        .collect()
}

fn hnsw_level(element: &[u8]) -> usize {
    let hash = xxhash_rust::xxh3::xxh3_64(element);
    hnsw_level_from_hash(hash)
}

fn hnsw_level_from_uid(uid: u64) -> usize {
    hnsw_level_from_hash(xxhash_rust::xxh3::xxh3_64(&uid.to_le_bytes()))
}

fn hnsw_level_from_hash(mut hash: u64) -> usize {
    let mut level = 0usize;
    while level < MAX_HNSW_LEVEL && hash & 0b11 == 0 {
        level += 1;
        hash >>= 2;
    }
    level
}

fn cosine_similarity(left: &[f64], right: &[f64]) -> f64 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0;
    let mut left_norm = 0.0;
    let mut right_norm = 0.0;
    for (left, right) in left.iter().zip(right) {
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot / (left_norm.sqrt() * right_norm.sqrt())
    }
}

fn l2_norm(values: &[f64]) -> f64 {
    values.iter().map(|value| value * value).sum::<f64>().sqrt()
}

fn lex_in_range(element: &[u8], start: &[u8], end: &[u8]) -> bool {
    lex_lower_bound_ok(element, start) && lex_upper_bound_ok(element, end)
}

fn lex_lower_bound_ok(element: &[u8], start: &[u8]) -> bool {
    match start {
        b"-" => true,
        [b'[', rest @ ..] => element >= rest,
        [b'(', rest @ ..] => element > rest,
        _ => element >= start,
    }
}

fn lex_upper_bound_ok(element: &[u8], end: &[u8]) -> bool {
    match end {
        b"+" => true,
        [b'[', rest @ ..] => element <= rest,
        [b'(', rest @ ..] => element < rest,
        _ => element <= end,
    }
}

fn validate_attributes(raw: &[u8]) -> Result<(), Frame> {
    if raw.is_empty() {
        return Ok(());
    }
    let key = vector_attribute_validation_cache_key(raw);
    if let Some(valid) = vector_attribute_validation_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(key, raw))
    {
        return if valid {
            Ok(())
        } else {
            Err(error("ERR invalid vector set attribute JSON"))
        };
    }
    let valid = validate_json_object(raw);
    if let Ok(mut cache) = vector_attribute_validation_cache().lock() {
        cache.insert(key, raw, valid);
    }
    if valid {
        Ok(())
    } else {
        Err(error("ERR invalid vector set attribute JSON"))
    }
}

fn validate_json_object(raw: &[u8]) -> bool {
    struct ObjectVisitor;

    impl<'de> serde::de::Visitor<'de> for ObjectVisitor {
        type Value = ();

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a JSON object")
        }

        fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            while map
                .next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?
                .is_some()
            {}
            Ok(())
        }
    }

    let mut deserializer = serde_json::Deserializer::from_slice(raw);
    serde::de::Deserializer::deserialize_any(&mut deserializer, ObjectVisitor).is_ok()
        && deserializer.end().is_ok()
}

fn attributes_match(attributes: Option<&[u8]>, expression: &[u8]) -> bool {
    let Some(attributes) = attributes else {
        return false;
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(attributes) else {
        return false;
    };
    let Ok(expression) = std::str::from_utf8(expression) else {
        return false;
    };
    eval_filter_or(&value, expression)
}

#[derive(Debug)]
struct CompiledVectorFilter {
    field: String,
    operator: FilterOperator,
    expected: FilterExpected,
}

#[derive(Debug, Clone, Copy)]
enum FilterOperator {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug)]
enum FilterExpected {
    Bool(bool),
    Null,
    Number(f64),
    String(String),
    Invalid,
}

impl CompiledVectorFilter {
    fn parse(expression: &[u8]) -> Option<Self> {
        let expression = std::str::from_utf8(expression).ok()?.trim();
        if expression.contains("&&") || expression.contains("||") {
            return None;
        }
        let expression = expression.strip_prefix('.')?;
        let (field, operator, expected) = parse_filter_comparison(expression)?;
        Some(Self {
            field: field.to_string(),
            operator: FilterOperator::parse(operator)?,
            expected: FilterExpected::parse(expected.trim()),
        })
    }

    fn matches(&self, attributes: Option<&[u8]>) -> bool {
        let Some(attributes) = attributes else {
            return false;
        };
        if let Some(result) = self.matches_raw_attributes(attributes) {
            return result;
        }
        self.matches_parsed_attributes(attributes)
    }

    fn matches_raw_attributes(&self, attributes: &[u8]) -> Option<bool> {
        let raw = match find_top_level_json_field_raw(attributes, self.field.as_bytes()) {
            JsonFieldLookup::Found(raw) => raw,
            JsonFieldLookup::Missing => return Some(false),
            JsonFieldLookup::Unsupported => return None,
        };
        self.expected.compare_raw(raw, self.operator)
    }

    fn matches_parsed_attributes(&self, attributes: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(attributes) else {
            return false;
        };
        let Some(actual) = value.get(&self.field) else {
            return false;
        };
        match self.operator {
            FilterOperator::Eq => self.expected.equals(actual),
            FilterOperator::Ne => !self.expected.equals(actual),
            FilterOperator::Gt | FilterOperator::Ge | FilterOperator::Lt | FilterOperator::Le => {
                let Some(left) = actual.as_f64() else {
                    return false;
                };
                let Some(right) = self.expected.as_f64() else {
                    return false;
                };
                match self.operator {
                    FilterOperator::Gt => left > right,
                    FilterOperator::Ge => left >= right,
                    FilterOperator::Lt => left < right,
                    FilterOperator::Le => left <= right,
                    FilterOperator::Eq | FilterOperator::Ne => false,
                }
            }
        }
    }
}

impl FilterOperator {
    fn parse(operator: &str) -> Option<Self> {
        match operator {
            "==" => Some(Self::Eq),
            "!=" => Some(Self::Ne),
            ">" => Some(Self::Gt),
            ">=" => Some(Self::Ge),
            "<" => Some(Self::Lt),
            "<=" => Some(Self::Le),
            _ => None,
        }
    }
}

impl FilterExpected {
    fn parse(expected: &str) -> Self {
        if let Some(value) = parse_quoted_filter_value(expected) {
            return Self::String(value);
        }
        match expected {
            "true" => Self::Bool(true),
            "false" => Self::Bool(false),
            "null" => Self::Null,
            _ => expected
                .parse::<f64>()
                .map(Self::Number)
                .unwrap_or(Self::Invalid),
        }
    }

    fn equals(&self, actual: &serde_json::Value) -> bool {
        match self {
            Self::Bool(expected) => actual.as_bool() == Some(*expected),
            Self::Null => actual.is_null(),
            Self::Number(expected) => actual
                .as_f64()
                .map(|actual| (actual - expected).abs() < f64::EPSILON)
                .unwrap_or(false),
            Self::String(expected) => actual.as_str() == Some(expected),
            Self::Invalid => false,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(value) => Some(*value),
            Self::Bool(_) | Self::Null | Self::String(_) | Self::Invalid => None,
        }
    }

    fn compare_raw(&self, raw: &[u8], operator: FilterOperator) -> Option<bool> {
        match operator {
            FilterOperator::Eq => Some(self.raw_equals(raw)),
            FilterOperator::Ne => Some(!self.raw_equals(raw)),
            FilterOperator::Gt | FilterOperator::Ge | FilterOperator::Lt | FilterOperator::Le => {
                let left = parse_json_number_raw(raw)?;
                let right = self.as_f64()?;
                Some(match operator {
                    FilterOperator::Gt => left > right,
                    FilterOperator::Ge => left >= right,
                    FilterOperator::Lt => left < right,
                    FilterOperator::Le => left <= right,
                    FilterOperator::Eq | FilterOperator::Ne => false,
                })
            }
        }
    }

    fn raw_equals(&self, raw: &[u8]) -> bool {
        match self {
            Self::Bool(expected) => {
                raw == if *expected {
                    &b"true"[..]
                } else {
                    &b"false"[..]
                }
            }
            Self::Null => raw == b"null",
            Self::Number(expected) => parse_json_number_raw(raw)
                .map(|actual| (actual - expected).abs() < f64::EPSILON)
                .unwrap_or(false),
            Self::String(expected) => raw_json_string_equals(raw, expected),
            Self::Invalid => false,
        }
    }
}

enum JsonFieldLookup<'a> {
    Found(&'a [u8]),
    Missing,
    Unsupported,
}

fn find_top_level_json_field_raw<'a>(raw: &'a [u8], field: &[u8]) -> JsonFieldLookup<'a> {
    if !field
        .iter()
        .all(|byte| matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'-'))
    {
        return JsonFieldLookup::Unsupported;
    }

    let mut index = skip_json_ws(raw, 0);
    if raw.get(index) != Some(&b'{') {
        return JsonFieldLookup::Unsupported;
    }
    index += 1;

    loop {
        index = skip_json_ws(raw, index);
        match raw.get(index) {
            Some(b'}') => return JsonFieldLookup::Missing,
            Some(b'"') => {}
            _ => return JsonFieldLookup::Unsupported,
        }

        let (key, next) = match read_simple_json_string(raw, index) {
            Some(value) => value,
            None => return JsonFieldLookup::Unsupported,
        };
        index = skip_json_ws(raw, next);
        if raw.get(index) != Some(&b':') {
            return JsonFieldLookup::Unsupported;
        }
        index = skip_json_ws(raw, index + 1);
        let value_start = index;
        let Some(value_end) = skip_json_value(raw, index) else {
            return JsonFieldLookup::Unsupported;
        };
        if key == field {
            return JsonFieldLookup::Found(trim_json_raw(&raw[value_start..value_end]));
        }
        index = skip_json_ws(raw, value_end);
        match raw.get(index) {
            Some(b',') => index += 1,
            Some(b'}') => return JsonFieldLookup::Missing,
            _ => return JsonFieldLookup::Unsupported,
        }
    }
}

fn skip_json_ws(raw: &[u8], mut index: usize) -> usize {
    while raw
        .get(index)
        .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
    {
        index += 1;
    }
    index
}

fn trim_json_raw(mut raw: &[u8]) -> &[u8] {
    while raw
        .first()
        .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
    {
        raw = &raw[1..];
    }
    while raw
        .last()
        .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
    {
        raw = &raw[..raw.len() - 1];
    }
    raw
}

fn read_simple_json_string(raw: &[u8], start: usize) -> Option<(&[u8], usize)> {
    if raw.get(start) != Some(&b'"') {
        return None;
    }
    let mut index = start + 1;
    while let Some(byte) = raw.get(index) {
        match byte {
            b'\\' => return None,
            b'"' => return Some((&raw[start + 1..index], index + 1)),
            _ => index += 1,
        }
    }
    None
}

fn skip_json_string(raw: &[u8], start: usize) -> Option<usize> {
    if raw.get(start) != Some(&b'"') {
        return None;
    }
    let mut index = start + 1;
    while let Some(byte) = raw.get(index) {
        match byte {
            b'\\' => index = index.checked_add(2)?,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

fn skip_json_value(raw: &[u8], start: usize) -> Option<usize> {
    let index = skip_json_ws(raw, start);
    match raw.get(index)? {
        b'"' => skip_json_string(raw, index),
        b'{' | b'[' => skip_json_compound(raw, index),
        b't' if raw.get(index..index + 4) == Some(b"true") => Some(index + 4),
        b'f' if raw.get(index..index + 5) == Some(b"false") => Some(index + 5),
        b'n' if raw.get(index..index + 4) == Some(b"null") => Some(index + 4),
        b'-' | b'0'..=b'9' => {
            let mut end = index + 1;
            while raw.get(end).is_some_and(|byte| {
                !matches!(byte, b' ' | b'\n' | b'\r' | b'\t' | b',' | b'}' | b']')
            }) {
                end += 1;
            }
            Some(end)
        }
        _ => None,
    }
}

fn skip_json_compound(raw: &[u8], start: usize) -> Option<usize> {
    let mut stack = Vec::new();
    let mut index = start;
    loop {
        match raw.get(index)? {
            b'"' => index = skip_json_string(raw, index)?,
            b'{' => {
                stack.push(b'}');
                index += 1;
            }
            b'[' => {
                stack.push(b']');
                index += 1;
            }
            b'}' | b']' => {
                if stack.pop()? != raw[index] {
                    return None;
                }
                index += 1;
                if stack.is_empty() {
                    return Some(index);
                }
            }
            _ => index += 1,
        }
    }
}

fn parse_json_number_raw(raw: &[u8]) -> Option<f64> {
    std::str::from_utf8(trim_json_raw(raw)).ok()?.parse().ok()
}

fn raw_json_string_equals(raw: &[u8], expected: &str) -> bool {
    let raw = trim_json_raw(raw);
    match raw {
        [b'"', inner @ .., b'"'] if !inner.contains(&b'\\') => inner == expected.as_bytes(),
        [b'"', .., b'"'] => serde_json::from_slice::<String>(raw)
            .map(|actual| actual == expected)
            .unwrap_or(false),
        _ => false,
    }
}

fn eval_filter_or(value: &serde_json::Value, expression: &str) -> bool {
    split_filter(expression, "||")
        .into_iter()
        .any(|part| eval_filter_and(value, part.trim()))
}

fn eval_filter_and(value: &serde_json::Value, expression: &str) -> bool {
    split_filter(expression, "&&")
        .into_iter()
        .all(|part| eval_filter_comparison(value, part.trim()))
}

fn eval_filter_comparison(value: &serde_json::Value, expression: &str) -> bool {
    let Some(expression) = expression.strip_prefix('.') else {
        return false;
    };
    let Some((field, operator, expected)) = parse_filter_comparison(expression) else {
        return false;
    };
    let Some(actual) = value.get(field) else {
        return false;
    };
    compare_json_value(actual, operator, expected.trim())
}

fn parse_filter_comparison(expression: &str) -> Option<(&str, &str, &str)> {
    for operator in ["==", "!=", ">=", "<=", ">", "<"] {
        if let Some(index) = find_operator_outside_quotes(expression, operator) {
            return Some((
                expression[..index].trim(),
                operator,
                &expression[index + operator.len()..],
            ));
        }
    }
    None
}

fn compare_json_value(actual: &serde_json::Value, operator: &str, expected: &str) -> bool {
    let expected_string = parse_quoted_filter_value(expected);
    match operator {
        "==" => json_equals(actual, expected, expected_string.as_deref()),
        "!=" => !json_equals(actual, expected, expected_string.as_deref()),
        ">" | ">=" | "<" | "<=" => {
            let Some(left) = actual.as_f64() else {
                return false;
            };
            let Ok(right) = expected.parse::<f64>() else {
                return false;
            };
            match operator {
                ">" => left > right,
                ">=" => left >= right,
                "<" => left < right,
                "<=" => left <= right,
                _ => false,
            }
        }
        _ => false,
    }
}

fn json_equals(actual: &serde_json::Value, expected: &str, expected_string: Option<&str>) -> bool {
    if let Some(expected) = expected_string {
        return actual.as_str() == Some(expected);
    }
    match expected {
        "true" => actual.as_bool() == Some(true),
        "false" => actual.as_bool() == Some(false),
        "null" => actual.is_null(),
        _ => expected
            .parse::<f64>()
            .ok()
            .and_then(|expected| {
                actual
                    .as_f64()
                    .map(|actual| (actual - expected).abs() < f64::EPSILON)
            })
            .unwrap_or(false),
    }
}

fn parse_quoted_filter_value(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.len() < 2 || !trimmed.starts_with('"') || !trimmed.ends_with('"') {
        return None;
    }
    serde_json::from_str::<String>(trimmed).ok()
}

fn split_filter<'a>(expression: &'a str, delimiter: &str) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_string = false;
    let bytes = expression.as_bytes();
    let delimiter_bytes = delimiter.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'"' if index == 0 || bytes[index - 1] != b'\\' => {
                in_string = !in_string;
                index += 1;
            }
            _ if !in_string && bytes[index..].starts_with(delimiter_bytes) => {
                parts.push(&expression[start..index]);
                index += delimiter_bytes.len();
                start = index;
            }
            _ => index += 1,
        }
    }
    parts.push(&expression[start..]);
    parts
}

fn find_operator_outside_quotes(expression: &str, operator: &str) -> Option<usize> {
    let mut in_string = false;
    let bytes = expression.as_bytes();
    let operator_bytes = operator.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'"' if index == 0 || bytes[index - 1] != b'\\' => {
                in_string = !in_string;
                index += 1;
            }
            _ if !in_string && bytes[index..].starts_with(operator_bytes) => {
                return Some(index);
            }
            _ => index += 1,
        }
    }
    None
}

impl Quantization {
    fn tag(self) -> u32 {
        match self {
            Self::NoQuant => 0,
            Self::Q8 => 1,
            Self::Bin => 2,
        }
    }

    fn from_tag(tag: u32) -> Option<Self> {
        match tag {
            0 => Some(Self::NoQuant),
            1 => Some(Self::Q8),
            2 => Some(Self::Bin),
            _ => None,
        }
    }

    fn vinfo_name(self) -> &'static str {
        match self {
            Self::NoQuant => "fp32",
            Self::Q8 => "int8",
            Self::Bin => "bin",
        }
    }
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 && value.is_finite() {
        (value as i64).to_string()
    } else {
        value.to_string()
    }
}

fn read_u32(raw: &mut &[u8]) -> Result<u32, ()> {
    if raw.len() < 4 {
        return Err(());
    }
    let (head, tail) = raw.split_at(4);
    *raw = tail;
    Ok(u32::from_le_bytes(head.try_into().map_err(|_| ())?))
}

fn read_u64(raw: &mut &[u8]) -> Result<u64, ()> {
    if raw.len() < 8 {
        return Err(());
    }
    let (head, tail) = raw.split_at(8);
    *raw = tail;
    Ok(u64::from_le_bytes(head.try_into().map_err(|_| ())?))
}

fn read_f64(raw: &mut &[u8]) -> Result<f64, ()> {
    if raw.len() < 8 {
        return Err(());
    }
    let (head, tail) = raw.split_at(8);
    *raw = tail;
    Ok(f64::from_le_bytes(head.try_into().map_err(|_| ())?))
}

fn read_f64_slice(raw: &[u8]) -> Result<Vec<f64>, ()> {
    if !raw.len().is_multiple_of(8) {
        return Err(());
    }
    raw.chunks_exact(8)
        .map(|chunk| Ok(f64::from_le_bytes(chunk.try_into().map_err(|_| ())?)))
        .collect()
}

fn read_bytes_slice<'a>(raw: &mut &'a [u8]) -> Result<&'a [u8], ()> {
    let len = read_u32(raw)? as usize;
    if raw.len() < len {
        return Err(());
    }
    let (head, tail) = raw.split_at(len);
    *raw = tail;
    Ok(head)
}

fn read_bytes(raw: &mut &[u8]) -> Result<Bytes, ()> {
    let len = read_u32(raw)? as usize;
    if raw.len() < len {
        return Err(());
    }
    let (head, tail) = raw.split_at(len);
    *raw = tail;
    Ok(head.to_vec())
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn governed_vector_format_round_trips_without_changing_ungoverned_format() {
        let mut set = VectorSetState {
            dim: 2,
            original_dim: 2,
            quantization: Quantization::NoQuant,
            hnsw_m: DEFAULT_HNSW_M,
            ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
            max_level: 0,
            next_uid: 2,
            entries: vec![VectorEntry {
                uid: 1,
                level: 0,
                element: b"doc-a".to_vec(),
                vector: vec![1.0, 0.0],
                attributes: None,
                governance: None,
                links: vec![Vec::new()],
            }],
        };

        let ungoverned = encode_vector_set(&set);
        assert_eq!(
            u32::from_le_bytes(
                ungoverned[VECTOR_SET_PREFIX.len()..VECTOR_SET_PREFIX.len() + 4]
                    .try_into()
                    .unwrap()
            ),
            HNSW_VECTOR_SET_FORMAT
        );

        set.entries[0].governance = Some(b"tenant=acme".to_vec());
        let governed = encode_vector_set(&set);
        assert_eq!(
            u32::from_le_bytes(
                governed[VECTOR_SET_PREFIX.len()..VECTOR_SET_PREFIX.len() + 4]
                    .try_into()
                    .unwrap()
            ),
            HNSW_GOVERNED_VECTOR_SET_FORMAT
        );
        let decoded = decode_vector_set(Some(&governed)).unwrap();
        assert_eq!(
            decoded.entries[0].governance.as_deref(),
            Some(b"tenant=acme".as_slice())
        );
    }

    #[test]
    fn exact_vector_scan_uses_parallel_shard_partition_path() {
        let mut entries = Vec::with_capacity(VECTOR_SCAN_PARALLEL_MIN + 1);
        entries.push(VectorEntry {
            uid: 1,
            level: 0,
            element: b"top".to_vec(),
            vector: vec![1.0, 0.0],
            attributes: Some(br#"{"keep":true}"#.to_vec()),
            governance: Some(b"tenant=acme".to_vec()),
            links: Vec::new(),
        });
        for index in 1..=VECTOR_SCAN_PARALLEL_MIN {
            entries.push(VectorEntry {
                uid: index as u64 + 1,
                level: 0,
                element: format!("member:{index:04}").into_bytes(),
                vector: vec![0.0, 1.0],
                attributes: Some(br#"{"keep":false}"#.to_vec()),
                governance: None,
                links: Vec::new(),
            });
        }
        let set = VectorSetState {
            dim: 2,
            original_dim: 2,
            quantization: Quantization::NoQuant,
            hnsw_m: DEFAULT_HNSW_M,
            ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
            max_level: 0,
            next_uid: entries.len() as u64 + 1,
            entries,
        };

        let scored = exact_vector_scores(&set, &[1.0, 0.0], None, 8);
        assert_eq!(scored.len(), VECTOR_SCAN_PARALLEL_MIN + 1);
        assert_eq!(scored[0].0.element, b"top".to_vec());
        assert_eq!(scored[0].1, 1.0);

        let filtered = exact_vector_scores(&set, &[1.0, 0.0], Some(b".keep == true"), 8);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0.element, b"top".to_vec());
    }

    #[test]
    fn compiled_vector_filter_matches_raw_top_level_attributes() {
        let numeric = CompiledVectorFilter::parse(b".group == 1").unwrap();
        assert_eq!(
            numeric.matches_raw_attributes(br#"{"keep":true,"group":1,"nested":{"group":2}}"#),
            Some(true)
        );
        assert_eq!(
            numeric.matches_raw_attributes(br#"{"group":2,"keep":true}"#),
            Some(false)
        );

        let boolean = CompiledVectorFilter::parse(b".keep == true").unwrap();
        assert_eq!(
            boolean.matches_raw_attributes(br#"{"group":1,"keep":true}"#),
            Some(true)
        );

        let string = CompiledVectorFilter::parse(br#".name == "alpha""#).unwrap();
        assert_eq!(
            string.matches_raw_attributes(br#"{"name":"alpha","group":1}"#),
            Some(true)
        );

        let escaped_key = CompiledVectorFilter::parse(b".group == 1").unwrap();
        assert_eq!(
            escaped_key.matches_raw_attributes(br#"{"gr\u006fup":1}"#),
            None
        );
        assert!(escaped_key.matches(Some(br#"{"gr\u006fup":1}"#)));
    }
}
