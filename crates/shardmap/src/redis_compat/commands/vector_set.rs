use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
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
const MAX_VECTOR_DIMENSIONS: usize = 65_536;
const MAX_VECTOR_SET_ENTRIES: usize = 1_000_000;
const MAX_HNSW_M: usize = 1_024;
const MAX_HNSW_EF_CONSTRUCTION: usize = 1_000_000;
const MAX_VECTOR_RESPONSE_ITEMS: usize = 65_536;
const MAX_VECTOR_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

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
    AttributesAndGovernance,
    Governance,
    VectorAndGovernance,
    VectorAttributesAndGovernance,
}

impl VectorLookupProjection {
    #[inline(always)]
    fn include_vector(self) -> bool {
        matches!(
            self,
            Self::VectorAndGovernance | Self::VectorAttributesAndGovernance
        )
    }

    #[inline(always)]
    fn include_attributes(self) -> bool {
        matches!(
            self,
            Self::AttributesAndGovernance | Self::VectorAttributesAndGovernance
        )
    }

    #[inline(always)]
    fn include_governance(self) -> bool {
        matches!(
            self,
            Self::Governance
                | Self::AttributesAndGovernance
                | Self::VectorAndGovernance
                | Self::VectorAttributesAndGovernance
        )
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
    retained_bytes: usize,
    _raw: bytes::Bytes,
    set: Arc<VectorSetState>,
}

#[derive(Default)]
struct VectorDecodeCache {
    entries: Vec<VectorDecodeCacheEntry>,
    retained_bytes: usize,
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
        let retained_bytes = raw.len().saturating_add(decoded_vector_set_bytes(&set));
        if retained_bytes > VECTOR_DECODE_CACHE_MAX_BYTES {
            return;
        }
        self.retained_bytes = self.retained_bytes.saturating_add(retained_bytes);
        self.entries.push(VectorDecodeCacheEntry {
            key,
            retained_bytes,
            _raw: raw,
            set,
        });
        while self.entries.len() > VECTOR_DECODE_CACHE_MAX_ENTRIES
            || self.retained_bytes > VECTOR_DECODE_CACHE_MAX_BYTES
        {
            if self.entries.is_empty() {
                self.retained_bytes = 0;
                break;
            }
            let removed = self.entries.remove(0);
            self.retained_bytes = self.retained_bytes.saturating_sub(removed.retained_bytes);
        }
    }
}

fn decoded_vector_set_bytes(set: &VectorSetState) -> usize {
    let mut bytes = std::mem::size_of::<VectorSetState>().saturating_add(
        set.entries
            .capacity()
            .saturating_mul(std::mem::size_of::<VectorEntry>()),
    );
    for entry in &set.entries {
        bytes = bytes
            .saturating_add(entry.element.capacity())
            .saturating_add(
                entry
                    .vector
                    .capacity()
                    .saturating_mul(std::mem::size_of::<f64>()),
            )
            .saturating_add(entry.attributes.as_ref().map_or(0, Vec::capacity))
            .saturating_add(entry.governance.as_ref().map_or(0, Vec::capacity))
            .saturating_add(
                entry
                    .links
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<u64>>()),
            );
        for layer in &entry.links {
            bytes =
                bytes.saturating_add(layer.capacity().saturating_mul(std::mem::size_of::<u64>()));
        }
    }
    bytes
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
                        if !governance_mutation_authorized(
                            snapshot.governance.as_deref(),
                            parsed.expected_governance.as_deref(),
                        ) {
                            return error("NOPERM vector governance authorization failed");
                        }
                        let vector_changed =
                            snapshot.vector.as_deref() != Some(parsed.vector.as_slice());
                        let attributes_changed =
                            parsed.attributes.as_deref().is_some_and(|attributes| {
                                Some(attributes) != snapshot.attributes.as_deref()
                            });
                        let governance_changed = parsed.clear_governance
                            || parsed.governance.as_deref().is_some_and(|governance| {
                                Some(governance) != snapshot.governance.as_deref()
                            });
                        if !vector_changed && !attributes_changed && !governance_changed {
                            return int(0);
                        }
                    }
                    Ok(VectorEntryLookup::MissingElement | VectorEntryLookup::MissingKey) => {
                        if parsed.expected_governance.is_some() {
                            return error("NOPERM vector governance authorization failed");
                        }
                    }
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
                    if !governance_mutation_authorized(
                        entry.governance.as_deref(),
                        parsed.expected_governance.as_deref(),
                    ) {
                        return VectorWriteResult::Unchanged(error(
                            "NOPERM vector governance authorization failed",
                        ));
                    }
                    let vector_changed = entry.vector != parsed.vector;
                    let attributes_changed = parsed
                        .attributes
                        .as_deref()
                        .is_some_and(|attributes| Some(attributes) != entry.attributes.as_deref());
                    let governance_changed = parsed.clear_governance
                        || parsed.governance.as_deref().is_some_and(|governance| {
                            Some(governance) != entry.governance.as_deref()
                        });
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
                        entry.governance = if parsed.clear_governance {
                            None
                        } else {
                            parsed.governance
                        };
                    }
                    if vector_changed {
                        set.rebuild_hnsw();
                    }
                    VectorWriteResult::Changed(int(0))
                }
                None => {
                    if parsed.expected_governance.is_some() {
                        return VectorWriteResult::Unchanged(error(
                            "NOPERM vector governance authorization failed",
                        ));
                    }
                    let uid = set.next_uid;
                    let Some(next_uid) = uid.checked_add(1) else {
                        return VectorWriteResult::Unchanged(error(
                            "ERR vector UID space exhausted",
                        ));
                    };
                    if uid == 0 {
                        return VectorWriteResult::Unchanged(error(
                            "ERR vector UID space exhausted",
                        ));
                    }
                    set.next_uid = next_uid;
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
        let (raw, expected_governance) = match parse_vector_read_options(options, true) {
            Ok(parsed) => parsed,
            Err(frame) => return frame,
        };
        let metadata = match vector_read_metadata(store, key) {
            Ok(Some(metadata)) => metadata,
            Ok(None) => return Frame::Null,
            Err(frame) => return frame,
        };
        match vector_lookup_entry(
            store,
            key,
            element,
            VectorLookupProjection::VectorAndGovernance,
        ) {
            Ok(VectorEntryLookup::Found(snapshot))
                if governance_read_authorized(
                    snapshot.governance.as_deref(),
                    expected_governance.as_deref(),
                ) =>
            {
                match snapshot.vector {
                    Some(vector) if raw => raw_vector_values_frame(&vector, metadata.quantization),
                    Some(vector) => array_bulk(
                        vector
                            .iter()
                            .map(|value| format_number(*value).into_bytes())
                            .collect(),
                    ),
                    None => Frame::Null,
                }
            }
            Ok(VectorEntryLookup::Found(_)) => Frame::Null,
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => Frame::Null,
            Err(frame) => frame,
        }
    }
}

impl crate::commands::redis::RedisCommand for VGetAttr {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element, options @ ..] = args else {
            return wrong_arity("VGETATTR");
        };
        let expected_governance = match parse_governance_guard(options) {
            Ok(expected) => expected,
            Err(frame) => return frame,
        };
        match vector_lookup_entry(
            store,
            key,
            element,
            VectorLookupProjection::AttributesAndGovernance,
        ) {
            Ok(VectorEntryLookup::Found(snapshot))
                if governance_read_authorized(
                    snapshot.governance.as_deref(),
                    expected_governance.as_deref(),
                ) =>
            {
                snapshot.attributes.map_or(Frame::Null, bulk)
            }
            Ok(VectorEntryLookup::Found(_)) => Frame::Null,
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => Frame::Null,
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_frame(out, &Self::execute(store, args));
    }
}

impl crate::commands::redis::RedisCommand for VSetAttr {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element, attributes, options @ ..] = args else {
            return wrong_arity("VSETATTR");
        };
        let expected_governance = match parse_governance_guard(options) {
            Ok(expected) => expected,
            Err(frame) => return frame,
        };
        let next_attributes = (!attributes.is_empty()).then(|| (*attributes).to_vec());
        match vector_lookup_entry(
            store,
            key,
            element,
            VectorLookupProjection::AttributesAndGovernance,
        ) {
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => {
                if let Err(frame) = validate_attributes(attributes) {
                    return frame;
                }
                return int(0);
            }
            Ok(VectorEntryLookup::Found(snapshot)) => {
                if !governance_mutation_authorized(
                    snapshot.governance.as_deref(),
                    expected_governance.as_deref(),
                ) {
                    return int(0);
                }
                if snapshot.attributes == next_attributes {
                    return int(1);
                }
            }
            Err(frame) => return frame,
        }
        if let Err(frame) = validate_attributes(attributes) {
            return frame;
        }
        vector_write_existing_maybe(store, key, |set| {
            let Some(entry) = set.entry_mut(element) else {
                return VectorWriteResult::Unchanged(int(0));
            };
            if !governance_mutation_authorized(
                entry.governance.as_deref(),
                expected_governance.as_deref(),
            ) {
                return VectorWriteResult::Unchanged(int(0));
            }
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
        write_frame(out, &Self::execute(store, args));
    }
}

impl crate::commands::redis::RedisCommand for VIsMember {
    #[cfg(feature = "server")]
    vector_write_fast_from_resp!();

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element, options @ ..] = args else {
            return wrong_arity("VISMEMBER");
        };
        let expected_governance = match parse_governance_guard(options) {
            Ok(expected) => expected,
            Err(frame) => return frame,
        };
        match vector_lookup_entry(store, key, element, VectorLookupProjection::Governance) {
            Ok(VectorEntryLookup::Found(snapshot))
                if governance_read_authorized(
                    snapshot.governance.as_deref(),
                    expected_governance.as_deref(),
                ) =>
            {
                int(1)
            }
            Ok(VectorEntryLookup::Found(_)) => int(0),
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => int(0),
            Err(frame) => frame,
        }
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_frame(out, &Self::execute(store, args));
    }
}

impl crate::commands::redis::RedisCommand for VRem {
    #[cfg(feature = "server")]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_fast_frame(out, &Self::execute(store, args));
    }

    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, element, options @ ..] = args else {
            return wrong_arity("VREM");
        };
        let expected_governance = match parse_governance_guard(options) {
            Ok(expected) => expected,
            Err(frame) => return frame,
        };
        match vector_lookup_entry(store, key, element, VectorLookupProjection::Governance) {
            Ok(VectorEntryLookup::MissingKey | VectorEntryLookup::MissingElement) => {
                return int(0);
            }
            Ok(VectorEntryLookup::Found(snapshot)) => {
                if !governance_mutation_authorized(
                    snapshot.governance.as_deref(),
                    expected_governance.as_deref(),
                ) {
                    return int(0);
                }
            }
            Err(frame) => return frame,
        }
        vector_write_existing_maybe(store, key, |set| {
            let Some(index) = set
                .entries
                .iter()
                .position(|entry| entry.element.as_slice() == *element)
            else {
                return VectorWriteResult::Unchanged(int(0));
            };
            if !governance_mutation_authorized(
                set.entries[index].governance.as_deref(),
                expected_governance.as_deref(),
            ) {
                return VectorWriteResult::Unchanged(int(0));
            }
            set.entries.remove(index);
            set.rebuild_hnsw();
            VectorWriteResult::Changed(int(1))
        })
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_frame(out, &Self::execute(store, args));
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
        let mut with_scores = false;
        let mut allowed_governance = Vec::new();
        let mut index = 0;
        while index < options.len() {
            if eq_ignore_ascii_case(options[index], b"WITHSCORES") {
                with_scores = true;
                index += 1;
            } else if eq_ignore_ascii_case(options[index], b"GOVERNANCE") {
                let Some(raw) = options.get(index + 1) else {
                    return error("ERR syntax error");
                };
                if raw.len() > MAX_VECTOR_GOVERNANCE_BYTES {
                    return error("ERR vector governance metadata is too large");
                }
                allowed_governance.push((*raw).to_vec());
                index += 2;
            } else {
                return error("ERR syntax error");
            }
        }
        match vector_read_cached(store, key, VectorDecodeMode::Full) {
            Ok(Some(set)) => {
                let Some(source) = set.entry(element) else {
                    return Frame::Null;
                };
                if !governance_visible(source.governance.as_deref(), &allowed_governance) {
                    return Frame::Null;
                }
                let levels = source
                    .links
                    .iter()
                    .map(|links| {
                        let mut level = Vec::new();
                        for uid in links {
                            let Some(neighbor) = set.entry_by_uid(*uid) else {
                                continue;
                            };
                            if !governance_visible(
                                neighbor.governance.as_deref(),
                                &allowed_governance,
                            ) {
                                continue;
                            }
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
        let [key, rest @ ..] = args else {
            return wrong_arity("VRANDMEMBER");
        };
        let (count, options) = match rest {
            [] => (None, &[][..]),
            [token, tail @ ..] if !eq_ignore_ascii_case(token, b"GOVERNANCE") => {
                let count = match parse_i64(token) {
                    Ok(value) => value,
                    Err(()) => return error("ERR value is not an integer or out of range"),
                };
                (Some(count), tail)
            }
            options => (None, options),
        };
        let allowed_governance = match parse_allowed_governance(options) {
            Ok(allowed) => allowed,
            Err(frame) => return frame,
        };
        if count.is_some_and(|count| count.unsigned_abs() as usize > MAX_VECTOR_SET_ENTRIES) {
            return error("ERR count is out of range");
        }
        if count.is_none_or(|count| count >= 0) {
            let limit = count.map_or(1, |count| count as usize);
            return match vector_read_prefix_elements(store, key, limit, &allowed_governance) {
                Ok(Some(elements)) => match count {
                    None => elements.first().cloned().map_or(Frame::Null, bulk),
                    Some(_) => array_bulk(elements),
                },
                Ok(None) if count.is_some() => Frame::Array(Vec::new()),
                Ok(None) => Frame::Null,
                Err(frame) => frame,
            };
        }
        match vector_read_authorized_elements(store, key, &allowed_governance) {
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
        let [key, start, end, rest @ ..] = args else {
            return wrong_arity("VRANGE");
        };
        let (count, options) = match rest {
            [] => (None, &[][..]),
            [token, tail @ ..] if !eq_ignore_ascii_case(token, b"GOVERNANCE") => {
                let count = match parse_i64(token) {
                    Ok(value) => value,
                    Err(()) => return error("ERR value is not an integer or out of range"),
                };
                (Some(count), tail)
            }
            options => (None, options),
        };
        let allowed_governance = match parse_allowed_governance(options) {
            Ok(allowed) => allowed,
            Err(frame) => return frame,
        };
        if let Some(count) = count
            && count >= 0
        {
            return match vector_read_lex_range(
                store,
                key,
                start,
                end,
                count as usize,
                &allowed_governance,
            ) {
                Ok(Some(elements)) => array_bulk(elements),
                Ok(None) => Frame::Array(Vec::new()),
                Err(frame) => frame,
            };
        }
        match vector_read_authorized_elements(store, key, &allowed_governance) {
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
        write_frame(out, &Self::execute(store, args));
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
                &parsed.allowed_governance,
                store.shard_count(),
            )
        } else {
            hnsw_search(
                &set,
                &parsed.vector,
                parsed.count,
                parsed.ef_search.unwrap_or(DEFAULT_HNSW_EF_SEARCH),
                &parsed.allowed_governance,
            )
        };
        let mut scored = scored;
        scored.truncate(parsed.count);
        if vsim_response_bytes(&scored, &parsed)
            .is_none_or(|bytes| bytes > MAX_VECTOR_RESPONSE_BYTES)
        {
            return error("ERR VSIM response exceeds the server byte limit");
        }
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
    expected_governance: Option<Bytes>,
    clear_governance: bool,
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
    allowed_governance: Vec<Bytes>,
    filter: Option<Bytes>,
    ef_search: Option<usize>,
    truth: bool,
}

fn governance_mutation_authorized(stored: Option<&[u8]>, expected: Option<&[u8]>) -> bool {
    match stored {
        Some(stored) => expected == Some(stored),
        None => expected.is_none(),
    }
}

fn governance_read_authorized(stored: Option<&[u8]>, expected: Option<&[u8]>) -> bool {
    stored.is_none() || expected == stored
}

fn governance_visible(stored: Option<&[u8]>, allowed: &[Bytes]) -> bool {
    stored.is_none_or(|stored| {
        allowed
            .iter()
            .any(|candidate| candidate.as_slice() == stored)
    })
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
                VectorWriteResult::Changed(frame) => {
                    let value = encode_vector_set(&set);
                    if !store.vector_mutation_is_replicable(key, &value) {
                        return Err(error("ERR vector state exceeds replication frame limit"));
                    }
                    Ok(((frame, true), value))
                }
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
        crate::storage::RedisStringLookup::BackendError => {
            Err(error("ERR object overflow read failed"))
        }
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
        crate::storage::RedisStringLookup::BackendError => {
            Err(error("ERR object overflow read failed"))
        }
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
        crate::storage::RedisStringLookup::BackendError => {
            Err(error("ERR object overflow read failed"))
        }
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

fn vector_read_authorized_elements(
    store: &EmbeddedStore,
    key: &[u8],
    allowed_governance: &[Bytes],
) -> Result<Option<Vec<Bytes>>, Frame> {
    let set = vector_read_cached(store, key, VectorDecodeMode::EntriesOnly)?;
    Ok(set.map(|set| {
        set.entries
            .iter()
            .filter(|entry| governance_visible(entry.governance.as_deref(), allowed_governance))
            .map(|entry| entry.element.clone())
            .collect()
    }))
}

fn vector_read_prefix_elements(
    store: &EmbeddedStore,
    key: &[u8],
    limit: usize,
    allowed_governance: &[Bytes],
) -> Result<Option<Vec<Bytes>>, Frame> {
    let mut decoded = Ok(None);
    match store.get_raw_vector_value_into(key, |bytes| {
        decoded = if bytes.starts_with(VECTOR_SET_PREFIX) {
            match hnsw_collect_prefix_elements(bytes.as_ref(), limit, allowed_governance) {
                Ok(elements) => Ok(Some(elements)),
                Err(()) => decode_vector_set(Some(bytes.as_ref()))
                    .map(|set| {
                        Some(
                            set.entries
                                .into_iter()
                                .filter(|entry| {
                                    governance_visible(
                                        entry.governance.as_deref(),
                                        allowed_governance,
                                    )
                                })
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
        crate::storage::RedisStringLookup::BackendError => {
            Err(error("ERR object overflow read failed"))
        }
    }
}

fn vector_read_lex_range(
    store: &EmbeddedStore,
    key: &[u8],
    start: &[u8],
    end: &[u8],
    limit: usize,
    allowed_governance: &[Bytes],
) -> Result<Option<Vec<Bytes>>, Frame> {
    let mut decoded = Ok(None);
    match store.get_raw_vector_value_into(key, |bytes| {
        decoded = if bytes.starts_with(VECTOR_SET_PREFIX) {
            let format_offset = VECTOR_SET_PREFIX.len();
            let governed = bytes
                .get(format_offset..format_offset + 4)
                .and_then(|raw| raw.try_into().ok())
                .map(u32::from_le_bytes)
                == Some(HNSW_GOVERNED_VECTOR_SET_FORMAT);
            if governed {
                hnsw_collect_lex_range(bytes.as_ref(), start, end, limit, allowed_governance)
                    .map(Some)
                    .map_err(|_| wrongtype())
            } else {
                cached_vector_lex_range(bytes, start, end, limit)
                    .map(Some)
                    .map_err(|_| wrongtype())
            }
        } else {
            Err(wrongtype())
        };
    }) {
        crate::storage::RedisStringLookup::Hit => decoded,
        crate::storage::RedisStringLookup::Miss => Ok(None),
        crate::storage::RedisStringLookup::WrongType => Err(wrongtype()),
        crate::storage::RedisStringLookup::BackendError => {
            Err(error("ERR object overflow read failed"))
        }
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
        if dim > MAX_VECTOR_DIMENSIONS {
            return Err(error("ERR vector dimension is out of range"));
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
    let mut expected_governance = None;
    let mut clear_governance = false;
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
            token if eq_ignore_ascii_case(token, b"IFGOVERNANCE") => {
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                if raw.len() > MAX_VECTOR_GOVERNANCE_BYTES {
                    return Err(error("ERR vector governance metadata is too large"));
                }
                expected_governance = Some((*raw).to_vec());
                index += 2;
            }
            token if eq_ignore_ascii_case(token, b"CLEARGOVERNANCE") => {
                clear_governance = true;
                index += 1;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    if governance.is_some() && clear_governance {
        return Err(error(
            "ERR GOVERNANCE and CLEARGOVERNANCE are mutually exclusive",
        ));
    }
    if clear_governance && expected_governance.is_none() {
        return Err(error("ERR CLEARGOVERNANCE requires IFGOVERNANCE"));
    }
    if hnsw_m.is_some_and(|value| value > MAX_HNSW_M) {
        return Err(error("ERR HNSW M is out of range"));
    }
    if ef_construction.is_some_and(|value| value > MAX_HNSW_EF_CONSTRUCTION) {
        return Err(error("ERR HNSW EF is out of range"));
    }
    Ok(VAddArgs {
        element: (*element).to_vec(),
        vector,
        attributes,
        governance,
        expected_governance,
        clear_governance,
        quantization,
        reduce_dim,
        hnsw_m,
        ef_construction,
    })
}

fn parse_governance_guard(options: &[&[u8]]) -> Result<Option<Bytes>, Frame> {
    match options {
        [] => Ok(None),
        [token, governance] if eq_ignore_ascii_case(token, b"GOVERNANCE") => {
            if governance.len() > MAX_VECTOR_GOVERNANCE_BYTES {
                return Err(error("ERR vector governance metadata is too large"));
            }
            Ok(Some((*governance).to_vec()))
        }
        _ => Err(error("ERR syntax error")),
    }
}

fn parse_allowed_governance(options: &[&[u8]]) -> Result<Vec<Bytes>, Frame> {
    let mut allowed = Vec::new();
    let mut index = 0;
    while index < options.len() {
        if !eq_ignore_ascii_case(options[index], b"GOVERNANCE") {
            return Err(error("ERR syntax error"));
        }
        let Some(metadata) = options.get(index + 1) else {
            return Err(error("ERR syntax error"));
        };
        if metadata.len() > MAX_VECTOR_GOVERNANCE_BYTES {
            return Err(error("ERR vector governance metadata is too large"));
        }
        allowed.push((*metadata).to_vec());
        index += 2;
    }
    Ok(allowed)
}

fn parse_vector_read_options(
    options: &[&[u8]],
    allow_raw: bool,
) -> Result<(bool, Option<Bytes>), Frame> {
    let mut raw = false;
    let mut governance = None;
    let mut index = 0;
    while index < options.len() {
        if allow_raw && eq_ignore_ascii_case(options[index], b"RAW") {
            raw = true;
            index += 1;
        } else if eq_ignore_ascii_case(options[index], b"GOVERNANCE") {
            let Some(metadata) = options.get(index + 1) else {
                return Err(error("ERR syntax error"));
            };
            if metadata.len() > MAX_VECTOR_GOVERNANCE_BYTES {
                return Err(error("ERR vector governance metadata is too large"));
            }
            governance = Some((*metadata).to_vec());
            index += 2;
        } else {
            return Err(error("ERR syntax error"));
        }
    }
    Ok((raw, governance))
}

fn parse_vsim_args(args: &[&[u8]], set: &VectorSetState) -> Result<VSimArgs, Frame> {
    let mut index = 0usize;
    let (vector, source_governance) = match args.get(index) {
        Some(token) if eq_ignore_ascii_case(token, b"ELE") => {
            let Some(element) = args.get(index + 1) else {
                return Err(error("ERR syntax error"));
            };
            index += 2;
            match set.entry(element) {
                Some(entry) => (entry.vector.clone(), entry.governance.clone()),
                None => return Err(error("ERR no such element")),
            }
        }
        _ => (parse_vector_arg(args, &mut index)?, None),
    };
    let mut count = 10usize;
    let mut with_scores = false;
    let mut with_attribs = false;
    let mut with_governance = false;
    let mut allowed_governance = Vec::new();
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
            token if eq_ignore_ascii_case(token, b"GOVERNANCE") => {
                let Some(raw) = args.get(index + 1) else {
                    return Err(error("ERR syntax error"));
                };
                if raw.len() > MAX_VECTOR_GOVERNANCE_BYTES {
                    return Err(error("ERR vector governance metadata is too large"));
                }
                allowed_governance.push((*raw).to_vec());
                index += 2;
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
    if !governance_visible(source_governance.as_deref(), &allowed_governance) {
        return Err(error("ERR no such element"));
    }
    let tuple_width =
        1 + usize::from(with_scores) + usize::from(with_attribs) + usize::from(with_governance);
    if count > MAX_VECTOR_RESPONSE_ITEMS / tuple_width {
        return Err(error("ERR VSIM count exceeds the server result limit"));
    }
    Ok(VSimArgs {
        vector,
        count,
        with_scores,
        with_attribs,
        with_governance,
        allowed_governance,
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
            if count == 0 || count > MAX_VECTOR_DIMENSIONS {
                return Err(error("ERR vector dimension is out of range"));
            }
            let start = *index + 2;
            let end = start
                .checked_add(count)
                .ok_or_else(|| error("ERR vector dimension is out of range"))?;
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
            if blob.len() / 4 > MAX_VECTOR_DIMENSIONS {
                return Err(error("ERR vector dimension is out of range"));
            }
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
    allowed_governance: &[Bytes],
    shard_count: usize,
) -> Vec<(&'a VectorEntry, f64)> {
    let compiled_filter = filter.and_then(CompiledVectorFilter::parse);
    let workers = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    if set.entries.len() < VECTOR_SCAN_PARALLEL_MIN || workers <= 1 {
        return exact_vector_scores_sequential(
            set,
            query,
            filter,
            compiled_filter.as_ref(),
            allowed_governance,
        );
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
                        if !entry_matches_filter(entry, filter, compiled_filter, allowed_governance)
                        {
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
    allowed_governance: &[Bytes],
) -> Vec<(&'a VectorEntry, f64)> {
    sorted_scores(
        set.entries
            .iter()
            .filter(|entry| {
                entry_matches_filter(entry, filter, compiled_filter, allowed_governance)
            })
            .map(|entry| (entry, cosine_similarity(&entry.vector, query)))
            .collect(),
    )
}

fn entry_matches_filter(
    entry: &VectorEntry,
    filter: Option<&[u8]>,
    compiled_filter: Option<&CompiledVectorFilter>,
    allowed_governance: &[Bytes],
) -> bool {
    if !governance_visible(entry.governance.as_deref(), allowed_governance) {
        return false;
    }
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
    allowed_governance: &[Bytes],
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
            .filter_map(|index| {
                let entry = &set.entries[index];
                governance_visible(entry.governance.as_deref(), allowed_governance)
                    .then(|| (entry, cosine_similarity(&entry.vector, query)))
            })
            .collect(),
    )
}

fn vsim_response_bytes(scored: &[(&VectorEntry, f64)], parsed: &VSimArgs) -> Option<usize> {
    scored.iter().try_fold(0usize, |total, (entry, _)| {
        let mut bytes = entry.element.len().checked_add(16)?;
        if parsed.with_scores {
            bytes = bytes.checked_add(32)?;
        }
        if parsed.with_attribs {
            bytes = bytes.checked_add(entry.attributes.as_ref().map_or(0, Vec::len) + 16)?;
        }
        if parsed.with_governance {
            bytes = bytes.checked_add(entry.governance.as_ref().map_or(0, Vec::len) + 16)?;
        }
        total.checked_add(bytes)
    })
}

fn build_uid_index(set: &VectorSetState) -> HashMap<u64, usize> {
    let mut uid_to_index = HashMap::with_capacity(set.entries.len());
    for (index, entry) in set.entries.iter().enumerate() {
        uid_to_index.insert(entry.uid, index);
    }
    uid_to_index
}

fn lookup_uid_index(uid_to_index: &HashMap<u64, usize>, uid: u64) -> Option<usize> {
    uid_to_index.get(&uid).copied()
}

fn greedy_hnsw_layer_index(
    set: &VectorSetState,
    uid_to_index: &HashMap<u64, usize>,
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

pub(crate) fn validate_vector_set_bytes(existing: &[u8]) -> Result<(), ()> {
    decode_vector_set(Some(existing)).map(|_| ())
}

pub(crate) fn vector_set_contains_governance(existing: &[u8]) -> Result<bool, ()> {
    decode_vector_set_entries(Some(existing))
        .map(|set| set.entries.iter().any(|entry| entry.governance.is_some()))
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
    hnsw_collect_lex_range(existing, start, end, limit, &[]).or_else(|_| {
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

fn hnsw_collect_prefix_elements(
    existing: &[u8],
    limit: usize,
    allowed_governance: &[Bytes],
) -> Result<Vec<Bytes>, ()> {
    if limit == 0 {
        validate_hnsw_payload(existing)?;
        return Ok(Vec::new());
    }
    let mut elements = Vec::with_capacity(limit.min(16));
    let matched = scan_hnsw_entries(existing, |entry| {
        if governance_visible(entry.governance, allowed_governance) {
            elements.push(entry.element.to_vec());
        }
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
    allowed_governance: &[Bytes],
) -> Result<Vec<Bytes>, ()> {
    if limit == 0 {
        validate_hnsw_payload(existing)?;
        return Ok(Vec::new());
    }
    let mut elements = Vec::with_capacity(limit.min(16));
    let matched = scan_hnsw_entries(existing, |entry| {
        if governance_visible(entry.governance, allowed_governance)
            && lex_in_range(entry.element, start, end)
        {
            insert_bounded_lex(&mut elements, entry.element, limit);
        }
        None::<()>
    })?;
    debug_assert!(matched.is_none());
    Ok(elements)
}

fn validate_hnsw_payload(existing: &[u8]) -> Result<(), ()> {
    scan_hnsw_entries(existing, |_| None::<()>).map(|_| ())
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
    let dim = read_u32(&mut raw)? as usize;
    let _quantization = Quantization::from_tag(read_u32(&mut raw)?).ok_or(())?;
    let original_dim = read_u32(&mut raw)? as usize;
    let hnsw_m = read_u32(&mut raw)? as usize;
    let ef_construction = read_u32(&mut raw)? as usize;
    let max_level = read_u32(&mut raw)? as usize;
    let _next_uid = read_u64(&mut raw)?;
    let count = read_u32(&mut raw)? as usize;
    let minimum_entry_bytes = if governed { 28 } else { 24 };
    if dim > MAX_VECTOR_DIMENSIONS
        || original_dim > MAX_VECTOR_DIMENSIONS
        || hnsw_m == 0
        || hnsw_m > MAX_HNSW_M
        || ef_construction == 0
        || ef_construction > MAX_HNSW_EF_CONSTRUCTION
        || max_level > MAX_HNSW_LEVEL
        || count > MAX_VECTOR_SET_ENTRIES
        || count > raw.len() / minimum_entry_bytes
        || (count != 0 && dim == 0)
    {
        return Err(());
    }
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
    let minimum_entry_bytes = match format {
        VectorPayloadFormat::HnswGoverned => 28,
        VectorPayloadFormat::Hnsw => 24,
        VectorPayloadFormat::Current
        | VectorPayloadFormat::Quantized
        | VectorPayloadFormat::Legacy => 12,
    };
    if dim > MAX_VECTOR_DIMENSIONS
        || original_dim > MAX_VECTOR_DIMENSIONS
        || hnsw_m == 0
        || hnsw_m > MAX_HNSW_M
        || ef_construction == 0
        || ef_construction > MAX_HNSW_EF_CONSTRUCTION
        || max_level > MAX_HNSW_LEVEL
        || count > MAX_VECTOR_SET_ENTRIES
        || count > raw.len() / minimum_entry_bytes
        || (count != 0 && dim == 0)
    {
        return Err(());
    }
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
    if dim > MAX_VECTOR_DIMENSIONS {
        return Err(());
    }
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
    if hnsw_m == 0
        || hnsw_m > MAX_HNSW_M
        || ef_construction == 0
        || ef_construction > MAX_HNSW_EF_CONSTRUCTION
        || max_level > MAX_HNSW_LEVEL
    {
        return Err(());
    }
    let count = read_u32(&mut raw)? as usize;
    let minimum_entry_bytes = if matches!(format, VectorPayloadFormat::HnswGoverned) {
        28
    } else if matches!(format, VectorPayloadFormat::Hnsw) {
        24
    } else {
        12
    };
    if count > MAX_VECTOR_SET_ENTRIES
        || count > raw.len() / minimum_entry_bytes
        || (count != 0 && dim == 0)
    {
        return Err(());
    }
    let mut entries = Vec::new();
    entries.try_reserve_exact(count).map_err(|_| ())?;
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
        if level > MAX_HNSW_LEVEL
            || (matches!(
                format,
                VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned
            ) && level > max_level)
        {
            return Err(());
        }
        let element = read_bytes(&mut raw)?;
        let vector_len = read_u32(&mut raw)? as usize;
        if vector_len != dim || vector_len > raw.len() / std::mem::size_of::<f64>() {
            return Err(());
        }
        let mut vector = Vec::new();
        vector.try_reserve_exact(vector_len).map_err(|_| ())?;
        for _ in 0..vector_len {
            let value = read_f64(&mut raw)?;
            if !value.is_finite() {
                return Err(());
            }
            vector.push(value);
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
            if layer_count != entry.level.saturating_add(1)
                || layer_count > MAX_HNSW_LEVEL.saturating_add(1)
                || layer_count > raw.len() / 4
            {
                return Err(());
            }
            let mut links = Vec::new();
            links.try_reserve_exact(layer_count).map_err(|_| ())?;
            for _ in 0..layer_count {
                let link_count = read_u32(&mut raw)? as usize;
                if link_count > hnsw_m || link_count > raw.len() / 8 {
                    return Err(());
                }
                let mut layer = Vec::new();
                layer.try_reserve_exact(link_count).map_err(|_| ())?;
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
    let validate_links = !matches!(
        format,
        VectorPayloadFormat::Hnsw | VectorPayloadFormat::HnswGoverned
    ) || decode_links;
    validate_decoded_vector_set(&set, validate_links)?;
    Ok(set)
}

fn validate_decoded_vector_set(set: &VectorSetState, validate_links: bool) -> Result<(), ()> {
    if set.next_uid == 0 {
        return Err(());
    }

    let mut uid_levels = HashMap::with_capacity(set.entries.len());
    let mut elements: HashSet<&[u8]> = HashSet::with_capacity(set.entries.len());
    let mut max_uid = 0;
    for entry in &set.entries {
        if entry.uid == 0
            || uid_levels.insert(entry.uid, entry.level).is_some()
            || !elements.insert(entry.element.as_ref())
            || entry.vector.iter().any(|value| !value.is_finite())
        {
            return Err(());
        }
        max_uid = max_uid.max(entry.uid);
    }
    if !set.entries.is_empty() && set.next_uid <= max_uid {
        return Err(());
    }
    if !validate_links {
        return Ok(());
    }

    for entry in &set.entries {
        if entry.links.len() != entry.level.saturating_add(1) {
            return Err(());
        }
        for (level, links) in entry.links.iter().enumerate() {
            let mut seen = HashSet::with_capacity(links.len());
            for uid in links {
                let Some(target_level) = uid_levels.get(uid) else {
                    return Err(());
                };
                if *uid == entry.uid || *target_level < level || !seen.insert(*uid) {
                    return Err(());
                }
            }
        }
    }
    Ok(())
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
    let values: Vec<f64> = blob
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()) as f64)
        .collect();
    if values.iter().any(|value| !value.is_finite()) {
        return Err(error("ERR value is not a float"));
    }
    Ok(values)
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
    let values: Vec<f64> = raw
        .chunks_exact(8)
        .map(|chunk| {
            let bytes: [u8; 8] = chunk.try_into().map_err(|_| ())?;
            Ok::<f64, ()>(f64::from_le_bytes(bytes))
        })
        .collect::<Result<_, ()>>()?;
    if values.iter().any(|value| !value.is_finite()) {
        return Err(());
    }
    Ok(values)
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
    let mut out = Vec::new();
    out.try_reserve_exact(len).map_err(|_| ())?;
    out.extend_from_slice(head);
    Ok(out)
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::redis::RedisCommand;

    fn one_entry_set(governance: Option<&[u8]>) -> VectorSetState {
        VectorSetState {
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
                governance: governance.map(<[u8]>::to_vec),
                links: vec![Vec::new()],
            }],
        }
    }

    #[test]
    fn sparse_large_hnsw_uids_do_not_drive_dense_allocations() {
        let mut set = one_entry_set(None);
        set.entries[0].uid = u64::MAX - 1;
        set.next_uid = u64::MAX;
        let encoded = encode_vector_set(&set);
        let decoded = decode_vector_set(Some(&encoded)).expect("valid sparse UID state");

        assert_eq!(build_uid_index(&decoded).len(), 1);
        assert_eq!(hnsw_search(&decoded, &[1.0, 0.0], 1, 1, &[]).len(), 1);
    }

    #[test]
    fn canonical_vector_state_rejects_invalid_identity_graph_and_numbers() {
        let mut duplicate_uid = one_entry_set(None);
        let mut second = duplicate_uid.entries[0].clone();
        second.element = b"doc-b".to_vec();
        duplicate_uid.entries.push(second);
        duplicate_uid.next_uid = 3;
        assert!(validate_vector_set_bytes(&encode_vector_set(&duplicate_uid)).is_err());

        let mut duplicate_element = one_entry_set(None);
        let mut second = duplicate_element.entries[0].clone();
        second.uid = 2;
        duplicate_element.entries.push(second);
        duplicate_element.next_uid = 3;
        assert!(validate_vector_set_bytes(&encode_vector_set(&duplicate_element)).is_err());

        let mut dangling_link = one_entry_set(None);
        dangling_link.entries[0].links[0].push(99);
        assert!(validate_vector_set_bytes(&encode_vector_set(&dangling_link)).is_err());

        let mut non_finite = one_entry_set(None);
        non_finite.entries[0].vector[0] = f64::NAN;
        assert!(validate_vector_set_bytes(&encode_vector_set(&non_finite)).is_err());
    }

    #[test]
    fn fp32_input_rejects_non_finite_values() {
        assert!(fp32_values(&f32::NAN.to_le_bytes()).is_err());
        assert!(fp32_values(&f32::INFINITY.to_le_bytes()).is_err());
    }

    #[test]
    fn vector_add_rejects_exhausted_uid_space() {
        let store = EmbeddedStore::new(1);
        let mut set = one_entry_set(None);
        set.entries[0].uid = u64::MAX - 1;
        set.next_uid = u64::MAX;
        store.set_value_bytes(
            b"objects",
            bytes::Bytes::from(encode_vector_set(&set)),
            None,
        );

        assert!(matches!(
            VAdd::execute(
                &store,
                &[b"objects", b"VALUES", b"2", b"0", b"1", b"doc-b"],
            ),
            Frame::Error(message) if message.contains("UID space exhausted")
        ));
        assert_eq!(VCard::execute(&store, &[b"objects"]), Frame::Integer(1));
    }

    #[test]
    fn governed_vector_format_round_trips_without_changing_ungoverned_format() {
        let mut set = one_entry_set(None);

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
    fn malformed_vector_headers_are_rejected_before_allocation() {
        let valid = encode_vector_set(&one_entry_set(None));
        assert!(validate_vector_set_bytes(&valid).is_ok());

        let mut excessive_dim = valid.clone();
        let dim_offset = VECTOR_SET_PREFIX.len() + 4;
        excessive_dim[dim_offset..dim_offset + 4]
            .copy_from_slice(&((MAX_VECTOR_DIMENSIONS as u32) + 1).to_le_bytes());
        assert!(validate_vector_set_bytes(&excessive_dim).is_err());

        let mut excessive_m = valid.clone();
        let m_offset = VECTOR_SET_PREFIX.len() + 16;
        excessive_m[m_offset..m_offset + 4]
            .copy_from_slice(&((MAX_HNSW_M as u32) + 1).to_le_bytes());
        assert!(validate_vector_set_bytes(&excessive_m).is_err());

        let mut impossible_count = valid;
        let count_offset = VECTOR_SET_PREFIX.len() + 36;
        impossible_count[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(validate_vector_set_bytes(&impossible_count).is_err());
    }

    #[test]
    fn decoded_cache_accounting_includes_heap_owned_state() {
        let set = one_entry_set(Some(b"tenant=acme"));
        let encoded = encode_vector_set(&set);
        let retained = encoded.len().saturating_add(decoded_vector_set_bytes(&set));
        assert!(retained > encoded.len());

        let mut cache = VectorDecodeCache::default();
        let raw = bytes::Bytes::from(encoded);
        cache.insert(
            vector_decode_cache_key(&raw, VectorDecodeMode::Full),
            raw,
            Arc::new(set),
        );
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.retained_bytes, retained);
    }

    #[test]
    fn governed_vectors_fail_closed_and_support_guarded_rotation() {
        let store = EmbeddedStore::new(1);
        assert_eq!(
            VAdd::execute(
                &store,
                &[
                    b"objects",
                    b"VALUES",
                    b"2",
                    b"1",
                    b"0",
                    b"doc-a",
                    b"GOVERNANCE",
                    b"tenant=acme",
                ],
            ),
            Frame::Integer(1)
        );

        assert_eq!(VEmb::execute(&store, &[b"objects", b"doc-a"]), Frame::Null);
        assert_eq!(
            VIsMember::execute(&store, &[b"objects", b"doc-a"]),
            Frame::Integer(0)
        );
        assert_eq!(
            VSetAttr::execute(&store, &[b"objects", b"doc-a", br#"{"ok":true}"#]),
            Frame::Integer(0)
        );
        assert_eq!(
            VRem::execute(&store, &[b"objects", b"doc-a"]),
            Frame::Integer(0)
        );
        assert_eq!(
            VRange::execute(&store, &[b"objects", b"-", b"+"]),
            Frame::Array(Vec::new())
        );
        assert_eq!(VRandMember::execute(&store, &[b"objects"]), Frame::Null);
        assert_eq!(
            VLinks::execute(&store, &[b"objects", b"doc-a"]),
            Frame::Null
        );
        assert_eq!(
            VRange::execute(
                &store,
                &[b"objects", b"-", b"+", b"GOVERNANCE", b"tenant=acme"],
            ),
            Frame::Array(vec![bulk(b"doc-a".to_vec())])
        );
        assert_eq!(
            VRandMember::execute(&store, &[b"objects", b"GOVERNANCE", b"tenant=acme"],),
            bulk(b"doc-a".to_vec())
        );
        assert!(matches!(
            VLinks::execute(
                &store,
                &[b"objects", b"doc-a", b"GOVERNANCE", b"tenant=acme"],
            ),
            Frame::Array(_)
        ));
        assert_eq!(
            VSim::execute(&store, &[b"objects", b"VALUES", b"2", b"1", b"0", b"TRUTH"],),
            Frame::Array(Vec::new())
        );
        assert!(matches!(
            VSim::execute(
                &store,
                &[b"objects", b"ELE", b"doc-a", b"COUNT", b"1", b"TRUTH"],
            ),
            Frame::Error(message) if message.contains("no such element")
        ));
        assert_eq!(
            VSim::execute(
                &store,
                &[
                    b"objects",
                    b"VALUES",
                    b"2",
                    b"1",
                    b"0",
                    b"GOVERNANCE",
                    b"tenant=acme",
                    b"WITHGOVERNANCE",
                    b"TRUTH",
                ],
            ),
            Frame::Array(vec![bulk(b"doc-a".to_vec()), bulk(b"tenant=acme".to_vec()),])
        );
        assert_eq!(
            VSim::execute(
                &store,
                &[
                    b"objects",
                    b"VALUES",
                    b"2",
                    b"1",
                    b"0",
                    b"COUNT",
                    b"1",
                    b"GOVERNANCE",
                    b"tenant=acme",
                ],
            ),
            Frame::Array(vec![bulk(b"doc-a".to_vec())])
        );

        let denied = VAdd::execute(
            &store,
            &[
                b"objects",
                b"VALUES",
                b"2",
                b"0",
                b"1",
                b"doc-a",
                b"IFGOVERNANCE",
                b"tenant=other",
            ],
        );
        assert!(matches!(denied, Frame::Error(message) if message.contains("NOPERM")));

        assert_eq!(
            VAdd::execute(
                &store,
                &[
                    b"objects",
                    b"VALUES",
                    b"2",
                    b"0",
                    b"1",
                    b"doc-a",
                    b"IFGOVERNANCE",
                    b"tenant=acme",
                    b"GOVERNANCE",
                    b"tenant=beta",
                ],
            ),
            Frame::Integer(0)
        );
        assert_eq!(
            VEmb::execute(
                &store,
                &[b"objects", b"doc-a", b"GOVERNANCE", b"tenant=acme"],
            ),
            Frame::Null
        );
        assert!(matches!(
            VEmb::execute(
                &store,
                &[b"objects", b"doc-a", b"GOVERNANCE", b"tenant=beta"],
            ),
            Frame::Array(_)
        ));
        assert_eq!(
            VAdd::execute(
                &store,
                &[
                    b"objects",
                    b"VALUES",
                    b"2",
                    b"0",
                    b"1",
                    b"doc-a",
                    b"IFGOVERNANCE",
                    b"tenant=beta",
                    b"CLEARGOVERNANCE",
                ],
            ),
            Frame::Integer(0)
        );
        assert!(matches!(
            VEmb::execute(&store, &[b"objects", b"doc-a"]),
            Frame::Array(_)
        ));

        assert_eq!(
            VAdd::execute(
                &store,
                &[b"objects", b"VALUES", b"2", b"1", b"0", b"public"],
            ),
            Frame::Integer(1)
        );
        assert!(matches!(
            VEmb::execute(
                &store,
                &[b"objects", b"public", b"GOVERNANCE", b"tenant=beta"],
            ),
            Frame::Array(_)
        ));
    }

    #[test]
    fn vsim_bounds_result_items_and_retained_response_bytes() {
        let store = EmbeddedStore::new(1);
        assert_eq!(
            VAdd::execute(&store, &[b"objects", b"VALUES", b"2", b"1", b"0", b"doc-a"],),
            Frame::Integer(1)
        );
        let excessive_count = (MAX_VECTOR_RESPONSE_ITEMS + 1).to_string();
        assert!(matches!(
            VSim::execute(
                &store,
                &[
                    b"objects",
                    b"VALUES",
                    b"2",
                    b"1",
                    b"0",
                    b"COUNT",
                    excessive_count.as_bytes(),
                ],
            ),
            Frame::Error(message) if message.contains("result limit")
        ));

        let entry = VectorEntry {
            uid: 1,
            level: 0,
            element: b"doc-a".to_vec(),
            vector: vec![1.0, 0.0],
            attributes: None,
            governance: Some(vec![b'x'; MAX_VECTOR_GOVERNANCE_BYTES]),
            links: Vec::new(),
        };
        let scored = vec![(&entry, 1.0); 300];
        let parsed = VSimArgs {
            vector: vec![1.0, 0.0],
            count: scored.len(),
            with_scores: false,
            with_attribs: false,
            with_governance: true,
            allowed_governance: vec![entry.governance.clone().unwrap()],
            filter: None,
            ef_search: None,
            truth: false,
        };
        assert!(
            vsim_response_bytes(&scored, &parsed)
                .is_some_and(|bytes| bytes > MAX_VECTOR_RESPONSE_BYTES)
        );
    }

    #[test]
    fn governed_hnsw_does_not_scan_arbitrary_unvisited_entries() {
        let entry = |uid, element: &'static [u8], governance: Option<&'static [u8]>| VectorEntry {
            uid,
            level: 0,
            element: element.to_vec(),
            vector: vec![1.0, 0.0],
            attributes: None,
            governance: governance.map(<[u8]>::to_vec),
            links: vec![Vec::new()],
        };
        let set = VectorSetState {
            dim: 2,
            original_dim: 2,
            quantization: Quantization::NoQuant,
            hnsw_m: DEFAULT_HNSW_M,
            ef_construction: DEFAULT_HNSW_EF_CONSTRUCTION,
            max_level: 0,
            next_uid: 4,
            entries: vec![
                entry(1, b"allowed-but-unvisited", Some(b"tenant=allowed")),
                entry(2, b"denied-a", Some(b"tenant=denied")),
                entry(3, b"denied-entrypoint", Some(b"tenant=denied")),
            ],
        };

        let results = hnsw_search(&set, &[1.0, 0.0], 1, 1, &[b"tenant=allowed".to_vec()]);
        assert!(results.is_empty());
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

        let scored = exact_vector_scores(&set, &[1.0, 0.0], None, &[], 8);
        assert_eq!(scored.len(), VECTOR_SCAN_PARALLEL_MIN);
        assert_ne!(scored[0].0.element, b"top".to_vec());

        let filtered = exact_vector_scores(
            &set,
            &[1.0, 0.0],
            Some(b".keep == true"),
            &[b"tenant=acme".to_vec()],
            8,
        );
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
