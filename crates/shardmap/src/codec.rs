//! Typed embedded map facade over the shared byte storage engine.
//!
//! [`CodecShardMap`] gives Rust callers the familiar `CodecShardMap<K, V>` shape while
//! keeping the storage engine byte-oriented. Values can be materialized into
//! owned Rust types with [`CodecShardMap::get`], or decoded as borrowed views from a
//! short-lived guard with [`CodecShardMap::get_ref`].

use std::borrow::Borrow;
use std::marker::PhantomData;
use std::mem::size_of;

use bytes::{BufMut, Bytes as SharedBytes, BytesMut};
use thiserror::Error;

use crate::cache::{CacheOptions, DEFAULT_CACHE_SHARDS, SharedCache};
use crate::storage::{EmbeddedKeyRoute, EmbeddedRouteMode, PreparedPointKey};

/// Encoded key or value bytes produced by a typed map codec.
#[derive(Debug, Clone)]
pub enum EncodedBytes<'a> {
    /// Borrowed bytes that can be copied into the storage engine when needed.
    Borrowed(&'a [u8]),
    /// Owned bytes that can be moved into the storage engine without copying.
    Owned(SharedBytes),
}

impl<'a> EncodedBytes<'a> {
    /// Returns the encoded bytes as a borrowed slice.
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes.as_ref(),
        }
    }

    /// Converts the encoded bytes into an owned `Bytes` handle.
    #[inline(always)]
    pub fn into_owned(self) -> SharedBytes {
        match self {
            Self::Borrowed(bytes) => SharedBytes::copy_from_slice(bytes),
            Self::Owned(bytes) => bytes,
        }
    }
}

/// Error returned when a stored byte value cannot be decoded as the typed value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodecError {
    /// A typed facade over a shared engine must use a non-empty namespace.
    #[error("typed shared-engine facades require a non-empty namespace")]
    EmptyNamespace,
    /// The stored bytes were not valid UTF-8 for a string value.
    #[error("stored value is not valid UTF-8")]
    InvalidUtf8,
    /// A fixed-width codec received the wrong number of bytes.
    #[error("invalid fixed-width value length: expected {expected} bytes, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
    /// A bool codec received a byte other than `0` or `1`.
    #[error("invalid bool value byte: {0}")]
    InvalidBool(u8),
    /// Custom codec failure.
    #[error("{0}")]
    Custom(String),
}

impl CodecError {
    /// Builds a custom codec error.
    pub fn custom(message: impl Into<String>) -> Self {
        Self::Custom(message.into())
    }
}

impl From<std::str::Utf8Error> for CodecError {
    fn from(_: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8
    }
}

impl From<std::string::FromUtf8Error> for CodecError {
    fn from(_: std::string::FromUtf8Error) -> Self {
        Self::InvalidUtf8
    }
}

/// A type that can encode itself as a shardmap key.
pub trait CodecKey {
    /// Returns the byte representation used for routing and equality.
    fn encode_key(&self) -> EncodedBytes<'_>;
}

/// A key type that can decode itself from shardmap key bytes.
pub trait CodecKeyDecode: CodecKey + Sized {
    /// Decodes a stored key suffix into an owned key.
    fn decode_key(bytes: &[u8]) -> Result<Self, CodecError>;
}

/// A type that can encode itself as a shardmap value.
pub trait CodecValueEncode {
    /// Returns the byte representation stored by the engine.
    fn encode_value(&self) -> EncodedBytes<'_>;
}

/// A typed value codec for the embedded [`CodecShardMap`] facade.
///
/// Custom structs can implement this trait directly. Use [`Self::Borrowed`]
/// for a zero-copy view when the serialized format can be interpreted from the
/// stored byte slice.
pub trait CodecValue: CodecValueEncode + Sized {
    /// Borrowed value view returned by [`CodecShardMapRef::value`].
    type Borrowed<'a>
    where
        Self: 'a;

    /// Decodes a borrowed byte slice into an owned value.
    fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError>;

    /// Decodes an owned byte handle into an owned value.
    #[inline(always)]
    fn decode_bytes(bytes: SharedBytes) -> Result<Self, CodecError> {
        Self::decode_owned(bytes.as_ref())
    }

    /// Decodes a borrowed byte slice into a borrowed value view.
    fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError>;
}

/// Typed read guard returned by [`CodecShardMap::get_ref`].
pub struct CodecShardMapRef<'a, V>
where
    V: CodecValue,
{
    raw: crate::cache::CacheRef<'a>,
    _marker: PhantomData<V>,
}

impl<V> CodecShardMapRef<'_, V>
where
    V: CodecValue,
{
    /// Returns the raw stored bytes.
    #[inline(always)]
    pub fn raw(&self) -> &[u8] {
        self.raw.value()
    }

    /// Decodes the stored bytes as the borrowed value view.
    #[inline(always)]
    pub fn value(&self) -> Result<V::Borrowed<'_>, CodecError> {
        V::decode_borrowed(self.raw.value())
    }
}

/// Cloneable typed embedded map.
///
/// `CodecShardMap<K, V>` keeps the same storage engine as the raw byte cache, but
/// exposes owned typed reads through [`Self::get`] and borrowed reads through
/// [`Self::get_ref`]. The `codec` feature enables constructors for sharing one
/// underlying engine across multiple typed maps with distinct namespaces.
#[derive(Debug)]
pub struct CodecShardMap<
    K = SharedBytes,
    V = SharedBytes,
    const SHARDS: usize = DEFAULT_CACHE_SHARDS,
> {
    raw: SharedCache<SHARDS>,
    namespace: SharedBytes,
    namespace_prefix: SharedBytes,
    default_ttl_ms: Option<u64>,
    _marker: PhantomData<fn(K, V)>,
}

/// Typed map with an explicit compile-time shard count.
pub type CodecShardMapWithShards<const SHARDS: usize, K = SharedBytes, V = SharedBytes> =
    CodecShardMap<K, V, SHARDS>;

impl<K, V, const SHARDS: usize> Clone for CodecShardMap<K, V, SHARDS> {
    fn clone(&self) -> Self {
        Self {
            raw: self.raw.clone(),
            namespace: self.namespace.clone(),
            namespace_prefix: self.namespace_prefix.clone(),
            default_ttl_ms: self.default_ttl_ms,
            _marker: PhantomData,
        }
    }
}

impl<K, V, const SHARDS: usize> Default for CodecShardMap<K, V, SHARDS> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, const SHARDS: usize> CodecShardMap<K, V, SHARDS> {
    /// Creates a typed map with default options.
    #[inline(always)]
    pub fn new() -> Self {
        Self::with_options(CacheOptions::default())
    }

    /// Creates a typed map with a total point-key capacity hint.
    #[inline(always)]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_options(CacheOptions {
            capacity_hint: Some(capacity),
            ..CacheOptions::default()
        })
    }

    /// Creates a typed map with explicit cache options.
    #[inline(always)]
    pub fn with_options(options: CacheOptions) -> Self {
        let mut raw_options = options;
        let default_ttl_ms = raw_options.default_ttl_ms;
        raw_options.default_ttl_ms = None;
        Self {
            raw: SharedCache::with_options(raw_options),
            namespace: SharedBytes::new(),
            namespace_prefix: SharedBytes::new(),
            default_ttl_ms,
            _marker: PhantomData,
        }
    }

    /// Wraps an existing shared engine with a typed, namespaced view.
    #[cfg(feature = "codec")]
    #[inline(always)]
    pub fn from_shared_engine(
        namespace: impl Into<SharedBytes>,
        raw: SharedCache<SHARDS>,
    ) -> Result<Self, CodecError> {
        Self::from_shared_engine_with_options(namespace, raw, CacheOptions::default())
    }

    /// Wraps an existing shared engine with a typed, namespaced view and
    /// facade-local options.
    ///
    /// The shared engine already owns storage-level settings such as memory
    /// limits. For a shared facade, [`CacheOptions::default_ttl_ms`] is applied
    /// only to writes made through this namespace.
    #[cfg(feature = "codec")]
    #[inline(always)]
    pub fn from_shared_engine_with_options(
        namespace: impl Into<SharedBytes>,
        raw: SharedCache<SHARDS>,
        options: CacheOptions,
    ) -> Result<Self, CodecError> {
        let namespace = namespace.into();
        if namespace.is_empty() {
            return Err(CodecError::EmptyNamespace);
        }
        let namespace_prefix = namespace_prefix_bytes(namespace.as_ref());
        Ok(Self {
            raw,
            namespace,
            namespace_prefix,
            default_ttl_ms: options.default_ttl_ms,
            _marker: PhantomData,
        })
    }

    /// Returns the raw shared engine backing this typed facade.
    #[cfg(feature = "codec")]
    #[inline(always)]
    pub fn shared_engine(&self) -> &SharedCache<SHARDS> {
        &self.raw
    }

    /// Consumes this typed facade and returns the raw shared engine.
    #[cfg(feature = "codec")]
    #[inline(always)]
    pub fn into_shared_engine(self) -> SharedCache<SHARDS> {
        self.raw
    }

    /// Returns the configured number of stripes.
    #[inline(always)]
    pub const fn shard_count(&self) -> usize {
        SHARDS
    }

    /// Returns the configured route mode.
    #[inline(always)]
    pub fn route_mode(&self) -> EmbeddedRouteMode {
        self.raw.route_mode()
    }

    /// Returns the default TTL applied to writes made through this typed
    /// facade when no explicit TTL is passed.
    #[inline(always)]
    pub const fn default_ttl_ms(&self) -> Option<u64> {
        self.default_ttl_ms
    }

    /// Computes the storage route for an encoded typed key.
    #[inline(always)]
    pub fn route_key<Q>(&self, key: &Q) -> EmbeddedKeyRoute
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
    {
        let key = self.namespaced_key(key.encode_key());
        self.raw.route_key(key.as_slice())
    }

    /// Precomputes route and exact-match metadata for repeated point lookups.
    #[inline(always)]
    pub fn prepare_key<Q>(&self, key: &Q) -> PreparedPointKey
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
    {
        let key = self.namespaced_key(key.encode_key());
        self.raw.prepare_key(key.as_slice())
    }

    /// Inserts or replaces a typed value.
    #[inline(always)]
    pub fn insert(&self, key: K, value: V)
    where
        K: CodecKey,
        V: CodecValueEncode,
    {
        self.insert_ref(&key, &value);
    }

    /// Inserts or replaces a value from borrowed typed views.
    #[inline(always)]
    pub fn insert_ref<Q, W>(&self, key: &Q, value: &W)
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
        W: CodecValueEncode + ?Sized,
    {
        let key = self.namespaced_key(key.encode_key()).into_owned();
        let value = value.encode_value().into_owned();
        self.raw.insert_with_ttl(key, value, self.default_ttl_ms);
    }

    /// Inserts or replaces a typed value with an optional relative TTL.
    #[inline(always)]
    pub fn insert_with_ttl(&self, key: K, value: V, ttl_ms: Option<u64>)
    where
        K: CodecKey,
        V: CodecValueEncode,
    {
        self.insert_ref_with_ttl(&key, &value, ttl_ms);
    }

    /// Inserts or replaces a value with an optional relative TTL.
    #[inline(always)]
    pub fn insert_ref_with_ttl<Q, W>(&self, key: &Q, value: &W, ttl_ms: Option<u64>)
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
        W: CodecValueEncode + ?Sized,
    {
        let key = self.namespaced_key(key.encode_key()).into_owned();
        let value = value.encode_value().into_owned();
        self.raw.insert_with_ttl(key, value, ttl_ms);
    }

    /// Inserts only when the encoded key is absent or expired.
    #[inline(always)]
    pub fn try_insert_ref<Q, W>(&self, key: &Q, value: &W) -> bool
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
        W: CodecValueEncode + ?Sized,
    {
        let key = self.namespaced_key(key.encode_key()).into_owned();
        let value = value.encode_value().into_owned();
        self.raw
            .try_insert_with_ttl(key, value, self.default_ttl_ms)
    }

    /// Inserts only when the encoded key is absent or expired, with an explicit
    /// optional relative TTL.
    #[inline(always)]
    pub fn try_insert_ref_with_ttl<Q, W>(&self, key: &Q, value: &W, ttl_ms: Option<u64>) -> bool
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
        W: CodecValueEncode + ?Sized,
    {
        let key = self.namespaced_key(key.encode_key()).into_owned();
        let value = value.encode_value().into_owned();
        self.raw.try_insert_with_ttl(key, value, ttl_ms)
    }

    /// Returns true when the encoded key is present.
    #[inline(always)]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
    {
        let key = self.namespaced_key(key.encode_key());
        self.raw.contains_key(key.as_slice())
    }

    /// Returns true when the encoded key is present.
    #[inline(always)]
    pub fn exists<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
    {
        self.contains_key(key)
    }

    /// Returns an owned decoded value.
    #[inline(always)]
    pub fn get<Q>(&self, key: &Q) -> Result<Option<V>, CodecError>
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
        V: CodecValue,
    {
        let key = self.namespaced_key(key.encode_key());
        self.raw
            .get_owned(key.as_slice())
            .map(V::decode_bytes)
            .transpose()
    }

    /// Returns a borrowed read guard that can decode zero-copy borrowed views.
    #[inline(always)]
    pub fn get_ref<Q>(&self, key: &Q) -> Option<CodecShardMapRef<'_, V>>
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
        V: CodecValue,
    {
        let key = self.namespaced_key(key.encode_key());
        self.raw
            .get_ref(key.as_slice())
            .map(|raw| CodecShardMapRef {
                raw,
                _marker: PhantomData,
            })
    }

    /// Removes a key and decodes the removed value when present.
    #[inline(always)]
    pub fn remove<Q>(&self, key: &Q) -> Result<Option<V>, CodecError>
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
        V: CodecValue,
    {
        let key = self.namespaced_key(key.encode_key());
        self.raw
            .remove(key.as_slice())
            .map(V::decode_bytes)
            .transpose()
    }

    /// Removes a key without decoding the removed value.
    #[inline(always)]
    pub fn delete<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: CodecKey + ?Sized,
    {
        let key = self.namespaced_key(key.encode_key());
        self.raw.remove(key.as_slice()).is_some()
    }

    /// Visits keys visible to this typed facade.
    ///
    /// For shared-engine facades this filters by namespace and decodes only
    /// keys written through the same facade namespace.
    pub fn visit_keys(&self, mut visitor: impl FnMut(K) -> bool) -> Result<(), CodecError>
    where
        K: CodecKeyDecode,
    {
        let mut result = Ok(());
        self.raw.visit_keys(|raw_key| {
            let Some(key) = self.strip_namespace(raw_key) else {
                return true;
            };
            match K::decode_key(key) {
                Ok(key) => visitor(key),
                Err(error) => {
                    result = Err(error);
                    false
                }
            }
        });
        result
    }

    /// Returns a sorted snapshot of keys visible to this typed facade.
    pub fn keys(&self) -> Result<Vec<K>, CodecError>
    where
        K: CodecKeyDecode + Ord,
    {
        let mut keys = Vec::new();
        self.visit_keys(|key| {
            keys.push(key);
            true
        })?;
        keys.sort();
        Ok(keys)
    }

    /// Visits entries visible to this typed facade.
    ///
    /// Values are decoded while the shard read lock is held. Keep callbacks
    /// lightweight.
    pub fn visit_entries(&self, mut visitor: impl FnMut(K, V) -> bool) -> Result<(), CodecError>
    where
        K: CodecKeyDecode,
        V: CodecValue,
    {
        let mut result = Ok(());
        self.raw.visit_entries(|raw_key, raw_value, _expire_at_ms| {
            let Some(key) = self.strip_namespace(raw_key) else {
                return true;
            };
            match K::decode_key(key)
                .and_then(|key| V::decode_owned(raw_value).map(|value| (key, value)))
            {
                Ok((key, value)) => visitor(key, value),
                Err(error) => {
                    result = Err(error);
                    false
                }
            }
        });
        result
    }

    /// Returns a sorted snapshot of entries visible to this typed facade.
    pub fn entries(&self) -> Result<Vec<(K, V)>, CodecError>
    where
        K: CodecKeyDecode + Ord,
        V: CodecValue,
    {
        let mut entries = Vec::new();
        self.visit_entries(|key, value| {
            entries.push((key, value));
            true
        })?;
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(entries)
    }

    #[inline(always)]
    fn namespaced_key<'a>(&self, key: EncodedBytes<'a>) -> EncodedBytes<'a> {
        if self.namespace_prefix.is_empty() {
            return key;
        }

        let key_bytes = key.as_slice();
        let mut encoded = BytesMut::with_capacity(self.namespace_prefix.len() + key_bytes.len());
        encoded.extend_from_slice(self.namespace_prefix.as_ref());
        encoded.extend_from_slice(key_bytes);
        EncodedBytes::Owned(encoded.freeze())
    }

    #[inline(always)]
    fn strip_namespace<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        if self.namespace_prefix.is_empty() {
            return Some(key);
        }
        key.strip_prefix(self.namespace_prefix.as_ref())
    }
}

fn namespace_prefix_bytes(namespace: &[u8]) -> SharedBytes {
    let mut encoded = BytesMut::with_capacity(size_of::<u32>() + namespace.len());
    encoded.put_u32(namespace.len() as u32);
    encoded.extend_from_slice(namespace);
    encoded.freeze()
}

impl CodecKey for str {
    #[inline(always)]
    fn encode_key(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self.as_bytes())
    }
}

impl CodecValueEncode for str {
    #[inline(always)]
    fn encode_value(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self.as_bytes())
    }
}

impl CodecKey for String {
    #[inline(always)]
    fn encode_key(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self.as_bytes())
    }
}

impl CodecKeyDecode for String {
    #[inline(always)]
    fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

impl CodecValueEncode for String {
    #[inline(always)]
    fn encode_value(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self.as_bytes())
    }
}

impl CodecValue for String {
    type Borrowed<'a> = &'a str;

    #[inline(always)]
    fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    #[inline(always)]
    fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError> {
        Ok(std::str::from_utf8(bytes)?)
    }
}

impl CodecKey for [u8] {
    #[inline(always)]
    fn encode_key(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self)
    }
}

impl CodecValueEncode for [u8] {
    #[inline(always)]
    fn encode_value(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self)
    }
}

impl<const N: usize> CodecKey for [u8; N] {
    #[inline(always)]
    fn encode_key(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self.as_slice())
    }
}

impl<const N: usize> CodecValueEncode for [u8; N] {
    #[inline(always)]
    fn encode_value(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self.as_slice())
    }
}

impl CodecKey for Vec<u8> {
    #[inline(always)]
    fn encode_key(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self.as_slice())
    }
}

impl CodecKeyDecode for Vec<u8> {
    #[inline(always)]
    fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(bytes.to_vec())
    }
}

impl CodecValueEncode for Vec<u8> {
    #[inline(always)]
    fn encode_value(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self.as_slice())
    }
}

impl CodecValue for Vec<u8> {
    type Borrowed<'a> = &'a [u8];

    #[inline(always)]
    fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(bytes.to_vec())
    }

    #[inline(always)]
    fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError> {
        Ok(bytes)
    }
}

impl CodecKey for SharedBytes {
    #[inline(always)]
    fn encode_key(&self) -> EncodedBytes<'_> {
        EncodedBytes::Borrowed(self.as_ref())
    }
}

impl CodecKeyDecode for SharedBytes {
    #[inline(always)]
    fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(SharedBytes::copy_from_slice(bytes))
    }
}

impl CodecValueEncode for SharedBytes {
    #[inline(always)]
    fn encode_value(&self) -> EncodedBytes<'_> {
        EncodedBytes::Owned(self.clone())
    }
}

impl CodecValue for SharedBytes {
    type Borrowed<'a> = &'a [u8];

    #[inline(always)]
    fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(SharedBytes::copy_from_slice(bytes))
    }

    #[inline(always)]
    fn decode_bytes(bytes: SharedBytes) -> Result<Self, CodecError> {
        Ok(bytes)
    }

    #[inline(always)]
    fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError> {
        Ok(bytes)
    }
}

macro_rules! impl_fixed_width_codec {
    ($ty:ty) => {
        impl CodecKey for $ty {
            #[inline(always)]
            fn encode_key(&self) -> EncodedBytes<'_> {
                EncodedBytes::Owned(SharedBytes::copy_from_slice(&self.to_be_bytes()))
            }
        }

        impl CodecKeyDecode for $ty {
            #[inline(always)]
            fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
                <$ty as CodecValue>::decode_owned(bytes)
            }
        }

        impl CodecValueEncode for $ty {
            #[inline(always)]
            fn encode_value(&self) -> EncodedBytes<'_> {
                EncodedBytes::Owned(SharedBytes::copy_from_slice(&self.to_be_bytes()))
            }
        }

        impl CodecValue for $ty {
            type Borrowed<'a> = $ty;

            #[inline(always)]
            fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError> {
                const WIDTH: usize = size_of::<$ty>();
                if bytes.len() != WIDTH {
                    return Err(CodecError::InvalidLength {
                        expected: WIDTH,
                        actual: bytes.len(),
                    });
                }
                let mut array = [0u8; WIDTH];
                array.copy_from_slice(bytes);
                Ok(<$ty>::from_be_bytes(array))
            }

            #[inline(always)]
            fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError> {
                Self::decode_owned(bytes)
            }
        }
    };
}

impl_fixed_width_codec!(u8);
impl_fixed_width_codec!(u16);
impl_fixed_width_codec!(u32);
impl_fixed_width_codec!(u64);
impl_fixed_width_codec!(u128);
impl_fixed_width_codec!(i8);
impl_fixed_width_codec!(i16);
impl_fixed_width_codec!(i32);
impl_fixed_width_codec!(i64);
impl_fixed_width_codec!(i128);

impl CodecKey for bool {
    #[inline(always)]
    fn encode_key(&self) -> EncodedBytes<'_> {
        EncodedBytes::Owned(SharedBytes::copy_from_slice(&[*self as u8]))
    }
}

impl CodecKeyDecode for bool {
    #[inline(always)]
    fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
        <bool as CodecValue>::decode_owned(bytes)
    }
}

impl CodecValueEncode for bool {
    #[inline(always)]
    fn encode_value(&self) -> EncodedBytes<'_> {
        EncodedBytes::Owned(SharedBytes::copy_from_slice(&[*self as u8]))
    }
}

impl CodecValue for bool {
    type Borrowed<'a> = bool;

    #[inline(always)]
    fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError> {
        match bytes {
            [0] => Ok(false),
            [1] => Ok(true),
            [byte] => Err(CodecError::InvalidBool(*byte)),
            _ => Err(CodecError::InvalidLength {
                expected: 1,
                actual: bytes.len(),
            }),
        }
    }

    #[inline(always)]
    fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError> {
        Self::decode_owned(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn typed_string_map_round_trips_owned_and_borrowed() {
        let map: CodecShardMap<String, String, 4> = CodecShardMap::with_capacity(16);

        map.insert_ref("user:42", "ready");
        assert_eq!(map.get("user:42").unwrap().as_deref(), Some("ready"));

        {
            let value = map.get_ref("user:42").unwrap();
            assert_eq!(value.value().unwrap(), "ready");
        }

        map.insert("user:42".to_owned(), "done".to_owned());
        assert_eq!(map.remove("user:42").unwrap().as_deref(), Some("done"));
        assert!(!map.contains_key("user:42"));
    }

    #[cfg(not(feature = "no-ttl"))]
    #[test]
    fn default_ttl_is_namespace_local_for_shared_engine_facades() {
        let raw = SharedCache::<4>::new();
        let expiring: CodecShardMap<String, String, 4> =
            CodecShardMap::from_shared_engine_with_options(
                SharedBytes::from_static(b"expiring"),
                raw.clone(),
                CacheOptions {
                    default_ttl_ms: Some(20),
                    ..CacheOptions::default()
                },
            )
            .unwrap();
        let durable: CodecShardMap<String, String, 4> =
            CodecShardMap::from_shared_engine(SharedBytes::from_static(b"durable"), raw).unwrap();

        assert_eq!(expiring.default_ttl_ms(), Some(20));
        assert_eq!(durable.default_ttl_ms(), None);

        expiring.insert_ref("same-key", "expires");
        durable.insert_ref("same-key", "stays");
        std::thread::sleep(std::time::Duration::from_millis(30));

        assert_eq!(expiring.get("same-key").unwrap(), None);
        assert_eq!(durable.get("same-key").unwrap().as_deref(), Some("stays"));

        expiring.insert_ref_with_ttl("override", "stays", None);
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert_eq!(expiring.get("override").unwrap().as_deref(), Some("stays"));
    }

    #[test]
    fn typed_numeric_keys_use_stable_byte_order() {
        let map: CodecShardMap<u64, String, 4> = CodecShardMap::new();

        map.insert(42, "answer".to_owned());

        assert_eq!(map.get(&42).unwrap().as_deref(), Some("answer"));
        assert!(map.contains_key(&42));
        assert!(!map.contains_key(&43));
    }

    #[test]
    fn built_in_numeric_and_bool_codecs_round_trip_boundary_values() {
        macro_rules! assert_fixed_width_round_trip {
            ($ty:ty, $key:expr, $value:expr) => {{
                let map: CodecShardMap<$ty, $ty, 4> = CodecShardMap::new();
                let key: $ty = $key;
                let value: $ty = $value;

                map.insert(key, value);

                assert_eq!(map.get(&key).unwrap(), Some(value));
                assert_eq!(map.get_ref(&key).unwrap().value().unwrap(), value);
                assert_eq!(map.keys().unwrap(), vec![key]);
                assert_eq!(map.entries().unwrap(), vec![(key, value)]);
                assert_eq!(map.remove(&key).unwrap(), Some(value));
                assert!(!map.contains_key(&key));
            }};
        }

        assert_fixed_width_round_trip!(u8, u8::MIN, u8::MAX);
        assert_fixed_width_round_trip!(u16, u16::MIN, u16::MAX);
        assert_fixed_width_round_trip!(u32, u32::MIN, u32::MAX);
        assert_fixed_width_round_trip!(u64, u64::MIN, u64::MAX);
        assert_fixed_width_round_trip!(u128, u128::MIN, u128::MAX);
        assert_fixed_width_round_trip!(i8, i8::MIN, i8::MAX);
        assert_fixed_width_round_trip!(i16, i16::MIN, i16::MAX);
        assert_fixed_width_round_trip!(i32, i32::MIN, i32::MAX);
        assert_fixed_width_round_trip!(i64, i64::MIN, i64::MAX);
        assert_fixed_width_round_trip!(i128, i128::MIN, i128::MAX);

        let bools: CodecShardMap<bool, bool, 4> = CodecShardMap::new();
        bools.insert(true, false);

        assert_eq!(bools.get(&true).unwrap(), Some(false));
        assert!(!bools.get_ref(&true).unwrap().value().unwrap());
        assert_eq!(bools.keys().unwrap(), vec![true]);
        assert_eq!(bools.entries().unwrap(), vec![(true, false)]);
        assert_eq!(bools.remove(&true).unwrap(), Some(false));
        assert!(!bools.contains_key(&true));
    }

    #[test]
    fn byte_and_string_codecs_cover_owned_borrowed_and_fixed_array_inputs() {
        let strings: CodecShardMap<String, String, 4> = CodecShardMap::new();
        strings.insert_ref("unicode:☃", "value:☃");
        assert_eq!(
            strings.get("unicode:☃").unwrap().as_deref(),
            Some("value:☃")
        );
        assert_eq!(
            strings.get_ref("unicode:☃").unwrap().value().unwrap(),
            "value:☃"
        );

        let byte_slices: CodecShardMap<Vec<u8>, Vec<u8>, 4> = CodecShardMap::new();
        let binary_key = b"\0key\xff".as_slice();
        let binary_value = b"\0value\xfe".as_slice();
        byte_slices.insert_ref(binary_key, binary_value);
        assert_eq!(
            byte_slices.get(binary_key).unwrap().as_deref(),
            Some(binary_value)
        );
        assert_eq!(
            byte_slices.get_ref(binary_key).unwrap().value().unwrap(),
            binary_value
        );
        assert_eq!(byte_slices.keys().unwrap(), vec![binary_key.to_vec()]);
        assert_eq!(
            byte_slices.entries().unwrap(),
            vec![(binary_key.to_vec(), binary_value.to_vec())]
        );

        let shared: CodecShardMap<SharedBytes, SharedBytes, 4> = CodecShardMap::new();
        let bytes_key = SharedBytes::from_static(b"bytes-key");
        let bytes_value = SharedBytes::from_static(b"bytes-value");
        shared.insert(bytes_key.clone(), bytes_value.clone());
        assert_eq!(shared.get(&bytes_key).unwrap(), Some(bytes_value.clone()));
        assert_eq!(
            shared.get_ref(&bytes_key).unwrap().value().unwrap(),
            bytes_value.as_ref()
        );

        let arrays: CodecShardMap<[u8; 4], Vec<u8>, 4> = CodecShardMap::new();
        let array_key = [0, 1, 2, 255];
        let array_value = [9, 8, 7, 6];
        arrays.insert_ref(&array_key, &array_value);
        assert_eq!(arrays.get(&array_key).unwrap(), Some(array_value.to_vec()));
        assert_eq!(
            arrays.get_ref(&array_key).unwrap().value().unwrap(),
            array_value.as_slice()
        );
    }

    #[test]
    fn built_in_codecs_reject_invalid_user_input_bytes() {
        assert_eq!(
            <String as CodecValue>::decode_owned(&[0xff]),
            Err(CodecError::InvalidUtf8)
        );
        assert_eq!(
            <u64 as CodecValue>::decode_owned(&[1, 2, 3]),
            Err(CodecError::InvalidLength {
                expected: size_of::<u64>(),
                actual: 3,
            })
        );
        assert_eq!(
            <bool as CodecValue>::decode_owned(&[2]),
            Err(CodecError::InvalidBool(2))
        );
        assert_eq!(
            <bool as CodecValue>::decode_owned(&[0, 1]),
            Err(CodecError::InvalidLength {
                expected: 1,
                actual: 2,
            })
        );
    }

    #[test]
    fn bytes_values_can_be_read_without_copying() {
        let map: CodecShardMap<String, SharedBytes, 4> = CodecShardMap::new();
        let bytes = SharedBytes::from_static(b"payload");

        map.insert("blob".to_owned(), bytes.clone());

        let owned = map.get("blob").unwrap().unwrap();
        assert_eq!(owned, bytes);
        let borrowed = map.get_ref("blob").unwrap();
        assert_eq!(borrowed.value().unwrap(), b"payload");
    }

    #[derive(Debug, PartialEq, Eq)]
    struct User {
        id: u64,
        name: String,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct UserRef<'a> {
        id: u64,
        name: &'a str,
    }

    #[derive(Clone, Debug, Ord, PartialOrd, PartialEq, Eq)]
    struct UserKey {
        tenant: u16,
        id: u64,
    }

    impl CodecKey for UserKey {
        fn encode_key(&self) -> EncodedBytes<'_> {
            let mut bytes = BytesMut::with_capacity(size_of::<u16>() + size_of::<u64>());
            bytes.extend_from_slice(&self.tenant.to_be_bytes());
            bytes.extend_from_slice(&self.id.to_be_bytes());
            EncodedBytes::Owned(bytes.freeze())
        }
    }

    impl CodecKeyDecode for UserKey {
        fn decode_key(bytes: &[u8]) -> Result<Self, CodecError> {
            let expected = size_of::<u16>() + size_of::<u64>();
            if bytes.len() != expected {
                return Err(CodecError::InvalidLength {
                    expected,
                    actual: bytes.len(),
                });
            }
            let (tenant, id) = bytes.split_at(size_of::<u16>());
            Ok(Self {
                tenant: u16::from_be_bytes(tenant.try_into().unwrap()),
                id: u64::from_be_bytes(id.try_into().unwrap()),
            })
        }
    }

    impl CodecValueEncode for User {
        fn encode_value(&self) -> EncodedBytes<'_> {
            let mut bytes = BytesMut::with_capacity(size_of::<u64>() + self.name.len());
            bytes.extend_from_slice(&self.id.to_be_bytes());
            bytes.extend_from_slice(self.name.as_bytes());
            EncodedBytes::Owned(bytes.freeze())
        }
    }

    impl CodecValue for User {
        type Borrowed<'a> = UserRef<'a>;

        fn decode_owned(bytes: &[u8]) -> Result<Self, CodecError> {
            let borrowed = Self::decode_borrowed(bytes)?;
            Ok(Self {
                id: borrowed.id,
                name: borrowed.name.to_owned(),
            })
        }

        fn decode_borrowed(bytes: &[u8]) -> Result<Self::Borrowed<'_>, CodecError> {
            let (id, name) =
                bytes
                    .split_at_checked(size_of::<u64>())
                    .ok_or(CodecError::InvalidLength {
                        expected: size_of::<u64>(),
                        actual: bytes.len(),
                    })?;
            Ok(UserRef {
                id: u64::from_be_bytes(id.try_into().unwrap()),
                name: std::str::from_utf8(name)?,
            })
        }
    }

    #[test]
    fn custom_struct_codecs_support_owned_and_borrowed_views() {
        let map: CodecShardMap<String, User, 4> = CodecShardMap::new();

        map.insert(
            "devon".to_owned(),
            User {
                id: 7,
                name: "Devon".to_owned(),
            },
        );

        assert_eq!(
            map.get("devon").unwrap(),
            Some(User {
                id: 7,
                name: "Devon".to_owned(),
            })
        );
        assert_eq!(
            map.get_ref("devon").unwrap().value().unwrap(),
            UserRef {
                id: 7,
                name: "Devon",
            }
        );
    }

    #[test]
    fn custom_struct_key_and_value_codecs_round_trip_entries() {
        let map: CodecShardMap<UserKey, User, 4> = CodecShardMap::new();
        let key = UserKey {
            tenant: 11,
            id: u64::MAX,
        };
        let value = User {
            id: 7,
            name: "Devon".to_owned(),
        };

        map.insert(key.clone(), value);

        assert_eq!(
            map.get(&key).unwrap(),
            Some(User {
                id: 7,
                name: "Devon".to_owned(),
            })
        );
        assert_eq!(
            map.get_ref(&key).unwrap().value().unwrap(),
            UserRef {
                id: 7,
                name: "Devon",
            }
        );
        assert_eq!(map.keys().unwrap(), vec![key.clone()]);
        assert_eq!(
            map.entries().unwrap(),
            vec![(
                key,
                User {
                    id: 7,
                    name: "Devon".to_owned(),
                }
            )]
        );
    }

    #[cfg(feature = "codec")]
    #[test]
    fn codec_feature_allows_namespaced_facades_to_share_one_engine() {
        let raw = SharedCache::<4>::new();
        let strings: CodecShardMap<String, String, 4> =
            CodecShardMap::from_shared_engine(SharedBytes::from_static(b"strings"), raw.clone())
                .unwrap();
        let numbers: CodecShardMap<u64, String, 4> =
            CodecShardMap::from_shared_engine(SharedBytes::from_static(b"numbers"), raw).unwrap();

        strings.insert_ref("42", "string");
        numbers.insert(42, "number".to_owned());

        assert_eq!(strings.get("42").unwrap().as_deref(), Some("string"));
        assert_eq!(numbers.get(&42).unwrap().as_deref(), Some("number"));
        assert_eq!(strings.keys().unwrap(), vec!["42".to_owned()]);
        assert_eq!(numbers.keys().unwrap(), vec![42]);
        assert_eq!(
            strings.entries().unwrap(),
            vec![("42".to_owned(), "string".to_owned())]
        );
        assert_eq!(numbers.entries().unwrap(), vec![(42, "number".to_owned())]);
        assert_ne!(strings.route_key("42").shard_id, usize::MAX);
    }

    #[cfg(feature = "codec")]
    #[test]
    fn shared_engine_facades_require_non_empty_namespaces() {
        let raw = SharedCache::<4>::new();
        let result =
            CodecShardMap::<String, String, 4>::from_shared_engine(SharedBytes::new(), raw);

        assert_eq!(
            result.unwrap_err().to_string(),
            CodecError::EmptyNamespace.to_string()
        );
    }
}
