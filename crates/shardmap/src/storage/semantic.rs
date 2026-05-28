use bytes::Bytes as SharedBytes;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use thiserror::Error;

use crate::storage::Bytes;

const LSH_MIN_ROWS: usize = 512;
const LSH_PLANES: usize = 64;
const LSH_MAX_CANDIDATES: usize = 1024;
const LSH_BUCKET_MAX_CANDIDATES: usize = 320;
const LSH_MIN_CANDIDATES: usize = 8;
const LSH_SPARSE_COMPONENTS: usize = 32;
const LSH_BAND_BITS: usize = 8;
const LSH_BANDS: usize = LSH_PLANES / LSH_BAND_BITS;
const LSH_BAND_MASK: u64 = (1u64 << LSH_BAND_BITS) - 1;

/// A best-match semantic cache hit.
#[derive(Debug, Clone, PartialEq)]
pub struct SemanticMatch {
    /// Cache key that produced the match.
    pub key: Bytes,
    /// Cached value stored for `key`.
    pub value: SharedBytes,
    /// Opaque governance metadata stored with the semantic cache entry.
    ///
    /// Applications can encode tenant, subject, ACL, policy version, source
    /// document IDs, or other authorization context here and validate it before
    /// serving a cross-user semantic cache hit.
    pub governance: SharedBytes,
    /// Cosine similarity score between the query and stored embedding.
    pub score: f32,
}

/// Error returned by semantic cache APIs.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum SemanticCacheError {
    /// Embeddings must contain at least one component.
    #[error("semantic embeddings cannot be empty")]
    EmptyEmbedding,
    /// Embeddings and thresholds must be finite floating-point values.
    #[error("semantic embeddings and thresholds must be finite")]
    NonFinite,
    /// Embeddings with zero magnitude cannot be normalized for cosine search.
    #[error("semantic embeddings must have non-zero magnitude")]
    ZeroMagnitude,
}

#[derive(Debug, Clone)]
pub(crate) struct SemanticEmbedding {
    vector: Box<[f32]>,
}

impl SemanticEmbedding {
    pub(crate) fn from_slice(values: &[f32]) -> Result<Self, SemanticCacheError> {
        if values.is_empty() {
            return Err(SemanticCacheError::EmptyEmbedding);
        }

        let mut norm_squared = 0.0f64;
        for value in values {
            if !value.is_finite() {
                return Err(SemanticCacheError::NonFinite);
            }
            let value = f64::from(*value);
            norm_squared += value * value;
        }
        if norm_squared == 0.0 {
            return Err(SemanticCacheError::ZeroMagnitude);
        }

        let norm = norm_squared.sqrt();
        let vector = values
            .iter()
            .map(|value| (f64::from(*value) / norm) as f32)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self { vector })
    }

    #[inline(always)]
    pub(crate) fn as_slice(&self) -> &[f32] {
        &self.vector
    }

    #[inline(always)]
    fn stored_bytes(&self) -> usize {
        self.vector.len().saturating_mul(std::mem::size_of::<f32>())
    }

    #[inline(always)]
    pub(crate) fn cosine_similarity(&self, query: &Self) -> Option<f32> {
        if self.vector.len() != query.vector.len() {
            return None;
        }
        Some(dot_product(self.vector.as_ref(), query.vector.as_ref()))
    }
}

#[inline(always)]
pub(crate) fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    dot_product_same_len(left, right)
}

#[inline(always)]
fn dot_product_same_len(left: &[f32], right: &[f32]) -> f32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            // SAFETY: feature detection above guarantees AVX2/FMA support, and
            // both slices have the same length by construction.
            return unsafe { dot_product_avx2_fma(left, right) };
        }
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: feature detection above guarantees AVX2 support, and both
            // slices have the same length by construction.
            return unsafe { dot_product_avx2(left, right) };
        }
    }

    dot_product_scalar(left, right)
}

#[inline(always)]
fn dot_product_scalar(left: &[f32], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    let mut a0 = 0.0f32;
    let mut a1 = 0.0f32;
    let mut a2 = 0.0f32;
    let mut a3 = 0.0f32;
    let mut a4 = 0.0f32;
    let mut a5 = 0.0f32;
    let mut a6 = 0.0f32;
    let mut a7 = 0.0f32;

    let len = left.len();
    let mut index = 0;
    while index + 8 <= len {
        // SAFETY: `index + 8 <= len`, and both slices have equal length.
        unsafe {
            a0 += *left.get_unchecked(index) * *right.get_unchecked(index);
            a1 += *left.get_unchecked(index + 1) * *right.get_unchecked(index + 1);
            a2 += *left.get_unchecked(index + 2) * *right.get_unchecked(index + 2);
            a3 += *left.get_unchecked(index + 3) * *right.get_unchecked(index + 3);
            a4 += *left.get_unchecked(index + 4) * *right.get_unchecked(index + 4);
            a5 += *left.get_unchecked(index + 5) * *right.get_unchecked(index + 5);
            a6 += *left.get_unchecked(index + 6) * *right.get_unchecked(index + 6);
            a7 += *left.get_unchecked(index + 7) * *right.get_unchecked(index + 7);
        }
        index += 8;
    }

    let mut sum = (a0 + a1) + (a2 + a3) + (a4 + a5) + (a6 + a7);
    while index < len {
        // SAFETY: `index < len`, and both slices have equal length.
        unsafe {
            sum += *left.get_unchecked(index) * *right.get_unchecked(index);
        }
        index += 1;
    }
    sum
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_product_avx2_fma(left: &[f32], right: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(left.len(), right.len());

    let len = left.len();
    let mut index = 0;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();

    while index + 32 <= len {
        // SAFETY: `index + 32 <= len`, and both slices have equal length.
        unsafe {
            let left0 = _mm256_loadu_ps(left.as_ptr().add(index));
            let right0 = _mm256_loadu_ps(right.as_ptr().add(index));
            acc0 = _mm256_fmadd_ps(left0, right0, acc0);

            let left1 = _mm256_loadu_ps(left.as_ptr().add(index + 8));
            let right1 = _mm256_loadu_ps(right.as_ptr().add(index + 8));
            acc1 = _mm256_fmadd_ps(left1, right1, acc1);

            let left2 = _mm256_loadu_ps(left.as_ptr().add(index + 16));
            let right2 = _mm256_loadu_ps(right.as_ptr().add(index + 16));
            acc2 = _mm256_fmadd_ps(left2, right2, acc2);

            let left3 = _mm256_loadu_ps(left.as_ptr().add(index + 24));
            let right3 = _mm256_loadu_ps(right.as_ptr().add(index + 24));
            acc3 = _mm256_fmadd_ps(left3, right3, acc3);
        }
        index += 32;
    }

    let acc = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    let mut lanes = [0.0f32; 8];
    // SAFETY: `lanes` has room for exactly one 256-bit vector.
    unsafe {
        _mm256_storeu_ps(lanes.as_mut_ptr(), acc);
    }
    let mut sum = lanes.into_iter().sum::<f32>();

    while index < len {
        // SAFETY: `index < len`, and both slices have equal length.
        unsafe {
            sum += *left.get_unchecked(index) * *right.get_unchecked(index);
        }
        index += 1;
    }
    sum
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn dot_product_avx2(left: &[f32], right: &[f32]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    debug_assert_eq!(left.len(), right.len());

    let len = left.len();
    let mut index = 0;
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();

    while index + 32 <= len {
        // SAFETY: `index + 32 <= len`, and both slices have equal length.
        unsafe {
            let left0 = _mm256_loadu_ps(left.as_ptr().add(index));
            let right0 = _mm256_loadu_ps(right.as_ptr().add(index));
            acc0 = _mm256_add_ps(_mm256_mul_ps(left0, right0), acc0);

            let left1 = _mm256_loadu_ps(left.as_ptr().add(index + 8));
            let right1 = _mm256_loadu_ps(right.as_ptr().add(index + 8));
            acc1 = _mm256_add_ps(_mm256_mul_ps(left1, right1), acc1);

            let left2 = _mm256_loadu_ps(left.as_ptr().add(index + 16));
            let right2 = _mm256_loadu_ps(right.as_ptr().add(index + 16));
            acc2 = _mm256_add_ps(_mm256_mul_ps(left2, right2), acc2);

            let left3 = _mm256_loadu_ps(left.as_ptr().add(index + 24));
            let right3 = _mm256_loadu_ps(right.as_ptr().add(index + 24));
            acc3 = _mm256_add_ps(_mm256_mul_ps(left3, right3), acc3);
        }
        index += 32;
    }

    let acc = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    let mut lanes = [0.0f32; 8];
    // SAFETY: `lanes` has room for exactly one 256-bit vector.
    unsafe {
        _mm256_storeu_ps(lanes.as_mut_ptr(), acc);
    }
    let mut sum = lanes.into_iter().sum::<f32>();

    while index < len {
        // SAFETY: `index < len`, and both slices have equal length.
        unsafe {
            sum += *left.get_unchecked(index) * *right.get_unchecked(index);
        }
        index += 1;
    }
    sum
}

pub(crate) fn validate_similarity_threshold(threshold: f32) -> Result<f32, SemanticCacheError> {
    match threshold.is_finite() {
        true => Ok(threshold),
        false => Err(SemanticCacheError::NonFinite),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SemanticIndexToken {
    id: u64,
    dims: usize,
}

impl SemanticIndexToken {
    #[inline(always)]
    pub(crate) fn id(self) -> u64 {
        self.id
    }

    #[inline(always)]
    pub(crate) fn stored_bytes(self) -> usize {
        self.dims.saturating_mul(std::mem::size_of::<f32>())
    }
}

#[derive(Debug)]
pub(crate) struct SemanticIndexCandidate<'a> {
    pub(crate) id: u64,
    pub(crate) hash: u64,
    pub(crate) key: &'a [u8],
    pub(crate) score: f32,
}

#[derive(Debug, Default)]
pub(crate) struct SemanticIndex {
    next_id: u64,
    partitions: HashMap<usize, SemanticIndexPartition>,
}

impl SemanticIndex {
    pub(crate) fn insert(
        &mut self,
        hash: u64,
        key: &[u8],
        embedding: &SemanticEmbedding,
    ) -> SemanticIndexToken {
        let dims = embedding.as_slice().len();
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);

        let partition = self.partitions.entry(dims).or_default();
        let row = partition.entries.len();
        partition.vectors.extend_from_slice(embedding.as_slice());
        partition.entries.push(SemanticIndexEntry {
            id,
            hash,
            key: key.to_vec().into_boxed_slice(),
        });
        partition
            .exact
            .entry(embedding_fingerprint(embedding.as_slice()))
            .or_default()
            .push(row);
        partition.insert_lsh(row, embedding.as_slice());

        SemanticIndexToken { id, dims }
    }

    pub(crate) fn search<T>(
        &self,
        query: &SemanticEmbedding,
        min_score: f32,
        mut accept: impl FnMut(SemanticIndexCandidate<'_>) -> Option<T>,
    ) -> Option<T> {
        let dims = query.as_slice().len();
        let partition = self.partitions.get(&dims)?;
        partition.search(query.as_slice(), min_score, &mut accept)
    }

    pub(crate) fn search_exact<T>(
        &self,
        query: &SemanticEmbedding,
        min_score: f32,
        mut accept: impl FnMut(SemanticIndexCandidate<'_>) -> Option<T>,
    ) -> Option<T> {
        let dims = query.as_slice().len();
        let partition = self.partitions.get(&dims)?;
        partition.search_exact(query.as_slice(), min_score, &mut accept)
    }
}

#[derive(Debug, Default)]
struct SemanticIndexPartition {
    vectors: Vec<f32>,
    entries: Vec<SemanticIndexEntry>,
    exact: HashMap<u64, Vec<usize>>,
    signatures: Vec<u64>,
    lsh_buckets: HashMap<u16, Vec<usize>>,
}

impl SemanticIndexPartition {
    fn insert_lsh(&mut self, row: usize, embedding: &[f32]) {
        let signature = lsh_signature(embedding);
        self.signatures.push(signature);
        for band in 0..LSH_BANDS {
            self.lsh_buckets
                .entry(lsh_bucket_key(signature, band))
                .or_default()
                .push(row);
        }
    }

    fn search<T>(
        &self,
        query: &[f32],
        min_score: f32,
        accept: &mut impl FnMut(SemanticIndexCandidate<'_>) -> Option<T>,
    ) -> Option<T> {
        debug_assert_eq!(
            self.vectors.len(),
            self.entries.len().saturating_mul(query.len())
        );

        if min_score <= 1.0
            && let Some(exact) = self.search_exact(query, min_score, accept)
        {
            return Some(exact);
        }

        if let Some(lsh) = self.search_lsh(query, min_score, accept) {
            return Some(lsh);
        }
        if self.should_use_lsh(query) {
            return None;
        }

        let mut best_score = min_score;
        let mut best = None;
        for (index, entry) in self.entries.iter().enumerate() {
            let start = index.saturating_mul(query.len());
            let end = start.saturating_add(query.len());
            let score = dot_product_same_len(&self.vectors[start..end], query);
            if score < best_score {
                continue;
            }
            let candidate = SemanticIndexCandidate {
                id: entry.id,
                hash: entry.hash,
                key: entry.key.as_ref(),
                score,
            };
            if let Some(accepted) = accept(candidate) {
                best_score = score;
                best = Some(accepted);
            }
        }
        best
    }

    fn search_exact<T>(
        &self,
        query: &[f32],
        min_score: f32,
        accept: &mut impl FnMut(SemanticIndexCandidate<'_>) -> Option<T>,
    ) -> Option<T> {
        let rows = self.exact.get(&embedding_fingerprint(query))?;
        for row in rows {
            let start = row.saturating_mul(query.len());
            let end = start.saturating_add(query.len());
            if &self.vectors[start..end] != query {
                continue;
            }
            let score = 1.0;
            if score < min_score {
                continue;
            }
            let entry = &self.entries[*row];
            let candidate = SemanticIndexCandidate {
                id: entry.id,
                hash: entry.hash,
                key: entry.key.as_ref(),
                score,
            };
            if let Some(accepted) = accept(candidate) {
                return Some(accepted);
            }
        }
        None
    }

    fn search_lsh<T>(
        &self,
        query: &[f32],
        min_score: f32,
        accept: &mut impl FnMut(SemanticIndexCandidate<'_>) -> Option<T>,
    ) -> Option<T> {
        if !self.should_use_lsh(query) {
            return None;
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if std::is_x86_feature_detected!("popcnt") {
                // SAFETY: feature detection above guarantees POPCNT support.
                return unsafe { self.search_lsh_popcnt(query, min_score, accept) };
            }
        }

        self.search_lsh_portable(query, min_score, accept)
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "popcnt")]
    unsafe fn search_lsh_popcnt<T>(
        &self,
        query: &[f32],
        min_score: f32,
        accept: &mut impl FnMut(SemanticIndexCandidate<'_>) -> Option<T>,
    ) -> Option<T> {
        self.search_lsh_inner::<true, T>(query, min_score, accept)
    }

    fn search_lsh_portable<T>(
        &self,
        query: &[f32],
        min_score: f32,
        accept: &mut impl FnMut(SemanticIndexCandidate<'_>) -> Option<T>,
    ) -> Option<T> {
        self.search_lsh_inner::<false, T>(query, min_score, accept)
    }

    fn search_lsh_inner<const USE_POPCNT: bool, T>(
        &self,
        query: &[f32],
        min_score: f32,
        accept: &mut impl FnMut(SemanticIndexCandidate<'_>) -> Option<T>,
    ) -> Option<T> {
        let signature = lsh_signature(query);
        let max_distance = lsh_max_hamming_distance(min_score);
        let scan_candidate_limit = self.lsh_candidate_limit();
        let bucket_rows = self.lsh_bucket_rows(signature);
        if !bucket_rows.is_empty() && bucket_rows.len().saturating_mul(2) < self.signatures.len() {
            let bucket_candidate_limit = scan_candidate_limit.min(LSH_BUCKET_MAX_CANDIDATES);
            return self.search_lsh_rows::<USE_POPCNT, _, T>(
                bucket_rows,
                signature,
                max_distance,
                bucket_candidate_limit,
                query,
                min_score,
                accept,
            );
        }

        self.search_lsh_rows::<USE_POPCNT, _, T>(
            0..self.signatures.len(),
            signature,
            max_distance,
            scan_candidate_limit,
            query,
            min_score,
            accept,
        )
    }

    fn search_lsh_rows<const USE_POPCNT: bool, I, T>(
        &self,
        rows: I,
        signature: u64,
        max_distance: u32,
        candidate_limit: usize,
        query: &[f32],
        min_score: f32,
        accept: &mut impl FnMut(SemanticIndexCandidate<'_>) -> Option<T>,
    ) -> Option<T>
    where
        I: IntoIterator<Item = usize>,
    {
        let mut candidates = BinaryHeap::with_capacity(candidate_limit.saturating_add(1));
        for row in rows {
            let Some(stored_signature) = self.signatures.get(row).copied() else {
                continue;
            };
            let distance = hamming_distance::<USE_POPCNT>(signature ^ stored_signature);
            if distance > max_distance {
                continue;
            }
            let candidate = SignatureCandidate { distance, row };
            if candidates.len() < candidate_limit {
                candidates.push(candidate);
                continue;
            }
            let Some(mut worst) = candidates.peek_mut() else {
                continue;
            };
            if candidate < *worst {
                *worst = candidate;
            }
        }
        if candidates.is_empty() {
            return None;
        }

        let mut best_score = min_score;
        let mut best = None;
        for candidate in candidates.into_sorted_vec() {
            let row = candidate.row;
            let start = row.saturating_mul(query.len());
            let end = start.saturating_add(query.len());
            let score = dot_product_same_len(&self.vectors[start..end], query);
            if score < best_score {
                continue;
            }
            let entry = &self.entries[row];
            let candidate = SemanticIndexCandidate {
                id: entry.id,
                hash: entry.hash,
                key: entry.key.as_ref(),
                score,
            };
            if let Some(accepted) = accept(candidate) {
                best_score = score;
                best = Some(accepted);
            }
        }
        best
    }

    fn lsh_bucket_rows(&self, signature: u64) -> Vec<usize> {
        let mut rows = Vec::new();
        let mut seen = vec![0u64; self.entries.len().div_ceil(64)];
        for band in 0..LSH_BANDS {
            self.extend_lsh_bucket_rows(signature, band, &mut rows, &mut seen);
            for bit in 0..LSH_BAND_BITS {
                self.extend_lsh_bucket_rows(
                    signature ^ (1u64 << (band * LSH_BAND_BITS + bit)),
                    band,
                    &mut rows,
                    &mut seen,
                );
            }
        }
        rows
    }

    fn extend_lsh_bucket_rows(
        &self,
        signature: u64,
        band: usize,
        rows: &mut Vec<usize>,
        seen: &mut [u64],
    ) {
        let Some(bucket) = self.lsh_buckets.get(&lsh_bucket_key(signature, band)) else {
            return;
        };
        rows.reserve(bucket.len());
        for row in bucket.iter().copied() {
            let Some(seen_word) = seen.get_mut(row / 64) else {
                continue;
            };
            let seen_bit = 1u64 << (row % 64);
            if *seen_word & seen_bit != 0 {
                continue;
            }
            *seen_word |= seen_bit;
            rows.push(row);
        }
    }

    #[inline(always)]
    fn should_use_lsh(&self, query: &[f32]) -> bool {
        self.entries.len() >= LSH_MIN_ROWS
            && query.len() >= LSH_SPARSE_COMPONENTS
            && self.signatures.len() == self.entries.len()
    }

    #[inline(always)]
    fn lsh_candidate_limit(&self) -> usize {
        self.entries
            .len()
            .div_ceil(192)
            .clamp(LSH_MIN_CANDIDATES, LSH_MAX_CANDIDATES)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SignatureCandidate {
    distance: u32,
    row: usize,
}

impl Ord for SignatureCandidate {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.distance
            .cmp(&other.distance)
            .then_with(|| self.row.cmp(&other.row))
    }
}

impl PartialOrd for SignatureCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct SemanticIndexEntry {
    id: u64,
    hash: u64,
    key: Box<[u8]>,
}

#[inline(always)]
fn embedding_fingerprint(values: &[f32]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for value in values {
        hash ^= u64::from(value.to_bits());
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn lsh_signature(values: &[f32]) -> u64 {
    let mut signature = 0u64;
    for plane in 0..LSH_PLANES {
        let mut projection = 0.0f32;
        for component in 0..LSH_SPARSE_COMPONENTS {
            let mixed =
                lsh_mix(((plane as u64) << 32) ^ ((component as u64) << 16) ^ values.len() as u64);
            let index = (mixed as usize) % values.len();
            let sign = if (mixed >> 63) == 0 { 1.0 } else { -1.0 };
            // SAFETY: `index` is reduced modulo `values.len()`.
            projection += unsafe { *values.get_unchecked(index) } * sign;
        }
        if projection >= 0.0 {
            signature |= 1u64 << plane;
        }
    }
    signature
}

#[inline(always)]
fn lsh_bucket_key(signature: u64, band: usize) -> u16 {
    debug_assert!(band < LSH_BANDS);
    let band_bits = ((signature >> (band * LSH_BAND_BITS)) & LSH_BAND_MASK) as u16;
    ((band as u16) << LSH_BAND_BITS) | band_bits
}

#[inline(always)]
fn hamming_distance<const USE_POPCNT: bool>(bits: u64) -> u32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if USE_POPCNT {
        return bits.count_ones();
    }

    let mut count = bits;
    count = count.wrapping_sub((count >> 1) & 0x5555_5555_5555_5555);
    count = (count & 0x3333_3333_3333_3333) + ((count >> 2) & 0x3333_3333_3333_3333);
    (((count + (count >> 4)) & 0x0f0f_0f0f_0f0f_0f0f).wrapping_mul(0x0101_0101_0101_0101) >> 56)
        as u32
}

#[inline(always)]
fn lsh_max_hamming_distance(min_score: f32) -> u32 {
    let score = min_score.clamp(-1.0, 1.0);
    let expected_distance = score.acos() / std::f32::consts::PI * LSH_PLANES as f32;
    expected_distance.ceil() as u32 + 6
}

#[inline(always)]
fn lsh_mix(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51afd7ed558ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ceb9fe1a85ec53);
    value ^ (value >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_vector(seed: usize, dims: usize) -> Vec<f32> {
        (0..dims)
            .map(|component| {
                let mixed = lsh_mix(((seed as u64) << 32) ^ component as u64);
                let magnitude = ((mixed >> 8) & 0xff) as f32 + 1.0;
                if mixed & 1 == 0 {
                    magnitude
                } else {
                    -magnitude
                }
            })
            .collect()
    }

    #[test]
    fn banded_lsh_candidates_include_matching_signature() {
        let dims = LSH_SPARSE_COMPONENTS;
        let mut index = SemanticIndex::default();

        for seed in 0..(LSH_MIN_ROWS + 64) {
            let embedding = SemanticEmbedding::from_slice(&test_vector(seed + 1, dims)).unwrap();
            let key = format!("entry-{seed}");
            index.insert(seed as u64, key.as_bytes(), &embedding);
        }

        let mut target = vec![0.0f32; dims];
        target[0] = 1.0;
        let target_embedding = SemanticEmbedding::from_slice(&target).unwrap();
        index.insert(9_999, b"target", &target_embedding);

        let partition = index.partitions.get(&dims).unwrap();
        let target_row = partition.entries.len() - 1;
        let rows = partition.lsh_bucket_rows(lsh_signature(target_embedding.as_slice()));

        assert!(rows.contains(&target_row));
        assert!(rows.len() < partition.signatures.len());

        let hit = partition
            .search_lsh(target_embedding.as_slice(), 0.99, &mut |candidate| {
                Some((candidate.hash, candidate.key.to_vec(), candidate.score))
            })
            .unwrap();

        assert_eq!(hit.0, 9_999);
        assert_eq!(hit.1, b"target");
        assert!(hit.2 >= 0.99);
    }
}
