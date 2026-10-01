use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

use crate::backend::Op;

#[derive(Debug, Clone, Copy)]
pub struct Mix {
    pub get_pct: u8,
}

impl Mix {
    pub fn read_only() -> Self {
        Self { get_pct: 100 }
    }
    pub fn write_only() -> Self {
        Self { get_pct: 0 }
    }
    pub fn read_heavy() -> Self {
        Self { get_pct: 80 }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "get" | "100-0" => Ok(Self::read_only()),
            "set" | "0-100" => Ok(Self::write_only()),
            "80-20" => Ok(Self::read_heavy()),
            other => {
                if let Some((g, _)) = other.split_once('-') {
                    let g: u8 = g.parse().map_err(|e| format!("mix get_pct: {e}"))?;
                    if g > 100 {
                        return Err(format!("mix get_pct > 100: {g}"));
                    }
                    Ok(Self { get_pct: g })
                } else {
                    Err(format!("unknown mix: {other}"))
                }
            }
        }
    }

    pub fn label(&self) -> String {
        format!("{}-{}", self.get_pct, 100 - self.get_pct)
    }
}

#[derive(Debug, Clone)]
pub struct WorkloadSpec {
    pub key_count: usize,
    pub value_size: usize,
    pub mix: Mix,
    pub key_pattern: KeyPattern,
    pub key_distribution: KeyDistribution,
}

#[derive(Debug, Clone, Copy)]
pub enum KeyPattern {
    Point,
    Session,
}

impl KeyPattern {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "point" | "point_key" | "point-key" => Ok(Self::Point),
            "session" | "session_prefix" | "session-prefix" => Ok(Self::Session),
            other => Err(format!(
                "unknown key pattern `{other}`; use point or session"
            )),
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::Session => "session",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum KeyDistribution {
    Uniform,
    HotSet { hot_keys: usize, hot_pct: u8 },
    Zipf { theta: f64 },
}

impl KeyDistribution {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "uniform" => Ok(Self::Uniform),
            "zipf" => Ok(Self::Zipf { theta: 1.1 }),
            other => {
                if let Some(theta) = other.strip_prefix("zipf:") {
                    let theta = theta.parse().map_err(|e| format!("zipf theta: {e}"))?;
                    if theta <= 0.0 {
                        return Err(format!("zipf theta must be > 0: {theta}"));
                    }
                    return Ok(Self::Zipf { theta });
                }

                if let Some(rest) = other.strip_prefix("hot:") {
                    let mut parts = rest.split(':');
                    let hot_keys = parts
                        .next()
                        .ok_or_else(|| format!("hot distribution missing key count: {other}"))?
                        .parse()
                        .map_err(|e| format!("hot key count: {e}"))?;
                    let hot_pct = parts
                        .next()
                        .unwrap_or("90")
                        .parse()
                        .map_err(|e| format!("hot percentage: {e}"))?;
                    if hot_keys == 0 {
                        return Err("hot key count must be > 0".into());
                    }
                    if hot_pct > 100 {
                        return Err(format!("hot percentage > 100: {hot_pct}"));
                    }
                    return Ok(Self::HotSet { hot_keys, hot_pct });
                }

                Err(format!(
                    "unknown key distribution `{other}`; use uniform, zipf[:theta], or hot:<keys>[:pct]"
                ))
            }
        }
    }

    pub fn label(&self) -> String {
        match self {
            Self::Uniform => "uniform".to_string(),
            Self::HotSet { hot_keys, hot_pct } => format!("hot:{hot_keys}:{hot_pct}"),
            Self::Zipf { theta } => format!("zipf:{theta:.2}"),
        }
    }
}

pub struct Workload {
    keys: Vec<Vec<u8>>,
    value: Vec<u8>,
    key_distribution: KeyDistribution,
}

#[derive(Debug, Clone, Copy)]
pub enum ValuePattern {
    Repeating,
    Compressible,
    HighEntropy,
}

impl ValuePattern {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "repeating" => Ok(Self::Repeating),
            "compressible" => Ok(Self::Compressible),
            "high-entropy" => Ok(Self::HighEntropy),
            other => Err(format!(
                "unknown value pattern `{other}`; use repeating, compressible, or high-entropy"
            )),
        }
    }
}

impl Workload {
    /// All keys use the same payload, as in the original saturation workload.
    /// HighEntropy uses deterministic SplitMix64 bytes without a short period;
    /// this profile measures per-value encoding, not inter-key deduplication.
    pub fn with_value_pattern(mut self, pattern: ValuePattern) -> Self {
        match pattern {
            ValuePattern::Repeating => {
                for (index, byte) in self.value.iter_mut().enumerate() {
                    *byte = (index & 0xff) as u8;
                }
            }
            ValuePattern::Compressible => self.value.fill(b'x'),
            ValuePattern::HighEntropy => {
                let mut state = 0x4544_454e_2266_0001u64;
                for chunk in self.value.chunks_mut(8) {
                    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
                    let mut word = state;
                    word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                    word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                    word ^= word >> 31;
                    chunk.copy_from_slice(&word.to_le_bytes()[..chunk.len()]);
                }
            }
        }
        self
    }

    pub fn build(spec: &WorkloadSpec) -> Self {
        let mut keys = Vec::with_capacity(spec.key_count);
        for i in 0..spec.key_count {
            keys.push(build_key(spec.key_pattern, spec.key_count, i));
        }
        let mut value = vec![0u8; spec.value_size];
        for (i, b) in value.iter_mut().enumerate() {
            *b = (i & 0xff) as u8;
        }
        Self {
            keys,
            value,
            key_distribution: spec.key_distribution,
        }
    }

    pub fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn key_distribution(&self) -> KeyDistribution {
        self.key_distribution
    }
}

fn build_key(pattern: KeyPattern, key_count: usize, index: usize) -> Vec<u8> {
    match pattern {
        KeyPattern::Point => format!("k:{index:016x}").into_bytes(),
        KeyPattern::Session => {
            let session_count = key_count.clamp(1, 4096);
            let session_id = index % session_count;
            let chunk_id = index / session_count;
            format!("s:bench-session-{session_id:04x}:c:{chunk_id:016x}").into_bytes()
        }
    }
}

pub struct OpStream {
    rng: SmallRng,
    mode: OpMode,
    selector: KeySelector,
}

#[derive(Debug, Clone, Copy)]
enum OpMode {
    Fixed(Op),
    Mixed { get_pct: u8 },
}

enum KeySelector {
    Uniform {
        key_count: usize,
    },
    HotSet {
        key_count: usize,
        hot_keys: usize,
        hot_pct: u8,
    },
    Zipf {
        cdf: Vec<f64>,
    },
}

impl OpStream {
    pub fn new(seed: u64, key_count: usize, mix: Mix, key_distribution: KeyDistribution) -> Self {
        let mode = match mix.get_pct {
            100 => OpMode::Fixed(Op::Get),
            0 => OpMode::Fixed(Op::Set),
            get_pct => OpMode::Mixed { get_pct },
        };
        let selector = KeySelector::new(key_count, key_distribution);
        Self {
            rng: SmallRng::seed_from_u64(seed),
            mode,
            selector,
        }
    }

    #[inline(always)]
    pub fn next_op(&mut self) -> (Op, usize) {
        let key_idx = self.selector.next_key_index(&mut self.rng);
        let op = match self.mode {
            OpMode::Fixed(op) => op,
            OpMode::Mixed { get_pct } => {
                if self.rng.gen_range(0..100) < get_pct {
                    Op::Get
                } else {
                    Op::Set
                }
            }
        };
        (op, key_idx)
    }
}

impl KeySelector {
    fn new(key_count: usize, distribution: KeyDistribution) -> Self {
        assert!(key_count > 0, "key_count must be > 0");
        match distribution {
            KeyDistribution::Uniform => Self::Uniform { key_count },
            KeyDistribution::HotSet { hot_keys, hot_pct } => Self::HotSet {
                key_count,
                hot_keys: hot_keys.min(key_count),
                hot_pct,
            },
            KeyDistribution::Zipf { theta } => Self::Zipf {
                cdf: build_zipf_cdf(key_count, theta),
            },
        }
    }

    #[inline(always)]
    fn next_key_index(&self, rng: &mut SmallRng) -> usize {
        match self {
            Self::Uniform { key_count } => rng.gen_range(0..*key_count),
            Self::HotSet {
                key_count,
                hot_keys,
                hot_pct,
            } => {
                if *hot_keys >= *key_count || rng.gen_range(0..100) < *hot_pct {
                    rng.gen_range(0..*hot_keys)
                } else {
                    rng.gen_range(*hot_keys..*key_count)
                }
            }
            Self::Zipf { cdf } => {
                let sample = rng.r#gen::<f64>();
                cdf.partition_point(|&cutoff| cutoff < sample)
                    .min(cdf.len().saturating_sub(1))
            }
        }
    }
}

fn build_zipf_cdf(key_count: usize, theta: f64) -> Vec<f64> {
    let mut cdf = Vec::with_capacity(key_count);
    let mut total = 0.0;
    for rank in 1..=key_count {
        total += 1.0 / (rank as f64).powf(theta);
        cdf.push(total);
    }
    for cutoff in &mut cdf {
        *cutoff /= total;
    }
    if let Some(last) = cdf.last_mut() {
        *last = 1.0;
    }
    cdf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workload(size: usize) -> Workload {
        Workload::build(&WorkloadSpec {
            key_count: 4,
            value_size: size,
            mix: Mix::read_heavy(),
            key_pattern: KeyPattern::Point,
            key_distribution: KeyDistribution::HotSet {
                hot_keys: 2,
                hot_pct: 90,
            },
        })
    }

    #[test]
    fn value_patterns_preserve_keyspace_and_access_distribution() {
        let default = workload(513);
        let keys = default.keys().to_vec();
        let compressible = default.with_value_pattern(ValuePattern::Compressible);
        assert_eq!(compressible.value(), vec![b'x'; 513]);
        assert_eq!(compressible.keys(), keys);
        assert!(matches!(
            compressible.key_distribution(),
            KeyDistribution::HotSet {
                hot_keys: 2,
                hot_pct: 90
            }
        ));
        let repeating = compressible.with_value_pattern(ValuePattern::Repeating);
        assert_eq!(repeating.value(), workload(513).value());
        assert_eq!(repeating.value()[0], repeating.value()[256]);
    }

    #[test]
    fn high_entropy_values_are_deterministic_and_handle_partial_words() {
        let full = workload(4096).with_value_pattern(ValuePattern::HighEntropy);
        assert_eq!(
            full.value(),
            workload(4096)
                .with_value_pattern(ValuePattern::HighEntropy)
                .value()
        );
        assert_ne!(&full.value()[..256], &full.value()[256..512]);
        assert_ne!(full.value(), workload(4096).value());
        for size in [0, 1, 7, 8, 9, 16, 64, 256, 1024] {
            let partial = workload(size).with_value_pattern(ValuePattern::HighEntropy);
            assert_eq!(partial.value(), &full.value()[..size]);
        }
    }

    #[test]
    fn value_pattern_parser_rejects_unknown_profiles() {
        for valid in ["repeating", "compressible", "high-entropy"] {
            assert!(ValuePattern::parse(valid).is_ok());
        }
        assert!(ValuePattern::parse("random").is_err());
    }
}
