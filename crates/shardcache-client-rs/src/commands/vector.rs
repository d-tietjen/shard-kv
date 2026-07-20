use std::io::Write;

use crate::commands::ScnpCommand;
use crate::commands::compact::{owned_arg_list_len, write_owned_arg_list};
use crate::connection::ScnpConnection;
use crate::error::{Result, ShardCacheClientError};
use crate::protocol::{FAST_FLAG_REDIS_COMMAND_ARGS, ROUTED_FLAGS};
use crate::routing::ShardCacheRoute;

const PING_OPCODE: u8 = 9;
const VADD_OPCODE: u8 = 231;
const VREM_OPCODE: u8 = 241;
const VSIM_OPCODE: u8 = 243;
const ROUTE_PREFIX_BYTES: usize = 20;
const DEFAULT_VSIM_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_TYPED_ARRAY_ITEMS: usize = 65_536;
const MAX_VSIM_RESULTS: usize = MAX_TYPED_ARRAY_ITEMS / 3;
const MAX_VECTOR_GOVERNANCE_BYTES: usize = 64 * 1024;

/// Vector quantization selected when a vector set is created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorQuantization {
    NoQuant,
    Q8,
    Binary,
}

impl VectorQuantization {
    fn token(self) -> &'static [u8] {
        match self {
            Self::NoQuant => b"NOQUANT",
            Self::Q8 => b"Q8",
            Self::Binary => b"BIN",
        }
    }
}

/// Options for a typed SCNP `VADD` request.
#[derive(Debug, Clone, Default)]
pub struct VAddOptions<'a> {
    attributes: Option<&'a [u8]>,
    governance: Option<&'a [u8]>,
    quantization: Option<VectorQuantization>,
    reduce_dim: Option<usize>,
    hnsw_m: Option<usize>,
    ef_construction: Option<usize>,
    cas: bool,
}

impl<'a> VAddOptions<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores JSON attributes alongside the vector element.
    pub fn attributes(mut self, attributes: &'a [u8]) -> Self {
        self.attributes = Some(attributes);
        self
    }

    /// Stores opaque governance metadata alongside this vector element.
    pub fn governance_metadata(mut self, metadata: &'a [u8]) -> Self {
        self.governance = Some(metadata);
        self
    }

    pub fn quantization(mut self, quantization: VectorQuantization) -> Self {
        self.quantization = Some(quantization);
        self
    }

    pub fn reduce_dim(mut self, dimensions: usize) -> Self {
        self.reduce_dim = Some(dimensions);
        self
    }

    pub fn hnsw_m(mut self, value: usize) -> Self {
        self.hnsw_m = Some(value);
        self
    }

    pub fn ef_construction(mut self, value: usize) -> Self {
        self.ef_construction = Some(value);
        self
    }

    pub fn compare_and_set(mut self, enabled: bool) -> Self {
        self.cas = enabled;
        self
    }
}

/// Options for typed SCNP `VSIM` requests.
///
/// Typed requests always ask the server for scores and attributes. Governance
/// metadata can be included after all vector servers are on 0.7.2.
#[derive(Debug, Clone)]
pub struct VSimOptions<'a> {
    count: usize,
    filter: Option<&'a [u8]>,
    ef_search: Option<usize>,
    truth: bool,
    with_governance: bool,
    max_response_bytes: usize,
}

impl Default for VSimOptions<'_> {
    fn default() -> Self {
        Self {
            count: 10,
            filter: None,
            ef_search: None,
            truth: false,
            with_governance: false,
            max_response_bytes: DEFAULT_VSIM_RESPONSE_BYTES,
        }
    }
}

impl<'a> VSimOptions<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }

    pub fn filter(mut self, expression: &'a [u8]) -> Self {
        self.filter = Some(expression);
        self
    }

    pub fn ef_search(mut self, value: usize) -> Self {
        self.ef_search = Some(value);
        self
    }

    pub fn exact(mut self, enabled: bool) -> Self {
        self.truth = enabled;
        self
    }

    /// Includes opaque governance metadata in every returned match.
    pub fn with_governance(mut self, enabled: bool) -> Self {
        self.with_governance = enabled;
        self
    }

    /// Bounds the response body before the client allocates it.
    pub fn max_response_bytes(mut self, bytes: usize) -> Self {
        self.max_response_bytes = bytes;
        self
    }
}

/// One scored, attributed, and governed `VSIM` result.
#[derive(Debug, Clone, PartialEq)]
pub struct VSimMatch {
    pub element: Vec<u8>,
    pub score: f64,
    pub attributes: Option<Vec<u8>>,
    pub governance: Option<Vec<u8>>,
}

pub(crate) struct Ping;

impl ScnpCommand for Ping {
    type Output = Vec<u8>;

    const NAME: &'static str = "PING";
    const OPCODE: u8 = PING_OPCODE;

    fn flags(&self) -> u8 {
        FAST_FLAG_REDIS_COMMAND_ARGS
    }

    fn body_len(&self) -> usize {
        1
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        w.write_all(&[0])?;
        Ok(())
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        conn.read_typed_value(Self::NAME, 64)
    }
}

struct VectorRequest {
    opcode: u8,
    name: &'static str,
    route: Option<ShardCacheRoute>,
    args: Vec<Vec<u8>>,
}

impl VectorRequest {
    fn body_len(&self) -> usize {
        self.route.map_or(0, |_| ROUTE_PREFIX_BYTES) + owned_arg_list_len(&self.args)
    }

    fn flags(&self) -> u8 {
        FAST_FLAG_REDIS_COMMAND_ARGS | self.route.map_or(0, |_| ROUTED_FLAGS)
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        if let Some(route) = self.route {
            route.write_to(w)?;
        }
        write_owned_arg_list(w, &self.args)
    }
}

pub(crate) struct VAdd {
    request: VectorRequest,
}

impl VAdd {
    pub(crate) fn new(
        route: Option<ShardCacheRoute>,
        key: &[u8],
        element: &[u8],
        vector: &[f32],
        options: VAddOptions<'_>,
    ) -> Result<Self> {
        validate_vector(vector)?;
        validate_positive("VADD REDUCE", options.reduce_dim)?;
        validate_positive("VADD M", options.hnsw_m)?;
        validate_positive("VADD EF", options.ef_construction)?;
        if options
            .governance
            .is_some_and(|metadata| metadata.len() > MAX_VECTOR_GOVERNANCE_BYTES)
        {
            return Err(ShardCacheClientError::Config(format!(
                "VADD governance metadata exceeds {MAX_VECTOR_GOVERNANCE_BYTES} bytes"
            )));
        }

        let mut args = Vec::with_capacity(14);
        args.push(key.to_vec());
        if let Some(dimensions) = options.reduce_dim {
            args.push(b"REDUCE".to_vec());
            args.push(dimensions.to_string().into_bytes());
        }
        args.push(b"FP32".to_vec());
        args.push(encode_fp32(vector)?);
        args.push(element.to_vec());
        if options.cas {
            args.push(b"CAS".to_vec());
        }
        if let Some(quantization) = options.quantization {
            args.push(quantization.token().to_vec());
        }
        if let Some(value) = options.ef_construction {
            args.push(b"EF".to_vec());
            args.push(value.to_string().into_bytes());
        }
        if let Some(value) = options.hnsw_m {
            args.push(b"M".to_vec());
            args.push(value.to_string().into_bytes());
        }
        if let Some(attributes) = options.attributes {
            args.push(b"SETATTR".to_vec());
            args.push(attributes.to_vec());
        }
        if let Some(governance) = options.governance {
            args.push(b"GOVERNANCE".to_vec());
            args.push(governance.to_vec());
        }
        Ok(Self {
            request: VectorRequest {
                opcode: VADD_OPCODE,
                name: "VADD",
                route,
                args,
            },
        })
    }
}

impl ScnpCommand for VAdd {
    type Output = bool;

    const NAME: &'static str = "VADD";
    const OPCODE: u8 = VADD_OPCODE;

    fn opcode(&self) -> u8 {
        self.request.opcode
    }

    fn flags(&self) -> u8 {
        self.request.flags()
    }

    fn body_len(&self) -> usize {
        self.request.body_len()
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        self.request.write_body(w)
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        conn.read_typed_integer(self.request.name)
            .map(|value| value != 0)
    }
}

pub(crate) struct VRem {
    request: VectorRequest,
}

impl VRem {
    pub(crate) fn new(route: Option<ShardCacheRoute>, key: &[u8], element: &[u8]) -> Self {
        Self {
            request: VectorRequest {
                opcode: VREM_OPCODE,
                name: "VREM",
                route,
                args: vec![key.to_vec(), element.to_vec()],
            },
        }
    }
}

impl ScnpCommand for VRem {
    type Output = bool;

    const NAME: &'static str = "VREM";
    const OPCODE: u8 = VREM_OPCODE;

    fn opcode(&self) -> u8 {
        self.request.opcode
    }

    fn flags(&self) -> u8 {
        self.request.flags()
    }

    fn body_len(&self) -> usize {
        self.request.body_len()
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        self.request.write_body(w)
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        conn.read_typed_integer(self.request.name)
            .map(|value| value != 0)
    }
}

pub(crate) struct VSim {
    request: VectorRequest,
    count: usize,
    max_response_bytes: usize,
    tuple_width: usize,
}

impl VSim {
    pub(crate) fn new(
        route: Option<ShardCacheRoute>,
        key: &[u8],
        vector: &[f32],
        options: VSimOptions<'_>,
    ) -> Result<Self> {
        validate_vector(vector)?;
        validate_positive("VSIM EF", options.ef_search)?;
        if options.count == 0 {
            return Err(ShardCacheClientError::Config(
                "VSIM count must be positive".into(),
            ));
        }
        if options.count > MAX_VSIM_RESULTS {
            return Err(ShardCacheClientError::Config(format!(
                "VSIM count exceeds the client limit of {MAX_VSIM_RESULTS}"
            )));
        }
        if options.max_response_bytes == 0 {
            return Err(ShardCacheClientError::Config(
                "VSIM max response bytes must be positive".into(),
            ));
        }
        let tuple_width = if options.with_governance { 4 } else { 3 };
        let max_items = options.count.checked_mul(tuple_width).ok_or_else(|| {
            ShardCacheClientError::Config("VSIM count exceeds the client limit".into())
        })?;
        if max_items > MAX_TYPED_ARRAY_ITEMS {
            return Err(ShardCacheClientError::Config(
                "VSIM count exceeds the protocol limit".into(),
            ));
        }

        let mut args = Vec::with_capacity(13);
        args.push(key.to_vec());
        args.push(b"FP32".to_vec());
        args.push(encode_fp32(vector)?);
        args.push(b"COUNT".to_vec());
        args.push(options.count.to_string().into_bytes());
        args.push(b"WITHSCORES".to_vec());
        args.push(b"WITHATTRIBS".to_vec());
        if options.with_governance {
            args.push(b"WITHGOVERNANCE".to_vec());
        }
        if let Some(filter) = options.filter {
            args.push(b"FILTER".to_vec());
            args.push(filter.to_vec());
        }
        if let Some(value) = options.ef_search {
            args.push(b"EF".to_vec());
            args.push(value.to_string().into_bytes());
        }
        if options.truth {
            args.push(b"TRUTH".to_vec());
        }
        Ok(Self {
            request: VectorRequest {
                opcode: VSIM_OPCODE,
                name: "VSIM",
                route,
                args,
            },
            count: options.count,
            max_response_bytes: options.max_response_bytes,
            tuple_width,
        })
    }
}

impl ScnpCommand for VSim {
    type Output = Vec<VSimMatch>;

    const NAME: &'static str = "VSIM";
    const OPCODE: u8 = VSIM_OPCODE;

    fn opcode(&self) -> u8 {
        self.request.opcode
    }

    fn flags(&self) -> u8 {
        self.request.flags()
    }

    fn body_len(&self) -> usize {
        self.request.body_len()
    }

    fn write_body<W: Write>(&self, w: &mut W) -> Result<()> {
        self.request.write_body(w)
    }

    fn read_response(self, conn: &mut ScnpConnection) -> Result<Self::Output> {
        let max_items = self.count.saturating_mul(self.tuple_width);
        let values =
            conn.read_typed_array(self.request.name, self.max_response_bytes, max_items)?;
        if !values.len().is_multiple_of(self.tuple_width) {
            let shape = if self.tuple_width == 4 {
                "element/score/attributes/governance tuples"
            } else {
                "element/score/attributes triplets"
            };
            return Err(ShardCacheClientError::Protocol(format!(
                "VSIM response did not contain {shape}"
            )));
        }
        let mut matches = Vec::with_capacity(values.len() / self.tuple_width);
        let mut values = values.into_iter();
        while let Some(element) = values.next() {
            let raw_score = values.next().ok_or_else(|| {
                ShardCacheClientError::Protocol("VSIM response is missing a score".into())
            })?;
            let attributes = values.next().ok_or_else(|| {
                ShardCacheClientError::Protocol("VSIM response is missing attributes".into())
            })?;
            let governance = if self.tuple_width == 4 {
                values.next().ok_or_else(|| {
                    ShardCacheClientError::Protocol("VSIM response is missing governance".into())
                })?
            } else {
                None
            };
            let element = element.ok_or_else(|| {
                ShardCacheClientError::Protocol("VSIM returned a null element".into())
            })?;
            let raw_score = raw_score.as_deref().ok_or_else(|| {
                ShardCacheClientError::Protocol("VSIM returned a null score".into())
            })?;
            let score = std::str::from_utf8(raw_score)
                .map_err(|_| ShardCacheClientError::Protocol("VSIM score is not UTF-8".into()))?
                .parse::<f64>()
                .map_err(|_| ShardCacheClientError::Protocol("VSIM score is not a float".into()))?;
            if !score.is_finite() {
                return Err(ShardCacheClientError::Protocol(
                    "VSIM score is not finite".into(),
                ));
            }
            matches.push(VSimMatch {
                element,
                score,
                attributes,
                governance,
            });
        }
        Ok(matches)
    }
}

fn validate_vector(vector: &[f32]) -> Result<()> {
    if vector.is_empty() {
        return Err(ShardCacheClientError::Config(
            "vector dimensions must be positive".into(),
        ));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(ShardCacheClientError::Config(
            "vector values must be finite".into(),
        ));
    }
    Ok(())
}

fn validate_positive(label: &str, value: Option<usize>) -> Result<()> {
    if value == Some(0) {
        return Err(ShardCacheClientError::Config(format!(
            "{label} must be positive"
        )));
    }
    Ok(())
}

fn encode_fp32(vector: &[f32]) -> Result<Vec<u8>> {
    let byte_len = vector.len().checked_mul(4).ok_or_else(|| {
        ShardCacheClientError::Config("vector byte length exceeds the client limit".into())
    })?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(byte_len)
        .map_err(|_| ShardCacheClientError::Config("vector allocation failed".into()))?;
    for value in vector {
        encoded.extend_from_slice(&value.to_le_bytes());
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vadd_uses_fp32_attributes_and_governance_without_public_opcodes() {
        let command = VAdd::new(
            None,
            b"objects",
            b"doc-1",
            &[1.0, 0.5],
            VAddOptions::new()
                .attributes(br#"{"source":"test"}"#)
                .governance_metadata(b"tenant=acme"),
        )
        .unwrap();
        assert_eq!(command.request.args[0], b"objects");
        assert_eq!(command.request.args[1], b"FP32");
        assert_eq!(command.request.args[3], b"doc-1");
        assert_eq!(command.request.args[4], b"SETATTR");
        assert!(command.request.args.iter().any(|arg| arg == b"GOVERNANCE"));
        assert!(command.request.args.iter().any(|arg| arg == b"tenant=acme"));
    }

    #[test]
    fn vsim_always_requests_stable_governed_result_tuples() {
        let compatible =
            VSim::new(None, b"objects", &[1.0, 0.5], VSimOptions::new().count(7)).unwrap();
        assert_eq!(compatible.tuple_width, 3);
        assert!(
            !compatible
                .request
                .args
                .iter()
                .any(|arg| arg == b"WITHGOVERNANCE")
        );

        let command = VSim::new(
            None,
            b"objects",
            &[1.0, 0.5],
            VSimOptions::new().count(7).with_governance(true),
        )
        .unwrap();
        assert!(command.request.args.iter().any(|arg| arg == b"WITHSCORES"));
        assert!(command.request.args.iter().any(|arg| arg == b"WITHATTRIBS"));
        assert!(
            command
                .request
                .args
                .iter()
                .any(|arg| arg == b"WITHGOVERNANCE")
        );
        assert_eq!(command.count, 7);
        assert_eq!(command.tuple_width, 4);
    }

    #[test]
    fn invalid_vectors_fail_before_network_io() {
        assert!(VAdd::new(None, b"k", b"e", &[], VAddOptions::new()).is_err());
        let oversized_governance = vec![0; MAX_VECTOR_GOVERNANCE_BYTES + 1];
        assert!(
            VAdd::new(
                None,
                b"k",
                b"e",
                &[1.0],
                VAddOptions::new().governance_metadata(&oversized_governance),
            )
            .is_err()
        );
        assert!(VSim::new(None, b"k", &[f32::NAN], VSimOptions::new()).is_err());
        assert!(VSim::new(None, b"k", &[1.0], VSimOptions::new().count(0)).is_err());
        assert!(
            VSim::new(
                None,
                b"k",
                &[1.0],
                VSimOptions::new().count(MAX_VSIM_RESULTS + 1),
            )
            .is_err()
        );
    }
}
