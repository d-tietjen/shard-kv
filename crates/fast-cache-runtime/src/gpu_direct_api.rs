use serde::{Deserialize, Serialize};

use crate::runtime::{RuntimeError, RuntimeResult};

pub const HOST_DIRECT_V1_PATH: &str = "host_direct_v1";
pub const GPU_DIRECT_API_V0_PATH: &str = "gpu_direct_api_v0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuDirectApiVersion {
    V0,
}

impl GpuDirectApiVersion {
    #[inline(always)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V0 => GPU_DIRECT_API_V0_PATH,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuDirectPathSelection {
    HostDirectV1,
    GpuDirectApiV0,
}

impl GpuDirectPathSelection {
    #[inline(always)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HostDirectV1 => HOST_DIRECT_V1_PATH,
            Self::GpuDirectApiV0 => GPU_DIRECT_API_V0_PATH,
        }
    }

    pub fn parse(value: &str) -> RuntimeResult<Self> {
        match value {
            "" | HOST_DIRECT_V1_PATH => Ok(Self::HostDirectV1),
            GPU_DIRECT_API_V0_PATH => Ok(Self::GpuDirectApiV0),
            other => Err(RuntimeError::Engine(format!(
                "unsupported direct restore path version {other:?}; expected {HOST_DIRECT_V1_PATH:?} or {GPU_DIRECT_API_V0_PATH:?}"
            ))),
        }
    }

    pub fn supported_names() -> &'static [&'static str] {
        &[HOST_DIRECT_V1_PATH, GPU_DIRECT_API_V0_PATH]
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GPU_DIRECT_API_V0_PATH, GpuDirectApiVersion, GpuDirectPathSelection, HOST_DIRECT_V1_PATH,
    };

    #[test]
    fn path_selection_parses_supported_names() {
        assert_eq!(
            GpuDirectPathSelection::parse(HOST_DIRECT_V1_PATH).unwrap(),
            GpuDirectPathSelection::HostDirectV1
        );
        assert_eq!(
            GpuDirectPathSelection::parse(GPU_DIRECT_API_V0_PATH).unwrap(),
            GpuDirectPathSelection::GpuDirectApiV0
        );
        assert_eq!(GpuDirectApiVersion::V0.as_str(), GPU_DIRECT_API_V0_PATH);
    }
}
