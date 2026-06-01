#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-topk")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TopKError {
    AlreadyExists,
    MissingKey,
    WrongType,
    InvalidArgument,
}

#[cfg(feature = "redis-module-topk")]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct TopKInfo {
    pub(crate) k: usize,
    pub(crate) width: usize,
    pub(crate) depth: usize,
    pub(crate) decay: f64,
}
