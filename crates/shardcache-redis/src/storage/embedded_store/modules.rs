#![allow(dead_code, unused_imports)]

use super::EmbeddedStore;
#[cfg(feature = "redis-modules")]
use parking_lot::RwLock;
#[cfg(feature = "redis-module-topk")]
use std::cmp::Ordering;
#[cfg(feature = "redis-modules")]
use std::collections::{BTreeMap, BTreeSet};

use crate::storage::Bytes;
#[cfg(feature = "redis-modules")]
use crate::storage::{EmbeddedKeyRoute, FastHashMap, FastHashSet, now_millis};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisModuleFamily {
    RediSearch,
    RedisBloom,
    RedisTimeSeries,
    RedisGraph,
    RedisJson,
    RedisAi,
    RedisGears,
    RedisCell,
    NeuralRedis,
    RedisTDigest,
    Cthulhu,
    RedisSnowflake,
    RedisRoaring,
    SessionGate,
    ReDe,
    TopK,
    CountMinSketch,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RedisModuleApiResult {
    Simple(&'static str),
    Integer(i64),
    Bulk(Option<Bytes>),
    Array(Vec<RedisModuleApiResult>),
    Error(String),
    TopKInfo {
        k: usize,
        width: usize,
        depth: usize,
        decay: f64,
    },
    Unsupported {
        family: RedisModuleFamily,
        command: String,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct RedisModuleApi<'a> {
    store: &'a EmbeddedStore,
    family: RedisModuleFamily,
}

impl<'a> RedisModuleApi<'a> {
    #[inline(always)]
    pub fn family(self) -> RedisModuleFamily {
        self.family
    }

    #[inline(always)]
    pub fn store(self) -> &'a EmbeddedStore {
        self.store
    }

    pub fn execute(self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        self.store.redis_module_execute(self.family, command, args)
    }
}

impl EmbeddedStore {
    #[cfg(feature = "redis-module-search")]
    pub fn redis_search(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RediSearch,
        }
    }

    #[cfg(feature = "redis-module-bloom")]
    pub fn redis_bloom(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisBloom,
        }
    }

    #[cfg(feature = "redis-module-timeseries")]
    pub fn redis_timeseries(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisTimeSeries,
        }
    }

    #[cfg(feature = "redis-module-graph")]
    pub fn redis_graph(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisGraph,
        }
    }

    #[cfg(feature = "redis-module-json")]
    pub fn redis_json(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisJson,
        }
    }

    #[cfg(feature = "redis-module-ai")]
    pub fn redis_ai(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisAi,
        }
    }

    #[cfg(feature = "redis-module-gears")]
    pub fn redis_gears(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisGears,
        }
    }

    #[cfg(feature = "redis-module-cell")]
    pub fn redis_cell(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisCell,
        }
    }

    #[cfg(feature = "redis-module-neural")]
    pub fn neural_redis(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::NeuralRedis,
        }
    }

    #[cfg(feature = "redis-module-tdigest")]
    pub fn redis_tdigest(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisTDigest,
        }
    }

    #[cfg(feature = "redis-module-cthulhu")]
    pub fn cthulhu(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::Cthulhu,
        }
    }

    #[cfg(feature = "redis-module-snowflake")]
    pub fn redis_snowflake(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisSnowflake,
        }
    }

    #[cfg(feature = "redis-module-roaring")]
    pub fn redis_roaring(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::RedisRoaring,
        }
    }

    #[cfg(feature = "redis-module-session-gate")]
    pub fn session_gate(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::SessionGate,
        }
    }

    #[cfg(feature = "redis-module-rede")]
    pub fn rede(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::ReDe,
        }
    }

    #[cfg(feature = "redis-module-topk")]
    pub fn topk(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::TopK,
        }
    }

    #[cfg(feature = "redis-module-cms")]
    pub fn count_min_sketch(&self) -> RedisModuleApi<'_> {
        RedisModuleApi {
            store: self,
            family: RedisModuleFamily::CountMinSketch,
        }
    }
}

#[cfg(feature = "redis-modules")]
#[derive(Debug)]
pub(crate) struct RedisModuleState {
    shards: Vec<RwLock<ModuleShard>>,
}

#[cfg(feature = "redis-modules")]
impl RedisModuleState {
    pub(crate) fn new(shard_count: usize) -> Self {
        Self {
            shards: (0..shard_count.max(1))
                .map(|_| RwLock::new(ModuleShard::default()))
                .collect(),
        }
    }

    fn read(&self, route: EmbeddedKeyRoute) -> parking_lot::RwLockReadGuard<'_, ModuleShard> {
        self.shards[route.shard_id].read()
    }

    fn write(&self, route: EmbeddedKeyRoute) -> parking_lot::RwLockWriteGuard<'_, ModuleShard> {
        self.shards[route.shard_id].write()
    }
}

#[cfg(feature = "redis-modules")]
#[derive(Debug, Default)]
struct ModuleShard {
    records: FastHashMap<Bytes, ModuleRecord>,
    sets: FastHashMap<Bytes, FastHashSet<Bytes>>,
    multisets: FastHashMap<Bytes, FastHashMap<Bytes, i64>>,
    json: FastHashMap<Bytes, serde_json::Value>,
    floats: FastHashMap<Bytes, Vec<f64>>,
    series: FastHashMap<Bytes, BTreeMap<i64, f64>>,
    bits: FastHashMap<Bytes, BTreeSet<u64>>,
    counters: FastHashMap<Bytes, u64>,
    cell_buckets: FastHashMap<Bytes, CellBucket>,
}

#[cfg(feature = "redis-modules")]
impl ModuleShard {
    fn contains_any(&self, key: &[u8]) -> bool {
        self.records.contains_key(key)
            || self.sets.contains_key(key)
            || self.multisets.contains_key(key)
            || self.json.contains_key(key)
            || self.floats.contains_key(key)
            || self.series.contains_key(key)
            || self.bits.contains_key(key)
            || self.counters.contains_key(key)
            || self.cell_buckets.contains_key(key)
    }
}

#[cfg(feature = "redis-modules")]
#[derive(Debug, Clone)]
struct ModuleRecord {
    args: Vec<Bytes>,
    hits: u64,
}

#[cfg(feature = "redis-modules")]
impl ModuleRecord {
    fn new(args: &[&[u8]]) -> Self {
        Self {
            args: args.iter().map(|arg| (*arg).to_vec()).collect(),
            hits: 0,
        }
    }
}

#[cfg(feature = "redis-modules")]
#[derive(Debug, Clone, Copy, Default)]
struct CellBucket {
    remaining: i64,
    reset_after: i64,
}

#[cfg(feature = "redis-modules")]
impl EmbeddedStore {
    pub(crate) fn redis_module_execute(
        &self,
        family: RedisModuleFamily,
        command: &str,
        args: &[&[u8]],
    ) -> RedisModuleApiResult {
        let _ = args;
        #[allow(unreachable_patterns)]
        match family {
            #[cfg(feature = "redis-module-search")]
            RedisModuleFamily::RediSearch => self.search_api_execute(command, args),
            #[cfg(feature = "redis-module-bloom")]
            RedisModuleFamily::RedisBloom => self.bloom_api_execute(command, args),
            #[cfg(feature = "redis-module-timeseries")]
            RedisModuleFamily::RedisTimeSeries => self.timeseries_api_execute(command, args),
            #[cfg(feature = "redis-module-graph")]
            RedisModuleFamily::RedisGraph => self.graph_api_execute(command, args),
            #[cfg(feature = "redis-module-json")]
            RedisModuleFamily::RedisJson => self.json_api_execute(command, args),
            #[cfg(feature = "redis-module-ai")]
            RedisModuleFamily::RedisAi => self.ai_api_execute(command, args),
            #[cfg(feature = "redis-module-gears")]
            RedisModuleFamily::RedisGears => self.gears_api_execute(command, args),
            #[cfg(feature = "redis-module-cell")]
            RedisModuleFamily::RedisCell => self.cell_api_execute(command, args),
            #[cfg(feature = "redis-module-neural")]
            RedisModuleFamily::NeuralRedis => self.neural_api_execute(command, args),
            #[cfg(feature = "redis-module-tdigest")]
            RedisModuleFamily::RedisTDigest => self.tdigest_api_execute(command, args),
            #[cfg(feature = "redis-module-cthulhu")]
            RedisModuleFamily::Cthulhu => self.cthulhu_api_execute(command, args),
            #[cfg(feature = "redis-module-snowflake")]
            RedisModuleFamily::RedisSnowflake => self.snowflake_api_execute(command, args),
            #[cfg(feature = "redis-module-roaring")]
            RedisModuleFamily::RedisRoaring => self.roaring_api_execute(command, args),
            #[cfg(feature = "redis-module-session-gate")]
            RedisModuleFamily::SessionGate => self.session_gate_api_execute(command, args),
            #[cfg(feature = "redis-module-rede")]
            RedisModuleFamily::ReDe => self.rede_api_execute(command, args),
            #[cfg(feature = "redis-module-topk")]
            RedisModuleFamily::TopK => self.topk_api_execute(command, args),
            #[cfg(feature = "redis-module-cms")]
            RedisModuleFamily::CountMinSketch => self.cms_api_execute(command, args),
            _ => RedisModuleApiResult::Unsupported {
                family,
                command: command.to_string(),
            },
        }
    }

    fn module_record_command(
        &self,
        family: RedisModuleFamily,
        command: &str,
        args: &[&[u8]],
    ) -> RedisModuleApiResult {
        let cmd = command.to_ascii_uppercase();
        let key = args.first().copied().unwrap_or(cmd.as_bytes());
        let route = self.route_key(key);
        match cmd.as_str() {
            name if name.ends_with("GET")
                || name.ends_with("INFO")
                || name.ends_with("LIST")
                || name.ends_with("DUMP")
                || name.ends_with("DUMPSESSIONS")
                || name.ends_with("DUMPEXECUTIONS")
                || name.ends_with("DUMPREGISTRATIONS")
                || name.ends_with("GETRESULTS")
                || name.ends_with("GETRESULTSBLOCKING")
                || name.ends_with("STATS") =>
            {
                let mut shard = self.module_state.write(route);
                if let Some(record) = shard.records.get_mut(key) {
                    record.hits = record.hits.saturating_add(1);
                    RedisModuleApiResult::Array(
                        record
                            .args
                            .iter()
                            .cloned()
                            .map(|arg| RedisModuleApiResult::Bulk(Some(arg)))
                            .collect(),
                    )
                } else {
                    RedisModuleApiResult::Array(Vec::new())
                }
            }
            name if name.ends_with("DEL")
                || name.ends_with("DELETE")
                || name.ends_with("UNREGISTER")
                || name.ends_with("ABORTEXECUTION") =>
            {
                let removed = self.module_state.write(route).records.remove(key).is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            _ if command_mutates(command) => {
                self.module_state
                    .write(route)
                    .records
                    .insert(key.to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            _ => RedisModuleApiResult::Unsupported {
                family,
                command: command.to_string(),
            },
        }
    }
}

#[cfg(feature = "redis-modules")]
fn command_mutates(command: &str) -> bool {
    let command = command.to_ascii_uppercase();
    !(command.ends_with("GET")
        || command.ends_with("INFO")
        || command.ends_with("LIST")
        || command.ends_with("QUERY")
        || command.ends_with("SEARCH")
        || command.ends_with("EXISTS")
        || command.ends_with("COUNT")
        || command.ends_with("LEN")
        || command.ends_with("RANGE")
        || command.ends_with("RO_QUERY")
        || command.ends_with("PROFILE")
        || command.ends_with("EXPLAIN")
        || command.ends_with("STATS"))
}

#[cfg(feature = "redis-modules")]
fn result_bulk_string(value: impl Into<String>) -> RedisModuleApiResult {
    RedisModuleApiResult::Bulk(Some(value.into().into_bytes()))
}

#[cfg(feature = "redis-modules")]
fn result_bulk_bytes(value: Bytes) -> RedisModuleApiResult {
    RedisModuleApiResult::Bulk(Some(value))
}

#[cfg(feature = "redis-modules")]
fn result_null() -> RedisModuleApiResult {
    RedisModuleApiResult::Bulk(None)
}

#[cfg(feature = "redis-modules")]
fn invalid_arg(message: &str) -> RedisModuleApiResult {
    RedisModuleApiResult::Error(format!("ERR {message}"))
}

#[cfg(feature = "redis-modules")]
fn parse_f64_lossy(raw: &[u8]) -> Option<f64> {
    std::str::from_utf8(raw).ok()?.parse::<f64>().ok()
}

#[cfg(feature = "redis-modules")]
fn parse_i64_lossy(raw: &[u8]) -> Option<i64> {
    std::str::from_utf8(raw).ok()?.parse::<i64>().ok()
}

#[cfg(feature = "redis-modules")]
fn parse_u64_lossy(raw: &[u8]) -> Option<u64> {
    std::str::from_utf8(raw).ok()?.parse::<u64>().ok()
}

#[cfg(feature = "redis-modules")]
fn parse_usize_lossy(raw: &[u8]) -> Option<usize> {
    parse_i64_lossy(raw).and_then(|value| usize::try_from(value).ok())
}

#[cfg(feature = "redis-modules")]
fn bytes_eq(raw: &[u8], expected: &[u8]) -> bool {
    raw.eq_ignore_ascii_case(expected)
}

#[cfg(feature = "redis-modules")]
impl EmbeddedStore {
    fn module_record_keys(&self) -> Vec<Bytes> {
        let mut keys = Vec::new();
        for shard in &self.module_state.shards {
            keys.extend(shard.read().records.keys().cloned());
        }
        keys.sort();
        keys
    }
}

#[cfg(feature = "redis-module-ai")]
#[path = "modules/ai/mod.rs"]
mod ai;
#[cfg(feature = "redis-module-bloom")]
#[path = "modules/bloom/mod.rs"]
mod bloom;
#[cfg(feature = "redis-module-cell")]
#[path = "modules/cell/mod.rs"]
mod cell;
#[cfg(feature = "redis-module-cms")]
#[path = "modules/count_min_sketch/mod.rs"]
mod count_min_sketch;
#[cfg(feature = "redis-module-cthulhu")]
#[path = "modules/cthulhu/mod.rs"]
mod cthulhu;
#[cfg(feature = "redis-module-gears")]
#[path = "modules/gears/mod.rs"]
mod gears;
#[cfg(feature = "redis-module-graph")]
#[path = "modules/graph/mod.rs"]
mod graph;
#[cfg(feature = "redis-module-json")]
#[path = "modules/json/mod.rs"]
mod json;
#[cfg(feature = "redis-module-neural")]
#[path = "modules/neural/mod.rs"]
mod neural;
#[cfg(feature = "redis-module-rede")]
#[path = "modules/rede/mod.rs"]
mod rede;
#[cfg(feature = "redis-module-roaring")]
#[path = "modules/roaring/mod.rs"]
mod roaring;
#[cfg(feature = "redis-module-search")]
#[path = "modules/search/mod.rs"]
mod search;
#[cfg(feature = "redis-module-session-gate")]
#[path = "modules/session_gate/mod.rs"]
mod session_gate;
#[cfg(feature = "redis-module-snowflake")]
#[path = "modules/snowflake/mod.rs"]
mod snowflake;
#[cfg(feature = "redis-module-tdigest")]
#[path = "modules/tdigest/mod.rs"]
mod tdigest;
#[cfg(feature = "redis-module-timeseries")]
#[path = "modules/timeseries/mod.rs"]
mod timeseries;
#[cfg(feature = "redis-module-topk")]
#[path = "modules/topk/mod.rs"]
mod topk;
#[cfg(feature = "redis-module-topk")]
pub(crate) use topk::{TopKError, TopKInfo, TopKStore};
