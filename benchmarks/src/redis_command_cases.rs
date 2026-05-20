#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RedisCommandFamily {
    Connection,
    Server,
    String,
    Key,
    Hash,
    List,
    Set,
    ZSet,
}

impl RedisCommandFamily {
    pub fn label(self) -> &'static str {
        match self {
            Self::Connection => "connection",
            Self::Server => "server",
            Self::String => "string",
            Self::Key => "key",
            Self::Hash => "hash",
            Self::List => "list",
            Self::Set => "set",
            Self::ZSet => "zset",
        }
    }
}

pub type RedisCommandParts = &'static [&'static str];

#[derive(Debug, Clone, Copy)]
pub struct RedisCommandCase {
    pub family: RedisCommandFamily,
    pub command_name: &'static str,
    pub case_name: &'static str,
    pub parts: RedisCommandParts,
}

impl RedisCommandCase {
    pub fn matches_filter(self, filter: &str) -> bool {
        filter.eq_ignore_ascii_case("all")
            || filter.eq_ignore_ascii_case(self.family.label())
            || filter.eq_ignore_ascii_case(self.command_name)
            || filter.eq_ignore_ascii_case(self.case_name)
    }
}

macro_rules! case {
    ($family:ident, $command_name:literal, $case_name:literal, [$($part:literal),+ $(,)?]) => {
        RedisCommandCase {
            family: RedisCommandFamily::$family,
            command_name: $command_name,
            case_name: $case_name,
            parts: &[$($part),+],
        }
    };
}

pub const REDIS_COMMAND_CASES: &[RedisCommandCase] = &[
    case!(Connection, "PING", "PING", ["PING"]),
    case!(Connection, "ECHO", "ECHO", ["ECHO", "hello"]),
    case!(Connection, "SELECT", "SELECT", ["SELECT", "0"]),
    case!(Server, "DBSIZE", "DBSIZE empty", ["DBSIZE"]),
    case!(String, "SET", "SET string", ["SET", "s", "v"]),
    case!(String, "GET", "GET string", ["GET", "s"]),
    case!(String, "SET", "SET NX miss", ["SET", "s-nx", "v", "NX"]),
    case!(String, "SET", "SET XX hit", ["SET", "s", "v2", "XX"]),
    case!(String, "SET", "SET EX", ["SET", "exp", "v", "EX", "60"]),
    case!(Key, "TTL", "TTL positive", ["TTL", "exp"]),
    case!(Key, "PTTL", "PTTL positive", ["PTTL", "exp"]),
    case!(Key, "PERSIST", "PERSIST", ["PERSIST", "exp"]),
    case!(Key, "TYPE", "TYPE string", ["TYPE", "s"]),
    case!(Key, "EXISTS", "EXISTS mixed", ["EXISTS", "s", "missing"]),
    case!(Key, "KEYS", "KEYS all", ["KEYS", "*"]),
    case!(
        Key,
        "SCAN",
        "SCAN all",
        ["SCAN", "0", "MATCH", "*", "COUNT", "1000"]
    ),
    case!(
        Key,
        "SCAN",
        "SCAN type string",
        ["SCAN", "0", "TYPE", "string", "COUNT", "1000"]
    ),
    case!(String, "APPEND", "APPEND", ["APPEND", "s", "+"]),
    case!(String, "STRLEN", "STRLEN", ["STRLEN", "s"]),
    case!(String, "GETRANGE", "GETRANGE", ["GETRANGE", "s", "1", "-1"]),
    case!(String, "SETRANGE", "SETRANGE", ["SETRANGE", "s", "1", "XX"]),
    case!(String, "GETSET", "GETSET", ["GETSET", "s", "old"]),
    case!(String, "GETDEL", "GETDEL", ["GETDEL", "s-del"]),
    case!(String, "INCR", "INCR", ["INCR", "n"]),
    case!(String, "INCRBY", "INCRBY", ["INCRBY", "n", "4"]),
    case!(String, "DECR", "DECR", ["DECR", "n"]),
    case!(String, "DECRBY", "DECRBY", ["DECRBY", "n", "2"]),
    case!(
        String,
        "INCRBYFLOAT",
        "INCRBYFLOAT",
        ["INCRBYFLOAT", "nf", "1.5"]
    ),
    case!(String, "MSET", "MSET", ["MSET", "ma", "1", "mb", "2"]),
    case!(
        String,
        "MGET",
        "MGET order",
        ["MGET", "mb", "missing", "ma"]
    ),
    case!(
        String,
        "MSETNX",
        "MSETNX existing",
        ["MSETNX", "ma", "x", "mc", "3"]
    ),
    case!(Hash, "HSET", "HSET", ["HSET", "h", "f1", "v1", "f2", "v2"]),
    case!(Hash, "HGET", "HGET", ["HGET", "h", "f1"]),
    case!(
        Hash,
        "HMGET",
        "HMGET",
        ["HMGET", "h", "f2", "missing", "f1"]
    ),
    case!(Hash, "HLEN", "HLEN", ["HLEN", "h"]),
    case!(Hash, "HEXISTS", "HEXISTS", ["HEXISTS", "h", "f2"]),
    case!(Hash, "HSETNX", "HSETNX", ["HSETNX", "h", "f3", "v3"]),
    case!(Hash, "HINCRBY", "HINCRBY", ["HINCRBY", "h", "num", "5"]),
    case!(
        Hash,
        "HINCRBYFLOAT",
        "HINCRBYFLOAT",
        ["HINCRBYFLOAT", "h", "float", "0.5"]
    ),
    case!(Hash, "HKEYS", "HKEYS", ["HKEYS", "h"]),
    case!(Hash, "HVALS", "HVALS", ["HVALS", "h"]),
    case!(Hash, "HGETALL", "HGETALL", ["HGETALL", "h"]),
    case!(
        Hash,
        "HSCAN",
        "HSCAN",
        ["HSCAN", "h", "0", "MATCH", "*", "COUNT", "1000"]
    ),
    case!(
        Hash,
        "HRANDFIELD",
        "HRANDFIELD WITHVALUES",
        ["HRANDFIELD", "h", "10", "WITHVALUES"]
    ),
    case!(Hash, "HDEL", "HDEL", ["HDEL", "h", "f1", "missing"]),
    case!(List, "LPUSH", "LPUSH", ["LPUSH", "l", "b", "a"]),
    case!(List, "RPUSH", "RPUSH", ["RPUSH", "l", "c"]),
    case!(List, "LRANGE", "LRANGE", ["LRANGE", "l", "0", "-1"]),
    case!(List, "LLEN", "LLEN", ["LLEN", "l"]),
    case!(List, "LINDEX", "LINDEX", ["LINDEX", "l", "1"]),
    case!(List, "LSET", "LSET", ["LSET", "l", "1", "B"]),
    case!(List, "LREM", "LREM", ["LREM", "l", "0", "B"]),
    case!(
        List,
        "LINSERT",
        "LINSERT",
        ["LINSERT", "l", "BEFORE", "c", "b"]
    ),
    case!(List, "LTRIM", "LTRIM", ["LTRIM", "l", "0", "1"]),
    case!(List, "LPOP", "LPOP", ["LPOP", "l"]),
    case!(List, "RPOP", "RPOP", ["RPOP", "l"]),
    case!(
        List,
        "LPUSHX",
        "LPUSHX missing",
        ["LPUSHX", "missing-list", "x"]
    ),
    case!(
        List,
        "RPUSHX",
        "RPUSHX missing",
        ["RPUSHX", "missing-list", "x"]
    ),
    case!(List, "RPUSH", "RPUSH bl seed", ["RPUSH", "bl", "a", "b"]),
    case!(List, "BLPOP", "BLPOP ready", ["BLPOP", "bl", "0.001"]),
    case!(List, "RPUSH", "RPUSH br seed", ["RPUSH", "br", "a", "b"]),
    case!(List, "BRPOP", "BRPOP ready", ["BRPOP", "br", "0.001"]),
    case!(List, "RPUSH", "RPUSH bm seed", ["RPUSH", "bm", "a", "b"]),
    case!(
        List,
        "BLMOVE",
        "BLMOVE ready",
        ["BLMOVE", "bm", "bmd", "RIGHT", "LEFT", "0.001"]
    ),
    case!(Set, "SADD", "SADD set-a", ["SADD", "set-a", "a", "b"]),
    case!(Set, "SADD", "SADD set-b", ["SADD", "set-b", "b", "c"]),
    case!(Set, "SISMEMBER", "SISMEMBER", ["SISMEMBER", "set-a", "a"]),
    case!(
        Set,
        "SMISMEMBER",
        "SMISMEMBER",
        ["SMISMEMBER", "set-a", "a", "x"]
    ),
    case!(Set, "SCARD", "SCARD", ["SCARD", "set-a"]),
    case!(Set, "SMEMBERS", "SMEMBERS", ["SMEMBERS", "set-a"]),
    case!(Set, "SUNION", "SUNION", ["SUNION", "set-a", "set-b"]),
    case!(Set, "SINTER", "SINTER", ["SINTER", "set-a", "set-b"]),
    case!(Set, "SDIFF", "SDIFF", ["SDIFF", "set-a", "set-b"]),
    case!(
        Set,
        "SUNIONSTORE",
        "SUNIONSTORE",
        ["SUNIONSTORE", "set-u", "set-a", "set-b"]
    ),
    case!(
        Set,
        "SINTERSTORE",
        "SINTERSTORE",
        ["SINTERSTORE", "set-i", "set-a", "set-b"]
    ),
    case!(
        Set,
        "SDIFFSTORE",
        "SDIFFSTORE",
        ["SDIFFSTORE", "set-d", "set-a", "set-b"]
    ),
    case!(Set, "SMOVE", "SMOVE", ["SMOVE", "set-a", "set-b", "a"]),
    case!(Set, "SREM", "SREM", ["SREM", "set-b", "a"]),
    case!(
        Set,
        "SSCAN",
        "SSCAN",
        ["SSCAN", "set-a", "0", "MATCH", "*", "COUNT", "1000"]
    ),
    case!(
        Set,
        "SRANDMEMBER",
        "SRANDMEMBER",
        ["SRANDMEMBER", "set-b", "2"]
    ),
    case!(Set, "SPOP", "SPOP", ["SPOP", "set-b", "1"]),
    case!(ZSet, "ZADD", "ZADD", ["ZADD", "z", "1", "a", "2", "b"]),
    case!(ZSet, "ZSCORE", "ZSCORE", ["ZSCORE", "z", "a"]),
    case!(ZSet, "ZCARD", "ZCARD", ["ZCARD", "z"]),
    case!(ZSet, "ZRANGE", "ZRANGE", ["ZRANGE", "z", "0", "-1"]),
    case!(
        ZSet,
        "ZRANGE",
        "ZRANGE WITHSCORES",
        ["ZRANGE", "z", "0", "-1", "WITHSCORES"]
    ),
    case!(ZSet, "ZINCRBY", "ZINCRBY", ["ZINCRBY", "z", "2", "a"]),
    case!(ZSet, "ZCOUNT", "ZCOUNT", ["ZCOUNT", "z", "0", "10"]),
    case!(ZSet, "ZRANK", "ZRANK", ["ZRANK", "z", "a"]),
    case!(ZSet, "ZREVRANK", "ZREVRANK", ["ZREVRANK", "z", "a"]),
    case!(ZSet, "ZPOPMIN", "ZPOPMIN", ["ZPOPMIN", "z", "1"]),
    case!(ZSet, "ZPOPMAX", "ZPOPMAX", ["ZPOPMAX", "z", "1"]),
    case!(
        ZSet,
        "ZADD",
        "ZADD z2 seed",
        ["ZADD", "z2", "1", "a", "2", "b", "3", "c"]
    ),
    case!(
        ZSet,
        "ZADD",
        "ZADD CH",
        ["ZADD", "z2", "CH", "5", "a", "4", "d"]
    ),
    case!(
        ZSet,
        "ZADD",
        "ZADD NX",
        ["ZADD", "z2", "NX", "6", "a", "7", "e"]
    ),
    case!(
        ZSet,
        "ZADD",
        "ZADD XX GT",
        ["ZADD", "z2", "XX", "GT", "8", "a"]
    ),
    case!(ZSet, "ZSCORE", "ZSCORE z2", ["ZSCORE", "z2", "a"]),
    case!(
        ZSet,
        "ZRANGE",
        "ZRANGE BYSCORE REV LIMIT",
        [
            "ZRANGE", "z2", "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "0", "3"
        ]
    ),
    case!(
        ZSet,
        "ZRANGEBYSCORE",
        "ZRANGEBYSCORE exclusive",
        ["ZRANGEBYSCORE", "z2", "(2", "8"]
    ),
    case!(
        ZSet,
        "ZSCAN",
        "ZSCAN",
        ["ZSCAN", "z2", "0", "MATCH", "*", "COUNT", "1000"]
    ),
    case!(
        ZSet,
        "ZADD",
        "ZADD zlex seed",
        ["ZADD", "zlex", "0", "a", "0", "b", "0", "c"]
    ),
    case!(
        ZSet,
        "ZRANGEBYLEX",
        "ZRANGEBYLEX",
        ["ZRANGEBYLEX", "zlex", "[a", "[c"]
    ),
    case!(
        ZSet,
        "ZREVRANGEBYLEX",
        "ZREVRANGEBYLEX",
        ["ZREVRANGEBYLEX", "zlex", "[c", "[a"]
    ),
    case!(
        ZSet,
        "ZLEXCOUNT",
        "ZLEXCOUNT",
        ["ZLEXCOUNT", "zlex", "(a", "[c"]
    ),
    case!(
        ZSet,
        "ZRANGESTORE",
        "ZRANGESTORE",
        ["ZRANGESTORE", "zstored", "z2", "0", "1"]
    ),
    case!(
        ZSet,
        "ZRANGE",
        "ZRANGE zstored",
        ["ZRANGE", "zstored", "0", "-1"]
    ),
    case!(
        ZSet,
        "ZADD",
        "ZADD zu1",
        ["ZADD", "zu1", "1", "a", "2", "b"]
    ),
    case!(
        ZSet,
        "ZADD",
        "ZADD zu2",
        ["ZADD", "zu2", "3", "a", "4", "c"]
    ),
    case!(
        ZSet,
        "ZUNIONSTORE",
        "ZUNIONSTORE weighted",
        [
            "ZUNIONSTORE",
            "zout",
            "2",
            "zu1",
            "zu2",
            "WEIGHTS",
            "2",
            "3",
            "AGGREGATE",
            "SUM",
        ]
    ),
    case!(
        ZSet,
        "ZRANGE",
        "ZRANGE zout",
        ["ZRANGE", "zout", "0", "-1", "WITHSCORES"]
    ),
    case!(
        ZSet,
        "ZINTERSTORE",
        "ZINTERSTORE weighted",
        [
            "ZINTERSTORE",
            "zi",
            "2",
            "zu1",
            "zu2",
            "WEIGHTS",
            "2",
            "3",
            "AGGREGATE",
            "SUM",
        ]
    ),
    case!(
        ZSet,
        "ZRANGE",
        "ZRANGE zi",
        ["ZRANGE", "zi", "0", "-1", "WITHSCORES"]
    ),
    case!(
        ZSet,
        "ZDIFFSTORE",
        "ZDIFFSTORE",
        ["ZDIFFSTORE", "zd", "2", "zu1", "zu2"]
    ),
    case!(
        ZSet,
        "ZRANGE",
        "ZRANGE zd",
        ["ZRANGE", "zd", "0", "-1", "WITHSCORES"]
    ),
    case!(ZSet, "ZADD", "ZADD bzmin", ["ZADD", "bzmin", "1", "a"]),
    case!(
        ZSet,
        "BZPOPMIN",
        "BZPOPMIN ready",
        ["BZPOPMIN", "bzmin", "0.001"]
    ),
    case!(ZSet, "ZADD", "ZADD bzmax", ["ZADD", "bzmax", "1", "a"]),
    case!(
        ZSet,
        "BZPOPMAX",
        "BZPOPMAX ready",
        ["BZPOPMAX", "bzmax", "0.001"]
    ),
];

pub const BENCHMARKED_COMMANDS: &[&str] = &[
    "APPEND",
    "BLMOVE",
    "BLPOP",
    "BRPOP",
    "BZPOPMAX",
    "BZPOPMIN",
    "DBSIZE",
    "DECR",
    "DECRBY",
    "ECHO",
    "EXISTS",
    "GET",
    "GETDEL",
    "GETRANGE",
    "GETSET",
    "HDEL",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HRANDFIELD",
    "HSCAN",
    "HSET",
    "HSETNX",
    "HVALS",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "KEYS",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LPOP",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "MGET",
    "MSET",
    "MSETNX",
    "PERSIST",
    "PING",
    "PTTL",
    "RPOP",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SCAN",
    "SCARD",
    "SDIFF",
    "SDIFFSTORE",
    "SELECT",
    "SET",
    "SETRANGE",
    "SINTER",
    "SINTERSTORE",
    "SISMEMBER",
    "SMEMBERS",
    "SMISMEMBER",
    "SMOVE",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "SSCAN",
    "STRLEN",
    "SUNION",
    "SUNIONSTORE",
    "TTL",
    "TYPE",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZDIFFSTORE",
    "ZINCRBY",
    "ZINTERSTORE",
    "ZLEXCOUNT",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZRANGESTORE",
    "ZRANK",
    "ZREVRANGEBYLEX",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
    "ZUNIONSTORE",
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{BENCHMARKED_COMMANDS, REDIS_COMMAND_CASES};

    #[test]
    fn benchmark_cases_cover_declared_commands() {
        let actual = REDIS_COMMAND_CASES
            .iter()
            .map(|case| case.command_name)
            .collect::<BTreeSet<_>>();
        let expected = BENCHMARKED_COMMANDS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
        let extra = actual.difference(&expected).copied().collect::<Vec<_>>();

        assert!(
            missing.is_empty(),
            "benchmark cases missing commands: {missing:?}"
        );
        assert!(
            extra.is_empty(),
            "benchmark cases include undeclared commands: {extra:?}"
        );
    }

    #[test]
    fn benchmark_case_names_are_unique() {
        let mut names = BTreeSet::new();
        for case in REDIS_COMMAND_CASES {
            assert!(
                names.insert(case.case_name),
                "duplicate Redis command benchmark case: {}",
                case.case_name
            );
            assert!(
                !case.parts.is_empty(),
                "{} has no command parts",
                case.case_name
            );
        }
    }
}
