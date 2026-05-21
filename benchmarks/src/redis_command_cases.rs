#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RedisCommandFamily {
    Connection,
    Server,
    Transaction,
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
            Self::Transaction => "transaction",
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
pub type RedisCommandSetup = &'static [RedisCommandParts];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RedisCommandProfile {
    Small,
    Large,
}

impl RedisCommandProfile {
    pub fn label(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Large => "large",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RedisCommandCase {
    pub family: RedisCommandFamily,
    pub command_name: &'static str,
    pub case_name: &'static str,
    pub profile: RedisCommandProfile,
    pub parts: RedisCommandParts,
    pub script: RedisCommandSetup,
    pub setup: RedisCommandSetup,
}

impl RedisCommandCase {
    pub fn matches_filter(self, filter: &str) -> bool {
        filter.eq_ignore_ascii_case("all")
            || filter.eq_ignore_ascii_case(self.profile.label())
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
            profile: RedisCommandProfile::Small,
            parts: &[$($part),+],
            script: &[],
            setup: &[],
        }
    };
}

macro_rules! case_with_setup {
    (
        $family:ident,
        $command_name:literal,
        $case_name:literal,
        [$($part:literal),+ $(,)?],
        [$([$($setup_part:literal),+ $(,)?]),* $(,)?]
    ) => {
        RedisCommandCase {
            family: RedisCommandFamily::$family,
            command_name: $command_name,
            case_name: $case_name,
            profile: RedisCommandProfile::Small,
            parts: &[$($part),+],
            script: &[],
            setup: &[$(&[$($setup_part),+] as RedisCommandParts),*],
        }
    };
}

macro_rules! case_script {
    (
        $family:ident,
        $command_name:literal,
        $case_name:literal,
        [$($display_part:literal),+ $(,)?],
        [$([$($script_part:literal),+ $(,)?]),+ $(,)?]
    ) => {
        RedisCommandCase {
            family: RedisCommandFamily::$family,
            command_name: $command_name,
            case_name: $case_name,
            profile: RedisCommandProfile::Small,
            parts: &[$($display_part),+],
            script: &[$(&[$($script_part),+] as RedisCommandParts),+],
            setup: &[],
        }
    };
}

macro_rules! large_case {
    (
        $family:ident,
        $command_name:literal,
        $case_name:literal,
        [$($part:literal),+ $(,)?],
        [$([$($setup_part:literal),+ $(,)?]),* $(,)?]
    ) => {
        RedisCommandCase {
            family: RedisCommandFamily::$family,
            command_name: $command_name,
            case_name: $case_name,
            profile: RedisCommandProfile::Large,
            parts: &[$($part),+],
            script: &[],
            setup: &[$(&[$($setup_part),+] as RedisCommandParts),*],
        }
    };
}

pub const REDIS_COMMAND_CASES: &[RedisCommandCase] = &[
    case!(Connection, "PING", "PING", ["PING"]),
    case!(Connection, "ECHO", "ECHO", ["ECHO", "hello"]),
    case!(Connection, "SELECT", "SELECT", ["SELECT", "0"]),
    case!(Server, "DBSIZE", "DBSIZE empty", ["DBSIZE"]),
    case_script!(
        Transaction,
        "MULTI",
        "MULTI DISCARD",
        ["MULTI"],
        [["MULTI"], ["DISCARD"]]
    ),
    case_script!(
        Transaction,
        "EXEC",
        "MULTI EXEC SET GET",
        ["EXEC"],
        [["MULTI"], ["SET", "txn", "v"], ["GET", "txn"], ["EXEC"]]
    ),
    case_script!(
        Transaction,
        "DISCARD",
        "DISCARD queued SET",
        ["DISCARD"],
        [["MULTI"], ["SET", "txn-discard", "v"], ["DISCARD"]]
    ),
    case!(String, "SET", "SET string", ["SET", "s", "v"]),
    case!(String, "GET", "GET string", ["GET", "s"]),
    case!(String, "SET", "SET NX miss", ["SET", "s-nx", "v", "NX"]),
    case!(String, "SET", "SET XX hit", ["SET", "s", "v2", "XX"]),
    case_with_setup!(
        String,
        "SETNX",
        "SETNX existing",
        ["SETNX", "setnx-bench", "v2"],
        [["SET", "setnx-bench", "v"]]
    ),
    case!(String, "SET", "SET EX", ["SET", "exp", "v", "EX", "60"]),
    case!(Key, "TTL", "TTL positive", ["TTL", "exp"]),
    case!(Key, "PTTL", "PTTL positive", ["PTTL", "exp"]),
    case!(Key, "PERSIST", "PERSIST", ["PERSIST", "exp"]),
    case_with_setup!(
        Key,
        "EXPIREAT",
        "EXPIREAT future",
        ["EXPIREAT", "expireat-bench", "9999999999"],
        [["SET", "expireat-bench", "v"]]
    ),
    case_with_setup!(
        Key,
        "PEXPIREAT",
        "PEXPIREAT future",
        ["PEXPIREAT", "pexpireat-bench", "9999999999000"],
        [["SET", "pexpireat-bench", "v"]]
    ),
    case!(Key, "TYPE", "TYPE string", ["TYPE", "s"]),
    case_with_setup!(
        Key,
        "OBJECT",
        "OBJECT ENCODING string",
        ["OBJECT", "ENCODING", "object-bench"],
        [["SET", "object-bench", "v"]]
    ),
    case!(Key, "EXISTS", "EXISTS mixed", ["EXISTS", "s", "missing"]),
    case_with_setup!(
        Key,
        "TOUCH",
        "TOUCH mixed",
        ["TOUCH", "touch-a", "touch-b", "missing"],
        [["MSET", "touch-a", "1", "touch-b", "2"]]
    ),
    case_with_setup!(
        Key,
        "RANDOMKEY",
        "RANDOMKEY nonempty",
        ["RANDOMKEY"],
        [["SET", "randomkey-bench", "v"]]
    ),
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
    case_with_setup!(
        Key,
        "RENAME",
        "RENAME a to b",
        ["RENAME", "rename-a", "rename-b"],
        [["SET", "rename-a", "v"]]
    ),
    case_with_setup!(
        Key,
        "RENAME",
        "RENAME b to a",
        ["RENAME", "rename-b", "rename-a"],
        [["SET", "rename-a", "v"]]
    ),
    case_with_setup!(
        Key,
        "RENAMENX",
        "RENAMENX existing dest",
        ["RENAMENX", "renamenx-src", "renamenx-dst"],
        [
            ["SET", "renamenx-src", "v"],
            ["SET", "renamenx-dst", "existing"]
        ]
    ),
    case!(
        Key,
        "UNLINK",
        "UNLINK missing",
        ["UNLINK", "missing-unlink"]
    ),
    case_with_setup!(
        Key,
        "COPY",
        "COPY existing dest",
        ["COPY", "copy-src", "copy-dst"],
        [["SET", "copy-src", "v"], ["SET", "copy-dst", "existing"]]
    ),
    case!(String, "APPEND", "APPEND", ["APPEND", "s", "+"]),
    case!(String, "STRLEN", "STRLEN", ["STRLEN", "s"]),
    case!(String, "GETRANGE", "GETRANGE", ["GETRANGE", "s", "1", "-1"]),
    case!(String, "SETRANGE", "SETRANGE", ["SETRANGE", "s", "1", "XX"]),
    case_with_setup!(
        String,
        "GETBIT",
        "GETBIT",
        ["GETBIT", "bitstr", "1"],
        [["SET", "bitstr", "A"]]
    ),
    case_with_setup!(
        String,
        "SETBIT",
        "SETBIT",
        ["SETBIT", "bitstr", "3", "1"],
        [["SET", "bitstr", "A"]]
    ),
    case_with_setup!(
        String,
        "BITCOUNT",
        "BITCOUNT",
        ["BITCOUNT", "bitstr"],
        [["SET", "bitstr", "A"]]
    ),
    case_with_setup!(
        String,
        "BITPOS",
        "BITPOS",
        ["BITPOS", "bitstr", "1"],
        [["SET", "bitstr", "A"]]
    ),
    case_with_setup!(
        String,
        "BITOP",
        "BITOP OR",
        ["BITOP", "OR", "bit-out", "bit-a", "bit-b"],
        [["SET", "bit-a", "A"], ["SET", "bit-b", "B"]]
    ),
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
    case!(
        Hash,
        "HMSET",
        "HMSET",
        ["HMSET", "hm", "f1", "v1", "f2", "v2"]
    ),
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
    case_with_setup!(
        Hash,
        "HSTRLEN",
        "HSTRLEN",
        ["HSTRLEN", "h", "f2"],
        [["HSET", "h", "f2", "v2"]]
    ),
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
    case_with_setup!(
        List,
        "RPOPLPUSH",
        "RPOPLPUSH self",
        ["RPOPLPUSH", "l", "l"],
        [["RPUSH", "l", "a", "b", "c"]]
    ),
    case_with_setup!(
        List,
        "LMOVE",
        "LMOVE self",
        ["LMOVE", "lmove-bench", "lmove-bench", "RIGHT", "LEFT"],
        [["RPUSH", "lmove-bench", "a", "b", "c"]]
    ),
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
    case_with_setup!(
        ZSet,
        "ZREVRANGE",
        "ZREVRANGE",
        ["ZREVRANGE", "z", "0", "-1"],
        [["ZADD", "z", "1", "a", "2", "b"]]
    ),
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
    case_with_setup!(
        ZSet,
        "ZREVRANGEBYSCORE",
        "ZREVRANGEBYSCORE limit",
        ["ZREVRANGEBYSCORE", "z2", "8", "(2", "LIMIT", "0", "2"],
        [["ZADD", "z2", "1", "a", "2", "b", "3", "c"]]
    ),
    case_with_setup!(
        ZSet,
        "ZREMRANGEBYRANK",
        "ZREMRANGEBYRANK no-op",
        ["ZREMRANGEBYRANK", "z2", "999", "1000"],
        [["ZADD", "z2", "1", "a", "2", "b", "3", "c"]]
    ),
    case_with_setup!(
        ZSet,
        "ZREMRANGEBYSCORE",
        "ZREMRANGEBYSCORE no-op",
        ["ZREMRANGEBYSCORE", "z2", "999", "1000"],
        [["ZADD", "z2", "1", "a", "2", "b", "3", "c"]]
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
    case_with_setup!(
        ZSet,
        "ZREMRANGEBYLEX",
        "ZREMRANGEBYLEX no-op",
        ["ZREMRANGEBYLEX", "zlex", "[z", "[zz"],
        [["ZADD", "zlex", "0", "a", "0", "b", "0", "c"]]
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

pub const REDIS_COMMAND_LARGE_CASES: &[RedisCommandCase] = &[
    large_case!(
        String,
        "SET",
        "SET large 4KiB value",
        ["SET", "$key:large-s-4k", "$value:4096"],
        []
    ),
    large_case!(
        String,
        "GET",
        "GET large 4KiB value",
        ["GET", "$key:large-s-4k"],
        [["SET", "$key:large-s-4k", "$value:4096"]]
    ),
    large_case!(
        String,
        "SET",
        "SET large 64KiB value",
        ["SET", "$key:large-s-64k", "$value:65536"],
        []
    ),
    large_case!(
        String,
        "GET",
        "GET large 64KiB value",
        ["GET", "$key:large-s-64k"],
        [["SET", "$key:large-s-64k", "$value:65536"]]
    ),
    large_case!(
        String,
        "STRLEN",
        "STRLEN large 64KiB value",
        ["STRLEN", "$key:large-s-64k"],
        [["SET", "$key:large-s-64k", "$value:65536"]]
    ),
    large_case!(
        String,
        "GETRANGE",
        "GETRANGE large 64KiB full",
        ["GETRANGE", "$key:large-s-64k", "0", "-1"],
        [["SET", "$key:large-s-64k", "$value:65536"]]
    ),
    large_case!(
        Key,
        "KEYS",
        "KEYS large keyspace",
        ["KEYS", "ks:*"],
        [["MSET", "$kvpairs:4096"]]
    ),
    large_case!(
        Key,
        "SCAN",
        "SCAN large keyspace",
        ["SCAN", "0", "MATCH", "ks:*", "COUNT", "1000"],
        [["MSET", "$kvpairs:4096"]]
    ),
    large_case!(
        Hash,
        "HGETALL",
        "HGETALL large 1K fields",
        ["HGETALL", "$key:large-h"],
        [
            ["DEL", "$key:large-h"],
            ["HSET", "$key:large-h", "$hash-fields:1024"]
        ]
    ),
    large_case!(
        Hash,
        "HKEYS",
        "HKEYS large 1K fields",
        ["HKEYS", "$key:large-h"],
        [
            ["DEL", "$key:large-h"],
            ["HSET", "$key:large-h", "$hash-fields:1024"]
        ]
    ),
    large_case!(
        Hash,
        "HVALS",
        "HVALS large 1K fields",
        ["HVALS", "$key:large-h"],
        [
            ["DEL", "$key:large-h"],
            ["HSET", "$key:large-h", "$hash-fields:1024"]
        ]
    ),
    large_case!(
        Hash,
        "HSCAN",
        "HSCAN large 1K fields",
        ["HSCAN", "$key:large-h", "0", "MATCH", "*", "COUNT", "1000"],
        [
            ["DEL", "$key:large-h"],
            ["HSET", "$key:large-h", "$hash-fields:1024"]
        ]
    ),
    large_case!(
        Hash,
        "HLEN",
        "HLEN large 1K fields",
        ["HLEN", "$key:large-h"],
        [
            ["DEL", "$key:large-h"],
            ["HSET", "$key:large-h", "$hash-fields:1024"]
        ]
    ),
    large_case!(
        Hash,
        "HMGET",
        "HMGET large selected fields",
        [
            "HMGET",
            "$key:large-h",
            "f:000000",
            "f:000511",
            "missing",
            "f:001023"
        ],
        [
            ["DEL", "$key:large-h"],
            ["HSET", "$key:large-h", "$hash-fields:1024"]
        ]
    ),
    large_case!(
        List,
        "LRANGE",
        "LRANGE large 1K full",
        ["LRANGE", "$key:large-l", "0", "-1"],
        [
            ["DEL", "$key:large-l"],
            ["RPUSH", "$key:large-l", "$list-values:1024"]
        ]
    ),
    large_case!(
        List,
        "LLEN",
        "LLEN large 1K list",
        ["LLEN", "$key:large-l"],
        [
            ["DEL", "$key:large-l"],
            ["RPUSH", "$key:large-l", "$list-values:1024"]
        ]
    ),
    large_case!(
        List,
        "LINDEX",
        "LINDEX large middle",
        ["LINDEX", "$key:large-l", "512"],
        [
            ["DEL", "$key:large-l"],
            ["RPUSH", "$key:large-l", "$list-values:1024"]
        ]
    ),
    large_case!(
        Set,
        "SCARD",
        "SCARD large 1K set",
        ["SCARD", "$key:large-set-a"],
        [
            ["DEL", "$key:large-set-a"],
            ["SADD", "$key:large-set-a", "$members:1024"]
        ]
    ),
    large_case!(
        Set,
        "SMEMBERS",
        "SMEMBERS large 1K set",
        ["SMEMBERS", "$key:large-set-a"],
        [
            ["DEL", "$key:large-set-a"],
            ["SADD", "$key:large-set-a", "$members:1024"]
        ]
    ),
    large_case!(
        Set,
        "SSCAN",
        "SSCAN large 1K set",
        [
            "SSCAN",
            "$key:large-set-a",
            "0",
            "MATCH",
            "*",
            "COUNT",
            "1000"
        ],
        [
            ["DEL", "$key:large-set-a"],
            ["SADD", "$key:large-set-a", "$members:1024"]
        ]
    ),
    large_case!(
        Set,
        "SRANDMEMBER",
        "SRANDMEMBER large 32",
        ["SRANDMEMBER", "$key:large-set-a", "32"],
        [
            ["DEL", "$key:large-set-a"],
            ["SADD", "$key:large-set-a", "$members:1024"]
        ]
    ),
    large_case!(
        Set,
        "SMISMEMBER",
        "SMISMEMBER large selected",
        [
            "SMISMEMBER",
            "$key:large-set-a",
            "m:000000",
            "missing",
            "m:001023"
        ],
        [
            ["DEL", "$key:large-set-a"],
            ["SADD", "$key:large-set-a", "$members:1024"]
        ]
    ),
    large_case!(
        ZSet,
        "ZRANGE",
        "ZRANGE large 1K full",
        ["ZRANGE", "$key:large-z", "0", "-1"],
        [
            ["DEL", "$key:large-z"],
            ["ZADD", "$key:large-z", "$zitems:1024"]
        ]
    ),
    large_case!(
        ZSet,
        "ZRANGE",
        "ZRANGE large 100 WITHSCORES",
        ["ZRANGE", "$key:large-z", "0", "99", "WITHSCORES"],
        [
            ["DEL", "$key:large-z"],
            ["ZADD", "$key:large-z", "$zitems:1024"]
        ]
    ),
    large_case!(
        ZSet,
        "ZCOUNT",
        "ZCOUNT large 1K all",
        ["ZCOUNT", "$key:large-z", "0", "2000"],
        [
            ["DEL", "$key:large-z"],
            ["ZADD", "$key:large-z", "$zitems:1024"]
        ]
    ),
    large_case!(
        ZSet,
        "ZRANK",
        "ZRANK large middle",
        ["ZRANK", "$key:large-z", "m:000512"],
        [
            ["DEL", "$key:large-z"],
            ["ZADD", "$key:large-z", "$zitems:1024"]
        ]
    ),
    large_case!(
        ZSet,
        "ZREVRANK",
        "ZREVRANK large middle",
        ["ZREVRANK", "$key:large-z", "m:000512"],
        [
            ["DEL", "$key:large-z"],
            ["ZADD", "$key:large-z", "$zitems:1024"]
        ]
    ),
    large_case!(
        ZSet,
        "ZSCORE",
        "ZSCORE large middle",
        ["ZSCORE", "$key:large-z", "m:000512"],
        [
            ["DEL", "$key:large-z"],
            ["ZADD", "$key:large-z", "$zitems:1024"]
        ]
    ),
    large_case!(
        ZSet,
        "ZSCAN",
        "ZSCAN large 1K zset",
        ["ZSCAN", "$key:large-z", "0", "MATCH", "*", "COUNT", "1000"],
        [
            ["DEL", "$key:large-z"],
            ["ZADD", "$key:large-z", "$zitems:1024"]
        ]
    ),
];

pub const BENCHMARKED_COMMANDS: &[&str] = &[
    "APPEND",
    "BITCOUNT",
    "BITOP",
    "BITPOS",
    "BLMOVE",
    "BLPOP",
    "BRPOP",
    "BZPOPMAX",
    "BZPOPMIN",
    "COPY",
    "DBSIZE",
    "DECR",
    "DECRBY",
    "DISCARD",
    "ECHO",
    "EXEC",
    "EXISTS",
    "EXPIREAT",
    "GET",
    "GETBIT",
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
    "HMSET",
    "HMGET",
    "HRANDFIELD",
    "HSCAN",
    "HSET",
    "HSETNX",
    "HSTRLEN",
    "HVALS",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "KEYS",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LMOVE",
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
    "MULTI",
    "OBJECT",
    "PERSIST",
    "PING",
    "PEXPIREAT",
    "PTTL",
    "RANDOMKEY",
    "RENAME",
    "RENAMENX",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SCAN",
    "SCARD",
    "SDIFF",
    "SDIFFSTORE",
    "SELECT",
    "SET",
    "SETBIT",
    "SETNX",
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
    "TOUCH",
    "TYPE",
    "UNLINK",
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
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYLEX",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
    "ZUNIONSTORE",
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{BENCHMARKED_COMMANDS, REDIS_COMMAND_CASES, REDIS_COMMAND_LARGE_CASES};

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
        for case in REDIS_COMMAND_CASES
            .iter()
            .chain(REDIS_COMMAND_LARGE_CASES.iter())
        {
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

    #[test]
    fn large_benchmark_cases_are_in_large_profile() {
        assert!(
            !REDIS_COMMAND_LARGE_CASES.is_empty(),
            "large command benchmark profile is empty"
        );
        for case in REDIS_COMMAND_LARGE_CASES {
            assert!(case.matches_filter("large"));
            assert!(
                case.case_name.contains("large"),
                "large profile case should be clearly labeled: {}",
                case.case_name
            );
        }
    }
}
