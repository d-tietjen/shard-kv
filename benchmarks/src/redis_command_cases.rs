#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RedisCommandFamily {
    Connection,
    Server,
    PubSub,
    Transaction,
    Scripting,
    String,
    HyperLogLog,
    Geo,
    Stream,
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
            Self::PubSub => "pubsub",
            Self::Transaction => "transaction",
            Self::Scripting => "scripting",
            Self::String => "string",
            Self::HyperLogLog => "hyperloglog",
            Self::Geo => "geo",
            Self::Stream => "stream",
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
    Destructive,
}

impl RedisCommandProfile {
    pub fn label(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Large => "large",
            Self::Destructive => "destructive",
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
    pub expect_error: bool,
}

impl RedisCommandCase {
    pub fn matches_filter(self, filter: &str) -> bool {
        filter.eq_ignore_ascii_case("all")
            || filter.eq_ignore_ascii_case(self.profile.label())
            || filter.eq_ignore_ascii_case(self.family.label())
            || filter.eq_ignore_ascii_case(self.command_name)
            || filter.eq_ignore_ascii_case(self.case_name)
    }

    pub fn is_keyspace_wide(self) -> bool {
        self.command_name == "FLUSHALL"
            || self.command_name == "FLUSHDB"
            || self.command_name == "KEYS"
            || self.command_name == "RANDOMKEY"
            || matches!(
                self.case_name,
                "SCAN all" | "SCAN type string" | "SCAN large keyspace"
            )
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
            expect_error: false,
        }
    };
}

macro_rules! error_case {
    ($family:ident, $command_name:literal, $case_name:literal, [$($part:literal),+ $(,)?]) => {
        RedisCommandCase {
            family: RedisCommandFamily::$family,
            command_name: $command_name,
            case_name: $case_name,
            profile: RedisCommandProfile::Small,
            parts: &[$($part),+],
            script: &[],
            setup: &[],
            expect_error: true,
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
            expect_error: false,
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
            expect_error: false,
        }
    };
}

macro_rules! destructive_case_script {
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
            profile: RedisCommandProfile::Destructive,
            parts: &[$($display_part),+],
            script: &[$(&[$($script_part),+] as RedisCommandParts),+],
            setup: &[],
            expect_error: false,
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
            expect_error: false,
        }
    };
}

pub const REDIS_COMMAND_CASES: &[RedisCommandCase] = &[
    case!(Connection, "AUTH", "AUTH", ["AUTH", "unused"]),
    case!(Connection, "PING", "PING", ["PING"]),
    case!(Connection, "ECHO", "ECHO", ["ECHO", "hello"]),
    case!(Connection, "HELLO", "HELLO 2", ["HELLO", "2"]),
    case!(Connection, "SELECT", "SELECT", ["SELECT", "0"]),
    case!(
        Connection,
        "CLIENT",
        "CLIENT GETNAME",
        ["CLIENT", "GETNAME"]
    ),
    case!(
        Connection,
        "CLIENT",
        "CLIENT SETNAME",
        ["CLIENT", "SETNAME", "bench"]
    ),
    case!(Connection, "CLIENT", "CLIENT ID", ["CLIENT", "ID"]),
    case!(Connection, "CLIENT", "CLIENT LIST", ["CLIENT", "LIST"]),
    case!(
        Connection,
        "CLIENT",
        "CLIENT KILL ID 0",
        ["CLIENT", "KILL", "ID", "0"]
    ),
    case!(Server, "DBSIZE", "DBSIZE empty", ["DBSIZE"]),
    case!(Server, "TIME", "TIME", ["TIME"]),
    case!(Server, "INFO", "INFO", ["INFO"]),
    case_with_setup!(
        Server,
        "MEMORY",
        "MEMORY USAGE string",
        ["MEMORY", "USAGE", "memory-bench"],
        [["SET", "memory-bench", "v"]]
    ),
    case!(Server, "COMMAND", "COMMAND", ["COMMAND"]),
    case!(Server, "COMMAND", "COMMAND COUNT", ["COMMAND", "COUNT"]),
    case!(Server, "COMMAND", "COMMAND LIST", ["COMMAND", "LIST"]),
    case!(
        Server,
        "COMMAND",
        "COMMAND INFO GET",
        ["COMMAND", "INFO", "GET"]
    ),
    case!(
        Server,
        "COMMAND",
        "COMMAND GETKEYS MGET",
        ["COMMAND", "GETKEYS", "MGET", "ca", "cb"]
    ),
    case!(
        Server,
        "COMMAND",
        "COMMAND GETKEYSANDFLAGS MGET",
        ["COMMAND", "GETKEYSANDFLAGS", "MGET", "ca", "cb"]
    ),
    case!(Server, "COMMAND", "COMMAND DOCS", ["COMMAND", "DOCS"]),
    case!(Server, "COMMAND", "COMMAND HELP", ["COMMAND", "HELP"]),
    case!(
        Scripting,
        "EVAL",
        "EVAL return bulk",
        ["EVAL", "return 'ok'", "0"]
    ),
    case_with_setup!(
        Scripting,
        "EVALSHA",
        "EVALSHA return bulk",
        ["EVALSHA", "34f6a80fdc91746367dd8b572351df66b92c67ed", "0"],
        [["SCRIPT", "LOAD", "return 'ok'"]]
    ),
    case!(
        Scripting,
        "SCRIPT",
        "SCRIPT LOAD",
        ["SCRIPT", "LOAD", "return 'ok'"]
    ),
    case!(Server, "CONFIG", "CONFIG GET all", ["CONFIG", "GET", "*"]),
    case!(Server, "ASKING", "ASKING", ["ASKING"]),
    case!(Server, "BGREWRITEAOF", "BGREWRITEAOF", ["BGREWRITEAOF"]),
    case!(Server, "BGSAVE", "BGSAVE", ["BGSAVE"]),
    error_case!(Server, "CLUSTER", "CLUSTER INFO", ["CLUSTER", "INFO"]),
    case!(Server, "DEBUG", "DEBUG HELP", ["DEBUG", "HELP"]),
    error_case!(Server, "HOST:", "HOST attack warning", ["HOST:"]),
    case!(Server, "LASTSAVE", "LASTSAVE", ["LASTSAVE"]),
    case!(Server, "LATENCY", "LATENCY LATEST", ["LATENCY", "LATEST"]),
    case!(Server, "LOLWUT", "LOLWUT", ["LOLWUT"]),
    error_case!(
        Server,
        "MIGRATE",
        "MIGRATE unsupported",
        ["MIGRATE", "127.0.0.1", "9", "missing", "0", "1"]
    ),
    case!(Server, "MODULE", "MODULE LIST", ["MODULE", "LIST"]),
    error_case!(Server, "MONITOR", "MONITOR disabled", ["MONITOR"]),
    error_case!(Server, "MOVE", "MOVE same db", ["MOVE", "move-bench", "0"]),
    error_case!(Server, "POST", "POST attack warning", ["POST"]),
    error_case!(Server, "PSYNC", "PSYNC unsupported", ["PSYNC", "?", "-1"]),
    case!(Server, "READONLY", "READONLY", ["READONLY"]),
    case!(Server, "READWRITE", "READWRITE", ["READWRITE"]),
    case!(Server, "REPLCONF", "REPLCONF ACK", ["REPLCONF", "ACK", "0"]),
    case!(
        Server,
        "REPLICAOF",
        "REPLICAOF NO ONE",
        ["REPLICAOF", "NO", "ONE"]
    ),
    case!(
        Server,
        "SLAVEOF",
        "SLAVEOF NO ONE",
        ["SLAVEOF", "NO", "ONE"]
    ),
    case!(Server, "ROLE", "ROLE", ["ROLE"]),
    case!(Server, "SAVE", "SAVE", ["SAVE"]),
    error_case!(Server, "SHUTDOWN", "SHUTDOWN disabled", ["SHUTDOWN"]),
    case!(Server, "SLOWLOG", "SLOWLOG LEN", ["SLOWLOG", "LEN"]),
    case!(Server, "SORT", "SORT missing", ["SORT", "sort-missing"]),
    case!(Server, "SWAPDB", "SWAPDB 0 0", ["SWAPDB", "0", "0"]),
    error_case!(Server, "SYNC", "SYNC unsupported", ["SYNC"]),
    case!(Server, "WAIT", "WAIT", ["WAIT", "1", "1"]),
    case!(
        PubSub,
        "PUBLISH",
        "PUBLISH no subscribers",
        ["PUBLISH", "$key:bench-channel", "payload"]
    ),
    case!(PubSub, "PUBSUB", "PUBSUB NUMPAT", ["PUBSUB", "NUMPAT"]),
    case!(
        PubSub,
        "SUBSCRIBE",
        "SUBSCRIBE ack",
        ["SUBSCRIBE", "$key:bench-channel"]
    ),
    case!(
        PubSub,
        "UNSUBSCRIBE",
        "UNSUBSCRIBE ack",
        ["UNSUBSCRIBE", "$key:bench-channel"]
    ),
    case!(
        PubSub,
        "PSUBSCRIBE",
        "PSUBSCRIBE ack",
        ["PSUBSCRIBE", "$key:bench-*"]
    ),
    case!(
        PubSub,
        "PUNSUBSCRIBE",
        "PUNSUBSCRIBE ack",
        ["PUNSUBSCRIBE", "$key:bench-*"]
    ),
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
    case_script!(
        Transaction,
        "WATCH",
        "WATCH simple",
        ["WATCH", "$key:watch-bench"],
        [["WATCH", "$key:watch-bench"], ["UNWATCH"]]
    ),
    case_script!(
        Transaction,
        "UNWATCH",
        "UNWATCH simple",
        ["UNWATCH"],
        [["WATCH", "$key:unwatch-bench"], ["UNWATCH"]]
    ),
    case!(String, "SET", "SET string", ["SET", "s", "v"]),
    case!(String, "GET", "GET string", ["GET", "s"]),
    case!(
        String,
        "SETEX",
        "SETEX",
        ["SETEX", "$key:setex-bench", "60", "v"]
    ),
    case!(
        String,
        "PSETEX",
        "PSETEX",
        ["PSETEX", "$key:psetex-bench", "60000", "v"]
    ),
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
        "EXPIRE",
        "EXPIRE future",
        ["EXPIRE", "$key:expire-bench", "60"],
        [["SET", "$key:expire-bench", "v"]]
    ),
    case_with_setup!(
        Key,
        "PEXPIRE",
        "PEXPIRE future",
        ["PEXPIRE", "$key:pexpire-bench", "60000"],
        [["SET", "$key:pexpire-bench", "v"]]
    ),
    case_with_setup!(
        Key,
        "EXPIREAT",
        "EXPIREAT future",
        ["EXPIREAT", "expireat-bench", "2000000000"],
        [["SET", "expireat-bench", "v"]]
    ),
    case_with_setup!(
        Key,
        "PEXPIREAT",
        "PEXPIREAT future",
        ["PEXPIREAT", "pexpireat-bench", "2000000000000"],
        [["SET", "pexpireat-bench", "v"]]
    ),
    case_with_setup!(
        Key,
        "EXPIRE",
        "EXPIRE NX",
        ["EXPIRE", "$key:expire-nx-bench", "60", "NX"],
        [["SET", "$key:expire-nx-bench", "v"]]
    ),
    case_with_setup!(
        Key,
        "PEXPIRE",
        "PEXPIRE XX",
        ["PEXPIRE", "$key:pexpire-xx-bench", "60000", "XX"],
        [
            ["SET", "$key:pexpire-xx-bench", "v"],
            ["PEXPIRE", "$key:pexpire-xx-bench", "60000"]
        ]
    ),
    case_with_setup!(
        Key,
        "EXPIRETIME",
        "EXPIRETIME future",
        ["EXPIRETIME", "$key:expiretime-bench"],
        [
            ["SET", "$key:expiretime-bench", "v"],
            ["EXPIRE", "$key:expiretime-bench", "60"]
        ]
    ),
    case_with_setup!(
        Key,
        "PEXPIRETIME",
        "PEXPIRETIME future",
        ["PEXPIRETIME", "$key:pexpiretime-bench"],
        [
            ["SET", "$key:pexpiretime-bench", "v"],
            ["PEXPIRE", "$key:pexpiretime-bench", "60000"]
        ]
    ),
    case!(Key, "TYPE", "TYPE string", ["TYPE", "s"]),
    case_with_setup!(
        Key,
        "OBJECT",
        "OBJECT ENCODING string",
        ["OBJECT", "ENCODING", "object-bench"],
        [["SET", "object-bench", "v"]]
    ),
    case_with_setup!(
        Key,
        "DUMP",
        "DUMP string",
        ["DUMP", "$key:dump-bench"],
        [["SET", "$key:dump-bench", "v"]]
    ),
    case!(
        Key,
        "RESTORE",
        "RESTORE string replace",
        [
            "RESTORE",
            "$key:restore-bench",
            "0",
            "$dump:string:v",
            "REPLACE"
        ]
    ),
    case!(
        Key,
        "RESTORE-ASKING",
        "RESTORE-ASKING string replace",
        [
            "RESTORE-ASKING",
            "$key:restore-asking-bench",
            "0",
            "$dump:string:v",
            "REPLACE"
        ]
    ),
    case!(
        Key,
        "EXISTS",
        "EXISTS mixed",
        ["EXISTS", "s", "$key:missing"]
    ),
    case_with_setup!(
        Key,
        "TOUCH",
        "TOUCH mixed",
        ["TOUCH", "touch-a", "touch-b", "$key:missing"],
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
    case!(Key, "DEL", "DEL missing", ["DEL", "$key:del-bench-missing"]),
    case!(
        Key,
        "UNLINK",
        "UNLINK missing",
        ["UNLINK", "$key:missing-unlink"]
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
    case!(String, "SUBSTR", "SUBSTR", ["SUBSTR", "s", "1", "-1"]),
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
    case_with_setup!(
        String,
        "BITFIELD",
        "BITFIELD GET SET",
        [
            "BITFIELD",
            "$key:bitfield-bench",
            "GET",
            "u8",
            "0",
            "SET",
            "u8",
            "8",
            "42"
        ],
        [["SET", "$key:bitfield-bench", "AB"]]
    ),
    case!(String, "GETSET", "GETSET", ["GETSET", "s", "old"]),
    case_with_setup!(
        String,
        "GETEX",
        "GETEX PX",
        ["GETEX", "$key:getex-bench", "PX", "60000"],
        [["SET", "$key:getex-bench", "v"]]
    ),
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
        ["MGET", "mb", "$key:missing", "ma"]
    ),
    case!(
        String,
        "MSETNX",
        "MSETNX existing",
        ["MSETNX", "ma", "x", "mc", "3"]
    ),
    case!(
        HyperLogLog,
        "PFADD",
        "PFADD",
        ["PFADD", "$key:hll", "a", "b"]
    ),
    case_with_setup!(
        HyperLogLog,
        "PFCOUNT",
        "PFCOUNT",
        ["PFCOUNT", "$key:hll"],
        [["PFADD", "$key:hll", "a", "b", "c"]]
    ),
    case_with_setup!(
        HyperLogLog,
        "PFMERGE",
        "PFMERGE",
        ["PFMERGE", "$key:hll-merged", "$key:hll"],
        [["PFADD", "$key:hll", "a", "b", "c"]]
    ),
    case_with_setup!(
        HyperLogLog,
        "PFDEBUG",
        "PFDEBUG ENCODING",
        ["PFDEBUG", "ENCODING", "$key:hll"],
        [["PFADD", "$key:hll", "a", "b", "c"]]
    ),
    case!(HyperLogLog, "PFSELFTEST", "PFSELFTEST", ["PFSELFTEST"]),
    case!(
        Geo,
        "GEOADD",
        "GEOADD",
        ["GEOADD", "$key:geo", "-73.9857", "40.7484", "empire"]
    ),
    case_with_setup!(
        Geo,
        "GEODIST",
        "GEODIST",
        ["GEODIST", "$key:geo", "empire", "flatiron", "km"],
        [[
            "GEOADD", "$key:geo", "-73.9857", "40.7484", "empire", "-73.9897", "40.7411",
            "flatiron"
        ]]
    ),
    case_with_setup!(
        Geo,
        "GEOHASH",
        "GEOHASH",
        ["GEOHASH", "$key:geo", "empire"],
        [["GEOADD", "$key:geo", "-73.9857", "40.7484", "empire"]]
    ),
    case_with_setup!(
        Geo,
        "GEOPOS",
        "GEOPOS",
        ["GEOPOS", "$key:geo", "empire"],
        [["GEOADD", "$key:geo", "-73.9857", "40.7484", "empire"]]
    ),
    case_with_setup!(
        Geo,
        "GEORADIUS",
        "GEORADIUS",
        ["GEORADIUS", "$key:geo", "-73.9857", "40.7484", "2", "km"],
        [[
            "GEOADD", "$key:geo", "-73.9857", "40.7484", "empire", "-73.9897", "40.7411",
            "flatiron"
        ]]
    ),
    case_with_setup!(
        Geo,
        "GEORADIUSBYMEMBER",
        "GEORADIUSBYMEMBER",
        ["GEORADIUSBYMEMBER", "$key:geo", "empire", "2", "km"],
        [[
            "GEOADD", "$key:geo", "-73.9857", "40.7484", "empire", "-73.9897", "40.7411",
            "flatiron"
        ]]
    ),
    case_with_setup!(
        Geo,
        "GEORADIUS_RO",
        "GEORADIUS_RO",
        ["GEORADIUS_RO", "$key:geo", "-73.9857", "40.7484", "2", "km"],
        [["GEOADD", "$key:geo", "-73.9857", "40.7484", "empire"]]
    ),
    case_with_setup!(
        Geo,
        "GEORADIUSBYMEMBER_RO",
        "GEORADIUSBYMEMBER_RO",
        ["GEORADIUSBYMEMBER_RO", "$key:geo", "empire", "2", "km"],
        [["GEOADD", "$key:geo", "-73.9857", "40.7484", "empire"]]
    ),
    case!(
        Stream,
        "XADD",
        "XADD",
        [
            "XADD",
            "$key:stream-xadd",
            "MAXLEN",
            "~",
            "1000",
            "*",
            "field",
            "value"
        ]
    ),
    case_with_setup!(
        Stream,
        "XLEN",
        "XLEN",
        ["XLEN", "$key:stream-xlen"],
        [
            ["DEL", "$key:stream-xlen"],
            ["XADD", "$key:stream-xlen", "1-0", "field", "value"]
        ]
    ),
    case_with_setup!(
        Stream,
        "XRANGE",
        "XRANGE",
        ["XRANGE", "$key:stream-xrange", "-", "+"],
        [
            ["DEL", "$key:stream-xrange"],
            ["XADD", "$key:stream-xrange", "1-0", "field", "value"]
        ]
    ),
    case_with_setup!(
        Stream,
        "XREVRANGE",
        "XREVRANGE",
        ["XREVRANGE", "$key:stream-xrevrange", "+", "-"],
        [
            ["DEL", "$key:stream-xrevrange"],
            ["XADD", "$key:stream-xrevrange", "1-0", "field", "value"]
        ]
    ),
    case_with_setup!(
        Stream,
        "XDEL",
        "XDEL",
        ["XDEL", "$key:stream-xdel", "1-0"],
        [
            ["DEL", "$key:stream-xdel"],
            ["XADD", "$key:stream-xdel", "1-0", "field", "value"]
        ]
    ),
    case_with_setup!(
        Stream,
        "XTRIM",
        "XTRIM",
        ["XTRIM", "$key:stream-xtrim", "MAXLEN", "1"],
        [
            ["DEL", "$key:stream-xtrim"],
            ["XADD", "$key:stream-xtrim", "1-0", "field", "value"],
            ["XADD", "$key:stream-xtrim", "2-0", "field", "value"]
        ]
    ),
    case_with_setup!(
        Stream,
        "XSETID",
        "XSETID",
        ["XSETID", "$key:stream-xsetid", "5-0"],
        [
            ["DEL", "$key:stream-xsetid"],
            ["XADD", "$key:stream-xsetid", "1-0", "field", "value"]
        ]
    ),
    case_with_setup!(
        Stream,
        "XREAD",
        "XREAD",
        ["XREAD", "COUNT", "1", "STREAMS", "$key:stream-xread", "0-0"],
        [
            ["DEL", "$key:stream-xread"],
            ["XADD", "$key:stream-xread", "1-0", "field", "value"]
        ]
    ),
    case_with_setup!(
        Stream,
        "XGROUP",
        "XGROUP CREATECONSUMER",
        ["XGROUP", "CREATECONSUMER", "$key:stream-xgroup", "g", "c"],
        [
            ["DEL", "$key:stream-xgroup"],
            ["XADD", "$key:stream-xgroup", "1-0", "field", "value"],
            ["XGROUP", "CREATE", "$key:stream-xgroup", "g", "0-0"]
        ]
    ),
    case_with_setup!(
        Stream,
        "XREADGROUP",
        "XREADGROUP",
        [
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "1",
            "STREAMS",
            "$key:stream-xreadgroup",
            ">"
        ],
        [
            ["DEL", "$key:stream-xreadgroup"],
            ["XADD", "$key:stream-xreadgroup", "1-0", "field", "value"],
            ["XGROUP", "CREATE", "$key:stream-xreadgroup", "g", "0-0"]
        ]
    ),
    case_with_setup!(
        Stream,
        "XPENDING",
        "XPENDING summary",
        ["XPENDING", "$key:stream-xpending", "g"],
        [
            ["DEL", "$key:stream-xpending"],
            ["XADD", "$key:stream-xpending", "1-0", "field", "value"],
            ["XGROUP", "CREATE", "$key:stream-xpending", "g", "0-0"],
            [
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "COUNT",
                "1",
                "STREAMS",
                "$key:stream-xpending",
                ">"
            ]
        ]
    ),
    case_with_setup!(
        Stream,
        "XCLAIM",
        "XCLAIM empty",
        ["XCLAIM", "$key:stream-xclaim", "g", "c2", "0", "1-0"],
        [
            ["DEL", "$key:stream-xclaim"],
            ["XADD", "$key:stream-xclaim", "1-0", "field", "value"],
            ["XGROUP", "CREATE", "$key:stream-xclaim", "g", "0-0"],
            [
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "COUNT",
                "1",
                "STREAMS",
                "$key:stream-xclaim",
                ">"
            ]
        ]
    ),
    case_with_setup!(
        Stream,
        "XACK",
        "XACK empty",
        ["XACK", "$key:stream-xack", "g", "1-0"],
        [
            ["DEL", "$key:stream-xack"],
            ["XADD", "$key:stream-xack", "1-0", "field", "value"],
            ["XGROUP", "CREATE", "$key:stream-xack", "g", "0-0"],
            [
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "COUNT",
                "1",
                "STREAMS",
                "$key:stream-xack",
                ">"
            ]
        ]
    ),
    case_with_setup!(
        Stream,
        "XINFO",
        "XINFO STREAM",
        ["XINFO", "STREAM", "$key:stream-xinfo"],
        [
            ["DEL", "$key:stream-xinfo"],
            ["XADD", "$key:stream-xinfo", "1-0", "field", "value"]
        ]
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
        "BRPOPLPUSH",
        "BRPOPLPUSH ready",
        ["BRPOPLPUSH", "brpl", "brpl-dst", "0.001"],
        [["RPUSH", "brpl", "a", "b", "c"]]
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
    case_with_setup!(
        List,
        "LMPOP",
        "LMPOP left count",
        ["LMPOP", "1", "lmpop-bench", "LEFT", "COUNT", "2"],
        [["RPUSH", "lmpop-bench", "a", "b", "c"]]
    ),
    case_with_setup!(
        List,
        "BLMPOP",
        "BLMPOP right count",
        [
            "BLMPOP",
            "0.001",
            "1",
            "blmpop-bench",
            "RIGHT",
            "COUNT",
            "2"
        ],
        [["RPUSH", "blmpop-bench", "a", "b", "c"]]
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
    case_with_setup!(
        ZSet,
        "ZREM",
        "ZREM missing",
        ["ZREM", "$key:zrem-bench", "missing"],
        [["ZADD", "$key:zrem-bench", "1", "a", "2", "b"]]
    ),
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
        "ZMSCORE",
        "ZMSCORE z2",
        ["ZMSCORE", "z2", "a", "missing", "b"]
    ),
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
        "ZUNION",
        "ZUNION WITHSCORES",
        ["ZUNION", "2", "zu1", "zu2", "WITHSCORES"]
    ),
    case!(
        ZSet,
        "ZINTER",
        "ZINTER WITHSCORES",
        ["ZINTER", "2", "zu1", "zu2", "WITHSCORES"]
    ),
    case!(ZSet, "ZDIFF", "ZDIFF", ["ZDIFF", "2", "zu1", "zu2"]),
    case!(
        ZSet,
        "ZINTERCARD",
        "ZINTERCARD",
        ["ZINTERCARD", "2", "zu1", "zu2", "LIMIT", "10"]
    ),
    case!(
        ZSet,
        "ZRANDMEMBER",
        "ZRANDMEMBER WITHSCORES",
        ["ZRANDMEMBER", "zu1", "2", "WITHSCORES"]
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
    case_with_setup!(
        ZSet,
        "ZMPOP",
        "ZMPOP min count",
        ["ZMPOP", "1", "zmpop-bench", "MIN", "COUNT", "2"],
        [["ZADD", "zmpop-bench", "1", "a", "2", "b", "3", "c"]]
    ),
    case_with_setup!(
        ZSet,
        "BZMPOP",
        "BZMPOP max count",
        ["BZMPOP", "0.001", "1", "bzmpop-bench", "MAX", "COUNT", "2"],
        [["ZADD", "bzmpop-bench", "1", "a", "2", "b", "3", "c"]]
    ),
];

pub const REDIS_COMMAND_DESTRUCTIVE_CASES: &[RedisCommandCase] = &[
    destructive_case_script!(
        Server,
        "FLUSHDB",
        "FLUSHDB one key",
        ["FLUSHDB"],
        [["SET", "$key:flushdb-bench", "v"], ["FLUSHDB"]]
    ),
    destructive_case_script!(
        Server,
        "FLUSHALL",
        "FLUSHALL one key",
        ["FLUSHALL"],
        [["SET", "$key:flushall-bench", "v"], ["FLUSHALL", "SYNC"]]
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
    "ASKING",
    "AUTH",
    "BGREWRITEAOF",
    "BGSAVE",
    "BITCOUNT",
    "BITFIELD",
    "BITOP",
    "BITPOS",
    "BLMOVE",
    "BLMPOP",
    "BLPOP",
    "BRPOP",
    "BRPOPLPUSH",
    "BZMPOP",
    "BZPOPMAX",
    "BZPOPMIN",
    "CLIENT",
    "CLUSTER",
    "COMMAND",
    "CONFIG",
    "COPY",
    "DBSIZE",
    "DEBUG",
    "DECR",
    "DECRBY",
    "DEL",
    "DISCARD",
    "DUMP",
    "ECHO",
    "EVAL",
    "EVALSHA",
    "EXEC",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "FLUSHALL",
    "FLUSHDB",
    "GEOADD",
    "GEODIST",
    "GEOHASH",
    "GEOPOS",
    "GEORADIUS",
    "GEORADIUSBYMEMBER",
    "GEORADIUSBYMEMBER_RO",
    "GEORADIUS_RO",
    "GET",
    "GETBIT",
    "GETDEL",
    "GETEX",
    "GETRANGE",
    "GETSET",
    "HELLO",
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
    "HOST:",
    "INFO",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "KEYS",
    "LASTSAVE",
    "LATENCY",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LOLWUT",
    "LMOVE",
    "LMPOP",
    "LPOP",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "MGET",
    "MEMORY",
    "MIGRATE",
    "MODULE",
    "MONITOR",
    "MOVE",
    "MSET",
    "MSETNX",
    "MULTI",
    "OBJECT",
    "PERSIST",
    "PING",
    "POST",
    "PEXPIRE",
    "PEXPIREAT",
    "PEXPIRETIME",
    "PFADD",
    "PFCOUNT",
    "PFDEBUG",
    "PFMERGE",
    "PFSELFTEST",
    "PSETEX",
    "PSUBSCRIBE",
    "PSYNC",
    "PTTL",
    "PUBLISH",
    "PUBSUB",
    "PUNSUBSCRIBE",
    "RANDOMKEY",
    "READONLY",
    "READWRITE",
    "RENAME",
    "RENAMENX",
    "REPLCONF",
    "REPLICAOF",
    "RESTORE",
    "RESTORE-ASKING",
    "ROLE",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SAVE",
    "SCAN",
    "SCARD",
    "SCRIPT",
    "SDIFF",
    "SDIFFSTORE",
    "SELECT",
    "SET",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "SHUTDOWN",
    "SINTER",
    "SINTERSTORE",
    "SISMEMBER",
    "SLAVEOF",
    "SLOWLOG",
    "SMEMBERS",
    "SMISMEMBER",
    "SMOVE",
    "SORT",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "SSCAN",
    "STRLEN",
    "SUBSCRIBE",
    "SUBSTR",
    "SUNION",
    "SUNIONSTORE",
    "SWAPDB",
    "SYNC",
    "TIME",
    "TTL",
    "TOUCH",
    "TYPE",
    "UNLINK",
    "UNSUBSCRIBE",
    "UNWATCH",
    "WAIT",
    "WATCH",
    "XACK",
    "XADD",
    "XCLAIM",
    "XDEL",
    "XGROUP",
    "XINFO",
    "XLEN",
    "XPENDING",
    "XRANGE",
    "XREAD",
    "XREADGROUP",
    "XREVRANGE",
    "XSETID",
    "XTRIM",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZDIFF",
    "ZDIFFSTORE",
    "ZINCRBY",
    "ZINTER",
    "ZINTERCARD",
    "ZINTERSTORE",
    "ZLEXCOUNT",
    "ZMPOP",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANDMEMBER",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZRANGESTORE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYLEX",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
    "ZUNION",
    "ZUNIONSTORE",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisCompatibilityExclusion {
    pub command: &'static str,
    pub family: &'static str,
    pub reason: &'static str,
}

/// Command names from Redis 5.0.14's `redisCommandTable`.
///
/// Source: https://github.com/redis/redis/blob/5.0.14/src/server.c
pub const REDIS_5_0_14_COMMANDS: &[&str] = &[
    "APPEND",
    "ASKING",
    "AUTH",
    "BGREWRITEAOF",
    "BGSAVE",
    "BITCOUNT",
    "BITFIELD",
    "BITOP",
    "BITPOS",
    "BLPOP",
    "BRPOP",
    "BRPOPLPUSH",
    "BZPOPMAX",
    "BZPOPMIN",
    "CLIENT",
    "CLUSTER",
    "COMMAND",
    "CONFIG",
    "DBSIZE",
    "DEBUG",
    "DECR",
    "DECRBY",
    "DEL",
    "DISCARD",
    "DUMP",
    "ECHO",
    "EVAL",
    "EVALSHA",
    "EXEC",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "FLUSHALL",
    "FLUSHDB",
    "GEOADD",
    "GEODIST",
    "GEOHASH",
    "GEOPOS",
    "GEORADIUS",
    "GEORADIUSBYMEMBER",
    "GEORADIUSBYMEMBER_RO",
    "GEORADIUS_RO",
    "GET",
    "GETBIT",
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
    "HMSET",
    "HSCAN",
    "HSET",
    "HSETNX",
    "HSTRLEN",
    "HVALS",
    "HOST:",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "INFO",
    "KEYS",
    "LASTSAVE",
    "LATENCY",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LOLWUT",
    "LPOP",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "MEMORY",
    "MGET",
    "MIGRATE",
    "MODULE",
    "MONITOR",
    "MOVE",
    "MSET",
    "MSETNX",
    "MULTI",
    "OBJECT",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PFADD",
    "PFCOUNT",
    "PFDEBUG",
    "PFMERGE",
    "PFSELFTEST",
    "PING",
    "POST",
    "PSETEX",
    "PSUBSCRIBE",
    "PSYNC",
    "PTTL",
    "PUBLISH",
    "PUBSUB",
    "PUNSUBSCRIBE",
    "RANDOMKEY",
    "READONLY",
    "READWRITE",
    "RENAME",
    "RENAMENX",
    "REPLCONF",
    "REPLICAOF",
    "RESTORE",
    "RESTORE-ASKING",
    "ROLE",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SAVE",
    "SCAN",
    "SCARD",
    "SCRIPT",
    "SDIFF",
    "SDIFFSTORE",
    "SELECT",
    "SET",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "SHUTDOWN",
    "SINTER",
    "SINTERSTORE",
    "SISMEMBER",
    "SLAVEOF",
    "SLOWLOG",
    "SMEMBERS",
    "SMOVE",
    "SORT",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "SSCAN",
    "STRLEN",
    "SUBSCRIBE",
    "SUBSTR",
    "SUNION",
    "SUNIONSTORE",
    "SWAPDB",
    "SYNC",
    "TIME",
    "TOUCH",
    "TTL",
    "TYPE",
    "UNLINK",
    "UNSUBSCRIBE",
    "UNWATCH",
    "WAIT",
    "WATCH",
    "XACK",
    "XADD",
    "XCLAIM",
    "XDEL",
    "XGROUP",
    "XINFO",
    "XLEN",
    "XPENDING",
    "XRANGE",
    "XREAD",
    "XREADGROUP",
    "XREVRANGE",
    "XSETID",
    "XTRIM",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZINCRBY",
    "ZINTERSTORE",
    "ZLEXCOUNT",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZRANK",
    "ZREM",
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

pub const REDIS_5_0_14_EXCLUSIONS: &[RedisCompatibilityExclusion] = &[];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        BENCHMARKED_COMMANDS, REDIS_5_0_14_COMMANDS, REDIS_5_0_14_EXCLUSIONS, REDIS_COMMAND_CASES,
        REDIS_COMMAND_DESTRUCTIVE_CASES, REDIS_COMMAND_LARGE_CASES, RedisCommandFamily,
    };

    #[test]
    fn benchmark_cases_cover_declared_commands() {
        let actual = REDIS_COMMAND_CASES
            .iter()
            .chain(REDIS_COMMAND_LARGE_CASES.iter())
            .chain(REDIS_COMMAND_DESTRUCTIVE_CASES.iter())
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
    fn benchmarked_commands_have_fcnp_transport_path() {
        assert!(
            !BENCHMARKED_COMMANDS.is_empty(),
            "benchmark manifest must not be empty"
        );
    }

    #[test]
    fn redis_5_command_surface_has_no_exclusions() {
        let redis5 = REDIS_5_0_14_COMMANDS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let supported = BENCHMARKED_COMMANDS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let excluded = REDIS_5_0_14_EXCLUSIONS
            .iter()
            .map(|entry| entry.command)
            .collect::<BTreeSet<_>>();

        let missing = redis5
            .difference(&supported)
            .copied()
            .filter(|command| !excluded.contains(command))
            .collect::<Vec<_>>();
        let stale_exclusions = excluded.difference(&redis5).copied().collect::<Vec<_>>();

        assert!(
            stale_exclusions.is_empty(),
            "Redis 5.0.14 exclusions that are not in the source command table: {stale_exclusions:?}"
        );
        assert_eq!(redis5.len(), 200);
        assert_eq!(supported.intersection(&redis5).count(), 200);
        assert_eq!(excluded.len(), 0);
        assert_eq!(missing.len(), 0);
    }

    #[test]
    fn redis_5_exclusions_have_unique_names_and_reasons() {
        let mut names = BTreeSet::new();
        for entry in REDIS_5_0_14_EXCLUSIONS {
            assert!(
                names.insert(entry.command),
                "duplicate Redis 5.0.14 exclusion: {}",
                entry.command
            );
            assert!(!entry.family.is_empty());
            assert!(!entry.reason.is_empty());
        }
    }

    #[test]
    fn benchmark_case_names_are_unique() {
        let mut names = BTreeSet::new();
        for case in REDIS_COMMAND_CASES
            .iter()
            .chain(REDIS_COMMAND_LARGE_CASES.iter())
            .chain(REDIS_COMMAND_DESTRUCTIVE_CASES.iter())
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
    fn expected_error_cases_are_explicit() {
        let actual = REDIS_COMMAND_CASES
            .iter()
            .chain(REDIS_COMMAND_LARGE_CASES.iter())
            .chain(REDIS_COMMAND_DESTRUCTIVE_CASES.iter())
            .filter(|case| case.expect_error)
            .map(|case| case.command_name)
            .collect::<BTreeSet<_>>();
        let expected = [
            "CLUSTER", "HOST:", "MIGRATE", "MONITOR", "MOVE", "POST", "PSYNC", "SHUTDOWN", "SYNC",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn pubsub_benchmark_channels_are_worker_scoped() {
        for case in REDIS_COMMAND_CASES
            .iter()
            .filter(|case| case.family == RedisCommandFamily::PubSub)
        {
            for part in case.parts.iter().skip(1) {
                if part.starts_with("bench-") {
                    panic!(
                        "{} uses unscoped pubsub channel/pattern `{part}`",
                        case.case_name
                    );
                }
            }
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

    #[test]
    fn destructive_benchmark_cases_are_isolated_from_default_profiles() {
        assert!(
            !REDIS_COMMAND_DESTRUCTIVE_CASES.is_empty(),
            "destructive command benchmark profile is empty"
        );
        for case in REDIS_COMMAND_DESTRUCTIVE_CASES {
            assert!(case.matches_filter("destructive"));
            assert!(
                case.is_keyspace_wide(),
                "destructive case should be keyspace-wide: {}",
                case.case_name
            );
        }
    }
}
