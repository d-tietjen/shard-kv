mod common;

use std::env;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use fast_cache::protocol::Frame;
use tempfile::TempDir;
use tokio::net::TcpStream;

use common::{free_port, send_command, test_config};

#[derive(Debug, Clone, Copy)]
enum CompareMode {
    Exact,
    PositiveInteger,
    UnorderedArray,
    UnorderedPairArray,
    ScanUnorderedArray,
    ScanUnorderedPairArray,
}

#[derive(Debug)]
struct CompatCase {
    name: &'static str,
    parts: Vec<Vec<u8>>,
    compare: CompareMode,
}

struct ReferenceServer {
    child: Child,
    _data_dir: TempDir,
    addr: String,
    binary: PathBuf,
}

impl Drop for ReferenceServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn resp_supported_commands_match_redis_reference() {
    let Some(reference) = start_reference_server().await else {
        eprintln!(
            "skipping Redis compatibility differential test: set FAST_CACHE_COMPAT_SERVER_BIN or install valkey-server/redis-server"
        );
        return;
    };

    let local = tokio::task::LocalSet::new();
    local.run_until(run_differential(reference)).await;
}

async fn run_differential(reference: ReferenceServer) {
    let temp_dir = tempfile::TempDir::new().expect("fast-cache temp dir");
    let mut config = test_config(temp_dir.path().join("fast-cache-data"), false);
    config.bind_addr = format!("127.0.0.1:{}", free_port());
    config.shard_count = 4;
    config.persistence.enabled = false;

    let server = fast_cache::server::FastCacheServer::direct(config.clone());
    let join = tokio::task::spawn_local(async move { server.run().await });
    wait_for_tcp(&config.bind_addr).await;

    let mut fast_cache = TcpStream::connect(&config.bind_addr)
        .await
        .expect("connect fast-cache");
    let mut redis = TcpStream::connect(&reference.addr)
        .await
        .expect("connect reference Redis-compatible server");

    for case in compat_cases() {
        let parts = case.parts.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let fast_reply = send_command(&mut fast_cache, &parts).await;
        let redis_reply = send_command(&mut redis, &parts).await;
        assert_compatible(&case, fast_reply, redis_reply);
    }

    join.abort();
    let _ = join.await;
    drop(reference);
}

async fn start_reference_server() -> Option<ReferenceServer> {
    let explicit = env::var_os("FAST_CACHE_COMPAT_SERVER_BIN").map(PathBuf::from);
    let candidates = explicit
        .clone()
        .into_iter()
        .chain(find_on_path("valkey-server"))
        .chain(find_on_path("redis-server"))
        .chain(existing_path("/opt/homebrew/bin/valkey-server"))
        .chain(existing_path("/opt/homebrew/bin/redis-server"))
        .chain(existing_path("/usr/local/bin/valkey-server"))
        .chain(existing_path("/usr/local/bin/redis-server"))
        .collect::<Vec<_>>();

    for binary in candidates {
        match spawn_reference_server(&binary).await {
            Ok(server) => return Some(server),
            Err(error) if explicit.is_some() => {
                panic!(
                    "failed to start FAST_CACHE_COMPAT_SERVER_BIN={}: {error}",
                    binary.display()
                );
            }
            Err(error) => {
                eprintln!("skipping reference candidate {}: {error}", binary.display());
            }
        }
    }
    None
}

async fn spawn_reference_server(binary: &Path) -> Result<ReferenceServer, String> {
    let data_dir = tempfile::TempDir::new().map_err(|error| error.to_string())?;
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let child = Command::new(binary)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--protected-mode")
        .arg("no")
        .arg("--save")
        .arg("")
        .arg("--appendonly")
        .arg("no")
        .arg("--dir")
        .arg(data_dir.path())
        .arg("--loglevel")
        .arg("warning")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| error.to_string())?;

    let mut server = ReferenceServer {
        child,
        _data_dir: data_dir,
        addr,
        binary: binary.to_path_buf(),
    };
    if wait_for_process_or_tcp(&mut server.child, &server.addr).await {
        Ok(server)
    } else {
        Err(format!(
            "{} did not start listening",
            server.binary.display()
        ))
    }
}

async fn wait_for_process_or_tcp(child: &mut Child, addr: &str) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        if child.try_wait().ok().flatten().is_some() {
            return false;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_tcp(addr: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("{addr} did not start listening");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn find_on_path(binary: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|dir| dir.join(binary))
            .find(|path| path.is_file())
    })
}

fn existing_path(path: &str) -> Option<PathBuf> {
    let path = PathBuf::from(path);
    path.is_file().then_some(path)
}

fn b(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|part| part.to_vec()).collect()
}

fn case(name: &'static str, parts: &[&[u8]]) -> CompatCase {
    CompatCase {
        name,
        parts: b(parts),
        compare: CompareMode::Exact,
    }
}

fn case_with(name: &'static str, parts: &[&[u8]], compare: CompareMode) -> CompatCase {
    CompatCase {
        name,
        parts: b(parts),
        compare,
    }
}

fn compat_cases() -> Vec<CompatCase> {
    vec![
        case("PING", &[b"PING"]),
        case("ECHO", &[b"ECHO", b"hello"]),
        case("SELECT 0", &[b"SELECT", b"0"]),
        case("WATCH simple", &[b"WATCH", b"watched-diff"]),
        case("UNWATCH simple", &[b"UNWATCH"]),
        case("DBSIZE empty", &[b"DBSIZE"]),
        case("SET string", &[b"SET", b"s", b"v"]),
        case("GET string", &[b"GET", b"s"]),
        case("SET NX miss", &[b"SET", b"s", b"v2", b"NX"]),
        case("SET XX hit", &[b"SET", b"s", b"v2", b"XX"]),
        case("GET after SET XX", &[b"GET", b"s"]),
        case("SETNX miss", &[b"SETNX", b"setnx-diff", b"v"]),
        case("SETNX hit", &[b"SETNX", b"setnx-diff", b"v2"]),
        case("GET after SETNX", &[b"GET", b"setnx-diff"]),
        case("SET EX", &[b"SET", b"exp", b"v", b"EX", b"60"]),
        case_with(
            "TTL positive",
            &[b"TTL", b"exp"],
            CompareMode::PositiveInteger,
        ),
        case_with(
            "PTTL positive",
            &[b"PTTL", b"exp"],
            CompareMode::PositiveInteger,
        ),
        case("PERSIST", &[b"PERSIST", b"exp"]),
        case("TTL persisted", &[b"TTL", b"exp"]),
        case("SET expat", &[b"SET", b"expat", b"v"]),
        case("EXPIREAT future", &[b"EXPIREAT", b"expat", b"9999999999"]),
        case_with(
            "TTL after EXPIREAT",
            &[b"TTL", b"expat"],
            CompareMode::PositiveInteger,
        ),
        case("PEXPIREAT past", &[b"PEXPIREAT", b"expat", b"1"]),
        case("EXISTS after PEXPIREAT", &[b"EXISTS", b"expat"]),
        case("TYPE string", &[b"TYPE", b"s"]),
        case("EXISTS mixed", &[b"EXISTS", b"s", b"missing"]),
        case("TOUCH mixed", &[b"TOUCH", b"setnx-diff", b"s", b"missing"]),
        case_with("KEYS all", &[b"KEYS", b"*"], CompareMode::UnorderedArray),
        case_with(
            "SCAN all",
            &[b"SCAN", b"0", b"MATCH", b"*", b"COUNT", b"1000"],
            CompareMode::ScanUnorderedArray,
        ),
        case_with(
            "SCAN type string",
            &[b"SCAN", b"0", b"TYPE", b"string", b"COUNT", b"1000"],
            CompareMode::ScanUnorderedArray,
        ),
        case("SET rename-src", &[b"SET", b"rename-src", b"v"]),
        case("RENAME", &[b"RENAME", b"rename-src", b"rename-dst"]),
        case("GET renamed", &[b"GET", b"rename-dst"]),
        case("SET renamenx-src", &[b"SET", b"renamenx-src", b"v"]),
        case("SET renamenx-dst", &[b"SET", b"renamenx-dst", b"existing"]),
        case(
            "RENAMENX existing dest",
            &[b"RENAMENX", b"renamenx-src", b"renamenx-dst"],
        ),
        case("GET renamenx source", &[b"GET", b"renamenx-src"]),
        case("UNLINK missing", &[b"UNLINK", b"missing-unlink"]),
        case("RENAME missing", &[b"RENAME", b"missing-rename", b"dest"]),
        case("COPY string", &[b"COPY", b"setnx-diff", b"copy-diff"]),
        case("GET copy", &[b"GET", b"copy-diff"]),
        case(
            "COPY existing without replace",
            &[b"COPY", b"setnx-diff", b"copy-diff"],
        ),
        case(
            "COPY existing with replace",
            &[b"COPY", b"setnx-diff", b"copy-diff", b"REPLACE"],
        ),
        case("APPEND", &[b"APPEND", b"s", b"+"]),
        case("STRLEN", &[b"STRLEN", b"s"]),
        case("GETRANGE", &[b"GETRANGE", b"s", b"1", b"-1"]),
        case("SETRANGE", &[b"SETRANGE", b"s", b"1", b"XX"]),
        case("SET bitstr", &[b"SET", b"bitstr", b"\x81"]),
        case("GETBIT", &[b"GETBIT", b"bitstr", b"0"]),
        case("SETBIT", &[b"SETBIT", b"bitstr", b"1", b"1"]),
        case("BITCOUNT", &[b"BITCOUNT", b"bitstr"]),
        case(
            "BITCOUNT BIT range",
            &[b"BITCOUNT", b"bitstr", b"0", b"7", b"BIT"],
        ),
        case("BITPOS one", &[b"BITPOS", b"bitstr", b"1"]),
        case("SET bit-a", &[b"SET", b"bit-a", b"\x0f"]),
        case("SET bit-b", &[b"SET", b"bit-b", b"\xf0"]),
        case(
            "BITOP OR",
            &[b"BITOP", b"OR", b"bit-out", b"bit-a", b"bit-b"],
        ),
        case("GET bit-out", &[b"GET", b"bit-out"]),
        case("BITOP NOT", &[b"BITOP", b"NOT", b"bit-not", b"bit-a"]),
        case("GET bit-not", &[b"GET", b"bit-not"]),
        case(
            "BITFIELD wrap",
            &[
                b"BITFIELD",
                b"bitfield",
                b"SET",
                b"u8",
                b"0",
                b"255",
                b"GET",
                b"u8",
                b"0",
                b"INCRBY",
                b"u8",
                b"0",
                b"1",
            ],
        ),
        case(
            "BITFIELD sat",
            &[
                b"BITFIELD",
                b"bitfield-sat",
                b"OVERFLOW",
                b"SAT",
                b"INCRBY",
                b"i4",
                b"0",
                b"8",
            ],
        ),
        case(
            "BITFIELD fail",
            &[
                b"BITFIELD",
                b"bitfield-fail",
                b"SET",
                b"u2",
                b"0",
                b"3",
                b"OVERFLOW",
                b"FAIL",
                b"INCRBY",
                b"u2",
                b"0",
                b"1",
                b"GET",
                b"u2",
                b"0",
            ],
        ),
        case("GETSET", &[b"GETSET", b"s", b"old"]),
        case("GETDEL", &[b"GETDEL", b"s"]),
        case("GET after GETDEL", &[b"GET", b"s"]),
        case("INCR", &[b"INCR", b"n"]),
        case("INCRBY", &[b"INCRBY", b"n", b"4"]),
        case("DECR", &[b"DECR", b"n"]),
        case("DECRBY", &[b"DECRBY", b"n", b"2"]),
        case("INCRBYFLOAT", &[b"INCRBYFLOAT", b"nf", b"1.5"]),
        case("MSET", &[b"MSET", b"ma", b"1", b"mb", b"2"]),
        case("MGET order", &[b"MGET", b"mb", b"missing", b"ma"]),
        case("MSETNX existing", &[b"MSETNX", b"ma", b"x", b"mc", b"3"]),
        case("MSETNX miss", &[b"MSETNX", b"mc", b"3", b"md", b"4"]),
        case("HSET", &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]),
        case("HMSET", &[b"HMSET", b"hm", b"f1", b"v1", b"f2", b"v2"]),
        case("HGET after HMSET", &[b"HGET", b"hm", b"f2"]),
        case("COPY hash", &[b"COPY", b"hm", b"hm-copy"]),
        case("HGET copied hash", &[b"HGET", b"hm-copy", b"f1"]),
        case("HGET", &[b"HGET", b"h", b"f1"]),
        case("HMGET", &[b"HMGET", b"h", b"f2", b"missing", b"f1"]),
        case("HLEN", &[b"HLEN", b"h"]),
        case("HEXISTS", &[b"HEXISTS", b"h", b"f2"]),
        case("HSETNX", &[b"HSETNX", b"h", b"f3", b"v3"]),
        case("HSTRLEN", &[b"HSTRLEN", b"h", b"f2"]),
        case("HSTRLEN missing", &[b"HSTRLEN", b"missing-h", b"f"]),
        case("SET hwrong", &[b"SET", b"hwrong", b"value"]),
        case("HSTRLEN wrongtype", &[b"HSTRLEN", b"hwrong", b"f"]),
        case("HSET num", &[b"HSET", b"h", b"num", b"1", b"float", b"1.5"]),
        case("HINCRBY", &[b"HINCRBY", b"h", b"num", b"5"]),
        case("HINCRBYFLOAT", &[b"HINCRBYFLOAT", b"h", b"float", b"0.5"]),
        case_with("HKEYS", &[b"HKEYS", b"h"], CompareMode::UnorderedArray),
        case_with("HVALS", &[b"HVALS", b"h"], CompareMode::UnorderedArray),
        case_with(
            "HGETALL",
            &[b"HGETALL", b"h"],
            CompareMode::UnorderedPairArray,
        ),
        case_with(
            "HSCAN",
            &[b"HSCAN", b"h", b"0", b"MATCH", b"*", b"COUNT", b"1000"],
            CompareMode::ScanUnorderedPairArray,
        ),
        case_with(
            "HRANDFIELD all WITHVALUES",
            &[b"HRANDFIELD", b"h", b"10", b"WITHVALUES"],
            CompareMode::UnorderedPairArray,
        ),
        case("HDEL", &[b"HDEL", b"h", b"f1", b"missing"]),
        case("HGET after HDEL", &[b"HGET", b"h", b"f1"]),
        case("LPUSH", &[b"LPUSH", b"l", b"b", b"a"]),
        case("RPUSH", &[b"RPUSH", b"l", b"c"]),
        case("LRANGE", &[b"LRANGE", b"l", b"0", b"-1"]),
        case("RPUSH lm", &[b"RPUSH", b"lm", b"a", b"b", b"c"]),
        case(
            "LMOVE ready",
            &[b"LMOVE", b"lm", b"lm-dst", b"RIGHT", b"LEFT"],
        ),
        case("LRANGE lm-dst", &[b"LRANGE", b"lm-dst", b"0", b"-1"]),
        case(
            "LMOVE missing",
            &[b"LMOVE", b"missing-lm", b"lm-dst", b"RIGHT", b"LEFT"],
        ),
        case("RPOPLPUSH self", &[b"RPOPLPUSH", b"l", b"l"]),
        case("RPOPLPUSH missing", &[b"RPOPLPUSH", b"missing-rpl", b"l"]),
        case("RPUSH rplsrc", &[b"RPUSH", b"rplsrc", b"a", b"b"]),
        case("SET rpldst", &[b"SET", b"rpldst", b"wrongtype"]),
        case(
            "RPOPLPUSH dest wrongtype",
            &[b"RPOPLPUSH", b"rplsrc", b"rpldst"],
        ),
        case(
            "LRANGE rplsrc unchanged",
            &[b"LRANGE", b"rplsrc", b"0", b"-1"],
        ),
        case("LLEN", &[b"LLEN", b"l"]),
        case("LINDEX", &[b"LINDEX", b"l", b"1"]),
        case("LSET", &[b"LSET", b"l", b"1", b"B"]),
        case("LREM", &[b"LREM", b"l", b"0", b"B"]),
        case("LINSERT", &[b"LINSERT", b"l", b"BEFORE", b"c", b"b"]),
        case("LTRIM", &[b"LTRIM", b"l", b"0", b"1"]),
        case("LRANGE after LTRIM", &[b"LRANGE", b"l", b"0", b"-1"]),
        case("LPOP", &[b"LPOP", b"l"]),
        case("RPOP", &[b"RPOP", b"l"]),
        case("LLEN after pops", &[b"LLEN", b"l"]),
        case("LPUSHX missing", &[b"LPUSHX", b"missing-list", b"x"]),
        case("RPUSHX missing", &[b"RPUSHX", b"missing-list", b"x"]),
        case("RPUSH bl", &[b"RPUSH", b"bl", b"a", b"b"]),
        case("BLPOP ready", &[b"BLPOP", b"bl", b"0"]),
        case("RPUSH br", &[b"RPUSH", b"br", b"a", b"b"]),
        case("BRPOP ready", &[b"BRPOP", b"br", b"0"]),
        case("RPUSH bm", &[b"RPUSH", b"bm", b"a", b"b"]),
        case(
            "BLMOVE ready",
            &[b"BLMOVE", b"bm", b"bmd", b"RIGHT", b"LEFT", b"0"],
        ),
        case("SADD set-a", &[b"SADD", b"set-a", b"a", b"b"]),
        case("SADD set-b", &[b"SADD", b"set-b", b"b", b"c"]),
        case("SISMEMBER", &[b"SISMEMBER", b"set-a", b"a"]),
        case("SMISMEMBER", &[b"SMISMEMBER", b"set-a", b"a", b"x"]),
        case("SCARD", &[b"SCARD", b"set-a"]),
        case_with(
            "SMEMBERS",
            &[b"SMEMBERS", b"set-a"],
            CompareMode::UnorderedArray,
        ),
        case_with(
            "SUNION",
            &[b"SUNION", b"set-a", b"set-b"],
            CompareMode::UnorderedArray,
        ),
        case_with(
            "SINTER",
            &[b"SINTER", b"set-a", b"set-b"],
            CompareMode::UnorderedArray,
        ),
        case_with(
            "SDIFF",
            &[b"SDIFF", b"set-a", b"set-b"],
            CompareMode::UnorderedArray,
        ),
        case(
            "SUNIONSTORE",
            &[b"SUNIONSTORE", b"set-u", b"set-a", b"set-b"],
        ),
        case(
            "SINTERSTORE",
            &[b"SINTERSTORE", b"set-i", b"set-a", b"set-b"],
        ),
        case("SDIFFSTORE", &[b"SDIFFSTORE", b"set-d", b"set-a", b"set-b"]),
        case("SMOVE", &[b"SMOVE", b"set-a", b"set-b", b"a"]),
        case("SREM", &[b"SREM", b"set-b", b"a"]),
        case_with(
            "SSCAN",
            &[b"SSCAN", b"set-a", b"0", b"MATCH", b"*", b"COUNT", b"1000"],
            CompareMode::ScanUnorderedArray,
        ),
        case("SADD set-r", &[b"SADD", b"set-r", b"a", b"b"]),
        case_with(
            "SRANDMEMBER all",
            &[b"SRANDMEMBER", b"set-r", b"2"],
            CompareMode::UnorderedArray,
        ),
        case_with(
            "SPOP all",
            &[b"SPOP", b"set-r", b"2"],
            CompareMode::UnorderedArray,
        ),
        case("ZADD", &[b"ZADD", b"z", b"1", b"a", b"2", b"b"]),
        case("ZSCORE", &[b"ZSCORE", b"z", b"a"]),
        case("ZCARD", &[b"ZCARD", b"z"]),
        case("ZRANGE", &[b"ZRANGE", b"z", b"0", b"-1"]),
        case("ZREVRANGE", &[b"ZREVRANGE", b"z", b"0", b"-1"]),
        case(
            "ZREVRANGE missing",
            &[b"ZREVRANGE", b"missing-z", b"0", b"-1"],
        ),
        case(
            "ZRANGE WITHSCORES",
            &[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"],
        ),
        case("ZINCRBY", &[b"ZINCRBY", b"z", b"2", b"a"]),
        case("ZCOUNT", &[b"ZCOUNT", b"z", b"0", b"10"]),
        case("ZRANK", &[b"ZRANK", b"z", b"a"]),
        case("ZREVRANK", &[b"ZREVRANK", b"z", b"a"]),
        case("ZPOPMIN", &[b"ZPOPMIN", b"z", b"1"]),
        case("ZPOPMAX", &[b"ZPOPMAX", b"z", b"1"]),
        case(
            "ZADD z2",
            &[b"ZADD", b"z2", b"1", b"a", b"2", b"b", b"3", b"c"],
        ),
        case("ZADD CH", &[b"ZADD", b"z2", b"CH", b"5", b"a", b"4", b"d"]),
        case("ZADD NX", &[b"ZADD", b"z2", b"NX", b"6", b"a", b"7", b"e"]),
        case("ZADD XX GT", &[b"ZADD", b"z2", b"XX", b"GT", b"8", b"a"]),
        case("ZSCORE after ZADD GT", &[b"ZSCORE", b"z2", b"a"]),
        case(
            "ZRANGE BYSCORE REV LIMIT",
            &[
                b"ZRANGE", b"z2", b"+inf", b"-inf", b"BYSCORE", b"REV", b"LIMIT", b"0", b"3",
            ],
        ),
        case(
            "ZRANGEBYSCORE exclusive",
            &[b"ZRANGEBYSCORE", b"z2", b"(2", b"8"],
        ),
        case(
            "ZREVRANGEBYSCORE limit",
            &[
                b"ZREVRANGEBYSCORE",
                b"z2",
                b"8",
                b"(2",
                b"LIMIT",
                b"0",
                b"2",
            ],
        ),
        case(
            "ZREVRANGEBYSCORE missing",
            &[b"ZREVRANGEBYSCORE", b"missing-z", b"+inf", b"-inf"],
        ),
        case_with(
            "ZSCAN",
            &[b"ZSCAN", b"z2", b"0", b"MATCH", b"*", b"COUNT", b"1000"],
            CompareMode::ScanUnorderedPairArray,
        ),
        case(
            "ZADD zlex",
            &[b"ZADD", b"zlex", b"0", b"a", b"0", b"b", b"0", b"c"],
        ),
        case("ZRANGEBYLEX", &[b"ZRANGEBYLEX", b"zlex", b"[a", b"[c"]),
        case(
            "ZREVRANGEBYLEX",
            &[b"ZREVRANGEBYLEX", b"zlex", b"[c", b"[a"],
        ),
        case("ZLEXCOUNT", &[b"ZLEXCOUNT", b"zlex", b"(a", b"[c"]),
        case("SET zwrong", &[b"SET", b"zwrong", b"value"]),
        case(
            "ZREVRANGE wrongtype",
            &[b"ZREVRANGE", b"zwrong", b"0", b"-1"],
        ),
        case(
            "ZREVRANGEBYSCORE wrongtype",
            &[b"ZREVRANGEBYSCORE", b"zwrong", b"+inf", b"-inf"],
        ),
        case(
            "ZADD zremrank",
            &[b"ZADD", b"zremrank", b"1", b"a", b"2", b"b", b"3", b"c"],
        ),
        case(
            "ZREMRANGEBYRANK missing",
            &[b"ZREMRANGEBYRANK", b"missing-z", b"0", b"1"],
        ),
        case(
            "ZREMRANGEBYRANK wrongtype",
            &[b"ZREMRANGEBYRANK", b"zwrong", b"0", b"1"],
        ),
        case(
            "ZREMRANGEBYRANK",
            &[b"ZREMRANGEBYRANK", b"zremrank", b"1", b"1"],
        ),
        case("ZRANGE zremrank", &[b"ZRANGE", b"zremrank", b"0", b"-1"]),
        case(
            "ZADD zremscore",
            &[b"ZADD", b"zremscore", b"1", b"a", b"2", b"b", b"3", b"c"],
        ),
        case(
            "ZREMRANGEBYSCORE",
            &[b"ZREMRANGEBYSCORE", b"zremscore", b"(1", b"3"],
        ),
        case(
            "ZREMRANGEBYSCORE missing",
            &[b"ZREMRANGEBYSCORE", b"missing-z", b"-inf", b"+inf"],
        ),
        case(
            "ZREMRANGEBYSCORE wrongtype",
            &[b"ZREMRANGEBYSCORE", b"zwrong", b"-inf", b"+inf"],
        ),
        case("ZRANGE zremscore", &[b"ZRANGE", b"zremscore", b"0", b"-1"]),
        case(
            "ZADD zremlex",
            &[b"ZADD", b"zremlex", b"0", b"a", b"0", b"b", b"0", b"c"],
        ),
        case(
            "ZREMRANGEBYLEX",
            &[b"ZREMRANGEBYLEX", b"zremlex", b"[b", b"[c"],
        ),
        case(
            "ZREMRANGEBYLEX missing",
            &[b"ZREMRANGEBYLEX", b"missing-z", b"-", b"+"],
        ),
        case(
            "ZREMRANGEBYLEX wrongtype",
            &[b"ZREMRANGEBYLEX", b"zwrong", b"-", b"+"],
        ),
        case("ZRANGE zremlex", &[b"ZRANGE", b"zremlex", b"0", b"-1"]),
        case(
            "ZRANGESTORE",
            &[b"ZRANGESTORE", b"zstored", b"z2", b"0", b"1"],
        ),
        case("ZRANGE zstored", &[b"ZRANGE", b"zstored", b"0", b"-1"]),
        case("ZADD zu1", &[b"ZADD", b"zu1", b"1", b"a", b"2", b"b"]),
        case("ZADD zu2", &[b"ZADD", b"zu2", b"3", b"a", b"4", b"c"]),
        case(
            "ZUNIONSTORE weighted",
            &[
                b"ZUNIONSTORE",
                b"zout",
                b"2",
                b"zu1",
                b"zu2",
                b"WEIGHTS",
                b"2",
                b"3",
                b"AGGREGATE",
                b"SUM",
            ],
        ),
        case(
            "ZRANGE zout",
            &[b"ZRANGE", b"zout", b"0", b"-1", b"WITHSCORES"],
        ),
        case(
            "ZINTERSTORE weighted",
            &[
                b"ZINTERSTORE",
                b"zi",
                b"2",
                b"zu1",
                b"zu2",
                b"WEIGHTS",
                b"2",
                b"3",
                b"AGGREGATE",
                b"SUM",
            ],
        ),
        case("ZRANGE zi", &[b"ZRANGE", b"zi", b"0", b"-1", b"WITHSCORES"]),
        case("ZDIFFSTORE", &[b"ZDIFFSTORE", b"zd", b"2", b"zu1", b"zu2"]),
        case("ZRANGE zd", &[b"ZRANGE", b"zd", b"0", b"-1", b"WITHSCORES"]),
        case("ZADD bzmin", &[b"ZADD", b"bzmin", b"1", b"a"]),
        case("BZPOPMIN ready", &[b"BZPOPMIN", b"bzmin", b"0"]),
        case("ZADD bzmax", &[b"ZADD", b"bzmax", b"1", b"a"]),
        case("BZPOPMAX ready", &[b"BZPOPMAX", b"bzmax", b"0"]),
        case("SET wrongtype", &[b"SET", b"wrong", b"value"]),
        case("HGET wrongtype", &[b"HGET", b"wrong", b"field"]),
    ]
}

fn assert_compatible(case: &CompatCase, fast_cache: Frame, reference: Frame) {
    match case.compare {
        CompareMode::Exact => assert_eq!(
            fast_cache, reference,
            "{} diverged for {:?}",
            case.name, case.parts
        ),
        CompareMode::PositiveInteger => {
            let fast = integer(&fast_cache);
            let reference = integer(&reference);
            assert!(
                fast > 0 && reference > 0,
                "{} expected positive integers, got fast-cache={fast_cache:?} reference={reference:?}",
                case.name
            );
        }
        CompareMode::UnorderedArray => assert_eq!(
            sorted_bulk_array(fast_cache),
            sorted_bulk_array(reference),
            "{} unordered array diverged",
            case.name
        ),
        CompareMode::UnorderedPairArray => assert_eq!(
            sorted_bulk_pairs(fast_cache),
            sorted_bulk_pairs(reference),
            "{} unordered pair array diverged",
            case.name
        ),
        CompareMode::ScanUnorderedArray => assert_eq!(
            sorted_scan_bulk_array(fast_cache),
            sorted_scan_bulk_array(reference),
            "{} scan array diverged",
            case.name
        ),
        CompareMode::ScanUnorderedPairArray => assert_eq!(
            sorted_scan_bulk_pairs(fast_cache),
            sorted_scan_bulk_pairs(reference),
            "{} scan pair array diverged",
            case.name
        ),
    }
}

fn integer(frame: &Frame) -> i64 {
    match frame {
        Frame::Integer(value) => *value,
        other => panic!("expected integer frame, got {other:?}"),
    }
}

fn sorted_bulk_array(frame: Frame) -> Vec<Vec<u8>> {
    let Frame::Array(items) = frame else {
        panic!("expected array frame");
    };
    let mut values = items
        .into_iter()
        .map(|item| match item {
            Frame::BlobString(value) => value,
            other => panic!("expected bulk string array item, got {other:?}"),
        })
        .collect::<Vec<_>>();
    values.sort();
    values
}

fn sorted_bulk_pairs(frame: Frame) -> Vec<(Vec<u8>, Vec<u8>)> {
    let Frame::Array(items) = frame else {
        panic!("expected array frame");
    };
    assert!(
        items.len().is_multiple_of(2),
        "expected even pair array length"
    );
    let mut pairs = items
        .chunks_exact(2)
        .map(|pair| {
            let key = match &pair[0] {
                Frame::BlobString(value) => value.clone(),
                other => panic!("expected bulk string key, got {other:?}"),
            };
            let value = match &pair[1] {
                Frame::BlobString(value) => value.clone(),
                other => panic!("expected bulk string value, got {other:?}"),
            };
            (key, value)
        })
        .collect::<Vec<_>>();
    pairs.sort();
    pairs
}

fn scan_values(frame: Frame) -> Vec<Frame> {
    let Frame::Array(items) = frame else {
        panic!("expected scan response array");
    };
    assert_eq!(
        items.len(),
        2,
        "scan response should have cursor and values"
    );
    match items.into_iter().nth(1).expect("scan values") {
        Frame::Array(values) => values,
        other => panic!("expected scan values array, got {other:?}"),
    }
}

fn sorted_scan_bulk_array(frame: Frame) -> Vec<Vec<u8>> {
    sorted_bulk_array(Frame::Array(scan_values(frame)))
}

fn sorted_scan_bulk_pairs(frame: Frame) -> Vec<(Vec<u8>, Vec<u8>)> {
    sorted_bulk_pairs(Frame::Array(scan_values(frame)))
}
