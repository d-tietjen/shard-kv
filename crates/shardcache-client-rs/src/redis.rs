use crate::client::{ShardCacheClient, ShardCacheDirectClient, ShardCacheDirectShardClient};
use crate::commands::redis::RedisResponse;
use crate::error::Result;

/// Converts a Rust value into one Redis argv item.
pub trait RedisArg {
    fn to_redis_arg(&self) -> Vec<u8>;
}

impl RedisArg for [u8] {
    fn to_redis_arg(&self) -> Vec<u8> {
        self.to_vec()
    }
}

impl<const N: usize> RedisArg for [u8; N] {
    fn to_redis_arg(&self) -> Vec<u8> {
        self.to_vec()
    }
}

impl RedisArg for Vec<u8> {
    fn to_redis_arg(&self) -> Vec<u8> {
        self.clone()
    }
}

impl RedisArg for str {
    fn to_redis_arg(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl RedisArg for String {
    fn to_redis_arg(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl<T: RedisArg + ?Sized> RedisArg for &T {
    fn to_redis_arg(&self) -> Vec<u8> {
        (*self).to_redis_arg()
    }
}

macro_rules! redis_numeric_arg {
    ($($type:ty),+ $(,)?) => {
        $(
            impl RedisArg for $type {
                fn to_redis_arg(&self) -> Vec<u8> {
                    self.to_string().into_bytes()
                }
            }
        )+
    };
}

redis_numeric_arg!(i8, i16, i32, i64, i128, isize);
redis_numeric_arg!(u8, u16, u32, u64, u128, usize);
redis_numeric_arg!(f32, f64);

impl RedisArg for bool {
    fn to_redis_arg(&self) -> Vec<u8> {
        if *self { b"1".to_vec() } else { b"0".to_vec() }
    }
}

macro_rules! raw_redis_methods {
    ($($method:ident => $command:literal;)+) => {
        $(
            #[doc = concat!("Executes Redis `", $command, "` with raw command arguments.")]
            pub fn $method<A, I>(&mut self, args: I) -> Result<RedisResponse>
            where
                A: RedisArg,
                I: IntoIterator<Item = A>,
            {
                self.query_command($command, args)
            }
        )+
    };
}

/// Executes first-party Redis commands over native SCNP.
///
/// Construct this with [`ShardCacheClient::redis`],
/// [`ShardCacheDirectClient::redis`], or [`ShardCacheDirectShardClient::redis`].
/// Fanout clients can fall back to the SCNP command-name path for commands
/// without compact opcodes. Direct shard clients support compact-opcode Redis
/// commands only, because shard-owned SCNP listeners require pre-routed frames.
#[derive(Debug)]
pub struct Redis<'client, C: RedisCommandExecutor + ?Sized> {
    client: &'client mut C,
}

impl<'client, C: RedisCommandExecutor + ?Sized> Redis<'client, C> {
    pub(crate) fn new(client: &'client mut C) -> Self {
        Self { client }
    }

    /// Starts building a Redis command by name.
    ///
    /// This mirrors the common Redis client command-construction style:
    ///
    /// ```rust,ignore
    /// client.redis().command("SET").arg("key").arg("value").query()?;
    /// ```
    pub fn command(&mut self, command: impl RedisArg) -> RedisCmd<'_, 'client, C> {
        RedisCmd::new(self, command.to_redis_arg())
    }

    /// Executes a Redis command by name with generic Redis argv.
    ///
    /// Prefer [`command`](Self::command) when building a command incrementally.
    /// This helper is useful when the argv is already available as an iterable.
    pub fn query_command<A, I>(&mut self, command: impl RedisArg, args: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        let mut command = self.command(command);
        command.args(args);
        command.query()
    }

    /// Executes a Redis command by name with borrowed byte argv.
    ///
    /// Prefer [`command`](Self::command) for generic Redis-style use. This
    /// preserves the original zero-copy escape hatch for callers that already
    /// have command bytes and argv slices.
    pub fn execute(&mut self, command: &[u8], args: &[&[u8]]) -> Result<RedisResponse> {
        self.client.execute_redis_command(command, args)
    }

    /// Executes Redis `PING`.
    pub fn ping(&mut self) -> Result<RedisResponse> {
        self.command(b"PING").query()
    }

    /// Executes Redis `ECHO`.
    pub fn echo(&mut self, message: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"ECHO").arg(message).query()
    }

    /// Executes Redis `GET`.
    pub fn get(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"GET").arg(key).query()
    }

    /// Executes Redis `SET`.
    pub fn set(&mut self, key: impl RedisArg, value: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"SET").arg(key).arg(value).query()
    }

    /// Executes Redis `SETNX`.
    pub fn setnx(&mut self, key: impl RedisArg, value: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"SETNX").arg(key).arg(value).query()
    }

    /// Executes Redis `SETNX`.
    pub fn set_nx(&mut self, key: impl RedisArg, value: impl RedisArg) -> Result<RedisResponse> {
        self.setnx(key, value)
    }

    /// Executes Redis `SETEX` using redis-rs style argument order.
    pub fn set_ex(
        &mut self,
        key: impl RedisArg,
        value: impl RedisArg,
        seconds: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"SETEX")
            .arg(key)
            .arg(seconds)
            .arg(value)
            .query()
    }

    /// Executes Redis `SETEX` using Redis command argument order.
    pub fn setex(
        &mut self,
        key: impl RedisArg,
        seconds: impl RedisArg,
        value: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"SETEX")
            .arg(key)
            .arg(seconds)
            .arg(value)
            .query()
    }

    /// Executes Redis `PSETEX` using redis-rs style argument order.
    pub fn pset_ex(
        &mut self,
        key: impl RedisArg,
        value: impl RedisArg,
        milliseconds: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"PSETEX")
            .arg(key)
            .arg(milliseconds)
            .arg(value)
            .query()
    }

    /// Executes Redis `PSETEX` using Redis command argument order.
    pub fn psetex(
        &mut self,
        key: impl RedisArg,
        milliseconds: impl RedisArg,
        value: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"PSETEX")
            .arg(key)
            .arg(milliseconds)
            .arg(value)
            .query()
    }

    /// Executes Redis `GETDEL`.
    pub fn getdel(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"GETDEL").arg(key).query()
    }

    /// Executes Redis `GETDEL`.
    pub fn get_del(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.getdel(key)
    }

    /// Executes Redis `MGET`.
    pub fn mget<A, I>(&mut self, keys: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"MGET").args(keys).query()
    }

    /// Executes Redis `MSET`.
    pub fn mset<K, V, I>(&mut self, items: I) -> Result<RedisResponse>
    where
        K: RedisArg,
        V: RedisArg,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut command = self.command(b"MSET");
        for (key, value) in items {
            command.arg(key).arg(value);
        }
        command.query()
    }

    /// Executes Redis `DEL` for one key.
    pub fn del(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"DEL").arg(key).query()
    }

    /// Executes Redis `DEL` for multiple keys.
    pub fn del_many<A, I>(&mut self, keys: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"DEL").args(keys).query()
    }

    /// Executes Redis `EXISTS` for one key.
    pub fn exists(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"EXISTS").arg(key).query()
    }

    /// Executes Redis `EXISTS` for multiple keys.
    pub fn exists_many<A, I>(&mut self, keys: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"EXISTS").args(keys).query()
    }

    /// Executes Redis `EXPIRE`.
    pub fn expire(&mut self, key: impl RedisArg, seconds: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"EXPIRE").arg(key).arg(seconds).query()
    }

    /// Executes Redis `PEXPIRE`.
    pub fn pexpire(
        &mut self,
        key: impl RedisArg,
        milliseconds: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"PEXPIRE").arg(key).arg(milliseconds).query()
    }

    /// Executes Redis `TTL`.
    pub fn ttl(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"TTL").arg(key).query()
    }

    /// Executes Redis `PTTL`.
    pub fn pttl(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"PTTL").arg(key).query()
    }

    /// Executes Redis `TYPE`.
    pub fn type_(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"TYPE").arg(key).query()
    }

    /// Executes Redis `RENAME`.
    pub fn rename(&mut self, key: impl RedisArg, new_key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"RENAME").arg(key).arg(new_key).query()
    }

    /// Executes Redis `INCR`.
    pub fn incr(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"INCR").arg(key).query()
    }

    /// Executes Redis `INCRBY`.
    pub fn incrby(
        &mut self,
        key: impl RedisArg,
        increment: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"INCRBY").arg(key).arg(increment).query()
    }

    /// Executes Redis `DECR`.
    pub fn decr(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"DECR").arg(key).query()
    }

    /// Executes Redis `DECRBY`.
    pub fn decrby(
        &mut self,
        key: impl RedisArg,
        decrement: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"DECRBY").arg(key).arg(decrement).query()
    }

    /// Executes Redis `HSET`.
    pub fn hset(
        &mut self,
        key: impl RedisArg,
        field: impl RedisArg,
        value: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"HSET").arg(key).arg(field).arg(value).query()
    }

    /// Executes Redis `HGET`.
    pub fn hget(&mut self, key: impl RedisArg, field: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"HGET").arg(key).arg(field).query()
    }

    /// Executes Redis `HGETALL`.
    pub fn hgetall(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"HGETALL").arg(key).query()
    }

    /// Executes Redis `HDEL`.
    pub fn hdel<A, I>(&mut self, key: impl RedisArg, fields: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"HDEL").arg(key).args(fields).query()
    }

    /// Executes Redis `HEXISTS`.
    pub fn hexists(&mut self, key: impl RedisArg, field: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"HEXISTS").arg(key).arg(field).query()
    }

    /// Executes Redis `HLEN`.
    pub fn hlen(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"HLEN").arg(key).query()
    }

    /// Executes Redis `HMGET`.
    pub fn hmget<A, I>(&mut self, key: impl RedisArg, fields: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"HMGET").arg(key).args(fields).query()
    }

    /// Executes Redis `HMSET`.
    pub fn hmset<F, V, I>(&mut self, key: impl RedisArg, items: I) -> Result<RedisResponse>
    where
        F: RedisArg,
        V: RedisArg,
        I: IntoIterator<Item = (F, V)>,
    {
        let mut command = self.command(b"HMSET");
        command.arg(key);
        for (field, value) in items {
            command.arg(field).arg(value);
        }
        command.query()
    }

    /// Executes Redis `HKEYS`.
    pub fn hkeys(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"HKEYS").arg(key).query()
    }

    /// Executes Redis `HVALS`.
    pub fn hvals(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"HVALS").arg(key).query()
    }

    /// Executes Redis `LPUSH`.
    pub fn lpush<A, I>(&mut self, key: impl RedisArg, values: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"LPUSH").arg(key).args(values).query()
    }

    /// Executes Redis `RPUSH`.
    pub fn rpush<A, I>(&mut self, key: impl RedisArg, values: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"RPUSH").arg(key).args(values).query()
    }

    /// Executes Redis `LPOP`.
    pub fn lpop(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"LPOP").arg(key).query()
    }

    /// Executes Redis `RPOP`.
    pub fn rpop(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"RPOP").arg(key).query()
    }

    /// Executes Redis `LLEN`.
    pub fn llen(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"LLEN").arg(key).query()
    }

    /// Executes Redis `LRANGE`.
    pub fn lrange(
        &mut self,
        key: impl RedisArg,
        start: impl RedisArg,
        stop: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"LRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .query()
    }

    /// Executes Redis `SADD`.
    pub fn sadd<A, I>(&mut self, key: impl RedisArg, members: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"SADD").arg(key).args(members).query()
    }

    /// Executes Redis `SREM`.
    pub fn srem<A, I>(&mut self, key: impl RedisArg, members: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"SREM").arg(key).args(members).query()
    }

    /// Executes Redis `SCARD`.
    pub fn scard(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"SCARD").arg(key).query()
    }

    /// Executes Redis `SISMEMBER`.
    pub fn sismember(
        &mut self,
        key: impl RedisArg,
        member: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"SISMEMBER").arg(key).arg(member).query()
    }

    /// Executes Redis `SMEMBERS`.
    pub fn smembers(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"SMEMBERS").arg(key).query()
    }

    /// Executes Redis `ZADD`.
    pub fn zadd(
        &mut self,
        key: impl RedisArg,
        score: impl RedisArg,
        member: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"ZADD")
            .arg(key)
            .arg(score)
            .arg(member)
            .query()
    }

    /// Executes Redis `ZREM`.
    pub fn zrem<A, I>(&mut self, key: impl RedisArg, members: I) -> Result<RedisResponse>
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.command(b"ZREM").arg(key).args(members).query()
    }

    /// Executes Redis `ZSCORE`.
    pub fn zscore(&mut self, key: impl RedisArg, member: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"ZSCORE").arg(key).arg(member).query()
    }

    /// Executes Redis `ZCARD`.
    pub fn zcard(&mut self, key: impl RedisArg) -> Result<RedisResponse> {
        self.command(b"ZCARD").arg(key).query()
    }

    /// Executes Redis `ZRANGE`.
    pub fn zrange(
        &mut self,
        key: impl RedisArg,
        start: impl RedisArg,
        stop: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"ZRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .query()
    }

    /// Executes Redis `GEOADD`.
    pub fn geoadd(
        &mut self,
        key: impl RedisArg,
        longitude: impl RedisArg,
        latitude: impl RedisArg,
        member: impl RedisArg,
    ) -> Result<RedisResponse> {
        self.command(b"GEOADD")
            .arg(key)
            .arg(longitude)
            .arg(latitude)
            .arg(member)
            .query()
    }

    raw_redis_methods! {
        append => "APPEND";
        asking => "ASKING";
        auth => "AUTH";
        bgrewriteaof => "BGREWRITEAOF";
        bgsave => "BGSAVE";
        bitcount => "BITCOUNT";
        bitfield => "BITFIELD";
        bitop => "BITOP";
        bitpos => "BITPOS";
        blmove => "BLMOVE";
        blmpop => "BLMPOP";
        blpop => "BLPOP";
        brpop => "BRPOP";
        brpoplpush => "BRPOPLPUSH";
        bzmpop => "BZMPOP";
        bzpopmax => "BZPOPMAX";
        bzpopmin => "BZPOPMIN";
        client => "CLIENT";
        cluster => "CLUSTER";
        config => "CONFIG";
        copy => "COPY";
        dbsize => "DBSIZE";
        debug => "DEBUG";
        discard => "DISCARD";
        dump => "DUMP";
        eval => "EVAL";
        evalsha => "EVALSHA";
        exec => "EXEC";
        expireat => "EXPIREAT";
        expiretime => "EXPIRETIME";
        flushall => "FLUSHALL";
        flushdb => "FLUSHDB";
        geodist => "GEODIST";
        geohash => "GEOHASH";
        geopos => "GEOPOS";
        georadius => "GEORADIUS";
        georadius_ro => "GEORADIUS_RO";
        georadiusbymember => "GEORADIUSBYMEMBER";
        georadiusbymember_ro => "GEORADIUSBYMEMBER_RO";
        getbit => "GETBIT";
        getex => "GETEX";
        getrange => "GETRANGE";
        getset => "GETSET";
        hello => "HELLO";
        hincrby => "HINCRBY";
        hincrbyfloat => "HINCRBYFLOAT";
        host_warning => "HOST:";
        hrandfield => "HRANDFIELD";
        hscan => "HSCAN";
        hsetnx => "HSETNX";
        hstrlen => "HSTRLEN";
        incrbyfloat => "INCRBYFLOAT";
        info => "INFO";
        keys => "KEYS";
        lastsave => "LASTSAVE";
        latency => "LATENCY";
        lindex => "LINDEX";
        linsert => "LINSERT";
        lmove => "LMOVE";
        lmpop => "LMPOP";
        lolwut => "LOLWUT";
        lpushx => "LPUSHX";
        lrem => "LREM";
        lset => "LSET";
        ltrim => "LTRIM";
        memory => "MEMORY";
        migrate => "MIGRATE";
        module => "MODULE";
        monitor => "MONITOR";
        move_ => "MOVE";
        msetnx => "MSETNX";
        multi => "MULTI";
        object => "OBJECT";
        persist => "PERSIST";
        pexpireat => "PEXPIREAT";
        pexpiretime => "PEXPIRETIME";
        pfadd => "PFADD";
        pfcount => "PFCOUNT";
        pfdebug => "PFDEBUG";
        pfmerge => "PFMERGE";
        pfselftest => "PFSELFTEST";
        post => "POST";
        psubscribe => "PSUBSCRIBE";
        psync => "PSYNC";
        publish => "PUBLISH";
        pubsub => "PUBSUB";
        punsubscribe => "PUNSUBSCRIBE";
        quit => "QUIT";
        randomkey => "RANDOMKEY";
        readonly => "READONLY";
        readwrite => "READWRITE";
        renamenx => "RENAMENX";
        replconf => "REPLCONF";
        replicaof => "REPLICAOF";
        restore => "RESTORE";
        restore_asking => "RESTORE-ASKING";
        role => "ROLE";
        rpoplpush => "RPOPLPUSH";
        rpushx => "RPUSHX";
        save => "SAVE";
        scan => "SCAN";
        script => "SCRIPT";
        sdiff => "SDIFF";
        sdiffstore => "SDIFFSTORE";
        select => "SELECT";
        setbit => "SETBIT";
        setrange => "SETRANGE";
        shutdown => "SHUTDOWN";
        sinter => "SINTER";
        sinterstore => "SINTERSTORE";
        slaveof => "SLAVEOF";
        slowlog => "SLOWLOG";
        smismember => "SMISMEMBER";
        smove => "SMOVE";
        sort => "SORT";
        spop => "SPOP";
        srandmember => "SRANDMEMBER";
        sscan => "SSCAN";
        strlen => "STRLEN";
        subscribe => "SUBSCRIBE";
        substr => "SUBSTR";
        sunion => "SUNION";
        sunionstore => "SUNIONSTORE";
        swapdb => "SWAPDB";
        sync => "SYNC";
        time => "TIME";
        touch => "TOUCH";
        unlink => "UNLINK";
        unsubscribe => "UNSUBSCRIBE";
        unwatch => "UNWATCH";
        wait => "WAIT";
        watch => "WATCH";
        xack => "XACK";
        xadd => "XADD";
        xclaim => "XCLAIM";
        xdel => "XDEL";
        xgroup => "XGROUP";
        xinfo => "XINFO";
        xlen => "XLEN";
        xpending => "XPENDING";
        xrange => "XRANGE";
        xread => "XREAD";
        xreadgroup => "XREADGROUP";
        xrevrange => "XREVRANGE";
        xsetid => "XSETID";
        xtrim => "XTRIM";
        zcount => "ZCOUNT";
        zdiff => "ZDIFF";
        zdiffstore => "ZDIFFSTORE";
        zincrby => "ZINCRBY";
        zinter => "ZINTER";
        zintercard => "ZINTERCARD";
        zinterstore => "ZINTERSTORE";
        zlexcount => "ZLEXCOUNT";
        zmpop => "ZMPOP";
        zmscore => "ZMSCORE";
        zpopmax => "ZPOPMAX";
        zpopmin => "ZPOPMIN";
        zrandmember => "ZRANDMEMBER";
        zrangebylex => "ZRANGEBYLEX";
        zrangebyscore => "ZRANGEBYSCORE";
        zrangestore => "ZRANGESTORE";
        zrank => "ZRANK";
        zremrangebylex => "ZREMRANGEBYLEX";
        zremrangebyrank => "ZREMRANGEBYRANK";
        zremrangebyscore => "ZREMRANGEBYSCORE";
        zrevrange => "ZREVRANGE";
        zrevrangebylex => "ZREVRANGEBYLEX";
        zrevrangebyscore => "ZREVRANGEBYSCORE";
        zrevrank => "ZREVRANK";
        zscan => "ZSCAN";
        zunion => "ZUNION";
        zunionstore => "ZUNIONSTORE";
    }
}

/// Incremental Redis command builder.
///
/// This follows the redis-rs style: start with [`Redis::command`], append
/// arguments with [`arg`](Self::arg) or [`args`](Self::args), then execute with
/// [`query`](Self::query).
#[derive(Debug)]
pub struct RedisCmd<'redis, 'client, C: RedisCommandExecutor + ?Sized> {
    redis: &'redis mut Redis<'client, C>,
    command: Vec<u8>,
    args: Vec<Vec<u8>>,
}

impl<'redis, 'client, C: RedisCommandExecutor + ?Sized> RedisCmd<'redis, 'client, C> {
    fn new(redis: &'redis mut Redis<'client, C>, command: Vec<u8>) -> Self {
        Self {
            redis,
            command,
            args: Vec::new(),
        }
    }

    /// Appends one Redis argv item.
    pub fn arg(&mut self, arg: impl RedisArg) -> &mut Self {
        self.args.push(arg.to_redis_arg());
        self
    }

    /// Appends multiple Redis argv items.
    pub fn args<A, I>(&mut self, args: I) -> &mut Self
    where
        A: RedisArg,
        I: IntoIterator<Item = A>,
    {
        self.args.extend(collect_args(args));
        self
    }

    /// Executes the built command and returns the Redis response.
    pub fn query(&mut self) -> Result<RedisResponse> {
        let refs = self.args.iter().map(Vec::as_slice).collect::<Vec<_>>();
        self.redis.execute(&self.command, &refs)
    }
}

/// Backend used by the first-party Redis command namespace.
pub trait RedisCommandExecutor {
    fn execute_redis_command(&mut self, command: &[u8], args: &[&[u8]]) -> Result<RedisResponse>;
}

impl RedisCommandExecutor for ShardCacheClient {
    fn execute_redis_command(&mut self, command: &[u8], args: &[&[u8]]) -> Result<RedisResponse> {
        self.redis_command_by_name(command, args)
    }
}

impl RedisCommandExecutor for ShardCacheDirectClient {
    fn execute_redis_command(&mut self, command: &[u8], args: &[&[u8]]) -> Result<RedisResponse> {
        self.redis_command_by_name(command, args)
    }
}

impl RedisCommandExecutor for ShardCacheDirectShardClient {
    fn execute_redis_command(&mut self, command: &[u8], args: &[&[u8]]) -> Result<RedisResponse> {
        self.redis_command_by_name(command, args)
    }
}

fn collect_args<A, I>(args: I) -> Vec<Vec<u8>>
where
    A: RedisArg,
    I: IntoIterator<Item = A>,
{
    args.into_iter().map(|arg| arg.to_redis_arg()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Recorder {
        calls: Vec<(Vec<u8>, Vec<Vec<u8>>)>,
    }

    impl RedisCommandExecutor for Recorder {
        fn execute_redis_command(
            &mut self,
            command: &[u8],
            args: &[&[u8]],
        ) -> Result<RedisResponse> {
            self.calls.push((
                command.to_vec(),
                args.iter().map(|arg| arg.to_vec()).collect(),
            ));
            Ok(RedisResponse::Ok)
        }
    }

    #[test]
    fn typed_methods_dispatch_command_names_and_args() {
        let mut recorder = Recorder::default();
        {
            let mut redis = Redis::new(&mut recorder);
            redis.hgetall(b"user:42").unwrap();
            redis
                .geoadd("places", -122.4194, 37.7749, "san-francisco")
                .unwrap();
            redis.type_("user:42").unwrap();
            redis.zadd("rank", 1.5, "alice").unwrap();
        }

        assert_eq!(recorder.calls[0].0, b"HGETALL");
        assert_eq!(recorder.calls[0].1, vec![b"user:42".to_vec()]);
        assert_eq!(recorder.calls[1].0, b"GEOADD");
        assert_eq!(recorder.calls[1].1[0], b"places");
        assert_eq!(recorder.calls[2].0, b"TYPE");
        assert_eq!(recorder.calls[3].0, b"ZADD");
        assert_eq!(
            recorder.calls[3].1,
            vec![b"rank".to_vec(), b"1.5".to_vec(), b"alice".to_vec()]
        );
    }

    #[test]
    fn command_dispatches_generic_argv() {
        let mut recorder = Recorder::default();
        {
            let mut redis = Redis::new(&mut recorder);
            redis.command("COMMAND").arg("COUNT").query().unwrap();

            let mut command = redis.command("MSET");
            command.arg("a").arg("1").args(["b", "2"]);
            command.query().unwrap();
        }

        assert_eq!(recorder.calls[0].0, b"COMMAND");
        assert_eq!(recorder.calls[0].1, vec![b"COUNT".to_vec()]);
        assert_eq!(recorder.calls[1].0, b"MSET");
        assert_eq!(
            recorder.calls[1].1,
            vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"2".to_vec()]
        );
    }

    #[test]
    fn typed_variadic_helpers_expand_arguments() {
        let mut recorder = Recorder::default();
        {
            let mut redis = Redis::new(&mut recorder);
            redis.mset([("a", "1"), ("b", "2")]).unwrap();
            redis.hmget("hash", ["a", "b"]).unwrap();
            redis.del_many(["a", "b"]).unwrap();
        }

        assert_eq!(recorder.calls[0].0, b"MSET");
        assert_eq!(
            recorder.calls[0].1,
            vec![b"a".to_vec(), b"1".to_vec(), b"b".to_vec(), b"2".to_vec()]
        );
        assert_eq!(recorder.calls[1].0, b"HMGET");
        assert_eq!(
            recorder.calls[1].1,
            vec![b"hash".to_vec(), b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(recorder.calls[2].0, b"DEL");
        assert_eq!(recorder.calls[2].1, vec![b"a".to_vec(), b"b".to_vec()]);
    }
}
