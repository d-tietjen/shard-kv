use crate::commands::redis::{
    array_bulk, bulk, eq_ignore_ascii_case, error, int, optional_string_value, parse_usize, simple,
    wrong_arity, wrongtype,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisKeyStore, RedisObjectValue};

macro_rules! define_admin_command {
    ($type:ident, $static_name:ident, $name:literal, $mutates:expr) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static $static_name: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;
        }
    };
    ($type:ident, $static_name:ident, $name:literal, $mutates:expr, aliases: [$($alias:literal),+ $(,)?]) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) struct $type;

        pub(crate) static $static_name: $type = $type;

        impl crate::commands::CommandSpec for $type {
            const NAME: &'static str = $name;
            const MUTATES_VALUE: bool = $mutates;

            #[inline(always)]
            fn matches(name: &[u8]) -> bool {
                name.eq_ignore_ascii_case($name.as_bytes())
                    $(|| name.eq_ignore_ascii_case($alias.as_bytes()))+
            }
        }
    };
}

define_admin_command!(Asking, ASKING_COMMAND, "ASKING", false);
define_admin_command!(BgRewriteAof, BGREWRITEAOF_COMMAND, "BGREWRITEAOF", false);
define_admin_command!(BgSave, BGSAVE_COMMAND, "BGSAVE", false);
define_admin_command!(Cluster, CLUSTER_COMMAND, "CLUSTER", false);
define_admin_command!(Debug, DEBUG_COMMAND, "DEBUG", false);
define_admin_command!(HostWarning, HOST_WARNING_COMMAND, "HOST:", false);
define_admin_command!(LastSave, LASTSAVE_COMMAND, "LASTSAVE", false);
define_admin_command!(Latency, LATENCY_COMMAND, "LATENCY", false);
define_admin_command!(Lolwut, LOLWUT_COMMAND, "LOLWUT", false);
define_admin_command!(Migrate, MIGRATE_COMMAND, "MIGRATE", true);
define_admin_command!(Module, MODULE_COMMAND, "MODULE", false);
define_admin_command!(Monitor, MONITOR_COMMAND, "MONITOR", false);
define_admin_command!(Move, MOVE_COMMAND, "MOVE", true);
define_admin_command!(PostWarning, POST_WARNING_COMMAND, "POST", false);
define_admin_command!(PSync, PSYNC_COMMAND, "PSYNC", false);
define_admin_command!(ReadOnly, READONLY_COMMAND, "READONLY", false);
define_admin_command!(ReadWrite, READWRITE_COMMAND, "READWRITE", false);
define_admin_command!(ReplConf, REPLCONF_COMMAND, "REPLCONF", false);
define_admin_command!(ReplicaOf, REPLICAOF_COMMAND, "REPLICAOF", false, aliases: ["SLAVEOF"]);
define_admin_command!(Role, ROLE_COMMAND, "ROLE", false);
define_admin_command!(Save, SAVE_COMMAND, "SAVE", false);
define_admin_command!(Shutdown, SHUTDOWN_COMMAND, "SHUTDOWN", false);
define_admin_command!(SlowLog, SLOWLOG_COMMAND, "SLOWLOG", false);
define_admin_command!(Sort, SORT_COMMAND, "SORT", false);
define_admin_command!(SwapDb, SWAPDB_COMMAND, "SWAPDB", true);
define_admin_command!(Sync, SYNC_COMMAND, "SYNC", false);
define_admin_command!(Wait, WAIT_COMMAND, "WAIT", false);

macro_rules! simple_command {
    ($type:ident, $name:literal, $reply:literal) => {
        impl crate::commands::redis::RedisCommand for $type {
            fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
                match args {
                    [] => simple($reply),
                    _ => wrong_arity($name),
                }
            }
        }
    };
}

macro_rules! error_command {
    ($type:ident, $name:literal, $message:literal) => {
        impl crate::commands::redis::RedisCommand for $type {
            fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
                match args {
                    [] => error($message),
                    _ => wrong_arity($name),
                }
            }
        }
    };
}

simple_command!(Asking, "ASKING", "OK");
simple_command!(
    BgRewriteAof,
    "BGREWRITEAOF",
    "Background append only file rewriting started"
);
simple_command!(BgSave, "BGSAVE", "Background saving started");
simple_command!(Save, "SAVE", "OK");
simple_command!(ReadOnly, "READONLY", "OK");
simple_command!(ReadWrite, "READWRITE", "OK");
error_command!(
    Monitor,
    "MONITOR",
    "ERR MONITOR is not supported by shardcache"
);
error_command!(
    Shutdown,
    "SHUTDOWN",
    "ERR SHUTDOWN is disabled by shardcache"
);
error_command!(
    Sync,
    "SYNC",
    "ERR SYNC replication is not supported by shardcache"
);

impl crate::commands::redis::RedisCommand for LastSave {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => int((crate::storage::now_millis() / 1000) as i64),
            _ => wrong_arity("LASTSAVE"),
        }
    }
}

impl crate::commands::redis::RedisCommand for Cluster {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        if args.is_empty() {
            wrong_arity("CLUSTER")
        } else {
            error("ERR This instance has cluster support disabled")
        }
    }
}

impl crate::commands::redis::RedisCommand for Debug {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub] if eq_ignore_ascii_case(sub, b"HELP") => Frame::Array(vec![
                bulk(b"DEBUG HELP".to_vec()),
                bulk(b"DEBUG is intentionally limited in shardcache".to_vec()),
            ]),
            [_sub, ..] => error("ERR DEBUG command is disabled by shardcache"),
            _ => wrong_arity("DEBUG"),
        }
    }
}

impl crate::commands::redis::RedisCommand for HostWarning {
    fn execute(_store: &EmbeddedStore, _args: &[&[u8]]) -> Frame {
        security_warning()
    }
}

impl crate::commands::redis::RedisCommand for PostWarning {
    fn execute(_store: &EmbeddedStore, _args: &[&[u8]]) -> Frame {
        security_warning()
    }
}

impl crate::commands::redis::RedisCommand for Latency {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub] if eq_ignore_ascii_case(sub, b"LATEST") => Frame::Array(Vec::new()),
            [sub] if eq_ignore_ascii_case(sub, b"HISTORY") => Frame::Array(Vec::new()),
            [sub] if eq_ignore_ascii_case(sub, b"RESET") => int(0),
            [sub] if eq_ignore_ascii_case(sub, b"DOCTOR") => {
                bulk(b"No latency spikes observed by shardcache.\n".to_vec())
            }
            [sub] if eq_ignore_ascii_case(sub, b"HELP") => Frame::Array(vec![
                bulk(b"LATENCY LATEST".to_vec()),
                bulk(b"LATENCY HISTORY event".to_vec()),
                bulk(b"LATENCY RESET [event ...]".to_vec()),
                bulk(b"LATENCY DOCTOR".to_vec()),
            ]),
            _ => error("ERR unknown LATENCY subcommand or wrong number of arguments"),
        }
    }
}

impl crate::commands::redis::RedisCommand for Lolwut {
    fn execute(_store: &EmbeddedStore, _args: &[&[u8]]) -> Frame {
        bulk(b"shardcache Redis compatibility\n".to_vec())
    }
}

impl crate::commands::redis::RedisCommand for Migrate {
    fn execute(_store: &EmbeddedStore, _args: &[&[u8]]) -> Frame {
        error("ERR MIGRATE is not supported by shardcache")
    }
}

impl crate::commands::redis::RedisCommand for Module {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub] if eq_ignore_ascii_case(sub, b"LIST") => Frame::Array(Vec::new()),
            [sub] if eq_ignore_ascii_case(sub, b"HELP") => Frame::Array(vec![
                bulk(b"MODULE LIST".to_vec()),
                bulk(b"MODULE HELP".to_vec()),
            ]),
            [sub] if eq_ignore_ascii_case(sub, b"LOAD") || eq_ignore_ascii_case(sub, b"UNLOAD") => {
                error("ERR Redis modules are not supported by shardcache")
            }
            _ => error("ERR unknown MODULE subcommand or wrong number of arguments"),
        }
    }
}

impl crate::commands::redis::RedisCommand for Move {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [_key, db] if db == b"0" => error("ERR source and destination objects are the same"),
            [_key, _db] => int(0),
            _ => wrong_arity("MOVE"),
        }
    }
}

impl crate::commands::redis::RedisCommand for PSync {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [_replication_id, _offset] => {
                error("ERR PSYNC replication is not supported by shardcache")
            }
            _ => wrong_arity("PSYNC"),
        }
    }
}

impl crate::commands::redis::RedisCommand for ReplConf {
    fn execute(_store: &EmbeddedStore, _args: &[&[u8]]) -> Frame {
        simple("OK")
    }
}

impl crate::commands::redis::RedisCommand for ReplicaOf {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [host, port]
                if eq_ignore_ascii_case(host, b"NO") && eq_ignore_ascii_case(port, b"ONE") =>
            {
                simple("OK")
            }
            [_host, _port] => error("ERR replication is not supported by shardcache"),
            _ => wrong_arity("REPLICAOF"),
        }
    }
}

impl crate::commands::redis::RedisCommand for Role {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [] => Frame::Array(vec![
                bulk(b"master".to_vec()),
                Frame::Integer(0),
                Frame::Array(Vec::new()),
            ]),
            _ => wrong_arity("ROLE"),
        }
    }
}

impl crate::commands::redis::RedisCommand for SlowLog {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [sub] if eq_ignore_ascii_case(sub, b"LEN") => int(0),
            [sub] if eq_ignore_ascii_case(sub, b"GET") => Frame::Array(Vec::new()),
            [sub, _count] if eq_ignore_ascii_case(sub, b"GET") => Frame::Array(Vec::new()),
            [sub] if eq_ignore_ascii_case(sub, b"RESET") => simple("OK"),
            [sub] if eq_ignore_ascii_case(sub, b"HELP") => Frame::Array(vec![
                bulk(b"SLOWLOG GET [count]".to_vec()),
                bulk(b"SLOWLOG LEN".to_vec()),
                bulk(b"SLOWLOG RESET".to_vec()),
            ]),
            _ => error("ERR unknown SLOWLOG subcommand or wrong number of arguments"),
        }
    }
}

impl crate::commands::redis::RedisCommand for Sort {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [key, tail @ ..] = args else {
            return wrong_arity("SORT");
        };
        let mut alpha = false;
        let mut desc = false;
        let mut limit = None;
        let mut store_dest = None;
        let mut index = 0;
        while index < tail.len() {
            let option = tail[index];
            if eq_ignore_ascii_case(option, b"ASC") {
                desc = false;
                index += 1;
            } else if eq_ignore_ascii_case(option, b"DESC") {
                desc = true;
                index += 1;
            } else if eq_ignore_ascii_case(option, b"ALPHA") {
                alpha = true;
                index += 1;
            } else if eq_ignore_ascii_case(option, b"LIMIT") {
                let (Some(offset), Some(count)) = (tail.get(index + 1), tail.get(index + 2)) else {
                    return error("ERR syntax error");
                };
                let (Ok(offset), Ok(count)) = (parse_usize(offset), parse_usize(count)) else {
                    return error("ERR value is not an integer or out of range");
                };
                limit = Some((offset, count));
                index += 3;
            } else if eq_ignore_ascii_case(option, b"STORE") {
                let Some(dest) = tail.get(index + 1) else {
                    return error("ERR syntax error");
                };
                store_dest = Some(*dest);
                index += 2;
            } else if eq_ignore_ascii_case(option, b"BY") {
                let Some(pattern) = tail.get(index + 1) else {
                    return error("ERR syntax error");
                };
                if !eq_ignore_ascii_case(pattern, b"nosort") {
                    return error("ERR SORT BY patterns are not supported by shardcache");
                }
                index += 2;
            } else if eq_ignore_ascii_case(option, b"GET") {
                return error("ERR SORT GET patterns are not supported by shardcache");
            } else {
                return error("ERR syntax error");
            }
        }

        let mut values = match sort_values(store, key) {
            Ok(values) => values,
            Err(frame) => return frame,
        };
        if alpha {
            values.sort();
        } else {
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                let Ok(value_str) = std::str::from_utf8(&value) else {
                    return error("ERR One or more scores can't be converted into double");
                };
                let Ok(score) = value_str.parse::<f64>() else {
                    return error("ERR One or more scores can't be converted into double");
                };
                parsed.push((score, value));
            }
            parsed.sort_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            values = parsed.into_iter().map(|(_, value)| value).collect();
        }
        if desc {
            values.reverse();
        }
        if let Some((offset, count)) = limit {
            values = values.into_iter().skip(offset).take(count).collect();
        }

        match store_dest {
            Some(dest) => {
                let len = values.len() as i64;
                store.set_object_value(dest, RedisObjectValue::List(values), None);
                int(len)
            }
            None => array_bulk(values),
        }
    }
}

impl crate::commands::redis::RedisCommand for SwapDb {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [left, right] if left == b"0" && right == b"0" => simple("OK"),
            [_left, _right] => error("ERR DB index is out of range"),
            _ => wrong_arity("SWAPDB"),
        }
    }
}

impl crate::commands::redis::RedisCommand for Wait {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        match args {
            [_replicas, _timeout] => int(0),
            _ => wrong_arity("WAIT"),
        }
    }
}

fn security_warning() -> Frame {
    error("ERR possible SECURITY ATTACK detected. This command is not accepted by shardcache.")
}

fn sort_values(store: &EmbeddedStore, key: &[u8]) -> Result<Vec<Vec<u8>>, Frame> {
    match store.clone_object_value(key) {
        Some(RedisObjectValue::List(values)) | Some(RedisObjectValue::Set(values)) => Ok(values),
        Some(RedisObjectValue::ZSet(values)) => {
            Ok(values.into_iter().map(|(member, _)| member).collect())
        }
        Some(RedisObjectValue::Hash(_)) => Err(wrongtype()),
        None => match optional_string_value(store, key, true) {
            Ok(Some(value)) => Ok(vec![value]),
            Ok(None) => Ok(Vec::new()),
            Err(frame) => Err(frame),
        },
    }
}
