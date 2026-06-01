use crate::commands::redis::{
    bulk, eq_ignore_ascii_case, error, int, parse_i64, simple, wrong_arity,
};
use crate::protocol::Frame;
use crate::storage::EmbeddedStore;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Function;
pub(crate) static FUNCTION_COMMAND: Function = Function;

#[derive(Debug, Clone, Copy)]
pub(crate) struct FCall;
pub(crate) static FCALL_COMMAND: FCall = FCall;

#[derive(Debug, Clone, Copy)]
pub(crate) struct FCallRo;
pub(crate) static FCALL_RO_COMMAND: FCallRo = FCallRo;

impl crate::commands::CommandSpec for Function {
    const NAME: &'static str = "FUNCTION";
    const MUTATES_VALUE: bool = true;
}

impl crate::commands::CommandSpec for FCall {
    const NAME: &'static str = "FCALL";
    const MUTATES_VALUE: bool = true;
}

impl crate::commands::CommandSpec for FCallRo {
    const NAME: &'static str = "FCALL_RO";
    const MUTATES_VALUE: bool = false;
}

impl crate::commands::redis::RedisCommand for Function {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [subcommand, tail @ ..] = args else {
            return wrong_arity("FUNCTION");
        };
        match subcommand {
            subcommand if eq_ignore_ascii_case(subcommand, b"LIST") => function_list(tail),
            subcommand if eq_ignore_ascii_case(subcommand, b"STATS") => {
                if !tail.is_empty() {
                    return wrong_arity("FUNCTION");
                }
                Frame::Array(vec![
                    bulk(b"running_script".to_vec()),
                    Frame::Null,
                    bulk(b"engines".to_vec()),
                    Frame::Array(vec![
                        bulk(b"LUA".to_vec()),
                        Frame::Array(vec![
                            bulk(b"libraries_count".to_vec()),
                            int(0),
                            bulk(b"functions_count".to_vec()),
                            int(0),
                        ]),
                    ]),
                ])
            }
            subcommand if eq_ignore_ascii_case(subcommand, b"DUMP") => match tail {
                [] => bulk(Vec::new()),
                _ => wrong_arity("FUNCTION"),
            },
            subcommand if eq_ignore_ascii_case(subcommand, b"FLUSH") => match tail {
                [] => simple("OK"),
                [mode]
                    if eq_ignore_ascii_case(mode, b"ASYNC")
                        || eq_ignore_ascii_case(mode, b"SYNC") =>
                {
                    simple("OK")
                }
                _ => error("ERR syntax error"),
            },
            subcommand if eq_ignore_ascii_case(subcommand, b"DELETE") => match tail {
                [_library] => error("ERR Library not found"),
                _ => wrong_arity("FUNCTION"),
            },
            subcommand if eq_ignore_ascii_case(subcommand, b"KILL") => match tail {
                [] => error("NOTBUSY No scripts in execution right now."),
                _ => wrong_arity("FUNCTION"),
            },
            subcommand if eq_ignore_ascii_case(subcommand, b"LOAD") => function_load(tail),
            subcommand if eq_ignore_ascii_case(subcommand, b"RESTORE") => function_restore(tail),
            subcommand if eq_ignore_ascii_case(subcommand, b"HELP") => {
                if !tail.is_empty() {
                    return wrong_arity("FUNCTION");
                }
                Frame::Array(vec![
                    bulk(b"FUNCTION LIST".to_vec()),
                    bulk(b"FUNCTION STATS".to_vec()),
                    bulk(b"FUNCTION FLUSH [ASYNC|SYNC]".to_vec()),
                    bulk(b"FUNCTION DUMP".to_vec()),
                    bulk(b"FUNCTION DELETE library-name".to_vec()),
                    bulk(b"FUNCTION KILL".to_vec()),
                    bulk(b"FUNCTION LOAD [REPLACE] function-code".to_vec()),
                    bulk(b"FUNCTION RESTORE serialized-value [FLUSH|APPEND|REPLACE]".to_vec()),
                ])
            }
            _ => error("ERR unknown FUNCTION subcommand or wrong number of arguments"),
        }
    }
}

fn function_list(args: &[&[u8]]) -> Frame {
    let mut cursor = 0;
    let mut saw_library_name = false;
    let mut saw_with_code = false;
    while cursor < args.len() {
        let option = args[cursor];
        cursor += 1;
        match option {
            option if eq_ignore_ascii_case(option, b"LIBRARYNAME") => {
                if saw_library_name || cursor >= args.len() {
                    return error("ERR syntax error");
                }
                saw_library_name = true;
                cursor += 1;
            }
            option if eq_ignore_ascii_case(option, b"WITHCODE") => {
                if saw_with_code {
                    return error("ERR syntax error");
                }
                saw_with_code = true;
            }
            _ => return error("ERR syntax error"),
        }
    }
    Frame::Array(Vec::new())
}

fn function_load(args: &[&[u8]]) -> Frame {
    match args {
        [_code] => error("ERR Redis functions are not supported by shardcache"),
        [replace, _code] if eq_ignore_ascii_case(replace, b"REPLACE") => {
            error("ERR Redis functions are not supported by shardcache")
        }
        [..] if args.len() <= 1 => wrong_arity("FUNCTION"),
        _ => error("ERR syntax error"),
    }
}

fn function_restore(args: &[&[u8]]) -> Frame {
    match args {
        [_payload] => error("ERR Redis functions are not supported by shardcache"),
        [_payload, policy]
            if eq_ignore_ascii_case(policy, b"FLUSH")
                || eq_ignore_ascii_case(policy, b"APPEND")
                || eq_ignore_ascii_case(policy, b"REPLACE") =>
        {
            error("ERR Redis functions are not supported by shardcache")
        }
        [] => wrong_arity("FUNCTION"),
        _ => error("ERR syntax error"),
    }
}

impl crate::commands::redis::RedisCommand for FCall {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        fcall(args, "FCALL")
    }
}

impl crate::commands::redis::RedisCommand for FCallRo {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        fcall(args, "FCALL_RO")
    }
}

fn fcall(args: &[&[u8]], name: &str) -> Frame {
    let [_function, numkeys_raw, tail @ ..] = args else {
        return wrong_arity(name);
    };
    let Ok(numkeys) = parse_i64(numkeys_raw) else {
        return error("ERR value is not an integer or out of range");
    };
    if numkeys < 0 {
        return error("ERR value is not an integer or out of range");
    }
    if tail.len() < numkeys as usize {
        return error("ERR Number of keys can't be greater than number of args");
    }
    error("ERR Function not found")
}
