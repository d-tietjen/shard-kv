#[cfg(feature = "server")]
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

#[cfg(not(feature = "server"))]
use crate::commands::redis::dispatch_redis_command;
use crate::commands::redis::{
    bulk, eq_ignore_ascii_case, error, int, simple, write_frame, wrong_arity,
};
#[cfg(feature = "server")]
use crate::commands::redis::{write_resp_array_header, write_resp_null, write_resp_simple_string};
use crate::protocol::Frame;
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Eval;

#[derive(Debug, Clone, Copy)]
pub(crate) struct EvalSha;

type EvalKeysAndArgv<'a> = (&'a [&'a [u8]], &'a [&'a [u8]]);

#[derive(Debug, Clone, Copy)]
pub(crate) struct Script;

pub(crate) static EVAL_COMMAND: Eval = Eval;
pub(crate) static EVALSHA_COMMAND: EvalSha = EvalSha;
pub(crate) static SCRIPT_COMMAND: Script = Script;

impl crate::commands::CommandSpec for Eval {
    const NAME: &'static str = "EVAL";
    const MUTATES_VALUE: bool = true;
}

impl crate::commands::CommandSpec for EvalSha {
    const NAME: &'static str = "EVALSHA";
    const MUTATES_VALUE: bool = true;
}

impl crate::commands::CommandSpec for Script {
    const NAME: &'static str = "SCRIPT";
    const MUTATES_VALUE: bool = false;
}

impl crate::commands::redis::RedisCommand for Eval {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        eval(store, args)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_eval_resp(store, args, out);
    }
}

impl crate::commands::redis::RedisCommand for EvalSha {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        evalsha(store, args)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_evalsha_resp(store, args, out);
    }
}

impl crate::commands::redis::RedisCommand for Script {
    fn execute(_store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        script(args)
    }

    #[cfg(feature = "server")]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let _ = store;
        write_script_resp(args, out);
    }
}

pub(crate) fn script_route_key<'a>(args: &[&'a [u8]]) -> Option<&'a [u8]> {
    let numkeys = args.get(1).and_then(|raw| parse_numkeys(raw).ok())?;
    if numkeys == 0 {
        return None;
    }
    args.get(2).copied()
}

fn eval(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let Some(script) = args.first().copied() else {
        return wrong_arity("EVAL");
    };
    let Ok((keys, argv)) = eval_keys_and_argv("EVAL", args) else {
        return eval_args_error("EVAL", args);
    };
    let digest = sha1(script);
    let script = cache_script(digest, script);
    run_script(store, &script, keys, argv)
}

fn evalsha(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
    let Some(raw_sha) = args.first().copied() else {
        return wrong_arity("EVALSHA");
    };
    let Ok((keys, argv)) = eval_keys_and_argv("EVALSHA", args) else {
        return eval_args_error("EVALSHA", args);
    };
    let Some(digest) = parse_sha1_hex(raw_sha) else {
        return error("NOSCRIPT No matching script. Please use EVAL.");
    };
    let Some(script) = cached_script(&digest) else {
        return error("NOSCRIPT No matching script. Please use EVAL.");
    };
    run_script(store, &script, keys, argv)
}

#[cfg(feature = "server")]
fn write_eval_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
    let Some(source) = args.first().copied() else {
        write_frame(out, &wrong_arity("EVAL"));
        return;
    };
    let Ok((keys, argv)) = eval_keys_and_argv("EVAL", args) else {
        write_frame(out, &eval_args_error("EVAL", args));
        return;
    };
    let script = cache_script(sha1(source), source);
    write_cached_script_resp(store, &script, keys, argv, out);
}

#[cfg(feature = "server")]
fn write_evalsha_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
    let Some(raw_sha) = args.first().copied() else {
        write_frame(out, &wrong_arity("EVALSHA"));
        return;
    };
    let Ok((keys, argv)) = eval_keys_and_argv("EVALSHA", args) else {
        write_frame(out, &eval_args_error("EVALSHA", args));
        return;
    };
    let Some(digest) = parse_sha1_hex(raw_sha) else {
        ServerWire::write_resp_error(out, "NOSCRIPT No matching script. Please use EVAL.");
        return;
    };
    let Some(script) = cached_script(&digest) else {
        ServerWire::write_resp_error(out, "NOSCRIPT No matching script. Please use EVAL.");
        return;
    };
    write_cached_script_resp(store, &script, keys, argv, out);
}

#[cfg(feature = "server")]
fn write_script_resp(args: &[&[u8]], out: &mut BytesMut) {
    match args {
        [] => write_frame(out, &wrong_arity("SCRIPT")),
        [subcommand, script] if eq_ignore_ascii_case(subcommand, b"LOAD") => {
            let digest = sha1(script);
            cache_script(digest, script);
            let hex = sha1_hex_bytes(digest);
            ServerWire::write_resp_blob_string(out, &hex);
        }
        [subcommand, shas @ ..] if eq_ignore_ascii_case(subcommand, b"EXISTS") => {
            write_resp_array_header(out, shas.len());
            for sha in shas {
                ServerWire::write_resp_integer(
                    out,
                    if parse_sha1_hex(sha).is_some_and(|digest| script_exists(&digest)) {
                        1
                    } else {
                        0
                    },
                );
            }
        }
        [subcommand] if eq_ignore_ascii_case(subcommand, b"FLUSH") => {
            clear_scripts();
            write_resp_simple_string(out, "OK");
        }
        [subcommand, mode]
            if eq_ignore_ascii_case(subcommand, b"FLUSH")
                && (eq_ignore_ascii_case(mode, b"ASYNC")
                    || eq_ignore_ascii_case(mode, b"SYNC")) =>
        {
            clear_scripts();
            write_resp_simple_string(out, "OK");
        }
        [subcommand] if eq_ignore_ascii_case(subcommand, b"KILL") => {
            ServerWire::write_resp_error(out, "NOTBUSY No scripts in execution right now.");
        }
        [subcommand, mode]
            if eq_ignore_ascii_case(subcommand, b"DEBUG")
                && (eq_ignore_ascii_case(mode, b"YES")
                    || eq_ignore_ascii_case(mode, b"SYNC")
                    || eq_ignore_ascii_case(mode, b"NO")) =>
        {
            write_resp_simple_string(out, "OK");
        }
        _ => write_frame(out, &script(args)),
    }
}

fn script(args: &[&[u8]]) -> Frame {
    match args {
        [] => wrong_arity("SCRIPT"),
        [subcommand, script] if eq_ignore_ascii_case(subcommand, b"LOAD") => {
            let digest = sha1(script);
            cache_script(digest, script);
            bulk(sha1_hex_digest(digest).into_bytes())
        }
        [subcommand, shas @ ..] if eq_ignore_ascii_case(subcommand, b"EXISTS") => Frame::Array(
            shas.iter()
                .map(|sha| {
                    int(
                        if parse_sha1_hex(sha).is_some_and(|digest| script_exists(&digest)) {
                            1
                        } else {
                            0
                        },
                    )
                })
                .collect(),
        ),
        [subcommand] if eq_ignore_ascii_case(subcommand, b"FLUSH") => {
            clear_scripts();
            simple("OK")
        }
        [subcommand, mode]
            if eq_ignore_ascii_case(subcommand, b"FLUSH")
                && (eq_ignore_ascii_case(mode, b"ASYNC")
                    || eq_ignore_ascii_case(mode, b"SYNC")) =>
        {
            clear_scripts();
            simple("OK")
        }
        [subcommand] if eq_ignore_ascii_case(subcommand, b"KILL") => {
            error("NOTBUSY No scripts in execution right now.")
        }
        [subcommand, mode]
            if eq_ignore_ascii_case(subcommand, b"DEBUG")
                && (eq_ignore_ascii_case(mode, b"YES")
                    || eq_ignore_ascii_case(mode, b"SYNC")
                    || eq_ignore_ascii_case(mode, b"NO")) =>
        {
            simple("OK")
        }
        [subcommand] if eq_ignore_ascii_case(subcommand, b"HELP") => Frame::Array(vec![
            bulk(b"SCRIPT LOAD script".to_vec()),
            bulk(b"SCRIPT EXISTS sha1 [sha1 ...]".to_vec()),
            bulk(b"SCRIPT FLUSH [ASYNC|SYNC]".to_vec()),
            bulk(b"SCRIPT KILL".to_vec()),
            bulk(b"SCRIPT DEBUG YES|SYNC|NO".to_vec()),
        ]),
        [subcommand, ..] if eq_ignore_ascii_case(subcommand, b"LOAD") => wrong_arity("SCRIPT"),
        [subcommand, ..]
            if eq_ignore_ascii_case(subcommand, b"FLUSH")
                || eq_ignore_ascii_case(subcommand, b"KILL")
                || eq_ignore_ascii_case(subcommand, b"DEBUG") =>
        {
            error("ERR syntax error")
        }
        _ => error("ERR unknown SCRIPT subcommand or wrong number of arguments"),
    }
}

fn eval_keys_and_argv<'a>(_command: &str, args: &'a [&'a [u8]]) -> Result<EvalKeysAndArgv<'a>, ()> {
    if args.len() < 2 {
        return Err(());
    }
    let numkeys = parse_numkeys(args[1])?;
    let key_start = 2;
    let argv_start = key_start + numkeys;
    if args.len() < argv_start {
        return Err(());
    }
    Ok((&args[key_start..argv_start], &args[argv_start..]))
}

fn eval_args_error(command: &str, args: &[&[u8]]) -> Frame {
    if args.len() < 2 {
        return wrong_arity(command);
    }
    match parse_numkeys(args[1]) {
        Ok(numkeys) if args.len() < 2 + numkeys => {
            error("ERR Number of keys can't be greater than number of args")
        }
        _ => error("ERR value is not an integer or out of range"),
    }
}

fn parse_numkeys(raw: &[u8]) -> Result<usize, ()> {
    let value = std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(())?;
    usize::try_from(value).map_err(|_| ())
}

fn run_script(
    store: &EmbeddedStore,
    script: &CachedScript,
    keys: &[&[u8]],
    argv: &[&[u8]],
) -> Frame {
    let program = match script.program() {
        Ok(program) => program,
        Err(message) => return error(message),
    };
    match (ScriptRuntime { store, keys, argv }).eval_program(program) {
        Ok(frame) => frame,
        Err(message) => error(&message),
    }
}

#[cfg(feature = "server")]
fn write_cached_script_resp(
    store: &EmbeddedStore,
    script: &CachedScript,
    keys: &[&[u8]],
    argv: &[&[u8]],
    out: &mut BytesMut,
) {
    let program = match script.program() {
        Ok(program) => program,
        Err(message) => {
            ServerWire::write_resp_error(out, message);
            return;
        }
    };
    let runtime = ScriptRuntime { store, keys, argv };
    match runtime.write_program_resp(program, out) {
        Ok(true) => {}
        Ok(false) => match runtime.eval_program(program) {
            Ok(frame) => write_frame(out, &frame),
            Err(message) => ServerWire::write_resp_error(out, &message),
        },
        Err(message) => ServerWire::write_resp_error(out, &message),
    }
}

fn script_cache() -> &'static RwLock<HashMap<[u8; 20], Arc<CachedScript>>> {
    static SCRIPT_CACHE: OnceLock<RwLock<HashMap<[u8; 20], Arc<CachedScript>>>> = OnceLock::new();
    SCRIPT_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn cache_script(sha: [u8; 20], script: &[u8]) -> Arc<CachedScript> {
    {
        let cache = script_cache().read().expect("script cache poisoned");
        if let Some(existing) = cache.get(&sha) {
            return Arc::clone(existing);
        }
    }
    let mut cache = script_cache().write().expect("script cache poisoned");
    if let Some(existing) = cache.get(&sha) {
        return Arc::clone(existing);
    }
    let script = Arc::new(CachedScript::new(Arc::<[u8]>::from(script)));
    cache.insert(sha, Arc::clone(&script));
    script
}

fn cached_script(sha: &[u8; 20]) -> Option<Arc<CachedScript>> {
    script_cache()
        .read()
        .expect("script cache poisoned")
        .get(sha)
        .map(Arc::clone)
}

fn script_exists(sha: &[u8; 20]) -> bool {
    script_cache()
        .read()
        .expect("script cache poisoned")
        .contains_key(sha)
}

fn clear_scripts() {
    script_cache()
        .write()
        .expect("script cache poisoned")
        .clear();
}

struct CachedScript {
    source: Arc<[u8]>,
    program: OnceLock<Result<ScriptProgram, String>>,
}

impl CachedScript {
    fn new(source: Arc<[u8]>) -> Self {
        Self {
            source,
            program: OnceLock::new(),
        }
    }

    fn program(&self) -> Result<&ScriptProgram, &str> {
        let program = self
            .program
            .get_or_init(|| ScriptParser::new(&self.source).parse_program());
        match program {
            Ok(program) => Ok(program),
            Err(message) => Err(message.as_str()),
        }
    }
}

struct ScriptProgram {
    statements: Vec<ScriptExpr>,
    returned: Option<ScriptExpr>,
}

enum ScriptExpr {
    Bulk(Vec<u8>),
    Int(i64),
    Null,
    Array(Vec<ScriptExpr>),
    Key(usize),
    Argv(usize),
    RedisCall(Vec<ScriptExpr>),
    ToNumber(Box<ScriptExpr>),
}

struct ScriptRuntime<'a> {
    store: &'a EmbeddedStore,
    keys: &'a [&'a [u8]],
    argv: &'a [&'a [u8]],
}

impl ScriptRuntime<'_> {
    fn eval_program(&self, program: &ScriptProgram) -> Result<Frame, String> {
        for statement in &program.statements {
            self.eval_expr(statement)?;
        }
        match &program.returned {
            Some(expr) => self.eval_expr(expr),
            None => Ok(Frame::Null),
        }
    }

    #[cfg(feature = "server")]
    fn write_program_resp(
        &self,
        program: &ScriptProgram,
        out: &mut BytesMut,
    ) -> Result<bool, String> {
        if !program.statements.is_empty() {
            return Ok(false);
        }
        match program.returned.as_ref() {
            Some(ScriptExpr::Bulk(value)) => {
                ServerWire::write_resp_blob_string(out, value);
                Ok(true)
            }
            Some(ScriptExpr::Int(value)) => {
                ServerWire::write_resp_integer(out, *value);
                Ok(true)
            }
            Some(ScriptExpr::Null) | None => {
                write_resp_null(out);
                Ok(true)
            }
            Some(ScriptExpr::Key(index)) => {
                self.write_indexed_resp(out, self.keys, *index);
                Ok(true)
            }
            Some(ScriptExpr::Argv(index)) => {
                self.write_indexed_resp(out, self.argv, *index);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    #[cfg(feature = "server")]
    fn write_indexed_resp(&self, out: &mut BytesMut, values: &[&[u8]], index: usize) {
        match values.get(index.saturating_sub(1)) {
            Some(value) if index > 0 => ServerWire::write_resp_blob_string(out, value),
            _ => write_resp_null(out),
        }
    }

    fn eval_expr(&self, expr: &ScriptExpr) -> Result<Frame, String> {
        match expr {
            ScriptExpr::Bulk(value) => Ok(bulk(value.clone())),
            ScriptExpr::Int(value) => Ok(int(*value)),
            ScriptExpr::Null => Ok(Frame::Null),
            ScriptExpr::Array(values) => values
                .iter()
                .map(|value| self.eval_expr(value))
                .collect::<Result<Vec<_>, _>>()
                .map(Frame::Array),
            ScriptExpr::Key(index) => Ok(self.indexed_value(self.keys, *index)),
            ScriptExpr::Argv(index) => Ok(self.indexed_value(self.argv, *index)),
            ScriptExpr::RedisCall(args) => self.eval_redis_call(args),
            ScriptExpr::ToNumber(expr) => {
                let raw = frame_to_arg(self.eval_expr(expr)?)?;
                let text = std::str::from_utf8(&raw).map_err(|_| {
                    "ERR Error running script: tonumber argument is not utf8".to_string()
                })?;
                let parsed = text.parse::<f64>().map_err(|_| {
                    "ERR Error running script: tonumber argument is not numeric".to_string()
                })?;
                Ok(int(parsed as i64))
            }
        }
    }

    fn indexed_value(&self, values: &[&[u8]], index: usize) -> Frame {
        match values.get(index.saturating_sub(1)) {
            Some(value) if index > 0 => bulk(value.to_vec()),
            _ => Frame::Null,
        }
    }

    fn eval_redis_call(&self, arg_exprs: &[ScriptExpr]) -> Result<Frame, String> {
        let args = arg_exprs
            .iter()
            .map(|expr| self.eval_expr(expr).and_then(frame_to_arg))
            .collect::<Result<Vec<_>, _>>()?;
        let Some(command) = args.first() else {
            return Err("ERR Error running script: redis.call requires a command name".into());
        };
        let command = String::from_utf8_lossy(command).to_ascii_uppercase();
        if command == "EVAL" || command == "EVALSHA" || command == "SCRIPT" {
            return Err("ERR This Redis command is not allowed from scripts".into());
        }
        let refs = args.iter().skip(1).map(Vec::as_slice).collect::<Vec<_>>();
        #[cfg(feature = "server")]
        {
            let mut parts = Vec::with_capacity(refs.len() + 1);
            parts.push(command.as_bytes());
            parts.extend(refs.iter().copied());
            let command = crate::storage::BorrowedCommand::from_parts(&parts)
                .map_err(|error| format!("ERR {error}"))?;
            Ok(command.execute_borrowed_frame(self.store, crate::storage::now_millis()))
        }
        #[cfg(not(feature = "server"))]
        Ok(dispatch_redis_command(&command, self.store, &refs))
    }
}

struct ScriptParser<'script> {
    source: &'script [u8],
    cursor: usize,
}

impl<'script> ScriptParser<'script> {
    fn new(source: &'script [u8]) -> Self {
        Self { source, cursor: 0 }
    }

    fn parse_program(&mut self) -> Result<ScriptProgram, String> {
        let mut statements = Vec::new();
        loop {
            self.skip_ws_and_separators();
            if self.is_eof() {
                return Ok(ScriptProgram {
                    statements,
                    returned: None,
                });
            }
            if self.consume_word(b"return") {
                let returned = self.parse_expr()?;
                self.skip_ws_and_separators();
                if !self.is_eof() {
                    return Err("ERR Error running script: unsupported Lua statement".into());
                }
                return Ok(ScriptProgram {
                    statements,
                    returned: Some(returned),
                });
            }
            if self.starts_with(b"redis.call") || self.starts_with(b"redis.pcall") {
                statements.push(self.parse_redis_call()?);
                self.skip_ws();
                self.consume_byte(b';');
                continue;
            }
            return Err("ERR Error running script: unsupported Lua statement".into());
        }
    }

    fn parse_expr(&mut self) -> Result<ScriptExpr, String> {
        self.skip_ws();
        match self.peek() {
            Some(b'\'') | Some(b'"') => self.parse_string().map(ScriptExpr::Bulk),
            Some(b'{') => self.parse_table(),
            Some(b'-' | b'0'..=b'9') => self.parse_integer().map(ScriptExpr::Int),
            Some(_) if self.consume_word(b"nil") => Ok(ScriptExpr::Null),
            Some(_) if self.consume_word(b"false") => Ok(ScriptExpr::Null),
            Some(_) if self.consume_word(b"true") => Ok(ScriptExpr::Int(1)),
            Some(_) if self.starts_with(b"KEYS") => self.parse_indexed_value(b"KEYS", true),
            Some(_) if self.starts_with(b"ARGV") => self.parse_indexed_value(b"ARGV", false),
            Some(_) if self.starts_with(b"redis.call") || self.starts_with(b"redis.pcall") => {
                self.parse_redis_call()
            }
            Some(_) if self.starts_with(b"tonumber") => self.parse_tonumber(),
            Some(byte) => Err(format!(
                "ERR Error running script: unsupported Lua expression near '{}'",
                byte as char
            )),
            None => Err("ERR Error running script: unexpected end of script".into()),
        }
    }

    fn parse_table(&mut self) -> Result<ScriptExpr, String> {
        self.expect_byte(b'{')?;
        let mut values = Vec::new();
        loop {
            self.skip_ws();
            if self.consume_byte(b'}') {
                break;
            }
            values.push(self.parse_expr()?);
            self.skip_ws();
            if self.consume_byte(b'}') {
                break;
            }
            self.expect_byte(b',')?;
        }
        Ok(ScriptExpr::Array(values))
    }

    fn parse_tonumber(&mut self) -> Result<ScriptExpr, String> {
        self.expect_bytes(b"tonumber")?;
        self.skip_ws();
        self.expect_byte(b'(')?;
        let value = self.parse_expr()?;
        self.skip_ws();
        self.expect_byte(b')')?;
        Ok(ScriptExpr::ToNumber(Box::new(value)))
    }

    fn parse_redis_call(&mut self) -> Result<ScriptExpr, String> {
        if self.starts_with(b"redis.call") {
            self.expect_bytes(b"redis.call")?;
        } else {
            self.expect_bytes(b"redis.pcall")?;
        }
        self.skip_ws();
        self.expect_byte(b'(')?;
        let mut args = Vec::new();
        loop {
            self.skip_ws();
            if self.consume_byte(b')') {
                break;
            }
            args.push(self.parse_expr()?);
            self.skip_ws();
            if self.consume_byte(b')') {
                break;
            }
            self.expect_byte(b',')?;
        }
        Ok(ScriptExpr::RedisCall(args))
    }

    fn parse_indexed_value(&mut self, name: &[u8], key: bool) -> Result<ScriptExpr, String> {
        self.expect_bytes(name)?;
        self.skip_ws();
        self.expect_byte(b'[')?;
        self.skip_ws();
        let index = self.parse_usize()?;
        self.skip_ws();
        self.expect_byte(b']')?;
        if key {
            Ok(ScriptExpr::Key(index))
        } else {
            Ok(ScriptExpr::Argv(index))
        }
    }

    fn parse_string(&mut self) -> Result<Vec<u8>, String> {
        let quote = self.next().expect("peek checked quote");
        let mut value = Vec::new();
        while let Some(byte) = self.next() {
            if byte == quote {
                return Ok(value);
            }
            if byte != b'\\' {
                value.push(byte);
                continue;
            }
            let Some(escaped) = self.next() else {
                return Err("ERR Error running script: unfinished string escape".into());
            };
            match escaped {
                b'n' => value.push(b'\n'),
                b'r' => value.push(b'\r'),
                b't' => value.push(b'\t'),
                b'\\' => value.push(b'\\'),
                b'\'' => value.push(b'\''),
                b'"' => value.push(b'"'),
                other => value.push(other),
            }
        }
        Err("ERR Error running script: unfinished string".into())
    }

    fn parse_integer(&mut self) -> Result<i64, String> {
        let start = self.cursor;
        self.consume_byte(b'-');
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.cursor += 1;
        }
        std::str::from_utf8(&self.source[start..self.cursor])
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| "ERR Error running script: invalid number".to_string())
    }

    fn parse_usize(&mut self) -> Result<usize, String> {
        let value = self.parse_integer()?;
        usize::try_from(value)
            .map_err(|_| "ERR Error running script: invalid array index".to_string())
    }

    fn skip_ws_and_separators(&mut self) {
        loop {
            self.skip_ws();
            if !self.consume_byte(b';') {
                break;
            }
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\r' | b'\n')) {
            self.cursor += 1;
        }
    }

    fn consume_word(&mut self, word: &[u8]) -> bool {
        if !self.starts_with(word) {
            return false;
        }
        let end = self.cursor + word.len();
        if self
            .source
            .get(end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            return false;
        }
        self.cursor = end;
        true
    }

    fn expect_bytes(&mut self, expected: &[u8]) -> Result<(), String> {
        if self.starts_with(expected) {
            self.cursor += expected.len();
            Ok(())
        } else {
            Err("ERR Error running script: unexpected token".into())
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), String> {
        if self.consume_byte(expected) {
            Ok(())
        } else {
            Err(format!(
                "ERR Error running script: expected '{}'",
                expected as char
            ))
        }
    }

    fn consume_byte(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn starts_with(&self, expected: &[u8]) -> bool {
        self.source
            .get(self.cursor..)
            .is_some_and(|tail| tail.starts_with(expected))
    }

    fn peek(&self) -> Option<u8> {
        self.source.get(self.cursor).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.cursor += 1;
        Some(byte)
    }

    fn is_eof(&self) -> bool {
        self.cursor >= self.source.len()
    }
}

fn frame_to_arg(frame: Frame) -> Result<Vec<u8>, String> {
    match frame {
        Frame::BlobString(value) => Ok(value),
        Frame::SimpleString(value) => Ok(value.into_bytes()),
        Frame::Integer(value) => Ok(value.to_string().into_bytes()),
        Frame::Null => Err("ERR Lua redis() command arguments must be strings or integers".into()),
        Frame::Error(message) => Err(message),
        Frame::Array(_)
        | Frame::Map(_)
        | Frame::Set(_)
        | Frame::Push(_)
        | Frame::Boolean(_)
        | Frame::Double(_)
        | Frame::BigNumber(_)
        | Frame::VerbatimString { .. }
        | Frame::Attribute { .. } => {
            Err("ERR Lua redis() command arguments must be strings or integers".into())
        }
    }
}

#[cfg(test)]
fn sha1_hex(input: &[u8]) -> String {
    sha1_hex_digest(sha1(input))
}

fn sha1_hex_digest(digest: [u8; 20]) -> String {
    String::from_utf8(sha1_hex_bytes(digest).to_vec()).expect("hex digest is valid utf8")
}

fn sha1_hex_bytes(digest: [u8; 20]) -> [u8; 40] {
    let mut out = [0_u8; 40];
    for (index, byte) in digest.into_iter().enumerate() {
        out[index * 2] = hex_digit(byte >> 4);
        out[index * 2 + 1] = hex_digit(byte & 0x0f);
    }
    out
}

fn parse_sha1_hex(raw: &[u8]) -> Option<[u8; 20]> {
    if raw.len() != 40 {
        return None;
    }
    let mut digest = [0_u8; 20];
    for (index, chunk) in raw.chunks_exact(2).enumerate() {
        digest[index] = (hex_value(chunk[0])? << 4) | hex_value(chunk[1])?;
    }
    Some(digest)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn hex_digit(value: u8) -> u8 {
    match value {
        0..=9 => b'0' + value,
        10..=15 => b'a' + value - 10,
        _ => unreachable!("nibble is always 0..=15"),
    }
}

fn sha1(input: &[u8]) -> [u8; 20] {
    let mut state = [
        0x6745_2301_u32,
        0xefcd_ab89_u32,
        0x98ba_dcfe_u32,
        0x1032_5476_u32,
        0xc3d2_e1f0_u32,
    ];
    let bit_len = (input.len() as u64).wrapping_mul(8);

    let mut chunks = input.chunks_exact(64);
    for chunk in &mut chunks {
        sha1_process_chunk(&mut state, chunk);
    }

    let remainder = chunks.remainder();
    let mut block = [0_u8; 64];
    block[..remainder.len()].copy_from_slice(remainder);
    block[remainder.len()] = 0x80;

    if remainder.len() >= 56 {
        sha1_process_chunk(&mut state, &block);
        block = [0_u8; 64];
    }
    block[56..].copy_from_slice(&bit_len.to_be_bytes());
    sha1_process_chunk(&mut state, &block);

    let mut digest = [0_u8; 20];
    digest[0..4].copy_from_slice(&state[0].to_be_bytes());
    digest[4..8].copy_from_slice(&state[1].to_be_bytes());
    digest[8..12].copy_from_slice(&state[2].to_be_bytes());
    digest[12..16].copy_from_slice(&state[3].to_be_bytes());
    digest[16..20].copy_from_slice(&state[4].to_be_bytes());
    digest
}

fn sha1_process_chunk(state: &mut [u32; 5], chunk: &[u8]) {
    debug_assert_eq!(chunk.len(), 64);
    let mut words = [0_u32; 80];
    for (index, word) in words.iter_mut().take(16).enumerate() {
        let start = index * 4;
        *word = u32::from_be_bytes([
            chunk[start],
            chunk[start + 1],
            chunk[start + 2],
            chunk[start + 3],
        ]);
    }
    for index in 16..80 {
        words[index] =
            (words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16])
                .rotate_left(1);
    }

    let mut a = state[0];
    let mut b = state[1];
    let mut c = state[2];
    let mut d = state[3];
    let mut e = state[4];

    for (index, word) in words.iter().enumerate() {
        let (f, k) = match index {
            0..=19 => ((b & c) | ((!b) & d), 0x5a82_7999),
            20..=39 => (b ^ c ^ d, 0x6ed9_eba1),
            40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1b_bcdc),
            _ => (b ^ c ^ d, 0xca62_c1d6),
        };
        let temp = a
            .rotate_left(5)
            .wrapping_add(f)
            .wrapping_add(e)
            .wrapping_add(k)
            .wrapping_add(*word);
        e = d;
        d = c;
        c = b.rotate_left(30);
        b = a;
        a = temp;
    }

    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
    state[4] = state[4].wrapping_add(e);
}

#[cfg(test)]
mod tests {
    use super::sha1_hex;

    #[test]
    fn sha1_matches_redis_script_hash() {
        assert_eq!(
            sha1_hex(b"return 'ok'"),
            "34f6a80fdc91746367dd8b572351df66b92c67ed"
        );
    }
}
