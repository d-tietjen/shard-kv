//! First-party embedded Redis command API.
//!
//! This surface executes the same Redis-compatible command implementations used
//! by the server path directly against an in-process [`EmbeddedStore`].

use std::borrow::Cow;
use std::fmt;

use crate::protocol::Frame;
use crate::storage::{EmbeddedRouteMode, EmbeddedStore};

/// In-process Redis-compatible command executor.
///
/// `EmbeddedRedis` is intentionally small: it owns an [`EmbeddedStore`] and
/// exposes direct command execution plus a prepared-command form for workloads
/// that execute the same command shape repeatedly.
#[derive(Debug)]
pub struct EmbeddedRedis {
    store: EmbeddedStore,
}

impl EmbeddedRedis {
    /// Creates a Redis-compatible embedded store with full-key routing.
    pub fn new(shard_count: usize) -> Self {
        Self {
            store: EmbeddedStore::new(shard_count),
        }
    }

    /// Creates a Redis-compatible embedded store with an explicit route mode.
    pub fn with_route_mode(shard_count: usize, route_mode: EmbeddedRouteMode) -> Self {
        Self {
            store: EmbeddedStore::with_route_mode(shard_count, route_mode),
        }
    }

    /// Wraps an existing embedded store.
    pub fn from_store(store: EmbeddedStore) -> Self {
        Self { store }
    }

    /// Returns the underlying embedded store.
    pub fn store(&self) -> &EmbeddedStore {
        &self.store
    }

    /// Consumes the executor and returns the underlying embedded store.
    pub fn into_store(self) -> EmbeddedStore {
        self.store
    }

    /// Executes a command represented as a Redis argument vector.
    ///
    /// The first part is the command name and the remaining parts are command
    /// arguments. Redis command errors are returned as [`Frame::Error`].
    pub fn execute<B>(&self, parts: &[B]) -> Frame
    where
        B: AsRef<[u8]>,
    {
        execute_redis_command(&self.store, parts)
    }

    /// Executes a command and reports non-Redis API errors separately.
    ///
    /// Redis command errors still return successfully as [`Frame::Error`];
    /// this result only covers malformed embedded API calls such as an empty
    /// command vector or a non-UTF-8 command name.
    pub fn try_execute<B>(&self, parts: &[B]) -> Result<Frame, EmbeddedRedisCommandError>
    where
        B: AsRef<[u8]>,
    {
        try_execute_redis_command(&self.store, parts)
    }

    /// Executes a command from a command name and borrowed argument slices.
    pub fn execute_slices(&self, command: &[u8], args: &[&[u8]]) -> Frame {
        execute_redis_command_slices(&self.store, command, args)
    }

    /// Executes a command from a command name and borrowed argument slices,
    /// returning malformed embedded API calls as Rust errors.
    pub fn try_execute_slices(
        &self,
        command: &[u8],
        args: &[&[u8]],
    ) -> Result<Frame, EmbeddedRedisCommandError> {
        try_execute_redis_command_slices(&self.store, command, args)
    }

    /// Prepares a Redis command for repeated embedded execution.
    pub fn prepare<B>(
        &self,
        parts: &[B],
    ) -> Result<PreparedEmbeddedRedisCommand, EmbeddedRedisCommandError>
    where
        B: AsRef<[u8]>,
    {
        PreparedEmbeddedRedisCommand::new(parts)
    }

    /// Executes a previously prepared command against this store.
    pub fn execute_prepared(&self, command: &PreparedEmbeddedRedisCommand) -> Frame {
        command.execute(self)
    }

    /// Creates a stateful embedded Redis session for commands such as
    /// `MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH`.
    pub fn session(&self) -> EmbeddedRedisSession<'_> {
        EmbeddedRedisSession::new(self)
    }
}

/// Stateful Redis-compatible embedded command executor.
///
/// The session keeps per-client command state while still executing against the
/// same shared embedded store. Transaction queuing is intentionally scoped to
/// this object, so callers can create one session per embedded client thread.
#[derive(Debug)]
pub struct EmbeddedRedisSession<'a> {
    redis: &'a EmbeddedRedis,
    transaction: Option<Vec<PreparedEmbeddedRedisCommand>>,
}

impl<'a> EmbeddedRedisSession<'a> {
    /// Creates a session bound to an embedded Redis executor.
    pub fn new(redis: &'a EmbeddedRedis) -> Self {
        Self {
            redis,
            transaction: None,
        }
    }

    /// Executes a command represented as a Redis argument vector.
    pub fn execute<B>(&mut self, parts: &[B]) -> Frame
    where
        B: AsRef<[u8]>,
    {
        match PreparedEmbeddedRedisCommand::new(parts) {
            Ok(command) => self.execute_prepared(&command),
            Err(error) => error.as_redis_error(),
        }
    }

    /// Executes a prepared command while honoring this session's transaction
    /// state.
    pub fn execute_prepared(&mut self, command: &PreparedEmbeddedRedisCommand) -> Frame {
        match command.command_name() {
            "MULTI" => {
                if !command.args().is_empty() {
                    wrong_arity("multi")
                } else {
                    self.start_transaction()
                }
            }
            "DISCARD" => {
                if !command.args().is_empty() {
                    wrong_arity("discard")
                } else {
                    self.discard_transaction()
                }
            }
            "EXEC" => {
                if !command.args().is_empty() {
                    wrong_arity("exec")
                } else {
                    self.execute_transaction()
                }
            }
            "WATCH" => {
                if command.args().is_empty() {
                    wrong_arity("watch")
                } else {
                    Frame::SimpleString("OK".into())
                }
            }
            "UNWATCH" => {
                if !command.args().is_empty() {
                    wrong_arity("unwatch")
                } else {
                    Frame::SimpleString("OK".into())
                }
            }
            _ => {
                if let Some(transaction) = self.transaction.as_mut() {
                    transaction.push(command.clone());
                    Frame::SimpleString("QUEUED".into())
                } else {
                    command.execute(self.redis)
                }
            }
        }
    }

    fn start_transaction(&mut self) -> Frame {
        if self.transaction.is_some() {
            Frame::Error("ERR MULTI calls can not be nested".into())
        } else {
            self.transaction = Some(Vec::new());
            Frame::SimpleString("OK".into())
        }
    }

    fn discard_transaction(&mut self) -> Frame {
        if self.transaction.take().is_some() {
            Frame::SimpleString("OK".into())
        } else {
            Frame::Error("ERR DISCARD without MULTI".into())
        }
    }

    fn execute_transaction(&mut self) -> Frame {
        let Some(commands) = self.transaction.take() else {
            return Frame::Error("ERR EXEC without MULTI".into());
        };
        Frame::Array(
            commands
                .iter()
                .map(|command| command.execute(self.redis))
                .collect(),
        )
    }
}

fn wrong_arity(command: &str) -> Frame {
    Frame::Error(format!(
        "ERR wrong number of arguments for '{command}' command"
    ))
}

/// A Redis command with normalized command name and owned argument bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedEmbeddedRedisCommand {
    name: String,
    args: Vec<Vec<u8>>,
    bytes: usize,
}

impl PreparedEmbeddedRedisCommand {
    /// Creates a prepared command from a Redis argument vector.
    pub fn new<B>(parts: &[B]) -> Result<Self, EmbeddedRedisCommandError>
    where
        B: AsRef<[u8]>,
    {
        let Some((name, args)) = parts.split_first() else {
            return Err(EmbeddedRedisCommandError::EmptyCommand);
        };
        let name = normalized_command_name(name.as_ref())?.into_owned();
        let args = args
            .iter()
            .map(|arg| arg.as_ref().to_vec())
            .collect::<Vec<_>>();
        let bytes = name.len()
            + args.iter().map(Vec::len).sum::<usize>()
            + args.len().saturating_mul(std::mem::size_of::<Vec<u8>>());
        Ok(Self { name, args, bytes })
    }

    /// Returns the normalized command name.
    pub fn command_name(&self) -> &str {
        &self.name
    }

    /// Returns the owned command arguments.
    pub fn args(&self) -> &[Vec<u8>] {
        &self.args
    }

    /// Returns the approximate owned bytes used by this prepared command.
    pub fn encoded_bytes(&self) -> usize {
        self.bytes
    }

    /// Executes this prepared command against an embedded Redis executor.
    pub fn execute(&self, redis: &EmbeddedRedis) -> Frame {
        self.execute_on_store(redis.store())
    }

    /// Executes this prepared command against an embedded store.
    pub fn execute_on_store(&self, store: &EmbeddedStore) -> Frame {
        let args = self.args.iter().map(Vec::as_slice).collect::<Vec<_>>();
        crate::commands::redis::dispatch_redis_command(&self.name, store, &args)
    }
}

/// Errors from the embedded command API itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddedRedisCommandError {
    /// The supplied command vector was empty.
    EmptyCommand,
    /// The command name was not valid UTF-8.
    InvalidCommandName,
}

impl EmbeddedRedisCommandError {
    fn as_redis_error(&self) -> Frame {
        match self {
            Self::EmptyCommand => Frame::Error("ERR empty Redis command".into()),
            Self::InvalidCommandName => Frame::Error("ERR invalid Redis command name".into()),
        }
    }
}

impl fmt::Display for EmbeddedRedisCommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCommand => f.write_str("empty Redis command"),
            Self::InvalidCommandName => f.write_str("invalid Redis command name"),
        }
    }
}

impl std::error::Error for EmbeddedRedisCommandError {}

/// Executes a Redis-compatible command directly against an embedded store.
pub fn execute_redis_command<B>(store: &EmbeddedStore, parts: &[B]) -> Frame
where
    B: AsRef<[u8]>,
{
    match try_execute_redis_command(store, parts) {
        Ok(frame) => frame,
        Err(error) => error.as_redis_error(),
    }
}

/// Executes a Redis-compatible command directly against an embedded store and
/// returns malformed embedded API calls as Rust errors.
pub fn try_execute_redis_command<B>(
    store: &EmbeddedStore,
    parts: &[B],
) -> Result<Frame, EmbeddedRedisCommandError>
where
    B: AsRef<[u8]>,
{
    let Some((command, args)) = parts.split_first() else {
        return Err(EmbeddedRedisCommandError::EmptyCommand);
    };
    let args = args.iter().map(|arg| arg.as_ref()).collect::<Vec<_>>();
    try_execute_redis_command_slices(store, command.as_ref(), &args)
}

/// Executes a Redis-compatible command from a command name and argument slices.
pub fn execute_redis_command_slices(
    store: &EmbeddedStore,
    command: &[u8],
    args: &[&[u8]],
) -> Frame {
    match try_execute_redis_command_slices(store, command, args) {
        Ok(frame) => frame,
        Err(error) => error.as_redis_error(),
    }
}

/// Executes a Redis-compatible command from a command name and argument slices,
/// returning malformed embedded API calls as Rust errors.
pub fn try_execute_redis_command_slices(
    store: &EmbeddedStore,
    command: &[u8],
    args: &[&[u8]],
) -> Result<Frame, EmbeddedRedisCommandError> {
    let name = normalized_command_name(command)?;
    Ok(crate::commands::redis::dispatch_redis_command(
        name.as_ref(),
        store,
        args,
    ))
}

fn normalized_command_name(raw: &[u8]) -> Result<Cow<'_, str>, EmbeddedRedisCommandError> {
    let text =
        std::str::from_utf8(raw).map_err(|_| EmbeddedRedisCommandError::InvalidCommandName)?;
    if raw.iter().any(u8::is_ascii_lowercase) {
        Ok(Cow::Owned(text.to_ascii_uppercase()))
    } else {
        Ok(Cow::Borrowed(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_redis_executes_core_commands() {
        let redis = EmbeddedRedis::new(4);

        assert_eq!(
            redis.execute(&[b"PING".as_slice()]),
            Frame::SimpleString("PONG".into())
        );
        assert_eq!(
            redis.execute(&[b"set".as_slice(), b"k", b"v"]),
            Frame::SimpleString("OK".into())
        );
        assert_eq!(
            redis.execute(&[b"GET".as_slice(), b"k"]),
            Frame::BlobString(b"v".to_vec())
        );
        assert_eq!(
            redis.execute(&[b"HSET".as_slice(), b"h", b"f", b"v"]),
            Frame::Integer(1)
        );
        assert_eq!(
            redis.execute(&[b"HGET".as_slice(), b"h", b"f"]),
            Frame::BlobString(b"v".to_vec())
        );
    }

    #[test]
    fn prepared_commands_execute_without_renormalizing_callers() {
        let redis = EmbeddedRedis::new(2);
        let set = redis.prepare(&[b"set".as_slice(), b"k", b"v"]).unwrap();
        let get = redis.prepare(&[b"get".as_slice(), b"k"]).unwrap();

        assert_eq!(set.command_name(), "SET");
        assert_eq!(
            redis.execute_prepared(&set),
            Frame::SimpleString("OK".into())
        );
        assert_eq!(
            redis.execute_prepared(&get),
            Frame::BlobString(b"v".to_vec())
        );
        assert!(set.encoded_bytes() >= 3);
    }

    #[test]
    fn malformed_embedded_calls_are_redis_errors_on_lossy_api() {
        let redis = EmbeddedRedis::new(1);
        assert_eq!(
            redis.execute::<&[u8]>(&[]),
            Frame::Error("ERR empty Redis command".into())
        );
        assert_eq!(
            redis.try_execute::<&[u8]>(&[]).unwrap_err(),
            EmbeddedRedisCommandError::EmptyCommand
        );
    }

    #[test]
    fn embedded_sessions_queue_transactions() {
        let redis = EmbeddedRedis::new(2);
        let mut session = redis.session();

        assert_eq!(
            session.execute(&[b"MULTI".as_slice()]),
            Frame::SimpleString("OK".into())
        );
        assert_eq!(
            session.execute(&[b"SET".as_slice(), b"txn", b"v"]),
            Frame::SimpleString("QUEUED".into())
        );
        assert_eq!(
            session.execute(&[b"GET".as_slice(), b"txn"]),
            Frame::SimpleString("QUEUED".into())
        );
        assert_eq!(
            session.execute(&[b"EXEC".as_slice()]),
            Frame::Array(vec![
                Frame::SimpleString("OK".into()),
                Frame::BlobString(b"v".to_vec())
            ])
        );
    }

    #[cfg(feature = "redis-module-topk")]
    #[test]
    fn embedded_redis_executes_module_commands_when_enabled() {
        let redis = EmbeddedRedis::new(4);
        assert_eq!(
            redis.execute(&[b"TOPK.RESERVE".as_slice(), b"tk", b"10"]),
            Frame::SimpleString("OK".into())
        );
        let frame = redis.execute(&[b"TOPK.ADD".as_slice(), b"tk", b"alpha"]);
        assert!(matches!(frame, Frame::Array(_)));
    }
}
