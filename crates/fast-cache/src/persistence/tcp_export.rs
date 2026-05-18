use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError};

use crate::config::{WalTcpExportConfig, WalTcpExportMode};
use crate::{FastCacheError, Result};

use super::{WalFrameBytes, stats::WalStats};

#[cfg(all(target_os = "linux", feature = "monoio"))]
mod monoio_export;

const AUTH_PREFIX: &[u8] = b"FCWAL-AUTH/1 ";
const AUTH_LINE_MAX_LEN: usize = 1024;

pub(super) fn spawn(
    config: WalTcpExportConfig,
    receiver: Receiver<WalFrameBytes>,
    stop_rx: Receiver<()>,
    stats: Arc<WalStats>,
) -> Result<JoinHandle<()>> {
    #[cfg(all(target_os = "linux", feature = "monoio"))]
    if monoio_export::should_use() {
        return monoio_export::spawn(config, receiver, stop_rx, stats);
    }

    thread::Builder::new()
        .name("fast-cache-wal-tcp-export".into())
        .spawn(move || TcpWalExporter::new(config, receiver, stop_rx, stats).run())
        .map_err(|error| {
            FastCacheError::Persistence(format!("failed to start TCP WAL exporter: {error}"))
        })
}

struct TcpWalExporter {
    config: WalTcpExportConfig,
    receiver: Receiver<WalFrameBytes>,
    stop_rx: Receiver<()>,
    stats: Arc<WalStats>,
    stream: Option<TcpStream>,
    stopping: bool,
}

struct Subscriber {
    stream: TcpStream,
}

enum ExportFrame {
    Ready(WalFrameBytes),
    Idle,
    Closed,
}

enum CollectorConnection {
    Ready,
    Retry,
}

enum CollectorWrite {
    Sent,
    Retry,
}

enum ConnectAttempt {
    Connected(TcpStream),
    Failed,
    Stopped,
}

enum AcceptOutcome {
    Accepted { stream: TcpStream, peer: SocketAddr },
    Rejected,
    Empty,
    Failed,
}

enum ListenerBind {
    Ready(TcpListener),
    Failed,
}

struct AuthLine {
    bytes: Vec<u8>,
}

impl TcpWalExporter {
    fn new(
        config: WalTcpExportConfig,
        receiver: Receiver<WalFrameBytes>,
        stop_rx: Receiver<()>,
        stats: Arc<WalStats>,
    ) -> Self {
        Self {
            config,
            receiver,
            stop_rx,
            stats,
            stream: None,
            stopping: false,
        }
    }

    fn run(mut self) {
        match self.config.mode {
            WalTcpExportMode::Connect => self.run_connect(),
            WalTcpExportMode::Listen => self.run_listen(),
        }
    }

    fn run_connect(&mut self) {
        while self.is_running() {
            match self.recv_frame(Duration::from_millis(50)) {
                ExportFrame::Ready(frame) => self.export_frame_to_collector(frame),
                ExportFrame::Idle => {}
                ExportFrame::Closed => self.stop(),
            }
        }
    }

    fn run_listen(&mut self) {
        match self.bind_listener() {
            ListenerBind::Ready(listener) => self.run_bound_listener(listener),
            ListenerBind::Failed => {}
        }
    }

    fn run_bound_listener(&mut self, listener: TcpListener) {
        let mut subscribers = Vec::new();
        while self.is_running() {
            self.accept_available(&listener, &mut subscribers);

            match self.recv_frame(Duration::from_millis(10)) {
                ExportFrame::Ready(frame) => self.broadcast_frame(&mut subscribers, &frame),
                ExportFrame::Idle => {}
                ExportFrame::Closed => self.stop(),
            }

            self.record_active_subscribers(subscribers.len());
        }
        self.record_active_subscribers(0);
    }

    fn export_frame_to_collector(&mut self, frame: WalFrameBytes) {
        let mut sent = false;
        while self.is_running() && !sent {
            sent = match self.ensure_collector_connected() {
                CollectorConnection::Ready => match self.write_frame_to_collector(frame.as_ref()) {
                    CollectorWrite::Sent => true,
                    CollectorWrite::Retry => {
                        self.sleep_backoff();
                        false
                    }
                },
                CollectorConnection::Retry => {
                    self.sleep_backoff();
                    false
                }
            };
        }
    }

    fn connect(&mut self) -> bool {
        match self.resolve_collector_addrs() {
            Ok(addrs) => self.connect_first_available(addrs),
            Err(error) => {
                self.record_resolution_failure(error);
                false
            }
        }
    }

    fn connect_addr(&self, addr: SocketAddr) -> io::Result<TcpStream> {
        let timeout = Duration::from_millis(self.config.connect_timeout_ms);
        let mut stream = TcpStream::connect_timeout(&addr, timeout)?;
        stream.set_nodelay(true)?;
        stream.set_write_timeout(Some(Duration::from_millis(self.config.write_timeout_ms)))?;
        self.write_auth(&mut stream)?;
        Ok(stream)
    }

    fn accept_available(&mut self, listener: &TcpListener, subscribers: &mut Vec<Subscriber>) {
        loop {
            match self.accept_next(listener, subscribers.len()) {
                AcceptOutcome::Accepted { stream, peer } => {
                    tracing::debug!("accepted TCP WAL subscriber {peer}");
                    subscribers.push(Subscriber { stream });
                    self.record_subscriber_accepted(subscribers.len());
                }
                AcceptOutcome::Rejected => {}
                AcceptOutcome::Empty | AcceptOutcome::Failed => break,
            }
        }
    }

    fn prepare_subscriber(&self, mut stream: TcpStream) -> io::Result<TcpStream> {
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_millis(self.config.connect_timeout_ms)))?;
        stream.set_write_timeout(Some(Duration::from_millis(self.config.write_timeout_ms)))?;
        self.verify_auth(&mut stream)?;
        stream.set_read_timeout(None)?;
        Ok(stream)
    }

    fn broadcast_frame(&mut self, subscribers: &mut Vec<Subscriber>, frame: &[u8]) {
        let mut delivered = 0u64;
        let mut bytes_sent = 0u64;
        let mut failures = 0u64;

        subscribers.retain_mut(|subscriber| match subscriber.stream.write_all(frame) {
            Ok(()) => {
                delivered = delivered.saturating_add(1);
                bytes_sent = bytes_sent.saturating_add(frame.len() as u64);
                true
            }
            Err(error) => {
                tracing::warn!("dropping TCP WAL subscriber after write failure: {error}");
                failures = failures.saturating_add(1);
                false
            }
        });

        self.stats.record_tcp_export_sent(delivered, bytes_sent);
        self.stats.record_tcp_export_write_failures(failures);
        self.stats
            .set_tcp_export_active_subscribers(subscribers.len());
    }

    fn write_auth(&self, stream: &mut TcpStream) -> io::Result<()> {
        if let Some(token) = self.config.auth_token.as_deref() {
            stream.write_all(AUTH_PREFIX)?;
            stream.write_all(token.as_bytes())?;
            stream.write_all(b"\n")?;
        }
        Ok(())
    }

    fn verify_auth(&self, stream: &mut TcpStream) -> io::Result<()> {
        match self.config.auth_token.as_deref() {
            None => Ok(()),
            Some(token) => match AuthLine::read(stream)?.matches_token(token) {
                true => Ok(()),
                false => Err(Self::invalid_auth_error()),
            },
        }
    }

    fn bind_listener(&self) -> ListenerBind {
        match TcpListener::bind(&self.config.addr) {
            Ok(listener) => match listener.set_nonblocking(true) {
                Ok(()) => ListenerBind::Ready(listener),
                Err(error) => {
                    tracing::error!("failed to set TCP WAL export listener nonblocking: {error}");
                    ListenerBind::Failed
                }
            },
            Err(error) => {
                tracing::error!(
                    "failed to bind TCP WAL export listener {}: {error}",
                    self.config.addr
                );
                ListenerBind::Failed
            }
        }
    }

    fn recv_frame(&self, timeout: Duration) -> ExportFrame {
        match self.receiver.recv_timeout(timeout) {
            Ok(frame) => ExportFrame::Ready(frame),
            Err(RecvTimeoutError::Timeout) => ExportFrame::Idle,
            Err(RecvTimeoutError::Disconnected) => ExportFrame::Closed,
        }
    }

    fn ensure_collector_connected(&mut self) -> CollectorConnection {
        match self.stream.is_some() || self.connect() {
            true => CollectorConnection::Ready,
            false => CollectorConnection::Retry,
        }
    }

    fn write_frame_to_collector(&mut self, frame: &[u8]) -> CollectorWrite {
        match self
            .stream
            .as_mut()
            .expect("stream was just connected")
            .write_all(frame)
        {
            Ok(()) => {
                self.stats.record_tcp_export_sent(1, frame.len() as u64);
                CollectorWrite::Sent
            }
            Err(error) => {
                tracing::warn!("TCP WAL export write failed: {error}");
                self.stream = None;
                self.stats.record_tcp_export_write_failures(1);
                CollectorWrite::Retry
            }
        }
    }

    fn resolve_collector_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        self.config
            .addr
            .to_socket_addrs()
            .map(|addrs| addrs.collect())
    }

    fn connect_first_available(&mut self, addrs: Vec<SocketAddr>) -> bool {
        let mut connected = false;
        for addr in addrs {
            match self.connect_attempt(addr) {
                ConnectAttempt::Connected(stream) => {
                    self.stream = Some(stream);
                    connected = true;
                    break;
                }
                ConnectAttempt::Failed => {}
                ConnectAttempt::Stopped => break,
            }
        }
        connected
    }

    fn connect_attempt(&mut self, addr: SocketAddr) -> ConnectAttempt {
        match self.is_running() {
            false => ConnectAttempt::Stopped,
            true => match self.connect_addr(addr) {
                Ok(stream) => ConnectAttempt::Connected(stream),
                Err(error) => {
                    tracing::warn!("TCP WAL export connect failed to {addr}: {error}");
                    self.record_connect_failure();
                    ConnectAttempt::Failed
                }
            },
        }
    }

    fn accept_next(&mut self, listener: &TcpListener, active: usize) -> AcceptOutcome {
        match listener.accept() {
            Ok((_, peer)) if active >= self.config.max_subscribers => {
                tracing::warn!("rejecting TCP WAL subscriber {peer}: subscriber limit hit");
                self.record_subscriber_rejected();
                AcceptOutcome::Rejected
            }
            Ok((stream, peer)) => match self.prepare_subscriber(stream) {
                Ok(stream) => AcceptOutcome::Accepted { stream, peer },
                Err(error) => {
                    tracing::warn!("rejected TCP WAL subscriber {peer}: {error}");
                    self.record_subscriber_rejected();
                    AcceptOutcome::Rejected
                }
            },
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => AcceptOutcome::Empty,
            Err(error) => {
                tracing::warn!("TCP WAL export accept failed: {error}");
                AcceptOutcome::Failed
            }
        }
    }

    fn record_resolution_failure(&self, error: io::Error) {
        tracing::warn!(
            "TCP WAL export address resolution failed for {}: {error}",
            self.config.addr
        );
        self.record_connect_failure();
    }

    fn record_connect_failure(&self) {
        self.stats.record_tcp_export_connect_failure();
    }

    fn invalid_auth_error() -> io::Error {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid WAL subscriber auth token",
        )
    }

    fn record_subscriber_accepted(&self, active: usize) {
        self.stats.record_tcp_export_subscriber_accepted(active);
    }

    fn record_subscriber_rejected(&self) {
        self.stats.record_tcp_export_subscriber_rejected();
    }

    fn record_active_subscribers(&self, active: usize) {
        self.stats.set_tcp_export_active_subscribers(active);
    }

    fn sleep_backoff(&mut self) {
        let backoff = Duration::from_millis(self.config.reconnect_backoff_ms);
        let step = Duration::from_millis(10);
        let mut slept = Duration::ZERO;
        while slept < backoff {
            match self.is_running() {
                true => {
                    let remaining = backoff.saturating_sub(slept);
                    let sleep_for = remaining.min(step);
                    thread::sleep(sleep_for);
                    slept = slept.saturating_add(sleep_for);
                }
                false => break,
            }
        }
    }

    fn is_running(&mut self) -> bool {
        !self.should_stop()
    }

    fn stop(&mut self) {
        self.stopping = true;
    }

    fn should_stop(&mut self) -> bool {
        match self.stopping {
            true => true,
            false => match self.stop_rx.try_recv() {
                Ok(()) => {
                    self.stop();
                    true
                }
                Err(_) => false,
            },
        }
    }
}

impl AuthLine {
    fn read(stream: &mut TcpStream) -> io::Result<Self> {
        let mut bytes = Vec::new();
        loop {
            match Self::read_next_byte(stream, bytes.len())? {
                b'\n' => break Ok(Self::from_line_bytes(bytes)),
                byte => bytes.push(byte),
            }
        }
    }

    fn read_next_byte(stream: &mut TcpStream, current_len: usize) -> io::Result<u8> {
        match current_len >= AUTH_LINE_MAX_LEN {
            true => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL auth line is too long",
            )),
            false => Self::read_byte(stream),
        }
    }

    fn read_byte(stream: &mut TcpStream) -> io::Result<u8> {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        Ok(byte[0])
    }

    fn from_line_bytes(mut bytes: Vec<u8>) -> Self {
        match bytes.last() {
            Some(b'\r') => {
                bytes.pop();
                Self { bytes }
            }
            _ => Self { bytes },
        }
    }

    fn matches_token(&self, token: &str) -> bool {
        match self.bytes.strip_prefix(AUTH_PREFIX) {
            Some(value) => value == token.as_bytes(),
            None => false,
        }
    }
}
