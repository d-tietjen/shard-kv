use std::io;
use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{Receiver, TryRecvError};
use flume::{Receiver as AsyncFrameReceiver, Sender as AsyncFrameSender};
use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};

use crate::config::{WalTcpExportConfig, WalTcpExportMode};
use crate::monoio_runtime::MonoioRuntime;
use crate::{FastCacheError, Result};

use crate::persistence::stats::WalStats;

use super::{AUTH_LINE_MAX_LEN, AUTH_PREFIX, WalFrameBytes};

const USE_MONOIO_ENV: &str = "FAST_CACHE_WAL_TCP_USE_MONOIO";
const POLL_INTERVAL: Duration = Duration::from_micros(250);

pub(super) fn should_use() -> bool {
    MonoioRuntime::enabled_by_env(USE_MONOIO_ENV)
}

pub(super) fn spawn(
    config: WalTcpExportConfig,
    receiver: Receiver<WalFrameBytes>,
    stop_rx: Receiver<()>,
    stats: Arc<WalStats>,
) -> Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("fast-cache-wal-tcp-export-monoio".into())
        .spawn(move || {
            let result = MonoioRuntime::block_on("WAL TCP export", || async move {
                MonoioTcpWalExporter::new(config, receiver, stop_rx, stats)
                    .run()
                    .await
            });
            if let Err(error) = result {
                tracing::error!("monoio TCP WAL exporter failed: {error}");
            }
        })
        .map_err(|error| {
            FastCacheError::Persistence(format!("failed to start monoio TCP WAL exporter: {error}"))
        })
}

struct MonoioTcpWalExporter {
    config: WalTcpExportConfig,
    receiver: Receiver<WalFrameBytes>,
    stop_rx: Receiver<()>,
    stats: Arc<WalStats>,
    stream: Option<monoio::net::TcpStream>,
    stopping: bool,
}

struct MonoioSubscriber {
    tx: AsyncFrameSender<WalFrameBytes>,
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
    Connected(monoio::net::TcpStream),
    Failed,
    Stopped,
}

enum ListenerBind {
    Ready(monoio::net::TcpListener),
    Failed,
}

impl MonoioTcpWalExporter {
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

    async fn run(mut self) {
        match self.config.mode {
            WalTcpExportMode::Connect => self.run_connect().await,
            WalTcpExportMode::Listen => self.run_listen().await,
        }
    }

    async fn run_connect(&mut self) {
        while self.is_running() {
            match self.poll_frame() {
                ExportFrame::Ready(frame) => self.export_frame_to_collector(frame).await,
                ExportFrame::Idle => monoio::time::sleep(POLL_INTERVAL).await,
                ExportFrame::Closed => self.stop(),
            }
        }
    }

    async fn run_listen(&mut self) {
        match self.bind_listener() {
            ListenerBind::Ready(listener) => self.run_bound_listener(listener).await,
            ListenerBind::Failed => {}
        }
    }

    async fn run_bound_listener(&mut self, listener: monoio::net::TcpListener) {
        let mut subscribers = Vec::new();
        while self.is_running() {
            monoio::select! {
                accepted = listener.accept() => self.accept_one(accepted, &mut subscribers).await,
                _ = monoio::time::sleep(POLL_INTERVAL) => {
                    self.broadcast_available(&mut subscribers).await;
                    self.record_active_subscribers(subscribers.len());
                }
            }
        }
        self.record_active_subscribers(0);
    }

    async fn accept_one(
        &mut self,
        accepted: io::Result<(monoio::net::TcpStream, SocketAddr)>,
        subscribers: &mut Vec<MonoioSubscriber>,
    ) {
        match accepted {
            Ok((stream, peer)) if subscribers.len() >= self.config.max_subscribers => {
                tracing::warn!("rejecting TCP WAL subscriber {peer}: subscriber limit hit");
                drop(stream);
                self.record_subscriber_rejected();
            }
            Ok((stream, peer)) => match self.prepare_subscriber(stream).await {
                Ok(stream) => {
                    tracing::debug!("accepted monoio TCP WAL subscriber {peer}");
                    subscribers.push(MonoioSubscriber::spawn(
                        stream,
                        Arc::clone(&self.stats),
                        self.config.channel_capacity.max(1),
                    ));
                    self.record_subscriber_accepted(subscribers.len());
                }
                Err(error) => {
                    tracing::warn!("rejected monoio TCP WAL subscriber {peer}: {error}");
                    self.record_subscriber_rejected();
                }
            },
            Err(error) => tracing::warn!("monoio TCP WAL export accept failed: {error}"),
        }
    }

    async fn broadcast_available(&mut self, subscribers: &mut Vec<MonoioSubscriber>) {
        loop {
            match self.poll_frame() {
                ExportFrame::Ready(frame) => self.broadcast_frame(subscribers, frame),
                ExportFrame::Idle => break,
                ExportFrame::Closed => {
                    self.stop();
                    break;
                }
            }
        }
    }

    fn broadcast_frame(&self, subscribers: &mut Vec<MonoioSubscriber>, frame: WalFrameBytes) {
        let mut rejected = 0u64;
        subscribers.retain(|subscriber| match subscriber.tx.try_send(frame.clone()) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!("dropping monoio TCP WAL subscriber after queue failure: {error}");
                rejected = rejected.saturating_add(1);
                false
            }
        });
        if rejected > 0 {
            self.stats.record_tcp_export_write_failures(rejected);
            self.stats
                .set_tcp_export_active_subscribers(subscribers.len());
        }
    }

    async fn export_frame_to_collector(&mut self, frame: WalFrameBytes) {
        let mut sent = false;
        while self.is_running() && !sent {
            sent = match self.ensure_collector_connected().await {
                CollectorConnection::Ready => {
                    match self.write_frame_to_collector(frame.clone()).await {
                        CollectorWrite::Sent => true,
                        CollectorWrite::Retry => {
                            self.sleep_backoff().await;
                            false
                        }
                    }
                }
                CollectorConnection::Retry => {
                    self.sleep_backoff().await;
                    false
                }
            };
        }
    }

    async fn ensure_collector_connected(&mut self) -> CollectorConnection {
        match self.stream.is_some() || self.connect().await {
            true => CollectorConnection::Ready,
            false => CollectorConnection::Retry,
        }
    }

    async fn connect(&mut self) -> bool {
        match self.resolve_collector_addrs() {
            Ok(addrs) => self.connect_first_available(addrs).await,
            Err(error) => {
                self.record_resolution_failure(error);
                false
            }
        }
    }

    async fn connect_first_available(&mut self, addrs: Vec<SocketAddr>) -> bool {
        let mut connected = false;
        for addr in addrs {
            match self.connect_attempt(addr).await {
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

    async fn connect_attempt(&mut self, addr: SocketAddr) -> ConnectAttempt {
        match self.is_running() {
            false => ConnectAttempt::Stopped,
            true => match self.connect_addr(addr).await {
                Ok(stream) => ConnectAttempt::Connected(stream),
                Err(error) => {
                    tracing::warn!("monoio TCP WAL export connect failed to {addr}: {error}");
                    self.record_connect_failure();
                    ConnectAttempt::Failed
                }
            },
        }
    }

    async fn connect_addr(&self, addr: SocketAddr) -> io::Result<monoio::net::TcpStream> {
        let timeout = Duration::from_millis(self.config.connect_timeout_ms);
        let stream = monoio::time::timeout(timeout, monoio::net::TcpStream::connect_addr(addr))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
        stream.set_nodelay(true)?;
        self.write_auth(stream).await
    }

    async fn write_auth(
        &self,
        mut stream: monoio::net::TcpStream,
    ) -> io::Result<monoio::net::TcpStream> {
        if let Some(token) = self.config.auth_token.as_deref() {
            let mut bytes = Vec::with_capacity(AUTH_PREFIX.len() + token.len() + 1);
            bytes.extend_from_slice(AUTH_PREFIX);
            bytes.extend_from_slice(token.as_bytes());
            bytes.push(b'\n');
            write_all_owned(&mut stream, bytes).await?;
        }
        Ok(stream)
    }

    async fn prepare_subscriber(
        &self,
        stream: monoio::net::TcpStream,
    ) -> io::Result<monoio::net::TcpStream> {
        stream.set_nodelay(true)?;
        match self.config.auth_token.as_deref() {
            None => Ok(stream),
            Some(token) => {
                let timeout = Duration::from_millis(self.config.connect_timeout_ms);
                match monoio::time::timeout(timeout, AuthLine::read(stream)).await {
                    Ok(Ok((stream, line))) if line.matches_token(token) => Ok(stream),
                    Ok(Ok((_stream, _line))) => Err(Self::invalid_auth_error()),
                    Ok(Err((_stream, error))) => Err(error),
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "WAL auth timed out",
                    )),
                }
            }
        }
    }

    async fn write_frame_to_collector(&mut self, frame: WalFrameBytes) -> CollectorWrite {
        let result = match self.stream.as_mut() {
            Some(stream) => write_all_owned(stream, frame).await,
            None => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "collector is not connected",
            )),
        };
        match result {
            Ok(bytes) => {
                self.stats.record_tcp_export_sent(1, bytes);
                CollectorWrite::Sent
            }
            Err(error) => {
                tracing::warn!("monoio TCP WAL export write failed: {error}");
                self.stream = None;
                self.stats.record_tcp_export_write_failures(1);
                CollectorWrite::Retry
            }
        }
    }

    fn bind_listener(&self) -> ListenerBind {
        match TcpListener::bind(&self.config.addr) {
            Ok(listener) => match listener.set_nonblocking(true) {
                Ok(()) => match monoio::net::TcpListener::from_std(listener) {
                    Ok(listener) => ListenerBind::Ready(listener),
                    Err(error) => {
                        tracing::error!(
                            "failed to convert TCP WAL export listener to monoio: {error}"
                        );
                        ListenerBind::Failed
                    }
                },
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

    fn poll_frame(&self) -> ExportFrame {
        match self.receiver.try_recv() {
            Ok(frame) => ExportFrame::Ready(frame),
            Err(TryRecvError::Empty) => ExportFrame::Idle,
            Err(TryRecvError::Disconnected) => ExportFrame::Closed,
        }
    }

    fn resolve_collector_addrs(&self) -> io::Result<Vec<SocketAddr>> {
        self.config
            .addr
            .to_socket_addrs()
            .map(|addrs| addrs.collect())
    }

    fn record_resolution_failure(&self, error: io::Error) {
        tracing::warn!(
            "monoio TCP WAL export address resolution failed for {}: {error}",
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

    async fn sleep_backoff(&mut self) {
        let backoff = Duration::from_millis(self.config.reconnect_backoff_ms);
        let step = Duration::from_millis(10);
        let mut slept = Duration::ZERO;
        while slept < backoff && self.is_running() {
            let sleep_for = backoff.saturating_sub(slept).min(step);
            monoio::time::sleep(sleep_for).await;
            slept = slept.saturating_add(sleep_for);
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

impl MonoioSubscriber {
    fn spawn(stream: monoio::net::TcpStream, stats: Arc<WalStats>, capacity: usize) -> Self {
        let (tx, rx) = flume::bounded(capacity);
        monoio::spawn(async move {
            SubscriberWriter::new(stream, rx, stats).run().await;
        });
        Self { tx }
    }
}

struct SubscriberWriter {
    stream: monoio::net::TcpStream,
    rx: AsyncFrameReceiver<WalFrameBytes>,
    stats: Arc<WalStats>,
}

impl SubscriberWriter {
    fn new(
        stream: monoio::net::TcpStream,
        rx: AsyncFrameReceiver<WalFrameBytes>,
        stats: Arc<WalStats>,
    ) -> Self {
        Self { stream, rx, stats }
    }

    async fn run(&mut self) {
        while let Ok(frame) = self.rx.recv_async().await {
            let len = frame.len() as u64;
            match write_all_owned(&mut self.stream, frame).await {
                Ok(_) => {
                    self.stats.record_tcp_export_sent(1, len);
                }
                Err(error) => {
                    tracing::warn!("monoio TCP WAL subscriber write failed: {error}");
                    self.stats.record_tcp_export_write_failures(1);
                    break;
                }
            }
        }
    }
}

struct AuthLine {
    bytes: Vec<u8>,
}

impl AuthLine {
    async fn read(
        mut stream: monoio::net::TcpStream,
    ) -> std::result::Result<(monoio::net::TcpStream, Self), (monoio::net::TcpStream, io::Error)>
    {
        let mut bytes = Vec::new();
        loop {
            match Self::read_next_byte(&mut stream, bytes.len()).await {
                Ok(b'\n') => break Ok((stream, Self::from_line_bytes(bytes))),
                Ok(byte) => bytes.push(byte),
                Err(error) => break Err((stream, error)),
            }
        }
    }

    async fn read_next_byte(
        stream: &mut monoio::net::TcpStream,
        current_len: usize,
    ) -> io::Result<u8> {
        match current_len >= AUTH_LINE_MAX_LEN {
            true => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL auth line is too long",
            )),
            false => read_one_byte(stream).await,
        }
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

async fn read_one_byte(stream: &mut monoio::net::TcpStream) -> io::Result<u8> {
    let (result, buffer) = stream.read_exact(vec![0_u8; 1]).await;
    result.map(|_| buffer[0])
}

async fn write_all_owned<T>(stream: &mut monoio::net::TcpStream, buffer: T) -> io::Result<u64>
where
    T: monoio::buf::IoBuf + 'static,
{
    let expected = buffer.bytes_init() as u64;
    let (result, _buffer) = stream.write_all(buffer).await;
    result.map(|_| expected)
}
