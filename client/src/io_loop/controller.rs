use std::net::ToSocketAddrs;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;

use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token, Waker};
use protocol::generated::{ApiVersionsRequest, ApiVersionsResponse, ResponseHeader};
use protocol::traits::{ApiResponse, ApiVersion};

use super::connection::Connection;
use super::lifecycle_state::LifecycleState;
use super::metadata::MetadataCache;
use super::sender::CommandSender;
use crate::cluster::ClusterOptions;

/// Token used for the wakeup fd (eventfd/pipe) to interrupt poll.
const WAKEUP_TOKEN: Token = Token(0);

pub enum Command {
    Shutdown,
}

pub struct EventLoop {
    options: ClusterOptions,
    cmd_rx: mpsc::Receiver<Command>,
    poll: Poll,
    connections: Vec<Connection>,
    _metadata_cache: MetadataCache,
    lifecycle: LifecycleState,
    next_token: usize,
}

impl EventLoop {
    pub fn new(options: ClusterOptions) -> (Self, CommandSender, LifecycleState) {
        let poll = Poll::new().expect("failed to create mio Poll");
        let (tx, rx) = mpsc::channel();

        let waker = Waker::new(poll.registry(), WAKEUP_TOKEN).expect("failed to create mio Waker");

        let cmd_tx = CommandSender::new(tx, Arc::new(waker));
        let lifecycle = LifecycleState::new();

        let event_loop = EventLoop {
            options,
            cmd_rx: rx,
            poll,
            connections: Vec::new(),
            _metadata_cache: MetadataCache::new(),
            lifecycle: lifecycle.clone(),
            next_token: 1, // 0 is reserved for WAKEUP_TOKEN
        };

        (event_loop, cmd_tx, lifecycle)
    }

    pub fn run(&mut self) {
        self.bootstrap_cluster_connection();
        self.lifecycle.set_active();

        let mut events = Events::with_capacity(1024);

        while self.lifecycle.state() != super::State::ShutdownComplete {
            // Block indefinitely — the waker will interrupt poll when a command arrives.
            if let Err(e) = self.poll.poll(&mut events, None) {
                match e.kind() {
                    std::io::ErrorKind::Interrupted => {
                        // EINTR — signal was delivered while blocking. Retry.
                        continue;
                    }
                    _ => {
                        tracing::error!("poll error: {e}");
                        break;
                    }
                }
            }

            // Drain the wakeup fd (just read the event, no action needed).
            // Then process I/O events from broker sockets.
            for event in &events {
                let token = event.token();
                if token == WAKEUP_TOKEN {
                    continue;
                }

                if let Some(conn) = self.connections.iter_mut().find(|c| c.token() == token) {
                    if event.is_readable() {
                        let _ = conn.on_readable();
                    }
                    if event.is_writable() {
                        if let Err(e) = conn.on_writable() {
                            tracing::error!("write error on connection {}: {e}", conn.token().0);
                        }
                    }
                }
            }

            // Drain commands from the channel
            self.drain_commands();

            // Tick state machines
            self.tick();
        }
    }

    fn drain_commands(&mut self) {
        while let Ok(cmd) = self.cmd_rx.try_recv() {
            match cmd {
                Command::Shutdown => {
                    self.lifecycle.set_shutdown_triggered();
                    // TODO: graceful close sequence (flush pending sends, leave group, etc.)
                    self.lifecycle.set_shutdown_complete();
                }
            }
        }
    }

    fn bootstrap_cluster_connection(&mut self) {
        let timeout = self.options.connection_timeout;
        let retry_delay = self.options.connection_retry_delay;

        loop {
            // Re-resolve on each attempt so DNS changes are picked up.
            let addrs: Vec<_> = self
                .options
                .bootstrap_servers
                .iter()
                .flat_map(|s| {
                    s.to_socket_addrs()
                        .inspect_err(|e| tracing::warn!("failed to resolve {s}: {e}"))
                        .unwrap_or_default()
                })
                .collect();

            let mut candidates: Vec<(TcpStream, Token)> = Vec::new();

            for addr in &addrs {
                let token = Token(self.next_token);
                self.next_token += 1;

                if let Ok(mut stream) = TcpStream::connect(*addr)
                    && self
                        .poll
                        .registry()
                        .register(&mut stream, token, Interest::WRITABLE | Interest::READABLE)
                        .is_ok()
                {
                    candidates.push((stream, token));
                }
            }

            if candidates.is_empty() {
                tracing::warn!(
                    "no bootstrap addresses could be resolved, retrying in {retry_delay:?}"
                );
                thread::sleep(retry_delay);
                continue;
            }

            let deadline = Instant::now() + timeout;
            let mut poll_events = Events::with_capacity(candidates.len());

            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }

                if let Err(e) = self.poll.poll(&mut poll_events, Some(remaining)) {
                    match e.kind() {
                        std::io::ErrorKind::Interrupted => continue,
                        _ => {
                            tracing::error!("poll error during bootstrap: {e}");
                            break;
                        }
                    }
                }

                for event in &poll_events {
                    if event.is_writable() {
                        let token = event.token();
                        if let Some(pos) = candidates.iter().position(|(_, t)| *t == token) {
                            let (stream, _) = candidates.swap_remove(pos);

                            let mut conn = Connection::new(
                                token,
                                stream,
                                Some(self.options.client_name.clone()),
                            );

                            if let Err(reason) = self.fetch_api_versions(&mut conn, token) {
                                let addr = conn
                                    .stream()
                                    .peer_addr()
                                    .map_or_else(|_| "unknown".to_string(), |a| a.to_string());
                                tracing::warn!(
                                    "failed to fetch api versions from {addr}: {reason}"
                                );
                                let _ = self.poll.registry().deregister(conn.stream());
                                continue;
                            }

                            // Success — close remaining candidates, keep this one.
                            for (mut other, _) in candidates.drain(..) {
                                let _ = self.poll.registry().deregister(&mut other);
                            }
                            let addr = conn
                                .stream()
                                .peer_addr()
                                .map_or("unknown".to_string(), |a| a.to_string());
                            self.connections.push(conn);
                            tracing::info!("connected to bootstrap broker {addr}");
                            return;
                        }
                    }
                }
            }

            // All candidates exhausted — deregister, sleep, retry.
            for (mut other, _) in candidates.drain(..) {
                let _ = self.poll.registry().deregister(&mut other);
            }

            tracing::warn!(
                "no bootstrap connection within {timeout:?}, retrying in {retry_delay:?}"
            );
            thread::sleep(retry_delay);
        }
    }

    fn fetch_api_versions(&mut self, conn: &mut Connection, token: Token) -> Result<(), String> {
        let deadline = Instant::now() + self.options.request_timeout;
        let version = ApiVersion::new(0);

        conn.send_api_request(&ApiVersionsRequest::default(), Some(version))
            .map_err(|e| format!("serialize: {e}"))?;

        let mut poll_events = Events::with_capacity(1);
        while conn.can_write() {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err("write timed out".into());
            }
            let _ = self.poll.poll(&mut poll_events, Some(rem));
            if poll_events
                .iter()
                .any(|e| e.token() == token && e.is_writable())
            {
                let _ = conn.on_writable();
            }
        }

        loop {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err("read timed out".into());
            }
            let _ = self.poll.poll(&mut poll_events, Some(rem));
            if poll_events
                .iter()
                .any(|e| e.token() == token && e.is_readable())
            {
                match conn.on_readable() {
                    Ok(0) => return Err("connection closed".into()),
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(format!("read error: {e}")),
                }
                if let Some((_, mut body)) = conn.read_broker_response() {
                    if ResponseHeader::decode(&mut body, false).is_err() {
                        return Err("invalid response header".into());
                    }
                    match ApiVersionsResponse::deserialize(version, &mut body) {
                        Ok(resp) if resp.error_code == 0 => {
                            conn.set_api_versions(resp.api_keys);
                            return Ok(());
                        }
                        Ok(resp) => return Err(format!("broker error: {}", resp.error_code)),
                        Err(e) => return Err(format!("decode response: {e}")),
                    }
                }
            }
        }
    }

    fn tick(&mut self) {
        // TODO: tick state machines
    }
}
