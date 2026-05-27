use std::sync::{Arc, mpsc};

use mio::{Events, Poll, Token, Waker};

use super::lifecycle_state::LifecycleState;
use super::sender::CommandSender;
use crate::cluster::ClusterOptions;
use crate::connection::ConnectionPool;
use crate::metadata::MetadataCache;

/// Token used for the wakeup fd (eventfd/pipe) to interrupt poll.
const WAKEUP_TOKEN: Token = Token(0);

pub enum Command {
    Shutdown,
}

pub struct EventLoop {
    pub(super) options: ClusterOptions,
    cmd_rx: mpsc::Receiver<Command>,
    pub(super) poll: Poll,
    pub(super) connections: ConnectionPool,
    pub(super) metadata_cache: MetadataCache,
    lifecycle: LifecycleState,
}

impl EventLoop {
    pub fn new(options: ClusterOptions) -> (Self, CommandSender, LifecycleState) {
        let poll = Poll::new().expect("failed to create mio Poll");
        let (tx, rx) = mpsc::channel();

        let waker = Waker::new(poll.registry(), WAKEUP_TOKEN).expect("failed to create mio Waker");

        let cmd_tx = CommandSender::new(tx, Arc::new(waker));
        let lifecycle = LifecycleState::new();

        let event_loop = EventLoop {
            metadata_cache: MetadataCache::new(options.metadata_refresh_interval),
            connections: ConnectionPool::new(Some(options.client_name.clone())),
            options,
            cmd_rx: rx,
            poll,
            lifecycle: lifecycle.clone(),
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

            // Process I/O events from broker sockets.
            for event in &events {
                let token = event.token();
                if token == WAKEUP_TOKEN {
                    continue;
                }

                if let Some(conn) = self.connections.find_by_token(token) {
                    if event.is_readable() {
                        let _ = conn.on_readable();
                    }
                    if event.is_writable()
                        && let Err(e) = conn.on_writable()
                    {
                        tracing::error!("write error on connection {}: {e}", conn.node_id());
                    }
                }
            }

            self.drain_commands();
            self.process_responses();
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

    fn process_responses(&mut self) {
        for (conn_idx, conn) in self.connections.iter_mut().enumerate() {
            while let Some((corr_id, body)) = conn.read_broker_response() {
                if !self.metadata_cache.on_response(corr_id, conn_idx, body) {
                    // TODO: dispatch to producer/consumer state machines.
                    tracing::debug!(corr_id, "unhandled response (no state machine registered)");
                }
            }
        }
    }

    fn tick(&mut self) {
        let registry = self.poll.registry();
        self.metadata_cache.tick(&mut self.connections, registry);
    }
}
