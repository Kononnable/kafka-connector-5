use std::sync::{Arc, mpsc};

use mio::{Events, Poll, Token, Waker};

use super::connection::Connection;
use super::lifecycle_state::LifecycleState;
use super::metadata::MetadataCache;
use super::sender::CommandSender;
use crate::cluster::ClusterOptions;

/// Token used for the wakeup fd (eventfd/pipe) to interrupt poll.
const WAKEUP_TOKEN: Token = Token(0);

pub(crate) enum Command {
    Shutdown,
}

pub(crate) struct EventLoop {
    _options: ClusterOptions,
    cmd_rx: mpsc::Receiver<Command>,
    poll: Poll,
    connections: Vec<Connection>,
    _metadata_cache: MetadataCache,
    lifecycle: LifecycleState,
}

impl EventLoop {
    pub(crate) fn new(options: ClusterOptions) -> (Self, CommandSender, LifecycleState) {
        let poll = Poll::new().expect("failed to create mio Poll");
        let (tx, rx) = mpsc::channel();

        let waker = Waker::new(poll.registry(), WAKEUP_TOKEN).expect("failed to create mio Waker");

        let cmd_tx = CommandSender::new(tx, Arc::new(waker));
        let lifecycle = LifecycleState::new();

        let event_loop = EventLoop {
            _options: options,
            cmd_rx: rx,
            poll,
            connections: Vec::new(),
            _metadata_cache: MetadataCache::new(),
            lifecycle: lifecycle.clone(),
        };

        (event_loop, cmd_tx, lifecycle)
    }

    pub(crate) fn run(&mut self) {
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
                        conn.on_readable();
                    }
                    if event.is_writable() {
                        conn.on_writable();
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

    fn tick(&mut self) {
        // TODO: tick state machines
    }
}
