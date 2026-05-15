use crate::cluster::ClusterOptions;

use std::sync::mpsc;
use std::sync::Arc;

use mio::{Events, Poll, Token, Waker};

use super::connection::Connection;
use super::metadata::MetadataCache;
use super::sender::CommandSender;

/// Token used for the wakeup fd (eventfd/pipe) to interrupt poll.
const WAKEUP_TOKEN: Token = Token(0);

pub(crate) enum Command {
    Shutdown,
}

#[derive(PartialEq, Eq)]
pub(crate) enum LifecycleState {
    Active,
    ShutdownInitialized,
    ShutdownComplete,
}

pub(crate) struct EventLoop {
    options: ClusterOptions,
    cmd_rx: mpsc::Receiver<Command>,
    poll: Poll,
    connections: Vec<Connection>,
    metadata_cache: MetadataCache,
    lifecycle: LifecycleState,
}

impl EventLoop {
    pub(crate) fn new(options: ClusterOptions) -> (Self, CommandSender) {
        let poll = Poll::new().expect("failed to create mio Poll");
        let (tx, rx) = mpsc::channel();

        let waker = Waker::new(poll.registry(), WAKEUP_TOKEN).expect("failed to create mio Waker");

        let cmd_tx = CommandSender::new(tx, Arc::new(waker));
        let event_loop = EventLoop {
            options,
            cmd_rx: rx,
            poll,
            connections: Vec::new(),
            metadata_cache: MetadataCache::new(),
            lifecycle: LifecycleState::Active,
        };

        (event_loop, cmd_tx)
    }

    pub(crate) fn run(&mut self) {
        let mut events = Events::with_capacity(1024);

        while self.lifecycle != LifecycleState::ShutdownComplete {
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
                    self.lifecycle = LifecycleState::ShutdownComplete;
                    // TODO: graceful close sequence (flush pending sends, leave group, etc.)
                }
            }
        }
    }

    fn tick(&mut self) {
        // TODO: tick state machines
    }
}
