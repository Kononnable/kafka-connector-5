use std::sync::{Arc, mpsc};

use mio::Events;

use super::lifecycle_state::{LifecycleState, State};
use super::sender::CommandSender;
use crate::cluster::ClusterOptions;
use crate::cluster::state::ClusterState;

pub enum Command {
    Shutdown,
}

pub struct EventLoop {
    pub(super) state: ClusterState,
    lifecycle: LifecycleState,
    cmd_rx: mpsc::Receiver<Command>,
}

impl EventLoop {
    pub fn new(options: ClusterOptions) -> (Self, CommandSender, LifecycleState) {
        let (state, waker) = ClusterState::new(options);
        let lifecycle = LifecycleState::new();
        let handle = lifecycle.clone();

        let (tx, rx) = mpsc::channel();
        let cmd_tx = CommandSender::new(tx, Arc::new(waker));

        let event_loop = EventLoop {
            state,
            lifecycle,
            cmd_rx: rx,
        };

        (event_loop, cmd_tx, handle)
    }

    pub fn run(&mut self) {
        self.state.bootstrap();
        self.lifecycle.set_active();

        let mut events = Events::with_capacity(1024);

        while self.lifecycle.state() != State::ShutdownComplete {
            if let Err(e) = self.state.pool.poll_io(&mut events, None) {
                match e.kind() {
                    std::io::ErrorKind::Interrupted => continue,
                    _ => {
                        tracing::error!("poll error: {e}");
                        break;
                    }
                }
            }

            let dead_broker_ids = self.state.prune_dead_connections(&events);
            self.state.connect_to_brokers(&dead_broker_ids);
            self.send_api_requests();
            self.process_commands();
            self.process_api_responses();
            self.tick();
        }
    }

    fn send_api_requests(&mut self) {
        for &(broker_id, corr_id, version) in &self.state.pool.send_api_requests() {
            let idx = self
                .state
                .pool
                .connections()
                .iter()
                .position(|c| c.node_id() == broker_id)
                .expect("broker connection must exist for just-flushed request");
            self.state
                .metadata
                .register_inflight_refresh(idx, corr_id, version);
        }
    }

    fn process_commands(&mut self) {
        while let Ok(cmd) = self.cmd_rx.try_recv() {
            match cmd {
                Command::Shutdown => {
                    self.lifecycle.set_shutdown_triggered();
                    self.lifecycle.set_shutdown_complete();
                }
            }
        }
    }

    fn process_api_responses(&mut self) {
        for (conn_idx, corr_id, body) in self.state.pool.collect_responses() {
            if !self.state.metadata.on_response(corr_id, conn_idx, body) {
                tracing::debug!(corr_id, "unhandled response (no state machine registered)");
            }
        }
    }

    fn tick(&mut self) {
        if let Some(req) = self.state.metadata.tick() {
            let _ = self.state.send(None, req, None);
        }
    }
}
