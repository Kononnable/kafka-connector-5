use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use std::time::Instant;

use bytes::Bytes;
use mio::Events;
use protocol::traits::ApiVersion;

use super::lifecycle_state::{LifecycleState, State};
use super::sender::CommandSender;
use crate::cluster::ClusterOptions;
use crate::cluster::state::ClusterState;
use crate::types::{BrokerId, CorrelationId, InflightRequest, RequestHandlerId};

pub(crate) const METADATA_HANDLER_ID: RequestHandlerId = RequestHandlerId(0);

pub enum Command {
    Shutdown,
    GetApiVersions {
        broker_id: Option<BrokerId>,
        reply: futures::channel::oneshot::Sender<
            Option<indexmap::IndexMap<i16, protocol::generated::api_versions_response::ApiVersion>>,
        >,
    },
}

/// A state machine that processes Kafka protocol logic on the event loop.
///
/// Each handler owns its own state (metadata cache, producer accumulator,
/// consumer fetch state) and enqueues requests via [`ClusterState::send`].
pub(crate) trait RequestHandler: Send {
    /// Called when a response arrives for a request this handler registered.
    fn on_response(
        &mut self,
        corr_id: CorrelationId,
        body: Bytes,
        version: ApiVersion,
        state: &mut ClusterState,
    );

    /// Called when an inflight request exceeds its deadline.
    fn on_timeout(
        &mut self,
        corr_id: CorrelationId,
        inflight: &InflightRequest,
        state: &mut ClusterState,
    );

    /// Periodic tick — e.g., check for stale metadata, flush accumulators.
    fn tick(&mut self, now: Instant, state: &mut ClusterState);
}

pub struct EventLoop {
    pub(super) state: ClusterState,
    lifecycle: LifecycleState,
    cmd_rx: mpsc::Receiver<Command>,
    handlers: HashMap<RequestHandlerId, Box<dyn RequestHandler>>,
}

impl EventLoop {
    pub fn new(options: ClusterOptions) -> (Self, CommandSender, LifecycleState) {
        let (state, waker) = ClusterState::new(options);
        let lifecycle = LifecycleState::new();
        let handle = lifecycle.clone();

        let (tx, rx) = mpsc::channel();
        let cmd_tx = CommandSender::new(tx, Arc::new(waker));

        let handlers: HashMap<RequestHandlerId, Box<dyn RequestHandler>> = HashMap::new();

        let event_loop = EventLoop {
            state,
            lifecycle,
            cmd_rx: rx,
            handlers,
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
            self.state.pool.send_api_requests();
            self.process_commands();
            self.process_api_responses();
            self.tick();
        }
    }

    fn process_commands(&mut self) {
        while let Ok(cmd) = self.cmd_rx.try_recv() {
            match cmd {
                Command::Shutdown => {
                    self.lifecycle.set_shutdown_triggered();
                    self.lifecycle.set_shutdown_complete();
                }
                Command::GetApiVersions { broker_id, reply } => {
                    let versions = match broker_id {
                        Some(id) => self
                            .state
                            .pool
                            .connections()
                            .iter()
                            .find(|c| c.node_id() == id)
                            .map(|conn| conn.api_versions().clone()),
                        None => self
                            .state
                            .pool
                            .connections()
                            .first()
                            .map(|conn| conn.api_versions().clone()),
                    };
                    let _ = reply.send(versions);
                }
            }
        }
    }

    fn process_api_responses(&mut self) {
        for (handler_id, corr_id, _api_key, version, body) in self.state.pool.collect_responses() {
            if handler_id == METADATA_HANDLER_ID {
                self.state.metadata.on_response(corr_id, body, version);
            } else if let Some(handler) = self.handlers.get_mut(&handler_id) {
                handler.on_response(corr_id, body, version, &mut self.state);
            } else {
                tracing::debug!(
                    handler_id = %handler_id,
                    corr_id = %corr_id,
                    "response for unknown handler"
                );
            }
        }
    }

    fn tick(&mut self) {
        let now = Instant::now();

        // 1. Drain expired inflight entries and notify handlers
        for (handler_id, corr_id, inflight) in self.state.pool.drain_expired_inflight() {
            if handler_id == METADATA_HANDLER_ID {
                self.state.metadata.on_timeout(corr_id, &inflight);
            } else if let Some(handler) = self.handlers.get_mut(&handler_id) {
                handler.on_timeout(corr_id, &inflight, &mut self.state);
            } else {
                tracing::debug!(
                    handler_id = %handler_id,
                    corr_id = %corr_id,
                    "timeout for unknown handler"
                );
            }
        }

        // 2. Tick metadata cache
        if let Some(req) = self.state.metadata.tick() {
            let _ = self.state.send(
                None,
                req,
                None,
                METADATA_HANDLER_ID,
                self.state.options.request_timeout,
            );
        }

        // 3. Tick all registered handlers
        let handler_ids: Vec<RequestHandlerId> = self.handlers.keys().copied().collect();
        for id in handler_ids {
            if let Some(handler) = self.handlers.get_mut(&id) {
                handler.tick(now, &mut self.state);
            }
        }
    }
}
