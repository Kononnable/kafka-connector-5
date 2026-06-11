use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use futures_timer::Delay;
use indexmap::IndexMap;
use protocol::generated::api_versions_response::ApiVersion as ApiVersionEntry;

use super::ClusterOptions;
use super::error::ClusterOptionsValidationError;
use crate::consumer::{ConsumerController, ConsumerOptions, ConsumerOptionsValidationError};
use crate::io_loop::{Command, CommandSender, EventLoop, LifecycleState, State};
use crate::producer::{ProducerController, ProducerOptions, ProducerOptionsValidationError};
use crate::types::BrokerId;

pub struct ClusterController {
    event_loop: Option<JoinHandle<()>>,
    cmd_tx: CommandSender,
    lifecycle_state: LifecycleState,
}

impl ClusterController {
    pub fn state(&self) -> State {
        self.lifecycle_state.state()
    }

    pub fn new(options: ClusterOptions) -> Result<Arc<Self>, Vec<ClusterOptionsValidationError>> {
        options.validate()?;
        let (mut el, cmd_tx, lifecycle_state) = EventLoop::new(options);

        let event_loop = thread::spawn(move || {
            el.run();
        });

        Ok(Arc::new(ClusterController {
            event_loop: Some(event_loop),
            cmd_tx,
            lifecycle_state,
        }))
    }

    /// Create a producer using this cluster connection.
    ///
    /// Waits for the event loop to become active before constructing the producer.
    pub async fn new_producer(
        self: Arc<Self>,
        options: ProducerOptions,
    ) -> Result<ProducerController, Vec<ProducerOptionsValidationError>> {
        while self.lifecycle_state.state() != State::Active {
            Delay::new(Duration::from_millis(10)).await;
        }
        ProducerController::new(self, options)
    }

    /// Returns the API versions supported by a connected broker.
    ///
    /// Pass `None` to query any available broker.
    /// Waits for the event loop to become active before querying.
    pub async fn broker_api_versions(
        self: &Arc<Self>,
        broker_id: Option<BrokerId>,
    ) -> Option<IndexMap<i16, ApiVersionEntry>> {
        while self.lifecycle_state.state() != State::Active {
            Delay::new(Duration::from_millis(10)).await;
        }
        let (tx, rx) = futures::channel::oneshot::channel();
        self.cmd_tx
            .send(Command::GetApiVersions {
                broker_id,
                reply: tx,
            })
            .ok()?;
        rx.await.ok()?
    }

    /// Create a consumer using this cluster connection.
    ///
    /// Waits for the event loop to become active before constructing the consumer.
    pub async fn new_consumer(
        self: Arc<Self>,
        options: ConsumerOptions,
    ) -> Result<ConsumerController, Vec<ConsumerOptionsValidationError>> {
        while self.lifecycle_state.state() != State::Active {
            Delay::new(Duration::from_millis(10)).await;
        }
        ConsumerController::new(self, options)
    }
}

impl Drop for ClusterController {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(Command::Shutdown);
        if let Some(handle) = self.event_loop.take()
            && let Err(e) = handle.join()
        {
            tracing::error!("event loop thread panicked: {e:?}");
        }
    }
}
