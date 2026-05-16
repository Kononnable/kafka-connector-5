use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use futures_timer::Delay;
use std::time::Duration;

use super::ClusterOptions;
use super::error::ClusterOptionsValidationError;
use crate::consumer::ConsumerController;
use crate::consumer::ConsumerOptions;
use crate::consumer::ConsumerOptionsValidationError;
use crate::io_loop::Command;
use crate::io_loop::CommandSender;
use crate::io_loop::EventLoop;
use crate::io_loop::LifecycleState;
use crate::producer::ProducerController;
use crate::producer::ProducerOptions;
use crate::producer::ProducerOptionsValidationError;

pub struct ClusterController {
    event_loop: Option<JoinHandle<()>>,
    cmd_tx: CommandSender,
    lifecycle_state: LifecycleState,
}

impl ClusterController {
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
        while !self.lifecycle_state.is_active() {
            Delay::new(Duration::from_millis(10)).await;
        }
        ProducerController::new(self, options)
    }

    /// Create a consumer using this cluster connection.
    ///
    /// Waits for the event loop to become active before constructing the consumer.
    pub async fn new_consumer(
        self: Arc<Self>,
        options: ConsumerOptions,
    ) -> Result<ConsumerController, Vec<ConsumerOptionsValidationError>> {
        while !self.lifecycle_state.is_active() {
            Delay::new(Duration::from_millis(10)).await;
        }
        ConsumerController::new(self, options)
    }
}

impl Drop for ClusterController {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(Command::Shutdown);
        if let Some(handle) = self.event_loop.take() {
            if let Err(e) = handle.join() {
                tracing::error!("event loop thread panicked: {e:?}");
            }
        }
    }
}
