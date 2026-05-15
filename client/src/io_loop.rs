use std::sync::mpsc;
use std::time::Instant;

pub(crate) mod connection;
pub(crate) mod controller;
pub(crate) mod metadata;
pub(crate) mod sender;

pub(crate) use controller::{Command, EventLoop};
pub(crate) use sender::CommandSender;

/// Trait for state machines driven by the event loop.
///
/// Each end-client (producer, consumer, etc.) implements this trait.
/// The event loop owns a `Vec<Box<dyn EventHandler>>` and calls these
/// methods during each iteration.
pub(crate) trait EventHandler {
    /// Called once per event loop iteration. The state machine checks its
    /// internal deadlines (linger, heartbeat, metadata age, etc.) and
    /// enqueues requests or completes channels as needed.
    fn tick(&mut self, now: Instant);

    /// Called when a response arrives for a request sent by this handler.
    fn on_response(&mut self, correlation_id: i32, response_bytes: bytes::BytesMut);

    /// Initiate graceful close. May take multiple ticks to complete
    /// (e.g., flushing pending sends, sending LeaveGroup). The `on_closed`
    /// oneshot must be signalled once fully closed.
    fn close(&mut self, on_closed: mpsc::Sender<()>);
}
