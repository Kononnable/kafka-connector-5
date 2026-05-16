use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Tracks whether the event loop is still running.
///
/// Cloning shares the same underlying atomic state so the handle
/// can be observed from another thread without a channel round-trip.
#[derive(Clone, Debug)]
pub struct LifecycleState(Arc<AtomicU8>);

impl LifecycleState {
    const INITIALIZING: u8 = 0;
    const ACTIVE: u8 = 1;
    const SHUTDOWN_TRIGGERED: u8 = 2;
    const SHUTDOWN_COMPLETE: u8 = 3;

    pub(crate) fn new() -> Self {
        LifecycleState(Arc::new(AtomicU8::new(Self::INITIALIZING)))
    }

    pub(crate) fn set_active(&self) {
        self.0.store(Self::ACTIVE, Ordering::Relaxed);
    }

    /// Returns `true` when the event loop thread is running and polling.
    pub fn is_active(&self) -> bool {
        self.0.load(Ordering::Relaxed) == Self::ACTIVE
    }

    pub(crate) fn set_shutdown_triggered(&self) {
        self.0.store(Self::SHUTDOWN_TRIGGERED, Ordering::Relaxed);
    }

    pub(crate) fn set_shutdown_complete(&self) {
        self.0.store(Self::SHUTDOWN_COMPLETE, Ordering::Relaxed);
    }

    /// Returns `true` when the event loop has received a shutdown command.
    pub fn is_shutdown_triggered(&self) -> bool {
        self.0.load(Ordering::Relaxed) == Self::SHUTDOWN_TRIGGERED
    }

    /// Returns `true` when the event loop has fully shut down.
    pub fn is_shutdown_complete(&self) -> bool {
        self.0.load(Ordering::Relaxed) == Self::SHUTDOWN_COMPLETE
    }
}
