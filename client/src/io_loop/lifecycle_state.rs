use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Observable states of the event loop lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Initializing,
    Active,
    ShutdownTriggered,
    ShutdownComplete,
}

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

    pub(crate) fn set_shutdown_triggered(&self) {
        self.0.store(Self::SHUTDOWN_TRIGGERED, Ordering::Relaxed);
    }

    pub(crate) fn set_shutdown_complete(&self) {
        self.0.store(Self::SHUTDOWN_COMPLETE, Ordering::Relaxed);
    }

    /// Returns the current lifecycle state.
    pub fn state(&self) -> State {
        match self.0.load(Ordering::Relaxed) {
            Self::INITIALIZING => State::Initializing,
            Self::ACTIVE => State::Active,
            Self::SHUTDOWN_TRIGGERED => State::ShutdownTriggered,
            Self::SHUTDOWN_COMPLETE => State::ShutdownComplete,
            _ => unreachable!(),
        }
    }
}
