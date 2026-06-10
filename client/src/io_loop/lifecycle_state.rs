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

impl Default for LifecycleState {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleState {
    const INITIALIZING: u8 = 0;
    const ACTIVE: u8 = 1;
    const SHUTDOWN_TRIGGERED: u8 = 2;
    const SHUTDOWN_COMPLETE: u8 = 3;

    pub fn new() -> Self {
        LifecycleState(Arc::new(AtomicU8::new(Self::INITIALIZING)))
    }

    pub fn set_active(&self) {
        self.0.store(Self::ACTIVE, Ordering::Relaxed);
    }

    pub fn set_shutdown_triggered(&self) {
        self.0.store(Self::SHUTDOWN_TRIGGERED, Ordering::Relaxed);
    }

    pub fn set_shutdown_complete(&self) {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn starts_in_initializing() {
        let state = LifecycleState::new();
        assert_eq!(state.state(), State::Initializing);
    }

    #[test]
    fn default_is_initializing() {
        let state = LifecycleState::default();
        assert_eq!(state.state(), State::Initializing);
    }

    #[test]
    fn state_transitions() {
        let state = LifecycleState::new();
        assert_eq!(state.state(), State::Initializing);

        state.set_active();
        assert_eq!(state.state(), State::Active);

        state.set_shutdown_triggered();
        assert_eq!(state.state(), State::ShutdownTriggered);

        state.set_shutdown_complete();
        assert_eq!(state.state(), State::ShutdownComplete);
    }

    #[test]
    fn state_transitions_can_skip_stages() {
        let state = LifecycleState::new();
        // Can jump directly from Initializing to ShutdownComplete
        state.set_shutdown_complete();
        assert_eq!(state.state(), State::ShutdownComplete);
    }

    #[test]
    fn cloned_states_share_atomic() {
        let state = LifecycleState::new();
        let cloned = state.clone();

        // Both start at Initializing
        assert_eq!(state.state(), State::Initializing);
        assert_eq!(cloned.state(), State::Initializing);

        // Changing one affects the other
        state.set_active();
        assert_eq!(state.state(), State::Active);
        assert_eq!(cloned.state(), State::Active);

        // Changing via clone affects the original
        cloned.set_shutdown_triggered();
        assert_eq!(state.state(), State::ShutdownTriggered);
        assert_eq!(cloned.state(), State::ShutdownTriggered);
    }

    #[test]
    fn cloned_states_are_independent_handles() {
        let state = LifecycleState::new();
        let cloned = state.clone();

        // Verify they are different Arc pointers but share data
        assert!(Arc::as_ptr(&state.0) == Arc::as_ptr(&cloned.0));
    }

    #[test]
    fn state_is_thread_safe() {
        let state = Arc::new(LifecycleState::new());
        let mut handles = Vec::new();

        // Spawn multiple threads that all write to the state
        for i in 0..10 {
            let state_clone = Arc::clone(&state);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    match i % 4 {
                        0 => state_clone.set_active(),
                        1 => state_clone.set_shutdown_triggered(),
                        2 => state_clone.set_shutdown_complete(),
                        _ => {
                            let _ = state_clone.state();
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // Also have the main thread read concurrently
        let main_state = Arc::clone(&state);
        let main_handle = thread::spawn(move || {
            for _ in 0..100 {
                let _ = main_state.state();
            }
        });
        handles.push(main_handle);

        // All threads should complete without panicking
        for handle in handles {
            handle.join().expect("thread panicked");
        }

        // Final state should be valid (one of the four states)
        match state.state() {
            State::Initializing | State::Active | State::ShutdownTriggered
            | State::ShutdownComplete => {}
        }
    }

    #[test]
    fn state_values_are_distinct() {
        assert_ne!(State::Initializing, State::Active);
        assert_ne!(State::Active, State::ShutdownTriggered);
        assert_ne!(State::ShutdownTriggered, State::ShutdownComplete);
        assert_ne!(State::Initializing, State::ShutdownComplete);
    }

    #[test]
    fn state_is_copy() {
        let state = LifecycleState::new();
        let s1 = state.state();
        let s2 = state.state();
        // Copy should not move or invalidate
        assert_eq!(s1, s2);
        assert_eq!(state.state(), s1);
    }
}
