use std::sync::{Arc, mpsc};

use mio::Waker;

use super::Command;

/// A sender paired with a waker so the event loop can be notified of new commands.
#[derive(Clone)]
pub struct CommandSender {
    tx: mpsc::Sender<Command>,
    waker: Arc<Waker>,
}

impl CommandSender {
    pub fn new(tx: mpsc::Sender<Command>, waker: Arc<Waker>) -> Self {
        CommandSender { tx, waker }
    }

    pub fn send(&self, cmd: Command) -> Result<(), mpsc::SendError<Command>> {
        self.tx.send(cmd)?;
        self.waker
            .wake()
            .expect("failed to wake event loop: poll registry dropped before sender");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mio::{Poll, Token};

    use super::*;

    fn make_sender() -> (CommandSender, mpsc::Receiver<Command>, Poll) {
        let poll = Poll::new().unwrap();
        let waker = Waker::new(poll.registry(), Token(0)).unwrap();
        let (tx, rx) = mpsc::channel();
        let sender = CommandSender::new(tx, Arc::new(waker));
        (sender, rx, poll)
    }

    #[test]
    fn send_wakes_event_loop() {
        let (sender, _rx, mut poll) = make_sender();

        // Send a command — this should trigger the waker
        sender.send(Command::Shutdown).unwrap();

        // Poll with a short timeout — the waker should be ready immediately
        let mut events = mio::Events::with_capacity(1);
        let result = poll.poll(&mut events, Some(Duration::from_millis(100)));

        assert!(result.is_ok(), "poll should succeed");
        assert!(!events.is_empty(), "waker should have been triggered");
        assert!(
            events.iter().any(|e| e.token() == Token(0)),
            "waker token should be in events"
        );
    }

    #[test]
    fn send_wakes_event_loop_multiple_times() {
        let (sender, _rx, mut poll) = make_sender();

        // Send multiple commands
        sender.send(Command::Shutdown).unwrap();
        sender.send(Command::Shutdown).unwrap();

        // Poll — should see the waker triggered
        let mut events = mio::Events::with_capacity(1);
        let result = poll.poll(&mut events, Some(Duration::from_millis(100)));

        assert!(result.is_ok());
        assert!(!events.is_empty(), "waker should have been triggered");
    }

    #[test]
    fn send_fails_when_receiver_dropped() {
        let poll = Poll::new().unwrap();
        let waker = Waker::new(poll.registry(), Token(0)).unwrap();
        let (tx, rx) = mpsc::channel();
        drop(rx); // Drop the receiver

        let sender = CommandSender::new(tx, Arc::new(waker));
        let result = sender.send(Command::Shutdown);

        assert!(result.is_err(), "send should fail when receiver is dropped");
    }

    #[test]
    fn send_succeeds_when_receiver_alive() {
        let (sender, rx, _poll) = make_sender();

        let result = sender.send(Command::Shutdown);
        assert!(result.is_ok(), "send should succeed with alive receiver");

        // Verify the command was received
        let cmd = rx.try_recv().unwrap();
        assert!(matches!(cmd, Command::Shutdown));
    }

    #[test]
    fn sender_clone_shares_channel_and_waker() {
        let (sender, rx, mut poll) = make_sender();
        let sender2 = sender.clone();

        // Both senders should be able to send
        sender.send(Command::Shutdown).unwrap();
        sender2.send(Command::Shutdown).unwrap();

        // Both should trigger the waker
        let mut events = mio::Events::with_capacity(1);
        poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
        assert!(!events.is_empty());

        // Both commands should be in the channel
        assert!(matches!(rx.try_recv().unwrap(), Command::Shutdown));
        assert!(matches!(rx.try_recv().unwrap(), Command::Shutdown));
    }

    #[test]
    fn send_error_is_send_error() {
        let poll = Poll::new().unwrap();
        let waker = Waker::new(poll.registry(), Token(0)).unwrap();
        let (tx, rx) = mpsc::channel();
        drop(rx);

        let sender = CommandSender::new(tx, Arc::new(waker));
        let err = sender.send(Command::Shutdown).unwrap_err();

        // SendError displays as "sending on a closed channel"
        let msg = err.to_string();
        assert!(
            msg.contains("closed") || msg.contains("channel"),
            "error should indicate channel is closed, got: {}", msg
        );
    }
}
