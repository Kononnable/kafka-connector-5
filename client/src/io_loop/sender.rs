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
