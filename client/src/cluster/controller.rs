use std::thread;
use std::thread::JoinHandle;

use crate::io_loop::Command;
use crate::io_loop::CommandSender;
use crate::io_loop::EventLoop;
use super::KafkaOptions;

pub struct KafkaCluster {
    event_loop: Option<JoinHandle<()>>,
    cmd_tx: CommandSender,
}

impl KafkaCluster {
    pub fn new(_options: KafkaOptions) -> Self {
        let (mut el, cmd_tx) = EventLoop::new();

        let event_loop = thread::spawn(move || {
            el.run();
        });

        KafkaCluster {
            event_loop: Some(event_loop),
            cmd_tx,
        }
    }
}

impl Drop for KafkaCluster {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(Command::Shutdown);
        if let Some(handle) = self.event_loop.take() {
            if let Err(e) = handle.join() {
                tracing::error!("event loop thread panicked: {e:?}");
            }
        }
    }
}
