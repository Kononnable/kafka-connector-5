use std::sync::mpsc;
use std::thread;

use crate::io_loop::EventLoop;
use super::KafkaOptions;

pub struct KafkaCluster {
    event_loop: Option<thread::JoinHandle<()>>,
    cmd_tx: mpsc::Sender<()>,
}

impl KafkaCluster {
    pub fn new(_options: KafkaOptions) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel();

        let event_loop = thread::spawn(move || {
            let mut el = EventLoop::new(cmd_rx);
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
        if let Some(handle) = self.event_loop.take() {
            let _ = handle.join();
        }
    }
}
