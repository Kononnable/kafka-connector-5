use std::sync::mpsc;

pub(crate) struct EventLoop {
    cmd_rx: mpsc::Receiver<()>,
}

impl EventLoop {
    pub(crate) fn new(cmd_rx: mpsc::Receiver<()>) -> Self {
        EventLoop { cmd_rx }
    }

    pub(crate) fn run(&mut self) {
        loop {
            match self.cmd_rx.recv() {
                Ok(_) => {}
                Err(_) => break,
            }
        }
    }
}
