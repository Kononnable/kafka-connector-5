use mio::Token;

pub(crate) struct Connection {
    token: Token,
}

impl Connection {
    pub(crate) fn token(&self) -> Token {
        self.token
    }

    pub(crate) fn on_readable(&mut self) {
        // TODO: read bytes from socket, parse responses
    }

    pub(crate) fn on_writable(&mut self) {
        // TODO: write pending bytes to socket
    }
}
