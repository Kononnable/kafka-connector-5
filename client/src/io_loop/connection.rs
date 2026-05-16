use mio::net::TcpStream;
use mio::Token;

pub struct Connection {
    token: Token,
    stream: TcpStream,
}

impl Connection {
    pub fn new(token: Token, stream: TcpStream) -> Self {
        Connection { token, stream }
    }

    pub fn token(&self) -> Token {
        self.token
    }

    pub fn on_readable(&mut self) {
        // TODO: read bytes from socket, parse responses
    }

    pub fn on_writable(&mut self) {
        // TODO: write pending bytes to socket
    }
}
