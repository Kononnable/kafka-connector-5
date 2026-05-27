use bytes::Bytes;
use mio::Token;
use mio::net::TcpStream;

use super::transport::Connection;

/// Manages a set of broker connections.
pub struct ConnectionPool {
    connections: Vec<Connection>,
    client_id: Option<String>,
    next_token: usize,
}

impl ConnectionPool {
    pub fn new(client_id: Option<String>) -> Self {
        ConnectionPool {
            connections: Vec::new(),
            client_id,
            next_token: 1,
        }
    }

    /// Allocate the next token and advance the counter.
    pub fn next_token(&mut self) -> Token {
        let token = Token(self.next_token);
        self.next_token += 1;
        token
    }

    /// Find a connection by token.
    pub fn find_by_token(&mut self, token: Token) -> Option<&mut Connection> {
        self.connections.iter_mut().find(|c| c.token() == token)
    }

    /// Iterate over connections by mutable reference.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, Connection> {
        self.connections.iter_mut()
    }

    /// Add a new connection to the pool.
    pub fn push(&mut self, conn: Connection) {
        self.connections.push(conn);
    }

    /// Send a protocol request to the connection for the given broker.
    /// Handles connection lookup, establishment, and sending.
    ///
    /// # Arguments
    /// * `broker_id` — the broker node id to send to
    /// * `payload` — pre-serialized Kafka frame bytes
    /// * `addrs` — socket addresses to connect to if no connection exists
    /// * `registry` — mio registry for socket registration
    pub fn send_api_request(
        &mut self,
        broker_id: i32,
        payload: Bytes,
        addrs: &[std::net::SocketAddr],
        registry: &mio::Registry,
    ) -> Result<(), &'static str> {
        // Check if a connection for this broker already exists.
        if let Some(conn) = self
            .connections
            .iter_mut()
            .find(|c| c.node_id() == broker_id)
        {
            conn.send_raw_request(payload);
            return Ok(());
        }

        // No connection exists — attempt to connect.
        for addr in addrs {
            match TcpStream::connect(*addr) {
                Ok(mut stream) => {
                    let token = Token(self.connections.len() + 1); // placeholder, actual token assigned by caller
                    if registry
                        .register(
                            &mut stream,
                            token,
                            mio::Interest::READABLE | mio::Interest::WRITABLE,
                        )
                        .is_ok()
                    {
                        let mut conn =
                            Connection::new(token, stream, self.client_id.clone(), broker_id);
                        conn.send_raw_request(payload);
                        self.connections.push(conn);
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::warn!(broker_id, addr = ?addr, "failed to connect: {e}");
                }
            }
        }

        Err("failed to connect to broker")
    }
}
