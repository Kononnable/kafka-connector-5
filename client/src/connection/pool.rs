use std::io;
use std::time::Duration;

use bytes::Bytes;
use mio::{Events, Poll, Registry, Token, Waker};
use protocol::traits::{ApiRequest, ApiVersion};

use super::transport::Connection;

/// Token for the waker (eventfd/pipe) used to interrupt poll.
const WAKEUP_TOKEN: Token = Token(0);

/// A request queued for a broker that has no connection yet.
/// The closure captures the concrete request; it is invoked at flush time
/// with a live connection so proper version negotiation can happen.
struct QueuedRequest {
    api_key: i16,
    flush: Box<dyn FnOnce(&mut Connection) -> Result<i32, String> + Send>,
}

/// Manages a set of broker connections and pending outgoing requests.
pub struct ConnectionPool {
    connections: Vec<Connection>,
    pending: Vec<(i32, QueuedRequest)>,
    client_id: String,
    poll: Poll,
}

impl ConnectionPool {
    pub fn new(client_id: String) -> (Self, Waker) {
        let poll = Poll::new().expect("failed to create mio Poll");
        let waker = Waker::new(poll.registry(), WAKEUP_TOKEN).expect("failed to create mio Waker");
        let pool = ConnectionPool {
            connections: Vec::new(),
            pending: Vec::new(),
            client_id,
            poll,
        };
        (pool, waker)
    }

    /// Return the client id stored at construction time.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Queue a request for the given broker.
    /// Serialization + version negotiation happen at flush time.
    pub fn enqueue<R: ApiRequest + Send + 'static>(
        &mut self,
        broker_id: i32,
        request: R,
        version: Option<ApiVersion>,
    ) {
        let api_key = R::get_api_key().0;
        let flush = Box::new(move |conn: &mut Connection| {
            conn.send_api_request(&request, version)
                .map(|(cid, _)| cid)
                .map_err(|e| format!("{e}"))
        });
        self.pending.push((broker_id, QueuedRequest { api_key, flush }));
    }

    /// Return the set of broker ids that have queued requests.
    pub fn pending_brokers(&self) -> Vec<i32> {
        let mut seen = Vec::new();
        for (id, _) in &self.pending {
            if !seen.contains(id) {
                seen.push(*id);
            }
        }
        seen
    }

    /// Push all queued requests for `broker_id` onto the connection's
    /// write buffer.  Returns the correlation id of each flushed request.
    pub fn flush_pending(&mut self, broker_id: i32) -> Vec<i32> {
        let Some(conn) = self
            .connections
            .iter_mut()
            .find(|c| c.node_id() == broker_id)
        else {
            return Vec::new();
        };

        let mut corr_ids = Vec::new();
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].0 == broker_id {
                let (_, q) = self.pending.swap_remove(i);
                match (q.flush)(conn) {
                    Ok(cid) => corr_ids.push(cid),
                    Err(e) => tracing::warn!("flush failed (api_key={}): {e}", q.api_key),
                }
            } else {
                i += 1;
            }
        }
        corr_ids
    }

    /// Flush queued requests for every connected broker.
    /// Returns `(broker_id, correlation_id)` for each flushed request.
    pub fn send_api_requests(&mut self) -> Vec<(i32, i32)> {
        let ids: Vec<i32> = self.connections.iter().map(|c| c.node_id()).collect();
        let mut flushed = Vec::new();
        for broker_id in ids {
            for cid in self.flush_pending(broker_id) {
                flushed.push((broker_id, cid));
            }
        }
        flushed
    }

    /// Poll for I/O events on all registered connections.
    pub fn poll_io(&mut self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        self.poll.poll(events, timeout)
    }

    pub fn registry(&self) -> &Registry {
        self.poll.registry()
    }

    /// Access the live connections list.
    pub fn connections(&self) -> &[Connection] {
        &self.connections
    }

    /// Find a connection by token.
    pub fn find_by_token(&mut self, token: Token) -> Option<&mut Connection> {
        self.connections.iter_mut().find(|c| c.token() == token)
    }

    /// Collect all available broker responses from every connection.
    pub fn collect_responses(&mut self) -> Vec<(usize, i32, Bytes)> {
        self.connections
            .iter_mut()
            .enumerate()
            .flat_map(|(idx, conn)| {
                std::iter::from_fn(move || {
                    conn.read_broker_response()
                        .map(|(corr_id, body)| (idx, corr_id, body))
                })
            })
            .collect()
    }

    /// Add a new connection to the pool.
    pub fn push(&mut self, conn: Connection) {
        self.connections.push(conn);
    }
}
