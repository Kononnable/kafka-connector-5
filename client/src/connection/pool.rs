use std::io;
use std::time::{Duration, Instant};

use bytes::Bytes;
use indexmap::IndexMap;
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

/// Per-broker reconnect failure tracking.
struct BrokerReconnectState {
    failures: u32,
    next_attempt: Option<Instant>,
}

/// Manages a set of broker connections and pending outgoing requests.
pub struct ConnectionPool {
    connections: Vec<Connection>,
    pending: Vec<(i32, QueuedRequest)>,
    client_id: String,
    poll: Poll,
    /// Tracks reconnect backoff state per broker.
    broker_reconnect: IndexMap<i32, BrokerReconnectState>,
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
            broker_reconnect: IndexMap::new(),
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
    /// When no connections are registered, uses a timeout based on the
    /// earliest backoff expiration to avoid blocking forever.
    pub fn poll_io(
        &mut self,
        events: &mut Events,
        timeout: Option<Duration>,
    ) -> io::Result<()> {
        if self.connections.is_empty() {
            let mut earliest = None;
            for state in self.broker_reconnect.values() {
                if state.failures == 0 {
                    continue;
                }
                let wait = state.next_attempt.map_or(Duration::ZERO, |t| {
                    t.saturating_duration_since(Instant::now())
                });
                if wait.is_zero() {
                    return self.poll.poll(events, Some(Duration::from_millis(0)));
                }
                if earliest.map_or(true, |e| wait < e) {
                    earliest = Some(wait);
                }
            }
            if let Some(backoff) = earliest {
                return self.poll.poll(events, Some(backoff));
            }
        }
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

    /// Remove a dead connection by broker id. Increments the broker's
    /// reconnect failure counter and schedules the next retry.
    pub fn remove_connection(&mut self, broker_id: i32, base: Duration, max: Duration) {
        if let Some(pos) = self.connections.iter().position(|c| c.node_id() == broker_id) {
            let entry = self.broker_reconnect.entry(broker_id).or_insert_with(|| {
                BrokerReconnectState {
                    failures: 0,
                    next_attempt: None,
                }
            });
            entry.failures = entry.failures.saturating_add(1);
            entry.next_attempt = Some(Instant::now() + entry.backoff(base, max));
            self.connections.remove(pos);
        }
    }

    /// Check whether we should attempt reconnection to `broker_id`.
    /// Returns `true` if the backoff period has elapsed or there are no
    /// prior failures.
    pub fn can_reconnect(&self, broker_id: i32) -> bool {
        let Some(entry) = self.broker_reconnect.get(&broker_id) else {
            return true;
        };
        entry.failures == 0 || entry.next_attempt.map_or(true, |t| t.elapsed() > Duration::ZERO)
    }

    /// Record that a reconnect attempt is being made for `broker_id`.
    /// Sets the next attempt time based on current failure count.
    pub fn record_reconnect_attempt(&mut self, broker_id: i32, base: Duration, max: Duration) {
        if let Some(entry) = self.broker_reconnect.get_mut(&broker_id) {
            entry.next_attempt = Some(Instant::now() + entry.backoff(base, max));
        }
    }

    /// Reset reconnect failure tracking for `broker_id` (successful reconnect).
    pub fn reset_reconnect(&mut self, broker_id: i32) {
        if let Some(entry) = self.broker_reconnect.get_mut(&broker_id) {
            entry.failures = 0;
            entry.next_attempt = None;
        }
    }

    /// Increment reconnect failure counter and schedule next attempt
    /// (called when a reconnect attempt fails).
    pub fn increment_reconnect_failure(&mut self, broker_id: i32, base: Duration, max: Duration) {
        if let Some(entry) = self.broker_reconnect.get_mut(&broker_id) {
            entry.failures = entry.failures.saturating_add(1);
            entry.next_attempt = Some(Instant::now() + entry.backoff(base, max));
        }
    }

    /// Attempt to reconnect to a broker. Records the attempt, increments
    /// failure counter on failure, and resets on success.
    pub fn try_reconnect(
        &mut self,
        broker_id: i32,
        base: Duration,
        max: Duration,
        result: Result<(), String>,
    ) {
        self.record_reconnect_attempt(broker_id, base, max);
        if let Err(e) = result {
            self.increment_reconnect_failure(broker_id, base, max);
            tracing::warn!("reconnect to broker {broker_id} failed: {e}");
        }
    }

    /// Return broker ids that have pending reconnect attempts whose
    /// backoff has expired but are not currently connected.
    pub fn expired_reconnects(&self) -> Vec<i32> {
        let mut expired = Vec::new();
        let connected: Vec<i32> = self.connections.iter().map(|c| c.node_id()).collect();
        for (&broker_id, state) in &self.broker_reconnect {
            if connected.contains(&broker_id) {
                continue;
            }
            if state.failures == 0 {
                continue;
            }
            if state.next_attempt.map_or(true, |t| t.elapsed() <= Duration::ZERO) {
                expired.push(broker_id);
            }
        }
        expired
    }
}

impl BrokerReconnectState {
    /// Calculate exponential backoff with jitter.
    ///
    /// Formula: MIN(max, base * 2^(failures-1)) * jitter
    /// Jitter is a random factor in [0.8, 1.2] (KIP-144).
    fn backoff(&self, base: Duration, max: Duration) -> Duration {
        if self.failures == 0 {
            return Duration::from_millis(0);
        }
        let exp = (self.failures - 1) as u32;
        let capped = if exp >= 31 {
            max
        } else {
            let multiplier = (1u32 << exp) as u32;
            std::cmp::min(
                base.checked_mul(multiplier).unwrap_or(max),
                max,
            )
        };
        let jitter = 0.8 + 0.4 * rand::random::<f64>();
        capped.mul_f64(jitter)
    }
}
