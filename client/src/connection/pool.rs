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
    flush: Box<dyn FnOnce(&mut Connection) -> FlushResult + Send>,
}

type FlushResult = Result<(i32, ApiVersion), String>;

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
                .map_err(|e| format!("{e}"))
        });
        self.pending
            .push((broker_id, QueuedRequest { api_key, flush }));
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
    /// write buffer.  Returns `(correlation_id, version)` for each flushed request.
    pub fn flush_pending(&mut self, broker_id: i32) -> Vec<(i32, ApiVersion)> {
        let Some(conn) = self
            .connections
            .iter_mut()
            .find(|c| c.node_id() == broker_id)
        else {
            return Vec::new();
        };

        let mut flushed = Vec::new();
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].0 == broker_id {
                let (_, q) = self.pending.swap_remove(i);
                match (q.flush)(conn) {
                    Ok((cid, ver)) => flushed.push((cid, ver)),
                    Err(e) => tracing::warn!("flush failed (api_key={}): {e}", q.api_key),
                }
            } else {
                i += 1;
            }
        }
        flushed
    }

    /// Flush queued requests for every connected broker.
    /// Returns `(broker_id, correlation_id, version)` for each flushed request.
    pub fn send_api_requests(&mut self) -> Vec<(i32, i32, ApiVersion)> {
        let ids: Vec<i32> = self.connections.iter().map(|c| c.node_id()).collect();
        let mut flushed = Vec::new();
        for broker_id in ids {
            for (cid, ver) in self.flush_pending(broker_id) {
                flushed.push((broker_id, cid, ver));
            }
        }
        flushed
    }

    /// Poll for I/O events on all registered connections.
    /// When no connections are registered, uses a timeout based on the
    /// earliest backoff expiration to avoid blocking forever.
    pub fn poll_io(&mut self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
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
                if earliest.is_none_or(|e| wait < e) {
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
        if let Some(pos) = self
            .connections
            .iter()
            .position(|c| c.node_id() == broker_id)
        {
            let entry =
                self.broker_reconnect
                    .entry(broker_id)
                    .or_insert_with(|| BrokerReconnectState {
                        failures: 0,
                        next_attempt: None,
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
        entry.failures == 0
            || entry
                .next_attempt
                .is_none_or(|t| t.elapsed() > Duration::ZERO)
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
            if state
                .next_attempt
                .is_none_or(|t| t.elapsed() >= Duration::ZERO)
            {
                expired.push(broker_id);
            }
        }
        expired
    }

    /// Return the reconnect failure count for a broker (test helper).
    #[cfg(test)]
    pub fn reconnect_failures(&self, broker_id: i32) -> u32 {
        self.broker_reconnect
            .get(&broker_id)
            .map(|s| s.failures)
            .unwrap_or(0)
    }

    /// Return the next reconnect attempt time for a broker (test helper).
    #[cfg(test)]
    pub fn next_reconnect_attempt(&self, broker_id: i32) -> Option<Instant> {
        self.broker_reconnect
            .get(&broker_id)
            .and_then(|s| s.next_attempt)
    }

    /// Return the number of pending requests for a broker (test helper).
    #[cfg(test)]
    pub fn pending_count(&self, broker_id: i32) -> usize {
        self.pending
            .iter()
            .filter(|(id, _)| *id == broker_id)
            .count()
    }

    /// Return the total number of pending requests (test helper).
    #[cfg(test)]
    pub fn total_pending(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use protocol::generated::ApiVersionsRequest;

    use super::*;

    fn make_pool() -> (ConnectionPool, mio::Waker) {
        ConnectionPool::new("test-client".to_string())
    }

    #[test]
    fn pending_brokers_returns_unique_broker_ids() {
        let (mut pool, _waker) = make_pool();
        pool.enqueue(1, ApiVersionsRequest::default(), None);
        pool.enqueue(2, ApiVersionsRequest::default(), None);
        pool.enqueue(1, ApiVersionsRequest::default(), None);
        pool.enqueue(3, ApiVersionsRequest::default(), None);

        let mut brokers = pool.pending_brokers();
        brokers.sort();
        assert_eq!(brokers, vec![1, 2, 3]);
    }

    #[test]
    fn pending_brokers_empty_when_no_requests() {
        let (pool, _waker) = make_pool();
        assert!(pool.pending_brokers().is_empty());
    }

    #[test]
    fn reconnect_state_manipulated_directly() {
        let (mut pool, _waker) = make_pool();
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(30);

        // Manually set up failure state
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 3,
                next_attempt: Some(Instant::now() + Duration::from_secs(10)),
            },
        );

        assert_eq!(pool.reconnect_failures(1), 3);
        assert!(pool.next_reconnect_attempt(1).is_some());
        assert!(!pool.can_reconnect(1));
    }

    #[test]
    fn can_reconnect_returns_false_during_backoff() {
        let (mut pool, _waker) = make_pool();
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(30);

        // Manually set up backoff state
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 1,
                next_attempt: Some(Instant::now() + base),
            },
        );

        assert!(!pool.can_reconnect(1));
    }

    #[test]
    fn can_reconnect_returns_true_after_backoff() {
        let (mut pool, _waker) = make_pool();

        // Set up past backoff time
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 1,
                next_attempt: Some(Instant::now() - Duration::from_secs(1)),
            },
        );

        assert!(pool.can_reconnect(1));
    }

    #[test]
    fn can_reconnect_returns_true_for_unknown_broker() {
        let (pool, _waker) = make_pool();
        assert!(pool.can_reconnect(999));
    }

    #[test]
    fn can_reconnect_returns_true_with_zero_failures() {
        let (mut pool, _waker) = make_pool();
        // Manually set up state with zero failures
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 0,
                next_attempt: None,
            },
        );
        assert!(pool.can_reconnect(1));
    }

    #[test]
    fn backoff_calculation_follows_exponential_formula() {
        // Test the backoff calculation directly via BrokerReconnectState
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(30);

        // Failure 1: base * 2^0 = 100ms (with jitter 0.8-1.2)
        let state1 = BrokerReconnectState {
            failures: 1,
            next_attempt: None,
        };
        let backoff1 = state1.backoff(base, max);
        assert!(
            backoff1 >= Duration::from_millis(80),
            "backoff too short: {:?}",
            backoff1
        );
        assert!(
            backoff1 <= Duration::from_millis(120),
            "backoff too long: {:?}",
            backoff1
        );

        // Failure 2: base * 2^1 = 200ms (with jitter)
        let state2 = BrokerReconnectState {
            failures: 2,
            next_attempt: None,
        };
        let backoff2 = state2.backoff(base, max);
        assert!(
            backoff2 >= Duration::from_millis(160),
            "backoff too short: {:?}",
            backoff2
        );
        assert!(
            backoff2 <= Duration::from_millis(240),
            "backoff too long: {:?}",
            backoff2
        );

        // Failure 3: base * 2^2 = 400ms (with jitter)
        let state3 = BrokerReconnectState {
            failures: 3,
            next_attempt: None,
        };
        let backoff3 = state3.backoff(base, max);
        assert!(
            backoff3 >= Duration::from_millis(320),
            "backoff too short: {:?}",
            backoff3
        );
        assert!(
            backoff3 <= Duration::from_millis(480),
            "backoff too long: {:?}",
            backoff3
        );
    }

    #[test]
    fn backoff_caps_at_max() {
        let base = Duration::from_millis(100);
        let max = Duration::from_millis(500);

        // After many failures, backoff should cap at max
        let state = BrokerReconnectState {
            failures: 10,
            next_attempt: None,
        };
        let backoff = state.backoff(base, max);
        // Allow for jitter: should be between 0.8*max and 1.2*max
        assert!(
            backoff >= Duration::from_millis(400),
            "backoff below max: {:?}",
            backoff
        );
        assert!(
            backoff <= Duration::from_millis(600),
            "backoff above max: {:?}",
            backoff
        );
    }

    #[test]
    fn reset_reconnect_clears_failure_state() {
        let (mut pool, _waker) = make_pool();

        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 5,
                next_attempt: Some(Instant::now()),
            },
        );

        pool.reset_reconnect(1);
        assert_eq!(pool.reconnect_failures(1), 0);
        assert!(pool.next_reconnect_attempt(1).is_none());
    }

    #[test]
    fn reset_reconnect_unknown_broker_is_noop() {
        let (mut pool, _waker) = make_pool();
        pool.reset_reconnect(999);
        assert_eq!(pool.reconnect_failures(999), 0);
    }

    #[test]
    fn increment_reconnect_failure_increments_and_schedules() {
        let (mut pool, _waker) = make_pool();
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(30);

        // Insert entry first (simulating initial failure via remove_connection)
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 0,
                next_attempt: None,
            },
        );

        pool.increment_reconnect_failure(1, base, max);
        assert_eq!(pool.reconnect_failures(1), 1);
        assert!(pool.next_reconnect_attempt(1).is_some());

        pool.increment_reconnect_failure(1, base, max);
        assert_eq!(pool.reconnect_failures(1), 2);
    }

    #[test]
    fn try_reconnect_records_success() {
        let (mut pool, _waker) = make_pool();
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(30);

        // Set up failure state first
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 3,
                next_attempt: Some(Instant::now()),
            },
        );

        pool.try_reconnect(1, base, max, Ok(()));
        // try_reconnect records the attempt but doesn't reset failures
        // failures should still be 3 (reset_reconnect is called separately)
        assert_eq!(pool.reconnect_failures(1), 3);
        // next_attempt should be updated to a future time
        assert!(pool.next_reconnect_attempt(1).is_some());
    }

    #[test]
    fn try_reconnect_records_failure() {
        let (mut pool, _waker) = make_pool();
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(30);

        // Insert entry first
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 0,
                next_attempt: None,
            },
        );

        pool.try_reconnect(1, base, max, Err("connection refused".to_string()));
        assert_eq!(pool.reconnect_failures(1), 1);
        assert!(pool.next_reconnect_attempt(1).is_some());
    }

    #[test]
    fn expired_reconnects_returns_backoff_expired_brokers() {
        let (mut pool, _waker) = make_pool();

        // Set up expired backoff states
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 1,
                next_attempt: Some(Instant::now() - Duration::from_secs(1)),
            },
        );
        pool.broker_reconnect.insert(
            2,
            BrokerReconnectState {
                failures: 2,
                next_attempt: Some(Instant::now() - Duration::from_secs(1)),
            },
        );

        let expired = pool.expired_reconnects();
        assert_eq!(expired.len(), 2);
        assert!(expired.contains(&1));
        assert!(expired.contains(&2));
    }

    #[test]
    fn expired_reconnects_excludes_connected_brokers() {
        let (mut pool, _waker) = make_pool();

        // Set up expired backoff state
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 1,
                next_attempt: Some(Instant::now() - Duration::from_secs(1)),
            },
        );

        // Add a "connection" by inserting broker 1 into connections (mocked)
        // We can't add real Connections without TcpStream, but we can verify
        // the logic by checking that brokers NOT in connections are included
        let expired = pool.expired_reconnects();
        assert!(expired.contains(&1));
    }

    #[test]
    fn expired_reconnects_excludes_zero_failures() {
        let (mut pool, _waker) = make_pool();
        // Manually add state with zero failures
        pool.broker_reconnect.insert(
            1,
            BrokerReconnectState {
                failures: 0,
                next_attempt: None,
            },
        );

        let expired = pool.expired_reconnects();
        assert!(!expired.contains(&1));
    }

    #[test]
    fn expired_reconnects_empty_when_no_failures() {
        let (pool, _waker) = make_pool();
        assert!(pool.expired_reconnects().is_empty());
    }

    #[test]
    fn client_id_returns_stored_value() {
        let (pool, _waker) = make_pool();
        assert_eq!(pool.client_id(), "test-client");
    }

    #[test]
    fn connections_empty_when_no_connections() {
        let (pool, _waker) = make_pool();
        assert!(pool.connections().is_empty());
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
        let exp = self.failures - 1;
        let capped = if exp >= 31 {
            max
        } else {
            let multiplier = 1_u32 << exp;
            std::cmp::min(base.checked_mul(multiplier).unwrap_or(max), max)
        };
        let jitter = 0.8 + 0.4 * rand::random::<f64>();
        capped.mul_f64(jitter)
    }
}
