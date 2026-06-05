use std::net::ToSocketAddrs;

use mio::net::TcpStream;
use mio::{Events, Interest, Token, Waker};
use protocol::traits::{ApiRequest, ApiVersion};

use super::ClusterOptions;
use crate::connection::{Connection, ConnectionPool};
use crate::metadata::MetadataCache;

/// Owns the connection pool, metadata cache, and the mio `Poll` instance.
pub struct ClusterState {
    pub(crate) options: ClusterOptions,
    pub(crate) pool: ConnectionPool,
    pub(crate) metadata: MetadataCache,
    next_connection_token: usize,
}

impl ClusterState {
    pub fn new(options: ClusterOptions) -> (Self, Waker) {
        let (pool, waker) = ConnectionPool::new(options.client_name.clone());
        let state = Self {
            pool,
            metadata: MetadataCache::new(options.metadata_refresh_interval),
            options,
            next_connection_token: 1,
        };
        (state, waker)
    }

    /// Probe connections for errors and remove dead ones
    /// (read error, write error, or connection reset).
    /// Returns the list of broker ids whose connections were removed.
    pub fn prune_dead_connections(&mut self, events: &Events) -> Vec<i32> {
        let mut broker_ids = Vec::new();
        for event in events {
            let token = event.token();
            if token == Token(0) {
                continue;
            }
            if let Some(conn) = self.pool.find_by_token(token) {
                let node_id = conn.node_id();
                if event.is_readable() {
                    if conn.on_readable().is_err() {
                        tracing::error!(
                            "read error on connection {:?} (broker {})",
                            token.0,
                            node_id
                        );
                        broker_ids.push(node_id);
                    }
                }
                if event.is_writable() {
                    if conn.on_writable().is_err() {
                        tracing::error!(
                            "write error on connection {:?} (broker {})",
                            token.0,
                            node_id
                        );
                        broker_ids.push(node_id);
                    }
                }
            }
        }
        let base = self.options.reconnect_backoff_ms;
        let max = self.options.reconnect_backoff_max_ms;
        for &broker_id in &broker_ids {
            self.pool.remove_connection(broker_id, base, max);
        }
        broker_ids
    }

    /// Queue a request for the target broker.
    /// If `broker_id` is `None`, picks any known broker from metadata.
    /// Serialization + sending happen later when the connection flushes.
    pub fn send<R: ApiRequest + Send + 'static>(
        &mut self,
        broker_id: Option<i32>,
        request: R,
        version: Option<ApiVersion>,
    ) -> Result<(), String> {
        let broker_id = match broker_id {
            Some(id) => id,
            None => self
                .metadata
                .any_broker()
                .ok_or_else(|| "no brokers in metadata cache".to_string())?,
        };

        self.pool.enqueue(broker_id, request, version);
        Ok(())
    }

    /// Connect to new brokers (lazy), reconnect to dead ones, and retry
    /// backoff-expired brokers from previous iterations.
    pub fn connect_to_brokers(&mut self, dead_broker_ids: &[i32]) {
        let base = self.options.reconnect_backoff_ms;
        let max = self.options.reconnect_backoff_max_ms;

        // Reconnect to brokers whose connections just died
        for &broker_id in dead_broker_ids {
            if !self.pool.can_reconnect(broker_id) {
                continue;
            }
            let result = self.try_connect(broker_id);
            self.pool.try_reconnect(broker_id, base, max, result);
        }

        // Retry backoff-expired brokers from previous iterations
        for broker_id in self.pool.expired_reconnects() {
            let result = self.try_connect(broker_id);
            self.pool.try_reconnect(broker_id, base, max, result);
        }

        // Connect to new brokers from pending queue
        for broker_id in self.pool.pending_brokers() {
            if self
                .pool
                .connections()
                .iter()
                .any(|c| c.node_id() == broker_id)
            {
                continue;
            }
            let result = self.try_connect(broker_id);
            self.pool.try_reconnect(broker_id, base, max, result);
        }
    }

    fn try_connect(&mut self, broker_id: i32) -> Result<(), String> {
        let info = self
            .metadata
            .broker_info(broker_id)
            .ok_or_else(|| format!("unknown broker {broker_id}"))?;

        let addrs: Vec<_> = format!("{}:{}", info.host, info.port)
            .to_socket_addrs()
            .map_err(|e| format!("dns resolution failed: {e}"))?
            .collect();

        for addr in &addrs {
            match TcpStream::connect(*addr) {
                Ok(mut stream) => {
                    let token = Token(self.next_connection_token);
                    self.next_connection_token += 1;
                    if self
                        .pool
                        .registry()
                        .register(&mut stream, token, Interest::READABLE | Interest::WRITABLE)
                        .is_ok()
                    {
                        let conn = Connection::new(
                            token,
                            stream,
                            Some(self.pool.client_id().to_string()),
                            broker_id,
                        );
                        self.pool.push(conn);
                        self.pool.reset_reconnect(broker_id);
                        return Ok(());
                    }
                }
                Err(e) => {
                    tracing::warn!(broker_id, addr = ?addr, "connect failed: {e}");
                }
            }
        }
        Err(format!("could not connect to broker {broker_id}"))
    }

}

mod bootstrap;
