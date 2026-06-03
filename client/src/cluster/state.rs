use std::net::ToSocketAddrs;

use mio::net::TcpStream;
use mio::{Events, Interest, Token, Waker};
use protocol::traits::{ApiRequest, ApiVersion};

use super::ClusterOptions;
use crate::connection::{Connection, ConnectionPool};
use crate::metadata::MetadataCache;

/// Owns the connection pool, metadata cache, and the mio `Poll` instance.
pub struct ClusterState {
    options: ClusterOptions,
    pool: ConnectionPool,
    metadata: MetadataCache,
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

    pub fn dispatch_io_events(&mut self, events: &Events) {
        for event in events {
            let token = event.token();
            if token == Token(0) {
                continue;
            }
            if let Some(conn) = self.pool().find_by_token(token) {
                if event.is_readable() {
                    let _ = conn.on_readable();
                }
                if event.is_writable()
                    && let Err(e) = conn.on_writable()
                {
                    tracing::error!("write error on connection {}: {e}", conn.node_id());
                }
            }
        }
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

        self.pool().enqueue(broker_id, request, version);
        Ok(())
    }

    /// Establish connections for brokers with queued requests.
    pub fn connect_to_brokers(&mut self) {
        for broker_id in self.pool().pending_brokers() {
            if self.pool().connections().iter().any(|c| c.node_id() == broker_id) {
                continue;
            }
            if let Err(e) = self.try_connect(broker_id) {
                tracing::warn!("connect to broker {broker_id} failed: {e}");
            }
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
                        .pool()
                        .registry()
                        .register(&mut stream, token, Interest::READABLE | Interest::WRITABLE)
                        .is_ok()
                    {
                        let conn = Connection::new(
                            token,
                            stream,
                            Some(self.pool().client_id().to_string()),
                            broker_id,
                        );
                        self.pool().push(conn);
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

    pub fn pool(&mut self) -> &mut ConnectionPool {
        &mut self.pool
    }

    pub fn metadata_mut(&mut self) -> &mut MetadataCache {
        &mut self.metadata
    }
}

mod bootstrap;
