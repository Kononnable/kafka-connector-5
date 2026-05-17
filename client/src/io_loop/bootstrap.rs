use std::net::ToSocketAddrs;
use std::thread;
use std::time::Instant;

use bytes::Bytes;
use mio::net::TcpStream;
use mio::{Events, Interest, Token};
use protocol::generated::{
    ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse,
};
use protocol::traits::ApiVersion;

use super::connection::Connection;
use super::controller::EventLoop;
use super::metadata::BrokerInfo;

impl EventLoop {
    pub(super) fn bootstrap_cluster_connection(&mut self) {
        let timeout = self.options.connection_timeout;
        let retry_delay = self.options.connection_retry_delay;

        loop {
            // Re-resolve on each attempt so DNS changes are picked up.
            let addrs: Vec<_> = self
                .options
                .bootstrap_servers
                .iter()
                .flat_map(|s| {
                    s.to_socket_addrs()
                        .inspect_err(|e| tracing::warn!("failed to resolve {s}: {e}"))
                        .unwrap_or_default()
                })
                .collect();

            let mut candidates: Vec<(TcpStream, Token)> = Vec::new();

            for addr in &addrs {
                let token = Token(self.next_token);
                self.next_token += 1;

                if let Ok(mut stream) = TcpStream::connect(*addr)
                    && self
                        .poll
                        .registry()
                        .register(&mut stream, token, Interest::WRITABLE | Interest::READABLE)
                        .is_ok()
                {
                    candidates.push((stream, token));
                }
            }

            if candidates.is_empty() {
                tracing::warn!(
                    "no bootstrap addresses could be resolved, retrying in {retry_delay:?}"
                );
                thread::sleep(retry_delay);
                continue;
            }

            let deadline = Instant::now() + timeout;
            let mut poll_events = Events::with_capacity(candidates.len());

            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }

                if let Err(e) = self.poll.poll(&mut poll_events, Some(remaining)) {
                    match e.kind() {
                        std::io::ErrorKind::Interrupted => continue,
                        _ => {
                            tracing::error!("poll error during bootstrap: {e}");
                            break;
                        }
                    }
                }

                for event in &poll_events {
                    if event.is_writable() {
                        let token = event.token();
                        if let Some(pos) = candidates.iter().position(|(_, t)| *t == token) {
                            let (stream, _) = candidates.swap_remove(pos);

                            let mut conn = Connection::new(
                                token,
                                stream,
                                Some(self.options.client_name.clone()),
                                -1, // node_id unknown before metadata
                            );

                            let addr = conn.peer_addr();
                            let result = self
                                .fetch_api_versions(&mut conn, token)
                                .and_then(|_| self.fetch_metadata(&mut conn, token))
                                .and_then(|resp| self.apply_metadata(&mut conn, resp));

                            if let Err(reason) = result {
                                tracing::warn!("failed to bootstrap via {addr}: {reason}");
                                let _ = self.poll.registry().deregister(conn.stream());
                                continue;
                            }

                            // Close remaining candidates, keep this one.
                            for (mut other, _) in candidates.drain(..) {
                                let _ = self.poll.registry().deregister(&mut other);
                            }
                            let node_id = conn.node_id().to_string();
                            self.connections.push(conn);
                            tracing::info!("connected to broker {node_id} at {addr}");
                            return;
                        }
                    }
                }
            }

            // All candidates exhausted — deregister, sleep, retry.
            for (mut other, _) in candidates.drain(..) {
                let _ = self.poll.registry().deregister(&mut other);
            }

            tracing::warn!(
                "no bootstrap connection within {timeout:?}, retrying in {retry_delay:?}"
            );
            thread::sleep(retry_delay);
        }
    }

    /// Synchronously poll for a complete response frame.
    ///
    /// Blocks the event loop until data arrives or the deadline expires.
    /// Only safe during the bootstrap sequence where no other connections
    /// or channel commands are being processed.
    fn poll_response_frame(
        &mut self,
        conn: &mut Connection,
        token: Token,
        deadline: Instant,
        poll_events: &mut Events,
        ctx: &'static str,
    ) -> Result<Bytes, String> {
        loop {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err(format!("{ctx} timed out"));
            }
            let _ = self.poll.poll(poll_events, Some(rem));
            if poll_events
                .iter()
                .any(|e| e.token() == token && e.is_readable())
            {
                match conn.on_readable() {
                    Ok(0) => return Err("connection closed".into()),
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(format!("read error: {e}")),
                }
                if let Some((_, body)) = conn.read_broker_response() {
                    return Ok(body);
                }
            }
        }
    }

    fn fetch_api_versions(&mut self, conn: &mut Connection, token: Token) -> Result<(), String> {
        let deadline = Instant::now() + self.options.request_timeout;
        let version = ApiVersion::new(0);

        conn.send_api_request(&ApiVersionsRequest::default(), Some(version))
            .map_err(|e| format!("serialize: {e}"))?;

        let mut poll_events = Events::with_capacity(1);
        self.flush_sync(conn, token, deadline, &mut poll_events)?;

        let body =
            self.poll_response_frame(conn, token, deadline, &mut poll_events, "api versions")?;
        let resp = conn
            .decode_response::<ApiVersionsResponse>(body, version)
            .map_err(|e| format!("{e}"))?;
        if resp.error_code == 0 {
            conn.set_api_versions(resp.api_keys);
            Ok(())
        } else {
            Err(format!("broker error: {}", resp.error_code))
        }
    }

    /// Populate the metadata cache by sending a MetadataRequest to the bootstrap broker.
    fn fetch_metadata(
        &mut self,
        conn: &mut Connection,
        token: Token,
    ) -> Result<MetadataResponse, String> {
        let deadline = Instant::now() + self.options.request_timeout;

        let (_, version) = conn
            .send_api_request(&MetadataRequest::default(), None)
            .map_err(|e| format!("serialize MetadataRequest: {e}"))?;

        let mut poll_events = Events::with_capacity(1);
        self.flush_sync(conn, token, deadline, &mut poll_events)?;

        let body = self.poll_response_frame(conn, token, deadline, &mut poll_events, "metadata")?;
        let resp = conn
            .decode_response::<MetadataResponse>(body, version)
            .map_err(|e| format!("{e}"))?;
        Ok(resp)
    }

    /// Populate the cache from a MetadataResponse and set the connection's node_id.
    fn apply_metadata(
        &mut self,
        conn: &mut Connection,
        resp: MetadataResponse,
    ) -> Result<(), String> {
        for (node_id, broker) in &resp.brokers {
            self.metadata_cache.brokers.insert(
                *node_id,
                BrokerInfo {
                    host: broker.host.clone(),
                    port: broker.port,
                },
            );
        }

        // Identify which broker we're connected to by matching host:port.
        // The response key provides the authoritative node_id.
        let peer = conn.peer_addr();
        for (node_id, broker) in &resp.brokers {
            let broker_port = broker.port as u16;
            if broker.host == peer.ip().to_string() && broker_port == peer.port() {
                conn.set_node_id(*node_id);
                return Ok(());
            }
        }
        Err(format!(
            "connected broker at {peer} not found in cluster metadata"
        ))
    }

    /// Synchronously flush the write buffer until empty or deadline expires.
    ///
    /// Only safe during the bootstrap sequence where the event loop is not yet
    /// processing other connections or commands from the channel.
    fn flush_sync(
        &mut self,
        conn: &mut Connection,
        token: Token,
        deadline: Instant,
        poll_events: &mut Events,
    ) -> Result<(), String> {
        while conn.can_write() {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err("write timed out".into());
            }
            let _ = self.poll.poll(poll_events, Some(rem));
            if poll_events
                .iter()
                .any(|e| e.token() == token && e.is_writable())
            {
                conn.on_writable()
                    .map_err(|e| format!("write error: {e}"))?;
            }
        }
        Ok(())
    }
}
