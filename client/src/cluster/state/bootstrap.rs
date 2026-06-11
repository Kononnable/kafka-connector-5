use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};
use std::{io, thread};

use bytes::Bytes;
use indexmap::IndexMap;
use mio::net::TcpStream;
use mio::{Events, Interest, Token};
use protocol::ErrorCode;
use protocol::generated::api_versions_response::ApiVersion as ApiVersionEntry;
use protocol::generated::{
    ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse, ResponseHeader,
};
use protocol::traits::{ApiRequest, ApiResponse, ApiVersion};

use super::ClusterState;
use crate::connection::Connection;
use crate::types::{BrokerId, RequestHandlerId};

impl ClusterState {
    /// Bootstrap the cluster connection.
    ///
    /// 1. Resolve bootstrap server addresses.
    /// 2. Connect to each in parallel.
    /// 3. Negotiate API versions.
    /// 4. Fetch the initial MetadataResponse.
    /// 5. Populate the metadata cache and assign the connected broker's node id.
    ///
    /// Blocks the calling thread until a connection to the cluster is established.
    pub fn bootstrap(&mut self) {
        loop {
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

            if addrs.is_empty() {
                tracing::warn!(
                    "no bootstrap addresses could be resolved, retrying in {:?}",
                    self.options.connection_retry_delay,
                );
                thread::sleep(self.options.connection_retry_delay);
                continue;
            }

            let mut candidates: Vec<(TcpStream, Token)> = Vec::new();

            for addr in &addrs {
                let token = Token(self.next_connection_token);
                self.next_connection_token += 1;
                if let Ok(mut stream) = TcpStream::connect(*addr)
                    && self
                        .pool
                        .registry()
                        .register(&mut stream, token, Interest::WRITABLE | Interest::READABLE)
                        .is_ok()
                {
                    candidates.push((stream, token));
                }
            }

            if candidates.is_empty() {
                tracing::warn!(
                    "no bootstrap connections could be established, retrying in {:?}",
                    self.options.connection_retry_delay,
                );
                thread::sleep(self.options.connection_retry_delay);
                continue;
            }

            let deadline = Instant::now() + self.options.connection_timeout;
            let mut poll_events = Events::with_capacity(candidates.len());

            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }

                if let Err(e) = self.pool.poll_io(&mut poll_events, Some(remaining)) {
                    match e.kind() {
                        io::ErrorKind::Interrupted => continue,
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
                                BrokerId(-1),
                            );

                            let addr = conn.peer_addr();
                            let result = self
                                .bootstrap_api_versions(&mut conn, self.options.request_timeout)
                                .and_then(|_| {
                                    self.bootstrap_metadata(&mut conn, self.options.request_timeout)
                                })
                                .and_then(|resp| {
                                    self.metadata.bootstrap(&resp);
                                    self.bootstrap_assign_node_id(&mut conn, &resp)
                                });

                            if let Err(reason) = result {
                                tracing::warn!("failed to bootstrap via {addr}: {reason}");
                                let _ = self.pool.registry().deregister(conn.stream());
                                continue;
                            }

                            // Close remaining candidates.
                            for (mut other, _) in candidates.drain(..) {
                                let _ = self.pool.registry().deregister(&mut other);
                            }

                            let node_id = conn.node_id();
                            self.pool.push(conn);
                            tracing::info!("connected to broker {node_id} at {addr}");
                            return;
                        }
                    }
                }
            }

            // Clean up straggler candidates.
            for (mut other, _) in candidates.drain(..) {
                let _ = self.pool.registry().deregister(&mut other);
            }

            tracing::warn!(
                "no bootstrap connection within {:?}, retrying in {:?}",
                self.options.connection_timeout,
                self.options.connection_retry_delay,
            );
            thread::sleep(self.options.connection_retry_delay);
        }
    }

    /// Spin the poll loop until `conn` has flushed its send buffer and a
    /// complete response frame arrives in its read buffer.
    fn flush_and_poll_response(
        &mut self,
        conn: &mut Connection,
        token: Token,
        poll_events: &mut Events,
        request_timeout: Duration,
        ctx: &'static str,
    ) -> Result<Bytes, String> {
        let deadline = Instant::now() + request_timeout;

        // Flush write buffer.
        while conn.can_write() {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err(format!("{ctx} write timed out"));
            }
            let _ = self.pool.poll_io(poll_events, Some(rem));
            if poll_events
                .iter()
                .any(|e| e.token() == token && e.is_writable())
            {
                conn.on_writable()
                    .map_err(|e| format!("write error: {e}"))?;
            }
        }

        // Read response.
        loop {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err(format!("{ctx} timed out"));
            }
            let _ = self.pool.poll_io(poll_events, Some(rem));
            if poll_events
                .iter()
                .any(|e| e.token() == token && e.is_readable())
            {
                match conn.on_readable() {
                    Ok(0) => return Err("connection closed".into()),
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(format!("read error: {e}")),
                }
                if let Some((_, body)) = conn.read_broker_response() {
                    return Ok(body);
                }
            }
        }
    }

    fn bootstrap_api_versions(
        &mut self,
        conn: &mut Connection,
        request_timeout: Duration,
    ) -> Result<(), String> {
        let mut poll_events = Events::with_capacity(1);
        let version = ApiVersionsRequest::get_max_supported_version();
        let token = conn.token();

        conn.send_api_request(&ApiVersionsRequest::default(), Some(version), RequestHandlerId(0), self.options.request_timeout)
            .map_err(|e| format!("serialize: {e}"))?;

        let mut body = self.flush_and_poll_response(
            conn,
            token,
            &mut poll_events,
            request_timeout,
            "api versions",
        )?;

        // Try full version first.
        if let Ok(keys) = Self::decode_api_versions(&mut body, version) {
            conn.set_api_versions(keys);
            return Ok(());
        }

        // Check if the error was UnsupportedVersion — retry with v0.
        // body still contains the raw response after the frame length and
        // response header have been consumed (by read_broker_response), so
        // the first 4 bytes are the correlation_id.
        if body.len() < 6 {
            return Err("truncated api versions response".into());
        }
        let error_code = i16::from_be_bytes([body[4], body[5]]);
        if error_code != ErrorCode::UnsupportedVersion as i16 {
            return Err(format!("api versions error: {error_code}"));
        }

        // Fallback: decode v0 to find the max supported version.
        let resp = ApiVersionsResponse::deserialize(ApiVersion::new(0), &mut body)
            .map_err(|e| format!("decode v0 fallback: {e}"))?;

        let negotiated = resp
            .api_keys
            .get(&ApiVersionsRequest::get_api_key().0)
            .map_or(ApiVersion::new(0), |entry| {
                ApiVersion::new(
                    ApiVersionsRequest::get_max_supported_version()
                        .0
                        .min(entry.max_version),
                )
            });

        conn.send_api_request(&ApiVersionsRequest::default(), Some(negotiated), RequestHandlerId(0), self.options.request_timeout)
            .map_err(|e| format!("serialize retry: {e}"))?;

        let mut body = self.flush_and_poll_response(
            conn,
            token,
            &mut poll_events,
            request_timeout,
            "api versions retry",
        )?;

        let keys = Self::decode_api_versions(&mut body, negotiated)?;
        conn.set_api_versions(keys);
        Ok(())
    }

    fn decode_api_versions(
        body: &mut Bytes,
        version: ApiVersion,
    ) -> Result<IndexMap<i16, ApiVersionEntry>, String> {
        let _ = ResponseHeader::decode(body, false)
            .map_err(|e| format!("invalid response header: {e}"))?;

        let resp = ApiVersionsResponse::deserialize(version, body)
            .map_err(|e| format!("decode response: {e}"))?;

        if resp.error_code != 0 {
            return Err(format!("broker error: {}", resp.error_code));
        }
        Ok(resp.api_keys)
    }

    fn bootstrap_metadata(
        &mut self,
        conn: &mut Connection,
        request_timeout: Duration,
    ) -> Result<MetadataResponse, String> {
        let token = conn.token();
        let (_, version) = conn
            .send_api_request(&MetadataRequest::default(), None, RequestHandlerId(0), self.options.request_timeout)
            .map_err(|e| format!("serialize MetadataRequest: {e}"))?;

        let mut poll_events = Events::with_capacity(1);
        let body = self.flush_and_poll_response(
            conn,
            token,
            &mut poll_events,
            request_timeout,
            "metadata",
        )?;

        conn.decode_response::<MetadataResponse>(body, version)
            .map_err(|e| format!("{e}"))
    }

    fn bootstrap_assign_node_id(
        &self,
        conn: &mut Connection,
        resp: &MetadataResponse,
    ) -> Result<(), String> {
        let peer = conn.peer_addr();
        for (node_id, broker) in &resp.brokers {
            let broker_port = broker.port as u16;
            if broker.host == peer.ip().to_string() && broker_port == peer.port() {
                conn.set_node_id(BrokerId(*node_id));
                return Ok(());
            }
        }
        Err(format!(
            "connected broker at {peer} not found in cluster metadata"
        ))
    }
}
