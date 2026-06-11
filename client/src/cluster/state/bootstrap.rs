use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};
use std::{io, thread};

use bytes::Bytes;
use indexmap::IndexMap;
use mio::net::TcpStream;
use mio::{Events, Interest, Token};
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

        while conn.can_write() {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err(format!("{ctx} write timed out"));
            }
            match conn.on_writable() {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.pool.poll_io(poll_events, Some(rem)) {
                        if e.kind() != io::ErrorKind::Interrupted {
                            return Err(format!("write poll error: {e}"));
                        }
                        continue;
                    }
                    if poll_events
                        .iter()
                        .any(|e| e.token() == token && e.is_writable())
                    {
                        continue;
                    }
                }
                Err(e) => return Err(format!("write error: {e}")),
            }
        }

        loop {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err(format!("{ctx} timed out"));
            }
            match conn.on_readable() {
                Ok(n) if n > 0 => {}
                Ok(_) => {
                    if let Err(e) = self.pool.poll_io(poll_events, Some(rem)) {
                        if e.kind() != io::ErrorKind::Interrupted {
                            return Err(format!("read poll error: {e}"));
                        }
                        continue;
                    }
                    if poll_events
                        .iter()
                        .any(|e| e.token() == token && e.is_readable())
                    {
                        continue;
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if let Err(e) = self.pool.poll_io(poll_events, Some(rem)) {
                        if e.kind() != io::ErrorKind::Interrupted {
                            return Err(format!("read poll error: {e}"));
                        }
                        continue;
                    }
                    if poll_events
                        .iter()
                        .any(|e| e.token() == token && e.is_readable())
                    {
                        continue;
                    }
                }
                Err(e) => return Err(format!("read error: {e}")),
            }
            if let Some((_, body)) = conn.read_broker_response() {
                return Ok(body);
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

        // TODO: KIP-511 says to send the max supported version. v1 is used
        // instead because:
        //   - v0 produces header without client_id → broker rejects
        //   - v3+ is rejected by this broker despite claiming max_version=3
        //   - v1 works and the response reveals the true max version
        conn.send_api_request(
            &ApiVersionsRequest::default(),
            Some(ApiVersion::new(1)),
            RequestHandlerId(0),
            self.options.request_timeout,
        )
        .map_err(|e| format!("serialize: {e}"))?;

        let mut body = self.flush_and_poll_response(
            conn,
            token,
            &mut poll_events,
            request_timeout,
            "api versions",
        )?;

        let keys = Self::decode_api_versions(&mut body, version)?;
        conn.set_api_versions(keys);
        Ok(())
    }

    fn decode_api_versions(
        body: &mut Bytes,
        version: ApiVersion,
    ) -> Result<IndexMap<i16, ApiVersionEntry>, String> {
        // Always decode the response header as non-flexible first (v0-style).
        // The broker may respond in v0 format even for higher version requests
        // (e.g., UnsupportedVersion error), and the flexible compact-array
        // decoder panics on standard-format arrays.
        let _ = ResponseHeader::decode(body, false)
            .map_err(|e| format!("invalid response header: {e}"))?;

        // Decode with version 0 — the bootstrap path only needs
        // error_code and api_keys, which v0 provides regardless of
        // the actual response version.
        let resp = ApiVersionsResponse::deserialize(ApiVersion::new(0), body)
            .map_err(|e| format!("decode response v0: {e}"))?;
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
            .send_api_request(
                &MetadataRequest::default(),
                Some(ApiVersion::new(1)),
                RequestHandlerId(0),
                self.options.request_timeout,
            )
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
        let peer_ip = peer.ip().to_string();
        for (node_id, broker) in &resp.brokers {
            if broker.host == peer_ip {
                conn.set_node_id(BrokerId(*node_id));
                return Ok(());
            }
        }
        Err(format!(
            "connected broker at {peer} not found in cluster metadata (brokers: {:?})",
            resp.brokers
                .iter()
                .map(|(id, b)| format!("{}.{}:{}", id, b.host, b.port))
                .collect::<Vec<_>>()
        ))
    }
}
