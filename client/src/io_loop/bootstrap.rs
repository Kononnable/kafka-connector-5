use std::net::ToSocketAddrs;
use std::thread;
use std::time::Instant;

use bytes::{Buf, Bytes};
use indexmap::IndexMap;
use mio::net::TcpStream;
use mio::{Events, Interest, Token};
use protocol::ErrorCode;
use protocol::generated::api_versions_response::ApiVersion as ApiVersionEntry;
use protocol::generated::{
    ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse, ResponseHeader,
};
use protocol::traits::{ApiRequest, ApiResponse, ApiVersion};

use super::connection::Connection;
use super::controller::EventLoop;
use super::metadata::BrokerInfo;

enum ApiVersionNegotiation {
    Done(IndexMap<i16, ApiVersionEntry>),
    Retry(ApiVersion),
}

impl EventLoop {
    pub(super) fn bootstrap_cluster_connection(&mut self) {
        let timeout = self.options.connection_timeout;
        let retry_delay = self.options.connection_retry_delay;

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
                                -1,
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

            for (mut other, _) in candidates.drain(..) {
                let _ = self.poll.registry().deregister(&mut other);
            }

            tracing::warn!(
                "no bootstrap connection within {timeout:?}, retrying in {retry_delay:?}"
            );
            thread::sleep(retry_delay);
        }
    }

    /// Flush the write buffer and wait for a response frame,
    /// blocking the event loop until it arrives or `request_timeout` expires.
    fn flush_and_poll_response(
        &mut self,
        conn: &mut Connection,
        token: Token,
        poll_events: &mut Events,
        ctx: &'static str,
    ) -> Result<Bytes, String> {
        let deadline = Instant::now() + self.options.request_timeout;

        while conn.can_write() {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() {
                return Err(format!("{ctx} write timed out"));
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
        let mut poll_events = Events::with_capacity(1);

        // First attempt: send the highest version we support.
        let version = ApiVersionsRequest::get_max_supported_version();
        conn.send_api_request(&ApiVersionsRequest::default(), Some(version))
            .map_err(|e| format!("serialize: {e}"))?;

        let mut body =
            self.flush_and_poll_response(conn, token, &mut poll_events, "api versions")?;

        match Self::process_api_versions_response(&mut body, version)? {
            ApiVersionNegotiation::Done(keys) => {
                conn.set_api_versions(keys);
                Ok(())
            }
            ApiVersionNegotiation::Retry(v) => {
                // Second attempt: use the negotiated version.
                conn.send_api_request(&ApiVersionsRequest::default(), Some(v))
                    .map_err(|e| format!("serialize: {e}"))?;

                let mut body =
                    self.flush_and_poll_response(conn, token, &mut poll_events, "api versions")?;

                match Self::process_api_versions_response(&mut body, v)? {
                    ApiVersionNegotiation::Done(keys) => {
                        conn.set_api_versions(keys);
                        Ok(())
                    }
                    ApiVersionNegotiation::Retry(_) => {
                        Err("broker violated negotiation procedure per KIP-35/KIP-511".into())
                    }
                }
            }
        }
    }

    fn process_api_versions_response(
        body: &mut Bytes,
        version: ApiVersion,
    ) -> Result<ApiVersionNegotiation, String> {
        if ResponseHeader::decode(body, false).is_err() {
            return Err("invalid response header".into());
        }

        let error_code = i16::from_be_bytes(
            body.chunk()[..2]
                .try_into()
                .map_err(|_| "empty response body".to_string())?,
        );

        match error_code {
            0 => {
                let resp = ApiVersionsResponse::deserialize(version, body)
                    .map_err(|e| format!("decode response: {e}"))?;
                if resp.error_code != 0 {
                    return Err(format!("broker error: {}", resp.error_code));
                }
                Ok(ApiVersionNegotiation::Done(resp.api_keys))
            }

            n if n == ErrorCode::UnsupportedVersion as i16 => {
                let resp = ApiVersionsResponse::deserialize(ApiVersion::new(0), body)
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

                Ok(ApiVersionNegotiation::Retry(negotiated))
            }

            n => Err(format!("broker error: {n}")),
        }
    }

    fn fetch_metadata(
        &mut self,
        conn: &mut Connection,
        token: Token,
    ) -> Result<MetadataResponse, String> {
        let (_, version) = conn
            .send_api_request(&MetadataRequest::default(), None)
            .map_err(|e| format!("serialize MetadataRequest: {e}"))?;

        let mut poll_events = Events::with_capacity(1);
        let body = self.flush_and_poll_response(conn, token, &mut poll_events, "metadata")?;
        let resp = conn
            .decode_response::<MetadataResponse>(body, version)
            .map_err(|e| format!("{e}"))?;
        Ok(resp)
    }

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
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use protocol::ErrorCode;
    use protocol::generated::{ApiVersionsRequest, FetchRequest, MetadataRequest};
    use protocol::traits::{ApiKey, ApiRequest, ApiVersion};

    use super::super::controller::EventLoop;
    use super::ApiVersionNegotiation;

    fn make_response(error_code: Option<ErrorCode>, keys: &[(ApiKey, i16, i16)]) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&0i32.to_be_bytes());
        buf.extend_from_slice(&error_code.map_or(0, |e| e as i16).to_be_bytes());
        buf.extend_from_slice(&(keys.len() as i32).to_be_bytes());
        for &(key, min, max) in keys {
            buf.extend_from_slice(&key.0.to_be_bytes());
            buf.extend_from_slice(&min.to_be_bytes());
            buf.extend_from_slice(&max.to_be_bytes());
        }
        buf.freeze()
    }

    #[test_log::test]
    fn success_with_version_0_returns_done() {
        let mut body = make_response(
            None,
            &[
                (MetadataRequest::get_api_key(), 0, 9),
                (FetchRequest::get_api_key(), 0, 12),
            ],
        );

        let result = EventLoop::process_api_versions_response(&mut body, ApiVersion::new(0));

        match result.unwrap() {
            ApiVersionNegotiation::Done(keys) => {
                assert_eq!(
                    keys.get(&MetadataRequest::get_api_key().0)
                        .unwrap()
                        .max_version,
                    9
                );
                assert_eq!(
                    keys.get(&FetchRequest::get_api_key().0)
                        .unwrap()
                        .max_version,
                    12
                );
            }
            _ => panic!("expected Done"),
        }
    }

    #[test_log::test]
    fn unsupported_version_with_api_versions_request_key_returns_retry() {
        let mut body = make_response(
            Some(ErrorCode::UnsupportedVersion),
            &[
                (ApiVersionsRequest::get_api_key(), 0, 1),
                (ApiKey::new(0), 0, 10),
            ],
        );

        let result = EventLoop::process_api_versions_response(&mut body, ApiVersion::new(4));

        match result.unwrap() {
            ApiVersionNegotiation::Retry(v) => assert_eq!(v.0, 1),
            _ => panic!("expected Retry"),
        }
    }

    #[test_log::test]
    fn unsupported_version_without_api_versions_request_key_falls_back_to_v0() {
        let mut body = make_response(
            Some(ErrorCode::UnsupportedVersion),
            &[(ApiKey::new(0), 0, 10)],
        );

        let result = EventLoop::process_api_versions_response(&mut body, ApiVersion::new(4));

        match result.unwrap() {
            ApiVersionNegotiation::Retry(v) => assert_eq!(v.0, 0),
            _ => panic!("expected Retry(v0)"),
        }
    }

    #[test_log::test]
    fn other_error_code_returns_error() {
        let mut body = make_response(Some(ErrorCode::TopicAlreadyExists), &[]);

        let result = EventLoop::process_api_versions_response(&mut body, ApiVersion::new(4));
        assert!(result.is_err());
    }
}
