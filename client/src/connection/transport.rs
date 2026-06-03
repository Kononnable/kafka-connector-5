use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;
use mio::Token;
use mio::net::TcpStream;
use protocol::generated::api_versions_response::ApiVersion as ApiVersionEntry;
use protocol::generated::{ApiVersionsRequest, RequestHeader, ResponseHeader, api_key_name};
use protocol::traits::{ApiRequest, ApiResponse, ApiVersion, SerializationError};

pub struct Connection {
    token: Token,
    stream: TcpStream,
    client_id: Option<String>,
    /// The broker's node id. `-1` until the initial MetadataResponse arrives.
    node_id: i32,
    next_correlation_id: i32,
    // TODO: pick a sensible initial capacity
    write_buffer: BytesMut,
    // TODO: pick a sensible initial capacity
    read_buffer: BytesMut,
    /// Bytes ready to be written to the socket.
    bytes_to_send: BytesMut,
    /// Cached ApiVersions entries from this broker.
    api_versions: IndexMap<i16, ApiVersionEntry>,
}

impl Connection {
    pub fn new(token: Token, stream: TcpStream, client_id: Option<String>, node_id: i32) -> Self {
        Connection {
            token,
            stream,
            client_id,
            node_id,
            next_correlation_id: 0,
            write_buffer: BytesMut::new(),
            read_buffer: BytesMut::with_capacity(4096),
            bytes_to_send: BytesMut::new(),
            api_versions: IndexMap::new(),
        }
    }

    pub fn token(&self) -> Token {
        self.token
    }

    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    /// Serialize `request` into a full Kafka frame and store it in
    /// `bytes_to_send`.  `self.write_buffer` is left empty for reuse.
    ///
    /// Returns the correlation id **and** the actual version used (useful
    /// when `version` is `None` and negotiation happens internally).
    pub fn send_api_request<R: ApiRequest>(
        &mut self,
        request: &R,
        version: Option<ApiVersion>,
    ) -> Result<(i32, ApiVersion), SerializationError> {
        let version = match version {
            Some(v) => v,
            None => {
                let client_max = R::get_max_supported_version().0;
                let broker_entry = self.api_versions.get(&R::get_api_key().0);
                match broker_entry {
                    Some(entry) => ApiVersion::new(client_max.min(entry.max_version)),
                    None => {
                        return Err(SerializationError::UnsupportedVersion {
                            api: api_key_name(R::get_api_key().0),
                            version: R::get_min_supported_version(),
                        });
                    }
                }
            }
        };

        // Before we know broker capabilities, only ApiVersionsRequest is allowed.
        if self.api_versions.is_empty() {
            if R::get_api_key() != ApiVersionsRequest::get_api_key() {
                return Err(SerializationError::UnsupportedVersion {
                    api: api_key_name(R::get_api_key().0),
                    version,
                });
            }
        } else {
            let supported = self.api_versions.get(&R::get_api_key().0);
            if supported.is_none() {
                return Err(SerializationError::UnsupportedVersion {
                    api: api_key_name(R::get_api_key().0),
                    version,
                });
            } else if let Some(v) = supported
                && (version.0 < v.min_version || version.0 > v.max_version)
            {
                return Err(SerializationError::UnsupportedVersion {
                    api: api_key_name(R::get_api_key().0),
                    version,
                });
            }
        }

        let correlation_id = self.next_correlation_id;
        self.next_correlation_id += 1;

        // Reserve space for the frame length — filled in at the end.
        self.write_buffer.put_i32(0);

        let header = RequestHeader {
            request_api_key: R::get_api_key().0,
            request_api_version: version.0,
            correlation_id,
            client_id: self.client_id.clone(),
        };
        header.encode(&mut self.write_buffer)?;

        request.serialize(version, &mut self.write_buffer)?;

        // Write the actual frame length over the placeholder.
        let frame_len = (self.write_buffer.len() - 4) as i32;
        self.write_buffer[..4].copy_from_slice(&frame_len.to_be_bytes());

        self.bytes_to_send
            .extend_from_slice(&self.write_buffer.split());
        Ok((correlation_id, version))
    }

    pub fn node_id(&self) -> i32 {
        self.node_id
    }

    /// Set the broker node id.
    ///
    /// Only connections created during the bootstrap sequence should call this
    /// — the node_id is unknown at connect time and filled in after the first
    /// MetadataResponse. Connections to other brokers should have the correct
    /// node_id passed to [`Connection::new`] directly.
    pub(crate) fn set_node_id(&mut self, id: i32) {
        self.node_id = id;
    }

    pub(crate) fn set_api_versions(&mut self, versions: IndexMap<i16, ApiVersionEntry>) {
        self.api_versions = versions;
    }

    /// Returns the peer address of the connected socket.
    pub fn peer_addr(&self) -> std::net::SocketAddr {
        self.stream
            .peer_addr()
            .expect("connected socket always has a peer address")
    }

    /// Decode the response header from `body` and deserialize the payload as `ApiResponse`.
    ///
    /// `body` is the raw bytes for the response header + `ApiResponse`.
    pub fn decode_response<R: ApiResponse>(
        &self,
        mut body: Bytes,
        version: ApiVersion,
    ) -> Result<R, SerializationError> {
        let is_flexible = version.0 >= R::get_min_flexible_version().0;
        ResponseHeader::decode(&mut body, is_flexible)?;
        R::deserialize(version, &mut body)
    }

    /// If `read_buffer` contains a complete response frame, split it off
    /// and return the correlation ID together with the raw body bytes
    /// (everything after the response header within the frame).
    pub fn read_broker_response(&mut self) -> Option<(i32, Bytes)> {
        if self.read_buffer.len() < 4 {
            return None;
        }
        let frame_size = i32::from_be_bytes(self.read_buffer[..4].try_into().unwrap()) as usize;
        if self.read_buffer.len() < 4 + frame_size {
            return None;
        }

        self.read_buffer.advance(4); // consume frame_size
        let corr_id = ResponseHeader::peek_correlation_id(&self.read_buffer[..]).ok()?;
        let body = self.read_buffer.split_to(frame_size);
        Some((corr_id, body.freeze()))
    }

    /// Read bytes from the socket into `read_buffer`.
    /// Returns the number of bytes read, or an error.
    pub fn on_readable(&mut self) -> std::io::Result<usize> {
        use std::io::Read;

        let mut total = 0;
        loop {
            let mut tmp = [0u8; 4096];
            match self.stream.read(&mut tmp) {
                Ok(0) => {
                    return if total > 0 {
                        Ok(total)
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "connection closed by peer",
                        ))
                    };
                }
                Ok(n) => {
                    self.read_buffer.extend_from_slice(&tmp[..n]);
                    total += n;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(total);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Returns `true` while there are bytes waiting to be written to the socket.
    pub fn can_write(&self) -> bool {
        !self.bytes_to_send.is_empty()
    }

    /// Write bytes from the internal buffer to the socket.
    /// Returns the number of bytes written, or an error.
    pub fn on_writable(&mut self) -> std::io::Result<usize> {
        use std::io::Write;

        let mut total = 0;
        while !self.bytes_to_send.is_empty() {
            match self.stream.write(&self.bytes_to_send) {
                Ok(n) => {
                    let _ = self.bytes_to_send.split_to(n);
                    total += n;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(total),
                Err(e) => return Err(e),
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::thread;
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use indexmap::IndexMap;
    use mio::Token;
    use mio::net::TcpStream;
    use protocol::generated::api_versions_response::ApiVersion as ApiVersionEntry;
    use protocol::generated::{
        ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, RequestHeader, ResponseHeader,
    };
    use protocol::traits::{ApiRequest, ApiResponse, ApiVersion, SerializationError};

    use super::Connection;

    /// Read the body of a single Kafka frame from the peer (frame length
    /// prefix consumed, not included in the returned bytes).
    fn read_frame_body(peer: &mut std::net::TcpStream) -> Bytes {
        let mut len_buf = [0u8; 4];
        peer.read_exact(&mut len_buf).unwrap();
        let frame_len = i32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; frame_len];
        peer.read_exact(&mut body).unwrap();
        Bytes::from(body)
    }

    /// Create a connected socket pair for IO tests.
    /// The mio-side socket is set to non-blocking (required by mio).
    fn connected_pair() -> (std::net::TcpStream, TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = std::net::TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        (client, TcpStream::from_std(server))
    }

    /// Helper: build a complete kafka response frame (length + response_header + body)
    /// for ApiVersionsResponse.
    fn encode_api_versions_response_frame(correlation_id: i32) -> Bytes {
        let response = ApiVersionsResponse::default();
        let mut buf = BytesMut::new();
        // placeholder length
        buf.extend_from_slice(&[0u8; 4]);
        // response header: just correlation_id (i32) for version 0, non-flexible
        buf.extend_from_slice(&correlation_id.to_be_bytes());
        response.serialize(ApiVersion::new(0), &mut buf).unwrap();
        // fill in length
        let len = (buf.len() - 4) as i32;
        buf[..4].copy_from_slice(&len.to_be_bytes());
        buf.freeze()
    }

    mod send_api_request {
        use super::*;

        #[test_log::test]
        fn test_send_single_request_basic() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, Some("test-client".into()), -1);

            let (corr_id, _ver) = conn
                .send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                .unwrap();
            assert_eq!(corr_id, 0);

            assert!(!conn.bytes_to_send.is_empty(), "expected frame bytes");
            let frame_len = conn.bytes_to_send.len();
            let frame = conn.bytes_to_send.split_to(frame_len);
            let parsed_len = i32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
            assert_eq!(parsed_len, frame.len() - 4, "frame length prefix");

            let header = &frame[4..];
            assert_eq!(
                i16::from_be_bytes(header[..2].try_into().unwrap()),
                18,
                "api_key"
            );
            assert_eq!(
                i16::from_be_bytes(header[2..4].try_into().unwrap()),
                0,
                "api_version"
            );
            assert_eq!(
                i32::from_be_bytes(header[4..8].try_into().unwrap()),
                0,
                "correlation_id"
            );
            assert_eq!(header.len(), 8);
            assert!(conn.write_buffer.is_empty());
        }

        #[test_log::test]
        fn test_send_multiple_requests_correlation_ids() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            for expected_id in 0..5 {
                let (corr_id, _ver) = conn
                    .send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                    .unwrap();
                assert_eq!(corr_id, expected_id, "correlation_id {}", expected_id);
            }

            let total = conn.bytes_to_send.len();
            // each frame: 4 (length) + 8 (header at v0) + 0 (body) = 12 bytes
            assert_eq!(total, 5 * 12, "5 frames x 12 bytes each");

            for i in 0..5 {
                let offset = i * 12;
                let frame_len =
                    i32::from_be_bytes(conn.bytes_to_send[offset..offset + 4].try_into().unwrap());
                assert_eq!(frame_len, 8, "frame {} length", i);

                let api_key = i16::from_be_bytes(
                    conn.bytes_to_send[offset + 4..offset + 6]
                        .try_into()
                        .unwrap(),
                );
                assert_eq!(api_key, 18, "frame {} api_key", i);

                let corr_id = i32::from_be_bytes(
                    conn.bytes_to_send[offset + 8..offset + 12]
                        .try_into()
                        .unwrap(),
                );
                assert_eq!(corr_id, i as i32, "frame {} correlation_id", i);
            }

            assert!(conn.write_buffer.is_empty());
        }

        #[test_log::test]
        fn test_send_request_before_api_versions() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            // ApiVersionsRequest is allowed before api_versions is populated
            let (corr_id, _ver) = conn
                .send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                .unwrap();
            assert_eq!(corr_id, 0);

            // MetadataRequest is rejected before api_versions is populated
            let err = conn
                .send_api_request(&MetadataRequest::default(), Some(ApiVersion::new(0)))
                .unwrap_err();
            assert!(
                matches!(err, SerializationError::UnsupportedVersion { .. }),
                "expected UnsupportedVersion, got {}",
                err
            );
        }

        #[test_log::test]
        fn test_send_request_version_negotiation() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            // pin: update when supported version changes
            assert_eq!(MetadataRequest::get_max_supported_version().0, 13);
            assert_eq!(MetadataRequest::get_min_supported_version().0, 0);

            // case 1: broker_max (8) < client_max (13) -> picks 8
            conn.set_api_versions(IndexMap::from([(
                3,
                ApiVersionEntry {
                    min_version: 2,
                    max_version: 8,
                },
            )]));
            conn.send_api_request(&MetadataRequest::default(), None)
                .unwrap();
            assert_eq!(
                i16::from_be_bytes(conn.bytes_to_send[6..8].try_into().unwrap()),
                8,
                "case 1: capped at broker_max"
            );
            conn.bytes_to_send.clear();

            // case 2: broker_max (20) > client_max (13) -> picks client_max
            conn.set_api_versions(IndexMap::from([(
                3,
                ApiVersionEntry {
                    min_version: 2,
                    max_version: 20,
                },
            )]));
            conn.send_api_request(&MetadataRequest::default(), None)
                .unwrap();
            assert_eq!(
                i16::from_be_bytes(conn.bytes_to_send[6..8].try_into().unwrap()),
                MetadataRequest::get_max_supported_version().0,
                "case 2: capped at client_max"
            );
            conn.bytes_to_send.clear();
        }

        #[test_log::test]
        fn test_send_request_unsupported_version() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            // broker only supports api 0 (Produce), not api 3 (Metadata)
            conn.set_api_versions(IndexMap::from([(
                0,
                ApiVersionEntry {
                    min_version: 0,
                    max_version: 10,
                },
            )]));

            // (a) Nonexistent key
            let err = conn
                .send_api_request(&MetadataRequest::default(), Some(ApiVersion::new(0)))
                .unwrap_err();
            assert!(
                matches!(err, SerializationError::UnsupportedVersion { .. }),
                "expected UnsupportedVersion for absent key, got {}",
                err
            );

            // (b) Version below min_version
            let err = conn
                .send_api_request(&MetadataRequest::default(), Some(ApiVersion::new(1)))
                .unwrap_err();
            assert!(
                matches!(err, SerializationError::UnsupportedVersion { .. }),
                "expected UnsupportedVersion for version < min, got {}",
                err
            );

            // (c) Version above max_version
            let err = conn
                .send_api_request(&MetadataRequest::default(), Some(ApiVersion::new(9)))
                .unwrap_err();
            assert!(
                matches!(err, SerializationError::UnsupportedVersion { .. }),
                "expected UnsupportedVersion for version > max, got {}",
                err
            );
        }

        #[test_log::test]
        fn test_write_buffer_reused() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            conn.send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                .unwrap();
            assert!(conn.write_buffer.is_empty());
            let first_send_len = conn.bytes_to_send.len();

            conn.send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                .unwrap();
            assert!(conn.write_buffer.is_empty());
            assert_eq!(conn.bytes_to_send.len(), first_send_len + 12);
        }
    }

    mod read_broker_response {
        use super::*;

        fn make_frame(correlation_id: i32, body: &[u8]) -> Bytes {
            let frame_len = 4 + body.len(); // 4 for correlation_id
            let mut buf = Vec::with_capacity(4 + frame_len);
            buf.extend_from_slice(&(frame_len as i32).to_be_bytes());
            buf.extend_from_slice(&correlation_id.to_be_bytes());
            buf.extend_from_slice(body);
            Bytes::from(buf)
        }

        #[test_log::test]
        fn test_read_incomplete_frame_header() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);
            conn.read_buffer.extend_from_slice(&[0u8; 3]);
            assert!(conn.read_broker_response().is_none());
        }

        #[test_log::test]
        fn test_read_incomplete_frame_body() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);
            conn.read_buffer.extend_from_slice(&100i32.to_be_bytes());
            conn.read_buffer.extend_from_slice(&[0u8; 50]);
            assert!(conn.read_broker_response().is_none());
        }

        #[test_log::test]
        fn test_read_single_frame() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);
            let frame = make_frame(42, &[1, 2, 3]);
            conn.read_buffer.extend_from_slice(&frame);
            // body includes correlation_id bytes
            let expected_body = Bytes::from(vec![0, 0, 0, 42, 1, 2, 3]);
            assert_eq!(conn.read_broker_response(), Some((42, expected_body)));
            assert!(conn.read_buffer.is_empty());
        }

        #[test_log::test]
        fn test_read_multiple_frames_queued() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);
            let frame1 = make_frame(10, &[0xaa]);
            let frame2 = make_frame(20, &[0xbb, 0xcc]);
            conn.read_buffer.extend_from_slice(&frame1);
            conn.read_buffer.extend_from_slice(&frame2);

            let expected_body1 = Bytes::from(vec![0, 0, 0, 10, 0xaa]);
            let expected_body2 = Bytes::from(vec![0, 0, 0, 20, 0xbb, 0xcc]);
            assert_eq!(conn.read_broker_response(), Some((10, expected_body1)));
            assert_eq!(conn.read_broker_response(), Some((20, expected_body2)));
            assert!(conn.read_broker_response().is_none());
        }

        #[test_log::test]
        fn test_read_exact_frame_boundary() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);
            let frame = make_frame(7, &[]);
            conn.read_buffer.extend_from_slice(&frame);
            let expected_body = Bytes::from(vec![0, 0, 0, 7]);
            assert_eq!(conn.read_broker_response(), Some((7, expected_body)));
            assert!(conn.read_buffer.is_empty());
        }

        #[test_log::test]
        fn test_read_preserves_extra_bytes() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);
            let frame1 = make_frame(1, &[0xdd]);
            conn.read_buffer.extend_from_slice(&frame1);
            // partial next frame header
            conn.read_buffer.extend_from_slice(&[0x00, 0x00, 0x01]);

            let expected_body = Bytes::from(vec![0, 0, 0, 1, 0xdd]);
            assert_eq!(conn.read_broker_response(), Some((1, expected_body)));
            assert_eq!(conn.read_buffer.len(), 3);
        }
    }

    /// Create a connected pair where both sockets have minimal kernel
    /// buffers, so writing more than a few KB triggers WouldBlock.
    fn connected_pair_small_bufs() -> (std::net::TcpStream, Connection) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // Connect and set tiny receive buffer on the reader BEFORE accept
        // so the setting takes effect before any data exchange.
        let peer = std::net::TcpStream::connect(addr).unwrap();
        let rcvbuf: libc::c_int = 512;
        unsafe {
            libc::setsockopt(
                peer.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &rcvbuf as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        };
        let (accepted, _) = listener.accept().unwrap();
        accepted.set_nonblocking(true).unwrap();
        let sndbuf: libc::c_int = 512;
        unsafe {
            libc::setsockopt(
                accepted.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &sndbuf as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        };
        let stream = TcpStream::from_std(accepted);
        let conn = Connection::new(Token(1), stream, None, -1);
        (peer, conn)
    }

    mod on_writable {
        use super::*;

        #[test_log::test]
        fn test_writable_empty_buffer() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);
            assert!(conn.bytes_to_send.is_empty());
            conn.on_writable().unwrap();
        }

        #[test_log::test]
        fn test_writable_single_message() {
            let (mut peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            conn.send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                .unwrap();
            assert!(conn.can_write());

            conn.on_writable().unwrap();
            assert!(!conn.can_write());

            let mut frame = read_frame_body(&mut peer);
            let header = RequestHeader::decode(&mut frame).unwrap();
            assert_eq!(
                header,
                RequestHeader {
                    request_api_key: ApiVersionsRequest::get_api_key().0,
                    request_api_version: 0,
                    correlation_id: 0,
                    client_id: None,
                }
            );
            let request = ApiVersionsRequest::deserialize(ApiVersion::new(0), &mut frame).unwrap();
            assert_eq!(request, ApiVersionsRequest::default());
        }

        #[test_log::test]
        fn test_writable_multiple_messages() {
            let (mut peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            for _ in 0..3 {
                conn.send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                    .unwrap();
            }
            assert!(conn.can_write());

            conn.on_writable().unwrap();
            assert!(!conn.can_write());

            for i in 0..3 {
                let mut frame = read_frame_body(&mut peer);
                let header = RequestHeader::decode(&mut frame).unwrap();
                assert_eq!(
                    header,
                    RequestHeader {
                        request_api_key: ApiVersionsRequest::get_api_key().0,
                        request_api_version: 0,
                        correlation_id: i,
                        client_id: None,
                    }
                );
                let request =
                    ApiVersionsRequest::deserialize(ApiVersion::new(0), &mut frame).unwrap();
                assert_eq!(request, ApiVersionsRequest::default());
            }
        }

        #[test_log::test]
        fn test_writable_partial_write_large_message() {
            let (mut peer, mut conn) = connected_pair_small_bufs();

            // Fill ~100KB — exceeds the tiny socket buffers (~4KB total)
            conn.bytes_to_send.extend_from_slice(&vec![0u8; 100_000]);
            let total = conn.bytes_to_send.len();

            assert!(
                conn.on_writable().unwrap() > 0,
                "on_writable should have written some bytes before WouldBlock"
            );
            assert!(
                !conn.bytes_to_send.is_empty(),
                "all data written without blocking"
            );
            assert!(conn.can_write());

            // Second write before draining should write nothing (still blocked)
            assert_eq!(conn.on_writable().unwrap(), 0);

            // Drain + write in a loop until everything is transferred
            let mut buf = vec![0u8; total];
            let mut received = 0;
            while !conn.bytes_to_send.is_empty() {
                received += peer.read(&mut buf[received..]).unwrap();
                conn.on_writable().unwrap();
            }
            // Drain any remaining bytes still in flight
            while received < total {
                received += peer.read(&mut buf[received..]).unwrap();
            }
            assert!(!conn.can_write());
            assert_eq!(received, total);
        }

        #[test_log::test]
        fn test_writable_write_error() {
            let (peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            // Set SO_LINGER to 0 on the peer to force RST on close
            let linger: libc::linger = libc::linger {
                l_onoff: 1,
                l_linger: 0,
            };
            unsafe {
                libc::setsockopt(
                    peer.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_LINGER,
                    &linger as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::linger>() as libc::socklen_t,
                );
            }
            drop(peer);

            conn.send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                .unwrap();
            let err = conn.on_writable().unwrap_err();
            assert!(!conn.bytes_to_send.is_empty());
            assert!(
                err.kind() == std::io::ErrorKind::ConnectionReset
                    || err.kind() == std::io::ErrorKind::BrokenPipe
                    || err.kind() == std::io::ErrorKind::NotConnected,
                "unexpected error kind: {}",
                err.kind()
            );
        }
    }

    mod on_readable {
        use super::*;

        #[test_log::test]
        fn test_readable_exhausts_available_data() {
            let (mut peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            peer.write_all(&[0xbb; 50]).unwrap();
            // let kernel deliver the data
            thread::sleep(Duration::from_millis(10));

            let n = conn.on_readable().unwrap();
            assert_eq!(n, 50);
            assert_eq!(conn.read_buffer.len(), 50);

            let n = conn.on_readable().unwrap();
            assert_eq!(n, 0);
        }

        #[test_log::test]
        fn test_readable_eof() {
            let (mut peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            peer.write_all(&[0xcc; 30]).unwrap();
            drop(peer);

            let n = conn.on_readable().unwrap();
            assert_eq!(n, 30);

            let err = conn.on_readable().unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
        }

        #[test_log::test]
        fn test_readable_large_data_loops() {
            let (mut peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            // more than the 4096-byte internal temp buffer
            let data = vec![0xdd; 10_000];
            peer.write_all(&data).unwrap();

            let n = conn.on_readable().unwrap();
            assert_eq!(n, 10_000);
            assert_eq!(conn.read_buffer.len(), 10_000);
        }
    }

    mod round_trip {
        use super::*;

        #[test_log::test]
        fn test_multiple_sends_multiple_reads() {
            let (mut peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            conn.set_api_versions(IndexMap::from([(
                ApiVersionsRequest::get_api_key().0,
                ApiVersionEntry {
                    min_version: 0,
                    max_version: 4,
                },
            )]));

            for _ in 0..3 {
                conn.send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                    .unwrap();
            }
            conn.on_writable().unwrap();

            for correlation_id in 0..3 {
                let mut req_frame = read_frame_body(&mut peer);
                let req_header = RequestHeader::decode(&mut req_frame).unwrap();
                assert_eq!(
                    req_header,
                    RequestHeader {
                        request_api_key: ApiVersionsRequest::get_api_key().0,
                        request_api_version: 0,
                        correlation_id,
                        client_id: None,
                    }
                );
                let req_body =
                    ApiVersionsRequest::deserialize(ApiVersion::new(0), &mut req_frame).unwrap();
                assert_eq!(req_body, ApiVersionsRequest::default());
            }

            for corr_id in 0..3 {
                let frame = encode_api_versions_response_frame(corr_id);
                peer.write_all(&frame).unwrap();
            }

            conn.on_readable().unwrap();
            for expected_id in 0..3 {
                let (corr_id, mut body) = conn.read_broker_response().unwrap();
                assert_eq!(corr_id, expected_id);
                let response_header = ResponseHeader::decode(&mut body, false).unwrap();
                assert_eq!(response_header.correlation_id, expected_id);
                let response =
                    ApiVersionsResponse::deserialize(ApiVersion::new(0), &mut body).unwrap();
                assert_eq!(response, ApiVersionsResponse::default());
            }
            assert!(conn.read_broker_response().is_none());
            assert!(
                conn.read_buffer.is_empty(),
                "expected empty read_buffer after consuming all frames"
            );
        }
    }

    mod state_queries {
        use super::*;

        #[test_log::test]
        fn test_can_write_empty() {
            let (_peer, stream) = connected_pair();
            let conn = Connection::new(Token(1), stream, None, -1);
            assert!(!conn.can_write());
        }

        #[test_log::test]
        fn test_can_write_after_send() {
            let (_peer, stream) = connected_pair();
            let mut conn = Connection::new(Token(1), stream, None, -1);

            conn.set_api_versions(IndexMap::from([(
                ApiVersionsRequest::get_api_key().0,
                ApiVersionEntry {
                    min_version: 0,
                    max_version: 4,
                },
            )]));

            assert!(!conn.can_write());
            conn.send_api_request(&ApiVersionsRequest::default(), Some(ApiVersion::new(0)))
                .unwrap();
            assert!(conn.can_write());
            conn.on_writable().unwrap();
            assert!(!conn.can_write());
        }
    }
}
