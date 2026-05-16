use bytes::{Buf, BufMut, Bytes, BytesMut};
use mio::Token;
use mio::net::TcpStream;
use protocol::generated::api_versions_response::ApiVersion as ApiVersionEntry;
use protocol::generated::{ApiVersionsRequest, RequestHeader, ResponseHeader};
use protocol::traits::{ApiRequest, ApiVersion, SerializationError};

pub struct Connection {
    token: Token,
    stream: TcpStream,
    client_id: Option<String>,
    next_correlation_id: i32,
    // TODO: pick a sensible initial capacity
    write_buffer: BytesMut,
    // TODO: pick a sensible initial capacity
    read_buffer: BytesMut,
    /// Bytes ready to be written to the socket.
    bytes_to_send: BytesMut,
    /// Cached ApiVersions entries from this broker.
    api_versions: Vec<ApiVersionEntry>,
}

impl Connection {
    pub fn new(token: Token, stream: TcpStream, client_id: Option<String>) -> Self {
        Connection {
            token,
            stream,
            client_id,
            next_correlation_id: 0,
            write_buffer: BytesMut::new(),
            read_buffer: BytesMut::with_capacity(4096),
            bytes_to_send: BytesMut::new(),
            api_versions: Vec::new(),
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
    pub fn send_api_request<R: ApiRequest>(
        &mut self,
        request: &R,
        version: Option<ApiVersion>,
    ) -> Result<i32, SerializationError> {
        let version = match version {
            Some(v) => v,
            None => {
                let client_max = R::get_max_supported_version().0;
                let broker_entry = self
                    .api_versions
                    .iter()
                    .find(|k| k.api_key == R::get_api_key().0);
                match broker_entry {
                    Some(entry) => ApiVersion::new(client_max.min(entry.max_version)),
                    None => {
                        return Err(SerializationError::UnsupportedVersion(
                            R::get_min_supported_version(),
                        ));
                    }
                }
            }
        };

        // Before we know broker capabilities, only ApiVersionsRequest is allowed.
        if self.api_versions.is_empty() {
            if R::get_api_key() != ApiVersionsRequest::get_api_key() {
                return Err(SerializationError::UnsupportedVersion(version));
            }
        } else {
            let supported = self
                .api_versions
                .iter()
                .find(|v| v.api_key == R::get_api_key().0);
            if supported.is_none() {
                return Err(SerializationError::UnsupportedVersion(version));
            } else if let Some(v) = supported
                && (version.0 < v.min_version || version.0 > v.max_version)
            {
                return Err(SerializationError::UnsupportedVersion(version));
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
        Ok(correlation_id)
    }

    pub(super) fn set_api_versions(&mut self, versions: Vec<ApiVersionEntry>) {
        self.api_versions = versions;
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
                Ok(0) => return Ok(total), // EOF
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

    pub fn on_writable(&mut self) {
        use std::io::Write;

        while !self.bytes_to_send.is_empty() {
            match self.stream.write(&self.bytes_to_send) {
                Ok(n) => {
                    let _ = self.bytes_to_send.split_to(n);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    tracing::error!("write error on connection {}: {e}", self.token.0);
                    break;
                }
            }
        }
    }
}
