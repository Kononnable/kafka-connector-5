//! Per-broker connection controller.
//!
//! `BrokerController` manages a single TCP connection to one Kafka broker.
//! It tracks in-flight requests via an incrementing correlation ID, optionally
//! negotiates API versions at connect time, and reports connection status.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use protocol::generated::{ApiVersionsRequest, ApiVersionsResponse};
use protocol::traits::{ApiRequest, ApiResponse, ApiVersion, SerializationError};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Errors originating from the broker controller.
#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] SerializationError),

    #[error("connection to {0} is not established")]
    NotConnected(SocketAddr),

    #[error("response channel closed unexpectedly")]
    ResponseChannelClosed,

    #[error("timed out waiting for response")]
    Timeout,

    #[error("the requested API key {api_key} version {version} is not supported (broker supports {broker_min}-{broker_max})")]
    UnsupportedVersion {
        api_key: i16,
        version: i16,
        broker_min: i16,
        broker_max: i16,
    },

    #[error("broker returned error code {error_code} for api {api_key}")]
    BrokerError { api_key: i16, error_code: i16 },
}

/// Handle for receiving a response to a request.
pub type ResponseReceiver = oneshot::Receiver<Result<Bytes, BrokerError>>;

// ---------------------------------------------------------------------------
// BrokerController
// ---------------------------------------------------------------------------

/// Manages a single connection to a Kafka broker.
///
/// ## Example
///
/// ```ignore
/// let broker = BrokerController::connect("127.0.0.1:9092".parse().unwrap(), None).await?;
/// broker.wait_ready().await;
/// ```
#[derive(Debug)]
pub struct BrokerController {
    /// Address of the upstream broker.
    addr: SocketAddr,

    /// Whether the TCP connection is established.
    connected: Arc<AtomicBool>,

    /// Next correlation ID to use (monotonically increasing).
    next_corr: Arc<AtomicI32>,

    /// Known max supported API versions: api_key -> (min_version, max_version).
    /// `None` means we haven't negotiated yet.
    api_versions: Arc<Mutex<Option<HashMap<i16, (i16, i16)>>>>,

    /// Shared pending responses map, used by the reader task to route responses.
    pending: Arc<Mutex<HashMap<i32, oneshot::Sender<Result<Bytes, BrokerError>>>>>,

    /// Sender for outbound raw bytes to the write task.
    request_tx: mpsc::UnboundedSender<Bytes>,

    /// Channel to signal the background tasks to shut down.
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl BrokerController {
    /// Connect to a broker at `addr`.
    ///
    /// If `api_versions_hint` is `Some`, it is used directly without negotiating.
    /// If `None`, an `ApiVersionsRequest` (v3) is sent on connect to discover
    /// supported versions — this is the recommended path.
    pub async fn connect(
        addr: SocketAddr,
        api_versions_hint: Option<HashMap<i16, (i16, i16)>>,
    ) -> Result<Arc<Self>, BrokerError> {
        let stream = TcpStream::connect(addr).await.map_err(BrokerError::Io)?;
        let connected = Arc::new(AtomicBool::new(true));
        let next_corr = Arc::new(AtomicI32::new(1));
        let api_versions = Arc::new(Mutex::new(api_versions_hint.clone()));
        let pending: Arc<Mutex<HashMap<i32, oneshot::Sender<Result<Bytes, BrokerError>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let controller = Arc::new(Self {
            addr,
            connected: Arc::clone(&connected),
            next_corr: Arc::clone(&next_corr),
            api_versions: Arc::clone(&api_versions),
            pending: Arc::clone(&pending),
            request_tx,
            shutdown_tx,
        });

        let (reader_r, writer_w) = stream.into_split();

        // Spawn the write task
        let sr_w = shutdown_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = write_loop(writer_w, request_rx, sr_w).await {
                warn!("[{addr}] writer loop exited: {e}");
            }
        });

        // Spawn the read task
        let c_r = Arc::clone(&controller);
        tokio::spawn(async move {
            if let Err(e) = c_r.read_loop(reader_r, shutdown_rx, pending).await {
                warn!("[{addr}] read loop exited: {e}");
            }
        });

        // If no api_versions hint, negotiate on connect
        if api_versions_hint.is_none() {
            match controller.negotiate_api_versions().await {
                Ok(versions) => {
                    let mut ap = api_versions.lock().await;
                    *ap = Some(versions);
                    info!("[{addr}] negotiated API versions");
                }
                Err(e) => {
                    warn!("[{addr}] ApiVersions negotiation failed: {e}");
                }
            }
        }

        Ok(controller)
    }

    // ── Public accessors ──────────────────────────────────────────────

    /// The broker address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Whether the TCP connection is currently established.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Return the known API versions for this broker (if negotiated).
    pub async fn api_versions(&self) -> Option<HashMap<i16, (i16, i16)>> {
        self.api_versions.lock().await.clone()
    }

    /// Return the max version for a given API key, if known.
    pub async fn max_version_for(&self, api_key: i16) -> Option<i16> {
        let guard = self.api_versions.lock().await;
        guard
            .as_ref()
            .and_then(|m| m.get(&api_key).copied())
            .map(|(_, max)| max)
    }

    /// Return the min version for a given API key, if known.
    pub async fn min_version_for(&self, api_key: i16) -> Option<i16> {
        let guard = self.api_versions.lock().await;
        guard
            .as_ref()
            .and_then(|m| m.get(&api_key).copied())
            .map(|(min, _)| min)
    }

    // ── Sending requests ──────────────────────────────────────────────

    /// Send a Kafka request to the broker and return a receiver for the raw
    /// response bytes.  The caller is responsible for deserialising the response.
    ///
    /// `api_version` selects the protocol version to use for the request header
    /// *and* the request body serialisation.
    pub async fn send_raw<R: ApiRequest>(
        self: &Arc<Self>,
        req: &R,
        api_version: ApiVersion,
    ) -> Result<ResponseReceiver, BrokerError> {
        if !self.is_connected() {
            return Err(BrokerError::NotConnected(self.addr));
        }

        let corr_id = self.next_corr.fetch_add(1, Ordering::Relaxed);
        let is_flexible = api_version.0 >= 9;

        // Build the raw frame: [size: i32] [RequestHeader] [RequestBody]
        let mut frame = BytesMut::with_capacity(4096);
        frame.put_i32(0); // placeholder for size

        // Request header (always classic encoding for the header itself,
        // even in flexible mode — per Kafka spec)
        frame.put_i16(R::get_api_key().0); // request_api_key
        frame.put_i16(api_version.0); // request_api_version
        frame.put_i32(corr_id); // correlation_id

        // Client ID — always classic nullable string per Kafka spec
        frame.put_i16(-1); // null client ID

        if is_flexible {
            // Tag buffer for request header (always present in flexible mode)
            protocol::protocol::serialization::encode_unsigned_varint(0u64, &mut frame);
        }

        // Serialize the request body
        req.serialize(api_version, &mut frame)?;

        // Patch the size prefix
        let size = (frame.len() - 4) as i32;
        frame[..4].copy_from_slice(&size.to_be_bytes());

        let (tx, rx) = oneshot::channel();

        // Register the pending response before sending
        {
            let mut p = self.pending.lock().await;
            p.insert(corr_id, tx);
        }

        self.request_tx
            .send(frame.freeze())
            .map_err(|_| BrokerError::ResponseChannelClosed)?;

        Ok(rx)
    }

    /// Send a Kafka request and deserialise the response.
    pub async fn send<R: ApiRequest>(
        self: &Arc<Self>,
        req: &R,
        api_version: ApiVersion,
    ) -> Result<R::Response, BrokerError> {
        let rx = self.send_raw(req, api_version).await?;
        let raw_bytes = rx
            .await
            .map_err(|_| BrokerError::ResponseChannelClosed)?
            .map_err(|e| e)?;

        // Deserialize: skip response header
        // ResponseHeader v0: correlation_id (i32) = 4 bytes
        // ResponseHeader v1 (flexible): correlation_id (i32) + tag_buffer varint
        let is_flexible = api_version.0 >= 9;
        let header_size = if is_flexible {
            // correlation_id (4) + tag buffer (varint(0) = 1 byte)
            5
        } else {
            4
        };

        let mut body = Bytes::copy_from_slice(&raw_bytes[header_size..]);
        let resp =
            R::Response::deserialize(api_version, &mut body).map_err(BrokerError::Protocol)?;
        Ok(resp)
    }

    // ── Internal: version negotiation ──────────────────────────────────

    /// Negotiate API versions by sending an ApiVersionsRequest (v3).
    async fn negotiate_api_versions(
        self: &Arc<Self>,
    ) -> Result<HashMap<i16, (i16, i16)>, BrokerError> {
        let req = ApiVersionsRequest {
            client_software_name: "rust-client".into(),
            client_software_version: "0.1.0".into(),
        };
        // Use version 3 which supports flexible encoding + client software fields
        let ver = ApiVersion::new(3);

        let resp: ApiVersionsResponse = self.send(&req, ver).await?;

        if resp.error_code != 0 {
            return Err(BrokerError::BrokerError {
                api_key: 18,
                error_code: resp.error_code,
            });
        }

        let versions: HashMap<i16, (i16, i16)> = resp
            .api_keys
            .iter()
            .map(|ak| (ak.api_key, (ak.min_version, ak.max_version)))
            .collect();

        Ok(versions)
    }

    // ── Internal: read loop ────────────────────────────────────────────

    /// Background task: reads frames from the broker and routes them to pending
    /// response channels by correlation ID.
    async fn read_loop(
        self: &Arc<Self>,
        mut reader: tokio::net::tcp::OwnedReadHalf,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
        pending: Arc<Mutex<HashMap<i32, oneshot::Sender<Result<Bytes, BrokerError>>>>>,
    ) -> Result<(), BrokerError> {
        let mut buf = BytesMut::with_capacity(64 * 1024);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("[{}] read loop shutting down", self.addr);
                        break;
                    }
                }
                result = reader.read_buf(&mut buf) => {
                    let n = result?;
                    if n == 0 {
                        info!("[{}] broker connection closed", self.addr);
                        self.connected.store(false, Ordering::Relaxed);
                        // Fail all pending requests
                        let mut p = pending.lock().await;
                        for (_corr, tx) in p.drain() {
                            let _ = tx.send(Err(BrokerError::NotConnected(self.addr)));
                        }
                        break;
                    }

                    // Try to consume complete frames from the buffer
                    let mut offset = 0;
                    loop {
                        if buf.len() - offset < 4 {
                            break;
                        }
                        let size = u32::from_be_bytes(
                            buf[offset..offset + 4].try_into().unwrap(),
                        ) as usize;
                        let total = 4 + size;
                        if buf.len() - offset < total {
                            break; // frame not complete yet
                        }

                        // Extract correlation_id from the response header (first 4 bytes after size)
                        let corr_id = i32::from_be_bytes(
                            buf[offset + 4..offset + 8].try_into().unwrap(),
                        );

                        // Extract the raw frame body (including the response header)
                        let response_body = buf[offset + 4..offset + total].to_vec();

                        // Route to the pending response channel
                        let mut p = pending.lock().await;
                        if let Some(tx) = p.remove(&corr_id) {
                            let _ = tx.send(Ok(Bytes::from(response_body)));
                        } else {
                            warn!(
                                "[{}] orphan response for correlation_id={}",
                                self.addr, corr_id
                            );
                        }
                        drop(p);

                        offset += total;
                    }

                    // Advance the buffer past consumed bytes
                    if offset > 0 {
                        buf.advance(offset);
                    }
                }
            }
        }

        Ok(())
    }
}

impl Drop for BrokerController {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

// ── Standalone write loop ─────────────────────────────────────────────

/// Background task: reads outbound frames from the channel and writes them
/// to the broker's TCP connection.
async fn write_loop(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<Bytes>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<(), BrokerError> {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    debug!("write loop shutting down");
                    break;
                }
            }
            frame = rx.recv() => {
                match frame {
                    Some(raw) => {
                        if let Err(e) = writer.write_all(&raw).await {
                            warn!("write error: {e}");
                            return Err(BrokerError::Io(e));
                        }
                    }
                    None => {
                        // channel closed
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_controller_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<BrokerController>();
        assert_sync::<BrokerController>();
    }
}
