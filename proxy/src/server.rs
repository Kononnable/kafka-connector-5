//! TCP proxy server that accepts Kafka client connections and forwards
//! them to an upstream Kafka broker using bidirectional copy, with
//! frame inspection, logging, latency tracking, and Metadata rewrite.

use crate::config::ProxyConfig;
use crate::error::ProxyError;
use crate::frame;
use crate::tracker::RequestTracker;
use bytes::{BufMut, Bytes, BytesMut};
use protocol::generated::MetadataResponse;
use protocol::traits::{ApiResponse, ApiVersion};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// The Kafka proxy server.
#[derive(Debug, Clone)]
pub struct ProxyServer {
    config: Arc<ProxyConfig>,
}

impl ProxyServer {
    pub fn new(config: ProxyConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    pub async fn run(&self) -> Result<(), ProxyError> {
        let listener = TcpListener::bind(self.config.listen_addr).await?;
        tracing::info!(
            "Kafka proxy listening on {} -> {}",
            self.config.listen_addr,
            self.config.broker_addr
        );

        loop {
            let (client, addr) = listener.accept().await?;
            tracing::info!("accepted connection from {}", addr);
            let config = Arc::clone(&self.config);

            tokio::spawn(async move {
                if let Err(e) = handle_connection(client, config).await {
                    tracing::error!("connection error from {}: {}", addr, e);
                }
            });
        }
    }
}

/// Per-connection handler: pipes client ↔ broker, inspecting frames as they pass.
async fn handle_connection(
    client: TcpStream,
    config: Arc<ProxyConfig>,
) -> Result<(), ProxyError> {
    let broker = TcpStream::connect(config.broker_addr).await.map_err(|e| {
        ProxyError::Upstream(format!("failed to connect to broker {}: {}", config.broker_addr, e))
    })?;

    tracing::info!("connected to upstream broker at {}", config.broker_addr);

    let (client_r, client_w) = client.into_split();
    let (broker_r, broker_w) = broker.into_split();

    let tracker = Arc::new(Mutex::new(RequestTracker::new()));
    let tracker2 = Arc::clone(&tracker);
    let config2 = Arc::clone(&config);

    let c2b = tokio::spawn(async move {
        pipe_client_to_broker(client_r, broker_w, tracker, config).await
    });

    let b2c = tokio::spawn(async move {
        pipe_broker_to_client(broker_r, client_w, tracker2, config2).await
    });

    let _ = tokio::try_join!(c2b, b2c);
    Ok(())
}

// ── Client → Broker (request direction) ───────────────────────────────

async fn pipe_client_to_broker<R, W>(
    mut reader: R,
    mut writer: W,
    tracker: Arc<Mutex<RequestTracker>>,
    _config: Arc<ProxyConfig>,
) -> Result<(), ProxyError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        let n = tokio::io::AsyncReadExt::read_buf(&mut reader, &mut buf).await?;
        if n == 0 {
            tracing::debug!("client→broker: source EOF");
            break;
        }

        inspect_requests(&buf, &tracker).await;

        tokio::io::AsyncWriteExt::write_all_buf(&mut writer, &mut buf).await?;
    }
    Ok(())
}

// ── Broker → Client (response direction, with Metadata rewrite) ───────

async fn pipe_broker_to_client<R, W>(
    mut reader: R,
    mut writer: W,
    tracker: Arc<Mutex<RequestTracker>>,
    config: Arc<ProxyConfig>,
) -> Result<(), ProxyError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut buf = BytesMut::with_capacity(64 * 1024);

    loop {
        let n = tokio::io::AsyncReadExt::read_buf(&mut reader, &mut buf).await?;
        if n == 0 {
            tracing::debug!("broker→client: source EOF");
            break;
        }

        inspect_and_rewrite_responses(&mut buf, &tracker, &config).await?;

        tokio::io::AsyncWriteExt::write_all_buf(&mut writer, &mut buf).await?;
    }
    Ok(())
}

// ── Request inspection (read-only) ────────────────────────────────────

async fn inspect_requests(buf: &BytesMut, tracker: &Arc<Mutex<RequestTracker>>) {
    let mut offset = 0;
    loop {
        let remaining = &buf[offset..];
        let Some((parsed, consumed)) = frame::consume_frame(remaining, true) else {
            break;
        };

        if let Some(ref hdr) = parsed.request {
            tracing::info!(
                "→ REQ  corr={} api={}({}) v={} client={} | {} bytes",
                hdr.correlation_id,
                frame::api_key_name(hdr.api_key),
                hdr.api_key,
                hdr.api_version,
                hdr.client_id,
                parsed.size,
            );
            let mut t = tracker.lock().await;
            let inflight = t.track_request(hdr);
            tracing::debug!(
                "tracking req  corr={} api={}({}) v={} | in-flight={}",
                hdr.correlation_id,
                frame::api_key_name(hdr.api_key),
                hdr.api_key,
                hdr.api_version,
                inflight,
            );
        }
        offset += consumed;
    }
}

// ── Response inspection + Metadata rewrite ────────────────────────────

async fn inspect_and_rewrite_responses(
    buf: &mut BytesMut,
    tracker: &Arc<Mutex<RequestTracker>>,
    config: &ProxyConfig,
) -> Result<(), ProxyError> {
    let mut offset = 0;
    loop {
        let remaining = &buf[offset..];
        let Some((parsed, consumed)) = frame::consume_frame(remaining, false) else {
            break;
        };

        if let Some(ref res) = parsed.response {
            tracing::info!(
                "← RES  corr={} | {} bytes",
                res.correlation_id,
                parsed.size,
            );

            // Look up the matching request to get api_key and version
            let meta_version = {
                let mut t = tracker.lock().await;
                if let Some(completion) = t.complete_response(res.correlation_id) {
                    tracing::info!(
                        "latency  corr={} api={}({}) v={} client={} | {:?}",
                        completion.correlation_id,
                        frame::api_key_name(completion.api_key),
                        completion.api_key,
                        completion.api_version,
                        completion.client_id,
                        completion.latency,
                    );
                    if completion.api_key == 3 {
                        Some(completion.api_version)
                    } else {
                        None
                    }
                } else {
                    tracing::warn!(
                        "orphan response corr={} (no matching request)",
                        res.correlation_id,
                    );
                    None
                }
            };

            // If this was a Metadata response, rewrite broker addresses
            if let Some(api_version) = meta_version {
                rewrite_metadata_in_buf(buf, offset, consumed, api_version, config)?;
            }
        }

        offset += consumed;
    }
    Ok(())
}

// ── Metadata rewrite logic ────────────────────────────────────────────

/// Rewrite a Metadata response frame in-place within `buf`.
///
/// The frame starts at `buf[offset..offset+frame_size]`.
fn rewrite_metadata_in_buf(
    buf: &mut BytesMut,
    offset: usize,
    frame_size: usize,
    api_version: i16,
    config: &ProxyConfig,
) -> Result<(), ProxyError> {
    // Frame layout: [4-byte size] [ResponseHeader] [ResponseBody]
    // ResponseHeader v0:  correlation_id (i32) = 4 bytes
    // ResponseHeader v1+: correlation_id (i32) + tag_buffer (unsigned varint)
    let is_flexible = api_version >= 9;
    let frame_body = &buf[offset + 4..offset + frame_size];
    let header_len = frame::response_body_offset(frame_body, is_flexible);
    let body_bytes = &frame_body[header_len..];

    let version = ApiVersion::new(api_version);

    let mut body_buf = Bytes::copy_from_slice(body_bytes);
    let mut response = match MetadataResponse::deserialize(version, &mut body_buf) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                "failed to deserialize MetadataResponse v{api_version}: {e} — forwarding unchanged"
            );
            return Ok(());
        }
    };

    // Rewrite broker addresses
    let proxy_host = config.proxy_host();
    let proxy_port = config.proxy_port();
    for broker in &mut response.brokers {
        tracing::info!(
            "rewriting broker {}: {}:{} → {}:{}",
            broker.node_id,
            broker.host,
            broker.port,
            proxy_host,
            proxy_port,
        );
        broker.host = proxy_host.clone();
        broker.port = proxy_port;
    }

    // Re-serialize the body using version-aware ApiResponse::serialize
    let mut new_body = BytesMut::new();
    if let Err(e) = response.serialize(version, &mut new_body) {
        tracing::warn!("failed to serialize MetadataResponse v{api_version}: {e}");
        return Ok(());
    }

    // Build new frame: [size: i32] [header] [body]
    let total_body_size = header_len + new_body.len();
    if total_body_size > i32::MAX as usize {
        tracing::warn!("rewritten MetadataResponse too large");
        return Ok(());
    }

    let mut new_frame = BytesMut::with_capacity(4 + total_body_size);
    new_frame.put_i32(total_body_size as i32);
    new_frame.extend_from_slice(&frame_body[..header_len]);
    new_frame.extend_from_slice(&new_body);

    // Replace bytes in the original buffer
    if new_frame.len() <= frame_size {
        buf[offset..offset + new_frame.len()].copy_from_slice(&new_frame);
    } else {
        let tail = buf.split_off(offset + frame_size);
        let head = buf.split_to(offset);
        buf.clear();
        buf.extend_from_slice(&head);
        buf.extend_from_slice(&new_frame);
        buf.extend_from_slice(&tail);
    }

    tracing::debug!(
        "rewrote MetadataResponse: {} bytes → {} bytes, brokers point to {}:{}",
        frame_size,
        new_frame.len(),
        proxy_host,
        proxy_port,
    );

    Ok(())
}
