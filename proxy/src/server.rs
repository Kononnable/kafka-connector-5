//! TCP proxy server that accepts Kafka client connections and forwards
//! them to an upstream Kafka broker using bidirectional copy, with
//! frame inspection, logging, latency tracking, and Metadata rewrite.

use crate::config::ProxyConfig;
use crate::error::ProxyError;
use crate::frame;
use crate::tracker::RequestTracker;
use bytes::{Buf, Bytes, BytesMut};
use protocol::protocol::serialization::KafkaCodec;

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



/// Try to deserialize a request body, returning a description string.
fn describe_request_body(api_key: i16, version: i16, body: &[u8]) -> String {
    match protocol::generated::decode_request_body(api_key, version, body) {
        Ok(s) => s,
        Err(e) => format!("{} bytes (undecoded: {e})", body.len()),
    }
}

/// Try to deserialize a response body, returning a description string.
fn describe_response_body(api_key: i16, version: i16, body: &[u8]) -> String {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        protocol::generated::decode_response_body(api_key, version, body)
    }));
    match result {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => format!("{} bytes (undecoded: {e})", body.len()),
        Err(_) => format!("{} bytes (panic)", body.len()),
    }
}

async fn inspect_requests(buf: &BytesMut, tracker: &Arc<Mutex<RequestTracker>>) {
    let mut offset = 0;
    loop {
        let remaining = &buf[offset..];
        let Some((parsed, consumed)) = frame::consume_frame(remaining, true) else {
            break;
        };

        if let Some(ref hdr) = parsed.request {
            let frame_body = &remaining[4..consumed];
            let is_flex = frame::is_flexible_api(hdr.api_key, hdr.api_version);
            let body_off = frame::request_body_offset(frame_body, is_flex);
            let body_desc = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                describe_request_body(hdr.api_key, hdr.api_version, &frame_body[body_off..])
            })).unwrap_or_else(|_| format!("{} bytes (panic)", frame_body.len() - body_off));
            tracing::info!(
                "→ REQ  corr={} api={}({}) v={} client={} | {} bytes | {}",
                hdr.correlation_id,
                protocol::generated::api_key_name(hdr.api_key),
                hdr.api_key,
                hdr.api_version,
                hdr.client_id,
                parsed.size,
                body_desc,
            );
            let mut t = tracker.lock().await;
            let inflight = t.track_request(hdr);
            tracing::trace!(
                "tracking req  corr={} api={}({}) v={} | in-flight={}",
                hdr.correlation_id,
                protocol::generated::api_key_name(hdr.api_key),
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
            // Look up the matching request
            let (meta_version, _body_desc) = {
                let mut t = tracker.lock().await;
                if let Some(completion) = t.complete_response(res.correlation_id) {
                    let frame_body = &remaining[4..consumed];
                    let is_flex = completion.api_key != 18
                        && frame::is_flexible_api(completion.api_key, completion.api_version);
                    let body_off = frame::response_body_offset(frame_body, is_flex);
                    let body_desc = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        describe_response_body(completion.api_key, completion.api_version, &frame_body[body_off..])
                    })).unwrap_or_else(|_| format!("{} bytes (panic)", frame_body.len() - body_off));
                    tracing::info!(
                        "← RES  corr={} api={}({}) v={} client={} | {} bytes | {:?} | {}",
                        res.correlation_id,
                        protocol::generated::api_key_name(completion.api_key),
                        completion.api_key,
                        completion.api_version,
                        completion.client_id,
                        parsed.size,
                        completion.latency,
                        body_desc,
                    );
                    let meta = if completion.api_key == 3 { Some(completion.api_version) } else { None };
                    (meta, body_desc)
                } else {
                    let desc = format!("orphan corr={}", res.correlation_id);
                    tracing::info!(
                        "← RES  corr={} | {} bytes | {}",
                        res.correlation_id,
                        parsed.size,
                        desc,
                    );
                    (None, desc)
                }
            };

            if let Some(api_version) = meta_version {
                rewrite_broker_port_in_metadata(buf, offset, consumed, api_version, config)?;
            }
        }

        offset += consumed;
    }
    Ok(())
}

// ── Metadata rewrite: byte-level in-place broker port patching ────────

/// Rewrite broker addresses in a Metadata response by patching the port
/// bytes in-place. Uses the protocol crate's primitive decoders to find
/// the port fields, then overwrites just the 4 i32 bytes in the buffer.
///
/// This avoids full round-trip deserialize/serialize, preserving all
/// tagged fields and unknown schema elements.
fn rewrite_broker_port_in_metadata(
    buf: &mut BytesMut,
    offset: usize,
    frame_size: usize,
    api_version: i16,
    config: &ProxyConfig,
) -> Result<(), ProxyError> {
    let is_flexible = api_version >= 9;
    // Compute header_len and body_bytes WITHOUT holding a borrow on buf
    let header_len = {
        let frame_body = &buf[offset + 4..offset + frame_size];
        frame::response_body_offset(frame_body, is_flexible)
    };
    let body_start = offset + 4 + header_len;
    let body_end = offset + frame_size;
    let body_slice = &buf[body_start..body_end];

    let proxy_host = config.proxy_host();

    // Collect (offset, new_port) pairs
    let patches: Vec<(usize, i32)> = {
        let mut cursor = Bytes::copy_from_slice(body_slice);
        let mut patches = Vec::new();
        let body_total = body_slice.len();
        let ver = protocol::traits::ApiVersion::new(api_version);

        // Skip throttle_time_ms (v3+)
        if api_version >= 3 {
            let _: i32 = match KafkaCodec::decode(&mut cursor, ver, is_flexible) {
                Ok(v) => v,
                Err(_) => return Ok(()),
            };
        }

        // Read broker count
        let broker_count = if is_flexible {
            let (raw, _) =
                protocol::protocol::serialization::decode_unsigned_varint(&mut cursor)
                    .map_err(|e| {
                        ProxyError::Upstream(format!("failed to decode broker count: {e}"))
                    })?;
            if raw == 0 {
                return Ok(());
            }
            (raw - 1) as usize
        } else {
            let count: i32 = KafkaCodec::decode(&mut cursor, ver, is_flexible)
                .map_err(|_| ProxyError::Upstream("failed to decode broker count".into()))?;
            if count < 0 {
                return Ok(());
            }
            count as usize
        };

        for _ in 0..broker_count {
            let _: i32 = match KafkaCodec::decode(&mut cursor, ver, is_flexible) {
                Ok(v) => v,
                Err(_) => return Ok(()),
            };
            let host: String = match KafkaCodec::decode(&mut cursor, ver, is_flexible) {
                Ok(v) => v,
                Err(_) => return Ok(()),
            };
            let port_offset = body_total - cursor.len();
            let port: i32 = match KafkaCodec::decode(&mut cursor, ver, is_flexible) {
                Ok(v) => v,
                Err(_) => return Ok(()),
            };
            let _: Option<String> = match KafkaCodec::decode(&mut cursor, ver, is_flexible) {
                Ok(v) => v,
                Err(_) => return Ok(()),
            };
            if is_flexible {
                let (tag_count, _) =
                    protocol::protocol::serialization::decode_unsigned_varint(&mut cursor)
                        .map_err(|_| ProxyError::Upstream("tag count error".into()))?;
                for _ in 0..tag_count {
                    let (_, _) = protocol::protocol::serialization::decode_unsigned_varint(&mut cursor)
                        .map_err(|_| ProxyError::Upstream("tag id error".into()))?;
                    let (len, _) = protocol::protocol::serialization::decode_unsigned_varint(&mut cursor)
                        .map_err(|_| ProxyError::Upstream("tag len error".into()))?;
                    cursor.advance(len as usize);
                }
            }
            // Rewrite any broker matching proxy_host that has a port mapping
            if host == proxy_host && let Some(new_port) = config.proxy_port_for(port) {
                patches.push((port_offset, new_port));
            }
        }
        patches
    };

    for &(port_offset, new_port) in &patches {
        let abs_offset = body_start + port_offset;
        tracing::info!(
            "rewriting broker port at buf[{abs_offset}..]: → {}",
            new_port,
        );
        buf[abs_offset..abs_offset + 4].copy_from_slice(&new_port.to_be_bytes());
    }

    if !patches.is_empty() {
        tracing::debug!(
            "patched {} broker port(s) at {}:{:?}",
            patches.len(),
            proxy_host,
            patches.iter().map(|(_, p)| p).collect::<Vec<_>>(),
        );
    }

    Ok(())
}
