//! Kafka wire protocol frame parsing.
//!
//! Kafka's wire format uses a 4-byte big-endian size prefix followed by:
//!
//! **Request:**  `[size: i32] [RequestHeader] [RequestBody]`
//!
//! **Response:** `[size: i32] [ResponseHeader] [ResponseBody]`
//!
//! Headers are parsed using the generated `RequestHeader::decode` and
//! `ResponseHeader::peek_correlation_id` (two-phase approach for responses
//! where is_flexible is not yet known).

use std::fmt;
use std::time::Instant;

use bytes::Buf;
use protocol::protocol::serialization::SerializationError;

/// Information extracted from a single Kafka request or response frame.
#[derive(Debug, Clone)]
pub struct ParsedFrame {
    /// Direction (request or response).
    pub direction: FrameDirection,
    /// Total frame size (not including the 4-byte size prefix itself).
    pub size: usize,
    /// Offsets within the raw buffer: [size_field..size_field+size]
    pub raw_start: usize,
    pub raw_end: usize,
    /// Parsed request header fields (only for requests).
    pub request: Option<ParsedRequestHeader>,
    /// Parsed response header (only for responses).
    pub response: Option<ParsedResponseHeader>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDirection {
    Request,
    Response,
}

impl fmt::Display for FrameDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameDirection::Request => write!(f, "REQ"),
            FrameDirection::Response => write!(f, "RES"),
        }
    }
}

/// Parsed fields from a Kafka RequestHeader (v0/v1).
#[derive(Debug, Clone)]
pub struct ParsedRequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: String,
}

/// Parsed fields from a Kafka ResponseHeader (v0/v1).
#[derive(Debug, Clone)]
pub struct ParsedResponseHeader {
    pub correlation_id: i32,
}

// ---------------------------------------------------------------------------
/// Return the byte offset where the request body starts after the request header.
///
/// Wire format (all versions):
///   request_api_key:    int16
///   request_api_version: int16
///   correlation_id:     int32
///   client_id:          NULLABLE_STRING (2-byte i16 length, -1 = null)
///                       Per Kafka spec: ALWAYS classic, even in flexible mode!
///                       Older brokers need to parse ApiVersionsRequest.
///
/// Flexible (v2+) additionally has:
///   _tag_buffer:        unsigned varint count + tagged field entries
pub fn request_body_offset(data: &[u8], is_flexible: bool) -> usize {
    use protocol::protocol::serialization::KafkaCodec;
    let mut cur: &[u8] = data;
    let _ = cur.get_i16(); // api_key
    let _ = cur.get_i16(); // api_version
    let _ = cur.get_i32(); // correlation_id
    // Per Kafka protocol spec: ClientId is ALWAYS a classic nullable string
    // (2-byte i16 length prefix, -1 = null), even in flexible mode.
    // This is because older brokers must be able to parse the request header
    // from newer clients before they negotiate the version range.
    let _: Option<String> =
        match KafkaCodec::decode(&mut cur, protocol::traits::ApiVersion::new(0), false) {
            Ok(v) => v,
            Err(_) => return data.len(),
        };
    if is_flexible {
        // tag_buffer: read and skip the varint
        let (tag_count, _) =
            protocol::protocol::serialization::decode_unsigned_varint(&mut cur).unwrap_or((0, 0));
        for _ in 0..tag_count {
            let (_, _) = protocol::protocol::serialization::decode_unsigned_varint(&mut cur)
                .unwrap_or((0, 0));
            let (len, _) = protocol::protocol::serialization::decode_unsigned_varint(&mut cur)
                .unwrap_or((0, 0));
            cur.advance(len as usize);
        }
    }
    data.len() - cur.len()
}

/// A tracked in-flight request.
#[derive(Debug, Clone)]
pub struct InFlightRequest {
    pub correlation_id: i32,
    pub api_key: i16,
    pub api_version: i16,
    pub client_id: String,
    pub sent_at: Instant,
}

/// Try to parse the size prefix from a buffer.
/// Returns `Some(size)` if at least 4 bytes are available, `None` otherwise.
pub fn try_parse_size(data: &[u8]) -> Option<usize> {
    if data.len() < 4 {
        return None;
    }
    let size = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
    Some(size)
}

/// Parse a `RequestHeader` from raw bytes (after the 4-byte size prefix).
///
/// Wire format v1 (non-flexible):
///   request_api_key:    int16
///   request_api_version: int16
///   correlation_id:     int32
///   client_id:          NULLABLE_STRING (2-byte length + UTF-8 bytes; -1 = null → "")
///
/// Wire format v2 (flexible):
///   request_api_key:    int16
///   request_api_version: int16
///   correlation_id:     int32
///   client_id:          NULLABLE_STRING (2-byte length + UTF-8 — always classic!)
///   _tag_buffer:        unsigned varint count [+ tagged fields]
///
/// Per Kafka spec: ClientId is ALWAYS a classic nullable string (2-byte i16 length),
/// even in flexible mode. This is so older brokers can parse ApiVersionsRequest
/// from newer clients before version negotiation.
pub fn parse_request_header(data: &[u8]) -> Result<ParsedRequestHeader, SerializationError> {
    use protocol::generated::request_header::RequestHeader;
    let mut buf = bytes::Bytes::copy_from_slice(data);
    let hdr = RequestHeader::decode(&mut buf)?;
    Ok(ParsedRequestHeader {
        api_key: hdr.request_api_key,
        api_version: hdr.request_api_version,
        correlation_id: hdr.correlation_id,
        client_id: hdr.client_id.unwrap_or_default(),
    })
}

/// Parse a `ResponseHeader` from raw bytes (after the 4-byte size prefix).
/// Tag buffer (if flexible) is accounted for via `response_body_offset`.
pub fn parse_response_header(data: &[u8]) -> Result<ParsedResponseHeader, SerializationError> {
    use protocol::generated::response_header::ResponseHeader;
    let correlation_id = ResponseHeader::peek_correlation_id(data)?;
    Ok(ParsedResponseHeader { correlation_id })
}

/// Return the byte offset where the response body starts after the response header.
///
/// Non-flexible (v0):       correlation_id is 4 bytes → offset 4.
/// Flexible (v1+):          correlation_id (4) + tag_buffer varint → 4 + varint bytes.
/// ApiVersionsResponse special case (KIP-511): always uses v0 header.
pub fn response_body_offset(data: &[u8], is_flexible: bool) -> usize {
    if data.len() < 4 {
        return 4;
    }
    if !is_flexible {
        return 4; // just correlation_id
    }
    // correlation_id (i32) = 4 bytes
    // Then tag_buffer as unsigned varint (always varint(0) = 1 byte for responses)
    let mut varint_bytes: usize = 0;
    for &b in data[4..].iter() {
        varint_bytes += 1;
        if b & 0x80 == 0 {
            break;
        }
    }
    4 + varint_bytes
}

/// Parse a complete frame from `buf`, returning the parsed info and the
/// total frame size (including the 4-byte size prefix).
///
/// Returns `None` if there isn't enough data for a complete frame yet.
pub fn consume_frame(buf: &[u8], is_request: bool) -> Option<(ParsedFrame, usize)> {
    if buf.len() < 4 {
        return None;
    }
    let size = try_parse_size(buf)?;
    let total = 4 + size;
    if buf.len() < total {
        return None; // frame not complete
    }

    let raw = &buf[4..total]; // skip size prefix

    let (request, response) = if is_request {
        match parse_request_header(raw) {
            Ok(hdr) => (Some(hdr), None),
            Err(e) => {
                tracing::warn!("failed to parse request header: {e}");
                // Still forward the frame even if we can't parse
                (None, None)
            }
        }
    } else {
        match parse_response_header(raw) {
            Ok(hdr) => (None, Some(hdr)),
            Err(e) => {
                tracing::warn!("failed to parse response header: {e}");
                (None, None)
            }
        }
    };

    let frame = ParsedFrame {
        direction: if is_request {
            FrameDirection::Request
        } else {
            FrameDirection::Response
        },
        size,
        raw_start: 4,
        raw_end: total,
        request,
        response,
    };

    Some((frame, total))
}
