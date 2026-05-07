//! Kafka wire protocol frame parsing.
//!
//! Kafka's wire format uses a 4-byte big-endian size prefix followed by:
//!
//! **Request:**  `[size: i32] [RequestHeader] [RequestBody]`
//!
//! **Response:** `[size: i32] [ResponseHeader] [ResponseBody]`
//!
//! The headers are decoded manually using the protocol crate's primitive decoders
//! because the generated `RequestHeader`/`ResponseHeader` structs do not yet
//! implement `KafkaDeserialize`.

use protocol::protocol::serialization::{DecodeError, KafkaDeserialize};
use std::fmt;
use std::time::Instant;

// ---------------------------------------------------------------------------
// API key names (subset of common keys — extend as needed)
// ---------------------------------------------------------------------------

/// Return a human-readable name for a Kafka API key.
pub fn api_key_name(key: i16) -> &'static str {
    match key {
        0 => "Produce",
        1 => "Fetch",
        2 => "ListOffsets",
        3 => "Metadata",
        4 => "LeaderAndIsr",
        5 => "StopReplica",
        6 => "UpdateMetadata",
        7 => "ControlledShutdown",
        8 => "OffsetCommit",
        9 => "OffsetFetch",
        10 => "FindCoordinator",
        11 => "JoinGroup",
        12 => "Heartbeat",
        13 => "LeaveGroup",
        14 => "SyncGroup",
        15 => "DescribeGroups",
        16 => "ListGroups",
        17 => "SaslHandshake",
        18 => "ApiVersions",
        19 => "CreateTopics",
        20 => "DeleteTopics",
        21 => "DeleteRecords",
        22 => "InitProducerId",
        23 => "OffsetForLeaderEpoch",
        24 => "AddPartitionsToTxn",
        25 => "AddOffsetsToTxn",
        26 => "EndTxn",
        27 => "WriteTxnMarkers",
        28 => "TxnOffsetCommit",
        29 => "DescribeAcls",
        30 => "CreateAcls",
        31 => "DeleteAcls",
        32 => "DescribeConfigs",
        33 => "AlterConfigs",
        34 => "AlterReplicaLogDirs",
        35 => "DescribeLogDirs",
        36 => "SaslAuthenticate",
        37 => "CreatePartitions",
        38 => "CreateDelegationToken",
        39 => "RenewDelegationToken",
        40 => "ExpireDelegationToken",
        41 => "DescribeDelegationToken",
        42 => "ElectPreferredLeaders",
        _ => "Unknown",
    }
}

// ---------------------------------------------------------------------------
// ParsedFrame
// ---------------------------------------------------------------------------

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
/// RequestHeader v0:  api_key (i16) + api_version (i16) + correlation_id (i32)
///                    + client_id (nullable string) = variable
/// RequestHeader v1+ (flexible): same but client_id is compact nullable string
///                               + tag_buffer (unsigned varint).
/// The tag buffer is always varint(0) = 1 byte for request headers (no tagged
/// fields defined). We assume 1 byte rather than iterating, to avoid bleeding
/// into the body bytes when the body happens to start with a non-zero byte.
pub fn request_body_offset(data: &[u8], is_flexible: bool) -> usize {
    use protocol::protocol::serialization::KafkaDeserialize;
    use bytes::Buf;
    let mut cur: &[u8] = data;
    let _ = cur.get_i16(); // api_key
    let _ = cur.get_i16(); // api_version
    let _ = cur.get_i32(); // correlation_id
    if is_flexible {
        // client_id as compact nullable string
        let _: Option<String> = match KafkaDeserialize::decode_flexible(&mut cur, true) {
            Ok(v) => v,
            Err(_) => return data.len(),
        };
        // tag_buffer: read and skip the varint (always 0 for request headers)
        let (_tag_count, _) = protocol::protocol::serialization::decode_unsigned_varint(&mut cur).unwrap_or((0, 0));
    } else {
        let _: String = match KafkaDeserialize::decode(&mut cur) {
            Ok(v) => v,
            Err(_) => return data.len(),
        };
    }
    data.len() - cur.len()
}

// In-flight tracking
// ---------------------------------------------------------------------------

/// A tracked in-flight request.
#[derive(Debug, Clone)]
pub struct InFlightRequest {
    pub correlation_id: i32,
    pub api_key: i16,
    pub api_version: i16,
    pub client_id: String,
    pub sent_at: Instant,
}

// ---------------------------------------------------------------------------
// Frame parsing helpers
// ---------------------------------------------------------------------------

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
/// Wire format (v0/v1):
///   request_api_key:    int16
///   request_api_version: int16
///   correlation_id:     int32
///   client_id:          string (2-byte length + UTF-8 bytes; -1 length = "")
pub fn parse_request_header(data: &[u8]) -> Result<ParsedRequestHeader, DecodeError> {
    let mut buf: &[u8] = data;
    let api_key = i16::decode(&mut buf)?;
    let api_version = i16::decode(&mut buf)?;
    let correlation_id = i32::decode(&mut buf)?;
    // client_id is a nullable string; if length is -1, treat as empty
    let client_id = match String::decode(&mut buf) {
        Ok(s) => s,
        Err(DecodeError::UnexpectedNull) => String::new(),
        Err(e) => return Err(e),
    };
    Ok(ParsedRequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

/// Parse a `ResponseHeader` from raw bytes (after the 4-byte size prefix).
///
/// Wire format:
///   v0:             correlation_id (i32)
///   v1+ (flexible): correlation_id (i32) + tag_buffer (unsigned varint)
pub fn parse_response_header(data: &[u8]) -> Result<ParsedResponseHeader, DecodeError> {
    let mut buf: &[u8] = data;
    let correlation_id = i32::decode(&mut buf)?;
    Ok(ParsedResponseHeader { correlation_id })
}

/// Return the byte offset where the response body starts after the response header.
///
/// For non-flexible (v0): correlation_id is 4 bytes → offset 4.
/// For flexible (v1+):     correlation_id (4) + tag_buffer varint → 4 + varint bytes.
pub fn response_body_offset(data: &[u8], is_flexible: bool) -> usize {
    if data.len() < 4 {
        return 4;
    }
    if !is_flexible {
        return 4; // just correlation_id
    }
    // correlation_id (i32) = 4 bytes
    // Then tag_buffer as unsigned varint — for v1 response headers
    // this is always varint(0) = 1 byte, but parse properly to be robust.
    let cursor = &data[4..];
    // We don't care about the value, just how many bytes the varint consumes.
    // Scan bytes until we find one with MSB=0.
    let mut varint_bytes: usize = 0;
    for &b in cursor.iter() {
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
