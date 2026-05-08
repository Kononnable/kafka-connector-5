//! Generic dispatch for decoding Kafka request/response bodies by API key.
//!
//! This replaces proxy-specific match statements with a single function
//! call that uses the protocol crate's generated types internally.

use bytes::Bytes;
use crate::generated::*;
use crate::traits::{ApiRequest, ApiResponse, ApiVersion, SerializationError};

/// Deserialize a request body for the given API key and version.
///
/// Returns `Ok(debug_string)` on success, `Err(error_msg)` on failure.
pub fn decode_request_body(api_key: i16, version: i16, body: &[u8]) -> Result<String, String> {
    let ver = ApiVersion::new(version);
    let mut buf = Bytes::copy_from_slice(body);
    let result = match api_key {
        0  => ProduceRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        1  => FetchRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        2  => ListOffsetsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        3  => MetadataRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        8  => OffsetCommitRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        9  => OffsetFetchRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        10 => FindCoordinatorRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        11 => JoinGroupRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        12 => HeartbeatRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        13 => LeaveGroupRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        14 => SyncGroupRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        15 => DescribeGroupsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        16 => ListGroupsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        17 => SaslHandshakeRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        18 => ApiVersionsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        19 => CreateTopicsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        20 => DeleteTopicsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        21 => DeleteRecordsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        22 => InitProducerIdRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        23 => OffsetForLeaderEpochRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        24 => AddPartitionsToTxnRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        25 => AddOffsetsToTxnRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        26 => EndTxnRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        27 => WriteTxnMarkersRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        28 => TxnOffsetCommitRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        29 => DescribeAclsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        30 => CreateAclsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        31 => DeleteAclsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        32 => DescribeConfigsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        33 => AlterConfigsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        34 => AlterReplicaLogDirsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        35 => DescribeLogDirsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        36 => SaslAuthenticateRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        37 => CreatePartitionsRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        38 => CreateDelegationTokenRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        39 => RenewDelegationTokenRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        40 => ExpireDelegationTokenRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        41 => DescribeDelegationTokenRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        42 => ElectLeadersRequest::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        _  => return Err(format!("unknown api key {api_key}")),
    };
    result.map_err(|e| format!("{e}"))
}

/// Deserialize a response body for the given API key and version.
///
/// Returns `Ok(debug_string)` on success, `Err(error_msg)` on failure.
/// Only MetadataResponse is safe to decode — others may OOM from misaligned varints.
pub fn decode_response_body(api_key: i16, version: i16, body: &[u8]) -> Result<String, String> {
    let ver = ApiVersion::new(version);
    let mut buf = Bytes::copy_from_slice(body);
    let result = match api_key {
        3  => MetadataResponse::deserialize(ver, &mut buf).map(|v| format!("{v:?}")),
        _  => return Err(format!("no decode path for response api key {api_key}")),
    };
    result.map_err(|e| format!("{e}"))
}
