//! Core traits and types for Kafka request/response messages.
//!
//! Every Kafka API message implements either [`ApiRequest`] or [`ApiResponse`],
//! providing serialization/deserialization support and version metadata.

use bytes::{Bytes, BytesMut};
use std::fmt;

// ---------------------------------------------------------------------------
// ApiVersion
// ---------------------------------------------------------------------------

/// Kafka protocol version number (i16 on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ApiVersion(pub i16);

impl ApiVersion {
    pub const fn new(v: i16) -> Self {
        Self(v)
    }

    pub const fn min_value() -> Self {
        Self(i16::MIN)
    }

    pub const fn max_value() -> Self {
        Self(i16::MAX)
    }
}

impl fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i16> for ApiVersion {
    fn from(v: i16) -> Self {
        Self(v)
    }
}

impl From<ApiVersion> for i16 {
    fn from(v: ApiVersion) -> Self {
        v.0
    }
}

// ---------------------------------------------------------------------------
// ApiKey
// ---------------------------------------------------------------------------

/// Kafka API key (i16 on the wire).
///
/// Each request/response pair is identified by a unique numeric key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ApiKey(pub i16);

impl ApiKey {
    pub const fn new(k: i16) -> Self {
        Self(k)
    }
}

impl fmt::Display for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i16> for ApiKey {
    fn from(v: i16) -> Self {
        Self(v)
    }
}

impl From<ApiKey> for i16 {
    fn from(v: ApiKey) -> Self {
        v.0
    }
}

// ---------------------------------------------------------------------------
// SerializationError
// ---------------------------------------------------------------------------

use thiserror::Error as DeriveError;

/// Errors that can occur during message serialization or deserialization.
#[derive(Debug, Clone, DeriveError)]
pub enum SerializationError {
    /// The wire data is malformed or incomplete.
    #[error("decode error: {0}")]
    Decode(&'static str),
    /// A value could not be encoded (e.g. string too long).
    #[error("encode error: {0}")]
    Encode(&'static str),
    /// The requested version is not supported by this message.
    #[error("unsupported version {0}")]
    UnsupportedVersion(ApiVersion),
    /// The buffer ran out of space.
    #[error("buffer underrun")]
    BufferUnderrun,
}

// ---------------------------------------------------------------------------
// ApiRequest trait
// ---------------------------------------------------------------------------

/// A Kafka request message that can be serialized to the wire.
pub trait ApiRequest: Clone + fmt::Debug + Default {
    /// The response paired with this request.
    type Response: ApiResponse;

    /// The numeric API key for this message type.
    fn get_api_key() -> ApiKey;

    /// The minimum supported protocol version.
    fn get_min_supported_version() -> ApiVersion;

    /// The maximum supported protocol version.
    fn get_max_supported_version() -> ApiVersion;

    /// Serialize `self` into `buf` for the given protocol `version`.
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError>;

    /// Deserialize an instance of `Self` from `buf` for the given protocol `version`.
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError>;
}

// ---------------------------------------------------------------------------
// ApiResponse trait
// ---------------------------------------------------------------------------

/// A Kafka response message that can be deserialized from the wire.
pub trait ApiResponse: Clone + fmt::Debug + Default {
    /// The request paired with this response.
    type Request: ApiRequest;

    /// The numeric API key for this message type.
    fn get_api_key() -> ApiKey;

    /// The minimum supported protocol version.
    fn get_min_supported_version() -> ApiVersion;

    /// The maximum supported protocol version.
    fn get_max_supported_version() -> ApiVersion;

    /// Serialize `self` into `buf` for the given protocol `version`.
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError>;

    /// Deserialize an instance of `Self` from `buf` for the given protocol `version`.
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError>;
}

/// Whether a given API key uses flexible (compact) encoding at the given protocol version.
///
/// This mirrors the `flexibleVersions` ranges from each message's JSON definition.
pub fn is_flexible_api(api_key: i16, api_version: i16) -> bool {
    // API keys that are never flexible
    if matches!(api_key, 4 | 5 | 6 | 7 | 17 | 47) {
        return false;
    }
    // API keys that are always flexible (all newer KRaft-era APIs)
    if matches!(
        api_key,
        45 | 46 | 50 | 51 | 52 | 53 | 55 | 56 | 57 | 58 | 59 | 60 | 61 | 62 | 63 | 64
            | 65 | 66 | 67 | 68 | 69 | 70 | 71 | 72 | 73 | 74 | 75 | 76 | 77 | 78 | 79
            | 80 | 81 | 82 | 83 | 84 | 85 | 86 | 87 | 88 | 89 | 90 | 91 | 92
    ) {
        return true;
    }
    // Version-gated flexible encoding (from their JSON flexibleVersions)
    let min_flex: i16 = match api_key {
        0  => 9,  // Produce
        1  => 12, // Fetch
        2  => 6,  // ListOffsets
        3  => 9,  // Metadata
        8  => 8,  // OffsetCommit
        9  => 6,  // OffsetFetch
        10 => 3,  // FindCoordinator
        11 => 6,  // JoinGroup
        12 => 4,  // Heartbeat
        13 => 4,  // LeaveGroup
        14 => 4,  // SyncGroup
        15 => 5,  // DescribeGroups
        16 => 3,  // ListGroups
        18 => 3,  // ApiVersions
        19 => 5,  // CreateTopics
        20 => 4,  // DeleteTopics
        21 => 2,  // DeleteRecords
        22 => 2,  // InitProducerId
        23 => 4,  // OffsetForLeaderEpoch
        24 => 3,  // AddPartitionsToTxn
        25 => 3,  // AddOffsetsToTxn
        26 => 3,  // EndTxn
        27 => 1,  // WriteTxnMarkers
        28 => 3,  // TxnOffsetCommit
        29 => 2,  // DescribeAcls
        30 => 2,  // CreateAcls
        31 => 2,  // DeleteAcls
        32 => 4,  // DescribeConfigs
        33 => 2,  // AlterConfigs
        34 => 2,  // AlterReplicaLogDirs
        35 => 2,  // DescribeLogDirs
        36 => 2,  // SaslAuthenticate
        37 => 2,  // CreatePartitions
        38 => 2,  // CreateDelegationToken
        39 => 2,  // RenewDelegationToken
        40 => 2,  // ExpireDelegationToken
        41 => 2,  // DescribeDelegationToken
        42 | 43 | 44 => 2, // DeleteGroups, ElectLeaders, IncrementalAlterConfigs
        48 => 1,  // DescribeClientQuotas
        49 => 1,  // AlterClientQuotas
        54 => 1,  // EndQuorumEpoch
        _ => {
            // Unknown key — assume flexible (modern Kafka convention)
            return true;
        }
    };
    api_version >= min_flex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_version_order() {
        assert!(ApiVersion::new(0) < ApiVersion::new(1));
        assert!(ApiVersion::new(5) > ApiVersion::new(3));
    }

    #[test]
    fn test_api_key_from_i16() {
        let k: ApiKey = 3.into();
        assert_eq!(k.0, 3);
        let n: i16 = k.into();
        assert_eq!(n, 3);
    }

    #[test]
    fn test_serialization_error_display() {
        let e = SerializationError::Decode("bad data");
        assert!(e.to_string().contains("decode error"));
    }
}
