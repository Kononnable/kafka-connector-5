//! Core traits and types for Kafka request/response messages.
//!
//! Every Kafka API message implements either [`ApiRequest`] or [`ApiResponse`],
//! providing serialization/deserialization support and version metadata.

use bytes::{Bytes, BytesMut};
use std::fmt;

impl From<std::str::Utf8Error> for SerializationError {
    fn from(_: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8
    }
}

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
    /// A value could not be encoded (e.g. string too large).
    #[error("encode error: {0}")]
    Encode(&'static str),
    /// The requested version is not supported by this message.
    #[error("unsupported version {0}")]
    UnsupportedVersion(ApiVersion),
    /// The buffer ran out of bytes while reading.
    #[error("insufficient bytes")]
    InsufficientBytes,
    /// The encoded length is negative but the type does not support null.
    #[error("unexpected null value")]
    UnexpectedNull,
    /// The encoded length is invalid.
    #[error("invalid length: {message}")]
    InvalidLength { message: String },
    /// The string data is not valid UTF-8.
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    /// A value is too large for its wire representation.
    #[error("value too large: {message}")]
    ValueTooLarge { message: String },
    /// A generic protocol error.
    #[error("protocol error: {message}")]
    Protocol { message: String },
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

    /// The minimum protocol version at which this message uses flexible (compact)
    /// wire encoding.  Versions at or above this threshold use unsigned-varint
    /// length prefixes for strings, bytes, and arrays.
    fn get_min_flexible_version() -> ApiVersion;

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

    /// The minimum protocol version at which this message uses flexible (compact)
    /// wire encoding.
    fn get_min_flexible_version() -> ApiVersion;

    /// Serialize `self` into `buf` for the given protocol `version`.
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError>;

    /// Deserialize an instance of `Self` from `buf` for the given protocol `version`.
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError>;
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
