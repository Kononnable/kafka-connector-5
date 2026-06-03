//! Core traits and types for Kafka request/response messages.
//!
//! Every Kafka API message implements either [`ApiRequest`] or [`ApiResponse`],
//! providing serialization/deserialization support and version metadata.

use std::fmt;

use bytes::{Bytes, BytesMut};

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

use thiserror::Error as DeriveError;

/// Errors that can occur during message serialization or deserialization.
#[derive(Debug, Clone, DeriveError)]
pub enum SerializationError {
    /// The requested version is not supported by the API.
    #[error("unsupported version {version} for {api}")]
    UnsupportedVersion {
        /// The API name.
        api: &'static str,
        /// The unsupported version.
        version: ApiVersion,
    },
    /// A field has a non-default value but is not supported in the requested version.
    #[error("field '{field}' is not available in version {version} of {api_name}")]
    FieldNotAvailable {
        /// The name of the field.
        field: &'static str,
        /// The requested API version.
        version: ApiVersion,
        /// The name of the API message.
        api_name: &'static str,
    },
}

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
