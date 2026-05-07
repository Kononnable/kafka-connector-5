#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ApiVersionsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApiVersionsResponse {
    /// The top-level error code.
    pub error_code: i16,
    /// The APIs supported by the broker.
    pub api_keys: Vec<ApiVersionsResponseKey>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApiVersionsResponseKey {
    /// The API index.
    pub index: i16,
    /// The minimum supported version, inclusive.
    pub min_version: i16,
    /// The maximum supported version, inclusive.
    pub max_version: i16,
}

impl ApiResponse for ApiVersionsResponse {
    type Request = crate::generated::ApiVersionsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(18)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(2)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        self.error_code
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.api_keys
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ApiKeys"))?;
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let api_keys = <Vec<ApiVersionsResponseKey> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ApiKeys"))?;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
        })
    }
}
impl KafkaSerialize for ApiVersionsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.api_keys
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ApiKeys".into(),
            })?;
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ApiVersionsResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let api_keys =
            <Vec<ApiVersionsResponseKey> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ApiKeys".into(),
                }
            })?;
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
        })
    }
}

impl KafkaSerialize for ApiVersionsResponseKey {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Index".into(),
            })?;
        self.min_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MinVersion".into(),
            })?;
        self.max_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxVersion".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ApiVersionsResponseKey {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let index = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Index".into(),
        })?;
        let min_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MinVersion".into(),
            })?;
        let max_version =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxVersion".into(),
            })?;
        Ok(Self {
            index,
            min_version,
            max_version,
        })
    }
}
