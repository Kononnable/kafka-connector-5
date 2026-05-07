#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DeleteTopicsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeleteTopicsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// The results for each topic.
    pub responses: Vec<DeletableTopicResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeletableTopicResult {
    /// The topic name
    pub name: String,
    /// The deletion error, or 0 if the deletion succeeded.
    pub error_code: i16,
}

impl ApiResponse for DeleteTopicsResponse {
    type Request = crate::generated::DeleteTopicsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(20)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(3)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 0-3)",
            version.0,
            stringify!(Self)
        );
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode throttleTimeMs"))?;
        }
        self.responses
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Responses"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode throttleTimeMs"))?
        } else {
            Default::default()
        };
        let responses = <Vec<DeletableTopicResult> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Responses"))?;
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }
}
impl KafkaSerialize for DeleteTopicsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode throttleTimeMs".into(),
            })?;
        self.responses
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Responses".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DeleteTopicsResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode throttleTimeMs".into(),
            })?;
        let responses =
            <Vec<DeletableTopicResult> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Responses".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }
}

impl KafkaSerialize for DeletableTopicResult {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DeletableTopicResult {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        Ok(Self { name, error_code })
    }
}
