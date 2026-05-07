#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AlterConfigsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterConfigsResponse {
    /// Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The responses for each resource.
    pub resources: Vec<AlterConfigsResourceResponse>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterConfigsResourceResponse {
    /// The resource error code.
    pub error_code: i16,
    /// The resource error message, or null if there was no error.
    pub error_message: Option<String>,
    /// The resource type.
    pub resource_type: i8,
    /// The resource name.
    pub resource_name: String,
}

impl ApiResponse for AlterConfigsResponse {
    type Request = crate::generated::AlterConfigsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(33)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(1)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.resources
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Resources"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let resources = <Vec<AlterConfigsResourceResponse> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Resources"))?;
        Ok(Self {
            throttle_time_ms,
            resources,
        })
    }
}
impl KafkaSerialize for AlterConfigsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.resources
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Resources".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AlterConfigsResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let resources = <Vec<AlterConfigsResourceResponse> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Resources".into(),
            })?;
        Ok(Self {
            throttle_time_ms,
            resources,
        })
    }
}

impl KafkaSerialize for AlterConfigsResourceResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
        self.resource_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceType".into(),
            })?;
        self.resource_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ResourceName".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for AlterConfigsResourceResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            }
        })?;
        let resource_type =
            <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceType".into(),
            })?;
        let resource_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ResourceName".into(),
            })?;
        Ok(Self {
            error_code,
            error_message,
            resource_type,
            resource_name,
        })
    }
}
