#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DeleteTopicsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeleteTopicsRequest {
    /// The names of the topics to delete
    pub topic_names: Vec<String>,
    /// The length of time in milliseconds to wait for the deletions to complete.
    pub timeout_ms: i32,
}

impl ApiRequest for DeleteTopicsRequest {
    type Response = crate::generated::DeleteTopicsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(20)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(4)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (4),
            "version {} is not supported by {} (supported: 0-4)",
            version.0,
            stringify!(Self)
        );
        self.topic_names
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TopicNames"))?;
        self.timeout_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TimeoutMs"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let topic_names = <Vec<String> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TopicNames"))?;
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TimeoutMs"))?;
        Ok(Self {
            topic_names,
            timeout_ms,
        })
    }
}
impl KafkaSerialize for DeleteTopicsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_names
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicNames".into(),
            })?;
        self.timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TimeoutMs".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DeleteTopicsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_names =
            <Vec<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicNames".into(),
            })?;
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TimeoutMs".into(),
            })?;
        Ok(Self {
            topic_names,
            timeout_ms,
        })
    }
}
