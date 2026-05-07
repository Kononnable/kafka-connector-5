#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// MetadataRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataRequest {
    /// The topics to fetch metadata for.
    pub topics: Option<Vec<MetadataRequestTopic>>,
    /// If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
    /// Available in version 4+.
    pub allow_auto_topic_creation: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataRequestTopic {
    /// The topic name.
    pub name: String,
}

impl ApiRequest for MetadataRequest {
    type Response = crate::generated::MetadataResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(3)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(7)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (7),
            "version {} is not supported by {} (supported: 0-7)",
            version.0,
            stringify!(Self)
        );
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        if (4) <= version.0 {
            self.allow_auto_topic_creation.encode(buf).map_err(|_| {
                SerializationError::Encode("failed to encode AllowAutoTopicCreation")
            })?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let topics = <Option<Vec<MetadataRequestTopic>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let allow_auto_topic_creation = if (4) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| {
                SerializationError::Decode("failed to decode AllowAutoTopicCreation")
            })?
        } else {
            Default::default()
        };
        Ok(Self {
            topics,
            allow_auto_topic_creation,
        })
    }
}
impl KafkaSerialize for MetadataRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.allow_auto_topic_creation
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AllowAutoTopicCreation".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for MetadataRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topics =
            <Option<Vec<MetadataRequestTopic>> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        let allow_auto_topic_creation =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode AllowAutoTopicCreation".into(),
            })?;
        Ok(Self {
            topics,
            allow_auto_topic_creation,
        })
    }
}

impl KafkaSerialize for MetadataRequestTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for MetadataRequestTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        Ok(Self { name })
    }
}
