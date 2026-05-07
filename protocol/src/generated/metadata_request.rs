#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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
    /// Whether to include cluster authorized operations.
    /// Available in version 8-10.
    pub include_cluster_authorized_operations: bool,
    /// Whether to include topic authorized operations.
    /// Available in version 8+.
    pub include_topic_authorized_operations: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataRequestTopic {
    /// The topic id.
    /// Available in version 10+.
    pub topic_id: [u8; 16],
    /// The topic name.
    pub name: Option<String>,
}

impl ApiRequest for MetadataRequest {
    type Response = crate::generated::MetadataResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(3)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(11)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (11),
            "version {} is not supported by {} (supported: 0-11)",
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
        if (8) <= version.0 && version.0 <= (10) {
            self.include_cluster_authorized_operations
                .encode(buf)
                .map_err(|_| {
                    SerializationError::Encode(
                        "failed to encode IncludeClusterAuthorizedOperations",
                    )
                })?;
        }
        if (8) <= version.0 {
            self.include_topic_authorized_operations
                .encode(buf)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode IncludeTopicAuthorizedOperations")
                })?;
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let topics = <Option<Vec<MetadataRequestTopic>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let allow_auto_topic_creation = if (4) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| {
                SerializationError::Decode("failed to decode AllowAutoTopicCreation")
            })?
        } else {
            Default::default()
        };
        let include_cluster_authorized_operations = if (8) <= version.0 && version.0 <= (10) {
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| {
                SerializationError::Decode("failed to decode IncludeClusterAuthorizedOperations")
            })?
        } else {
            Default::default()
        };
        let include_topic_authorized_operations = if (8) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| {
                SerializationError::Decode("failed to decode IncludeTopicAuthorizedOperations")
            })?
        } else {
            Default::default()
        };
        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
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
        self.include_cluster_authorized_operations
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncludeClusterAuthorizedOperations".into(),
            })?;
        self.include_topic_authorized_operations
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncludeTopicAuthorizedOperations".into(),
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
        let include_cluster_authorized_operations = <bool as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode IncludeClusterAuthorizedOperations".into(),
            })?;
        let include_topic_authorized_operations =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IncludeTopicAuthorizedOperations".into(),
            })?;
        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
        })
    }
}

impl KafkaSerialize for MetadataRequestTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
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
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicId".into(),
            })?;
        let name = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Name".into(),
            }
        })?;
        Ok(Self { topic_id, name })
    }
}
