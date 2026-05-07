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
        crate::traits::ApiVersion::new(13)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (13),
            "version {} is not supported by {} (supported: 0-13)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (9) <= version.0;
        self.topics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        if (4) <= version.0 {
            self.allow_auto_topic_creation
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode AllowAutoTopicCreation")
                })?;
        }
        if (8) <= version.0 && version.0 <= (10) {
            self.include_cluster_authorized_operations
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode(
                        "failed to encode IncludeClusterAuthorizedOperations",
                    )
                })?;
        }
        if (8) <= version.0 {
            self.include_topic_authorized_operations
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode IncludeTopicAuthorizedOperations")
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = (9) <= version.0;
        let topics = <Option<Vec<MetadataRequestTopic>> as KafkaDeserialize>::decode_flexible(
            buf,
            is_flexible,
        )
        .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let allow_auto_topic_creation = if (4) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode AllowAutoTopicCreation")
            })?
        } else {
            Default::default()
        };
        let include_cluster_authorized_operations = if (8) <= version.0 && version.0 <= (10) {
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode IncludeClusterAuthorizedOperations")
            })?
        } else {
            Default::default()
        };
        let include_topic_authorized_operations = if (8) <= version.0 {
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
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
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if is_flexible {
            if let Some(ref __val) = self.topics {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Topics".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.topics {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Topics".into(),
                })?;
            }
        }
        self.allow_auto_topic_creation
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AllowAutoTopicCreation".into(),
            })?;
        self.include_cluster_authorized_operations
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncludeClusterAuthorizedOperations".into(),
            })?;
        self.include_topic_authorized_operations
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IncludeTopicAuthorizedOperations".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MetadataRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics =
            <Option<Vec<MetadataRequestTopic>> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] classic decode field `AllowAutoTopicCreation` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let allow_auto_topic_creation =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode AllowAutoTopicCreation".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `IncludeClusterAuthorizedOperations` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let include_cluster_authorized_operations = <bool as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode IncludeClusterAuthorizedOperations".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `IncludeTopicAuthorizedOperations` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
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
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics = if is_flexible {
            <Option<Vec<MetadataRequestTopic>> as KafkaDeserialize>::decode_flexible(buf, true)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                })?
        } else {
            <Option<Vec<MetadataRequestTopic>> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `AllowAutoTopicCreation` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let allow_auto_topic_creation =
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode AllowAutoTopicCreation".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] decoding field `IncludeClusterAuthorizedOperations` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let include_cluster_authorized_operations =
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IncludeClusterAuthorizedOperations".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] decoding field `IncludeTopicAuthorizedOperations` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let include_topic_authorized_operations =
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IncludeTopicAuthorizedOperations".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
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
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.topic_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.name {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Name".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.name {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Name".into(),
                })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MetadataRequestTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `TopicId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Name".into(),
            }
        })?;
        Ok(Self { topic_id, name })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `TopicId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] decoding field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Name".into(),
                }
            })?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Name".into(),
                }
            })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_id, name })
    }
}
