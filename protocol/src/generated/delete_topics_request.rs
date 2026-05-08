#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DeleteTopicsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeleteTopicsRequest {
    /// The name or topic ID of the topic.
    /// Available in version 6+.
    pub topics: Vec<DeleteTopicState>,
    /// The names of the topics to delete.
    /// Available in version 0-5.
    pub topic_names: Vec<String>,
    /// The length of time in milliseconds to wait for the deletions to complete.
    pub timeout_ms: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeleteTopicState {
    /// The topic name.
    /// Available in version 6+.
    pub name: Option<String>,
    /// The unique topic ID.
    /// Available in version 6+.
    pub topic_id: [u8; 16],
}

impl ApiRequest for DeleteTopicsRequest {
    type Response = crate::generated::DeleteTopicsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(20)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(6)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (6),
            "version {} is not supported by {} (supported: 1-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (6) <= version.0 {
            self.topics
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        } else if !self.topics.is_empty() {
            return Err(SerializationError::Encode(
                "field 'Topics' is not available in this version",
            ));
        }
        if (0) <= version.0 && version.0 <= (5) {
            self.topic_names
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode TopicNames"))?;
        } else if !self.topic_names.is_empty() {
            return Err(SerializationError::Encode(
                "field 'TopicNames' is not available in this version",
            ));
        }
        self.timeout_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode TimeoutMs"))?;
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
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let topics = if (6) <= version.0 {
            <Vec<DeleteTopicState> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Topics"))?
        } else {
            Default::default()
        };
        let topic_names = if (0) <= version.0 && version.0 <= (5) {
            <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode TopicNames"))?
        } else {
            Default::default()
        };
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode TimeoutMs"))?;
        Ok(Self {
            topics,
            topic_names,
            timeout_ms,
        })
    }
}
impl KafkaSerialize for DeleteTopicsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (6) <= version.0 {
            self.topics.encode(buf, version, is_flexible).map_err(|_| {
                EncodeError::ValueTooLarge {
                    message: "failed to encode Topics".into(),
                }
            })?;
        }
        if (0) <= version.0 && version.0 <= (5) {
            self.topic_names
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TopicNames".into(),
                })?;
        }
        self.timeout_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TimeoutMs".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DeleteTopicsRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topics = if (6) <= version.0 {
            <Vec<DeleteTopicState> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                },
            )?
        } else {
            Default::default()
        };
        let topic_names = if (0) <= version.0 && version.0 <= (5) {
            <Vec<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicNames".into(),
                }
            })?
        } else {
            Default::default()
        };
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TimeoutMs".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            topic_names,
            timeout_ms,
        })
    }
}

impl KafkaSerialize for DeleteTopicState {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (6) <= version.0 {
            self.name.encode(buf, version, is_flexible).map_err(|_| {
                EncodeError::ValueTooLarge {
                    message: "failed to encode Name".into(),
                }
            })?;
        }
        if (6) <= version.0 {
            self.topic_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TopicId".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DeleteTopicState {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let name = if (6) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Name".into(),
                },
            )?
        } else {
            Default::default()
        };
        let topic_id = if (6) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, topic_id })
    }
}
