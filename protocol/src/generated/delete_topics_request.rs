#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(6)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            1 <= version.0 && version.0 <= 6,
            "version {} is not supported by {} (supported: 1-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 6 <= version.0 {
            self.topics.encode(buf, version, is_flexible)?;
        } else if !self.topics.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "Topics",
                version,
                api_name: "DeleteTopicsRequest",
            });
        }
        if 0 <= version.0 && version.0 <= 5 {
            self.topic_names.encode(buf, version, is_flexible)?;
        } else if !self.topic_names.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "TopicNames",
                version,
                api_name: "DeleteTopicsRequest",
            });
        }
        self.timeout_ms.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let topics = if 6 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_names = if 0 <= version.0 && version.0 <= 5 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            topic_names,
            timeout_ms,
        })
    }
}
impl KafkaCodec for DeleteTopicsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 6 <= version.0 {
            self.topics.encode(buf, version, is_flexible)?;
        }
        if 0 <= version.0 && version.0 <= 5 {
            self.topic_names.encode(buf, version, is_flexible)?;
        }
        self.timeout_ms.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topics = if 6 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_names = if 0 <= version.0 && version.0 <= 5 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            topic_names,
            timeout_ms,
        })
    }
}

impl KafkaCodec for DeleteTopicState {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 6 <= version.0 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if 6 <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = if 6 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_id = if 6 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, topic_id })
    }
}
