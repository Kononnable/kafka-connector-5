#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeTopicPartitionsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeTopicPartitionsRequest {
    /// The topics to fetch details for.
    pub topics: Vec<TopicRequest>,
    /// The maximum number of partitions included in the response.
    pub response_partition_limit: i32,
    /// The first topic and partition index to fetch details for.
    pub cursor: Option<Cursor>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Cursor {
    /// The name for the first topic to process.
    pub topic_name: String,
    /// The partition index to start with.
    pub partition_index: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicRequest {
    /// The topic name.
    pub name: String,
}

impl ApiRequest for DescribeTopicPartitionsRequest {
    type Response = crate::generated::DescribeTopicPartitionsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(75)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.topics.encode(buf, version, is_flexible)?;
        self.response_partition_limit
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            if let Some(ref __val) = self.cursor {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode(buf, version, true)?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.cursor {
                __val.encode(buf, version, false)?;
            }
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let topics = <Vec<TopicRequest> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let response_partition_limit =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let cursor = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<Cursor as KafkaDeserialize>::decode(buf, version, true)?)
            }
        } else {
            Some(<Cursor as KafkaDeserialize>::decode(buf, version, false)?)
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            response_partition_limit,
            cursor,
        })
    }
}
impl KafkaSerialize for DescribeTopicPartitionsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.topics.encode(buf, version, is_flexible)?;
        self.response_partition_limit
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            if let Some(ref __val) = self.cursor {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode(buf, version, true)?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.cursor {
                __val.encode(buf, version, false)?;
            }
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeTopicPartitionsRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let topics = <Vec<TopicRequest> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let response_partition_limit =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let cursor = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<Cursor as KafkaDeserialize>::decode(buf, version, true)?)
            }
        } else {
            Some(<Cursor as KafkaDeserialize>::decode(buf, version, false)?)
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            response_partition_limit,
            cursor,
        })
    }
}

impl KafkaSerialize for Cursor {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.topic_name.encode(buf, version, is_flexible)?;
        self.partition_index.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Cursor {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let topic_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let partition_index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partition_index,
        })
    }
}

impl KafkaSerialize for TopicRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name })
    }
}
