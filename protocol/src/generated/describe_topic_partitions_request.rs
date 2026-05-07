#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
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
    fn get_api_key() -> ApiKey { ApiKey::new(75) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (0), "version {} is not supported by {} (supported: 0-0)", version.0, stringify!(Self));
        let is_flexible = true;
        self.topics.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        self.response_partition_limit.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ResponsePartitionLimit"))?;
        if is_flexible {
        if let Some(ref __val) = self.cursor {
        crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
        __val.encode_flexible(buf, true).map_err(|_| SerializationError::Encode("failed to encode Cursor"))?;
        } else {
        crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        } else {
        if let Some(ref __val) = self.cursor {
        __val.encode(buf).map_err(|_| SerializationError::Encode("failed to encode Cursor"))?;
        }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = true;
        let topics = <Vec<TopicRequest> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let response_partition_limit = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ResponsePartitionLimit"))?;
        let cursor = 
            if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf).map_err(|_| SerializationError::Decode("tagged field error"))?;
            if __present == 0 {
            None
            } else {
            Some(<Cursor as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| SerializationError::Decode("failed to decode Cursor"))?)
            }
            } else {
            Some(<Cursor as KafkaDeserialize>::decode(buf).map_err(|_| SerializationError::Decode("failed to decode Cursor"))?)
            }
        ;
        Ok(Self { topics, response_partition_limit, cursor })
    }
}
impl KafkaSerialize for DescribeTopicPartitionsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topics.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        self.response_partition_limit.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ResponsePartitionLimit".into() })?;
        if let Some(ref __val) = self.cursor {
            __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Cursor".into() })?;
        }
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.topics.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        self.response_partition_limit.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ResponsePartitionLimit".into() })?;
        if is_flexible {
            if let Some(ref __val) = self.cursor {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val.encode_flexible(buf, true).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Cursor".into() })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.cursor {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Cursor".into() })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeTopicPartitionsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topics = <Vec<TopicRequest> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        let response_partition_limit = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ResponsePartitionLimit".into() })?;
        let cursor = Some(<Cursor as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Cursor".into() })?);
        Ok(Self { topics, response_partition_limit, cursor })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let topics = <Vec<TopicRequest> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        let response_partition_limit = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ResponsePartitionLimit".into() })?;
        let cursor = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(<Cursor as KafkaDeserialize>::decode_flexible(buf, true).map_err(|_| DecodeError::Protocol { message: "failed to decode Cursor".into() })?)
            }
        } else {
            Some(<Cursor as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Cursor".into() })?)
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topics, response_partition_limit, cursor })
    }
}

impl KafkaSerialize for Cursor {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_name.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicName".into() })?;
        self.partition_index.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode PartitionIndex".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.topic_name.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode TopicName".into() })?;
        self.partition_index.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode PartitionIndex".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Cursor {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_name = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicName".into() })?;
        let partition_index = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode PartitionIndex".into() })?;
        Ok(Self { topic_name, partition_index })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let topic_name = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode TopicName".into() })?;
        let partition_index = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode PartitionIndex".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_name, partition_index })
    }
}

impl KafkaSerialize for TopicRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.name.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        Ok(Self { name })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name })
    }
}

