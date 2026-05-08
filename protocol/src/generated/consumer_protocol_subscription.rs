#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ConsumerProtocolSubscription
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsumerProtocolSubscription {
    /// The topics that the member wants to consume.
    pub topics: Vec<String>,
    /// User data that will be passed back to the consumer.
    pub user_data: Option<Vec<u8>>,
    /// The partitions that the member owns.
    /// Available in version 1+.
    pub owned_partitions: Vec<TopicPartition>,
    /// The generation id of the member.
    /// Available in version 2+.
    pub generation_id: i32,
    /// The rack id of the member.
    /// Available in version 3+.
    pub rack_id: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartition {
    /// The topic name.
    /// Available in version 1+.
    pub topic: String,
    /// The partition ids.
    /// Available in version 1+.
    pub partitions: Vec<i32>,
}

impl KafkaCodec for ConsumerProtocolSubscription {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topics.encode(buf, version, is_flexible)?;
        self.user_data.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.owned_partitions.encode(buf, version, is_flexible)?;
        }
        if 2 <= version.0 {
            self.generation_id.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.rack_id.encode(buf, version, is_flexible)?;
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
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let user_data = KafkaCodec::decode(buf, version, is_flexible)?;
        let owned_partitions = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let generation_id = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let rack_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            user_data,
            owned_partitions,
            generation_id,
            rack_id,
        })
    }
}

impl KafkaCodec for TopicPartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 1 <= version.0 {
            self.topic.encode(buf, version, is_flexible)?;
        }
        if 1 <= version.0 {
            self.partitions.encode(buf, version, is_flexible)?;
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
        let topic = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partitions = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic, partitions })
    }
}
