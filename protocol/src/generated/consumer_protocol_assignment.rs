#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// ConsumerProtocolAssignment
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsumerProtocolAssignment {
    /// The list of topics and partitions assigned to this consumer.
    /// IndexMap key `Topic` (string): The topic name.
    pub assigned_partitions: IndexMap<String, TopicPartition>,
    /// User data.
    pub user_data: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartition {
    /// The list of partitions assigned to this consumer.
    pub partitions: Vec<i32>,
}

impl KafkaCodec for ConsumerProtocolAssignment {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.assigned_partitions.encode(buf, version, is_flexible)?;
        self.user_data.encode(buf, version, is_flexible)?;
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
        let assigned_partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        let user_data = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            assigned_partitions,
            user_data,
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
        self.partitions.encode(buf, version, is_flexible)?;
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
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { partitions })
    }
}
