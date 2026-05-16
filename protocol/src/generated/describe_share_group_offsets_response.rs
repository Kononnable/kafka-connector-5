#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// DescribeShareGroupOffsetsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The results for each group.
    pub groups: Vec<DescribeShareGroupOffsetsResponseGroup>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsResponseGroup {
    /// The group identifier.
    pub group_id: String,
    /// The results for each topic.
    pub topics: Vec<DescribeShareGroupOffsetsResponseTopic>,
    /// The group-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// The group-level error message, or null if there was no error.
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsResponsePartition {
    /// The partition index.
    pub partition_index: i32,
    /// The share-partition start offset.
    pub start_offset: i64,
    /// The leader epoch of the partition.
    pub leader_epoch: i32,
    /// The partition-level error code, or 0 if there was no error.
    pub error_code: i16,
    /// The partition-level error message, or null if there was no error.
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsResponseTopic {
    /// The topic name.
    pub topic_name: String,
    /// The unique topic ID.
    pub topic_id: [u8; 16],
    /// Partitions. Type: []DescribeShareGroupOffsetsResponsePartition.
    pub partitions: Vec<DescribeShareGroupOffsetsResponsePartition>,
}

impl ApiResponse for DescribeShareGroupOffsetsResponse {
    type Request = crate::generated::DescribeShareGroupOffsetsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(90)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 0,
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.groups.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let groups = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}
impl KafkaCodec for DescribeShareGroupOffsetsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.groups.encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let groups = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

impl KafkaCodec for DescribeShareGroupOffsetsResponseGroup {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
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
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            topics,
            error_code,
            error_message,
        })
    }
}

impl KafkaCodec for DescribeShareGroupOffsetsResponsePartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.start_offset.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
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
        let partition_index = KafkaCodec::decode(buf, version, is_flexible)?;
        let start_offset = KafkaCodec::decode(buf, version, is_flexible)?;
        let leader_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            start_offset,
            leader_epoch,
            error_code,
            error_message,
        })
    }
}

impl KafkaCodec for DescribeShareGroupOffsetsResponseTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topic_name.encode(buf, version, is_flexible)?;
        self.topic_id.encode(buf, version, is_flexible)?;
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
        let topic_name = KafkaCodec::decode(buf, version, is_flexible)?;
        let topic_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            topic_id,
            partitions,
        })
    }
}
