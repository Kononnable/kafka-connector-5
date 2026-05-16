#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// DescribeProducersResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeProducersResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each topic in the response.
    pub topics: Vec<TopicResponse>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionResponse {
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code, or 0 if there was no error.
    pub error_code: i16,
    /// The partition error message, which may be null if no additional details are available.
    pub error_message: Option<String>,
    /// The active producers for the partition.
    pub active_producers: Vec<ProducerState>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProducerState {
    /// The producer id.
    pub producer_id: i64,
    /// The producer epoch.
    pub producer_epoch: i32,
    /// The last sequence number sent by the producer.
    pub last_sequence: i32,
    /// The last timestamp sent by the producer.
    pub last_timestamp: i64,
    /// The current epoch of the producer group.
    pub coordinator_epoch: i32,
    /// The current transaction start offset of the producer.
    pub current_txn_start_offset: i64,
}
impl Default for ProducerState {
    fn default() -> Self {
        Self {
            producer_id: 0,
            producer_epoch: 0,
            last_sequence: -1,
            last_timestamp: -1,
            coordinator_epoch: 0,
            current_txn_start_offset: -1,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicResponse {
    /// The topic name.
    pub name: String,
    /// Each partition in the response.
    pub partitions: Vec<PartitionResponse>,
}

impl ApiResponse for DescribeProducersResponse {
    type Request = crate::generated::DescribeProducersRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(61)
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
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}
impl KafkaCodec for DescribeProducersResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
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
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

impl KafkaCodec for PartitionResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        self.active_producers.encode(buf, version, is_flexible)?;
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
        let error_code = KafkaCodec::decode(buf, version, is_flexible)?;
        let error_message = KafkaCodec::decode(buf, version, is_flexible)?;
        let active_producers = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            error_code,
            error_message,
            active_producers,
        })
    }
}

impl KafkaCodec for ProducerState {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.producer_id.encode(buf, version, is_flexible)?;
        self.producer_epoch.encode(buf, version, is_flexible)?;
        self.last_sequence.encode(buf, version, is_flexible)?;
        self.last_timestamp.encode(buf, version, is_flexible)?;
        self.coordinator_epoch.encode(buf, version, is_flexible)?;
        self.current_txn_start_offset
            .encode(buf, version, is_flexible)?;
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
        let producer_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let producer_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let last_sequence = KafkaCodec::decode(buf, version, is_flexible)?;
        let last_timestamp = KafkaCodec::decode(buf, version, is_flexible)?;
        let coordinator_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let current_txn_start_offset = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            producer_id,
            producer_epoch,
            last_sequence,
            last_timestamp,
            coordinator_epoch,
            current_txn_start_offset,
        })
    }
}

impl KafkaCodec for TopicResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
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
        let name = KafkaCodec::decode(buf, version, is_flexible)?;
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}
