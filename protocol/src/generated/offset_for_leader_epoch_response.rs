#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// OffsetForLeaderEpochResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetForLeaderEpochResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 2+.
    pub throttle_time_ms: i32,
    /// Each topic we fetched offsets for.
    pub topics: Vec<OffsetForLeaderTopicResult>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EpochEndOffset {
    /// The error code 0, or if there was no error.
    pub error_code: i16,
    /// The partition index.
    pub partition: i32,
    /// The leader epoch of the partition.
    /// Available in version 1+.
    pub leader_epoch: i32,
    /// The end offset of the epoch.
    pub end_offset: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetForLeaderTopicResult {
    /// The topic name.
    pub topic: String,
    /// Each partition in the topic we fetched offsets for.
    pub partitions: Vec<EpochEndOffset>,
}

impl ApiResponse for OffsetForLeaderEpochResponse {
    type Request = crate::generated::OffsetForLeaderEpochRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(23)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(2)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
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
            (2) <= version.0 && version.0 <= (4),
            "version {} is not supported by {} (supported: 2-4)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (2) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        self.topics.encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}
impl KafkaSerialize for OffsetForLeaderEpochResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (2) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetForLeaderEpochResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

impl KafkaSerialize for EpochEndOffset {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        self.partition.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.leader_epoch.encode(buf, version, is_flexible)?;
        }
        self.end_offset.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for EpochEndOffset {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let partition = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let leader_epoch = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let end_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            partition,
            leader_epoch,
            end_offset,
        })
    }
}

impl KafkaSerialize for OffsetForLeaderTopicResult {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.topic.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetForLeaderTopicResult {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let topic = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic, partitions })
    }
}
