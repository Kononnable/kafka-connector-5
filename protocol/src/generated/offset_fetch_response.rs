#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// OffsetFetchResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 3+.
    pub throttle_time_ms: i32,
    /// The responses per topic.
    /// Available in version 0-7.
    pub topics: Vec<OffsetFetchResponseTopic>,
    /// The top-level error code, or 0 if there was no error.
    /// Available in version 2-7.
    pub error_code: i16,
    /// The responses per group id.
    /// Available in version 8+.
    pub groups: Vec<OffsetFetchResponseGroup>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchResponseGroup {
    /// The group ID.
    /// Available in version 8+.
    pub group_id: String,
    /// The responses per topic.
    /// Available in version 8+.
    pub topics: Vec<OffsetFetchResponseTopics>,
    /// The group-level error code, or 0 if there was no error.
    /// Available in version 8+.
    pub error_code: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchResponsePartition {
    /// The partition index.
    /// Available in version 0-7.
    pub partition_index: i32,
    /// The committed message offset.
    /// Available in version 0-7.
    pub committed_offset: i64,
    /// The leader epoch.
    /// Available in version 5-7.
    pub committed_leader_epoch: i32,
    /// The partition metadata.
    /// Available in version 0-7.
    pub metadata: Option<String>,
    /// The error code, or 0 if there was no error.
    /// Available in version 0-7.
    pub error_code: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchResponsePartitions {
    /// The partition index.
    /// Available in version 8+.
    pub partition_index: i32,
    /// The committed message offset.
    /// Available in version 8+.
    pub committed_offset: i64,
    /// The leader epoch.
    /// Available in version 8+.
    pub committed_leader_epoch: i32,
    /// The partition metadata.
    /// Available in version 8+.
    pub metadata: Option<String>,
    /// The partition-level error code, or 0 if there was no error.
    /// Available in version 8+.
    pub error_code: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchResponseTopic {
    /// The topic name.
    /// Available in version 0-7.
    pub name: String,
    /// The responses per partition.
    /// Available in version 0-7.
    pub partitions: Vec<OffsetFetchResponsePartition>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchResponseTopics {
    /// The topic name.
    /// Available in version 8-9.
    pub name: String,
    /// The topic ID.
    /// Available in version 10+.
    pub topic_id: [u8; 16],
    /// The responses per partition.
    /// Available in version 8+.
    pub partitions: Vec<OffsetFetchResponsePartitions>,
}

impl ApiResponse for OffsetFetchResponse {
    type Request = crate::generated::OffsetFetchRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(9)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(10)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(6)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (10),
            "version {} is not supported by {} (supported: 1-10)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (3) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        if (0) <= version.0 && version.0 <= (7) {
            self.topics.encode(buf, version, is_flexible)?;
        } else if !self.topics.is_empty() {
            return Err(SerializationError::Encode(
                "field 'Topics' is not available in this version",
            ));
        }
        if (2) <= version.0 && version.0 <= (7) {
            self.error_code.encode(buf, version, is_flexible)?;
        } else if self.error_code != 0 {
            return Err(SerializationError::Encode(
                "field 'ErrorCode' is not available in this version",
            ));
        }
        if (8) <= version.0 {
            self.groups.encode(buf, version, is_flexible)?;
        } else if !self.groups.is_empty() {
            return Err(SerializationError::Encode(
                "field 'Groups' is not available in this version",
            ));
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = if (0) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (2) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let groups = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
            groups,
        })
    }
}
impl KafkaSerialize for OffsetFetchResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (3) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        if (0) <= version.0 && version.0 <= (7) {
            self.topics.encode(buf, version, is_flexible)?;
        }
        if (2) <= version.0 && version.0 <= (7) {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 {
            self.groups.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let throttle_time_ms = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = if (0) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (2) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let groups = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
            groups,
        })
    }
}

impl KafkaSerialize for OffsetFetchResponseGroup {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (8) <= version.0 {
            self.group_id.encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 {
            self.topics.encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponseGroup {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let group_id = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            topics,
            error_code,
        })
    }
}

impl KafkaSerialize for OffsetFetchResponsePartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (0) <= version.0 && version.0 <= (7) {
            self.partition_index.encode(buf, version, is_flexible)?;
        }
        if (0) <= version.0 && version.0 <= (7) {
            self.committed_offset.encode(buf, version, is_flexible)?;
        }
        if (5) <= version.0 && version.0 <= (7) {
            self.committed_leader_epoch
                .encode(buf, version, is_flexible)?;
        }
        if (0) <= version.0 && version.0 <= (7) {
            self.metadata.encode(buf, version, is_flexible)?;
        }
        if (0) <= version.0 && version.0 <= (7) {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponsePartition {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition_index = if (0) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let committed_offset = if (0) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let committed_leader_epoch = if (5) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let metadata = if (0) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (0) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
        })
    }
}

impl KafkaSerialize for OffsetFetchResponsePartitions {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (8) <= version.0 {
            self.partition_index.encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 {
            self.committed_offset.encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 {
            self.committed_leader_epoch
                .encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 {
            self.metadata.encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponsePartitions {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition_index = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let committed_offset = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let committed_leader_epoch = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let metadata = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
        })
    }
}

impl KafkaSerialize for OffsetFetchResponseTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (0) <= version.0 && version.0 <= (7) {
            self.name.encode(buf, version, is_flexible)?;
        }
        if (0) <= version.0 && version.0 <= (7) {
            self.partitions.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponseTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = if (0) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partitions = if (0) <= version.0 && version.0 <= (7) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}

impl KafkaSerialize for OffsetFetchResponseTopics {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (8) <= version.0 && version.0 <= (9) {
            self.name.encode(buf, version, is_flexible)?;
        }
        if (10) <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 {
            self.partitions.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponseTopics {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = if (8) <= version.0 && version.0 <= (9) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_id = if (10) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partitions = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            topic_id,
            partitions,
        })
    }
}
