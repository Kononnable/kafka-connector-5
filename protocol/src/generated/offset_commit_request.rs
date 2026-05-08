#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// OffsetCommitRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetCommitRequest {
    /// The unique group identifier.
    pub group_id: String,
    /// The generation of the group if using the classic group protocol or the member epoch if using the consumer protocol.
    /// Available in version 1+.
    pub generation_id_or_member_epoch: i32,
    /// The member ID assigned by the group coordinator.
    /// Available in version 1+.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    /// Available in version 7+.
    pub group_instance_id: Option<String>,
    /// The time period in ms to retain the offset.
    /// Available in version 2-4.
    pub retention_time_ms: i64,
    /// The topics to commit offsets for.
    pub topics: Vec<OffsetCommitRequestTopic>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetCommitRequestPartition {
    /// The partition index.
    pub partition_index: i32,
    /// The message offset to be committed.
    pub committed_offset: i64,
    /// The leader epoch of this partition.
    /// Available in version 6+.
    pub committed_leader_epoch: i32,
    /// Any associated metadata the client wants to keep.
    pub committed_metadata: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetCommitRequestTopic {
    /// The topic name.
    /// Available in version 0-9.
    pub name: String,
    /// The topic ID.
    /// Available in version 10+.
    pub topic_id: [u8; 16],
    /// Each partition to commit offsets for.
    pub partitions: Vec<OffsetCommitRequestPartition>,
}

impl ApiRequest for OffsetCommitRequest {
    type Response = crate::generated::OffsetCommitResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(8)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(10)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(8)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (2) <= version.0 && version.0 <= (10),
            "version {} is not supported by {} (supported: 2-10)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.group_id.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.generation_id_or_member_epoch
                .encode(buf, version, is_flexible)?;
        } else if self.generation_id_or_member_epoch != 0 {
            return Err(SerializationError::Encode(
                "field 'GenerationIdOrMemberEpoch' is not available in this version",
            ));
        }
        if (1) <= version.0 {
            self.member_id.encode(buf, version, is_flexible)?;
        } else if !self.member_id.is_empty() {
            return Err(SerializationError::Encode(
                "field 'MemberId' is not available in this version",
            ));
        }
        if (7) <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        } else if self.group_instance_id.is_some() {
            return Err(SerializationError::Encode(
                "field 'GroupInstanceId' is not available in this version",
            ));
        }
        if (2) <= version.0 && version.0 <= (4) {
            self.retention_time_ms.encode(buf, version, is_flexible)?;
        } else if self.retention_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'RetentionTimeMs' is not available in this version",
            ));
        }
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let group_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let generation_id_or_member_epoch = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let member_id = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let group_instance_id = if (7) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let retention_time_ms = if (2) <= version.0 && version.0 <= (4) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            generation_id_or_member_epoch,
            member_id,
            group_instance_id,
            retention_time_ms,
            topics,
        })
    }
}
impl KafkaSerialize for OffsetCommitRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.generation_id_or_member_epoch
                .encode(buf, version, is_flexible)?;
        }
        if (1) <= version.0 {
            self.member_id.encode(buf, version, is_flexible)?;
        }
        if (7) <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        }
        if (2) <= version.0 && version.0 <= (4) {
            self.retention_time_ms.encode(buf, version, is_flexible)?;
        }
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetCommitRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let group_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let generation_id_or_member_epoch = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let member_id = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let group_instance_id = if (7) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let retention_time_ms = if (2) <= version.0 && version.0 <= (4) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            generation_id_or_member_epoch,
            member_id,
            group_instance_id,
            retention_time_ms,
            topics,
        })
    }
}

impl KafkaSerialize for OffsetCommitRequestPartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.committed_offset.encode(buf, version, is_flexible)?;
        if (6) <= version.0 {
            self.committed_leader_epoch
                .encode(buf, version, is_flexible)?;
        }
        self.committed_metadata.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetCommitRequestPartition {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition_index = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let committed_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let committed_leader_epoch = if (6) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let committed_metadata = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            committed_offset,
            committed_leader_epoch,
            committed_metadata,
        })
    }
}

impl KafkaSerialize for OffsetCommitRequestTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (0) <= version.0 && version.0 <= (9) {
            self.name.encode(buf, version, is_flexible)?;
        }
        if (10) <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetCommitRequestTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = if (0) <= version.0 && version.0 <= (9) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topic_id = if (10) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
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
