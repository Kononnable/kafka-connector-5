#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// OffsetCommitRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetCommitRequest {
    /// The unique group identifier.
    pub group_id: String,
    /// The generation of the group.
    /// Available in version 1+.
    pub generation_id: i32,
    /// The member ID assigned by the group coordinator.
    /// Available in version 1+.
    pub member_id: String,
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
    /// The timestamp of the commit.
    /// Available in version 1.
    pub commit_timestamp: i64,
    /// Any associated metadata the client wants to keep.
    pub committed_metadata: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetCommitRequestTopic {
    /// The topic name.
    pub name: String,
    /// Each partition to commit offsets for.
    pub partitions: Vec<OffsetCommitRequestPartition>,
}

impl ApiRequest for OffsetCommitRequest {
    type Response = crate::generated::OffsetCommitResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(8)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(6)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (6),
            "version {} is not supported by {} (supported: 0-6)",
            version.0,
            stringify!(Self)
        );
        self.group_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode GroupId"))?;
        if (1) <= version.0 {
            self.generation_id
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode GenerationId"))?;
        }
        if (1) <= version.0 {
            self.member_id
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode MemberId"))?;
        }
        if (2) <= version.0 && version.0 <= (4) {
            self.retention_time_ms
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode RetentionTimeMs"))?;
        }
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let group_id = <String as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode GroupId"))?;
        let generation_id = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode GenerationId"))?
        } else {
            Default::default()
        };
        let member_id = if (1) <= version.0 {
            <String as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode MemberId"))?
        } else {
            Default::default()
        };
        let retention_time_ms = if (2) <= version.0 && version.0 <= (4) {
            <i64 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode RetentionTimeMs"))?
        } else {
            Default::default()
        };
        let topics = <Vec<OffsetCommitRequestTopic> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            retention_time_ms,
            topics,
        })
    }
}
impl KafkaSerialize for OffsetCommitRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.group_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        self.generation_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GenerationId".into(),
            })?;
        self.member_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        self.retention_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RetentionTimeMs".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for OffsetCommitRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let group_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode GroupId".into(),
            })?;
        let generation_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode GenerationId".into(),
            })?;
        let member_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MemberId".into(),
            })?;
        let retention_time_ms =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode RetentionTimeMs".into(),
            })?;
        let topics =
            <Vec<OffsetCommitRequestTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            retention_time_ms,
            topics,
        })
    }
}

impl KafkaSerialize for OffsetCommitRequestPartition {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.committed_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CommittedOffset".into(),
            })?;
        self.committed_leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CommittedLeaderEpoch".into(),
            })?;
        self.commit_timestamp
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CommitTimestamp".into(),
            })?;
        self.committed_metadata
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CommittedMetadata".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for OffsetCommitRequestPartition {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let committed_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CommittedOffset".into(),
            })?;
        let committed_leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CommittedLeaderEpoch".into(),
            })?;
        let commit_timestamp =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CommitTimestamp".into(),
            })?;
        let committed_metadata =
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode CommittedMetadata".into(),
                }
            })?;
        Ok(Self {
            partition_index,
            committed_offset,
            committed_leader_epoch,
            commit_timestamp,
            committed_metadata,
        })
    }
}

impl KafkaSerialize for OffsetCommitRequestTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for OffsetCommitRequestTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partitions = <Vec<OffsetCommitRequestPartition> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        Ok(Self { name, partitions })
    }
}
