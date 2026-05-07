#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(10)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (10),
            "version {} is not supported by {} (supported: 1-10)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (6) <= version.0;
        if (3) <= version.0 {
            self.throttle_time_ms
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        if (0) <= version.0 && version.0 <= (7) {
            self.topics
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        }
        if (2) <= version.0 && version.0 <= (7) {
            self.error_code
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        }
        if (8) <= version.0 {
            self.groups
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode Groups"))?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = (6) <= version.0;
        let throttle_time_ms = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let topics = if (0) <= version.0 && version.0 <= (7) {
            <Vec<OffsetFetchResponseTopic> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?
        } else {
            Default::default()
        };
        let error_code = if (2) <= version.0 && version.0 <= (7) {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?
        } else {
            Default::default()
        };
        let groups = if (8) <= version.0 {
            <Vec<OffsetFetchResponseGroup> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| SerializationError::Decode("failed to decode Groups"))?
        } else {
            Default::default()
        };
        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
            groups,
        })
    }
}
impl KafkaSerialize for OffsetFetchResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.groups
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Groups".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.topics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.groups
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Groups".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics =
            <Vec<OffsetFetchResponseTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Groups` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let groups =
            <Vec<OffsetFetchResponseGroup> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Groups".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
            groups,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics = if (0) <= version.0 && version.0 <= (7) {
            <Vec<OffsetFetchResponseTopic> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = if (2) <= version.0 && version.0 <= (7) {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Groups` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let groups = if (8) <= version.0 {
            <Vec<OffsetFetchResponseGroup> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Groups".into(),
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.group_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.group_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        self.topics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponseGroup {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `GroupId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let group_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode GroupId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics =
            <Vec<OffsetFetchResponseTopics> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        Ok(Self {
            group_id,
            topics,
            error_code,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `GroupId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let group_id = if (8) <= version.0 {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode GroupId".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics = if (8) <= version.0 {
            <Vec<OffsetFetchResponseTopics> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = if (8) <= version.0 {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            topics,
            error_code,
        })
    }
}

impl KafkaSerialize for OffsetFetchResponsePartition {
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
        self.metadata
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Metadata".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.partition_index
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.committed_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CommittedOffset".into(),
            })?;
        self.committed_leader_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CommittedLeaderEpoch".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.metadata {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Metadata".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.metadata {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Metadata".into(),
                })?;
            }
        }
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponsePartition {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `PartitionIndex` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `CommittedOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let committed_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CommittedOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `CommittedLeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let committed_leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CommittedLeaderEpoch".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Metadata` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let metadata = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Metadata".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        Ok(Self {
            partition_index,
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `PartitionIndex` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_index = if (0) <= version.0 && version.0 <= (7) {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode PartitionIndex".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `CommittedOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let committed_offset = if (0) <= version.0 && version.0 <= (7) {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode CommittedOffset".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `CommittedLeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let committed_leader_epoch = if (5) <= version.0 && version.0 <= (7) {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode CommittedLeaderEpoch".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Metadata` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let metadata = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Metadata".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Metadata".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = if (0) <= version.0 && version.0 <= (7) {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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
        self.metadata
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Metadata".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.partition_index
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.committed_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CommittedOffset".into(),
            })?;
        self.committed_leader_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CommittedLeaderEpoch".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.metadata {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Metadata".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.metadata {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Metadata".into(),
                })?;
            }
        }
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponsePartitions {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `PartitionIndex` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `CommittedOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let committed_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CommittedOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `CommittedLeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let committed_leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CommittedLeaderEpoch".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Metadata` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let metadata = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Metadata".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        Ok(Self {
            partition_index,
            committed_offset,
            committed_leader_epoch,
            metadata,
            error_code,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `PartitionIndex` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_index = if (8) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode PartitionIndex".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `CommittedOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let committed_offset = if (8) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode CommittedOffset".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `CommittedLeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let committed_leader_epoch = if (8) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode CommittedLeaderEpoch".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Metadata` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let metadata = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Metadata".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Metadata".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = if (8) <= version.0 {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partitions
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponseTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Partitions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partitions = <Vec<OffsetFetchResponsePartition> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        Ok(Self { name, partitions })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name = if (0) <= version.0 && version.0 <= (7) {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Name".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Partitions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partitions = if (0) <= version.0 && version.0 <= (7) {
            <Vec<OffsetFetchResponsePartition> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}

impl KafkaSerialize for OffsetFetchResponseTopics {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.topic_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
        self.partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.topic_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
        self.partitions
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetFetchResponseTopics {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `TopicId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Partitions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partitions = <Vec<OffsetFetchResponsePartitions> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        Ok(Self {
            name,
            topic_id,
            partitions,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Name` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let name = if (8) <= version.0 && version.0 <= (9) {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Name".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `TopicId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_id = if (10) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Partitions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partitions = if (8) <= version.0 {
            <Vec<OffsetFetchResponsePartitions> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            topic_id,
            partitions,
        })
    }
}
