#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = true;
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.groups
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Groups"))?;
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
        let is_flexible = true;
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let groups =
            <Vec<DescribeShareGroupOffsetsResponseGroup> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| SerializationError::Decode("failed to decode Groups"))?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}
impl KafkaSerialize for DescribeShareGroupOffsetsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
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

impl KafkaDeserialize for DescribeShareGroupOffsetsResponse {
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
            "  [{}] classic decode field `Groups` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let groups = <Vec<DescribeShareGroupOffsetsResponseGroup> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Groups".into(),
            })?;
        Ok(Self {
            throttle_time_ms,
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
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `Groups` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let groups =
            <Vec<DescribeShareGroupOffsetsResponseGroup> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Groups".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

impl KafkaSerialize for DescribeShareGroupOffsetsResponseGroup {
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
        self.error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
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
            if let Some(ref __val) = self.error_message {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode ErrorMessage".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.error_message {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ErrorMessage".into(),
                })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeShareGroupOffsetsResponseGroup {
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
        let topics = <Vec<DescribeShareGroupOffsetsResponseTopic> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
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
            "  [{}] classic decode field `ErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            }
        })?;
        Ok(Self {
            group_id,
            topics,
            error_code,
            error_message,
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
        let group_id = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode GroupId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics =
            <Vec<DescribeShareGroupOffsetsResponseTopic> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_message = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                }
            })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            topics,
            error_code,
            error_message,
        })
    }
}

impl KafkaSerialize for DescribeShareGroupOffsetsResponsePartition {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.start_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode StartOffset".into(),
            })?;
        self.leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
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
        self.start_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode StartOffset".into(),
            })?;
        self.leader_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.error_message {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode ErrorMessage".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.error_message {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ErrorMessage".into(),
                })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeShareGroupOffsetsResponsePartition {
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
            "  [{}] classic decode field `StartOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let start_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode StartOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `LeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
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
            "  [{}] classic decode field `ErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            }
        })?;
        Ok(Self {
            partition_index,
            start_offset,
            leader_epoch,
            error_code,
            error_message,
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
        let partition_index = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `StartOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let start_offset = <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode StartOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `LeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let leader_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_message = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                }
            })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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

impl KafkaSerialize for DescribeShareGroupOffsetsResponseTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicName".into(),
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
        self.topic_name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicName".into(),
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

impl KafkaDeserialize for DescribeShareGroupOffsetsResponseTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `TopicName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicName".into(),
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
        let partitions =
            <Vec<DescribeShareGroupOffsetsResponsePartition> as KafkaDeserialize>::decode(buf)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                })?;
        Ok(Self {
            topic_name,
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
            "  [{}] decoding field `TopicName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_name = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode TopicName".into(),
        })?;
        tracing::trace!(
            "  [{}] decoding field `TopicId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode TopicId".into(),
        })?;
        tracing::trace!(
            "  [{}] decoding field `Partitions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partitions =
            <Vec<DescribeShareGroupOffsetsResponsePartition> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            topic_id,
            partitions,
        })
    }
}
