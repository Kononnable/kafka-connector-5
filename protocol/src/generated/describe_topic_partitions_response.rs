#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeTopicPartitionsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeTopicPartitionsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each topic in the response.
    pub topics: Vec<DescribeTopicPartitionsResponseTopic>,
    /// The next topic and partition index to fetch details for.
    pub next_cursor: Option<Cursor>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Cursor {
    /// The name for the first topic to process.
    pub topic_name: String,
    /// The partition index to start with.
    pub partition_index: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeTopicPartitionsResponsePartition {
    /// The partition error, or 0 if there was no error.
    pub error_code: i16,
    /// The partition index.
    pub partition_index: i32,
    /// The ID of the leader broker.
    pub leader_id: i32,
    /// The leader epoch of this partition.
    pub leader_epoch: i32,
    /// The set of all nodes that host this partition.
    pub replica_nodes: Vec<i32>,
    /// The set of nodes that are in sync with the leader for this partition.
    pub isr_nodes: Vec<i32>,
    /// The new eligible leader replicas otherwise.
    pub eligible_leader_replicas: Option<Vec<i32>>,
    /// The last known ELR.
    pub last_known_elr: Option<Vec<i32>>,
    /// The set of offline replicas of this partition.
    pub offline_replicas: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeTopicPartitionsResponseTopic {
    /// The topic error, or 0 if there was no error.
    pub error_code: i16,
    /// The topic name.
    pub name: Option<String>,
    /// The topic id.
    pub topic_id: [u8; 16],
    /// True if the topic is internal.
    pub is_internal: bool,
    /// Each partition in the topic.
    pub partitions: Vec<DescribeTopicPartitionsResponsePartition>,
    /// 32-bit bitfield to represent authorized operations for this topic.
    pub topic_authorized_operations: i32,
}

impl ApiResponse for DescribeTopicPartitionsResponse {
    type Request = crate::generated::DescribeTopicPartitionsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(75)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
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
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        if is_flexible {
            if let Some(ref __val) = self.next_cursor {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode(buf, version, true)
                    .map_err(|_| SerializationError::Encode("failed to encode NextCursor"))?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.next_cursor {
                __val
                    .encode(buf, version, false)
                    .map_err(|_| SerializationError::Encode("failed to encode NextCursor"))?;
            }
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
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let topics = <Vec<DescribeTopicPartitionsResponseTopic> as KafkaDeserialize>::decode(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let next_cursor = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)
                .map_err(|_| SerializationError::Decode("tagged field error"))?;
            if __present == 0 {
                None
            } else {
                Some(
                    <Cursor as KafkaDeserialize>::decode(buf, version, true)
                        .map_err(|_| SerializationError::Decode("failed to decode NextCursor"))?,
                )
            }
        } else {
            Some(
                <Cursor as KafkaDeserialize>::decode(buf, version, false)
                    .map_err(|_| SerializationError::Decode("failed to decode NextCursor"))?,
            )
        };
        Ok(Self {
            throttle_time_ms,
            topics,
            next_cursor,
        })
    }
}
impl KafkaSerialize for DescribeTopicPartitionsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.next_cursor {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode(buf, version, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode NextCursor".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.next_cursor {
                __val
                    .encode(buf, version, false)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode NextCursor".into(),
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

impl KafkaDeserialize for DescribeTopicPartitionsResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let topics = <Vec<DescribeTopicPartitionsResponseTopic> as KafkaDeserialize>::decode(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Topics".into(),
        })?;
        let next_cursor = if is_flexible {
            let (__present, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            if __present == 0 {
                None
            } else {
                Some(
                    <Cursor as KafkaDeserialize>::decode(buf, version, true).map_err(|_| {
                        DecodeError::Protocol {
                            message: "failed to decode NextCursor".into(),
                        }
                    })?,
                )
            }
        } else {
            Some(
                <Cursor as KafkaDeserialize>::decode(buf, version, false).map_err(|_| {
                    DecodeError::Protocol {
                        message: "failed to decode NextCursor".into(),
                    }
                })?,
            )
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            topics,
            next_cursor,
        })
    }
}

impl KafkaSerialize for Cursor {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.topic_name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicName".into(),
            })?;
        self.partition_index
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Cursor {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicName".into(),
                }
            })?;
        let partition_index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partition_index,
        })
    }
}

impl KafkaSerialize for DescribeTopicPartitionsResponsePartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.partition_index
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.leader_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderId".into(),
            })?;
        self.leader_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        self.replica_nodes
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicaNodes".into(),
            })?;
        self.isr_nodes
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsrNodes".into(),
            })?;
        self.eligible_leader_replicas
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode EligibleLeaderReplicas".into(),
            })?;
        self.last_known_elr
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LastKnownElr".into(),
            })?;
        self.offline_replicas
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode OfflineReplicas".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeTopicPartitionsResponsePartition {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let partition_index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let leader_id =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderId".into(),
                }
            })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderEpoch".into(),
                }
            })?;
        let replica_nodes = <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ReplicaNodes".into(),
            })?;
        let isr_nodes =
            <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IsrNodes".into(),
                }
            })?;
        let eligible_leader_replicas =
            <Option<Vec<i32>> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode EligibleLeaderReplicas".into(),
                },
            )?;
        let last_known_elr =
            <Option<Vec<i32>> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode LastKnownElr".into(),
                },
            )?;
        let offline_replicas = <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode OfflineReplicas".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            eligible_leader_replicas,
            last_known_elr,
            offline_replicas,
        })
    }
}

impl KafkaSerialize for DescribeTopicPartitionsResponseTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.topic_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
        self.is_internal
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsInternal".into(),
            })?;
        self.partitions
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        self.topic_authorized_operations
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicAuthorizedOperations".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeTopicPartitionsResponseTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let name = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?;
        let is_internal =
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IsInternal".into(),
                }
            })?;
        let partitions =
            <Vec<DescribeTopicPartitionsResponsePartition> as KafkaDeserialize>::decode(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        let topic_authorized_operations =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicAuthorizedOperations".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
        })
    }
}
