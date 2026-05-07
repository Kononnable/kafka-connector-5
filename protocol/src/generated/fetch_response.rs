#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// FetchResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// The top level response error code.
    /// Available in version 7+.
    pub error_code: i16,
    /// The fetch session ID, or 0 if this is not part of a fetch session.
    /// Available in version 7+.
    pub session_id: i32,
    /// The response topics.
    pub responses: Vec<FetchableTopicResponse>,
    /// Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
    /// Available in version 16+.
    pub node_endpoints: Vec<NodeEndpoint>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AbortedTransaction {
    /// The producer id associated with the aborted transaction.
    /// Available in version 4+.
    pub producer_id: i64,
    /// The first offset in the aborted transaction.
    /// Available in version 4+.
    pub first_offset: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EpochEndOffset {
    /// The largest epoch.
    /// Available in version 12+.
    pub epoch: i32,
    /// The end offset of the epoch.
    /// Available in version 12+.
    pub end_offset: i64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchableTopicResponse {
    /// The topic name.
    /// Available in version 0-12.
    pub topic: String,
    /// The unique topic ID.
    /// Available in version 13+.
    pub topic_id: [u8; 16],
    /// The topic partitions.
    pub partitions: Vec<PartitionData>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    /// Available in version 12+.
    pub leader_id: i32,
    /// The latest known leader epoch.
    /// Available in version 12+.
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    /// Available in version 16+.
    pub node_id: i32,
    /// The node's hostname.
    /// Available in version 16+.
    pub host: String,
    /// The node's port.
    /// Available in version 16+.
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    /// Available in version 16+.
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The error code, or 0 if there was no fetch error.
    pub error_code: i16,
    /// The current high water mark.
    pub high_watermark: i64,
    /// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
    /// Available in version 4+.
    pub last_stable_offset: i64,
    /// The current log start offset.
    /// Available in version 5+.
    pub log_start_offset: i64,
    /// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
    /// Available in version 12+.
    pub diverging_epoch: EpochEndOffset,
    /// The current leader of the partition.
    /// Available in version 12+.
    pub current_leader: LeaderIdAndEpoch,
    /// In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
    /// Available in version 12+.
    pub snapshot_id: SnapshotId,
    /// The aborted transactions.
    /// Available in version 4+.
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,
    /// The preferred read replica for the consumer to use on its next fetch request.
    /// Available in version 11+.
    pub preferred_read_replica: i32,
    /// The record data.
    pub records: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SnapshotId {
    /// The end offset of the epoch.
    pub end_offset: i64,
    /// The largest epoch.
    pub epoch: i32,
}

impl ApiResponse for FetchResponse {
    type Request = crate::generated::FetchRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(1)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(18)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (4) <= version.0 && version.0 <= (18),
            "version {} is not supported by {} (supported: 4-18)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (12) <= version.0;
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        if (7) <= version.0 {
            self.error_code
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        }
        if (7) <= version.0 {
            self.session_id
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode SessionId"))?;
        }
        self.responses
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Responses"))?;
        if (16) <= version.0 {
            self.node_endpoints
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode NodeEndpoints"))?;
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
        let is_flexible = (12) <= version.0;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let error_code = if (7) <= version.0 {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?
        } else {
            Default::default()
        };
        let session_id = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode SessionId"))?
        } else {
            Default::default()
        };
        let responses = <Vec<FetchableTopicResponse> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| SerializationError::Decode("failed to decode Responses"))?;
        let node_endpoints = if (16) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<NodeEndpoint> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                    .map_err(|_| SerializationError::Decode("failed to decode NodeEndpoints"))?
            }
        } else {
            Default::default()
        };
        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
        })
    }
}
impl KafkaSerialize for FetchResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.session_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SessionId".into(),
            })?;
        self.responses
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Responses".into(),
            })?;
        self.node_endpoints
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode NodeEndpoints".into(),
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
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.session_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SessionId".into(),
            })?;
        self.responses
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Responses".into(),
            })?;
        self.node_endpoints
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode NodeEndpoints".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FetchResponse {
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
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `SessionId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let session_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode SessionId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Responses` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let responses =
            <Vec<FetchableTopicResponse> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Responses".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] classic decode field `NodeEndpoints` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let node_endpoints =
            <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode NodeEndpoints".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
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
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = if (7) <= version.0 {
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `SessionId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let session_id = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode SessionId".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Responses` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let responses = <Vec<FetchableTopicResponse> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Responses".into(),
        })?;
        tracing::trace!(
            "  [{}] decoding field `NodeEndpoints` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let node_endpoints = if (16) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<NodeEndpoint> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                    .map_err(|_| DecodeError::Protocol {
                        message: "failed to decode NodeEndpoints".into(),
                    })?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
            node_endpoints,
        })
    }
}

impl KafkaSerialize for AbortedTransaction {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.producer_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerId".into(),
            })?;
        self.first_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FirstOffset".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.producer_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerId".into(),
            })?;
        self.first_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FirstOffset".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AbortedTransaction {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `ProducerId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_id =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `FirstOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let first_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode FirstOffset".into(),
            })?;
        Ok(Self {
            producer_id,
            first_offset,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ProducerId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let producer_id = if (4) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ProducerId".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `FirstOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let first_offset = if (4) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode FirstOffset".into(),
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
            producer_id,
            first_offset,
        })
    }
}

impl KafkaSerialize for EpochEndOffset {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Epoch".into(),
            })?;
        self.end_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode EndOffset".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Epoch".into(),
            })?;
        self.end_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode EndOffset".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for EpochEndOffset {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Epoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let epoch = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Epoch".into(),
        })?;
        tracing::trace!(
            "  [{}] classic decode field `EndOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let end_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode EndOffset".into(),
            })?;
        Ok(Self { epoch, end_offset })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Epoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let epoch = if (12) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Epoch".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `EndOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let end_offset = if (12) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode EndOffset".into(),
                }
            })?
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { epoch, end_offset })
    }
}

impl KafkaSerialize for FetchableTopicResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topic".into(),
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
        self.topic
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topic".into(),
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

impl KafkaDeserialize for FetchableTopicResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Topic` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topic".into(),
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
        let partitions = <Vec<PartitionData> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            }
        })?;
        Ok(Self {
            topic,
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
            "  [{}] decoding field `Topic` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic = if (0) <= version.0 && version.0 <= (12) {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Topic".into(),
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
        let topic_id = if (13) <= version.0 {
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
        let partitions =
            <Vec<PartitionData> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic,
            topic_id,
            partitions,
        })
    }
}

impl KafkaSerialize for LeaderIdAndEpoch {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.leader_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderId".into(),
            })?;
        self.leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.leader_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderId".into(),
            })?;
        self.leader_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for LeaderIdAndEpoch {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `LeaderId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let leader_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderId".into(),
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
        Ok(Self {
            leader_id,
            leader_epoch,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `LeaderId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let leader_id = if (12) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderId".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `LeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let leader_epoch = if (12) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderEpoch".into(),
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
            leader_id,
            leader_epoch,
        })
    }
}

impl KafkaSerialize for NodeEndpoint {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.node_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode NodeId".into(),
            })?;
        self.host
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.port
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        self.rack
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Rack".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.node_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode NodeId".into(),
            })?;
        self.host
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.port
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.rack {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Rack".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.rack {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Rack".into(),
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

impl KafkaDeserialize for NodeEndpoint {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `NodeId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let node_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode NodeId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Host` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let host =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Port` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let port = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Port".into(),
        })?;
        tracing::trace!(
            "  [{}] classic decode field `Rack` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let rack = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Rack".into(),
            }
        })?;
        Ok(Self {
            node_id,
            host,
            port,
            rack,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `NodeId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let node_id = if (16) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode NodeId".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Host` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let host = if (16) <= version.0 {
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Host".into(),
                },
            )?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Port` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let port = if (16) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Port".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Rack` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let rack = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Rack".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Rack".into(),
                }
            })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            node_id,
            host,
            port,
            rack,
        })
    }
}

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.high_watermark
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode HighWatermark".into(),
            })?;
        self.last_stable_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LastStableOffset".into(),
            })?;
        self.log_start_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LogStartOffset".into(),
            })?;
        self.diverging_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DivergingEpoch".into(),
            })?;
        self.current_leader
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeader".into(),
            })?;
        self.snapshot_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SnapshotId".into(),
            })?;
        self.aborted_transactions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AbortedTransactions".into(),
            })?;
        self.preferred_read_replica
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PreferredReadReplica".into(),
            })?;
        self.records
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Records".into(),
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
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.high_watermark
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode HighWatermark".into(),
            })?;
        self.last_stable_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LastStableOffset".into(),
            })?;
        self.log_start_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LogStartOffset".into(),
            })?;
        self.diverging_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DivergingEpoch".into(),
            })?;
        self.current_leader
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeader".into(),
            })?;
        self.snapshot_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SnapshotId".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.aborted_transactions {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode AbortedTransactions".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.aborted_transactions {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode AbortedTransactions".into(),
                })?;
            }
        }
        self.preferred_read_replica
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PreferredReadReplica".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.records {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Records".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.records {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Records".into(),
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

impl KafkaDeserialize for PartitionData {
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
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `HighWatermark` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let high_watermark =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode HighWatermark".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `LastStableOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let last_stable_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LastStableOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `LogStartOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let log_start_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LogStartOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `DivergingEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let diverging_epoch = <EpochEndOffset as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode DivergingEpoch".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `CurrentLeader` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let current_leader = <LeaderIdAndEpoch as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode CurrentLeader".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `SnapshotId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let snapshot_id =
            <SnapshotId as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode SnapshotId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `AbortedTransactions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let aborted_transactions =
            <Option<Vec<AbortedTransaction>> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode AbortedTransactions".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] classic decode field `PreferredReadReplica` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let preferred_read_replica =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PreferredReadReplica".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Records` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let records = <Option<Vec<u8>> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Records".into(),
            }
        })?;
        Ok(Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            diverging_epoch,
            current_leader,
            snapshot_id,
            aborted_transactions,
            preferred_read_replica,
            records,
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
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `HighWatermark` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let high_watermark = <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode HighWatermark".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `LastStableOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let last_stable_offset = if (4) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LastStableOffset".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `LogStartOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let log_start_offset = if (5) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LogStartOffset".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `DivergingEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let diverging_epoch = if (12) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <EpochEndOffset as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                    .map_err(|_| DecodeError::Protocol {
                        message: "failed to decode DivergingEpoch".into(),
                    })?
            }
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `CurrentLeader` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let current_leader = if (12) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <LeaderIdAndEpoch as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                    .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode CurrentLeader".into(),
                })?
            }
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `SnapshotId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let snapshot_id = if (12) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <SnapshotId as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                    .map_err(|_| DecodeError::Protocol {
                        message: "failed to decode SnapshotId".into(),
                    })?
            }
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `AbortedTransactions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let aborted_transactions = if is_flexible {
            <Option<Vec<AbortedTransaction>> as KafkaDeserialize>::decode_flexible(
                buf, version, true,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode AbortedTransactions".into(),
            })?
        } else {
            <Option<Vec<AbortedTransaction>> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode AbortedTransactions".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `PreferredReadReplica` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let preferred_read_replica = if (11) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode PreferredReadReplica".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Records` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let records = if is_flexible {
            <Option<Vec<u8>> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Records".into(),
                },
            )?
        } else {
            <Option<Vec<u8>> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Records".into(),
                }
            })?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            error_code,
            high_watermark,
            last_stable_offset,
            log_start_offset,
            diverging_epoch,
            current_leader,
            snapshot_id,
            aborted_transactions,
            preferred_read_replica,
            records,
        })
    }
}

impl KafkaSerialize for SnapshotId {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.end_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode EndOffset".into(),
            })?;
        self.epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Epoch".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.end_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode EndOffset".into(),
            })?;
        self.epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Epoch".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SnapshotId {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `EndOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let end_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode EndOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Epoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let epoch = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Epoch".into(),
        })?;
        Ok(Self { end_offset, epoch })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `EndOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let end_offset = <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode EndOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Epoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
            |_| DecodeError::Protocol {
                message: "failed to decode Epoch".into(),
            },
        )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { end_offset, epoch })
    }
}
