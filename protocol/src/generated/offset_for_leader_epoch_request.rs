#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// OffsetForLeaderEpochRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetForLeaderEpochRequest {
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    /// Available in version 3+.
    pub replica_id: i32,
    /// Each topic to get offsets for.
    pub topics: Vec<OffsetForLeaderTopic>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetForLeaderPartition {
    /// The partition index.
    pub partition: i32,
    /// An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH error code will be returned.
    /// Available in version 2+.
    pub current_leader_epoch: i32,
    /// The epoch to look up an offset for.
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetForLeaderTopic {
    /// The topic name.
    pub topic: String,
    /// Each partition to get offsets for.
    pub partitions: Vec<OffsetForLeaderPartition>,
}

impl ApiRequest for OffsetForLeaderEpochRequest {
    type Response = crate::generated::OffsetForLeaderEpochResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(23)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(2)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
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
        let is_flexible = (4) <= version.0;
        if (3) <= version.0 {
            self.replica_id
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ReplicaId"))?;
        }
        self.topics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
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
        let is_flexible = (4) <= version.0;
        let replica_id = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ReplicaId"))?
        } else {
            Default::default()
        };
        let topics = <Vec<OffsetForLeaderTopic> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self { replica_id, topics })
    }
}
impl KafkaSerialize for OffsetForLeaderEpochRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.replica_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicaId".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.replica_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicaId".into(),
            })?;
        self.topics
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for OffsetForLeaderEpochRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `ReplicaId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let replica_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ReplicaId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Topics` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topics =
            <Vec<OffsetForLeaderTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        Ok(Self { replica_id, topics })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ReplicaId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let replica_id = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ReplicaId".into(),
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
        let topics = <Vec<OffsetForLeaderTopic> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Topics".into(),
        })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { replica_id, topics })
    }
}

impl KafkaSerialize for OffsetForLeaderPartition {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partition".into(),
            })?;
        self.current_leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeaderEpoch".into(),
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
        self.partition
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partition".into(),
            })?;
        self.current_leader_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeaderEpoch".into(),
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

impl KafkaDeserialize for OffsetForLeaderPartition {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Partition` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partition".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `CurrentLeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let current_leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode CurrentLeaderEpoch".into(),
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
            partition,
            current_leader_epoch,
            leader_epoch,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Partition` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partition".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `CurrentLeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let current_leader_epoch = if (2) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode CurrentLeaderEpoch".into(),
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
        let leader_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition,
            current_leader_epoch,
            leader_epoch,
        })
    }
}

impl KafkaSerialize for OffsetForLeaderTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topic".into(),
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

impl KafkaDeserialize for OffsetForLeaderTopic {
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
            "  [{}] classic decode field `Partitions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partitions =
            <Vec<OffsetForLeaderPartition> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                }
            })?;
        Ok(Self { topic, partitions })
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
        let topic = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topic".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Partitions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partitions = <Vec<OffsetForLeaderPartition> as KafkaDeserialize>::decode_flexible(
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
        Ok(Self { topic, partitions })
    }
}
