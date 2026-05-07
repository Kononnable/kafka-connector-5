#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// TxnOffsetCommitRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TxnOffsetCommitRequest {
    /// The ID of the transaction.
    pub transactional_id: String,
    /// The ID of the group.
    pub group_id: String,
    /// The current producer ID in use by the transactional ID.
    pub producer_id: i64,
    /// The current epoch associated with the producer ID.
    pub producer_epoch: i16,
    /// Each topic that we want to committ offsets for.
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TxnOffsetCommitRequestPartition {
    /// The index of the partition within the topic.
    pub partition_index: i32,
    /// The message offset to be committed.
    pub committed_offset: i64,
    /// The leader epoch of the last consumed record.
    /// Available in version 2+.
    pub committed_leader_epoch: i32,
    /// Any associated metadata the client wants to keep.
    pub committed_metadata: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TxnOffsetCommitRequestTopic {
    /// The topic name.
    pub name: String,
    /// The partitions inside the topic that we want to committ offsets for.
    pub partitions: Vec<TxnOffsetCommitRequestPartition>,
}

impl ApiRequest for TxnOffsetCommitRequest {
    type Response = crate::generated::TxnOffsetCommitResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(28)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(2)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        self.transactional_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode TransactionalId"))?;
        self.group_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode GroupId"))?;
        self.producer_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ProducerId"))?;
        self.producer_epoch
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ProducerEpoch"))?;
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let transactional_id = <String as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode TransactionalId"))?;
        let group_id = <String as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode GroupId"))?;
        let producer_id = <i64 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ProducerId"))?;
        let producer_epoch = <i16 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ProducerEpoch"))?;
        let topics = <Vec<TxnOffsetCommitRequestTopic> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self {
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            topics,
        })
    }
}
impl KafkaSerialize for TxnOffsetCommitRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.transactional_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TransactionalId".into(),
            })?;
        self.group_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        self.producer_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerId".into(),
            })?;
        self.producer_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProducerEpoch".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for TxnOffsetCommitRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let transactional_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TransactionalId".into(),
            })?;
        let group_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode GroupId".into(),
            })?;
        let producer_id =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerId".into(),
            })?;
        let producer_epoch =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProducerEpoch".into(),
            })?;
        let topics =
            <Vec<TxnOffsetCommitRequestTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        Ok(Self {
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            topics,
        })
    }
}

impl KafkaSerialize for TxnOffsetCommitRequestPartition {
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
        self.committed_metadata
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CommittedMetadata".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for TxnOffsetCommitRequestPartition {
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
            committed_metadata,
        })
    }
}

impl KafkaSerialize for TxnOffsetCommitRequestTopic {
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

impl KafkaDeserialize for TxnOffsetCommitRequestTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partitions = <Vec<TxnOffsetCommitRequestPartition> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        Ok(Self { name, partitions })
    }
}
