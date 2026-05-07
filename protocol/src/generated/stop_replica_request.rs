#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// StopReplicaRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StopReplicaRequest {
    /// The controller id.
    pub controller_id: i32,
    /// The controller epoch.
    pub controller_epoch: i32,
    /// The broker epoch.
    /// Available in version 1+.
    pub broker_epoch: i64,
    /// Whether these partitions should be deleted.
    /// Available in version 0-2.
    pub delete_partitions: bool,
    /// The partitions to stop.
    /// Available in version 0.
    pub ungrouped_partitions: Vec<StopReplicaPartitionV0>,
    /// The topics to stop.
    /// Available in version 1-2.
    pub topics: Vec<StopReplicaTopicV1>,
    /// Each topic.
    /// Available in version 3+.
    pub topic_states: Vec<StopReplicaTopicState>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StopReplicaPartitionState {
    /// The partition index.
    /// Available in version 3+.
    pub partition_index: i32,
    /// The leader epoch.
    /// Available in version 3+.
    pub leader_epoch: i32,
    /// Whether this partition should be deleted.
    /// Available in version 3+.
    pub delete_partition: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StopReplicaPartitionV0 {
    /// The topic name.
    /// Available in version 0.
    pub topic_name: String,
    /// The partition index.
    /// Available in version 0.
    pub partition_index: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StopReplicaTopicState {
    /// The topic name.
    /// Available in version 3+.
    pub topic_name: String,
    /// The state of each partition
    /// Available in version 3+.
    pub partition_states: Vec<StopReplicaPartitionState>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StopReplicaTopicV1 {
    /// The topic name.
    /// Available in version 1-2.
    pub name: String,
    /// The partition indexes.
    /// Available in version 1-2.
    pub partition_indexes: Vec<i32>,
}

impl ApiRequest for StopReplicaRequest {
    type Response = crate::generated::StopReplicaResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(5)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(3)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 0-3)",
            version.0,
            stringify!(Self)
        );
        self.controller_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ControllerId"))?;
        self.controller_epoch
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ControllerEpoch"))?;
        if (1) <= version.0 {
            self.broker_epoch
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        }
        if (0) <= version.0 && version.0 <= (2) {
            self.delete_partitions
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode DeletePartitions"))?;
        }
        if version.0 == (0) {
            self.ungrouped_partitions
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode UngroupedPartitions"))?;
        }
        if (1) <= version.0 && version.0 <= (2) {
            self.topics
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        }
        if (3) <= version.0 {
            self.topic_states
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode TopicStates"))?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let controller_id = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ControllerId"))?;
        let controller_epoch = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ControllerEpoch"))?;
        let broker_epoch = if (1) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?
        } else {
            Default::default()
        };
        let delete_partitions = if (0) <= version.0 && version.0 <= (2) {
            <bool as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode DeletePartitions"))?
        } else {
            Default::default()
        };
        let ungrouped_partitions = if version.0 == (0) {
            <Vec<StopReplicaPartitionV0> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode UngroupedPartitions"))?
        } else {
            Default::default()
        };
        let topics = if (1) <= version.0 && version.0 <= (2) {
            <Vec<StopReplicaTopicV1> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode Topics"))?
        } else {
            Default::default()
        };
        let topic_states = if (3) <= version.0 {
            <Vec<StopReplicaTopicState> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode TopicStates"))?
        } else {
            Default::default()
        };
        Ok(Self {
            controller_id,
            controller_epoch,
            broker_epoch,
            delete_partitions,
            ungrouped_partitions,
            topics,
            topic_states,
        })
    }
}
impl KafkaSerialize for StopReplicaRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.controller_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ControllerId".into(),
            })?;
        self.controller_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ControllerEpoch".into(),
            })?;
        self.broker_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerEpoch".into(),
            })?;
        self.delete_partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DeletePartitions".into(),
            })?;
        self.ungrouped_partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode UngroupedPartitions".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        self.topic_states
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicStates".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for StopReplicaRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let controller_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ControllerId".into(),
            })?;
        let controller_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ControllerEpoch".into(),
            })?;
        let broker_epoch =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BrokerEpoch".into(),
            })?;
        let delete_partitions =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode DeletePartitions".into(),
            })?;
        let ungrouped_partitions = <Vec<StopReplicaPartitionV0> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode UngroupedPartitions".into(),
        })?;
        let topics = <Vec<StopReplicaTopicV1> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            }
        })?;
        let topic_states =
            <Vec<StopReplicaTopicState> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicStates".into(),
                }
            })?;
        Ok(Self {
            controller_id,
            controller_epoch,
            broker_epoch,
            delete_partitions,
            ungrouped_partitions,
            topics,
            topic_states,
        })
    }
}

impl KafkaSerialize for StopReplicaPartitionState {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        self.delete_partition
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DeletePartition".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for StopReplicaPartitionState {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        let delete_partition =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode DeletePartition".into(),
            })?;
        Ok(Self {
            partition_index,
            leader_epoch,
            delete_partition,
        })
    }
}

impl KafkaSerialize for StopReplicaPartitionV0 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicName".into(),
            })?;
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for StopReplicaPartitionV0 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicName".into(),
            })?;
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        Ok(Self {
            topic_name,
            partition_index,
        })
    }
}

impl KafkaSerialize for StopReplicaTopicState {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicName".into(),
            })?;
        self.partition_states
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionStates".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for StopReplicaTopicState {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicName".into(),
            })?;
        let partition_states = <Vec<StopReplicaPartitionState> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionStates".into(),
            })?;
        Ok(Self {
            topic_name,
            partition_states,
        })
    }
}

impl KafkaSerialize for StopReplicaTopicV1 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partition_indexes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndexes".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for StopReplicaTopicV1 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partition_indexes =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndexes".into(),
            })?;
        Ok(Self {
            name,
            partition_indexes,
        })
    }
}
