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
    pub delete_partitions: bool,
    /// The partitions to stop.
    /// Available in version 0.
    pub ungrouped_partitions: Vec<StopReplicaPartitionV0>,
    /// The topics to stop.
    /// Available in version 1+.
    pub topics: Vec<StopReplicaTopic>,
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
pub struct StopReplicaTopic {
    /// The topic name.
    /// Available in version 1+.
    pub name: String,
    /// The partition indexes.
    /// Available in version 1+.
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
        ApiVersion::new(2)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
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
        self.delete_partitions
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode DeletePartitions"))?;
        if version.0 == (0) {
            self.ungrouped_partitions
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode UngroupedPartitions"))?;
        }
        if (1) <= version.0 {
            self.topics
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
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
        let delete_partitions = <bool as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode DeletePartitions"))?;
        let ungrouped_partitions = if version.0 == (0) {
            <Vec<StopReplicaPartitionV0> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode UngroupedPartitions"))?
        } else {
            Default::default()
        };
        let topics = if (1) <= version.0 {
            <Vec<StopReplicaTopic> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode Topics"))?
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
        let topics = <Vec<StopReplicaTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            }
        })?;
        Ok(Self {
            controller_id,
            controller_epoch,
            broker_epoch,
            delete_partitions,
            ungrouped_partitions,
            topics,
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

impl KafkaSerialize for StopReplicaTopic {
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

impl KafkaDeserialize for StopReplicaTopic {
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
