#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// LeaderAndIsrRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrRequest {
    /// The current controller ID.
    pub controller_id: i32,
    /// The current controller epoch.
    pub controller_epoch: i32,
    /// The current broker epoch.
    /// Available in version 2+.
    pub broker_epoch: i64,
    /// Each topic.
    /// Available in version 2+.
    pub topic_states: Vec<LeaderAndIsrRequestTopicState>,
    /// The state of each partition
    /// Available in version 0-1.
    pub partition_states_v0: Vec<LeaderAndIsrRequestPartitionStateV0>,
    /// The current live leaders.
    pub live_leaders: Vec<LeaderAndIsrLiveLeader>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrLiveLeader {
    /// The leader's broker ID.
    pub broker_id: i32,
    /// The leader's hostname.
    pub host_name: String,
    /// The leader's port.
    pub port: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrRequestPartitionState {
    /// The partition index.
    pub partition_index: i32,
    /// The controller epoch.
    pub controller_epoch: i32,
    /// The broker ID of the leader.
    pub leader_key: i32,
    /// The leader epoch.
    pub leader_epoch: i32,
    /// The in-sync replica IDs.
    pub isr_replicas: Vec<i32>,
    /// The ZooKeeper version.
    pub zk_version: i32,
    /// The replica IDs.
    pub replicas: Vec<i32>,
    /// Whether the replica should have existed on the broker or not.
    /// Available in version 1+.
    pub is_new: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrRequestPartitionStateV0 {
    /// The topic name.
    /// Available in version 0-1.
    pub topic_name: String,
    /// The partition index.
    /// Available in version 0-1.
    pub partition_index: i32,
    /// The controller epoch.
    /// Available in version 0-1.
    pub controller_epoch: i32,
    /// The broker ID of the leader.
    /// Available in version 0-1.
    pub leader_key: i32,
    /// The leader epoch.
    /// Available in version 0-1.
    pub leader_epoch: i32,
    /// The in-sync replica IDs.
    /// Available in version 0-1.
    pub isr_replicas: Vec<i32>,
    /// The ZooKeeper version.
    /// Available in version 0-1.
    pub zk_version: i32,
    /// The replica IDs.
    /// Available in version 0-1.
    pub replicas: Vec<i32>,
    /// Whether the replica should have existed on the broker or not.
    /// Available in version 1.
    pub is_new: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrRequestTopicState {
    /// The topic name.
    /// Available in version 2+.
    pub name: String,
    /// The state of each partition
    pub partition_states: Vec<LeaderAndIsrRequestPartitionState>,
}

impl ApiRequest for LeaderAndIsrRequest {
    type Response = crate::generated::LeaderAndIsrResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(4)
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
        if (2) <= version.0 {
            self.broker_epoch
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        }
        if (2) <= version.0 {
            self.topic_states
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode TopicStates"))?;
        }
        if (0) <= version.0 && version.0 <= (1) {
            self.partition_states_v0
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode PartitionStatesV0"))?;
        }
        self.live_leaders
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode LiveLeaders"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let controller_id = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ControllerId"))?;
        let controller_epoch = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ControllerEpoch"))?;
        let broker_epoch = if (2) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?
        } else {
            Default::default()
        };
        let topic_states = if (2) <= version.0 {
            <Vec<LeaderAndIsrRequestTopicState> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode TopicStates"))?
        } else {
            Default::default()
        };
        let partition_states_v0 = if (0) <= version.0 && version.0 <= (1) {
            <Vec<LeaderAndIsrRequestPartitionStateV0> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode PartitionStatesV0"))?
        } else {
            Default::default()
        };
        let live_leaders = <Vec<LeaderAndIsrLiveLeader> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode LiveLeaders"))?;
        Ok(Self {
            controller_id,
            controller_epoch,
            broker_epoch,
            topic_states,
            partition_states_v0,
            live_leaders,
        })
    }
}
impl KafkaSerialize for LeaderAndIsrRequest {
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
        self.topic_states
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicStates".into(),
            })?;
        self.partition_states_v0
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionStatesV0".into(),
            })?;
        self.live_leaders
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LiveLeaders".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrRequest {
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
        let topic_states = <Vec<LeaderAndIsrRequestTopicState> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicStates".into(),
            })?;
        let partition_states_v0 =
            <Vec<LeaderAndIsrRequestPartitionStateV0> as KafkaDeserialize>::decode(buf).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PartitionStatesV0".into(),
                },
            )?;
        let live_leaders =
            <Vec<LeaderAndIsrLiveLeader> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LiveLeaders".into(),
                }
            })?;
        Ok(Self {
            controller_id,
            controller_epoch,
            broker_epoch,
            topic_states,
            partition_states_v0,
            live_leaders,
        })
    }
}

impl KafkaSerialize for LeaderAndIsrLiveLeader {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerId".into(),
            })?;
        self.host_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode HostName".into(),
            })?;
        self.port
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrLiveLeader {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let broker_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BrokerId".into(),
            })?;
        let host_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode HostName".into(),
            })?;
        let port = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Port".into(),
        })?;
        Ok(Self {
            broker_id,
            host_name,
            port,
        })
    }
}

impl KafkaSerialize for LeaderAndIsrRequestPartitionState {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.controller_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ControllerEpoch".into(),
            })?;
        self.leader_key
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderKey".into(),
            })?;
        self.leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        self.isr_replicas
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsrReplicas".into(),
            })?;
        self.zk_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ZkVersion".into(),
            })?;
        self.replicas
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Replicas".into(),
            })?;
        self.is_new
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsNew".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrRequestPartitionState {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let controller_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ControllerEpoch".into(),
            })?;
        let leader_key =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderKey".into(),
            })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        let isr_replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsrReplicas".into(),
            })?;
        let zk_version =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ZkVersion".into(),
            })?;
        let replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Replicas".into(),
            })?;
        let is_new =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsNew".into(),
            })?;
        Ok(Self {
            partition_index,
            controller_epoch,
            leader_key,
            leader_epoch,
            isr_replicas,
            zk_version,
            replicas,
            is_new,
        })
    }
}

impl KafkaSerialize for LeaderAndIsrRequestPartitionStateV0 {
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
        self.controller_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ControllerEpoch".into(),
            })?;
        self.leader_key
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderKey".into(),
            })?;
        self.leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        self.isr_replicas
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsrReplicas".into(),
            })?;
        self.zk_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ZkVersion".into(),
            })?;
        self.replicas
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Replicas".into(),
            })?;
        self.is_new
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsNew".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrRequestPartitionStateV0 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicName".into(),
            })?;
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let controller_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ControllerEpoch".into(),
            })?;
        let leader_key =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderKey".into(),
            })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        let isr_replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsrReplicas".into(),
            })?;
        let zk_version =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ZkVersion".into(),
            })?;
        let replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Replicas".into(),
            })?;
        let is_new =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsNew".into(),
            })?;
        Ok(Self {
            topic_name,
            partition_index,
            controller_epoch,
            leader_key,
            leader_epoch,
            isr_replicas,
            zk_version,
            replicas,
            is_new,
        })
    }
}

impl KafkaSerialize for LeaderAndIsrRequestTopicState {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.partition_states
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionStates".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrRequestTopicState {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let partition_states =
            <Vec<LeaderAndIsrRequestPartitionState> as KafkaDeserialize>::decode(buf).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PartitionStates".into(),
                },
            )?;
        Ok(Self {
            name,
            partition_states,
        })
    }
}
