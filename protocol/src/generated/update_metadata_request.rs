#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// UpdateMetadataRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateMetadataRequest {
    /// The controller id.
    pub controller_id: i32,
    /// The controller epoch.
    pub controller_epoch: i32,
    /// The broker epoch.
    /// Available in version 5+.
    pub broker_epoch: i64,
    /// Each topic that we would like to update.
    /// Available in version 5+.
    pub topic_states: Vec<UpdateMetadataRequestTopicState>,
    /// Each partition that we would like to update.
    /// Available in version 0-4.
    pub partition_states_v0: Vec<UpdateMetadataRequestPartitionStateV0>,
    /// Brokers. Type: []UpdateMetadataRequestBroker.
    pub brokers: Vec<UpdateMetadataRequestBroker>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateMetadataPartitionState {
    /// The partition index.
    /// Available in version 5+.
    pub partition_index: i32,
    /// The controller epoch.
    /// Available in version 5+.
    pub controller_epoch: i32,
    /// The ID of the broker which is the current partition leader.
    /// Available in version 5+.
    pub leader: i32,
    /// The leader epoch of this partition.
    /// Available in version 5+.
    pub leader_epoch: i32,
    /// The brokers which are in the ISR for this partition.
    /// Available in version 5+.
    pub isr: Vec<i32>,
    /// The Zookeeper version.
    /// Available in version 5+.
    pub zk_version: i32,
    /// All the replicas of this partition.
    /// Available in version 5+.
    pub replicas: Vec<i32>,
    /// The replicas of this partition which are offline.
    /// Available in version 5+.
    pub offline_replicas: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateMetadataRequestBroker {
    /// Id. Type: int32.
    pub id: i32,
    /// The broker hostname.
    /// Available in version 0.
    pub v0_host: String,
    /// The broker port.
    /// Available in version 0.
    pub v0_port: i32,
    /// The broker endpoints.
    /// Available in version 1+.
    pub endpoints: Vec<UpdateMetadataRequestEndpoint>,
    /// The rack which this broker belongs to.
    /// Available in version 2+.
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateMetadataRequestEndpoint {
    /// The port of this endpoint
    /// Available in version 1+.
    pub port: i32,
    /// The hostname of this endpoint
    /// Available in version 1+.
    pub host: String,
    /// The listener name.
    /// Available in version 3+.
    pub listener: String,
    /// The security protocol type.
    /// Available in version 1+.
    pub security_protocol: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateMetadataRequestPartitionStateV0 {
    /// The topic name.
    /// Available in version 0-4.
    pub topic_name: String,
    /// The partition index.
    /// Available in version 0-4.
    pub partition_index: i32,
    /// The controller epoch.
    /// Available in version 0-4.
    pub controller_epoch: i32,
    /// The ID of the broker which is the current partition leader.
    /// Available in version 0-4.
    pub leader: i32,
    /// The leader epoch of this partition.
    /// Available in version 0-4.
    pub leader_epoch: i32,
    /// The brokers which are in the ISR for this partition.
    /// Available in version 0-4.
    pub isr: Vec<i32>,
    /// The Zookeeper version.
    /// Available in version 0-4.
    pub zk_version: i32,
    /// All the replicas of this partition.
    /// Available in version 0-4.
    pub replicas: Vec<i32>,
    /// The replicas of this partition which are offline.
    /// Available in version 4.
    pub offline_replicas: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateMetadataRequestTopicState {
    /// The topic name.
    pub topic_name: String,
    /// The partition that we would like to update.
    /// Available in version 5+.
    pub partition_states: Vec<UpdateMetadataPartitionState>,
}

impl ApiRequest for UpdateMetadataRequest {
    type Response = crate::generated::UpdateMetadataResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(6)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(5)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (5),
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        self.controller_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ControllerId"))?;
        self.controller_epoch
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ControllerEpoch"))?;
        if (5) <= version.0 {
            self.broker_epoch
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        }
        if (5) <= version.0 {
            self.topic_states
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode TopicStates"))?;
        }
        if (0) <= version.0 && version.0 <= (4) {
            self.partition_states_v0
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode PartitionStatesV0"))?;
        }
        self.brokers
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Brokers"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let controller_id = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ControllerId"))?;
        let controller_epoch = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ControllerEpoch"))?;
        let broker_epoch = if (5) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?
        } else {
            Default::default()
        };
        let topic_states = if (5) <= version.0 {
            <Vec<UpdateMetadataRequestTopicState> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode TopicStates"))?
        } else {
            Default::default()
        };
        let partition_states_v0 = if (0) <= version.0 && version.0 <= (4) {
            <Vec<UpdateMetadataRequestPartitionStateV0> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode PartitionStatesV0"))?
        } else {
            Default::default()
        };
        let brokers = <Vec<UpdateMetadataRequestBroker> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Brokers"))?;
        Ok(Self {
            controller_id,
            controller_epoch,
            broker_epoch,
            topic_states,
            partition_states_v0,
            brokers,
        })
    }
}
impl KafkaSerialize for UpdateMetadataRequest {
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
        self.brokers
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Brokers".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for UpdateMetadataRequest {
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
        let topic_states = <Vec<UpdateMetadataRequestTopicState> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicStates".into(),
            })?;
        let partition_states_v0 =
            <Vec<UpdateMetadataRequestPartitionStateV0> as KafkaDeserialize>::decode(buf).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PartitionStatesV0".into(),
                },
            )?;
        let brokers =
            <Vec<UpdateMetadataRequestBroker> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Brokers".into(),
                }
            })?;
        Ok(Self {
            controller_id,
            controller_epoch,
            broker_epoch,
            topic_states,
            partition_states_v0,
            brokers,
        })
    }
}

impl KafkaSerialize for UpdateMetadataPartitionState {
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
        self.leader
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Leader".into(),
            })?;
        self.leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        self.isr
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Isr".into(),
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
        self.offline_replicas
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode OfflineReplicas".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for UpdateMetadataPartitionState {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let controller_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ControllerEpoch".into(),
            })?;
        let leader = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Leader".into(),
        })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        let isr =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Isr".into(),
            })?;
        let zk_version =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ZkVersion".into(),
            })?;
        let replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Replicas".into(),
            })?;
        let offline_replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode OfflineReplicas".into(),
            })?;
        Ok(Self {
            partition_index,
            controller_epoch,
            leader,
            leader_epoch,
            isr,
            zk_version,
            replicas,
            offline_replicas,
        })
    }
}

impl KafkaSerialize for UpdateMetadataRequestBroker {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Id".into(),
            })?;
        self.v0_host
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode V0Host".into(),
            })?;
        self.v0_port
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode V0Port".into(),
            })?;
        self.endpoints
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Endpoints".into(),
            })?;
        self.rack
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Rack".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for UpdateMetadataRequestBroker {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let id = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Id".into(),
        })?;
        let v0_host =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode V0Host".into(),
            })?;
        let v0_port =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode V0Port".into(),
            })?;
        let endpoints = <Vec<UpdateMetadataRequestEndpoint> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Endpoints".into(),
            })?;
        let rack = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Rack".into(),
            }
        })?;
        Ok(Self {
            id,
            v0_host,
            v0_port,
            endpoints,
            rack,
        })
    }
}

impl KafkaSerialize for UpdateMetadataRequestEndpoint {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.port
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        self.host
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.listener
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Listener".into(),
            })?;
        self.security_protocol
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SecurityProtocol".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for UpdateMetadataRequestEndpoint {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let port = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Port".into(),
        })?;
        let host =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
            })?;
        let listener =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Listener".into(),
            })?;
        let security_protocol =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode SecurityProtocol".into(),
            })?;
        Ok(Self {
            port,
            host,
            listener,
            security_protocol,
        })
    }
}

impl KafkaSerialize for UpdateMetadataRequestPartitionStateV0 {
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
        self.leader
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Leader".into(),
            })?;
        self.leader_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        self.isr
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Isr".into(),
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
        self.offline_replicas
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode OfflineReplicas".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for UpdateMetadataRequestPartitionStateV0 {
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
        let leader = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Leader".into(),
        })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        let isr =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Isr".into(),
            })?;
        let zk_version =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ZkVersion".into(),
            })?;
        let replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Replicas".into(),
            })?;
        let offline_replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode OfflineReplicas".into(),
            })?;
        Ok(Self {
            topic_name,
            partition_index,
            controller_epoch,
            leader,
            leader_epoch,
            isr,
            zk_version,
            replicas,
            offline_replicas,
        })
    }
}

impl KafkaSerialize for UpdateMetadataRequestTopicState {
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

impl KafkaDeserialize for UpdateMetadataRequestTopicState {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicName".into(),
            })?;
        let partition_states = <Vec<UpdateMetadataPartitionState> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionStates".into(),
            })?;
        Ok(Self {
            topic_name,
            partition_states,
        })
    }
}
