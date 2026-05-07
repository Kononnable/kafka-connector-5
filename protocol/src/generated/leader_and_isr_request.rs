#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
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
    /// The type that indicates whether all topics are included in the request
    /// Available in version 5+.
    pub r#type: i8,
    /// The state of each partition, in a v0 or v1 message.
    /// Available in version 0-1.
    pub ungrouped_partition_states: Vec<LeaderAndIsrPartitionState>,
    /// Each topic.
    /// Available in version 2+.
    pub topic_states: Vec<LeaderAndIsrTopicState>,
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
pub struct LeaderAndIsrPartitionState {
    /// The topic name.  This is only present in v0 or v1.
    /// Available in version 0-1.
    pub topic_name: String,
    /// The partition index.
    pub partition_index: i32,
    /// The controller epoch.
    pub controller_epoch: i32,
    /// The broker ID of the leader.
    pub leader: i32,
    /// The leader epoch.
    pub leader_epoch: i32,
    /// The in-sync replica IDs.
    pub isr: Vec<i32>,
    /// The ZooKeeper version.
    pub zk_version: i32,
    /// The replica IDs.
    pub replicas: Vec<i32>,
    /// The replica IDs that we are adding this partition to, or null if no replicas are being added.
    /// Available in version 3+.
    pub adding_replicas: Vec<i32>,
    /// The replica IDs that we are removing this partition from, or null if no replicas are being removed.
    /// Available in version 3+.
    pub removing_replicas: Vec<i32>,
    /// Whether the replica should have existed on the broker or not.
    /// Available in version 1+.
    pub is_new: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderAndIsrTopicState {
    /// The topic name.
    /// Available in version 2+.
    pub topic_name: String,
    /// The unique topic ID.
    /// Available in version 5+.
    pub topic_id: [u8; 16],
    /// The state of each partition
    /// Available in version 2+.
    pub partition_states: Vec<LeaderAndIsrPartitionState>,
}

impl ApiRequest for LeaderAndIsrRequest {
    type Response = crate::generated::LeaderAndIsrResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(4)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(5)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (5),
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (4) <= version.0;
        self.controller_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ControllerId"))?;
        self.controller_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ControllerEpoch"))?;
        if (2) <= version.0 {
            self.broker_epoch
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        }
        if (5) <= version.0 {
            self.r#type
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode Type"))?;
        }
        if (0) <= version.0 && version.0 <= (1) {
            self.ungrouped_partition_states
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode UngroupedPartitionStates")
                })?;
        }
        if (2) <= version.0 {
            self.topic_states
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode TopicStates"))?;
        }
        self.live_leaders
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode LiveLeaders"))?;
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
        let controller_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ControllerId"))?;
        let controller_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ControllerEpoch"))?;
        let broker_epoch = if (2) <= version.0 {
            <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?
        } else {
            Default::default()
        };
        let r#type = if (5) <= version.0 {
            <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Type"))?
        } else {
            Default::default()
        };
        let ungrouped_partition_states = if (0) <= version.0 && version.0 <= (1) {
            <Vec<LeaderAndIsrPartitionState> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Decode("failed to decode UngroupedPartitionStates")
                })?
        } else {
            Default::default()
        };
        let topic_states = if (2) <= version.0 {
            <Vec<LeaderAndIsrTopicState> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode TopicStates"))?
        } else {
            Default::default()
        };
        let live_leaders =
            <Vec<LeaderAndIsrLiveLeader> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode LiveLeaders"))?;
        Ok(Self {
            controller_id,
            controller_epoch,
            broker_epoch,
            r#type,
            ungrouped_partition_states,
            topic_states,
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
        self.r#type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Type".into(),
            })?;
        self.ungrouped_partition_states
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode UngroupedPartitionStates".into(),
            })?;
        self.topic_states
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicStates".into(),
            })?;
        self.live_leaders
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LiveLeaders".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.controller_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ControllerId".into(),
            })?;
        self.controller_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ControllerEpoch".into(),
            })?;
        self.broker_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerEpoch".into(),
            })?;
        self.r#type
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Type".into(),
            })?;
        self.ungrouped_partition_states
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode UngroupedPartitionStates".into(),
            })?;
        self.topic_states
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicStates".into(),
            })?;
        self.live_leaders
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LiveLeaders".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
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
        let r#type = <i8 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Type".into(),
        })?;
        let ungrouped_partition_states =
            <Vec<LeaderAndIsrPartitionState> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode UngroupedPartitionStates".into(),
                }
            })?;
        let topic_states =
            <Vec<LeaderAndIsrTopicState> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicStates".into(),
                }
            })?;
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
            r#type,
            ungrouped_partition_states,
            topic_states,
            live_leaders,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let controller_id =
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ControllerId".into(),
                }
            })?;
        let controller_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ControllerEpoch".into(),
            })?;
        let broker_epoch =
            <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BrokerEpoch".into(),
                }
            })?;
        let r#type = <i8 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Type".into(),
            }
        })?;
        let ungrouped_partition_states =
            <Vec<LeaderAndIsrPartitionState> as KafkaDeserialize>::decode_flexible(
                buf,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode UngroupedPartitionStates".into(),
            })?;
        let topic_states =
            <Vec<LeaderAndIsrTopicState> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode TopicStates".into(),
                })?;
        let live_leaders =
            <Vec<LeaderAndIsrLiveLeader> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode LiveLeaders".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            controller_id,
            controller_epoch,
            broker_epoch,
            r#type,
            ungrouped_partition_states,
            topic_states,
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
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.broker_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerId".into(),
            })?;
        self.host_name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode HostName".into(),
            })?;
        self.port
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
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
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let broker_id =
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BrokerId".into(),
                }
            })?;
        let host_name =
            <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode HostName".into(),
                }
            })?;
        let port = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Port".into(),
            }
        })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            broker_id,
            host_name,
            port,
        })
    }
}

impl KafkaSerialize for LeaderAndIsrPartitionState {
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
        self.adding_replicas
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AddingReplicas".into(),
            })?;
        self.removing_replicas
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RemovingReplicas".into(),
            })?;
        self.is_new
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsNew".into(),
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
        self.partition_index
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.controller_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ControllerEpoch".into(),
            })?;
        self.leader
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Leader".into(),
            })?;
        self.leader_epoch
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderEpoch".into(),
            })?;
        self.isr
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Isr".into(),
            })?;
        self.zk_version
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ZkVersion".into(),
            })?;
        self.replicas
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Replicas".into(),
            })?;
        self.adding_replicas
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AddingReplicas".into(),
            })?;
        self.removing_replicas
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RemovingReplicas".into(),
            })?;
        self.is_new
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsNew".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrPartitionState {
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
        let adding_replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode AddingReplicas".into(),
            })?;
        let removing_replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode RemovingReplicas".into(),
            })?;
        let is_new =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsNew".into(),
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
            adding_replicas,
            removing_replicas,
            is_new,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicName".into(),
                }
            })?;
        let partition_index = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let controller_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ControllerEpoch".into(),
            })?;
        let leader =
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Leader".into(),
                }
            })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderEpoch".into(),
                }
            })?;
        let isr =
            <Vec<i32> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Isr".into(),
                }
            })?;
        let zk_version =
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ZkVersion".into(),
                }
            })?;
        let replicas =
            <Vec<i32> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Replicas".into(),
                }
            })?;
        let adding_replicas = <Vec<i32> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode AddingReplicas".into(),
            })?;
        let removing_replicas = <Vec<i32> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode RemovingReplicas".into(),
        })?;
        let is_new =
            <bool as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IsNew".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partition_index,
            controller_epoch,
            leader,
            leader_epoch,
            isr,
            zk_version,
            replicas,
            adding_replicas,
            removing_replicas,
            is_new,
        })
    }
}

impl KafkaSerialize for LeaderAndIsrTopicState {
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
        self.partition_states
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionStates".into(),
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
        self.partition_states
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionStates".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for LeaderAndIsrTopicState {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicName".into(),
            })?;
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicId".into(),
            })?;
        let partition_states = <Vec<LeaderAndIsrPartitionState> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode PartitionStates".into(),
        })?;
        Ok(Self {
            topic_name,
            topic_id,
            partition_states,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let topic_name =
            <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicName".into(),
                }
            })?;
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?;
        let partition_states =
            <Vec<LeaderAndIsrPartitionState> as KafkaDeserialize>::decode_flexible(
                buf,
                is_flexible,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionStates".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            topic_id,
            partition_states,
        })
    }
}
