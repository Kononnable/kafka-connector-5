#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// MetadataResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 3+.
    pub throttle_time_ms: i32,
    /// Each broker in the response.
    pub brokers: Vec<MetadataResponseBroker>,
    /// The cluster ID that responding broker belongs to.
    /// Available in version 2+.
    pub cluster_id: Option<String>,
    /// The ID of the controller broker.
    /// Available in version 1+.
    pub controller_id: i32,
    /// Each topic in the response.
    pub topics: Vec<MetadataResponseTopic>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataResponseBroker {
    /// The broker ID.
    pub node_id: i32,
    /// The broker hostname.
    pub host: String,
    /// The broker port.
    pub port: i32,
    /// The rack of the broker, or null if it has not been assigned to a rack.
    /// Available in version 1+.
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataResponsePartition {
    /// The partition error, or 0 if there was no error.
    pub error_code: i16,
    /// The partition index.
    pub partition_index: i32,
    /// The ID of the leader broker.
    pub leader_id: i32,
    /// The leader epoch of this partition.
    /// Available in version 7+.
    pub leader_epoch: i32,
    /// The set of all nodes that host this partition.
    pub replica_nodes: Vec<i32>,
    /// The set of nodes that are in sync with the leader for this partition.
    pub isr_nodes: Vec<i32>,
    /// The set of offline replicas of this partition.
    /// Available in version 5+.
    pub offline_replicas: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataResponseTopic {
    /// The topic error, or 0 if there was no error.
    pub error_code: i16,
    /// The topic name.
    pub name: String,
    /// True if the topic is internal.
    /// Available in version 1+.
    pub is_internal: bool,
    /// Each partition in the topic.
    pub partitions: Vec<MetadataResponsePartition>,
}

impl ApiResponse for MetadataResponse {
    type Request = crate::generated::MetadataRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(3)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(7)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (7),
            "version {} is not supported by {} (supported: 0-7)",
            version.0,
            stringify!(Self)
        );
        if (3) <= version.0 {
            self.throttle_time_ms
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        self.brokers
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Brokers"))?;
        if (2) <= version.0 {
            self.cluster_id
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ClusterId"))?;
        }
        if (1) <= version.0 {
            self.controller_id
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ControllerId"))?;
        }
        self.topics
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let throttle_time_ms = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let brokers = <Vec<MetadataResponseBroker> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Brokers"))?;
        let cluster_id = if (2) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ClusterId"))?
        } else {
            Default::default()
        };
        let controller_id = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ControllerId"))?
        } else {
            Default::default()
        };
        let topics = <Vec<MetadataResponseTopic> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
        })
    }
}
impl KafkaSerialize for MetadataResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.brokers
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Brokers".into(),
            })?;
        self.cluster_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClusterId".into(),
            })?;
        self.controller_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ControllerId".into(),
            })?;
        self.topics
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for MetadataResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let brokers =
            <Vec<MetadataResponseBroker> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Brokers".into(),
                }
            })?;
        let cluster_id = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ClusterId".into(),
            }
        })?;
        let controller_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ControllerId".into(),
            })?;
        let topics =
            <Vec<MetadataResponseTopic> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
        })
    }
}

impl KafkaSerialize for MetadataResponseBroker {
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
}

impl KafkaDeserialize for MetadataResponseBroker {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let node_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode NodeId".into(),
            })?;
        let host =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
            })?;
        let port = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Port".into(),
        })?;
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
}

impl KafkaSerialize for MetadataResponsePartition {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
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
        self.replica_nodes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicaNodes".into(),
            })?;
        self.isr_nodes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsrNodes".into(),
            })?;
        self.offline_replicas
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode OfflineReplicas".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for MetadataResponsePartition {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let leader_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderId".into(),
            })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        let replica_nodes =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ReplicaNodes".into(),
            })?;
        let isr_nodes =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsrNodes".into(),
            })?;
        let offline_replicas =
            <Vec<i32> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode OfflineReplicas".into(),
            })?;
        Ok(Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            offline_replicas,
        })
    }
}

impl KafkaSerialize for MetadataResponseTopic {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        self.is_internal
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode IsInternal".into(),
            })?;
        self.partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for MetadataResponseTopic {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        let is_internal =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode IsInternal".into(),
            })?;
        let partitions = <Vec<MetadataResponsePartition> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            })?;
        Ok(Self {
            error_code,
            name,
            is_internal,
            partitions,
        })
    }
}
