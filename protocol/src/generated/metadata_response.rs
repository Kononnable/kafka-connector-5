#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// MetadataResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 3+.
    pub throttle_time_ms: i32,
    /// A list of brokers present in the cluster.
    pub brokers: Vec<MetadataResponseBroker>,
    /// The cluster ID that responding broker belongs to.
    /// Available in version 2+.
    pub cluster_id: Option<String>,
    /// The ID of the controller broker.
    /// Available in version 1+.
    pub controller_id: i32,
    /// Each topic in the response.
    pub topics: Vec<MetadataResponseTopic>,
    /// 32-bit bitfield to represent authorized operations for this cluster.
    /// Available in version 8-10.
    pub cluster_authorized_operations: i32,
    /// The top-level error code, or 0 if there was no error.
    /// Available in version 13+.
    pub error_code: i16,
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
    /// The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
    pub name: Option<String>,
    /// The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
    /// Available in version 10+.
    pub topic_id: [u8; 16],
    /// True if the topic is internal.
    /// Available in version 1+.
    pub is_internal: bool,
    /// Each partition in the topic.
    pub partitions: Vec<MetadataResponsePartition>,
    /// 32-bit bitfield to represent authorized operations for this topic.
    /// Available in version 8+.
    pub topic_authorized_operations: i32,
}

impl ApiResponse for MetadataResponse {
    type Request = crate::generated::MetadataRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(3)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(13)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(9)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (13),
            "version {} is not supported by {} (supported: 0-13)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (3) <= version.0 {
            self.throttle_time_ms
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        self.brokers
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Brokers"))?;
        if (2) <= version.0 {
            self.cluster_id
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ClusterId"))?;
        } else if self.cluster_id.is_some() {
            return Err(SerializationError::Encode(
                "field 'ClusterId' is not available in this version",
            ));
        }
        if (1) <= version.0 {
            self.controller_id
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ControllerId"))?;
        } else if self.controller_id != 0 {
            return Err(SerializationError::Encode(
                "field 'ControllerId' is not available in this version",
            ));
        }
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        if (8) <= version.0 && version.0 <= (10) {
            self.cluster_authorized_operations
                .encode(buf, version, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode ClusterAuthorizedOperations")
                })?;
        } else if self.cluster_authorized_operations != 0 {
            return Err(SerializationError::Encode(
                "field 'ClusterAuthorizedOperations' is not available in this version",
            ));
        }
        if (13) <= version.0 {
            self.error_code
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        } else if self.error_code != 0 {
            return Err(SerializationError::Encode(
                "field 'ErrorCode' is not available in this version",
            ));
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
        let throttle_time_ms = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let brokers =
            <Vec<MetadataResponseBroker> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Brokers"))?;
        let cluster_id = if (2) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ClusterId"))?
        } else {
            Default::default()
        };
        let controller_id = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ControllerId"))?
        } else {
            Default::default()
        };
        let topics =
            <Vec<MetadataResponseTopic> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let cluster_authorized_operations = if (8) <= version.0 && version.0 <= (10) {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode ClusterAuthorizedOperations")
            })?
        } else {
            Default::default()
        };
        let error_code = if (13) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?
        } else {
            Default::default()
        };
        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            cluster_authorized_operations,
            error_code,
        })
    }
}
impl KafkaSerialize for MetadataResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (3) <= version.0 {
            self.throttle_time_ms
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ThrottleTimeMs".into(),
                })?;
        }
        self.brokers
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Brokers".into(),
            })?;
        if (2) <= version.0 {
            self.cluster_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ClusterId".into(),
                })?;
        }
        if (1) <= version.0 {
            self.controller_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ControllerId".into(),
                })?;
        }
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        if (8) <= version.0 && version.0 <= (10) {
            self.cluster_authorized_operations
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ClusterAuthorizedOperations".into(),
                })?;
        }
        if (13) <= version.0 {
            self.error_code
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ErrorCode".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MetadataResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let throttle_time_ms = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                }
            })?
        } else {
            Default::default()
        };
        let brokers =
            <Vec<MetadataResponseBroker> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Brokers".into(),
                })?;
        let cluster_id = if (2) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ClusterId".into(),
                },
            )?
        } else {
            Default::default()
        };
        let controller_id = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ControllerId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let topics =
            <Vec<MetadataResponseTopic> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Topics".into(),
                })?;
        let cluster_authorized_operations = if (8) <= version.0 && version.0 <= (10) {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ClusterAuthorizedOperations".into(),
                }
            })?
        } else {
            Default::default()
        };
        let error_code = if (13) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
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
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            cluster_authorized_operations,
            error_code,
        })
    }
}

impl KafkaSerialize for MetadataResponseBroker {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.node_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode NodeId".into(),
            })?;
        self.host
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.port
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        if (1) <= version.0 {
            self.rack.encode(buf, version, is_flexible).map_err(|_| {
                EncodeError::ValueTooLarge {
                    message: "failed to encode Rack".into(),
                }
            })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MetadataResponseBroker {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let node_id =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode NodeId".into(),
                }
            })?;
        let host =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Host".into(),
                }
            })?;
        let port = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Port".into(),
            }
        })?;
        let rack = if (1) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Rack".into(),
                },
            )?
        } else {
            Default::default()
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

impl KafkaSerialize for MetadataResponsePartition {
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
        if (7) <= version.0 {
            self.leader_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode LeaderEpoch".into(),
                })?;
        }
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
        if (5) <= version.0 {
            self.offline_replicas
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode OfflineReplicas".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MetadataResponsePartition {
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
        let leader_epoch = if (7) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderEpoch".into(),
                }
            })?
        } else {
            Default::default()
        };
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
        let offline_replicas = if (5) <= version.0 {
            <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode OfflineReplicas".into(),
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
        if (10) <= version.0 {
            self.topic_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TopicId".into(),
                })?;
        }
        if (1) <= version.0 {
            self.is_internal
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode IsInternal".into(),
                })?;
        }
        self.partitions
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        if (8) <= version.0 {
            self.topic_authorized_operations
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TopicAuthorizedOperations".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MetadataResponseTopic {
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
        let topic_id = if (10) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let is_internal = if (1) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode IsInternal".into(),
                }
            })?
        } else {
            Default::default()
        };
        let partitions =
            <Vec<MetadataResponsePartition> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                })?;
        let topic_authorized_operations = if (8) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicAuthorizedOperations".into(),
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
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
        })
    }
}
