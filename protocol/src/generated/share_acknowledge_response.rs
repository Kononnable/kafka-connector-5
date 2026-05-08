#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ShareAcknowledgeResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShareAcknowledgeResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top level response error code.
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    pub error_message: Option<String>,
    /// The response topics.
    pub responses: Vec<ShareAcknowledgeTopicResponse>,
    /// Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
    pub node_endpoints: Vec<NodeEndpoint>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    pub leader_id: i32,
    /// The latest known leader epoch.
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    pub node_id: i32,
    /// The node's hostname.
    pub host: String,
    /// The node's port.
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    pub error_message: Option<String>,
    /// The current leader of the partition.
    pub current_leader: LeaderIdAndEpoch,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShareAcknowledgeTopicResponse {
    /// The unique topic ID.
    pub topic_id: [u8; 16],
    /// The topic partitions.
    pub partitions: Vec<PartitionData>,
}

impl ApiResponse for ShareAcknowledgeResponse {
    type Request = crate::generated::ShareAcknowledgeRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(79)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 1-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.error_message
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorMessage"))?;
        self.responses
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Responses"))?;
        self.node_endpoints
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode NodeEndpoints"))?;
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
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorMessage"))?;
        let responses = <Vec<ShareAcknowledgeTopicResponse> as KafkaDeserialize>::decode(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| SerializationError::Decode("failed to decode Responses"))?;
        let node_endpoints =
            <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode NodeEndpoints"))?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            responses,
            node_endpoints,
        })
    }
}
impl KafkaSerialize for ShareAcknowledgeResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
        self.responses
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Responses".into(),
            })?;
        self.node_endpoints
            .encode(buf, version, is_flexible)
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

impl KafkaDeserialize for ShareAcknowledgeResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            })?;
        let responses = <Vec<ShareAcknowledgeTopicResponse> as KafkaDeserialize>::decode(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Responses".into(),
        })?;
        let node_endpoints =
            <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode NodeEndpoints".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            responses,
            node_endpoints,
        })
    }
}

impl KafkaSerialize for LeaderIdAndEpoch {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.leader_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderId".into(),
            })?;
        self.leader_epoch
            .encode(buf, version, is_flexible)
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
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let leader_id =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderId".into(),
                }
            })?;
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderEpoch".into(),
                }
            })?;
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
        self.rack
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Rack".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for NodeEndpoint {
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
        let rack = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Rack".into(),
            })?;
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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
        self.current_leader
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeader".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let partition_index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            })?;
        let current_leader =
            <LeaderIdAndEpoch as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode CurrentLeader".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            error_code,
            error_message,
            current_leader,
        })
    }
}

impl KafkaSerialize for ShareAcknowledgeTopicResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.topic_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
        self.partitions
            .encode(buf, version, is_flexible)
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

impl KafkaDeserialize for ShareAcknowledgeTopicResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?;
        let partitions =
            <Vec<PartitionData> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}
