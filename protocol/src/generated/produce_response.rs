#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ProduceResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProduceResponse {
    /// Each produce response.
    pub responses: Vec<TopicProduceResponse>,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// Endpoints for all current-leaders enumerated in PartitionProduceResponses, with errors NOT_LEADER_OR_FOLLOWER.
    /// Available in version 10+.
    pub node_endpoints: Vec<NodeEndpoint>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BatchIndexAndErrorMessage {
    /// The batch index of the record that caused the batch to be dropped.
    /// Available in version 8+.
    pub batch_index: i32,
    /// The error message of the record that caused the batch to be dropped.
    /// Available in version 8+.
    pub batch_index_error_message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaderIdAndEpoch {
    /// The ID of the current leader or -1 if the leader is unknown.
    /// Available in version 10+.
    pub leader_id: i32,
    /// The latest known leader epoch.
    /// Available in version 10+.
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeEndpoint {
    /// The ID of the associated node.
    /// Available in version 10+.
    pub node_id: i32,
    /// The node's hostname.
    /// Available in version 10+.
    pub host: String,
    /// The node's port.
    /// Available in version 10+.
    pub port: i32,
    /// The rack of the node, or null if it has not been assigned to a rack.
    /// Available in version 10+.
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionProduceResponse {
    /// The partition index.
    pub index: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The base offset.
    pub base_offset: i64,
    /// The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.
    /// Available in version 2+.
    pub log_append_time_ms: i64,
    /// The log start offset.
    /// Available in version 5+.
    pub log_start_offset: i64,
    /// The batch indices of records that caused the batch to be dropped.
    /// Available in version 8+.
    pub record_errors: Vec<BatchIndexAndErrorMessage>,
    /// The global error message summarizing the common root cause of the records that caused the batch to be dropped.
    /// Available in version 8+.
    pub error_message: Option<String>,
    /// The leader broker that the producer should use for future requests.
    /// Available in version 10+.
    pub current_leader: LeaderIdAndEpoch,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicProduceResponse {
    /// The topic name.
    /// Available in version 0-12.
    pub name: String,
    /// The unique topic ID
    /// Available in version 13+.
    pub topic_id: [u8; 16],
    /// Each partition that we produced to within the topic.
    pub partition_responses: Vec<PartitionProduceResponse>,
}

impl ApiResponse for ProduceResponse {
    type Request = crate::generated::ProduceRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(0)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(3)
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
            (3) <= version.0 && version.0 <= (13),
            "version {} is not supported by {} (supported: 3-13)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.responses
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Responses"))?;
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        if (10) <= version.0 {
            self.node_endpoints
                .encode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode NodeEndpoints"))?;
        } else if !self.node_endpoints.is_empty() {
            return Err(SerializationError::Encode(
                "field 'NodeEndpoints' is not available in this version",
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
        let responses =
            <Vec<TopicProduceResponse> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Responses"))?;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let node_endpoints = if (10) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf, version, is_flexible)
                    .map_err(|_| SerializationError::Decode("failed to decode NodeEndpoints"))?
            }
        } else {
            Default::default()
        };
        Ok(Self {
            responses,
            throttle_time_ms,
            node_endpoints,
        })
    }
}
impl KafkaSerialize for ProduceResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.responses
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Responses".into(),
            })?;
        if (1) <= version.0 {
            self.throttle_time_ms
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ThrottleTimeMs".into(),
                })?;
        }
        if (10) <= version.0 {
            self.node_endpoints
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode NodeEndpoints".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ProduceResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let responses =
            <Vec<TopicProduceResponse> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Responses".into(),
                })?;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                }
            })?
        } else {
            Default::default()
        };
        let node_endpoints = if (10) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                    |_| DecodeError::Protocol {
                        message: "failed to decode NodeEndpoints".into(),
                    },
                )?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            responses,
            throttle_time_ms,
            node_endpoints,
        })
    }
}

impl KafkaSerialize for BatchIndexAndErrorMessage {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (8) <= version.0 {
            self.batch_index
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode BatchIndex".into(),
                })?;
        }
        if (8) <= version.0 {
            self.batch_index_error_message
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode BatchIndexErrorMessage".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for BatchIndexAndErrorMessage {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let batch_index = if (8) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BatchIndex".into(),
                }
            })?
        } else {
            Default::default()
        };
        let batch_index_error_message = if (8) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode BatchIndexErrorMessage".into(),
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
            batch_index,
            batch_index_error_message,
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
        if (10) <= version.0 {
            self.leader_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode LeaderId".into(),
                })?;
        }
        if (10) <= version.0 {
            self.leader_epoch
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode LeaderEpoch".into(),
                })?;
        }
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
        let leader_id = if (10) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let leader_epoch = if (10) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LeaderEpoch".into(),
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
        if (10) <= version.0 {
            self.node_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode NodeId".into(),
                })?;
        }
        if (10) <= version.0 {
            self.host.encode(buf, version, is_flexible).map_err(|_| {
                EncodeError::ValueTooLarge {
                    message: "failed to encode Host".into(),
                }
            })?;
        }
        if (10) <= version.0 {
            self.port.encode(buf, version, is_flexible).map_err(|_| {
                EncodeError::ValueTooLarge {
                    message: "failed to encode Port".into(),
                }
            })?;
        }
        if (10) <= version.0 {
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

impl KafkaDeserialize for NodeEndpoint {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let node_id = if (10) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode NodeId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let host = if (10) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Host".into(),
                }
            })?
        } else {
            Default::default()
        };
        let port = if (10) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Port".into(),
                }
            })?
        } else {
            Default::default()
        };
        let rack = if (10) <= version.0 {
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

impl KafkaSerialize for PartitionProduceResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.index
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Index".into(),
            })?;
        self.error_code
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.base_offset
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BaseOffset".into(),
            })?;
        if (2) <= version.0 {
            self.log_append_time_ms
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode LogAppendTimeMs".into(),
                })?;
        }
        if (5) <= version.0 {
            self.log_start_offset
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode LogStartOffset".into(),
                })?;
        }
        if (8) <= version.0 {
            self.record_errors
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode RecordErrors".into(),
                })?;
        }
        if (8) <= version.0 {
            self.error_message
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ErrorMessage".into(),
                })?;
        }
        if (10) <= version.0 {
            self.current_leader
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode CurrentLeader".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionProduceResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Index".into(),
            }
        })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let base_offset =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode BaseOffset".into(),
                }
            })?;
        let log_append_time_ms = if (2) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LogAppendTimeMs".into(),
                }
            })?
        } else {
            Default::default()
        };
        let log_start_offset = if (5) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode LogStartOffset".into(),
                }
            })?
        } else {
            Default::default()
        };
        let record_errors = if (8) <= version.0 {
            <Vec<BatchIndexAndErrorMessage> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode RecordErrors".into(),
                })?
        } else {
            Default::default()
        };
        let error_message = if (8) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                },
            )?
        } else {
            Default::default()
        };
        let current_leader = if (10) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <LeaderIdAndEpoch as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                    |_| DecodeError::Protocol {
                        message: "failed to decode CurrentLeader".into(),
                    },
                )?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            index,
            error_code,
            base_offset,
            log_append_time_ms,
            log_start_offset,
            record_errors,
            error_message,
            current_leader,
        })
    }
}

impl KafkaSerialize for TopicProduceResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if (0) <= version.0 && version.0 <= (12) {
            self.name.encode(buf, version, is_flexible).map_err(|_| {
                EncodeError::ValueTooLarge {
                    message: "failed to encode Name".into(),
                }
            })?;
        }
        if (13) <= version.0 {
            self.topic_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode TopicId".into(),
                })?;
        }
        self.partition_responses
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionResponses".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicProduceResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let name = if (0) <= version.0 && version.0 <= (12) {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Name".into(),
                }
            })?
        } else {
            Default::default()
        };
        let topic_id = if (13) <= version.0 {
            <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode TopicId".into(),
                }
            })?
        } else {
            Default::default()
        };
        let partition_responses =
            <Vec<PartitionProduceResponse> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode PartitionResponses".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            topic_id,
            partition_responses,
        })
    }
}
