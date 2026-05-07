#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ShareFetchResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShareFetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top-level response error code.
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    pub error_message: Option<String>,
    /// The time in milliseconds for which the acquired records are locked.
    /// Available in version 1+.
    pub acquisition_lock_timeout_ms: i32,
    /// The response topics.
    pub responses: Vec<ShareFetchableTopicResponse>,
    /// Endpoints for all current leaders enumerated in PartitionData with error NOT_LEADER_OR_FOLLOWER.
    pub node_endpoints: Vec<NodeEndpoint>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AcquiredRecords {
    /// The earliest offset in this batch of acquired records.
    pub first_offset: i64,
    /// The last offset of this batch of acquired records.
    pub last_offset: i64,
    /// The delivery count of this batch of acquired records.
    pub delivery_count: i16,
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
    /// The fetch error code, or 0 if there was no fetch error.
    pub error_code: i16,
    /// The fetch error message, or null if there was no fetch error.
    pub error_message: Option<String>,
    /// The acknowledge error code, or 0 if there was no acknowledge error.
    pub acknowledge_error_code: i16,
    /// The acknowledge error message, or null if there was no acknowledge error.
    pub acknowledge_error_message: Option<String>,
    /// The current leader of the partition.
    pub current_leader: LeaderIdAndEpoch,
    /// The record data.
    pub records: Option<Vec<u8>>,
    /// The acquired records.
    pub acquired_records: Vec<AcquiredRecords>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShareFetchableTopicResponse {
    /// The unique topic ID.
    pub topic_id: [u8; 16],
    /// The topic partitions.
    pub partitions: Vec<PartitionData>,
}

impl ApiResponse for ShareFetchResponse {
    type Request = crate::generated::ShareFetchRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(78)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
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
        let is_flexible = true;
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.error_message
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorMessage"))?;
        if (1) <= version.0 {
            self.acquisition_lock_timeout_ms
                .encode_flexible(buf, is_flexible)
                .map_err(|_| {
                    SerializationError::Encode("failed to encode AcquisitionLockTimeoutMs")
                })?;
        }
        self.responses
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Responses"))?;
        self.node_endpoints
            .encode_flexible(buf, is_flexible)
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
        let is_flexible = true;
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let error_message =
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ErrorMessage"))?;
        let acquisition_lock_timeout_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                SerializationError::Decode("failed to decode AcquisitionLockTimeoutMs")
            })?
        } else {
            Default::default()
        };
        let responses = <Vec<ShareFetchableTopicResponse> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| SerializationError::Decode("failed to decode Responses"))?;
        let node_endpoints =
            <Vec<NodeEndpoint> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode NodeEndpoints"))?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            acquisition_lock_timeout_ms,
            responses,
            node_endpoints,
        })
    }
}
impl KafkaSerialize for ShareFetchResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
        self.acquisition_lock_timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AcquisitionLockTimeoutMs".into(),
            })?;
        self.responses
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Responses".into(),
            })?;
        self.node_endpoints
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode NodeEndpoints".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.error_message {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode ErrorMessage".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.error_message {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ErrorMessage".into(),
                })?;
            }
        }
        self.acquisition_lock_timeout_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AcquisitionLockTimeoutMs".into(),
            })?;
        self.responses
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Responses".into(),
            })?;
        self.node_endpoints
            .encode_flexible(buf, is_flexible)
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

impl KafkaDeserialize for ShareFetchResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `AcquisitionLockTimeoutMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let acquisition_lock_timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode AcquisitionLockTimeoutMs".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Responses` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let responses = <Vec<ShareFetchableTopicResponse> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Responses".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `NodeEndpoints` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let node_endpoints =
            <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode NodeEndpoints".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            acquisition_lock_timeout_ms,
            responses,
            node_endpoints,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `ThrottleTimeMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ThrottleTimeMs".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_message = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `AcquisitionLockTimeoutMs` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let acquisition_lock_timeout_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode AcquisitionLockTimeoutMs".into(),
                }
            })?
        } else {
            Default::default()
        };
        tracing::trace!(
            "  [{}] decoding field `Responses` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let responses = <Vec<ShareFetchableTopicResponse> as KafkaDeserialize>::decode_flexible(
            buf,
            version,
            is_flexible,
        )
        .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Responses".into(),
        })?;
        tracing::trace!(
            "  [{}] decoding field `NodeEndpoints` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let node_endpoints =
            <Vec<NodeEndpoint> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode NodeEndpoints".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            acquisition_lock_timeout_ms,
            responses,
            node_endpoints,
        })
    }
}

impl KafkaSerialize for AcquiredRecords {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.first_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FirstOffset".into(),
            })?;
        self.last_offset
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LastOffset".into(),
            })?;
        self.delivery_count
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DeliveryCount".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.first_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FirstOffset".into(),
            })?;
        self.last_offset
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LastOffset".into(),
            })?;
        self.delivery_count
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode DeliveryCount".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AcquiredRecords {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `FirstOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let first_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode FirstOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `LastOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let last_offset =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LastOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `DeliveryCount` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let delivery_count =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode DeliveryCount".into(),
            })?;
        Ok(Self {
            first_offset,
            last_offset,
            delivery_count,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `FirstOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let first_offset = <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode FirstOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `LastOffset` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let last_offset = <i64 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode LastOffset".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `DeliveryCount` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let delivery_count = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode DeliveryCount".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            first_offset,
            last_offset,
            delivery_count,
        })
    }
}

impl KafkaSerialize for LeaderIdAndEpoch {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
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
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.leader_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode LeaderId".into(),
            })?;
        self.leader_epoch
            .encode_flexible(buf, is_flexible)
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
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `LeaderId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let leader_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `LeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let leader_epoch =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
            })?;
        Ok(Self {
            leader_id,
            leader_epoch,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `LeaderId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let leader_id = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `LeaderEpoch` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let leader_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode LeaderEpoch".into(),
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
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.node_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode NodeId".into(),
            })?;
        self.host
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Host".into(),
            })?;
        self.port
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Port".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.rack {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Rack".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.rack {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Rack".into(),
                })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for NodeEndpoint {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `NodeId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let node_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode NodeId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Host` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let host =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Port` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let port = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
            message: "failed to decode Port".into(),
        })?;
        tracing::trace!(
            "  [{}] classic decode field `Rack` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
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
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `NodeId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let node_id = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode NodeId".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Host` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let host = <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Host".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `Port` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let port = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
            |_| DecodeError::Protocol {
                message: "failed to decode Port".into(),
            },
        )?;
        tracing::trace!(
            "  [{}] decoding field `Rack` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let rack = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Rack".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Rack".into(),
                }
            })?
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

impl KafkaSerialize for PartitionData {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.partition_index
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorMessage".into(),
            })?;
        self.acknowledge_error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AcknowledgeErrorCode".into(),
            })?;
        self.acknowledge_error_message
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AcknowledgeErrorMessage".into(),
            })?;
        self.current_leader
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeader".into(),
            })?;
        self.records
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Records".into(),
            })?;
        self.acquired_records
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AcquiredRecords".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.partition_index
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PartitionIndex".into(),
            })?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.error_message {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode ErrorMessage".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.error_message {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ErrorMessage".into(),
                })?;
            }
        }
        self.acknowledge_error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AcknowledgeErrorCode".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.acknowledge_error_message {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode AcknowledgeErrorMessage".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.acknowledge_error_message {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode AcknowledgeErrorMessage".into(),
                })?;
            }
        }
        self.current_leader
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeader".into(),
            })?;
        if is_flexible {
            if let Some(ref __val) = self.records {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Records".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.records {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Records".into(),
                })?;
            }
        }
        self.acquired_records
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AcquiredRecords".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `PartitionIndex` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_index =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `ErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_message = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ErrorMessage".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `AcknowledgeErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let acknowledge_error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode AcknowledgeErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `AcknowledgeErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let acknowledge_error_message =
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode AcknowledgeErrorMessage".into(),
                }
            })?;
        tracing::trace!(
            "  [{}] classic decode field `CurrentLeader` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let current_leader = <LeaderIdAndEpoch as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode CurrentLeader".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `Records` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let records = <Option<Vec<u8>> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Records".into(),
            }
        })?;
        tracing::trace!(
            "  [{}] classic decode field `AcquiredRecords` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let acquired_records =
            <Vec<AcquiredRecords> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode AcquiredRecords".into(),
                }
            })?;
        Ok(Self {
            partition_index,
            error_code,
            error_message,
            acknowledge_error_code,
            acknowledge_error_message,
            current_leader,
            records,
            acquired_records,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `PartitionIndex` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partition_index = <i32 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode PartitionIndex".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        tracing::trace!(
            "  [{}] decoding field `ErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let error_message = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorMessage".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `AcknowledgeErrorCode` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let acknowledge_error_code =
            <i16 as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode AcknowledgeErrorCode".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `AcknowledgeErrorMessage` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let acknowledge_error_message = if is_flexible {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode AcknowledgeErrorMessage".into(),
                },
            )?
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode AcknowledgeErrorMessage".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `CurrentLeader` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let current_leader =
            <LeaderIdAndEpoch as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode CurrentLeader".into(),
                })?;
        tracing::trace!(
            "  [{}] decoding field `Records` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let records = if is_flexible {
            <Option<Vec<u8>> as KafkaDeserialize>::decode_flexible(buf, version, true).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Records".into(),
                },
            )?
        } else {
            <Option<Vec<u8>> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Records".into(),
                }
            })?
        };
        tracing::trace!(
            "  [{}] decoding field `AcquiredRecords` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let acquired_records =
            <Vec<AcquiredRecords> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode AcquiredRecords".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            error_code,
            error_message,
            acknowledge_error_code,
            acknowledge_error_message,
            current_leader,
            records,
            acquired_records,
        })
    }
}

impl KafkaSerialize for ShareFetchableTopicResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.topic_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
        self.partitions
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partitions".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.topic_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TopicId".into(),
            })?;
        self.partitions
            .encode_flexible(buf, is_flexible)
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

impl KafkaDeserialize for ShareFetchableTopicResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `TopicId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_id =
            <[u8; 16] as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TopicId".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `Partitions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partitions = <Vec<PartitionData> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Partitions".into(),
            }
        })?;
        Ok(Self {
            topic_id,
            partitions,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `TopicId` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode TopicId".into(),
        })?;
        tracing::trace!(
            "  [{}] decoding field `Partitions` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let partitions =
            <Vec<PartitionData> as KafkaDeserialize>::decode_flexible(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                })?;
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
