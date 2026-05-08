#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 1-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.acquisition_lock_timeout_ms
                .encode(buf, version, is_flexible)?;
        } else if self.acquisition_lock_timeout_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'AcquisitionLockTimeoutMs' is not available in this version",
            ));
        }
        self.responses.encode(buf, version, is_flexible)?;
        self.node_endpoints.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_message = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let acquisition_lock_timeout_ms = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let responses = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let node_endpoints = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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
impl KafkaSerialize for ShareFetchResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.acquisition_lock_timeout_ms
                .encode(buf, version, is_flexible)?;
        }
        self.responses.encode(buf, version, is_flexible)?;
        self.node_endpoints.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ShareFetchResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let throttle_time_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_message = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let acquisition_lock_timeout_ms = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let responses = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let node_endpoints = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.first_offset.encode(buf, version, is_flexible)?;
        self.last_offset.encode(buf, version, is_flexible)?;
        self.delivery_count.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AcquiredRecords {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let first_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let last_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let delivery_count = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            first_offset,
            last_offset,
            delivery_count,
        })
    }
}

impl KafkaSerialize for LeaderIdAndEpoch {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.leader_id.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for LeaderIdAndEpoch {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let leader_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let leader_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.node_id.encode(buf, version, is_flexible)?;
        self.host.encode(buf, version, is_flexible)?;
        self.port.encode(buf, version, is_flexible)?;
        self.rack.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for NodeEndpoint {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let node_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let host = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let port = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let rack = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.error_message.encode(buf, version, is_flexible)?;
        self.acknowledge_error_code
            .encode(buf, version, is_flexible)?;
        self.acknowledge_error_message
            .encode(buf, version, is_flexible)?;
        self.current_leader.encode(buf, version, is_flexible)?;
        self.records.encode(buf, version, is_flexible)?;
        self.acquired_records.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition_index = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let error_message = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let acknowledge_error_code = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let acknowledge_error_message = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let current_leader = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let records = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let acquired_records = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topic_id.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ShareFetchableTopicResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topic_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}
