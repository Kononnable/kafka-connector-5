#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// FetchSnapshotResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchSnapshotResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The top level response error code.
    pub error_code: i16,
    /// The topics to fetch.
    pub topics: Vec<TopicSnapshot>,
    /// Endpoints for all current-leaders enumerated in PartitionSnapshot.
    /// Available in version 1+.
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
    /// Available in version 1+.
    pub node_id: i32,
    /// The node's hostname.
    /// Available in version 1+.
    pub host: String,
    /// The node's port.
    /// Available in version 1+.
    pub port: u16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionSnapshot {
    /// The partition index.
    pub index: i32,
    /// The error code, or 0 if there was no fetch error.
    pub error_code: i16,
    /// The snapshot endOffset and epoch fetched.
    pub snapshot_id: SnapshotId,
    /// The leader of the partition at the time of the snapshot.
    pub current_leader: LeaderIdAndEpoch,
    /// The total size of the snapshot.
    pub size: i64,
    /// The starting byte position within the snapshot included in the Bytes field.
    pub position: i64,
    /// Snapshot data in records format which may not be aligned on an offset boundary.
    pub unaligned_records: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SnapshotId {
    /// The snapshot end offset.
    pub end_offset: i64,
    /// The snapshot epoch.
    pub epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicSnapshot {
    /// The name of the topic to fetch.
    pub name: String,
    /// The partitions to fetch.
    pub partitions: Vec<PartitionSnapshot>,
}

impl ApiResponse for FetchSnapshotResponse {
    type Request = crate::generated::FetchSnapshotRequest;
    fn get_api_key() -> ApiKey { ApiKey::new(59) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(1) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!((0) <= version.0 && version.0 <= (1), "version {} is not supported by {} (supported: 0-1)", version.0, stringify!(Self));
        let is_flexible = true;
        self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.topics.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
        if (1) <= version.0 {
            self.node_endpoints.encode_flexible(buf, is_flexible).map_err(|_| SerializationError::Encode("failed to encode NodeEndpoints"))?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = true;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let topics = <Vec<TopicSnapshot> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
        let node_endpoints = if (1) <= version.0 {
            <Vec<NodeEndpoint> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| SerializationError::Decode("failed to decode NodeEndpoints"))?
        } else {
            Default::default()
        };
        Ok(Self { throttle_time_ms, error_code, topics, node_endpoints })
    }
}
impl KafkaSerialize for FetchSnapshotResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.error_code.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.topics.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        self.node_endpoints.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NodeEndpoints".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.throttle_time_ms.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ThrottleTimeMs".into() })?;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.topics.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Topics".into() })?;
        self.node_endpoints.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NodeEndpoints".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FetchSnapshotResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let topics = <Vec<TopicSnapshot> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        let node_endpoints = <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode NodeEndpoints".into() })?;
        Ok(Self { throttle_time_ms, error_code, topics, node_endpoints })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ThrottleTimeMs".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let topics = <Vec<TopicSnapshot> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Topics".into() })?;
        let node_endpoints = <Vec<NodeEndpoint> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode NodeEndpoints".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { throttle_time_ms, error_code, topics, node_endpoints })
    }
}

impl KafkaSerialize for LeaderIdAndEpoch {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.leader_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderId".into() })?;
        self.leader_epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderEpoch".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.leader_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderId".into() })?;
        self.leader_epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode LeaderEpoch".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for LeaderIdAndEpoch {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let leader_id = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderId".into() })?;
        let leader_epoch = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderEpoch".into() })?;
        Ok(Self { leader_id, leader_epoch })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let leader_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderId".into() })?;
        let leader_epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode LeaderEpoch".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { leader_id, leader_epoch })
    }
}

impl KafkaSerialize for NodeEndpoint {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.node_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NodeId".into() })?;
        self.host.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Host".into() })?;
        self.port.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Port".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.node_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode NodeId".into() })?;
        self.host.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Host".into() })?;
        self.port.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Port".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for NodeEndpoint {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let node_id = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode NodeId".into() })?;
        let host = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Host".into() })?;
        let port = <u16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Port".into() })?;
        Ok(Self { node_id, host, port })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let node_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode NodeId".into() })?;
        let host = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Host".into() })?;
        let port = <u16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Port".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { node_id, host, port })
    }
}

impl KafkaSerialize for PartitionSnapshot {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.index.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Index".into() })?;
        self.error_code.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.snapshot_id.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode SnapshotId".into() })?;
        self.current_leader.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode CurrentLeader".into() })?;
        self.size.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Size".into() })?;
        self.position.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Position".into() })?;
        self.unaligned_records.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode UnalignedRecords".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.index.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Index".into() })?;
        self.error_code.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode ErrorCode".into() })?;
        self.snapshot_id.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode SnapshotId".into() })?;
        self.current_leader.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode CurrentLeader".into() })?;
        self.size.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Size".into() })?;
        self.position.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Position".into() })?;
        self.unaligned_records.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode UnalignedRecords".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionSnapshot {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let index = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Index".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let snapshot_id = <SnapshotId as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode SnapshotId".into() })?;
        let current_leader = <LeaderIdAndEpoch as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode CurrentLeader".into() })?;
        let size = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Size".into() })?;
        let position = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Position".into() })?;
        let unaligned_records = <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode UnalignedRecords".into() })?;
        Ok(Self { index, error_code, snapshot_id, current_leader, size, position, unaligned_records })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let index = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Index".into() })?;
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode ErrorCode".into() })?;
        let snapshot_id = <SnapshotId as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode SnapshotId".into() })?;
        let current_leader = <LeaderIdAndEpoch as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode CurrentLeader".into() })?;
        let size = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Size".into() })?;
        let position = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Position".into() })?;
        let unaligned_records = <Vec<u8> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode UnalignedRecords".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { index, error_code, snapshot_id, current_leader, size, position, unaligned_records })
    }
}

impl KafkaSerialize for SnapshotId {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.end_offset.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode EndOffset".into() })?;
        self.epoch.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Epoch".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.end_offset.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode EndOffset".into() })?;
        self.epoch.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Epoch".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SnapshotId {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let end_offset = <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode EndOffset".into() })?;
        let epoch = <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Epoch".into() })?;
        Ok(Self { end_offset, epoch })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let end_offset = <i64 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode EndOffset".into() })?;
        let epoch = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Epoch".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { end_offset, epoch })
    }
}

impl KafkaSerialize for TopicSnapshot {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.partitions.encode(buf).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        self.name.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Name".into() })?;
        self.partitions.encode_flexible(buf, is_flexible).map_err(|_| EncodeError::ValueTooLarge { message: "failed to encode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicSnapshot {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let partitions = <Vec<PartitionSnapshot> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        Ok(Self { name, partitions })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let name = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Name".into() })?;
        let partitions = <Vec<PartitionSnapshot> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| DecodeError::Protocol { message: "failed to decode Partitions".into() })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}

