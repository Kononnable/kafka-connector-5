#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
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
    fn get_api_key() -> ApiKey {
        ApiKey::new(59)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
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
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.node_endpoints.encode(buf, version, is_flexible)?;
        } else if !self.node_endpoints.is_empty() {
            return Err(SerializationError::Encode(
                "field 'NodeEndpoints' is not available in this version",
            ));
        }
        if is_flexible {
            let mut __tag_count = 0u64;
            if !self.node_endpoints.is_empty() {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if !self.node_endpoints.is_empty() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.node_endpoints.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topics = <Vec<TopicSnapshot> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let mut node_endpoints = if (1) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        node_endpoints =
                            <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            topics,
            node_endpoints,
        })
    }
}
impl KafkaSerialize for FetchSnapshotResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.throttle_time_ms.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.node_endpoints.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut __tag_count = 0u64;
            if !self.node_endpoints.is_empty() {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if !self.node_endpoints.is_empty() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.node_endpoints.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
        }
        Ok(())
    }
}

impl KafkaDeserialize for FetchSnapshotResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topics = <Vec<TopicSnapshot> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let mut node_endpoints = if (1) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf, version, is_flexible)?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        node_endpoints =
                            <Vec<NodeEndpoint> as KafkaDeserialize>::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            topics,
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
    ) -> Result<(), crate::traits::SerializationError> {
        self.leader_id.encode(buf, version, is_flexible)?;
        self.leader_epoch.encode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let leader_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let leader_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
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
    ) -> Result<(), crate::traits::SerializationError> {
        if (1) <= version.0 {
            self.node_id.encode(buf, version, is_flexible)?;
        }
        if (1) <= version.0 {
            self.host.encode(buf, version, is_flexible)?;
        }
        if (1) <= version.0 {
            self.port.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
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
    ) -> Result<Self, crate::traits::SerializationError> {
        let node_id = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let host = if (1) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let port = if (1) <= version.0 {
            <u16 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            node_id,
            host,
            port,
        })
    }
}

impl KafkaSerialize for PartitionSnapshot {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.index.encode(buf, version, is_flexible)?;
        self.error_code.encode(buf, version, is_flexible)?;
        self.snapshot_id.encode(buf, version, is_flexible)?;
        self.current_leader.encode(buf, version, is_flexible)?;
        self.size.encode(buf, version, is_flexible)?;
        self.position.encode(buf, version, is_flexible)?;
        self.unaligned_records.encode(buf, version, is_flexible)?;
        if is_flexible {
            let mut __tag_count = 0u64;
            if self.current_leader != Default::default() {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if self.current_leader != Default::default() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.current_leader.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionSnapshot {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let snapshot_id = <SnapshotId as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let mut current_leader = if is_flexible {
            Default::default()
        } else {
            <LeaderIdAndEpoch as KafkaDeserialize>::decode(buf, version, is_flexible)?
        };
        let size = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let position = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let unaligned_records = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        current_leader =
                            <LeaderIdAndEpoch as KafkaDeserialize>::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            index,
            error_code,
            snapshot_id,
            current_leader,
            size,
            position,
            unaligned_records,
        })
    }
}

impl KafkaSerialize for SnapshotId {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.end_offset.encode(buf, version, is_flexible)?;
        self.epoch.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SnapshotId {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let end_offset = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { end_offset, epoch })
    }
}

impl KafkaSerialize for TopicSnapshot {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for TopicSnapshot {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let partitions =
            <Vec<PartitionSnapshot> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}
