#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// FetchSnapshotRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FetchSnapshotRequest {
    /// The clusterId if known, this is used to validate metadata fetches prior to broker registration.
    pub cluster_id: Option<String>,
    /// The broker ID of the follower.
    pub replica_id: i32,
    /// The maximum bytes to fetch from all of the snapshots.
    pub max_bytes: i32,
    /// The topics to fetch.
    pub topics: Vec<TopicSnapshot>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartitionSnapshot {
    /// The partition index.
    pub partition: i32,
    /// The current leader epoch of the partition, -1 for unknown leader epoch.
    pub current_leader_epoch: i32,
    /// The snapshot endOffset and epoch to fetch.
    pub snapshot_id: SnapshotId,
    /// The byte position within the snapshot to start fetching from.
    pub position: i64,
    /// The directory id of the follower fetching.
    /// Available in version 1+.
    pub replica_directory_id: [u8; 16],
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SnapshotId {
    /// The end offset of the snapshot.
    pub end_offset: i64,
    /// The epoch of the snapshot.
    pub epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicSnapshot {
    /// The name of the topic to fetch.
    pub name: String,
    /// The partitions to fetch.
    pub partitions: Vec<PartitionSnapshot>,
}

impl ApiRequest for FetchSnapshotRequest {
    type Response = crate::generated::FetchSnapshotResponse;
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
        self.cluster_id.encode(buf, version, is_flexible)?;
        self.replica_id.encode(buf, version, is_flexible)?;
        self.max_bytes.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            let mut __tag_count = 0u64;
            if self.cluster_id.is_some() {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if self.cluster_id.is_some() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.cluster_id.encode(&mut __tmp, version, true)?;
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
        let mut cluster_id = if is_flexible {
            Default::default()
        } else {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        };
        let replica_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let max_bytes = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        cluster_id = KafkaDeserialize::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            cluster_id,
            replica_id,
            max_bytes,
            topics,
        })
    }
}
impl KafkaSerialize for FetchSnapshotRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if !is_flexible {
            self.cluster_id.encode(buf, version, is_flexible)?;
        }
        self.replica_id.encode(buf, version, is_flexible)?;
        self.max_bytes.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            let mut __tag_count = 0u64;
            if self.cluster_id.is_some() {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if self.cluster_id.is_some() {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.cluster_id.encode(&mut __tmp, version, true)?;
                crate::protocol::serialization::encode_unsigned_varint(__tmp.len() as u64, buf);
                buf.put_slice(&__tmp);
            }
        }
        Ok(())
    }
}

impl KafkaDeserialize for FetchSnapshotRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let mut cluster_id = if is_flexible {
            Default::default()
        } else {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        };
        let replica_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let max_bytes = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (__tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
            for _ in 0..__tag_count {
                let (__tag_id, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                let (__tag_len, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
                match __tag_id {
                    0 => {
                        cluster_id = KafkaDeserialize::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            cluster_id,
            replica_id,
            max_bytes,
            topics,
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
        self.partition.encode(buf, version, is_flexible)?;
        self.current_leader_epoch
            .encode(buf, version, is_flexible)?;
        self.snapshot_id.encode(buf, version, is_flexible)?;
        self.position.encode(buf, version, is_flexible)?;
        if (1) <= version.0 && !is_flexible {
            self.replica_directory_id
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            let mut __tag_count = 0u64;
            if self.replica_directory_id != [0u8; 16] {
                __tag_count += 1;
            }
            crate::protocol::serialization::encode_unsigned_varint(__tag_count, buf);
            if self.replica_directory_id != [0u8; 16] {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
                let mut __tmp = bytes::BytesMut::new();
                self.replica_directory_id
                    .encode(&mut __tmp, version, true)?;
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
        let partition = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let current_leader_epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let snapshot_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let position = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let mut replica_directory_id = if (1) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                KafkaDeserialize::decode(buf, version, is_flexible)?
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
                        replica_directory_id = KafkaDeserialize::decode(buf, version, true)?;
                    }
                    _ => {
                        buf.advance(__tag_len as usize);
                    }
                }
            }
        }
        Ok(Self {
            partition,
            current_leader_epoch,
            snapshot_id,
            position,
            replica_directory_id,
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
        let end_offset = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let epoch = KafkaDeserialize::decode(buf, version, is_flexible)?;
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
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}
