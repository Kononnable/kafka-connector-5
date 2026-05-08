#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
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
        self.cluster_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ClusterId"))?;
        self.replica_id
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ReplicaId"))?;
        self.max_bytes
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode MaxBytes"))?;
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Topics"))?;
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
        let cluster_id = if is_flexible {
            Default::default()
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ClusterId"))?
        };
        let replica_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ReplicaId"))?;
        let max_bytes = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode MaxBytes"))?;
        let topics = <Vec<TopicSnapshot> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Topics"))?;
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
    ) -> Result<(), EncodeError> {
        self.cluster_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClusterId".into(),
            })?;
        self.replica_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ReplicaId".into(),
            })?;
        self.max_bytes
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxBytes".into(),
            })?;
        self.topics
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Topics".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FetchSnapshotRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let cluster_id = if is_flexible {
            Default::default()
        } else {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode ClusterId".into(),
                },
            )?
        };
        let replica_id =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ReplicaId".into(),
                }
            })?;
        let max_bytes =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MaxBytes".into(),
                }
            })?;
        let topics = <Vec<TopicSnapshot> as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Topics".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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
    ) -> Result<(), EncodeError> {
        self.partition
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Partition".into(),
            })?;
        self.current_leader_epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode CurrentLeaderEpoch".into(),
            })?;
        self.snapshot_id
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode SnapshotId".into(),
            })?;
        self.position
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Position".into(),
            })?;
        if (1) <= version.0 {
            self.replica_directory_id
                .encode(buf, version, is_flexible)
                .map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode ReplicaDirectoryId".into(),
                })?;
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for PartitionSnapshot {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let partition =
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Partition".into(),
                }
            })?;
        let current_leader_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode CurrentLeaderEpoch".into(),
            })?;
        let snapshot_id = <SnapshotId as KafkaDeserialize>::decode(buf, version, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode SnapshotId".into(),
            })?;
        let position =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Position".into(),
                }
            })?;
        let replica_directory_id = if (1) <= version.0 {
            if is_flexible {
                Default::default()
            } else {
                <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                    DecodeError::Protocol {
                        message: "failed to decode ReplicaDirectoryId".into(),
                    }
                })?
            }
        } else {
            Default::default()
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
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
    ) -> Result<(), EncodeError> {
        self.end_offset
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode EndOffset".into(),
            })?;
        self.epoch
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Epoch".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
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
    ) -> Result<Self, DecodeError> {
        let end_offset =
            <i64 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode EndOffset".into(),
                }
            })?;
        let epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Epoch".into(),
            }
        })?;
        if is_flexible {
            // Tagged fields (skip)
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
    ) -> Result<(), EncodeError> {
        self.name
            .encode(buf, version, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
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

impl KafkaDeserialize for TopicSnapshot {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf, version, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Name".into(),
                }
            })?;
        let partitions =
            <Vec<PartitionSnapshot> as KafkaDeserialize>::decode(buf, version, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                    message: "failed to decode Partitions".into(),
                })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}
