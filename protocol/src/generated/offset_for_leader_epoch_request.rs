#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// OffsetForLeaderEpochRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetForLeaderEpochRequest {
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    /// Available in version 3+.
    pub replica_id: i32,
    /// Each topic to get offsets for.
    /// IndexMap key `Topic` (string): The topic name.
    pub topics: IndexMap<String, OffsetForLeaderTopic>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetForLeaderPartition {
    /// The partition index.
    pub partition: i32,
    /// An epoch used to fence consumers/replicas with old metadata. If the epoch provided by the client is larger than the current epoch known to the broker, then the UNKNOWN_LEADER_EPOCH error code will be returned. If the provided epoch is smaller, then the FENCED_LEADER_EPOCH error code will be returned.
    /// Available in version 2+.
    pub current_leader_epoch: i32,
    /// The epoch to look up an offset for.
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetForLeaderTopic {
    /// Each partition to get offsets for.
    pub partitions: Vec<OffsetForLeaderPartition>,
}

impl ApiRequest for OffsetForLeaderEpochRequest {
    type Response = crate::generated::OffsetForLeaderEpochResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(23)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            2 <= version.0 && version.0 <= 4,
            "version {} is not supported by {} (supported: 2-4)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 3 <= version.0 {
            self.replica_id.encode(buf, version, is_flexible)?;
        } else if self.replica_id != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "ReplicaId",
                version,
                api_name: "OffsetForLeaderEpochRequest",
            });
        }
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let replica_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { replica_id, topics })
    }
}
impl KafkaCodec for OffsetForLeaderEpochRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 3 <= version.0 {
            self.replica_id.encode(buf, version, is_flexible)?;
        }
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let replica_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { replica_id, topics })
    }
}

impl KafkaCodec for OffsetForLeaderPartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition.encode(buf, version, is_flexible)?;
        if 2 <= version.0 {
            self.current_leader_epoch
                .encode(buf, version, is_flexible)?;
        }
        self.leader_epoch.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition = KafkaCodec::decode(buf, version, is_flexible)?;
        let current_leader_epoch = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let leader_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition,
            current_leader_epoch,
            leader_epoch,
        })
    }
}

impl KafkaCodec for OffsetForLeaderTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { partitions })
    }
}
