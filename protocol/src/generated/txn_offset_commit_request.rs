#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// TxnOffsetCommitRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TxnOffsetCommitRequest {
    /// The ID of the transaction.
    pub transactional_id: String,
    /// The ID of the group.
    pub group_id: String,
    /// The current producer ID in use by the transactional ID.
    pub producer_id: i64,
    /// The current epoch associated with the producer ID.
    pub producer_epoch: i16,
    /// The generation of the consumer.
    /// Available in version 3+.
    pub generation_id: i32,
    /// The member ID assigned by the group coordinator.
    /// Available in version 3+.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    /// Available in version 3+.
    pub group_instance_id: Option<String>,
    /// Each topic that we want to commit offsets for.
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TxnOffsetCommitRequestPartition {
    /// The index of the partition within the topic.
    pub partition_index: i32,
    /// The message offset to be committed.
    pub committed_offset: i64,
    /// The leader epoch of the last consumed record.
    /// Available in version 2+.
    pub committed_leader_epoch: i32,
    /// Any associated metadata the client wants to keep.
    pub committed_metadata: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TxnOffsetCommitRequestTopic {
    /// The topic name.
    pub name: String,
    /// The partitions inside the topic that we want to commit offsets for.
    pub partitions: Vec<TxnOffsetCommitRequestPartition>,
}

impl ApiRequest for TxnOffsetCommitRequest {
    type Response = crate::generated::TxnOffsetCommitResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(28)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(5)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 5,
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.transactional_id.encode(buf, version, is_flexible)?;
        self.group_id.encode(buf, version, is_flexible)?;
        self.producer_id.encode(buf, version, is_flexible)?;
        self.producer_epoch.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.generation_id.encode(buf, version, is_flexible)?;
        } else if self.generation_id != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "GenerationId",
                version,
                api_name: "TxnOffsetCommitRequest",
            });
        }
        if 3 <= version.0 {
            self.member_id.encode(buf, version, is_flexible)?;
        } else if !self.member_id.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "MemberId",
                version,
                api_name: "TxnOffsetCommitRequest",
            });
        }
        if 3 <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        } else if self.group_instance_id.is_some() {
            return Err(SerializationError::FieldNotAvailable {
                field: "GroupInstanceId",
                version,
                api_name: "TxnOffsetCommitRequest",
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
        let transactional_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let producer_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let producer_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let generation_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let member_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let group_instance_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            generation_id,
            member_id,
            group_instance_id,
            topics,
        })
    }
}
impl KafkaCodec for TxnOffsetCommitRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.transactional_id.encode(buf, version, is_flexible)?;
        self.group_id.encode(buf, version, is_flexible)?;
        self.producer_id.encode(buf, version, is_flexible)?;
        self.producer_epoch.encode(buf, version, is_flexible)?;
        if 3 <= version.0 {
            self.generation_id.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.member_id.encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
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
        let transactional_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let producer_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let producer_epoch = KafkaCodec::decode(buf, version, is_flexible)?;
        let generation_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let member_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let group_instance_id = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            generation_id,
            member_id,
            group_instance_id,
            topics,
        })
    }
}

impl KafkaCodec for TxnOffsetCommitRequestPartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.committed_offset.encode(buf, version, is_flexible)?;
        if 2 <= version.0 {
            self.committed_leader_epoch
                .encode(buf, version, is_flexible)?;
        }
        self.committed_metadata.encode(buf, version, is_flexible)?;
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
        let partition_index = KafkaCodec::decode(buf, version, is_flexible)?;
        let committed_offset = KafkaCodec::decode(buf, version, is_flexible)?;
        let committed_leader_epoch = if 2 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let committed_metadata = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            committed_offset,
            committed_leader_epoch,
            committed_metadata,
        })
    }
}

impl KafkaCodec for TxnOffsetCommitRequestTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
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
        let name = KafkaCodec::decode(buf, version, is_flexible)?;
        let partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}
