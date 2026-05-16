#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// AlterPartitionReassignmentsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
pub struct AlterPartitionReassignmentsRequest {
    /// The time in ms to wait for the request to complete.
    pub timeout_ms: i32,
    /// The option indicating whether changing the replication factor of any given partition as part of this request is a valid move.
    /// Available in version 1+.
    pub allow_replication_factor_change: bool,
    /// The topics to reassign.
    pub topics: Vec<ReassignableTopic>,
}
impl Default for AlterPartitionReassignmentsRequest {
    fn default() -> Self {
        Self {
            timeout_ms: 60000,
            allow_replication_factor_change: true,
            topics: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReassignablePartition {
    /// The partition index.
    pub partition_index: i32,
    /// The replicas to place the partitions on, or null to cancel a pending reassignment for this partition.
    pub replicas: Option<Vec<i32>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReassignableTopic {
    /// The topic name.
    pub name: String,
    /// The partitions to reassign.
    pub partitions: Vec<ReassignablePartition>,
}

impl ApiRequest for AlterPartitionReassignmentsRequest {
    type Response = crate::generated::AlterPartitionReassignmentsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(45)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 1,
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.timeout_ms.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.allow_replication_factor_change
                .encode(buf, version, is_flexible)?;
        } else if !self.allow_replication_factor_change {
            return Err(SerializationError::FieldNotAvailable {
                field: "AllowReplicationFactorChange",
                version,
                api_name: "AlterPartitionReassignmentsRequest",
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
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let allow_replication_factor_change = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            true
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            timeout_ms,
            allow_replication_factor_change,
            topics,
        })
    }
}
impl KafkaCodec for AlterPartitionReassignmentsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.timeout_ms.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.allow_replication_factor_change
                .encode(buf, version, is_flexible)?;
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
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let allow_replication_factor_change = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            true
        };
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            timeout_ms,
            allow_replication_factor_change,
            topics,
        })
    }
}

impl KafkaCodec for ReassignablePartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.replicas.encode(buf, version, is_flexible)?;
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
        let replicas = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            replicas,
        })
    }
}

impl KafkaCodec for ReassignableTopic {
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
