#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AlterPartitionReassignmentsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AlterPartitionReassignmentsRequest {
    /// The time in ms to wait for the request to complete.
    pub timeout_ms: i32,
    /// The option indicating whether changing the replication factor of any given partition as part of this request is a valid move.
    /// Available in version 1+.
    pub allow_replication_factor_change: bool,
    /// The topics to reassign.
    pub topics: Vec<ReassignableTopic>,
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
        self.timeout_ms.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.allow_replication_factor_change
                .encode(buf, version, is_flexible)?;
        } else if self.allow_replication_factor_change {
            return Err(SerializationError::Encode(
                "field 'AllowReplicationFactorChange' is not available in this version",
            ));
        }
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let timeout_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let allow_replication_factor_change = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            timeout_ms,
            allow_replication_factor_change,
            topics,
        })
    }
}
impl KafkaSerialize for AlterPartitionReassignmentsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.timeout_ms.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.allow_replication_factor_change
                .encode(buf, version, is_flexible)?;
        }
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AlterPartitionReassignmentsRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let timeout_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let allow_replication_factor_change = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            timeout_ms,
            allow_replication_factor_change,
            topics,
        })
    }
}

impl KafkaSerialize for ReassignablePartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.replicas.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ReassignablePartition {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let partition_index = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let replicas = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            replicas,
        })
    }
}

impl KafkaSerialize for ReassignableTopic {
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

impl KafkaDeserialize for ReassignableTopic {
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
