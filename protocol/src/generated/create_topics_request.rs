#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// CreateTopicsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
pub struct CreateTopicsRequest {
    /// The topics to create.
    /// IndexMap key `Name` (string): The topic name.
    pub topics: IndexMap<String, CreatableTopic>,
    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,
    /// If true, check that the topics can be created as specified, but don't create anything.
    /// Available in version 1+.
    pub validate_only: bool,
}
impl Default for CreateTopicsRequest {
    fn default() -> Self {
        Self {
            topics: IndexMap::new(),
            timeout_ms: 60000,
            validate_only: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableReplicaAssignment {
    /// The brokers to place the partition on.
    pub broker_ids: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableTopic {
    /// The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
    pub num_partitions: i32,
    /// The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
    pub replication_factor: i16,
    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    /// IndexMap key `PartitionIndex` (int32): The partition index.
    pub assignments: IndexMap<i32, CreatableReplicaAssignment>,
    /// The custom topic configurations to set.
    /// IndexMap key `Name` (string): The configuration name.
    pub configs: IndexMap<String, CreatableTopicConfig>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableTopicConfig {
    /// The configuration value.
    pub value: Option<String>,
}

impl ApiRequest for CreateTopicsRequest {
    type Response = crate::generated::CreateTopicsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(19)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(7)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(5)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            2 <= version.0 && version.0 <= 7,
            "version {} is not supported by {} (supported: 2-7)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.topics.encode(buf, version, is_flexible)?;
        self.timeout_ms.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.validate_only.encode(buf, version, is_flexible)?;
        } else if self.validate_only {
            return Err(SerializationError::FieldNotAvailable {
                field: "validateOnly",
                version,
                api_name: "CreateTopicsRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let validate_only = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            timeout_ms,
            validate_only,
        })
    }
}
impl KafkaCodec for CreateTopicsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topics.encode(buf, version, is_flexible)?;
        self.timeout_ms.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.validate_only.encode(buf, version, is_flexible)?;
        }
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
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let validate_only = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            timeout_ms,
            validate_only,
        })
    }
}

impl KafkaCodec for CreatableReplicaAssignment {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.broker_ids.encode(buf, version, is_flexible)?;
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
        let broker_ids = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { broker_ids })
    }
}

impl KafkaCodec for CreatableTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.num_partitions.encode(buf, version, is_flexible)?;
        self.replication_factor.encode(buf, version, is_flexible)?;
        self.assignments.encode(buf, version, is_flexible)?;
        self.configs.encode(buf, version, is_flexible)?;
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
        let num_partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        let replication_factor = KafkaCodec::decode(buf, version, is_flexible)?;
        let assignments = KafkaCodec::decode(buf, version, is_flexible)?;
        let configs = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            num_partitions,
            replication_factor,
            assignments,
            configs,
        })
    }
}

impl KafkaCodec for CreatableTopicConfig {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.value.encode(buf, version, is_flexible)?;
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
        let value = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { value })
    }
}
