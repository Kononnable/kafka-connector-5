#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// CreateTopicsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateTopicsRequest {
    /// The topics to create.
    pub topics: Vec<CreatableTopic>,
    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,
    /// If true, check that the topics can be created as specified, but don't create anything.
    /// Available in version 1+.
    pub validate_only: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableReplicaAssignment {
    /// The partition index.
    pub partition_index: i32,
    /// The brokers to place the partition on.
    pub broker_ids: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableTopic {
    /// The topic name.
    pub name: String,
    /// The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
    pub num_partitions: i32,
    /// The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
    pub replication_factor: i16,
    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    pub assignments: Vec<CreatableReplicaAssignment>,
    /// The custom topic configurations to set.
    pub configs: Vec<CreatableTopicConfig>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatableTopicConfig {
    /// The configuration name.
    pub name: String,
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
            return Err(SerializationError::Encode(
                "field 'validateOnly' is not available in this version",
            ));
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let timeout_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let validate_only = if 1 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
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
impl KafkaSerialize for CreateTopicsRequest {
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
}

impl KafkaDeserialize for CreateTopicsRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let timeout_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let validate_only = if 1 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
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

impl KafkaSerialize for CreatableReplicaAssignment {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.broker_ids.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreatableReplicaAssignment {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let partition_index = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let broker_ids = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            broker_ids,
        })
    }
}

impl KafkaSerialize for CreatableTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.num_partitions.encode(buf, version, is_flexible)?;
        self.replication_factor.encode(buf, version, is_flexible)?;
        self.assignments.encode(buf, version, is_flexible)?;
        self.configs.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreatableTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let num_partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let replication_factor = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let assignments = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let configs = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
        })
    }
}

impl KafkaSerialize for CreatableTopicConfig {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.value.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for CreatableTopicConfig {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let value = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, value })
    }
}
