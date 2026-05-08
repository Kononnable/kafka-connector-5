#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// MetadataRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataRequest {
    /// The topics to fetch metadata for.
    pub topics: Option<Vec<MetadataRequestTopic>>,
    /// If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
    /// Available in version 4+.
    pub allow_auto_topic_creation: bool,
    /// Whether to include cluster authorized operations.
    /// Available in version 8-10.
    pub include_cluster_authorized_operations: bool,
    /// Whether to include topic authorized operations.
    /// Available in version 8+.
    pub include_topic_authorized_operations: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetadataRequestTopic {
    /// The topic id.
    /// Available in version 10+.
    pub topic_id: [u8; 16],
    /// The topic name.
    pub name: Option<String>,
}

impl ApiRequest for MetadataRequest {
    type Response = crate::generated::MetadataResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(3)
    }
    fn get_min_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn get_max_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(13)
    }
    fn get_min_flexible_version() -> ApiVersionTrait {
        ApiVersionTrait::new(9)
    }
    fn serialize(
        &self,
        version: ApiVersionTrait,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (13),
            "version {} is not supported by {} (supported: 0-13)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.topics.encode(buf, version, is_flexible)?;
        if (4) <= version.0 {
            self.allow_auto_topic_creation
                .encode(buf, version, is_flexible)?;
        } else if self.allow_auto_topic_creation {
            return Err(SerializationError::Encode(
                "field 'AllowAutoTopicCreation' is not available in this version",
            ));
        }
        if (8) <= version.0 && version.0 <= (10) {
            self.include_cluster_authorized_operations
                .encode(buf, version, is_flexible)?;
        } else if self.include_cluster_authorized_operations {
            return Err(SerializationError::Encode(
                "field 'IncludeClusterAuthorizedOperations' is not available in this version",
            ));
        }
        if (8) <= version.0 {
            self.include_topic_authorized_operations
                .encode(buf, version, is_flexible)?;
        } else if self.include_topic_authorized_operations {
            return Err(SerializationError::Encode(
                "field 'IncludeTopicAuthorizedOperations' is not available in this version",
            ));
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let allow_auto_topic_creation = if (4) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let include_cluster_authorized_operations = if (8) <= version.0 && version.0 <= (10) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let include_topic_authorized_operations = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
        })
    }
}
impl KafkaSerialize for MetadataRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topics.encode(buf, version, is_flexible)?;
        if (4) <= version.0 {
            self.allow_auto_topic_creation
                .encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 && version.0 <= (10) {
            self.include_cluster_authorized_operations
                .encode(buf, version, is_flexible)?;
        }
        if (8) <= version.0 {
            self.include_topic_authorized_operations
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MetadataRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topics = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let allow_auto_topic_creation = if (4) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let include_cluster_authorized_operations = if (8) <= version.0 && version.0 <= (10) {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let include_topic_authorized_operations = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
        })
    }
}

impl KafkaSerialize for MetadataRequestTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if (10) <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        self.name.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for MetadataRequestTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let topic_id = if (10) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_id, name })
    }
}
