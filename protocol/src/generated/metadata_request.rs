#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// MetadataRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
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
impl Default for MetadataRequest {
    fn default() -> Self {
        Self {
            topics: None,
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        }
    }
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
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(13)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(9)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 13,
            "version {} is not supported by {} (supported: 0-13)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.topics.encode(buf, version, is_flexible)?;
        if 4 <= version.0 {
            self.allow_auto_topic_creation
                .encode(buf, version, is_flexible)?;
        } else if !self.allow_auto_topic_creation {
            return Err(SerializationError::FieldNotAvailable {
                field: "AllowAutoTopicCreation",
                version,
                api_name: "MetadataRequest",
            });
        }
        if 8 <= version.0 && version.0 <= 10 {
            self.include_cluster_authorized_operations
                .encode(buf, version, is_flexible)?;
        } else if self.include_cluster_authorized_operations {
            return Err(SerializationError::FieldNotAvailable {
                field: "IncludeClusterAuthorizedOperations",
                version,
                api_name: "MetadataRequest",
            });
        }
        if 8 <= version.0 {
            self.include_topic_authorized_operations
                .encode(buf, version, is_flexible)?;
        } else if self.include_topic_authorized_operations {
            return Err(SerializationError::FieldNotAvailable {
                field: "IncludeTopicAuthorizedOperations",
                version,
                api_name: "MetadataRequest",
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
        let allow_auto_topic_creation = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            true
        };
        let include_cluster_authorized_operations = if 8 <= version.0 && version.0 <= 10 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        let include_topic_authorized_operations = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
        })
    }
}
impl KafkaCodec for MetadataRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topics.encode(buf, version, is_flexible)?;
        if 4 <= version.0 {
            self.allow_auto_topic_creation
                .encode(buf, version, is_flexible)?;
        }
        if 8 <= version.0 && version.0 <= 10 {
            self.include_cluster_authorized_operations
                .encode(buf, version, is_flexible)?;
        }
        if 8 <= version.0 {
            self.include_topic_authorized_operations
                .encode(buf, version, is_flexible)?;
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
        let allow_auto_topic_creation = if 4 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            true
        };
        let include_cluster_authorized_operations = if 8 <= version.0 && version.0 <= 10 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        let include_topic_authorized_operations = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
        })
    }
}

impl KafkaCodec for MetadataRequestTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 10 <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        self.name.encode(buf, version, is_flexible)?;
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
        let topic_id = if 10 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            [0u8; 16]
        };
        let name = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { topic_id, name })
    }
}
