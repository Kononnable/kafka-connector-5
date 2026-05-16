#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// OffsetFetchRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchRequest {
    /// The group to fetch offsets for.
    /// Available in version 0-7.
    pub group_id: String,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    /// Available in version 0-7.
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
    /// Each group we would like to fetch offsets for.
    /// Available in version 8+.
    pub groups: Vec<OffsetFetchRequestGroup>,
    /// Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
    /// Available in version 7+.
    pub require_stable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchRequestGroup {
    /// The group ID.
    /// Available in version 8+.
    pub group_id: String,
    /// The member id.
    /// Available in version 9+.
    pub member_id: Option<String>,
    /// The member epoch if using the new consumer protocol (KIP-848).
    /// Available in version 9+.
    pub member_epoch: i32,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    /// Available in version 8+.
    pub topics: Option<Vec<OffsetFetchRequestTopics>>,
}
impl Default for OffsetFetchRequestGroup {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            member_id: None,
            member_epoch: -1,
            topics: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchRequestTopic {
    /// The topic name.
    /// Available in version 0-7.
    pub name: String,
    /// The partition indexes we would like to fetch offsets for.
    /// Available in version 0-7.
    pub partition_indexes: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OffsetFetchRequestTopics {
    /// The topic name.
    /// Available in version 8-9.
    pub name: String,
    /// The topic ID.
    /// Available in version 10+.
    pub topic_id: [u8; 16],
    /// The partition indexes we would like to fetch offsets for.
    /// Available in version 8+.
    pub partition_indexes: Vec<i32>,
}

impl ApiRequest for OffsetFetchRequest {
    type Response = crate::generated::OffsetFetchResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(9)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(1)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(10)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(6)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            1 <= version.0 && version.0 <= 10,
            "version {} is not supported by {} (supported: 1-10)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 0 <= version.0 && version.0 <= 7 {
            self.group_id.encode(buf, version, is_flexible)?;
        } else if !self.group_id.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "GroupId",
                version,
                api_name: "OffsetFetchRequest",
            });
        }
        if 0 <= version.0 && version.0 <= 7 {
            self.topics.encode(buf, version, is_flexible)?;
        } else if self.topics.is_some() {
            return Err(SerializationError::FieldNotAvailable {
                field: "Topics",
                version,
                api_name: "OffsetFetchRequest",
            });
        }
        if 8 <= version.0 {
            self.groups.encode(buf, version, is_flexible)?;
        } else if !self.groups.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "Groups",
                version,
                api_name: "OffsetFetchRequest",
            });
        }
        if 7 <= version.0 {
            self.require_stable.encode(buf, version, is_flexible)?;
        } else if self.require_stable {
            return Err(SerializationError::FieldNotAvailable {
                field: "RequireStable",
                version,
                api_name: "OffsetFetchRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let group_id = if 0 <= version.0 && version.0 <= 7 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let topics = if 0 <= version.0 && version.0 <= 7 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        let groups = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        let require_stable = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            topics,
            groups,
            require_stable,
        })
    }
}
impl KafkaCodec for OffsetFetchRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 0 <= version.0 && version.0 <= 7 {
            self.group_id.encode(buf, version, is_flexible)?;
        }
        if 0 <= version.0 && version.0 <= 7 {
            self.topics.encode(buf, version, is_flexible)?;
        }
        if 8 <= version.0 {
            self.groups.encode(buf, version, is_flexible)?;
        }
        if 7 <= version.0 {
            self.require_stable.encode(buf, version, is_flexible)?;
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
        let group_id = if 0 <= version.0 && version.0 <= 7 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let topics = if 0 <= version.0 && version.0 <= 7 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        let groups = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        let require_stable = if 7 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            false
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            topics,
            groups,
            require_stable,
        })
    }
}

impl KafkaCodec for OffsetFetchRequestGroup {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 8 <= version.0 {
            self.group_id.encode(buf, version, is_flexible)?;
        }
        if 9 <= version.0 {
            self.member_id.encode(buf, version, is_flexible)?;
        }
        if 9 <= version.0 {
            self.member_epoch.encode(buf, version, is_flexible)?;
        }
        if 8 <= version.0 {
            self.topics.encode(buf, version, is_flexible)?;
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
        let group_id = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let member_id = if 9 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        let member_epoch = if 9 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        let topics = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            member_id,
            member_epoch,
            topics,
        })
    }
}

impl KafkaCodec for OffsetFetchRequestTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 0 <= version.0 && version.0 <= 7 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if 0 <= version.0 && version.0 <= 7 {
            self.partition_indexes.encode(buf, version, is_flexible)?;
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
        let name = if 0 <= version.0 && version.0 <= 7 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let partition_indexes = if 0 <= version.0 && version.0 <= 7 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            partition_indexes,
        })
    }
}

impl KafkaCodec for OffsetFetchRequestTopics {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 8 <= version.0 && version.0 <= 9 {
            self.name.encode(buf, version, is_flexible)?;
        }
        if 10 <= version.0 {
            self.topic_id.encode(buf, version, is_flexible)?;
        }
        if 8 <= version.0 {
            self.partition_indexes.encode(buf, version, is_flexible)?;
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
        let name = if 8 <= version.0 && version.0 <= 9 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            String::new()
        };
        let topic_id = if 10 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            [0u8; 16]
        };
        let partition_indexes = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Vec::new()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            topic_id,
            partition_indexes,
        })
    }
}
