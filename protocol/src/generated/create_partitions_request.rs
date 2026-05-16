#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// CreatePartitionsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatePartitionsRequest {
    /// Each topic that we want to create new partitions inside.
    /// IndexMap key `Name` (string): The topic name.
    pub topics: IndexMap<String, CreatePartitionsTopic>,
    /// The time in ms to wait for the partitions to be created.
    pub timeout_ms: i32,
    /// If true, then validate the request, but don't actually increase the number of partitions.
    pub validate_only: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatePartitionsAssignment {
    /// The assigned broker IDs.
    pub broker_ids: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreatePartitionsTopic {
    /// The new partition count.
    pub count: i32,
    /// The new partition assignments.
    pub assignments: Option<Vec<CreatePartitionsAssignment>>,
}

impl ApiRequest for CreatePartitionsRequest {
    type Response = crate::generated::CreatePartitionsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(37)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 3,
            "version {} is not supported by {} (supported: 0-3)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.topics.encode(buf, version, is_flexible)?;
        self.timeout_ms.encode(buf, version, is_flexible)?;
        self.validate_only.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let topics = KafkaCodec::decode(buf, version, is_flexible)?;
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let validate_only = KafkaCodec::decode(buf, version, is_flexible)?;
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
impl KafkaCodec for CreatePartitionsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.topics.encode(buf, version, is_flexible)?;
        self.timeout_ms.encode(buf, version, is_flexible)?;
        self.validate_only.encode(buf, version, is_flexible)?;
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
        let validate_only = KafkaCodec::decode(buf, version, is_flexible)?;
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

impl KafkaCodec for CreatePartitionsAssignment {
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

impl KafkaCodec for CreatePartitionsTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.count.encode(buf, version, is_flexible)?;
        self.assignments.encode(buf, version, is_flexible)?;
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
        let count = KafkaCodec::decode(buf, version, is_flexible)?;
        let assignments = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { count, assignments })
    }
}
