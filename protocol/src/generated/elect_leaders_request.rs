#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// ElectLeadersRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
pub struct ElectLeadersRequest {
    /// Type of elections to conduct for the partition. A value of '0' elects the preferred replica. A value of '1' elects the first live replica if there are no in-sync replica.
    /// Available in version 1+.
    pub election_type: i8,
    /// The topic partitions to elect leaders.
    /// IndexMap key `Topic` (string): The name of a topic.
    pub topic_partitions: Option<IndexMap<String, TopicPartitions>>,
    /// The time in ms to wait for the election to complete.
    pub timeout_ms: i32,
}
impl Default for ElectLeadersRequest {
    fn default() -> Self {
        Self {
            election_type: 0,
            topic_partitions: None,
            timeout_ms: 60000,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TopicPartitions {
    /// The partitions of this topic whose leader should be elected.
    pub partitions: Vec<i32>,
}

impl ApiRequest for ElectLeadersRequest {
    type Response = crate::generated::ElectLeadersResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(43)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 2,
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 1 <= version.0 {
            self.election_type.encode(buf, version, is_flexible)?;
        } else if self.election_type != 0 {
            return Err(SerializationError::FieldNotAvailable {
                field: "ElectionType",
                version,
                api_name: "ElectLeadersRequest",
            });
        }
        self.topic_partitions.encode(buf, version, is_flexible)?;
        self.timeout_ms.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let election_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let topic_partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            election_type,
            topic_partitions,
            timeout_ms,
        })
    }
}
impl KafkaCodec for ElectLeadersRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 1 <= version.0 {
            self.election_type.encode(buf, version, is_flexible)?;
        }
        self.topic_partitions.encode(buf, version, is_flexible)?;
        self.timeout_ms.encode(buf, version, is_flexible)?;
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
        let election_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            0
        };
        let topic_partitions = KafkaCodec::decode(buf, version, is_flexible)?;
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            election_type,
            topic_partitions,
            timeout_ms,
        })
    }
}

impl KafkaCodec for TopicPartitions {
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
