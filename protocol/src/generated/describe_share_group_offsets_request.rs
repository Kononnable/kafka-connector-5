#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeShareGroupOffsetsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsRequest {
    /// The groups to describe offsets for.
    pub groups: Vec<DescribeShareGroupOffsetsRequestGroup>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsRequestGroup {
    /// The group identifier.
    pub group_id: String,
    /// The topics to describe offsets for, or null for all topic-partitions.
    pub topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeShareGroupOffsetsRequestTopic {
    /// The topic name.
    pub topic_name: String,
    /// The partitions.
    pub partitions: Vec<i32>,
}

impl ApiRequest for DescribeShareGroupOffsetsRequest {
    type Response = crate::generated::DescribeShareGroupOffsetsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(90)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
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
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.groups.encode(buf, version, is_flexible)?;
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
        let groups = <Vec<DescribeShareGroupOffsetsRequestGroup> as KafkaDeserialize>::decode(
            buf,
            version,
            is_flexible,
        )?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { groups })
    }
}
impl KafkaSerialize for DescribeShareGroupOffsetsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.groups.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeShareGroupOffsetsRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let groups = <Vec<DescribeShareGroupOffsetsRequestGroup> as KafkaDeserialize>::decode(
            buf,
            version,
            is_flexible,
        )?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { groups })
    }
}

impl KafkaSerialize for DescribeShareGroupOffsetsRequestGroup {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeShareGroupOffsetsRequestGroup {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let group_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topics =
            <Option<Vec<DescribeShareGroupOffsetsRequestTopic>> as KafkaDeserialize>::decode(
                buf,
                version,
                is_flexible,
            )?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { group_id, topics })
    }
}

impl KafkaSerialize for DescribeShareGroupOffsetsRequestTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.topic_name.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeShareGroupOffsetsRequestTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let topic_name = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let partitions = <Vec<i32> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_name,
            partitions,
        })
    }
}
