#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ShareGroupDescribeRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShareGroupDescribeRequest {
    /// The ids of the groups to describe.
    pub group_ids: Vec<String>,
    /// Whether to include authorized operations.
    pub include_authorized_operations: bool,
}

impl ApiRequest for ShareGroupDescribeRequest {
    type Response = crate::generated::ShareGroupDescribeResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(77)
    }
    fn get_min_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(1)
    }
    fn get_max_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(1)
    }
    fn get_min_flexible_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn serialize(
        &self,
        version: ApiVersionTrait,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 1-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.group_ids.encode(buf, version, is_flexible)?;
        self.include_authorized_operations
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let group_ids = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let include_authorized_operations = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_ids,
            include_authorized_operations,
        })
    }
}
impl KafkaSerialize for ShareGroupDescribeRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.group_ids.encode(buf, version, is_flexible)?;
        self.include_authorized_operations
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ShareGroupDescribeRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let group_ids = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let include_authorized_operations = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_ids,
            include_authorized_operations,
        })
    }
}
