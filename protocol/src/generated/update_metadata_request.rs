#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// UpdateMetadataRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateMetadataRequest {}

impl ApiRequest for UpdateMetadataRequest {
    type Response = crate::generated::UpdateMetadataResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(6)
    }
    fn get_min_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn get_max_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(-1)
    }
    fn get_min_flexible_version() -> ApiVersionTrait {
        ApiVersionTrait::new(32767)
    }
    fn serialize(
        &self,
        version: ApiVersionTrait,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {})
    }
}
impl KafkaSerialize for UpdateMetadataRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for UpdateMetadataRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {})
    }
}
