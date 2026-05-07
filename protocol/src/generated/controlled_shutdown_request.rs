#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{EncodeError, DecodeError, KafkaSerialize, KafkaDeserialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ControlledShutdownRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ControlledShutdownRequest {
}

impl ApiRequest for ControlledShutdownRequest {
    type Response = crate::generated::ControlledShutdownResponse;
    fn get_api_key() -> ApiKey { ApiKey::new(7) }
    fn get_min_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(0) }
    fn get_max_supported_version() -> crate::traits::ApiVersion { crate::traits::ApiVersion::new(-1) }
    fn serialize(&self, version: crate::traits::ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        let is_flexible = false;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: crate::traits::ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = false;
        Ok(Self {  })
    }
}
impl KafkaSerialize for ControlledShutdownRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        Ok(())
    }
    fn encode_flexible<B: BufMut>(&self, buf: &mut B, is_flexible: bool) -> Result<(), EncodeError> {
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ControlledShutdownRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        Ok(Self {  })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {  })
    }
}

