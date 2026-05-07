#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ListGroupsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListGroupsRequest {}

impl ApiRequest for ListGroupsRequest {
    type Response = crate::generated::ListGroupsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(16)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(3)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 0-3)",
            version.0,
            stringify!(Self)
        );
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        Ok(Self {})
    }
}
impl KafkaSerialize for ListGroupsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl KafkaDeserialize for ListGroupsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        Ok(Self {})
    }
}
