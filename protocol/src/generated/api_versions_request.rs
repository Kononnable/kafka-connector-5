#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// ApiVersionsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApiVersionsRequest {
    /// The name of the client.
    /// Available in version 3+.
    pub client_software_name: String,
    /// The version of the client.
    /// Available in version 3+.
    pub client_software_version: String,
}

impl ApiRequest for ApiVersionsRequest {
    type Response = crate::generated::ApiVersionsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(18)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(4)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 4,
            "version {} is not supported by {} (supported: 0-4)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 3 <= version.0 {
            self.client_software_name
                .encode(buf, version, is_flexible)?;
        } else if !self.client_software_name.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "ClientSoftwareName",
                version,
                api_name: "ApiVersionsRequest",
            });
        }
        if 3 <= version.0 {
            self.client_software_version
                .encode(buf, version, is_flexible)?;
        } else if !self.client_software_version.is_empty() {
            return Err(SerializationError::FieldNotAvailable {
                field: "ClientSoftwareVersion",
                version,
                api_name: "ApiVersionsRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let client_software_name = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let client_software_version = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}
impl KafkaCodec for ApiVersionsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 3 <= version.0 {
            self.client_software_name
                .encode(buf, version, is_flexible)?;
        }
        if 3 <= version.0 {
            self.client_software_version
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
        let client_software_name = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let client_software_version = if 3 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}
