#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
        if (3) <= version.0 {
            self.client_software_name
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode ClientSoftwareName"))?;
        }
        if (3) <= version.0 {
            self.client_software_version.encode(buf).map_err(|_| {
                SerializationError::Encode("failed to encode ClientSoftwareVersion")
            })?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let client_software_name = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ClientSoftwareName"))?
        } else {
            Default::default()
        };
        let client_software_version = if (3) <= version.0 {
            <String as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode ClientSoftwareVersion"))?
        } else {
            Default::default()
        };
        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}
impl KafkaSerialize for ApiVersionsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.client_software_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClientSoftwareName".into(),
            })?;
        self.client_software_version
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClientSoftwareVersion".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ApiVersionsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let client_software_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ClientSoftwareName".into(),
            })?;
        let client_software_version =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ClientSoftwareVersion".into(),
            })?;
        Ok(Self {
            client_software_name,
            client_software_version,
        })
    }
}
