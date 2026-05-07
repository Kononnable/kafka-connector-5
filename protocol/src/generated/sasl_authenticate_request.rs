#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// SaslAuthenticateRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SaslAuthenticateRequest {
    /// The SASL authentication bytes from the client, as defined by the SASL mechanism.
    pub auth_bytes: Vec<u8>,
}

impl ApiRequest for SaslAuthenticateRequest {
    type Response = crate::generated::SaslAuthenticateResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(36)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(1)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        self.auth_bytes
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode AuthBytes"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let auth_bytes = <Vec<u8> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode AuthBytes"))?;
        Ok(Self { auth_bytes })
    }
}
impl KafkaSerialize for SaslAuthenticateRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.auth_bytes
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AuthBytes".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for SaslAuthenticateRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let auth_bytes =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode AuthBytes".into(),
            })?;
        Ok(Self { auth_bytes })
    }
}
